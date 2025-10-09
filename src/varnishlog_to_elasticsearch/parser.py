"""
Varnish log parser module.

This module provides functionality to parse Varnish HTTP logs and send them to Elasticsearch.
"""

import sys
import json
import time
import os
import requests
import threading
import queue
from requests.auth import HTTPBasicAuth
import urllib3
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict

# Get configuration from environment variables with fallbacks
ES_BASE_URL = os.getenv('ES_BASE_URL', 'http://localhost:9200')
ES_USERNAME = os.getenv('ES_USERNAME', 'elastic')
ES_PASSWORD = os.getenv('ES_PASSWORD', '')
ES_VERIFY_SSL = os.getenv('ES_VERIFY_SSL', 'true').lower() in ('true', '1', 't')
ES_BUFFER_SIZE = int(os.getenv('ES_BUFFER_SIZE', '100'))
ES_FLUSH_INTERVAL = int(os.getenv('ES_FLUSH_INTERVAL', '5'))  # seconds
DOCKER_HOST_NAME = os.getenv('DOCKER_HOST_NAME')  # Optional hostname of the Docker host
ES_QUEUE_MAXSIZE = int(os.getenv('ES_QUEUE_MAXSIZE', '50'))
ES_MAX_RETRIES = int(os.getenv('ES_MAX_RETRIES', '2'))  # number of retries after initial attempt
ES_RETRY_BACKOFF_SECS = float(os.getenv('ES_RETRY_BACKOFF_SECS', '0.5'))

# Only disable SSL warnings if explicitly configured to do so
if not ES_VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def format_request_error(e: Exception, operation: str) -> str:
    """Format error message with response body if available.
    
    Args:
        e: The exception that occurred
        operation: Description of the operation that failed
        
    Returns:
        str: Formatted error message
    """
    error_msg = str(e)
    if isinstance(e, requests.exceptions.RequestException) and hasattr(e, 'response') and e.response is not None:
        try:
            error_msg += f"\nResponse body: {e.response.text}"
        except Exception:
            pass  # If we can't get the response text, just use the original error
    return f"{operation} error: {error_msg}"

@dataclass
class BufferConfig:
    """Configuration for document buffering."""
    max_size: int
    flush_interval: int
    last_flush: float = 0.0

class DocumentBuffer:
    """Handles buffering and non-blocking bulk sending of documents to Elasticsearch.

    This implementation decouples network I/O from log ingestion using a background
    worker thread. The foreground adds documents and triggers flushes which enqueue
    batches for the worker to send. This avoids blocking stdin consumption and
    prevents backpressure onto the `varnishlog` producer.
    """

    def __init__(self, config: BufferConfig):
        """Initialize the document buffer and start the worker thread.

        Args:
            config: Buffer configuration settings
        """
        self.config = config
        self.buffer: List[Dict[str, Any]] = []
        self.config.last_flush = time.time()

        # Concurrency primitives
        self._buffer_lock = threading.Lock()

        # Queue of batches to send; bounded to limit memory if ES is slow
        self._flush_queue: "queue.Queue[list[dict]]" = queue.Queue(maxsize=ES_QUEUE_MAXSIZE)
        self._shutdown = threading.Event()
        self._worker = threading.Thread(target=self._worker_loop, name="es-bulk-worker", daemon=True)
        self._timer = threading.Thread(target=self._timer_loop, name="es-flush-timer", daemon=True)
        self._session = requests.Session()
        self._worker.start()
        self._timer.start()

    def add(self, doc: Dict[str, Any]) -> None:
        """Add a document to the buffer and flush if necessary.

        Args:
            doc: Document to add to the buffer
        """
        with self._buffer_lock:
            self.buffer.append(doc)
            should_flush_now = len(self.buffer) >= self.config.max_size
        if should_flush_now:
            self.flush()

    def should_flush(self) -> bool:
        """Check if the buffer should be flushed based on time interval.

        Returns:
            bool: True if buffer should be flushed, False otherwise
        """
        return (time.time() - self.config.last_flush) >= self.config.flush_interval

    def flush(self) -> None:
        """Enqueue the current buffer for background sending and reset timing."""
        with self._buffer_lock:
            if not self.buffer:
                return
            batch = self.buffer
            self.buffer = []
            self.config.last_flush = time.time()

        try:
            # Try to enqueue without blocking; drop if queue is full to avoid backpressure
            self._flush_queue.put_nowait(batch)
        except queue.Full:
            debug("Flush queue full; dropping batch to avoid blocking varnishlog reader")

    def close(self, wait: float = 2.0) -> None:
        """Flush remaining docs and stop the worker gracefully.

        Args:
            wait: Maximum seconds to wait for worker shutdown
        """
        # Enqueue any remaining docs
        with self._buffer_lock:
            has_remaining = bool(self.buffer)
        if has_remaining:
            self.flush()

        # Wait until queue is drained or timeout
        end_time = time.time() + wait
        while not self._flush_queue.empty() and time.time() < end_time:
            time.sleep(0.05)

        # Signal shutdown and wait briefly
        self._shutdown.set()
        self._worker.join(timeout=wait)
        # Timer exits when _shutdown is set
        self._timer.join(timeout=wait)

    def _worker_loop(self) -> None:
        """Continuously send batches from the queue to Elasticsearch."""
        while not self._shutdown.is_set() or not self._flush_queue.empty():
            try:
                batch = self._flush_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                self._send_batch(batch)
            except Exception as e:
                debug(format_request_error(e, "Bulk POST"))
            finally:
                self._flush_queue.task_done()

    def _send_batch(self, docs: List[Dict[str, Any]]) -> None:
        """Send a batch of documents to Elasticsearch, grouped by env."""
        grouped_docs = defaultdict(list)
        for doc in docs:
            env_type = doc.get("env", "stage")
            grouped_docs[env_type].append(doc)

        now = datetime.now().strftime('%Y_%m_%d')
        url = f"{ES_BASE_URL}/_bulk"
        headers = {"Content-Type": "application/x-ndjson"}

        for env_type, env_docs in grouped_docs.items():
            index = f"varnish_log_{env_type}_{now}"
            bulk_data_lines = []
            for d in env_docs:
                bulk_data_lines.append(json.dumps({"index": {"_index": index}}))
                bulk_data_lines.append(json.dumps(d))
            bulk_data = "\n".join(bulk_data_lines) + "\n"

            attempt = 0
            while True:
                attempt += 1
                try:
                    response = self._session.post(
                        url,
                        data=bulk_data,
                        headers=headers,
                        auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                        verify=ES_VERIFY_SSL,
                        timeout=10
                    )
                    # Retry on 429/5xx
                    status = response.status_code
                    if status == 429 or 500 <= status < 600:
                        raise requests.exceptions.HTTPError(f"Server error {status}", response=response)

                    response.raise_for_status()

                    # Log per-item errors if any (do not retry; usually data-related)
                    try:
                        body = response.json()
                        if isinstance(body, dict) and body.get("errors"):
                            error_items = [
                                item for item in body.get("items", [])
                                if any(op.get("error") for op in item.values())
                            ]
                            debug(f"Bulk to {index} completed with item errors: {len(error_items)} of {len(env_docs)}")
                    except Exception:
                        pass

                    debug(f"Bulk posted {len(env_docs)} documents to {index}: {response.status_code}")
                    break
                except requests.exceptions.RequestException as e:
                    # Decide retry
                    retryable = False
                    resp = getattr(e, 'response', None)
                    if resp is not None:
                        sc = resp.status_code
                        if sc == 429 or 500 <= sc < 600:
                            retryable = True
                    else:
                        # Network errors are retryable
                        retryable = True

                    if retryable and attempt <= (1 + ES_MAX_RETRIES):
                        backoff = ES_RETRY_BACKOFF_SECS * (2 ** (attempt - 1))
                        debug(f"Bulk POST retry {attempt}/{1 + ES_MAX_RETRIES} after error: {str(e)}; backing off {backoff:.2f}s")
                        time.sleep(backoff)
                        continue
                    # Exhausted retries or not retryable
                    raise

    def _timer_loop(self) -> None:
        """Periodic timer that triggers flushes based on time interval."""
        # Wake up frequently to enforce flush interval without adding latency
        poll_interval = min(max(self.config.flush_interval, 1), 5)
        while not self._shutdown.is_set():
            time.sleep(0.5)
            # Only flush if interval elapsed and there is something to flush
            if self.should_flush():
                with self._buffer_lock:
                    has_buffer = bool(self.buffer)
                if has_buffer:
                    self.flush()

def debug(msg: str) -> None:
    """Print debug message with timestamp.
    
    Args:
        msg: Message to print
    """
    print(f"[{datetime.now().isoformat()}] [DEBUG] {msg}", file=sys.stderr)

def parse_timestamp(line: str) -> tuple[Optional[str], Optional[float]]:
    """Parse timestamp from Varnish log line.
    
    Args:
        line: Log line containing timestamp
        
    Returns:
        tuple: (label, value) or (None, None) if parsing fails
    """
    try:
        parts = line.split("Timestamp")[1].strip().split(":", 1)
        label = parts[0].strip()
        value = float(parts[1].strip().split()[0])
        return label, value
    except Exception as e:
        debug(f"Timestamp parse error: {str(e)}")
        return None, None

def parse_header(line: str, prefix: str) -> tuple[Optional[str], Optional[str]]:
    """Parse header from Varnish log line.
    
    Args:
        line: Log line containing header
        prefix: Header prefix to look for
        
    Returns:
        tuple: (key, value) or (None, None) if parsing fails
    """
    try:
        header_line = line.split(prefix, 1)[1].strip()
        key, value = header_line.split(":", 1)
        return key.strip().lower(), value.strip()
    except Exception as e:
        debug(f"{prefix.strip()} parse error: {str(e)}")
        return None, None

def strip_prefix(line: str) -> str:
    """Strip prefix from log line.
    
    Args:
        line: Log line to process
        
    Returns:
        str: Line with prefix stripped
    """
    return line.lstrip("- ").strip()

def main_loop(input_stream) -> None:
    """Main processing loop for Varnish logs.
    
    Args:
        input_stream: Input stream containing Varnish logs
    """
    doc = {}
    start_time = None
    resp_time = None
    inside_request = False
    
    buffer_config = BufferConfig(
        max_size=ES_BUFFER_SIZE,
        flush_interval=ES_FLUSH_INTERVAL
    )
    document_buffer = DocumentBuffer(buffer_config)

    try:
        for line in input_stream:
            line = line.strip()

            if line.startswith("*") and "Request" in line:
                if inside_request:
                    if start_time and resp_time:
                        doc["duration_ms"] = round((resp_time - start_time) * 1000, 3)
                    env_val = doc.get("env", "false")
                    env_type = "prod" if env_val.lower() == "true" else "stage"
                    doc["env"] = env_type
                    doc["@timestamp"] = datetime.now().astimezone().isoformat()
                    
                    for field in ["backend_url", "backend_host", "backend_method", "backend_status", "backend_reason"]:
                        doc.setdefault(field, None)
                    
                    document_buffer.add(doc)
                    if document_buffer.should_flush():
                        document_buffer.flush()
                    
                    doc = {}
                    # Initialize new document with Docker hostname if available
                    if DOCKER_HOST_NAME:
                        doc["docker_host_name"] = DOCKER_HOST_NAME
                    start_time = None
                    resp_time = None
                inside_request = True
                continue

            if "Timestamp" in line:
                label, value = parse_timestamp(line)
                if label == "Start":
                    start_time = value
                elif label == "Resp":
                    resp_time = value
                continue

            tag_line = strip_prefix(line)

            if tag_line.startswith("ReqMethod"):
                doc["request_method"] = tag_line.split("ReqMethod")[1].strip()
            elif tag_line.startswith("ReqURL"):
                doc["request_url"] = tag_line.split("ReqURL")[1].strip()
            elif tag_line.startswith("RespStatus"):
                try:
                    doc["status"] = int(tag_line.split("RespStatus")[1].strip())
                except Exception as e:
                    debug(f"RespStatus parse error: {str(e)}")
            elif tag_line.startswith("ReqHeader"):
                key, value = parse_header(tag_line, "ReqHeader")
                if not key:
                    continue
                if key == "x-production":
                    doc["env"] = value
                elif key == "user-agent" and "user_agent" not in doc:
                    doc["user_agent"] = value
                elif key == "host" and "request_host" not in doc:
                    doc["request_host"] = value
                elif key in ["x-forwarded-proto", "scheme"] and "request_proto" not in doc:
                    doc["request_proto"] = value
                elif key == "accept-language" and "accept_language" not in doc:
                    doc["accept_language"] = value
                elif key == "x-cache":
                    doc["cache_status"] = value
                elif key == "x-real-ip":
                    doc["real_ip"] = value
            elif tag_line.startswith("BereqMethod"):
                doc["backend_method"] = tag_line.split("BereqMethod")[1].strip()
            elif tag_line.startswith("BereqURL"):
                doc["backend_url"] = tag_line.split("BereqURL")[1].strip()
            elif tag_line.startswith("BereqHeader"):
                key, value = parse_header(tag_line, "BereqHeader")
                if key == "host" and "backend_host" not in doc:
                    doc["backend_host"] = value
            elif tag_line.startswith("BerespStatus"):
                try:
                    doc["backend_status"] = int(tag_line.split("BerespStatus")[1].strip())
                except Exception as e:
                    debug(f"BerespStatus parse error: {str(e)}")
            elif tag_line.startswith("BerespReason"):
                doc["backend_reason"] = tag_line.split("BerespReason")[1].strip()
            elif tag_line.startswith("ReqStart"):
                parts = tag_line.split("ReqStart", 1)[1].strip().split()
                if parts:
                    doc["client_ip"] = parts[0]
    finally:
        # Ensure remaining documents are sent and worker is shut down
        document_buffer.close()