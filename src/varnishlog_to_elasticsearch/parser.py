"""
Varnish log parser module.

This module provides functionality to parse Varnish HTTP logs and send them to Elasticsearch.
"""

import sys
import json
import time
import os
import requests
from requests.auth import HTTPBasicAuth
import urllib3
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

# Get configuration from environment variables with fallbacks
ES_BASE_URL = os.getenv('ES_BASE_URL', 'http://localhost:9200')
ES_USERNAME = os.getenv('ES_USERNAME', 'elastic')
ES_PASSWORD = os.getenv('ES_PASSWORD', '')
ES_VERIFY_SSL = os.getenv('ES_VERIFY_SSL', 'true').lower() in ('true', '1', 't')
ES_BUFFER_SIZE = int(os.getenv('ES_BUFFER_SIZE', '100'))
ES_FLUSH_INTERVAL = int(os.getenv('ES_FLUSH_INTERVAL', '5'))  # seconds

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
    """Handles buffering and bulk sending of documents to Elasticsearch."""
    
    def __init__(self, config: BufferConfig):
        """Initialize the document buffer.
        
        Args:
            config: Buffer configuration settings
        """
        self.config = config
        self.buffer: List[Dict[str, Any]] = []
        self.config.last_flush = time.time()

    def add(self, doc: Dict[str, Any]) -> None:
        """Add a document to the buffer and flush if necessary.
        
        Args:
            doc: Document to add to the buffer
        """
        self.buffer.append(doc)
        if len(self.buffer) >= self.config.max_size:
            self.flush()

    def should_flush(self) -> bool:
        """Check if the buffer should be flushed based on time interval.
        
        Returns:
            bool: True if buffer should be flushed, False otherwise
        """
        return (time.time() - self.config.last_flush) >= self.config.flush_interval

    def flush(self) -> None:
        """Flush the buffer to Elasticsearch."""
        if not self.buffer:
            return

        try:
            # Create index name based on the first document's environment
            env_type = self.buffer[0].get("env", "stage")
            now = datetime.now().strftime('%Y_%m_%d')
            index = f"varnish_log_{env_type}_{now}"

            # Prepare bulk request - as NDJSON (Newline Delimited JSON)
            bulk_data_lines = []
            for doc in self.buffer:
                bulk_data_lines.append(json.dumps({"index": {"_index": index}}))
                bulk_data_lines.append(json.dumps(doc))
            
            # Ensure the request ends with a newline character as required by the Bulk API
            bulk_data = "\n".join(bulk_data_lines) + "\n"

            url = f"{ES_BASE_URL}/_bulk"
            headers = {"Content-Type": "application/x-ndjson"}
            response = requests.post(
                url,
                data=bulk_data,  # Use data instead of json to send NDJSON
                headers=headers,
                auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                verify=ES_VERIFY_SSL,
                timeout=10
            )
            response.raise_for_status()
            debug(f"Bulk posted {len(self.buffer)} documents to {index}: {response.status_code}")
        except Exception as e:
            debug(format_request_error(e, "Bulk POST"))
        finally:
            self.buffer = []
            self.config.last_flush = time.time()

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
                
                for field in ["backend_url", "backend_host", "backend_method", "backend_status"]:
                    doc.setdefault(field, None)
                
                document_buffer.add(doc)
                if document_buffer.should_flush():
                    document_buffer.flush()
                
                doc = {}
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
        elif tag_line.startswith("ReqStart"):
            parts = tag_line.split("ReqStart", 1)[1].strip().split()
            if parts:
                doc["client_ip"] = parts[0]

    # Flush any remaining documents
    document_buffer.flush() 