# parser.py
import sys
import json
import time
import os
import requests
from requests.auth import HTTPBasicAuth
import urllib3
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
from config import ES_BASE_URL, USERNAME, PASSWORD, VERIFY_SSL

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Get configuration from environment variables with fallbacks
ES_BASE_URL = os.getenv('ES_BASE_URL', 'http://localhost:9200')
ES_USERNAME = os.getenv('ES_USERNAME', 'elastic')
ES_PASSWORD = os.getenv('ES_PASSWORD', '')
ES_VERIFY_SSL = os.getenv('ES_VERIFY_SSL', 'true').lower() in ('true', '1', 't')
ES_BUFFER_SIZE = int(os.getenv('ES_BUFFER_SIZE', '100'))
ES_FLUSH_INTERVAL = int(os.getenv('ES_FLUSH_INTERVAL', '5'))  # seconds

@dataclass
class BufferConfig:
    max_size: int
    flush_interval: int
    last_flush: float = 0.0

class DocumentBuffer:
    def __init__(self, config: BufferConfig):
        self.config = config
        self.buffer: List[Dict[str, Any]] = []
        self.config.last_flush = time.time()

    def add(self, doc: Dict[str, Any]) -> None:
        self.buffer.append(doc)
        if len(self.buffer) >= self.config.max_size:
            self.flush()

    def should_flush(self) -> bool:
        return (time.time() - self.config.last_flush) >= self.config.flush_interval

    def flush(self) -> None:
        if not self.buffer:
            return

        try:
            # Create index name based on the first document's environment
            env_type = self.buffer[0].get("env", "stage")
            now = datetime.now().strftime('%Y_%m_%d')
            index = f"varnish_log_{env_type}_{now}"

            # Prepare bulk request
            bulk_data = []
            for doc in self.buffer:
                bulk_data.append({"index": {"_index": index}})
                bulk_data.append(doc)

            url = f"{ES_BASE_URL}/_bulk"
            response = requests.post(
                url,
                json=bulk_data,
                auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                verify=ES_VERIFY_SSL,
                timeout=10
            )
            response.raise_for_status()
            debug(f"Bulk posted {len(self.buffer)} documents to {index}: {response.status_code}")
        except Exception as e:
            debug(f"Bulk POST error: {str(e)}")
        finally:
            self.buffer = []
            self.config.last_flush = time.time()

def debug(msg: str) -> None:
    print(f"[{datetime.now().isoformat()}] [DEBUG] {msg}", file=sys.stderr)

def parse_timestamp(line):
    try:
        parts = line.split("Timestamp")[1].strip().split(":", 1)
        label = parts[0].strip()
        value = float(parts[1].strip().split()[0])
        return label, value
    except Exception as e:
        debug(f"Timestamp parse error: {str(e)}")
        return None, None

def parse_header(line, prefix):
    try:
        header_line = line.split(prefix, 1)[1].strip()
        key, value = header_line.split(":", 1)
        return key.strip().lower(), value.strip()
    except Exception as e:
        debug(f"{prefix.strip()} parse error: {str(e)}")
        return None, None

def strip_prefix(line):
    return line.lstrip("- ").strip()

def flush_doc(doc, start_time, resp_time):
    if not doc:
        return

    try:
        if start_time and resp_time:
            doc["duration_ms"] = round((resp_time - start_time) * 1000, 3)

        env_val = doc.get("env", "false")
        env_type = "prod" if env_val.lower() == "true" else "stage"
        doc["env"] = env_type

        now = time.strftime('%Y_%m_%d')
        index = f"varnish_log_{env_type}_{now}"
        doc["timestamp"] = time.strftime('%Y-%m-%dT%H:%M:%S%z')

        for field in ["backend_url", "backend_host", "backend_method", "backend_status"]:
            doc.setdefault(field, None)

        url = f"{ES_BASE_URL}/{index}/_doc/"
        response = requests.post(
            url,
            json=doc,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            verify=VERIFY_SSL,
            timeout=2
        )
        debug(f"Posted to {index}: {response.status_code}")
    except Exception as e:
        debug(f"POST error: {str(e)}")

def main_loop(input_stream):
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
                doc["timestamp"] = datetime.now().isoformat()
                
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
            elif key == "x-forwarded-for" and "client_ip" not in doc:
                doc["client_ip"] = value.split(",")[0]
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

    # Flush any remaining documents
    document_buffer.flush()
