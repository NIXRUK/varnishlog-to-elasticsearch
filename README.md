# varnishlog-to-elasticsearch

A lightweight, stream-processing tool that parses Varnish HTTP logs and sends structured JSON documents directly to Elasticsearch.

This tool is designed for simplicity, resilience, and easy integration into production environments. It supports both development and systemd-based deployments, and includes full support for runtime configuration via environment variables.

[![PyPI version](https://badge.fury.io/py/varnishlog-to-elasticsearch.svg)](https://badge.fury.io/py/varnishlog-to-elasticsearch)
[![Python Versions](https://img.shields.io/pypi/pyversions/varnishlog-to-elasticsearch)](https://pypi.org/project/varnishlog-to-elasticsearch/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Features

- Parses full Varnish request lifecycle (`-g request`)
- Captures client and backend metadata
- Automatically determines production/staging mode via headers
- Sends structured JSON documents to Elasticsearch
- Built-in support for environment-based configuration
- Production-ready systemd service example included
- Efficient bulk indexing with configurable buffering
- Secure credential management via environment variables

## 📦 Installation

### From PyPI (Recommended)

```bash
pip install varnishlog-to-elasticsearch
```

### From Source

1. Clone this repository:

```bash
git clone https://github.com/NIXRUK/varnishlog-to-elasticsearch.git
cd varnishlog-to-elasticsearch
```

2. Install in development mode:

```bash
pip install -e .
```

### Production Setup

For production deployment, it's recommended to use a virtual environment:

1. Create a new virtual environment:

```bash
sudo python3 -m venv /opt/varnishlog-to-elasticsearch
```

2. Install the package:

```bash
sudo /opt/varnishlog-to-elasticsearch/bin/pip install .
```

3. Create a symlink for easy access:

```bash
sudo ln -s /opt/varnishlog-to-elasticsearch/bin/varnishlog-to-es /usr/local/bin/varnishlog-to-es
```

## ⚙️ Configuration

All configuration is done through environment variables. The following variables are supported:

### Core Configuration

| Variable          | Description                                 | Default Value      |
|-------------------|---------------------------------------------|-------------------|
| `ES_BASE_URL`     | Elasticsearch base URL                      | `http://localhost:9200` |
| `ES_USERNAME`     | Elasticsearch username                      | `elastic`         |
| `ES_PASSWORD`     | Elasticsearch password                      | (empty)           |
| `ES_VERIFY_SSL`   | Verify SSL certs (`true`, `false`, etc.)    | `true`            |

### Buffering Configuration

| Variable              | Description                                 | Default Value |
|-----------------------|---------------------------------------------|---------------|
| `ES_BUFFER_SIZE`      | Maximum number of documents to buffer       | `100`         |
| `ES_FLUSH_INTERVAL`   | Maximum time (seconds) between flushes      | `5`           |

Set them manually like this:

```bash
export ES_BASE_URL=https://10.99.99.4:9200
export ES_USERNAME=elastic
export ES_PASSWORD=your_secure_password
export ES_VERIFY_SSL=true
export ES_BUFFER_SIZE=200
export ES_FLUSH_INTERVAL=10
```

Or define them in a system-wide config file for systemd (see below).

## 🧪 Usage

### Basic Usage

You can pipe Varnish output directly:

```bash
/usr/bin/varnishlog -g request | varnishlog-to-es
```

Or test with a sample log file:

```bash
cat example.log | varnishlog-to-es
```

### Python API

You can also use the tool programmatically:

```python
from varnishlog_to_elasticsearch.parser import main_loop

# Process logs from a file
with open('varnish.log', 'r') as f:
    main_loop(f)

# Or process from stdin
import sys
main_loop(sys.stdin)
```

## 🛠️ Running as a systemd Service

This project includes a ready-to-use systemd service file.

### 1. Create the environment file

Create a file at `/etc/varnishlog-to-es.env` with your credentials:

```
ES_BASE_URL=https://10.99.99.4:9200
ES_USERNAME=elastic
ES_PASSWORD=your_secure_password
ES_VERIFY_SSL=true
ES_BUFFER_SIZE=200
ES_FLUSH_INTERVAL=10
```

Protect it:

```bash
sudo chmod 640 /etc/varnishlog-to-es.env
sudo chown root:varnish /etc/varnishlog-to-es.env
```

### 2. Create the systemd unit file

Save this to `/etc/systemd/system/varnishlog-to-es.service`:

```
[Unit]
Description=Varnishlog to Elasticsearch
After=network.target

[Service]
Type=simple
User=varnish
Group=varnish
EnvironmentFile=/etc/varnishlog-to-es.env
ExecStart=/usr/bin/varnishlog -g request | varnishlog-to-es
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

### 3. Enable and start the service

```bash
sudo systemctl daemon-reload
sudo systemctl enable varnishlog-to-es
sudo systemctl start varnishlog-to-es
```

### 4. View logs

```bash
journalctl -u varnishlog-to-es -f
```

## 🔒 Security Notes

- Always use HTTPS for Elasticsearch, especially in production.
- Never commit passwords to source code — use environment variables or secrets managers.
- Use `.env` or systemd's `EnvironmentFile` to manage runtime secrets securely.
- SSL verification is enabled by default (`ES_VERIFY_SSL=true`). Only disable it in development environments.
- The buffer size and flush interval can be tuned based on your security and performance requirements.

## 🧹 Future Ideas

- Docker container
- GeoIP enrichment
- Log rotation / buffering
- Index lifecycle policy automation
- Retry mechanism for failed bulk operations
- Metrics and monitoring integration

## 📜 License

MIT © Will Riches — open to contributions and extensions.
