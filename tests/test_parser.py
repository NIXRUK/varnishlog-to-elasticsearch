"""
Tests for the Varnish log parser.
"""

import io
import pytest
from varnishlog_to_elasticsearch.parser import parse_timestamp, parse_header, strip_prefix

def test_parse_timestamp():
    """Test timestamp parsing."""
    line = "Timestamp Start: 1234567890.123456 0.000000"
    label, value = parse_timestamp(line)
    assert label == "Start"
    assert value == 1234567890.123456

    # Test invalid timestamp
    line = "Invalid timestamp"
    label, value = parse_timestamp(line)
    assert label is None
    assert value is None

def test_parse_header():
    """Test header parsing."""
    line = "ReqHeader host: example.com"
    key, value = parse_header(line, "ReqHeader")
    assert key == "host"
    assert value == "example.com"

    # Test invalid header
    line = "Invalid header"
    key, value = parse_header(line, "ReqHeader")
    assert key is None
    assert value is None

def test_strip_prefix():
    """Test prefix stripping."""
    line = "- ReqHeader host: example.com"
    assert strip_prefix(line) == "ReqHeader host: example.com"

    line = "  - ReqHeader host: example.com"
    assert strip_prefix(line) == "ReqHeader host: example.com" 