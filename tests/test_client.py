# tests/test_client.py
import pytest
from unittest.mock import patch
import requests

from gdelt_ingestion.client import (
    _extract_url_ending_with,
    _fetch_raw_text,
    get_latest_gdelt_url,
    GDELT_LAST_UPDATE_URL,
)

pytest.skip("Temporarily disabled", allow_module_level=True)

MOCK_GDELT_RESPONSE = """
96263 6ad45bd2f043ec467b71945b4a8848d3 http://data.gdeltproject.org/gdeltv2/20260305124500.export.CSV.zip
123555 44661a768f7631bdb71a4808a4bd8efe http://data.gdeltproject.org/gdeltv2/20260305124500.mentions.CSV.zip
5323606 23befae29b7c522423ca753ba2205b72 http://data.gdeltproject.org/gdeltv2/20260305124500.gkg.csv.zip
"""

MOCK_BAD_RESPONSE = """
123555 44661a768f7631bdb71a4808a4bd8efe http://data.gdeltproject.org/gdeltv2/20260305124500.mentions.CSV.zip
"""


def test_extract_url_ending_with_export():
    """Test that the function correctly extracts the URL ending with 'export.CSV.zip' from the mock response."""
    url = _extract_url_ending_with(MOCK_GDELT_RESPONSE, "export.CSV.zip")
    assert url == "http://data.gdeltproject.org/gdeltv2/20260305124500.export.CSV.zip"


def test_extract_url_ending_with_mentions():
    """Test that the function is reusable for other GDELT data types."""
    url = _extract_url_ending_with(MOCK_GDELT_RESPONSE, "mentions.CSV.zip")
    assert url == "http://data.gdeltproject.org/gdeltv2/20260305124500.mentions.CSV.zip"


def test_extract_url_ending_with_missing():
    """Test behavior when the target file is entirely missing."""
    with pytest.raises(ValueError, match="Missing .nonexistent.zip in GDELT response"):
        _extract_url_ending_with(MOCK_GDELT_RESPONSE, ".nonexistent.zip")


@patch("src.client.requests.get")
def test_fetch_raw_text_success(mock_get):
    """Test that a successful HTTP 200 returns the text."""
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = MOCK_GDELT_RESPONSE

    result = _fetch_raw_text("http://fake-url.com")

    assert result == MOCK_GDELT_RESPONSE
    mock_get.assert_called_once_with("http://fake-url.com", timeout=10)


@patch("src.client.requests.get")
def test_fetch_raw_text_network_error(mock_get):
    """Test that connection drops are caught and re-raised cleanly."""
    mock_get.side_effect = requests.exceptions.ConnectionError("Network Down")

    with pytest.raises(RuntimeError, match="HTTP request failed"):
        _fetch_raw_text("http://fake-url.com")


@patch("src.client.requests.get")
def test_get_latest_gdelt_url_orchestrator(mock_get):
    """Test that the orchestrator calls the fetcher and passes the text to the extractor."""
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = MOCK_GDELT_RESPONSE

    url = get_latest_gdelt_url()

    assert url == "http://data.gdeltproject.org/gdeltv2/20260305124500.export.CSV.zip"
    mock_get.assert_called_once_with(GDELT_LAST_UPDATE_URL, timeout=10)
