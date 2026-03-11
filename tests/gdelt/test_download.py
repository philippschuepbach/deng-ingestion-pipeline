import requests
import pytest

from gdelt_ingestion.gdelt.download import download_file


class FakeResponse:
    """Minimal stand-in for requests.Response used by download tests."""

    def __init__(self, payload: bytes, *, status_ok: bool = True):
        self._payload = payload
        self._status_ok = status_ok

    # Support: `with requests.get(...) as response:`
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False  # don't suppress exceptions

    def raise_for_status(self):
        # Simulate requests raising on non-2xx status codes
        if not self._status_ok:
            raise requests.HTTPError("boom")

    def iter_content(self, chunk_size: int):
        # Yield bytes in chunks, like requests does for streamed downloads
        for i in range(0, len(self._payload), chunk_size):
            yield self._payload[i : i + chunk_size]


def test_download_file_writes_bytes_and_cleans_temp(tmp_path, monkeypatch):
    payload = b"hello-world"
    url = "http://example.com/20250305123000.export.CSV.zip"

    def fake_get(got_url, *, stream, timeout):
        assert got_url == url
        assert stream is True
        assert timeout == 60
        return FakeResponse(payload, status_ok=True)

    monkeypatch.setattr(requests, "get", fake_get)

    dest = download_file(url, dest_dir=tmp_path, timeout_sec=60, chunk_size=4)

    assert dest.name == "20250305123000.export.CSV.zip"
    assert dest.read_bytes() == payload
    assert not (tmp_path / "20250305123000.export.CSV.zip.part").exists()


def test_download_file_skips_if_exists(tmp_path, monkeypatch):
    url = "http://example.com/file.zip"
    existing = tmp_path / "file.zip"
    existing.write_bytes(b"already-there")

    def fake_get(*args, **kwargs):
        raise AssertionError("requests.get should not be called when overwrite=False")

    monkeypatch.setattr(requests, "get", fake_get)

    dest = download_file(url, dest_dir=tmp_path, overwrite=False)

    assert dest == existing
    assert dest.read_bytes() == b"already-there"


def test_download_file_overwrites_if_requested(tmp_path, monkeypatch):
    url = "http://example.com/file.zip"
    existing = tmp_path / "file.zip"
    existing.write_bytes(b"old")

    payload = b"new-data"

    def fake_get(got_url, *, stream, timeout):
        return FakeResponse(payload, status_ok=True)

    monkeypatch.setattr(requests, "get", fake_get)

    dest = download_file(url, dest_dir=tmp_path, overwrite=True)

    assert dest == existing
    assert dest.read_bytes() == payload


def test_download_file_wraps_request_errors_and_cleans_temp(tmp_path, monkeypatch):
    url = "http://example.com/file.zip"

    def fake_get(got_url, *, stream, timeout):
        return FakeResponse(b"", status_ok=False)  # raise_for_status -> HTTPError

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(RuntimeError) as exc:
        download_file(url, dest_dir=tmp_path)

    assert "Download failed" in str(exc.value)
    assert not (tmp_path / "file.zip").exists()
    assert not (tmp_path / "file.zip.part").exists()


def test_download_file_raises_if_url_has_no_filename(tmp_path):
    with pytest.raises(ValueError):
        download_file("http://example.com/", dest_dir=tmp_path)
