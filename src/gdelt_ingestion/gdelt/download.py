from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import requests
from loguru import logger


def _filename_from_url(url: str) -> str:
    """Extract filename from URL path."""
    name = Path(urlparse(url).path).name
    if not name:
        raise ValueError(f"URL does not contain a filename: {url}")
    return name


def _build_paths(url: str, dest_dir: Path) -> tuple[Path, Path]:
    """Return (dest_path, tmp_path) where tmp_path is used for atomic downloads."""
    filename = _filename_from_url(url)  # e.g. "20250305123000.export.CSV.zip"
    dest_path = dest_dir / filename
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
    return dest_path, tmp_path


def _should_skip_existing(dest_path: Path, *, overwrite: bool) -> bool:
    return dest_path.exists() and not overwrite


def _stream_to_file(
    url: str,
    *,
    tmp_path: Path,
    timeout_sec: int,
    chunk_size: int,
) -> None:
    """Stream the URL content into tmp_path."""
    with requests.get(url, stream=True, timeout=timeout_sec) as response:
        response.raise_for_status()
        with tmp_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)


def _cleanup_temp_file(path: Path) -> None:
    """Best-effort delete (used for temp files)."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def download_file(
    url: str,
    *,
    dest_dir: Path,
    timeout_sec: int = 60,
    chunk_size: int = 8192,
    overwrite: bool = False,
) -> Path:
    """Download a URL to dest_dir and return the local file path.

    Uses a temporary '.part' file first and then atomically replaces the final file.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path, tmp_path = _build_paths(url, dest_dir)

    if _should_skip_existing(dest_path, overwrite=overwrite):
        logger.info("File already exists, skipping download: {}", dest_path)
        return dest_path

    logger.info("Downloading {} -> {}", url, dest_path)
    try:
        _stream_to_file(
            url, tmp_path=tmp_path, timeout_sec=timeout_sec, chunk_size=chunk_size
        )
        tmp_path.replace(dest_path)  # atomic on same filesystem
        logger.success("Downloaded to {}", dest_path)
        return dest_path

    except requests.RequestException as e:
        raise RuntimeError(f"Download failed for {url} (timeout={timeout_sec}s)") from e
    except OSError as e:
        raise RuntimeError(f"Failed to write file to {dest_path}") from e
    finally:
        _cleanup_temp_file(tmp_path)
