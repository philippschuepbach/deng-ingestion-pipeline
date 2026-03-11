from __future__ import annotations

import zipfile
from pathlib import Path

from loguru import logger


def _list_payload_files(zf: zipfile.ZipFile) -> list[str]:
    """Return all non-directory members in the ZIP."""
    return [name for name in zf.namelist() if not name.endswith("/")]


def _require_single_file(files: list[str], *, zip_path: Path) -> str:
    """Return the only file in the ZIP or raise a helpful error."""
    if not files:
        raise ValueError(f"ZIP {zip_path} contains no files")
    if len(files) != 1:
        raise ValueError(
            f"Expected 1 file in ZIP {zip_path}, found {len(files)}: {files}"
        )
    return files[0]


def safe_member_path(base_dir: Path, member_name: str) -> Path:
    """Prevent Zip Slip: ensure ZIP member path cannot escape base_dir."""
    member_name = member_name.replace("\\", "/")
    parts = Path(member_name).parts

    # Block absolute paths and obvious traversal attempts
    if member_name.startswith("/") or ".." in parts:
        raise ValueError(f"Unsafe ZIP member path: {member_name}")

    dest = (base_dir / member_name).resolve()
    base = base_dir.resolve()

    if base not in dest.parents and dest != base:
        raise ValueError(f"Unsafe ZIP extraction target: {dest}")

    return dest


def _build_tmp_path(dest_path: Path) -> Path:
    """Return temp path used for atomic extraction."""
    return dest_path.with_suffix(dest_path.suffix + ".part")


def _cleanup_temp_file(path: Path) -> None:
    """Best-effort delete (used for temp files)."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _extract_member_atomically(
    zf: zipfile.ZipFile,
    member: str,
    *,
    dest_path: Path,
    tmp_path: Path,
    chunk_size: int,
) -> None:
    """Copy a ZIP member to tmp_path and then atomically replace dest_path."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with zf.open(member, "r") as src, tmp_path.open("wb") as dst:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            dst.write(chunk)

    tmp_path.replace(dest_path)


def extract_single_file(
    zip_path: Path,
    *,
    dest_dir: Path,
    overwrite: bool = False,
    chunk_size: int = 8192,
) -> Path:
    """Extract a ZIP that is expected to contain exactly one file.

    Uses Zip Slip protection and writes via a temporary '.part' file before replacing the final file.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting {} -> {}", zip_path, dest_dir)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            files = _list_payload_files(zf)
            member = _require_single_file(files, zip_path=zip_path)

            dest_path = safe_member_path(dest_dir, member)
            if dest_path.exists() and not overwrite:
                logger.info("File already exists, skipping extraction: {}", dest_path)
                return dest_path

            tmp_path = _build_tmp_path(dest_path)

            try:
                _extract_member_atomically(
                    zf,
                    member,
                    dest_path=dest_path,
                    tmp_path=tmp_path,
                    chunk_size=chunk_size,
                )
            finally:
                _cleanup_temp_file(tmp_path)

            logger.success("Extracted to {}", dest_path)
            return dest_path

    except zipfile.BadZipFile as e:
        raise RuntimeError(f"Invalid or corrupted ZIP: {zip_path}") from e
    except OSError as e:
        raise RuntimeError(f"Failed to extract {zip_path} into {dest_dir}") from e
