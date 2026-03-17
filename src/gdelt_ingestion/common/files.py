from __future__ import annotations
from pathlib import Path

def build_part_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".part")

def cleanup_temp_file(path: Path) -> None:
    """Best-effort delete (used for temp files)."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass