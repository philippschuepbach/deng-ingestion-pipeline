from __future__ import annotations

from pathlib import Path

PROJECT_NAME = "deng-ingestion-pipeline"
COPYRIGHT_OWNER = "Philipp Schüpbach Schüpbach"

HEADER = f"""# Copyright (C) 2026 {COPYRIGHT_OWNER}
# This file is part of {PROJECT_NAME}.
#
# {PROJECT_NAME} is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# {PROJECT_NAME} is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with {PROJECT_NAME}. If not, see <https://www.gnu.org/licenses/>.

"""

HEADER_MARKER = "GNU Affero General Public License"

OLD_PROJECT_NAMES = ["deng-ingestion-pipeline"]

OLD_COPYRIGHT_OWNERS = []


def split_special_prefix(text: str) -> tuple[str, str]:
    lines = text.splitlines(keepends=True)
    prefix: list[str] = []
    idx = 0

    if idx < len(lines) and lines[idx].startswith("#!"):
        prefix.append(lines[idx])
        idx += 1

    if idx < len(lines) and "coding" in lines[idx]:
        prefix.append(lines[idx])
        idx += 1

    return "".join(prefix), "".join(lines[idx:])


def has_license_header(text: str) -> bool:
    return HEADER_MARKER in text


def replace_old_metadata(text: str) -> str:
    updated = text

    for old_name in OLD_PROJECT_NAMES:
        updated = updated.replace(old_name, PROJECT_NAME)

    for old_owner in OLD_COPYRIGHT_OWNERS:
        updated = updated.replace(old_owner, COPYRIGHT_OWNER)

    return updated


def process_file(path: Path) -> str:
    original = path.read_text(encoding="utf-8")

    if has_license_header(original):
        updated = replace_old_metadata(original)
        if updated != original:
            path.write_text(updated, encoding="utf-8")
            return "updated-existing-header"
        return "skipped-existing-header"

    prefix, rest = split_special_prefix(original)
    updated = f"{prefix}{HEADER}{rest}"
    path.write_text(updated, encoding="utf-8")
    return "inserted-new-header"


def main() -> None:
    project_root = Path(__file__).resolve().parents[3]

    search_dirs = [
        project_root / "src",
        project_root / "scripts",
        # project_root / "tests",
    ]

    all_py_files: list[Path] = []
    for search_dir in search_dirs:
        if search_dir.exists():
            all_py_files.extend(search_dir.rglob("*.py"))

    print(f"Project root: {project_root}")
    print(f"Scanned directories: {[str(p) for p in search_dirs if p.exists()]}")
    print(f"Found {len(all_py_files)} Python file(s).")

    inserted = 0
    updated = 0
    skipped_existing = 0

    for path in all_py_files:
        result = process_file(path)

        if result == "inserted-new-header":
            print(f"inserted: {path.relative_to(project_root)}")
            inserted += 1
        elif result == "updated-existing-header":
            print(f"updated:  {path.relative_to(project_root)}")
            updated += 1
        elif result == "skipped-existing-header":
            skipped_existing += 1

    print()
    print(f"Inserted new headers in {inserted} file(s).")
    print(f"Updated existing headers in {updated} file(s).")
    print(f"Skipped {skipped_existing} file(s) because the header was already correct.")


if __name__ == "__main__":
    main()
