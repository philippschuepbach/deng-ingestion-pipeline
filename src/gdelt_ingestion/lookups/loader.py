# Copyright (C) 2026 Philipp Schüpbach
# This file is part of deng-ingestion-pipeline.
#
# deng-ingestion-pipeline is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# deng-ingestion-pipeline is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with deng-ingestion-pipeline. If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import csv
from pathlib import Path


def _read_tsv_with_header(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows: list[dict[str, str]] = []

        for row in reader:
            cleaned_row = {
                str(key).strip(): (value.strip() if value is not None else "")
                for key, value in row.items()
            }
            rows.append(cleaned_row)

    return rows


def _read_tsv_without_header(path: Path, fieldnames: list[str]) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        rows: list[dict[str, str]] = []

        for raw_row in reader:
            if not raw_row:
                continue

            cleaned_values = [value.strip() for value in raw_row]
            if len(cleaned_values) != len(fieldnames):
                raise ValueError(
                    f"Unexpected column count in {path.name}: "
                    f"expected {len(fieldnames)}, got {len(cleaned_values)}"
                )

            rows.append(dict(zip(fieldnames, cleaned_values, strict=True)))

    return rows


def _derive_event_base_code(event_code: str) -> str:
    return event_code[:3] if len(event_code) == 4 else event_code


def _derive_event_root_code(event_code: str) -> str:
    return event_code[:2]


def load_fips_country_codes(lookup_dir: Path) -> list[dict[str, str]]:
    path = lookup_dir / "FIPS.country.txt"
    rows = _read_tsv_without_header(
        path=path,
        fieldnames=["country_code", "country_name"],
    )

    return rows


def load_cameo_country_codes(lookup_dir: Path) -> list[dict[str, str]]:
    path = lookup_dir / "CAMEO.country.txt"
    rows = _read_tsv_with_header(path)

    result: list[dict[str, str]] = []
    for row in rows:
        result.append(
            {
                "country_code": row["CODE"],
                "country_name": row["LABEL"],
            }
        )

    return result


def load_cameo_known_groups(lookup_dir: Path) -> list[dict[str, str | bool]]:
    path = lookup_dir / "CAMEO.knowngroup.txt"
    rows = _read_tsv_with_header(path)

    # Curated resolution:
    # - WTO: normalize naming variant
    # - CEM: conflicting mapping in source lookup, explicitly mark as ambiguous
    curated_by_code: dict[str, dict[str, str | bool]] = {}

    for row in rows:
        code = row["CODE"]
        name = row["LABEL"]

        if code == "WTO":
            name = "World Trade Organization"

        if code == "CEM":
            curated_by_code[code] = {
                "known_group_code": code,
                "known_group_name": "AMBIGUOUS: CEMAC / COMESA",
                "is_ambiguous": True,
            }
            continue

        if code not in curated_by_code:
            curated_by_code[code] = {
                "known_group_code": code,
                "known_group_name": name,
                "is_ambiguous": False,
            }

    return list(curated_by_code.values())


def load_cameo_event_roots_and_codes(
    lookup_dir: Path,
) -> tuple[list[dict[str, str]], list[dict[str, str | float]]]:
    eventcodes_path = lookup_dir / "CAMEO.eventcodes.txt"
    goldstein_path = lookup_dir / "CAMEO.goldsteinscale.txt"

    eventcode_rows = _read_tsv_with_header(eventcodes_path)
    goldstein_rows = _read_tsv_with_header(goldstein_path)

    goldstein_by_code = {
        row["CAMEOEVENTCODE"]: float(row["GOLDSTEINSCALE"]) for row in goldstein_rows
    }

    event_roots: list[dict[str, str]] = []
    seen_roots: set[str] = set()

    event_codes: list[dict[str, str | float]] = []

    for row in eventcode_rows:
        event_code = row["CAMEOEVENTCODE"]
        event_description = row["EVENTDESCRIPTION"]
        event_root_code = _derive_event_root_code(event_code)

        if len(event_code) == 2 and event_root_code not in seen_roots:
            event_roots.append(
                {
                    "event_root_code": event_root_code,
                    "event_root_description": event_description,
                }
            )
            seen_roots.add(event_root_code)

        if event_code not in goldstein_by_code:
            raise ValueError(f"Missing Goldstein scale for event code {event_code}")

        event_codes.append(
            {
                "event_code": event_code,
                "event_base_code": _derive_event_base_code(event_code),
                "event_root_code": event_root_code,
                "event_description": event_description,
                "goldstein_scale": goldstein_by_code[event_code],
            }
        )

    return event_roots, event_codes
