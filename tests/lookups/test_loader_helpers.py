from __future__ import annotations

from pathlib import Path

import pytest

from deng_ingestion.lookups.loader import (
    _derive_event_base_code,
    _derive_event_root_code,
    _read_tsv_with_header,
    _read_tsv_without_header,
    load_cameo_event_roots_and_codes,
    load_cameo_known_groups,
)


def test_derive_event_base_code_keeps_three_digit_code() -> None:
    assert _derive_event_base_code("123") == "123"


def test_derive_event_base_code_reduces_four_digit_code_to_base_code() -> None:
    assert _derive_event_base_code("1234") == "123"


def test_derive_event_root_code_returns_first_two_digits() -> None:
    assert _derive_event_root_code("1234") == "12"
    assert _derive_event_root_code("12") == "12"


def test_read_tsv_without_header_reads_rows_and_strips_values(tmp_path: Path) -> None:
    path = tmp_path / "sample.tsv"
    path.write_text(" AA \t Switzerland \nBB\t Germany \n", encoding="utf-8")

    rows = _read_tsv_without_header(path, ["country_code", "country_name"])

    assert rows == [
        {"country_code": "AA", "country_name": "Switzerland"},
        {"country_code": "BB", "country_name": "Germany"},
    ]


def test_read_tsv_without_header_raises_on_unexpected_column_count(
    tmp_path: Path,
) -> None:
    path = tmp_path / "sample.tsv"
    path.write_text("AA\tSwitzerland\tEXTRA\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Unexpected column count"):
        _read_tsv_without_header(path, ["country_code", "country_name"])


def test_read_tsv_with_header_reads_and_strips_keys_and_values(tmp_path: Path) -> None:
    path = tmp_path / "sample.tsv"
    path.write_text(" CODE \t LABEL \n AA \t Switzerland \n", encoding="utf-8")

    rows = _read_tsv_with_header(path)

    assert rows == [{"CODE": "AA", "LABEL": "Switzerland"}]


def test_load_cameo_known_groups_applies_curated_resolution(tmp_path: Path) -> None:
    path = tmp_path / "CAMEO.knowngroup.txt"
    path.write_text(
        "\n".join(
            [
                "CODE\tLABEL",
                "WTO\tWorld Trade Org",
                "CEM\tCEMAC",
                "CEM\tCOMESA",
                "UNO\tUnited Nations",
            ]
        ),
        encoding="utf-8",
    )

    rows = load_cameo_known_groups(tmp_path)

    assert {
        "known_group_code": "WTO",
        "known_group_name": "World Trade Organization",
        "is_ambiguous": False,
    } in rows
    assert {
        "known_group_code": "UNO",
        "known_group_name": "United Nations",
        "is_ambiguous": False,
    } in rows
    assert {
        "known_group_code": "CEM",
        "known_group_name": "AMBIGUOUS: CEMAC / COMESA",
        "is_ambiguous": True,
    } in rows


def test_load_cameo_event_roots_and_codes_builds_roots_and_codes(
    tmp_path: Path,
) -> None:
    eventcodes_path = tmp_path / "CAMEO.eventcodes.txt"
    goldstein_path = tmp_path / "CAMEO.goldsteinscale.txt"

    eventcodes_path.write_text(
        "\n".join(
            [
                "CAMEOEVENTCODE\tEVENTDESCRIPTION",
                "12\tReject",
                "121\tReject proposal",
                "1211\tReject mediation",
            ]
        ),
        encoding="utf-8",
    )

    goldstein_path.write_text(
        "\n".join(
            [
                "CAMEOEVENTCODE\tGOLDSTEINSCALE",
                "12\t-2.0",
                "121\t-2.5",
                "1211\t-3.0",
            ]
        ),
        encoding="utf-8",
    )

    event_roots, event_codes = load_cameo_event_roots_and_codes(tmp_path)

    assert event_roots == [
        {
            "event_root_code": "12",
            "event_root_description": "Reject",
        }
    ]

    assert event_codes == [
        {
            "event_code": "12",
            "event_base_code": "12",
            "event_root_code": "12",
            "event_description": "Reject",
            "goldstein_scale": -2.0,
        },
        {
            "event_code": "121",
            "event_base_code": "121",
            "event_root_code": "12",
            "event_description": "Reject proposal",
            "goldstein_scale": -2.5,
        },
        {
            "event_code": "1211",
            "event_base_code": "121",
            "event_root_code": "12",
            "event_description": "Reject mediation",
            "goldstein_scale": -3.0,
        },
    ]


def test_load_cameo_event_roots_and_codes_raises_when_goldstein_missing(
    tmp_path: Path,
) -> None:
    eventcodes_path = tmp_path / "CAMEO.eventcodes.txt"
    goldstein_path = tmp_path / "CAMEO.goldsteinscale.txt"

    eventcodes_path.write_text(
        "\n".join(
            [
                "CAMEOEVENTCODE\tEVENTDESCRIPTION",
                "12\tReject",
                "121\tReject proposal",
            ]
        ),
        encoding="utf-8",
    )

    goldstein_path.write_text(
        "\n".join(
            [
                "CAMEOEVENTCODE\tGOLDSTEINSCALE",
                "12\t-2.0",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Missing Goldstein scale"):
        load_cameo_event_roots_and_codes(tmp_path)
