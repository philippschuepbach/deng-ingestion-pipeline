import pytest

from gdelt_ingestion.gdelt.lastupdate import extract_url_ending_with


def test_extract_url_ending_with_finds_matching_url():
    lastupdate_text = (
        "123 20250305123000 http://data.gdeltproject.org/gdeltv2/20250305123000.export.CSV.zip\n"
        "456 20250305123000 http://data.gdeltproject.org/gdeltv2/20250305123000.gkg.csv.zip\n"
    )

    url = extract_url_ending_with(lastupdate_text, "export.CSV.zip")

    assert url == "http://data.gdeltproject.org/gdeltv2/20250305123000.export.CSV.zip"


def test_extract_url_ending_with_ignores_malformed_lines():
    lastupdate_text = (
        "this-is-not-valid\n"
        "123 only-two-parts\n"
        "789 20250305123000 http://data.gdeltproject.org/gdeltv2/20250305123000.export.CSV.zip\n"
    )

    url = extract_url_ending_with(lastupdate_text, "export.CSV.zip")

    assert url.endswith("export.CSV.zip")


def test_extract_url_ending_with_raises_if_not_found():
    lastupdate_text = "123 20250305123000 http://data.gdeltproject.org/gdeltv2/20250305123000.gkg.csv.zip\n"

    with pytest.raises(ValueError) as exc:
        extract_url_ending_with(lastupdate_text, "export.CSV.zip")

    assert "export.CSV.zip" in str(exc.value)
