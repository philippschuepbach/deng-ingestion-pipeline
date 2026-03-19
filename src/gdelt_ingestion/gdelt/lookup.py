from __future__ import annotations
from loguru import logger
from ..common.http import fetch_text
from dataclasses import dataclass
from pathlib import Path
import pandas as pd


def extract_url_ending_with(lastupdate_text: str, target_file: str) -> str:
    """Return the URL in lastupdate.txt whose filename ends with target_file."""
    for line in lastupdate_text.splitlines():
        parts = line.split(maxsplit=2)
        if len(parts) != 3:
            continue

        url = parts[2].strip()
        if url.endswith(target_file):
            return url

    raise ValueError(f"No URL ending with {target_file!r} found in lastupdate response")


def get_latest_gdelt_url(
    *,
    last_update_url: str,
    target_file: str = "export.CSV.zip",
    timeout_sec: int = 10,
) -> str:
    """Return the latest GDELT file URL."""
    text = fetch_text(last_update_url, timeout_sec=timeout_sec)
    url = extract_url_ending_with(text, target_file)

    logger.info("Latest GDELT URL: {}", url)
    return url

@dataclass(frozen=True)
class LookupSpec:
    filename: str
    table_name: str
    columns: list[str]
    dtype: dict[str, str]
    has_header: bool = True
    sep: str = "\t"


LOOKUP_SPECS: dict[str, LookupSpec] = {
    "CAMEO.country.txt": LookupSpec(
        filename="CAMEO.country.txt",
        table_name="cameo_country",
        columns=["code", "label"],
        dtype={"code": "string", "label": "string"},
        has_header=True,
    ),
    "FIPS.country.txt": LookupSpec(
        filename="FIPS.country.txt",
        table_name="fips_country",
        columns=["code", "label"],
        dtype={"code": "string", "label": "string"},
        has_header=False,
    ),
    "CAMEO.type.txt": LookupSpec(
        filename="CAMEO.type.txt",
        table_name="cameo_type",
        columns=["code", "label"],
        dtype={"code": "string", "label": "string"},
        has_header=True,
    ),
    "CAMEO.knowngroup.txt": LookupSpec(
        filename="CAMEO.knowngroup.txt",
        table_name="cameo_known_group",
        columns=["code", "label"],
        dtype={"code": "string", "label": "string"},
        has_header=True,
    ),
    "CAMEO.ethnic.txt": LookupSpec(
        filename="CAMEO.ethnic.txt",
        table_name="cameo_ethnic",
        columns=["code", "label"],
        dtype={"code": "string", "label": "string"},
        has_header=True,
    ),
    "CAMEO.religion.txt": LookupSpec(
        filename="CAMEO.religion.txt",
        table_name="cameo_religion",
        columns=["code", "label"],
        dtype={"code": "string", "label": "string"},
        has_header=True,
    ),
    "CAMEO.eventcodes.txt": LookupSpec(
        filename="CAMEO.eventcodes.txt",
        table_name="cameo_eventcode",
        columns=["event_code", "event_description"],
        dtype={"event_code": "string", "event_description": "string"},
        has_header=True,
    ),
    "CAMEO.goldsteinscale.txt": LookupSpec(
        filename="CAMEO.goldsteinscale.txt",
        table_name="cameo_goldstein_scale",
        columns=["event_code", "goldstein_scale"],
        dtype={"event_code": "string", "goldstein_scale": "float64"},
        has_header=True,
    ),
}

def read_lookup_file(path: Path, spec: LookupSpec) -> pd.DataFrame:
    if spec.has_header:
        df = pd.read_csv(
            path,
            sep=spec.sep,
            dtype=spec.dtype,
            header=0,
            keep_default_na=False,
        )
        df.columns = spec.columns
    else:
        df = pd.read_csv(
            path,
            sep=spec.sep,
            dtype=spec.dtype,
            header=None,
            names=spec.columns,
            keep_default_na=False,
        )

    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].str.strip()

    df = df.dropna(how="all").drop_duplicates()

    return df