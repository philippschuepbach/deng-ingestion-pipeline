from __future__ import annotations
from urllib.parse import urljoin

GDELT_LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# ---- Lookup base URL ----
GDELT_LOOKUPS_BASE_URL = "https://www.gdeltproject.org/data/lookups/"

# ---- Lookup file names ----
GDELT_LOOKUP_FILES = [
    "CAMEO.country.txt",
    "FIPS.country.txt",
    "CAMEO.type.txt",
    "CAMEO.knowngroup.txt",
    "CAMEO.ethnic.txt",
    "CAMEO.religion.txt",
    "CAMEO.eventcodes.txt",
    "CAMEO.goldsteinscale.txt",
]


def build_lookup_urls() -> list[str]:
    return [urljoin(GDELT_LOOKUPS_BASE_URL, name) for name in GDELT_LOOKUP_FILES]