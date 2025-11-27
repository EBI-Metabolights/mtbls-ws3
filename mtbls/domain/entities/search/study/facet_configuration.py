from datetime import datetime, timezone
from typing import Dict, List


def build_year_ranges(start_year: int = 2012) -> List[Dict[str, str]]:
    now = datetime.now(timezone.utc)
    current_year = now.year

    years = list(range(start_year, current_year + 1))
    years.sort(reverse=True)  # match the JS `.sort((one, two) => (one > two ? -1 : 1))`

    ranges: List[Dict[str, str]] = []
    for year in years:
        from_dt = datetime(year, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        to_dt = datetime(year, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc)

        ranges.append(
            {
                "from": _iso_utc(from_dt),
                "to": _iso_utc(to_dt),
                "name": str(year),
            }
        )
    return ranges


def _iso_utc(dt: datetime) -> str:
    """Return ISO8601 UTC string with trailing 'Z' (like JS)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


GB = 1024**3
YEAR_RANGES = build_year_ranges(2012)

VALUE_FACETS = {
    "technologyTypes": {
        "type": "value",
        "field": "technologyTypes",
    },
    "assayTechniques": {
        "type": "value",
        "field": "assayTechniques",
    },
    "submitters_country": {
        "type": "value",
        "field": "submitters.country.keyword",
    },
    "studyTypes": {
        "type": "value",
        "field": "studyTypes",
    },
    "factors": {
        "type": "value",
        "field": "factors",
    },
    "designDescriptors": {
        "type": "value",
        "field": "designDescriptors",
    },
    "organisms": {
        "type": "value",
        "field": "organisms.keyword",
    },
    "organismParts": {
        "type": "value",
        "field": "organismParts.keyword",
    },
    "variants": {
        "type": "value",
        "field": "variants",
    },
    "sampleTypes": {
        "type": "value",
        "field": "sampleTypes",
    },
    "organismKingdom": {
        "type": "value",
        "field": "organismKingdom",
    },
    "organismPhylum": {
        "type": "value",
        "field": "organismPhylum",
    },
    "organismClass": {
        "type": "value",
        "field": "organismClass",
    },
}
FACET_CONFIG = {
    **VALUE_FACETS,
    "sizeInBytes": {
        "type": "range",
        "field": "sizeInBytes",
        "ranges": [
            {"from": 0, "to": GB, "name": "0 - 1GiB"},
            {"from": GB, "to": GB * 10, "name": "1GiB - 10GiB"},
            {"from": GB * 10, "to": GB * 100, "name": "10GiB - 100GiB"},
            {"from": GB * 100, "to": GB * 500, "name": "100GiB - 500GiB"},
            {"from": GB * 500, "to": GB * 1024, "name": "500GiB - 1TiB"},
            {"from": GB * 1024, "name": "1TiB+"},
        ],
    },
    "publicReleaseDate": {
        "type": "range",
        "field": "publicReleaseDate",
        "ranges": YEAR_RANGES,
    },
}
