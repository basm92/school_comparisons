"""Fetch school data from DUO open data.

Downloads annual CSV files for:
- Bekostigde leerlingen (funded student counts) per school vestiging

Variable names are prefixed with 'duo_'.

Note: Doorstroomtoets (fundamenteel/streefniveau) and schooladvies data are
sourced from scholenopdekaart.nl rather than DUO, as DUO does not publish
those datasets in a consistent per-school CSV format.
"""

import io
import logging
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_CACHE_DIR = Path(__file__).parent.parent / ".cache"
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "school-comparisons/1.0 (educational research)"})

# Bekostigde leerlingen per vestiging — confirmed working URLs
# Year in filename = reference date (Feb 1 of that year)
_LEERLINGEN_URL = (
    "https://duo.nl/open_onderwijsdata/images/bekostigde-leerlingen-per-vestiging-{year}.csv"
)


def _cache_path(filename: str) -> Path:
    _CACHE_DIR.mkdir(exist_ok=True)
    return _CACHE_DIR / filename


def _download_csv(url: str, cache_name: str) -> Optional[bytes]:
    """Download a CSV, caching locally. Returns bytes or None on failure."""
    path = _cache_path(cache_name)
    if path.exists():
        return path.read_bytes()
    try:
        resp = _SESSION.get(url, timeout=30)
        if resp.status_code in (301, 302, 404):
            logger.debug("Not found (status %d): %s", resp.status_code, url)
            return None
        resp.raise_for_status()
        # Detect HTML soft-404 (server returns 200 but with an HTML error page)
        content = resp.content
        if content[:100].lstrip().lower().startswith((b"<!doctype", b"<html")):
            logger.debug("Soft-404 (HTML response) for %s", url)
            return None
        path.write_bytes(content)
        time.sleep(0.3)
        return content
    except requests.RequestException as exc:
        logger.warning("Download failed %s: %s", url, exc)
        return None


def _normalize_brin(brin: str) -> str:
    return str(brin).strip().upper()


def _fetch_leerlingen_year(year: int, brin: str) -> list[dict]:
    """Download and parse bekostigde leerlingen CSV for a given year."""
    url = _LEERLINGEN_URL.format(year=year)
    data = _download_csv(url, f"leerlingen_{year}.csv")
    if data is None:
        return []

    try:
        # File has a UTF-8 BOM but is actually latin-1; use utf-8-sig to strip BOM
        df = pd.read_csv(
            io.BytesIO(data), sep=";", encoding="utf-8-sig", dtype=str, low_memory=False
        )
    except Exception:
        try:
            df = pd.read_csv(
                io.BytesIO(data), sep=";", encoding="latin-1", dtype=str, low_memory=False
            )
        except Exception as exc:
            logger.warning("Failed to parse leerlingen CSV %d: %s", year, exc)
            return []

    # Normalize column names (strip BOM artifact and whitespace)
    df.columns = [c.strip().strip('"').lower().replace(" ", "_") for c in df.columns]

    # The CSV has 'vestigingscode' (e.g. '00ZR00') — BRIN is the first 4 chars
    vest_col = next((c for c in df.columns if "vestigingscode" in c), None)
    if vest_col is None:
        logger.debug("No vestigingscode column in leerlingen %d. Columns: %s", year, list(df.columns))
        return []

    target_brin = _normalize_brin(brin)
    # Match rows where the first 4 chars of vestigingscode equal the BRIN
    mask = df[vest_col].str.strip().str.upper().str[:4] == target_brin
    school_df = df[mask]
    if school_df.empty:
        return []

    # Student count column
    count_col = next(
        (c for c in df.columns if "leerling" in c and "bekostigd" in c),
        None,
    )
    if count_col is None:
        count_col = next(
            (c for c in df.columns if "aantal" in c and "leerling" in c), None
        )
    if count_col is None:
        count_col = next((c for c in df.columns if "leerling" in c), None)
    if count_col is None:
        logger.debug("No leerlingen count column in %d. Columns: %s", year, list(df.columns))
        return []

    total = 0.0
    for _, row in school_df.iterrows():
        try:
            total += float(str(row[count_col]).replace(",", ".").replace(" ", ""))
        except (TypeError, ValueError):
            pass
    if total > 0:
        return [{"year": year, "variable": "duo_leerlingen_bekostigd", "value": total}]
    return []


def fetch(school: dict, n_years: int = 7) -> pd.DataFrame:
    """Fetch DUO open data for a school.

    Args:
        school: dict with keys brin, name, city, ...
        n_years: number of most recent years to keep

    Returns:
        DataFrame with columns: school_name, year, variable, value
    """
    brin = school.get("brin", "")
    school_name = school["name"]

    if not brin:
        logger.warning("No BRIN for school %s, skipping DUO fetch", school_name)
        return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

    import datetime
    current_year = datetime.date.today().year
    # Reference date is Feb 1; the current-year file is published in early spring
    # and the next-year file doesn't exist yet
    years = list(range(current_year - n_years + 1, current_year + 1))

    rows: list[dict] = []
    for year in years:
        rows.extend(_fetch_leerlingen_year(year, brin))

    if not rows:
        return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

    df = pd.DataFrame(rows)
    df["school_name"] = school_name

    if len(df) > 0:
        cutoff = df["year"].max() - n_years + 1
        df = df[df["year"] >= cutoff]

    return df[["school_name", "year", "variable", "value"]]
