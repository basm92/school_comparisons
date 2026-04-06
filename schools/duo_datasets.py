"""Fetch school performance data from DUO CKAN open data CSVs.

Downloads and caches four datasets from onderwijsdata.duo.nl:
  - brin6_advies.csv         school advice distribution (schooladvies VO)
  - brin6_referentieniveau.csv  reference levels (fundamenteel/streef per subject)
  - brin6_zittenblijvers.csv    retained student counts
  - brin6_leerjaar.csv          enrolled students by grade (used for denominators)

All data is per schoolvestiging identified by INSTELLINGSCODE (4-char BRIN).
PEILJAAR is the reference year (October 1).

Variable names are prefixed with 'duo_'.
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

# DUO CKAN resource URLs
_URLS = {
    "brin6_advies": (
        "https://onderwijsdata.duo.nl/dataset/d6903313-8c64-4b12-b4ac-07e5655ef9f4"
        "/resource/454c5dd4-6815-4b52-ae2f-b9eebc6afa6d/download/brin6_advies.csv"
    ),
    "brin6_referentieniveau": (
        "https://onderwijsdata.duo.nl/dataset/c62ea867-72d4-41ae-81af-8b4db27c5e52"
        "/resource/89920163-8b28-4e1a-b322-c3bb90f74f41/download/brin6_referentieniveau.csv"
    ),
    "brin6_zittenblijvers": (
        "https://onderwijsdata.duo.nl/dataset/f0c79c94-6ffb-44bf-a3d5-9a05aef4ade4"
        "/resource/df7297a9-70a3-4a3a-b4a6-d289c7d16014/download/brin6_zittenblijvers.csv"
    ),
    "brin6_leerjaar": (
        "https://onderwijsdata.duo.nl/dataset/b1c8c9e6-f771-4e02-8888-af4673cec92c"
        "/resource/ddc2a9e9-ab70-4309-b32c-4ab824e2f157/download/brin6_leerjaar.csv"
    ),
}

# School advice numeric codes — DUO 1-cijfer WPO definitie (ROD/BRON encoding)
# Confirmed via cross-reference with allecijfers.nl uitstroom data:
#   code 10 = VWO (pure), code 11 = HAVO/VWO (combined)
#   code 9  = HAVO (pure), code 8  = VMBO-GT/HAVO (combined)
# Order: 1=PRO, 2=VSO, 3=VMBO-BL, 4=VMBO-KL, 5=VMBO-GT,
#        6=VMBO-BL/KL, 7=VMBO-KL/GT, 8=VMBO-GT/HAVO,
#        9=HAVO, 10=VWO, 11=HAVO/VWO, 12=Overig
_ADVIES_MAP: dict[int, str] = {
    1:  "duo_schooladvies_pro_pct",
    2:  "duo_schooladvies_vso_pct",
    3:  "duo_schooladvies_vmbo_b_pct",
    4:  "duo_schooladvies_vmbo_k_pct",
    5:  "duo_schooladvies_vmbo_gt_pct",
    6:  "duo_schooladvies_vmbo_bk_pct",
    7:  "duo_schooladvies_vmbo_k_gt_pct",
    8:  "duo_schooladvies_vmbo_gt_havo_pct",
    9:  "duo_schooladvies_havo_pct",
    10: "duo_schooladvies_vwo_pct",
    11: "duo_schooladvies_havo_vwo_pct",
    12: "duo_schooladvies_overig_pct",
}

# Reference level tags that count toward fundamenteel niveau (≥ 1F)
_FUND_TAGS = {
    "TAAL_LV": {"TAAL_LV_1F", "TAAL_LV_2F"},
    "TAAL_TV": {"TAAL_TV_1F", "TAAL_TV_2F"},
    "REKENEN":  {"REKENEN_1F", "REKENEN_1S", "REKENEN_2F"},
}
# Tags that count toward streefniveau.
# For taal: 2F is the streef level.
# For rekenen: 1S is the PO streef level (S-niveau); 2F is above PO streef (uncommon in PO data).
_STREEF_TAGS = {
    "TAAL_LV": {"TAAL_LV_2F"},
    "TAAL_TV": {"TAAL_TV_2F"},
    "REKENEN":  {"REKENEN_1S", "REKENEN_2F"},
}
# All tags per subject group (for computing denominator)
_ALL_TAGS = {
    "TAAL_LV": {"TAAL_LV_LAGER_1F", "TAAL_LV_1F", "TAAL_LV_2F"},
    "TAAL_TV": {"TAAL_TV_LAGER_1F", "TAAL_TV_1F", "TAAL_TV_2F"},
    "REKENEN":  {"REKENEN_LAGER_1F", "REKENEN_1F", "REKENEN_1S", "REKENEN_2F"},
}
_SUBJECT_VAR = {
    "TAAL_LV": ("duo_pct_fundamenteel_taal_lv", "duo_pct_streef_taal_lv"),
    "TAAL_TV": ("duo_pct_fundamenteel_taal_tv", "duo_pct_streef_taal_tv"),
    "REKENEN":  ("duo_pct_fundamenteel_reken",   "duo_pct_streef_reken"),
}


_EXPECTED_SIZES = {
    "brin6_advies":           18_157_395,
    "brin6_referentieniveau": 20_541_425,
    "brin6_zittenblijvers":    2_504_325,
    "brin6_leerjaar":         25_670_662,
}
# Download in 60 KB range chunks — stays well below the server's ~80 KB reset threshold
_RANGE_CHUNK = 60_000


def _cache_path(name: str) -> Path:
    _CACHE_DIR.mkdir(exist_ok=True)
    return _CACHE_DIR / f"{name}.csv"


def _cache_valid(name: str, path: Path) -> bool:
    """Return True if the cached file looks complete (size within 1% of expected)."""
    if not path.exists():
        return False
    expected = _EXPECTED_SIZES.get(name)
    if expected is None:
        return True  # unknown size, trust it
    actual = path.stat().st_size
    return actual >= expected * 0.99


def _download_csv(name: str) -> Optional[bytes]:
    """Download a DUO CKAN CSV via range requests, caching locally.

    The onderwijsdata.duo.nl server resets connections after ~80 KB.
    We work around this by downloading in small range chunks (60 KB each)
    with a brief pause between requests to avoid rate limits.
    """
    path = _cache_path(name)
    if _cache_valid(name, path):
        logger.debug("Using cached %s", path)
        return path.read_bytes()
    elif path.exists():
        logger.info("Cached %s appears incomplete — re-downloading", name)
        path.unlink()

    url = _URLS[name]
    logger.info("Downloading DUO dataset %s (chunked range requests)...", name)

    # Determine total size via HEAD
    try:
        head = _SESSION.head(url, timeout=15)
        total = int(head.headers.get("content-length", 0))
        if total == 0:
            logger.warning("Could not determine size of %s; skipping", name)
            return None
    except requests.RequestException as exc:
        logger.warning("HEAD request failed for %s: %s", name, exc)
        return None

    parts: list[bytes] = []
    downloaded = 0
    while downloaded < total:
        end = min(downloaded + _RANGE_CHUNK - 1, total - 1)
        for attempt in range(3):
            try:
                resp = _SESSION.get(
                    url,
                    headers={"Range": f"bytes={downloaded}-{end}"},
                    timeout=30,
                )
                if resp.status_code not in (200, 206):
                    logger.warning("HTTP %d fetching range %d-%d of %s", resp.status_code, downloaded, end, name)
                    return None
                parts.append(resp.content)
                downloaded += len(resp.content)
                break
            except requests.RequestException as exc:
                logger.debug("Range %d-%d attempt %d failed: %s", downloaded, end, attempt + 1, exc)
                time.sleep(1.0 * (attempt + 1))
        else:
            logger.warning("Failed to download range %d-%d of %s after 3 attempts", downloaded, end, name)
            return None

        if downloaded % (1024 * 1024) < _RANGE_CHUNK:
            logger.debug("  %s: %.1f / %.1f MB", name, downloaded / 1e6, total / 1e6)
        time.sleep(0.05)  # polite: ~20 req/s

    data = b"".join(parts)
    path.write_bytes(data)
    logger.info("Saved %s (%d bytes)", path, len(data))
    return data


def _load_csv(name: str) -> Optional[pd.DataFrame]:
    """Load a DUO CKAN CSV as a DataFrame."""
    data = _download_csv(name)
    if data is None:
        return None
    try:
        df = pd.read_csv(io.BytesIO(data), dtype=str, low_memory=False)
    except Exception as exc:
        logger.warning("Failed to parse %s: %s", name, exc)
        return None
    df.columns = [c.strip().strip('"').upper() for c in df.columns]
    return df


def _filter_school(df: pd.DataFrame, brin: str) -> pd.DataFrame:
    """Filter DataFrame to rows matching the 4-char BRIN code."""
    brin4 = str(brin).strip().upper()[:4]
    if "INSTELLINGSCODE" not in df.columns:
        return df.iloc[0:0]
    mask = df["INSTELLINGSCODE"].str.strip().str.upper() == brin4
    return df[mask].copy()


def _to_int(val: str) -> Optional[int]:
    """Parse a string to int, returning None for privacy-masked (-1) or invalid values."""
    try:
        n = int(str(val).strip())
        return None if n < 0 else n
    except (ValueError, TypeError):
        return None


# ─── School advice ────────────────────────────────────────────────────────────

def fetch_advies(school: dict, n_years: int = 7) -> list[dict]:
    """Return rows for school advice distribution from brin6_advies.csv."""
    df = _load_csv("brin6_advies")
    if df is None:
        return []

    school_df = _filter_school(df, school.get("brin", ""))
    if school_df.empty:
        return []

    # Only regular basisonderwijs (exclude SBO/WEC)
    if "TYPE_PO" in school_df.columns:
        school_df = school_df[school_df["TYPE_PO"].str.strip().str.upper() == "BO"]

    rows = []
    for year_str, year_df in school_df.groupby("PEILJAAR"):
        try:
            year = int(year_str)
        except (ValueError, TypeError):
            continue

        # Sum counts per advice category across vestingen, ignoring masked (-1) cells
        totals: dict[int, int] = {}
        for _, row in year_df.iterrows():
            code = _to_int(row.get("ADVIES", ""))
            count = _to_int(row.get("AANTAL_LEERLINGEN", ""))
            if code is None or count is None:
                continue
            totals[code] = totals.get(code, 0) + count

        grand_total = sum(totals.values())
        if grand_total == 0:
            continue

        for code, count in totals.items():
            var_name = _ADVIES_MAP.get(code)
            if var_name:
                rows.append({
                    "year": year,
                    "variable": var_name,
                    "value": round(count / grand_total * 100, 2),
                })
    return rows


# ─── Reference levels ─────────────────────────────────────────────────────────

def fetch_referentieniveaus(school: dict, n_years: int = 7) -> list[dict]:
    """Return rows for fundamenteel/streef reference levels from brin6_referentieniveau.csv.

    Produces per-subject percentages:
      duo_pct_fundamenteel_taal_lv / duo_pct_streef_taal_lv
      duo_pct_fundamenteel_taal_tv / duo_pct_streef_taal_tv
      duo_pct_fundamenteel_reken   / duo_pct_streef_reken
    """
    df = _load_csv("brin6_referentieniveau")
    if df is None:
        return []

    school_df = _filter_school(df, school.get("brin", ""))
    if school_df.empty:
        return []

    if "TYPE_PO" in school_df.columns:
        school_df = school_df[school_df["TYPE_PO"].str.strip().str.upper() == "BO"]

    rows = []
    for year_str, year_df in school_df.groupby("PEILJAAR"):
        try:
            year = int(year_str)
        except (ValueError, TypeError):
            continue

        # Aggregate counts per REFERENTIENIVEAU (sum across vestingen and test providers)
        level_counts: dict[str, int] = {}
        for _, row in year_df.iterrows():
            level = str(row.get("REFERENTIENIVEAU", "")).strip()
            count = _to_int(row.get("AANTAL_LEERLINGEN", ""))
            if not level or count is None:
                continue
            level_counts[level] = level_counts.get(level, 0) + count

        for subject, (fund_var, streef_var) in _SUBJECT_VAR.items():
            all_tags = _ALL_TAGS[subject]
            total = sum(level_counts.get(t, 0) for t in all_tags)
            if total == 0:
                continue
            fund = sum(level_counts.get(t, 0) for t in _FUND_TAGS[subject])
            streef = sum(level_counts.get(t, 0) for t in _STREEF_TAGS[subject])
            rows.append({"year": year, "variable": fund_var, "value": round(fund / total * 100, 2)})
            rows.append({"year": year, "variable": streef_var, "value": round(streef / total * 100, 2)})

    return rows


# ─── Zittenblijvers ───────────────────────────────────────────────────────────

def fetch_zittenblijvers(school: dict, n_years: int = 7) -> list[dict]:
    """Return rows for retention rate from brin6_zittenblijvers + brin6_leerjaar.

    Percentage = retained students / total enrolled * 100.
    If brin6_leerjaar is not yet cached, the raw count is returned instead.
    """
    df_zij = _load_csv("brin6_zittenblijvers")
    if df_zij is None:
        return []
    df_lee = _load_csv("brin6_leerjaar")  # may be None if not yet downloaded

    brin = school.get("brin", "")
    school_zij = _filter_school(df_zij, brin)
    school_lee = _filter_school(df_lee, brin)

    if school_zij.empty:
        return []

    if "TYPE_PO" in school_zij.columns:
        school_zij = school_zij[school_zij["TYPE_PO"].str.strip().str.upper() == "BO"]
    if "TYPE_PO" in school_lee.columns:
        school_lee = school_lee[school_lee["TYPE_PO"].str.strip().str.upper() == "BO"]

    # Total enrolled per year from leerjaar
    totals_by_year: dict[int, int] = {}
    if not school_lee.empty:
        for year_str, yr_df in school_lee.groupby("PEILJAAR"):
            try:
                year = int(year_str)
            except (ValueError, TypeError):
                continue
            total = sum(_to_int(r) or 0 for r in yr_df.get("AANTAL_LEERLINGEN", []))
            if total > 0:
                totals_by_year[year] = total

    rows = []
    for year_str, yr_df in school_zij.groupby("PEILJAAR"):
        try:
            year = int(year_str)
        except (ValueError, TypeError):
            continue
        retained = sum(_to_int(r) or 0 for r in yr_df.get("AANTAL_LEERLINGEN", []))
        if retained <= 0:
            continue
        total = totals_by_year.get(year, 0)
        if total > 0:
            rows.append({
                "year": year,
                "variable": "duo_pct_zittenblijvers",
                "value": round(retained / total * 100, 2),
            })
        else:
            # leerjaar not available yet: store raw count
            rows.append({
                "year": year,
                "variable": "duo_leerlingen_zittenblijvers",
                "value": float(retained),
            })
    return rows


# ─── Student counts ───────────────────────────────────────────────────────────

def fetch_leerlingen(school: dict, n_years: int = 7) -> list[dict]:
    """Return total enrolled student counts from brin6_leerjaar.csv."""
    df = _load_csv("brin6_leerjaar")
    if df is None:
        return []

    school_df = _filter_school(df, school.get("brin", ""))
    if school_df.empty:
        return []

    if "TYPE_PO" in school_df.columns:
        school_df = school_df[school_df["TYPE_PO"].str.strip().str.upper() == "BO"]

    rows = []
    for year_str, yr_df in school_df.groupby("PEILJAAR"):
        try:
            year = int(year_str)
        except (ValueError, TypeError):
            continue
        total = sum(_to_int(r) or 0 for r in yr_df.get("AANTAL_LEERLINGEN", []))
        if total > 0:
            rows.append({"year": year, "variable": "duo_leerlingen_totaal", "value": float(total)})
    return rows


# ─── Main entry point ─────────────────────────────────────────────────────────

def fetch(school: dict, n_years: int = 7) -> pd.DataFrame:
    """Fetch all DUO CKAN dataset variables for a school.

    Returns:
        DataFrame with columns: school_name, year, variable, value
    """
    school_name = school["name"]
    brin = school.get("brin", "")
    if not brin:
        logger.warning("No BRIN for %s, skipping duo_datasets fetch", school_name)
        return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

    rows: list[dict] = []

    try:
        rows.extend(fetch_advies(school, n_years))
    except Exception as exc:
        logger.warning("duo_datasets advies error for %s: %s", school_name, exc)

    try:
        rows.extend(fetch_referentieniveaus(school, n_years))
    except Exception as exc:
        logger.warning("duo_datasets referentieniveaus error for %s: %s", school_name, exc)

    try:
        rows.extend(fetch_zittenblijvers(school, n_years))
    except Exception as exc:
        logger.warning("duo_datasets zittenblijvers error for %s: %s", school_name, exc)

    try:
        rows.extend(fetch_leerlingen(school, n_years))
    except Exception as exc:
        logger.warning("duo_datasets leerlingen error for %s: %s", school_name, exc)

    if not rows:
        return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

    df = pd.DataFrame(rows)
    df["school_name"] = school_name

    # Trim to n_years most recent
    if len(df) > 0:
        max_year = df["year"].max()
        df = df[df["year"] >= max_year - n_years + 1]

    return df[["school_name", "year", "variable", "value"]]
