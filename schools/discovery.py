"""Find X nearest elementary schools for a given Dutch postal code.

Uses DUO's open school address data and the PDOK geocoding API.
PC4 coordinate lookups are cached to disk to keep subsequent runs fast.
"""

import json
import logging
import math
import re
import time
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_CACHE_DIR = Path(__file__).parent.parent / ".cache"
_DUO_LOCATIONS_URL = (
    "https://duo.nl/open_onderwijsdata/images/02.-alle-schoolvestigingen-basisonderwijs.csv"
)
_PDOK_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
_PC4_CACHE_FILE = _CACHE_DIR / "pc4_coords.json"

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "school-comparisons/1.0 (educational research)"})


def _load_pc4_cache() -> dict[str, tuple[float, float]]:
    if _PC4_CACHE_FILE.exists():
        try:
            raw = json.loads(_PC4_CACHE_FILE.read_text())
            return {k: tuple(v) for k, v in raw.items()}
        except Exception:
            pass
    return {}


def _save_pc4_cache(cache: dict[str, tuple[float, float]]) -> None:
    _CACHE_DIR.mkdir(exist_ok=True)
    _PC4_CACHE_FILE.write_text(json.dumps(cache))


def _geocode_postcode(postcode: str) -> tuple[float, float]:
    """Return (lat, lon) for a Dutch postcode using PDOK locatieserver."""
    pc = postcode.replace(" ", "").upper()
    resp = _SESSION.get(
        _PDOK_URL,
        params={"q": pc, "fq": "type:postcode", "rows": 1},
        timeout=10,
    )
    resp.raise_for_status()
    docs = resp.json().get("response", {}).get("docs", [])
    if not docs:
        raise ValueError(f"Postcode not found: {postcode}")
    wkt = docs[0].get("centroide_ll", "")
    m = re.search(r"POINT\(([0-9.]+)\s+([0-9.]+)\)", wkt)
    if not m:
        raise ValueError(f"Could not parse coordinates for {postcode}: {wkt}")
    lon, lat = float(m.group(1)), float(m.group(2))
    return lat, lon


def _geocode_batch(pc4_codes: list[str], cache: dict) -> dict[str, tuple[float, float]]:
    """Geocode a list of PC4 codes, using and updating the cache.

    Calls PDOK once per uncached code with a small polite delay.
    """
    results = dict(cache)
    missing = [pc4 for pc4 in pc4_codes if pc4 not in cache]
    if missing:
        logger.info("Geocoding %d PC4 codes (uncached)...", len(missing))
    for pc4 in missing:
        try:
            lat, lon = _geocode_postcode(pc4)
            results[pc4] = (lat, lon)
            time.sleep(0.05)  # polite: ~20 req/s
        except Exception:
            pass
    return results


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _load_duo_locations() -> pd.DataFrame:
    """Download (and cache) DUO school locations CSV."""
    _CACHE_DIR.mkdir(exist_ok=True)
    cache_file = _CACHE_DIR / "duo_schoolvestigingen.csv"

    if not cache_file.exists():
        logger.info("Downloading DUO school locations...")
        resp = _SESSION.get(_DUO_LOCATIONS_URL, timeout=60)
        resp.raise_for_status()
        cache_file.write_bytes(resp.content)
        logger.info("Saved to %s", cache_file)

    df = pd.read_csv(cache_file, sep=";", encoding="latin-1", dtype=str, low_memory=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _make_allecijfers_slug(name: str, city: str) -> str:
    """Convert school name + city to allecijfers.nl URL slug."""
    combined = f"{name} {city}"
    normalized = unicodedata.normalize("NFKD", combined)
    ascii_str = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")
    return slug


def find_nearest(
    postcode: str,
    n: int = 7,
) -> list[dict]:
    """Return list of n nearest elementary schools for the given postcode.

    Each entry is a dict with keys:
        brin, name, city, postcode, lat, lon, dist_km, allecijfers_slug

    Strategy:
    1. Geocode the input postcode (1 PDOK call).
    2. Pre-filter DUO schools to a ±WINDOW range of the PC4 number
       (Dutch PC4 codes are roughly geographically ordered).
    3. Geocode the unique PC4 codes for those candidates, using a persistent
       disk cache so repeated runs are instant.
    4. Compute exact haversine distances and return top-n.
    """
    target_lat, target_lon = _geocode_postcode(postcode)
    logger.info("Target postcode %s → lat=%.4f, lon=%.4f", postcode, target_lat, target_lon)

    df = _load_duo_locations()

    # Map column names (DUO CSV varies by year)
    col_map = {}
    for desired, candidates in {
        "brin": ["instellingscode", "brin_nummer", "brin"],
        "name": ["vestigingsnaam", "instellingsnaam", "naam"],
        "city": ["plaatsnaam", "gemeentenaam", "gemeente"],
        "postcode": ["postcode"],
    }.items():
        for cand in candidates:
            if cand in df.columns:
                col_map[desired] = cand
                break

    missing = [k for k in ["brin", "name", "city", "postcode"] if k not in col_map]
    if missing:
        raise RuntimeError(
            f"Could not find columns {missing} in DUO data. Available: {list(df.columns)}"
        )

    df = df.rename(columns={v: k for k, v in col_map.items()})
    df = df[["brin", "name", "city", "postcode"]].dropna(subset=["postcode"])
    df["pc4"] = df["postcode"].str.replace(r"\s", "", regex=True).str[:4]
    df = df.dropna(subset=["pc4"])

    # Rough PC4-numeric pre-filter; start with a tight window, widen if needed
    target_pc4_num = int(re.sub(r"\D", "", postcode.replace(" ", ""))[:4])

    pc4_cache = _load_pc4_cache()
    result_df: Optional[pd.DataFrame] = None

    for window in [80, 200, 500, 10000]:
        low, high = target_pc4_num - window, target_pc4_num + window
        df["pc4_num"] = pd.to_numeric(df["pc4"], errors="coerce")
        candidates = df[
            df["pc4_num"].between(low, high, inclusive="both")
        ].copy()

        # Geocode candidate PC4 codes (uncached ones only)
        unique_pc4 = candidates["pc4"].unique().tolist()
        pc4_cache = _geocode_batch(unique_pc4, pc4_cache)
        _save_pc4_cache(pc4_cache)

        candidates = candidates[candidates["pc4"].isin(pc4_cache)].copy()
        if len(candidates) == 0:
            continue

        candidates["lat"] = candidates["pc4"].map(lambda x: pc4_cache[x][0])
        candidates["lon"] = candidates["pc4"].map(lambda x: pc4_cache[x][1])
        candidates["dist_km"] = candidates.apply(
            lambda r: _haversine_km(target_lat, target_lon, r["lat"], r["lon"]), axis=1
        )
        top = candidates.nsmallest(n, "dist_km")
        # If max distance in top-n is within 30km, we're good; otherwise widen
        if len(top) >= n and top["dist_km"].max() <= 30.0:
            result_df = top
            break
        elif len(top) >= n:
            result_df = top
            # Don't break — keep widening to ensure we don't miss closer schools
            # outside the PC4 window due to non-geographic ordering
            if window >= 200:
                break

    if result_df is None or len(result_df) == 0:
        raise RuntimeError(f"No schools found near postcode {postcode}")

    result_df = result_df.head(n).copy()
    result_df["allecijfers_slug"] = result_df.apply(
        lambda r: _make_allecijfers_slug(r["name"], r["city"]), axis=1
    )

    results = result_df[
        ["brin", "name", "city", "postcode", "lat", "lon", "dist_km", "allecijfers_slug"]
    ].to_dict("records")

    for r in results:
        logger.info(
            "  %.2f km — %s (%s) BRIN=%s", r["dist_km"], r["name"], r["city"], r["brin"]
        )
    return results
