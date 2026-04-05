"""Scrape school performance data from allecijfers.nl.

Data is embedded as Google Charts arrayToDataTable() calls in script tags.
Variable names are prefixed with 'allecijfers_'.
"""

import ast
import logging
import re
import time
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_BASE_URL = "https://allecijfers.nl/basisschool/"
_SEARCH_URL = "https://allecijfers.nl/"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
_SESSION = requests.Session()
_SESSION.headers.update(_HEADERS)

# Map chart function names to (variable_name, extraction_strategy, extra_kwargs)
# strategy: "timeseries_col1", "uitstroom", "toetsen", "toetsscores", "schoolweging"
_CHART_CONFIG = {
    # grafiek_toetsen is a gauge (current-year only); grafiek_toetsscores is the historical line chart
    # Use toetsscores (more complete) and skip toetsen to avoid duplicates
    "grafiek_toetsscores": ("allecijfers_toetsscore_gem", "toetsscores", {}),
    "grafiek_uitstroom": ("allecijfers_uitstroom", "uitstroom", {}),
    "grafiek_aantal_leerlingen": ("allecijfers_aantal_leerlingen", "timeseries_col1", {}),
    # zittenblijvers are stored as fractions (0.05 = 5%); multiply by 100
    "grafiek_percentage_zittenblijvers": (
        "allecijfers_pct_zittenblijvers", "timeseries_col1", {"multiply": 100.0}
    ),
    "grafiek_schoolwegingen_regionaal": ("allecijfers_schoolweging", "schoolweging", {}),
}


def _js_array_to_python(js_str: str) -> Optional[list]:
    """Parse a JS array literal to Python list, best-effort.

    Handles:
    - Standard JSON-like arrays
    - JS objects like {role: 'style'} → replaced with None
    - null/true/false literals
    """
    # Remove JS-style objects {key: value} → replace with null placeholder
    cleaned = re.sub(r"\{[^}]*\}", "None", js_str)
    # Replace JS null/undefined with None
    cleaned = re.sub(r"\bnull\b|\bundefined\b", "None", cleaned)
    cleaned = re.sub(r"\btrue\b", "True", cleaned)
    cleaned = re.sub(r"\bfalse\b", "False", cleaned)
    try:
        result = ast.literal_eval(cleaned)
        return result if isinstance(result, list) else None
    except Exception as exc:
        logger.debug("Failed to parse JS array: %s", exc)
        return None


def _extract_charts(html: str) -> dict[str, list]:
    """Extract all arrayToDataTable data keyed by chart function name.

    allecijfers.nl embeds JS as single-line function definitions:
        function grafiek_XXX() { var data = arrayToDataTable([...]); ... }

    Strategy: find each arrayToDataTable call, then search backwards for the
    nearest function declaration to determine which chart it belongs to.
    """
    charts: dict[str, list] = {}

    # Find all arrayToDataTable([...]) positions and their content
    # The array ends at the matching ] — we use a depth-counting parser
    for table_match in re.finditer(r"arrayToDataTable\(\s*(\[)", html):
        start = table_match.start(1)
        depth = 0
        i = start
        while i < len(html):
            if html[i] == "[":
                depth += 1
            elif html[i] == "]":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        array_str = html[start : i + 1]

        # Find nearest preceding function name
        preceding = html[max(0, start - 500) : start]
        func_m = None
        for func_m in re.finditer(r"function\s+(grafiek_\w+|dashboard_\w+)\s*\(", preceding):
            pass  # keep last match = nearest to the arrayToDataTable call
        if func_m is None:
            continue
        func_name = func_m.group(1)

        data = _js_array_to_python(array_str)
        if data and len(data) >= 2:
            charts[func_name] = data

    return charts


def _schooljaar_to_year(schooljaar: str) -> Optional[int]:
    """Convert '2023-2024' or '2024-2025' to end year integer (2024, 2025)."""
    m = re.search(r"(\d{4})-(\d{4})", str(schooljaar))
    if m:
        return int(m.group(2))
    m2 = re.search(r"(\d{4})", str(schooljaar))
    if m2:
        return int(m2.group(1))
    return None


def _process_toetsen(data: list, school_name: str) -> list[dict]:
    """Extract test scores, filtering for the school (not gemeente/nederland)."""
    if not data or len(data) < 2:
        return []
    headers = [str(h) for h in data[0]]
    # Expected: ['Type toets', 'Schooljaar', 'Gemiddelde score', 'School / gemeente / Nederland']
    rows = []
    for row in data[1:]:
        if len(row) < 4 or row[3] is None:
            continue
        level = str(row[3]).strip()
        # Keep only the school row (not gemeente or Nederland)
        if level.lower() in ("gemeente", "nederland") or level.lower().startswith(
            ("gemeente ", "provincie ")
        ):
            continue
        year = _schooljaar_to_year(row[1])
        if year is None:
            continue
        try:
            score = float(row[2])
        except (TypeError, ValueError):
            continue
        rows.append({"year": year, "variable": "allecijfers_toetsscore_gem", "value": score})
    return rows


def _process_toetsscores(data: list, school_name: str) -> list[dict]:
    """Historical test score line chart.

    Headers: ['Schooljaar', '{School Name}', 'Gemeente {X}', 'Nederland']
    Extract school score (col 1) and optional comparison values.
    """
    if not data or len(data) < 2:
        return []
    headers = [str(h) if h is not None else "" for h in data[0]]
    rows = []
    for row in data[1:]:
        if len(row) < 2:
            continue
        year = _schooljaar_to_year(row[0])
        if year is None:
            continue
        # Col 1 = school score
        try:
            score = float(row[1])
            rows.append({"year": year, "variable": "allecijfers_toetsscore_gem", "value": score})
        except (TypeError, ValueError):
            pass
        # Col 2 = gemeente average
        if len(row) > 2 and row[2] is not None:
            try:
                rows.append({"year": year, "variable": "allecijfers_toetsscore_gem_gemeente", "value": float(row[2])})
            except (TypeError, ValueError):
                pass
        # Col 3 = national average
        if len(row) > 3 and row[3] is not None:
            try:
                rows.append({"year": year, "variable": "allecijfers_toetsscore_gem_nederland", "value": float(row[3])})
            except (TypeError, ValueError):
                pass
    return rows


def _process_uitstroom(data: list) -> list[dict]:
    """Extract secondary school flow percentages per year."""
    if not data or len(data) < 2:
        return []
    headers = [str(h) for h in data[0]]  # e.g. ['Schooljaar', 'Speciaal/praktijk', 'VMBO-B/K', ...]
    var_map = {
        "speciaal/praktijk": "allecijfers_uitstroom_spo_pct",
        "vmbo-b/k": "allecijfers_uitstroom_vmbo_bk_pct",
        "vmbo-t": "allecijfers_uitstroom_vmbo_t_pct",
        "havo": "allecijfers_uitstroom_havo_pct",
        "vwo": "allecijfers_uitstroom_vwo_pct",
        "overig": "allecijfers_uitstroom_overig_pct",
    }
    rows = []
    for row in data[1:]:
        if len(row) < 2:
            continue
        year = _schooljaar_to_year(row[0])
        if year is None:
            continue
        counts = []
        for i in range(1, len(headers)):
            try:
                counts.append(float(row[i]) if i < len(row) and row[i] is not None else 0.0)
            except (TypeError, ValueError):
                counts.append(0.0)
        total = sum(counts)
        if total == 0:
            continue
        for i, header in enumerate(headers[1:]):
            var_key = header.strip().lower()
            var_name = var_map.get(var_key)
            if var_name and i < len(counts):
                pct = round(counts[i] / total * 100, 2)
                rows.append({"year": year, "variable": var_name, "value": pct})
    return rows


def _process_timeseries_col1(data: list, var_name: str, multiply: float = 1.0) -> list[dict]:
    """Generic time-series: first col = schooljaar, second col = value."""
    if not data or len(data) < 2:
        return []
    rows = []
    for row in data[1:]:
        if len(row) < 2:
            continue
        year = _schooljaar_to_year(row[0])
        if year is None:
            continue
        try:
            value = float(row[1]) * multiply
        except (TypeError, ValueError):
            continue
        rows.append({"year": year, "variable": var_name, "value": round(value, 4)})
    return rows


def _process_schoolweging(data: list, school_name: str) -> list[dict]:
    """Extract school weight for the current year (cross-sectional)."""
    if not data or len(data) < 2:
        return []
    # Find the row for this school (not Nederland/Provincie/Gemeente)
    import datetime
    current_year = datetime.date.today().year
    for row in data[1:]:
        if len(row) < 2 or row[0] is None:
            continue
        label = str(row[0]).strip().lower()
        if label in ("nederland", "provincie", "gemeente") or label.startswith(
            ("provincie ", "gemeente ")
        ):
            continue
        try:
            value = float(row[1])
        except (TypeError, ValueError):
            continue
        return [{"year": current_year, "variable": "allecijfers_schoolweging", "value": value}]
    return []


def _fetch_page(slug: str) -> Optional[str]:
    """Fetch the allecijfers.nl page for the given slug.

    Returns HTML if the page contains chart data, None otherwise.
    allecijfers.nl returns status 200 for unknown slugs (soft-404).
    """
    url = f"{_BASE_URL}{slug}/"
    try:
        resp = _SESSION.get(url, timeout=20)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        html = resp.text
        # Soft-404 check: valid school pages always have arrayToDataTable charts
        if "arrayToDataTable" not in html:
            return None
        return html
    except requests.RequestException as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return None


_STRIP_PREFIXES = re.compile(
    r"^(?:"
    r"(?:p\.?c\.?|pc|r\.?k\.?|rk|obs|o\.b\.s\.|cbs|pcbs|ikc|kc)\s*"
    r"|(?:openbare|protestants[\-\s]christelijk(?:e)?|protestants(?:e)?|christelijk(?:e)?|"
    r"rooms[\-\s]katholiek(?:e)?|gereformeerde?|evangelische?|islamitische?|hindoe|"
    r"joodse?|algemeen[\-\s]bijzondere?|chr\.?)\s*"
    r"|(?:basisschool|kindcentrum|school|stichting|integraal[\-\s]kindcentrum|"
    r"brede[\-\s]school|samenwerking)\s*"
    r")+",
    re.IGNORECASE,
)


def _slug_candidates(name: str, city: str) -> list[str]:
    """Generate ordered list of slug candidates to try on allecijfers.nl."""
    import unicodedata

    def to_slug(s: str) -> str:
        n = unicodedata.normalize("NFKD", s)
        a = n.encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+", "-", a.lower()).strip("-")

    city_slug = to_slug(city)
    candidates = []

    # 1. Full name + city (original)
    candidates.append(to_slug(f"{name} {city}"))

    # 2. Strip common Dutch school type/denomination prefixes, then add city
    stripped = _STRIP_PREFIXES.sub("", name).strip()
    if stripped and stripped.lower() != name.lower():
        candidates.append(to_slug(f"{stripped} {city}"))

    # 3. Strip leading "De/Het/Een/d'" articles after prefix stripping
    no_article = re.sub(r"^(?:de|het|een|d')\s+", "", stripped, flags=re.IGNORECASE).strip()
    if no_article and no_article.lower() != stripped.lower():
        candidates.append(to_slug(f"{no_article} {city}"))

    # 4. Just the city + school name keywords (drop everything before the main noun)
    # e.g. "P.C. basisschool De Parkschool" → "parkschool-amersfoort"
    words = re.sub(r"[^a-zA-Z\s]", "", no_article).split()
    if words:
        main_word = words[-1] if len(words) == 1 else " ".join(words)
        candidates.append(to_slug(f"{main_word} {city}"))

    return list(dict.fromkeys(candidates))  # preserve order, deduplicate


def _search_school(school_name: str, city: str) -> Optional[str]:
    """Try slug candidates on allecijfers.nl and return the working slug.

    allecijfers.nl search is JavaScript-rendered, so we probe candidate slugs
    directly rather than using the search form.
    """
    for slug in _slug_candidates(school_name, city):
        url = f"{_BASE_URL}{slug}/"
        try:
            resp = _SESSION.get(url, timeout=10)
            if resp.status_code == 200 and "arrayToDataTable" in resp.text:
                logger.info("Found allecijfers slug: %s", slug)
                return slug
            time.sleep(0.2)
        except requests.RequestException:
            pass
    return None


def fetch(school: dict, n_years: int = 7) -> pd.DataFrame:
    """Fetch allecijfers.nl data for a school.

    Args:
        school: dict with keys brin, name, city, allecijfers_slug
        n_years: number of most recent years to keep

    Returns:
        DataFrame with columns: school_name, year, variable, value
    """
    school_name = school["name"]
    slug = school.get("allecijfers_slug", "")

    html = _fetch_page(slug)
    if html is None:
        logger.info("Slug '%s' not found, trying search...", slug)
        found_slug = _search_school(school_name, school.get("city", ""))
        if found_slug:
            slug = found_slug
            html = _fetch_page(slug)
    if html is None:
        logger.warning("Could not fetch allecijfers.nl page for %s", school_name)
        return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

    time.sleep(1)  # polite delay

    charts = _extract_charts(html)
    rows: list[dict] = []

    for func_name, chart_data in charts.items():
        if func_name not in _CHART_CONFIG:
            continue
        var_name, strategy, kwargs = _CHART_CONFIG[func_name]

        if strategy == "toetsscores":
            rows.extend(_process_toetsscores(chart_data, school_name))
        elif strategy == "uitstroom":
            rows.extend(_process_uitstroom(chart_data))
        elif strategy == "timeseries_col1":
            rows.extend(_process_timeseries_col1(chart_data, var_name, **kwargs))
        elif strategy == "schoolweging":
            rows.extend(_process_schoolweging(chart_data, school_name))

    if not rows:
        return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

    df = pd.DataFrame(rows)
    df["school_name"] = school_name

    # Keep only n_years most recent years
    if "year" in df.columns and len(df) > 0:
        cutoff = df["year"].max() - n_years + 1
        df = df[df["year"] >= cutoff]

    return df[["school_name", "year", "variable", "value"]]
