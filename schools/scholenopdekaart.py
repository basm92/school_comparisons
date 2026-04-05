"""Scrape parent satisfaction and learning results from scholenopdekaart.nl.

Uses Playwright (Chromium) to render JavaScript-heavy pages that block
regular HTTP requests.

Variable names are prefixed with 'scholenopdekaart_'.
"""

import logging
import re
import time
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_BASE_URL = "https://scholenopdekaart.nl/"
_SEARCH_URL = "https://scholenopdekaart.nl/basisscholen/"


def _make_sodk_slug(name: str) -> str:
    """Create scholenopdekaart.nl-style slug from school name."""
    import unicodedata
    normalized = unicodedata.normalize("NFKD", name)
    ascii_str = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")
    return slug


def _extract_number(text: str, pattern: str) -> Optional[float]:
    """Extract a Dutch-format number from text using a regex pattern.

    Dutch numbers use comma as decimal separator (8,0 → 8.0).
    """
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        num_str = m.group(1).replace(",", ".")
        try:
            return float(num_str)
        except ValueError:
            pass
    return None


def _parse_tevredenheid(html: str) -> list[dict]:
    """Extract numeric satisfaction scores from tevredenheid page HTML."""
    rows = []

    # Rapportcijfer: "rapportcijfer van X,X" or "rapportcijfer: X,X"
    score = _extract_number(html, r"rapportcijfer[^0-9]*?(\d+[,\.]\d+)")
    if score is not None:
        rows.append({"variable": "scholenopdekaart_oudertevredenheid_rapportcijfer", "value": score})

    # Average questionnaire score: "gemiddeld[e]? [eindcijfer|score] [van|:] X,XX"
    avg_score = _extract_number(
        html,
        r"gemiddeld[ae]?\s+(?:eindcijfer|score|totaalscore)\s+(?:van\s+|:?\s*)(\d+[,\.]\d+)",
    )
    if avg_score is not None:
        rows.append({"variable": "scholenopdekaart_oudertevredenheid_gem_score", "value": avg_score})

    # Response rate: "XX% " or "responspercentage van XX%"
    respons = _extract_number(
        html,
        r"(?:responspercentage|respons)[^0-9]*?(\d+[,\.]?\d*)\s*%",
    )
    if respons is not None:
        rows.append({"variable": "scholenopdekaart_oudertevredenheid_respons_pct", "value": respons})

    return rows


def _parse_resultaten_page(html: str) -> list[dict]:
    """Extract all metrics from a SODK resultaten page via Highcharts ARIA labels.

    SODK embeds chart values in aria-label attributes:
        "0, 97,7. deze school."        → % fundamenteel for this school
        "0, 85. signaleringswaarde."   → signaleringswaarde fundamenteel
        "0, Geen gegevens. correctiewaarde."  → separator between charts
        "vmbo-b / -k, 2,9. deze school."     → school advice
    """
    aria_entries = re.findall(r'aria-label="([^"]*\d[^"]*)"', html)

    rows = []

    # ------------------------------------------------------------------ #
    # Referentieniveaus (fundamenteel then streef)                        #
    # ------------------------------------------------------------------ #
    # Match both numeric and non-numeric (e.g. "Geen gegevens") values
    ref_pattern = re.compile(
        r"^0,\s*(.+?)\.\s*(deze school|vergelijkbare scholen|signaleringswaarde|correctiewaarde)",
        re.IGNORECASE,
    )
    # Build the full sequence including correctiewaarde as separators
    ref_seq = []
    for entry in aria_entries:
        m = ref_pattern.match(entry.strip())
        if m:
            raw = m.group(1).strip().replace(",", ".")
            label = m.group(2).lower()
            try:
                val = float(raw)
            except ValueError:
                val = None  # "Geen gegevens" etc.
            ref_seq.append((val, label))

    # Split into chart groups at each "correctiewaarde" boundary
    groups: list[list] = []
    current: list = []
    for item in ref_seq:
        if item[1] == "correctiewaarde":
            if current:
                groups.append(current)
                current = []
        else:
            current.append(item)
    if current:
        groups.append(current)

    chart_vars = [
        ("deze school", "scholenopdekaart_pct_fundamenteel"),
        ("signaleringswaarde", "scholenopdekaart_signaleringswaarde_fundamenteel"),
    ]
    streef_vars = [
        ("deze school", "scholenopdekaart_pct_streefniveau"),
        ("signaleringswaarde", "scholenopdekaart_signaleringswaarde_streefniveau"),
    ]
    group_var_lists = [chart_vars, streef_vars]

    for i, group in enumerate(groups[:2]):
        var_list = group_var_lists[i]
        for val, label in group:
            if val is None:
                continue
            for target_label, var_name in var_list:
                if label == target_label:
                    rows.append({"variable": var_name, "value": val})

    # ------------------------------------------------------------------ #
    # School advice distribution                                          #
    # ------------------------------------------------------------------ #
    adv_pattern = re.compile(r"^([^,\"]+),\s*([\d,\.]+)\.\s*deze school", re.IGNORECASE)
    adv_map = {
        "vwo": "scholenopdekaart_schooladvies_vwo_pct",
        "havo": "scholenopdekaart_schooladvies_havo_pct",
        "havo / vwo": "scholenopdekaart_schooladvies_havo_vwo_pct",
        "vmbo-(g)t": "scholenopdekaart_schooladvies_vmbo_t_pct",
        "vmbo-gt": "scholenopdekaart_schooladvies_vmbo_t_pct",
        "vmbo-t": "scholenopdekaart_schooladvies_vmbo_t_pct",
        "vmbo-(g)t / havo": "scholenopdekaart_schooladvies_vmbo_t_havo_pct",
        "vmbo-b / -k": "scholenopdekaart_schooladvies_vmbo_bk_pct",
        "vmbo-b": "scholenopdekaart_schooladvies_vmbo_b_pct",
        "vmbo-k": "scholenopdekaart_schooladvies_vmbo_k_pct",
        "vmbo-k / -(g)t": "scholenopdekaart_schooladvies_vmbo_k_t_pct",
        "praktijkonderwijs": "scholenopdekaart_schooladvies_pro_pct",
        "pro": "scholenopdekaart_schooladvies_pro_pct",
        "vso": "scholenopdekaart_schooladvies_vso_pct",
    }
    for entry in aria_entries:
        m = adv_pattern.match(entry.strip())
        if m:
            cat = m.group(1).strip().lower()
            var_name = adv_map.get(cat)
            if var_name:
                try:
                    val = float(m.group(2).replace(",", "."))
                    rows.append({"variable": var_name, "value": val})
                except ValueError:
                    pass

    return rows


def _parse_leerlingresultaten(html: str) -> list[dict]:
    """Extract all metrics from a SODK resultaten/leerlingresultaten page."""
    return _parse_resultaten_page(html)


def _parse_schooladvies(html: str) -> list[dict]:
    """Extract school advice from a dedicated schooladvies page (if it exists)."""
    return _parse_resultaten_page(html)


def _name_similarity(a: str, b: str) -> float:
    """Simple word-overlap similarity between two school names."""
    import unicodedata
    def normalize(s: str) -> set:
        n = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        words = re.sub(r"[^a-z0-9\s]", "", n.lower()).split()
        # Remove very common Dutch school words for better matching
        stopwords = {"basisschool", "school", "de", "het", "een", "van", "bs", "obs", "ikc", "kc"}
        return {w for w in words if w not in stopwords and len(w) > 2}
    words_a = normalize(a)
    words_b = normalize(b)
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    return len(intersection) / max(len(words_a), len(words_b))


def _find_school_url(page, school_name: str, city: str) -> Optional[str]:
    """Find a school's scholenopdekaart.nl URL via the city schools listing page."""
    city_slug = re.sub(r"[^a-z0-9]+", "-", city.strip().lower()).strip("-")
    city_url = f"{_BASE_URL}basisscholen/{city_slug}/"
    try:
        page.goto(city_url, wait_until="networkidle", timeout=30000)

        # Collect all school links with numeric codes: /basisscholen/{city}/{code}/{slug}/
        all_hrefs = page.eval_on_selector_all(
            "a[href]", "els => els.map(el => el.href)"
        )
        candidates = []
        for href in all_hrefs:
            m = re.match(
                r"(https?://[^/]+/basisscholen/[^/]+/\d+/[^/]+/?)$", href
            )
            if m:
                candidates.append(href.rstrip("/") + "/")

        if not candidates:
            logger.warning("No school links found on city page: %s", city_url)
            return None

        # Score by name similarity, pick the best match
        best_url = None
        best_score = 0.0
        for href in candidates:
            # Extract slug from URL to compare with school name
            slug_m = re.search(r"/\d+/([^/]+)/?$", href)
            slug = slug_m.group(1) if slug_m else ""
            score = _name_similarity(school_name, slug.replace("-", " "))
            if score > best_score:
                best_score = score
                best_url = href

        if best_score < 0.2:
            logger.warning(
                "Low confidence match (%.2f) for %s on SODK", best_score, school_name
            )
            return None

        logger.info("Matched '%s' to %s (score=%.2f)", school_name, best_url, best_score)
        return best_url

    except Exception as exc:
        logger.warning("Failed to find SODK URL for %s: %s", school_name, exc)
    return None


def _scrape_page(page, url: str, timeout: int = 20000) -> Optional[str]:
    """Navigate to URL and return page HTML after JS render."""
    try:
        page.goto(url, wait_until="networkidle", timeout=timeout)
        return page.content()
    except Exception as exc:
        logger.warning("Failed to load %s: %s", url, exc)
        return None


def fetch(school: dict, n_years: int = 7) -> pd.DataFrame:
    """Fetch scholenopdekaart.nl data for a school using Playwright.

    Attempts to extract:
    - Parent satisfaction scores (oudertevredenheid)
    - % fundamenteel/streefniveau (leerlingresultaten)
    - School advice distribution (schooladvies)

    Args:
        school: dict with keys brin, name, city, ...
        n_years: currently unused (SODK shows latest year only per page)

    Returns:
        DataFrame with columns: school_name, year, variable, value
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
        return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

    school_name = school["name"]
    city = school.get("city", "")

    rows: list[dict] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                ),
                locale="nl-NL",
                extra_http_headers={"Accept-Language": "nl-NL,nl;q=0.9"},
            )
            page = context.new_page()

            # Step 1: Find school URL on scholenopdekaart.nl
            base_url = _find_school_url(page, school_name, city)
            if base_url is None:
                logger.warning("Could not find SODK URL for %s (%s)", school_name, city)
                browser.close()
                return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

            logger.info("SODK URL for %s: %s", school_name, base_url)

            def _extract_year(html: str) -> Optional[int]:
                """Extract the most plausible survey/data year from page HTML.

                Looks for recent school years (e.g. '2024-2025' or '2024')
                in contexts that suggest data year, not navigation/copyright.
                """
                import datetime
                current = datetime.date.today().year
                # Prefer schooljaar pattern like "2023-2024" or "2024-2025"
                for m in re.finditer(r"(20\d{2})[-/–](20\d{2})", html):
                    y = int(m.group(2))
                    if current - 5 <= y <= current + 1:
                        return y
                # Fall back to standalone year in a data context
                for m in re.finditer(r"\b(20\d{2})\b", html):
                    y = int(m.group(1))
                    if current - 3 <= y <= current + 1:
                        return y
                return current  # default to current year

            # Step 2: Tevredenheid page
            tev_html = _scrape_page(page, base_url + "tevredenheid/")
            if tev_html:
                tev_rows = _parse_tevredenheid(tev_html)
                year = _extract_year(tev_html)
                for r in tev_rows:
                    r["year"] = year
                rows.extend(tev_rows)
                time.sleep(1)

            # Step 3: Resultaten page (may be /resultaten/ or /leerlingresultaten/)
            res_html = None
            for res_subpage in ("resultaten/", "leerlingresultaten/", "onderwijskwaliteit/"):
                html_candidate = _scrape_page(page, base_url + res_subpage)
                if html_candidate and "niet gevonden" not in html_candidate.lower()[:500]:
                    res_html = html_candidate
                    break
            if res_html:
                res_rows = _parse_leerlingresultaten(res_html)
                year = _extract_year(res_html)
                for r in res_rows:
                    r["year"] = year
                rows.extend(res_rows)
                time.sleep(1)

            browser.close()

    except Exception as exc:
        logger.error("Playwright error for %s: %s", school_name, exc)

    if not rows:
        return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

    df = pd.DataFrame(rows)
    df["school_name"] = school_name
    return df[["school_name", "year", "variable", "value"]]
