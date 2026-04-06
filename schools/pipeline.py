"""Orchestrate data fetching from all sources and produce a Parquet output."""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from . import allecijfers, discovery, duo, duo_datasets, scholenopdekaart

logger = logging.getLogger(__name__)


def run(
    postcode: str,
    n_schools: int = 7,
    n_years: int = 7,
    output_path: Optional[str] = None,
    skip_sodk: bool = False,
) -> pd.DataFrame:
    """Run the full pipeline.

    Args:
        postcode: Dutch postal code (e.g. '3813' or '3813 AB')
        n_schools: number of nearest schools to include
        n_years: number of most recent years to retain per variable
        output_path: if provided, write Parquet to this path
        skip_sodk: if True, skip scholenopdekaart.nl (faster, skips SODK)

    Returns:
        Long-format DataFrame: school_name, year, variable, value
    """
    logger.info("Finding %d nearest schools for postcode %s", n_schools, postcode)
    schools = discovery.find_nearest(postcode, n=n_schools)
    logger.info("Found %d schools", len(schools))

    frames: list[pd.DataFrame] = []

    for school in schools:
        name = school["name"]
        logger.info("Fetching data for: %s", name)

        # DUO CKAN datasets — schooladvies, referentieniveaus, zittenblijvers, leerlingen
        # Listed first so dedup keeps authoritative DUO data over scraped fallbacks.
        try:
            df_ckan = duo_datasets.fetch(school, n_years)
            logger.info("  DUO CKAN: %d rows", len(df_ckan))
            frames.append(df_ckan)
        except Exception as exc:
            logger.error("  DUO CKAN error for %s: %s", name, exc)

        # allecijfers.nl — test scores (with gemeente/NL benchmarks), schoolweging
        try:
            df_ac = allecijfers.fetch(school, n_years)
            logger.info("  allecijfers: %d rows", len(df_ac))
            frames.append(df_ac)
        except Exception as exc:
            logger.error("  allecijfers error for %s: %s", name, exc)

        # DUO open data — bekostigde leerlingen count
        try:
            df_duo = duo.fetch(school, n_years)
            logger.info("  DUO leerlingen: %d rows", len(df_duo))
            frames.append(df_duo)
        except Exception as exc:
            logger.error("  DUO leerlingen error for %s: %s", name, exc)

        # scholenopdekaart.nl — parent satisfaction, fundamenteel/streefniveau
        if not skip_sodk:
            try:
                df_sodk = scholenopdekaart.fetch(school, n_years)
                logger.info("  SODK: %d rows", len(df_sodk))
                frames.append(df_sodk)
            except Exception as exc:
                logger.error("  SODK error for %s: %s", name, exc)

    # Filter out empty DataFrames before concat to avoid pandas FutureWarning
    frames = [f for f in frames if len(f) > 0]
    if not frames:
        logger.warning("No data collected")
        return pd.DataFrame(columns=["school_name", "year", "variable", "value"])

    df = pd.concat(frames, ignore_index=True)

    # Deduplicate (same school_name + year + variable → keep first occurrence)
    df = df.drop_duplicates(subset=["school_name", "year", "variable"], keep="first")

    # Ensure correct dtypes
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["year", "value"])
    df["year"] = df["year"].astype(int)

    # Filter to n_years most recent years across the whole dataset
    if len(df) > 0:
        max_year = df["year"].max()
        df = df[df["year"] >= max_year - n_years + 1]

    df = df.sort_values(["school_name", "year", "variable"]).reset_index(drop=True)

    logger.info("Total rows in output: %d", len(df))

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info("Written to %s", output_path)

    return df
