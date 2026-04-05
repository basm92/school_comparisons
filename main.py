#!/usr/bin/env python3
"""Dutch elementary school data pipeline.

Usage:
    python main.py <postcode> [options]

Examples:
    python main.py 3813
    python main.py "3813 AB" --schools 5 --years 5
    python main.py 3813 --output results.parquet --skip-sodk
    python main.py 3813 -v
"""

import argparse
import logging
import sys

from schools.pipeline import run


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch performance data for the nearest Dutch elementary schools.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "postcode",
        help="Dutch postal code (e.g. 3813 or '3813 AB')",
    )
    parser.add_argument(
        "--schools",
        "-n",
        type=int,
        default=7,
        metavar="N",
        help="Number of nearest schools to include (default: 7)",
    )
    parser.add_argument(
        "--years",
        "-y",
        type=int,
        default=7,
        metavar="Y",
        help="Number of most recent years to fetch (default: 7)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        metavar="FILE",
        help="Output Parquet file path (default: <postcode>_schools.parquet)",
    )
    parser.add_argument(
        "--skip-sodk",
        action="store_true",
        help="Skip scholenopdekaart.nl scraping (no Playwright needed)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    output = args.output or f"{args.postcode.replace(' ', '_')}_schools.parquet"

    df = run(
        postcode=args.postcode,
        n_schools=args.schools,
        n_years=args.years,
        output_path=output,
        skip_sodk=args.skip_sodk,
    )

    print(f"\nDone. {len(df)} rows written to: {output}")
    if len(df) > 0:
        print(f"Schools: {df['school_name'].nunique()}")
        print(f"Years: {sorted(df['year'].unique())}")
        print(f"Variables ({df['variable'].nunique()} total):")
        for v in sorted(df["variable"].unique()):
            print(f"  {v}")


if __name__ == "__main__":
    main()
