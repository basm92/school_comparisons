# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Dutch elementary school data pipeline. Given a postal code, it finds the X nearest schools (default: 7) and fetches performance/satisfaction data for the Y most recent years (default: 7), outputting a long-format Parquet file: `{school_name, year, variable, value}`.

**Primary data sources:**
- [allecijfers.nl](https://allecijfers.nl) — test scores, group 8 secondary school flow ("Uitstroom van groep 8"), up to "Gemiddelde regionale schoolwegingen"
- [scholenopdekaart.nl](https://scholenopdekaart.nl) — fundamenteel niveau %, streefniveau %, school advice distribution, parent satisfaction (oudertevredenheid)
- DUO (Dienst Uitvoering Onderwijs) — raw public data on school performance

**Key requirement:** Data must span multiple years, not just the most recent year.

## Data Variables to Extract

From allecijfers.nl per school per year:
- Average test scores (doorstroomtoets/Cito)
- Uitstroom groep 8 naar middelbare school (secondary school flow percentages)
- Everything up to "Gemiddelde regionale schoolwegingen"

From scholenopdekaart.nl per school per year:
- % fundamenteel niveau (target: ≥85% per inspectie signaleringswaarde)
- % streefniveau (school-specific signaleringswaarde)
- School advice distribution (schooladvies distribution for VO)
- Parent satisfaction scores (oudertevredenheid) — numerical ratings where available

## Output Format

Parquet file with columns: `school_name`, `year`, `variable`, `value`

This is a long/tidy format — one row per (school, year, variable) combination.
