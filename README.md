# Schoolvergelijkingen

This is a repository to fetch public data from the Dienst Uitvoering Onderwijs (DUO) about performance and satisfaction of elementary schools in the Netherlands, and structure quantitative data from various years together.

## Setup

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run playwright install chromium  # first time only
```

## Usage

```bash
uv run python main.py <postcode> [options]
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--schools N` / `-n N` | 7 | Number of nearest schools |
| `--years Y` / `-y Y` | 7 | Number of most recent years |
| `--output FILE` / `-o FILE` | `<postcode>_schools.parquet` | Output path |
| `--skip-sodk` | — | Skip scholenopdekaart.nl (no Playwright needed) |
| `--verbose` / `-v` | — | Enable debug logging |

Examples:

```bash
uv run python main.py 3813
uv run python main.py "3813 AB" --schools 5 --years 5
uv run python main.py 3813 --output results.parquet --skip-sodk
```

## Output variable reference

The Parquet file has columns `school_name`, `year`, `variable`, `value`. Variable names are prefixed by their data source.

### allecijfers.nl (`allecijfers_`)

| Variable | Unit | Description |
|---|---|---|
| `allecijfers_toetsscore_gem` | score | Average doorstroomtoets/eindtoets score for this school. **Scale changed in 2023–2024**: old Cito eindtoets ≈ 75–90; new doorstroomtoets LIB ≈ 175–185. Do not compare values across the break. |
| `allecijfers_toetsscore_gem_gemeente` | score | Average toets score for all schools in the same municipality (same scale caveat as above). |
| `allecijfers_toetsscore_gem_nederland` | score | National average toets score (same scale caveat). |
| `allecijfers_uitstroom_vwo_pct` | % | Share of groep-8 pupils receiving a VWO secondary school advice/placement. |
| `allecijfers_uitstroom_havo_pct` | % | Share receiving HAVO advice/placement. |
| `allecijfers_uitstroom_vmbo_t_pct` | % | Share receiving VMBO-T (theoretische leerweg) advice/placement. |
| `allecijfers_uitstroom_vmbo_bk_pct` | % | Share receiving VMBO-B or VMBO-K (basis/kaderberoepsgerichte leerweg) advice/placement. |
| `allecijfers_uitstroom_spo_pct` | % | Share going to speciaal (basis)onderwijs or praktijkonderwijs. |
| `allecijfers_uitstroom_overig_pct` | % | Share with other/unknown secondary school destination. |
| `allecijfers_aantal_leerlingen` | count | Total number of enrolled pupils at the school in that schooljaar. |
| `allecijfers_pct_zittenblijvers` | % | Percentage of pupils who are held back (blijven zitten) in that schooljaar. |
| `allecijfers_schoolweging` | index | School weighting index (schoolweging) as published by DUO/inspectie: reflects the socioeconomic composition of the pupil population. Lower = more advantaged intake; national average ≈ 29–30. |

### DUO open data (`duo_`)

These variables come from two DUO sources: annual CSV files published at `duo.nl/open_onderwijsdata` (leerlingen) and the CKAN open data portal at `onderwijsdata.duo.nl` (all others). The CKAN datasets cover multiple years per school and are authoritative — they take precedence over allecijfers.nl in deduplication.

#### Leerlingen (bekostigd)

| Variable | Unit | Description |
|---|---|---|
| `duo_leerlingen_bekostigd` | count | Number of government-funded (bekostigde) pupils registered at the school on 1 February of the reference year, as published by DUO. Slightly different from `allecijfers_aantal_leerlingen` because of different reference dates. |

#### CKAN datasets (multi-year, per vestiging)

| Variable | Unit | Description |
|---|---|---|
| `duo_leerlingen_totaal` | count | Total enrolled pupils summed across all leerjaren (groep 1–8) for the schooljaar. |
| `duo_pct_zittenblijvers` | % | Percentage of pupils held back (zittenblijvers) in the schooljaar, calculated as (zittenblijvers / total leerlingen) × 100. |
| `duo_pct_fundamenteel_taal_lv` | % | Percentage of groep-8 pupils achieving the fundamenteel niveau for Taal Leesvaardigheid (1F or 2F). |
| `duo_pct_fundamenteel_taal_tv` | % | Percentage achieving fundamenteel niveau for Taal Taalverzorging. |
| `duo_pct_fundamenteel_reken` | % | Percentage achieving fundamenteel niveau for Rekenen (1F or higher). |
| `duo_pct_streef_taal_lv` | % | Percentage achieving the streefniveau for Taal Leesvaardigheid (2F). |
| `duo_pct_streef_taal_tv` | % | Percentage achieving the streefniveau for Taal Taalverzorging (2F). |
| `duo_pct_streef_reken` | % | Percentage achieving the streefniveau for Rekenen (1S or 2F). |
| `duo_schooladvies_vwo_pct` | % | Share of groep-8 pupils receiving a VWO school advice. |
| `duo_schooladvies_havo_pct` | % | Share receiving HAVO advice. |
| `duo_schooladvies_havo_vwo_pct` | % | Share receiving combined HAVO/VWO advice. |
| `duo_schooladvies_vmbo_gt_pct` | % | Share receiving VMBO-GT (theoretisch/gemengd) advice. |
| `duo_schooladvies_vmbo_gt_havo_pct` | % | Share receiving combined VMBO-GT/HAVO advice. |
| `duo_schooladvies_vmbo_k_gt_pct` | % | Share receiving combined VMBO-K/GT advice. |
| `duo_schooladvies_vmbo_bk_pct` | % | Share receiving VMBO-B or VMBO-K (basis/kaderberoepsgericht) advice. |
| `duo_schooladvies_vmbo_b_pct` | % | Share receiving VMBO-B advice. |
| `duo_schooladvies_vmbo_k_pct` | % | Share receiving VMBO-K advice. |
| `duo_schooladvies_pro_pct` | % | Share receiving Praktijkonderwijs advice. |
| `duo_schooladvies_vso_pct` | % | Share receiving Voortgezet Speciaal Onderwijs advice. |
| `duo_schooladvies_overig_pct` | % | Share with other/unknown school advice. |

### Scholen op de Kaart (`scholenopdekaart_`)

All SODK variables reflect the **most recent year** shown on scholenopdekaart.nl at time of scraping; historical series are not available from this source.

#### Doorstroomtoets referentieniveaus

| Variable | Unit | Description |
|---|---|---|
| `scholenopdekaart_pct_fundamenteel` | % | Percentage of groep-8 pupils who achieved the **fundamenteel niveau** (basic reference level for language and maths) on the doorstroomtoets. The national signaleringswaarde is 85% for every school. |
| `scholenopdekaart_signaleringswaarde_fundamenteel` | % | Inspectie target (signaleringswaarde) for fundamenteel niveau. Fixed at 85% nationally. |
| `scholenopdekaart_pct_streefniveau` | % | Percentage of groep-8 pupils who achieved the **streefniveau** (higher reference level). |
| `scholenopdekaart_signaleringswaarde_streefniveau` | % | School-specific inspectie target for streefniveau, set based on the school's pupil population. |

#### Schooladvies (VO advice distribution)

Percentage of groep-8 pupils who received each type of secondary school advice. Categories are mutually exclusive; combined advice types (e.g. VMBO-T/HAVO) indicate the school gave a combined track recommendation.

| Variable | Unit | Description |
|---|---|---|
| `scholenopdekaart_schooladvies_vwo_pct` | % | VWO (voorbereidend wetenschappelijk onderwijs). |
| `scholenopdekaart_schooladvies_havo_pct` | % | HAVO (hoger algemeen voortgezet onderwijs). |
| `scholenopdekaart_schooladvies_havo_vwo_pct` | % | Combined HAVO/VWO advice. |
| `scholenopdekaart_schooladvies_vmbo_t_pct` | % | VMBO theoretische leerweg (VMBO-T / VMBO-GT). |
| `scholenopdekaart_schooladvies_vmbo_t_havo_pct` | % | Combined VMBO-T/HAVO advice. |
| `scholenopdekaart_schooladvies_vmbo_k_t_pct` | % | Combined VMBO-K/VMBO-T advice. |
| `scholenopdekaart_schooladvies_vmbo_bk_pct` | % | VMBO basis/kader combined. |
| `scholenopdekaart_schooladvies_vmbo_b_pct` | % | VMBO basisberoepsgerichte leerweg. |
| `scholenopdekaart_schooladvies_vmbo_k_pct` | % | VMBO kaderberoepsgerichte leerweg. |
| `scholenopdekaart_schooladvies_pro_pct` | % | Praktijkonderwijs (PrO). |
| `scholenopdekaart_schooladvies_vso_pct` | % | Voortgezet speciaal onderwijs (VSO). |

#### Oudertevredenheid (parent satisfaction)

Extracted from the school's narrative text on the tevredenheid page when a satisfaction survey summary is published.

| Variable | Unit | Description |
|---|---|---|
| `scholenopdekaart_oudertevredenheid_rapportcijfer` | 1–10 | Overall school grade given by parents (rapportcijfer), e.g. 8.0. |
| `scholenopdekaart_oudertevredenheid_gem_score` | 1–4 | Average questionnaire score across all items (typically on a 1–4 scale). |
| `scholenopdekaart_oudertevredenheid_respons_pct` | % | Survey response rate (percentage of families who returned the questionnaire). |
