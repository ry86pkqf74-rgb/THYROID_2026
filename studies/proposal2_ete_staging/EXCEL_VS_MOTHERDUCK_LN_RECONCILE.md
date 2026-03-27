# Excel vs MotherDuck — lymph node reconciliation

- **Generated (UTC)**: 2026-03-27T06:22:39.061935+00:00
- **Excel**: `raw/All Diagnoses & synoptic 12_1_2025.xlsx` sheet `synoptics + Dx merged`
- **Database**: `md:thyroid_research_2026`
- **Verdict**: **FAIL** (cleaned `tumor_1_ln_examined` / `tumor_1_ln_involved` vs SQL-cleaned `path_synoptics`)

## Counts

| Metric | Value |
|--------|-------|
| Excel rows | 11688 |
| Excel unique (research_id, surgery_date) | 11687 |
| MotherDuck path_synoptics rows | 11688 |
| MD unique (research_id, surgery_date) | 11687 |
| Matched keys | 11687 |
| Unmatched keys (Excel only) | 0 |
| Unmatched keys (MD only) | 0 |
| Discordant cleaned LN (matched keys) | 0 |
| Excel duplicate-key internal ambiguity | 1 |
| MD duplicate-key internal ambiguity | 1 |
| PHI-safe CSV exports (no wide synoptic row) | True |

## Outputs

- Summary JSON: `audit_excel_vs_md_ln/excel_md_ln_summary.json`
- Discordant rows: `audit_excel_vs_md_ln/excel_md_ln_discordant.csv`
- Unmatched Excel keys: `audit_excel_vs_md_ln/excel_md_ln_unmatched_excel.csv`
- Unmatched MD keys: `audit_excel_vs_md_ln/excel_md_ln_unmatched_md.csv`

## Cleaning rule

Cleaned values mirror the specimen audit SQL on varchar: `TRIM`, remove `;`, remove literal `x` placeholders, then `TRY_CAST`/`float` (see `run_motherduck_ln_completeness_audit.py` `_ln_specimen`).

## Method

Rows are matched on `(research_id, surgery_encounter_date)` using the **canonical surgery-date chain** (same as `utils.surg_date_canonical` / `surgery_date_canonical_sql`): native `DATE`, trimmed cast, then `%m/%d/%Y`, `%m/%d/%y`, `%Y-%m-%d` after trim — so leading tabs/spaces and US-style strings align with MotherDuck. MotherDuck pulls include `surg_date_canonical` and `surg_date_parse_tier` for audits. Duplicate keys on a side are collapsed to a single row; if duplicate rows disagree on LN fields, they are listed in `excel_md_ln_ambiguous_keys.csv`.

## PHI safety

Row-level CSVs default to **no PHI** (`research_id`, dates, LN fields, flags only). Use `--wide-phi-export` locally if you need the full synoptic row for investigation.
