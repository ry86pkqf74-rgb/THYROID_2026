# Institutional chemistry wave — `final_institutional_20260407`

## BRANCH CONTEXT

Recorded from operator workstation at bundle creation time: `main` (THYROID_2026 repo), push after commit as usual.

## What ran

1. **Input CSV** — No separate analyst flat file was present under `exports/incoming/`. The deliverable was **assembled from live MotherDuck** (`assemble_institutional_lab_csv_from_md.py`):

   - `main.extracted_postop_labs_expanded_v1` — PTH / total calcium (rows with resolved calendar `lab_date`, year 1980–2035).
   - `main.canonical_extracted_fact_long_v2` — `fact_domain = 'labs'`, `present_or_negated = 'present'`, mapped entity types TSH / PTH / calcium / vitamin D with parsable `entity_date` (same year window).

   The CSV is **not** a direct institutional HL7/order feed; it **is** release-grade structured + promoted NLP, with explicit `source_table` and join tokens in `provenance_note`.

2. **Append** — `scripts/127_analyst_institutional_lab_append.py --md --md-sa` with  
   `--ingestion-wave final_institutional_20260407` and  
   `MOTHERDUCK_CUSTOM_USER_AGENT=THYROID_2026_institutional_lab_append/1.0`,  
   `MOTHERDUCK_SESSION_HINT=institutional_lab_append_<UTC>`.

3. **Dedup** — `main.longitudinal_lab_deduped_v` recreated in the same transaction (script 127).

4. **Strict validator** — `scripts/119_md_formalization_validate.py --md --md-sa --release-mode` → **26 PASS / 1 WARN / 0 FAIL** (WARN: specimen-adjacent review burden). Report: `119_release_validation/validation_report.md`.

5. **Final-master orchestrator (126)** — **Not rerun.** Live `qa.manual_review_queue` is **fully non-pending** but dominated by **automation statuses** (`auto_accepted_*`), not a manuscript human-adjudication package. Treat **119 PASS+WARN** as automation health only; do not conflate with signed manuscript release.

## Before / after (live MotherDuck)

**Before** (per `studies/20260407_publication_signoff_live/lab_coverage_memo.md` and spot checks): only `wave_tg*` / Tg-family chemistry in `main.longitudinal_lab_canonical_v1`; ILIKE checks for TSH / PTH / calcium / vitamin D returned **0 rows**.

**After** — `main.longitudinal_lab_canonical_v1` includes `final_institutional_20260407` (**989 rows**, **629 patients**) with analyte mix:

| lab_name_standardized | rows (canonical wave) |
|-----------------------|----------------------:|
| tsh                   |                   515 |
| pth                   |                   200 |
| calcium               |                   188 |
| vitamin_d             |                    86 |

`main.longitudinal_lab_deduped_v` (post-refresh), approximate panel visibility:

| Panel (ILIKE / name check) | rows | patients |
|----------------------------|-----:|---------:|
| TSH                        |  514 |      413 |
| PTH                        |  200 |      184 |
| calcium                    |  188 |      166 |
| vitamin D                  |   86 |       82 |
| Tg axis (broad)            | 55,210 | 3,258 |

## Provenance

- **Stable identity:** `source_lineage_key` (unique in CSV; script 127 fails closed on duplicates / blanks).
- **Join back to facts:** `provenance_note` includes `join_keys|table=canonical_extracted_fact_long_v2|note_row_id=…|extraction_run_id=…|entity_type=…` for NLP rows, or `…|table=extracted_postop_labs_expanded_v1|research_id=…|lab_type=…` for post-op structured rows.
- **799 / 799** NLP-derived wave rows match at least one `canonical_extracted_fact_long_v2` row via those tokens (`_verify_md_coverage.py`).
- **Units:** Live `longitudinal_lab_canonical_v1` had legacy `INTEGER` typing on `unit_raw` / related columns; script **127** stashes textual units into `provenance_note` when the remote catalog requires it, avoiding silent truncation.

## Artifacts

| File | Purpose |
|------|---------|
| `assemble_institutional_lab_csv_from_md.py` | Build `exports/incoming/final_institutional_chemistry_20260407.csv` from MotherDuck |
| `exports/incoming/final_institutional_chemistry_20260407.csv` | Input to 127 |
| `lab_wave_qc_by_ingestion.json` | Row + patient counts by `ingestion_wave` after append |
| `_verify_md_coverage.py` | Read-only post-flight checks |
| `119_release_validation/validation_report.md` | Timestamped `119 --release-mode` output |

## Acceptance mapping

| Criterion | Status |
|-----------|--------|
| `final_institutional_20260407` present | Yes |
| TSH / PTH / calcium / vitamin D in canonical + dedup spine | Yes |
| Dedup view refreshed | Yes |
| Deterministic / provenance-safe append | Yes — transactional replace-by-wave + lineage checks |
| No overclaim on MRQ | Yes — automation-complete queue; **126** not promoted as manuscript sign-off |
