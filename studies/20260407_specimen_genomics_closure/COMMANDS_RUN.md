# Exact commands run — specimen genomics closure / live molecular audit

**UTC context:** 2026-04-07  
**MotherDuck attribution (each session):**

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_specimen_genomics_closure/1.0"
export MOTHERDUCK_SESSION_HINT="specimen_genomics_closure_<UTC_YYYYMMDD_HHMM>"
```

Token resolution: `MD_SA_TOKEN` preferred via `--md-sa` where supported (never printed).

## Live introspection (prod — `Thyroid 2026`)

Row-count and diagnostics via embedded Python using `utils.md_connect.connect_md_or_file` (see transcript; counts: `molecular_results=0`, `molecular_test_episode_v2=10126`, review queue `9966`, `fna_molecular_linkage_v3` only **2** distinct `molecular_episode_id`, **9280** null `test_date_native` on episodes).

## Linkage rebuild

```bash
cd "/Users/ros/THyroid 2026/THYROID_2026"
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_specimen_genomics_closure/1.0"
export MOTHERDUCK_SESSION_HINT="specimen_genomics_closure_20260407_1225"
.venv/bin/python scripts/49_enhanced_linkage_v3.py --md --md-sa --md-env prod
```

Outcome: `fna_molecular_linkage_v3` still **838** rows — temporal join unchanged because almost all molecular episodes lack `test_date_native` (upstream table `main.molecular_testing` absent on this catalog).

## Release validation (fail-closed evidence)

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_specimen_genomics_closure/1.0"
export MOTHERDUCK_SESSION_HINT="specimen_genomics_closure_20260407_1235"
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-sa --release-mode \
  --output-dir studies/20260407_specimen_genomics_closure
```

Result: **2 FAIL** (molecular governed ingest empty; molecular_testing missing), **1 WARN** (specimen genomic link review burden), exit code **1**. Report: `validation_report.md` in this folder.

## Materialize `main.molecular_testing` from cohort workbook (prod)

Loads long-format **`main.molecular_testing`** from `raw/THYROSEQ_AFIRMA_12_5.xlsx`, then re-derives **`molecular_test_episode_v2`** via **`MOLECULAR_TEST_EPISODE_V2_SQL`** (same logic as `scripts/22_canonical_episodes_v2.py`). Does **not** run `register_parquets`.

```bash
cd "/Users/ros/THyroid 2026/THYROID_2026"
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_specimen_genomics_closure/1.0"
export MOTHERDUCK_SESSION_HINT="specimen_genomics_closure_20260407_moltest"
.venv/bin/python scripts/145_md_materialize_molecular_testing.py --md --md-sa --md-env prod \
  --input raw/THYROSEQ_AFIRMA_12_5.xlsx --grain patient --max-slot 1
```

Follow-up (same catalog):

```bash
.venv/bin/python scripts/49_enhanced_linkage_v3.py --md --md-sa --md-env prod
.venv/bin/python scripts/140_md_specimen_genomics_binding.py --md --md-sa --md-env prod --skip-snapshot
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-sa --release-mode \
  --output-dir studies/20260407_specimen_genomics_closure
```

**Outcome (approx.):** `molecular_testing` ≈ 10.9k rows; `molecular_test_episode_v2` ≈ 10.9k; many `test_date_native` still NULL where source `DATE_*` cells are placeholders (e.g. `x`). Release check **12b** → **PASS**.

## Governed `molecular_results` from cohort workbook (prod)

**131** + **117** ensure DDL + molecular contract views exist. **41** with **`--input-profile cohort_thyroseq_afirma_12_5`** maps wide slot columns (`MUTATION_*`, `RESULT_*`, `DATE_*`, etc.) into the ThyroSeq-complete field names the parser expects, then writes **`main.molecular_results`** / **`molecular_variant_long`** (source_table **`41_thyroseq_excel_workbook`**).

```bash
cd "/Users/ros/THyroid 2026/THYROID_2026"
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_molecular_governed_ingest/1.0"
export MOTHERDUCK_SESSION_HINT="molecular_41_cohort_20260407"
.venv/bin/python scripts/131_molecular_results_layer.py --execute --md --md-sa --md-env prod
.venv/bin/python scripts/117_md_contract_views.py --md --md-env prod --contract-views-only
.venv/bin/python scripts/41_ingest_thyroseq_excel.py --input raw/THYROSEQ_AFIRMA_12_5.xlsx \
  --input-profile cohort_thyroseq_afirma_12_5 --md --md-sa --md-env prod
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-sa --release-mode \
  --output-dir studies/20260407_specimen_genomics_closure
```

**Outcome (2026-04-07):** ~**10,862** `molecular_results` rows; **119** release-mode **0 FAIL** (check 12 **PASS**). **`42`** (Afirma) not run — no Afirma structured export in `raw/` for this session. Residual **WARN**: specimen genomic link review backlog; molecular dictionary/panel_version warnings on ThyroSeq assay metadata.

Vendor **`Thyroseq Data Complete.xlsx`** layout remains supported as **`--input-profile thyroseq_complete`** (default).

## Local tooling

```bash
.venv/bin/python -m py_compile scripts/49_enhanced_linkage_v3.py scripts/119_md_formalization_validate.py
.venv/bin/python -m pyflakes scripts/49_enhanced_linkage_v3.py scripts/119_md_formalization_validate.py
```
