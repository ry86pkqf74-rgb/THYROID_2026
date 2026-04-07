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

## Local tooling

```bash
.venv/bin/python -m py_compile scripts/49_enhanced_linkage_v3.py scripts/119_md_formalization_validate.py
.venv/bin/python -m pyflakes scripts/49_enhanced_linkage_v3.py scripts/119_md_formalization_validate.py
```
