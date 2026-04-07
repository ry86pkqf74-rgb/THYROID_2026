# Molecular live activation audit — dev / qa / prod

**Date:** 2026-04-07 (UTC)  
**Purpose:** Prove whether the governed **molecular contract surface** (`main.molecular_results`, `133` contract views, lineage `132` views) is active on MotherDuck, and document blockers.

## Summary verdict

| Environment | Database (per `config/motherduck_environments.yml`) | `molecular_results` | Contract / lineage active? |
|-------------|---------------------------------------------------|--------------------|----------------------------|
| **prod** | `Thyroid 2026` | **0 rows** | **No** — `molecular_results` empty; `molecular_fact_long_v` therefore empty; release checks now **FAIL** (see `119` update). |
| **dev** | `Thyroid 2026 Molecular Dev 20260407` | **0 rows** (table exists) | **No** — same empty governed layer; miniature ingest exists in repo study folder for replay, not applied here. |
| **qa** | `Thyroid 2026 Molecular QA 20260407` | **Table missing** | **No** — introspection errored: `main.molecular_results` does not exist on QA clone. |

## Root cause (prod)

1. **`main.molecular_results` empty** — governed ThyroSeq/Afirma ingests (**41**, **42**) have not been executed on prod with approved inputs (workbooks are PHI and not shipped in-repo).
2. **`main.molecular_testing` missing** — script **22** spine for molecular episodes is absent; `molecular_test_episode_v2` is populated but **9280/10126** rows have **`test_date_native` NULL**, which **blocks script 49** FNA–molecular temporal linkage.
3. **Specimen genomics (`140`)** correctly materializes **10,126** `specimen_genomic_assay_v1` rows, but **~9.9k** land in review because **`NO_FNA_MOLECULAR_LINK`** dominates — downstream of (2), not of fuzzy merging.

## Governance changes (proof / fail-closed)

Release-mode **`119_md_formalization_validate.py`** now:

- **FAILs** when `molecular_results` is empty while `molecular_test_episode_v2` has rows (no more PASS-with-skip).
- **FAILs** when `molecular_testing` is missing while episodes exist (deploy gap).

Latest evidence: `studies/20260407_specimen_genomics_closure/validation_report.md` — **2 FAIL**, **1 WARN** (genomic review burden).

## Operator path to activate (no fuzzy steps)

1. Load **`main.molecular_testing`** (and optional **`genetic_testing`** / **`thyroseq_molecular_enrichment`** per policy) onto the target MotherDuck catalog.
2. Rebuild **`molecular_test_episode_v2`** for molecular: **`scripts/22_canonical_episodes_v2.py --md`** (with appropriate `--md-env` / tokens).
3. Rebuild linkage: **`scripts/49_enhanced_linkage_v3.py --md --md-sa --md-env <env>`** (now supports `--md-sa` / `--md-env`).
4. Rebind genomics: **`scripts/140_md_specimen_genomics_binding.py --md`**
5. Governed assay layer (when inputs available): **`131`** → **`117 --contract-views-only`** → **`41`** / **`42`** → **`132`**.
6. Re-run **`119 --md --release-mode`**.

## Memo — signoff posture

- **Remaining review burden today:** **too large** for policies that assume WARN-level specimen queues are operator-triageable; the dominant bucket is **mechanical** (`NO_FNA_MOLECULAR_LINK`) from **catalog incompleteness**.
- **Live molecular contract:** **not active** until **`molecular_results`** is populated and **`119`** molecular checks pass.
