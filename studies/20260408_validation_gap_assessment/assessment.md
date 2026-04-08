# Validation & governance gap assessment (2026-04-08)

**Scope:** Compare existing repository validation and governance **surfaces** (CI, scheduled jobs, operator workflows, scripts, and selected tests) against a desired **generic safety net** across four pillars:

1. **Schema drift** — promoted artifacts stay aligned with declared contracts (tables, columns, enums).
2. **Impossible values / ranges** — numeric and categorical invariants that should never ship.
3. **Linkage-prep inconsistencies** — cross-domain keys, chronology, and episode-aware joins before downstream analytics.
4. **Append-only auditability** — durable trails for loads, promotions, extractions, and releases (who/what/when; reproducible fingerprints).

**Constraints honored in this memo:** No raw clinical note text, no fuzzy-matching recommendations, deterministic language only. This document is **evidence from code and workflow inspection**, not a live database audit.

---

## Executive summary

The repository already operates a **layered** governance model: **offline** pytest contracts (gold parsers, provenance, multimodal linkage logic), **CI** gates on `main` (static typing, MotherDuck smoke queries, coarse row-count bands, uniqueness checks, formalization path `116 → 112 → 119`), and **manual/scheduled** MotherDuck pipelines (episodes `22–29`, optional multimodal `129–128`, daily optimize). That stack is **strong for promotion-time and LLM-domain** safety and **moderate** for manuscript-layer regression.

**Gaps vs a *single* generic safety net** are mainly **coverage wiring** and **contract centralization**, not absence of validators:

- **`scripts/29_validation_engine.py`** rebuilds many `val_*` tables and is the richest **linkage/chronology/provenance/lab-plausibility** surface, but it is **not** invoked on every `push`/`pull_request` in `.github/workflows/ci.yml` (it runs in the **manual** `motherduck_episode_pipeline` workflow after scripts 22–25). Separately, **`tests/test_lab_canonical.py`** and **`tests/test_linkage_confidence.py`** exist but are **not** in either CI pytest job list.

- **Schema drift** beyond the formalization checks is **partially** covered: CI asserts **existence** of a fixed table list and **subset** null rules; `119_md_formalization_validate.py` performs broader structural/release checks; `112` G2 asserts **per-domain parquet core columns**. There is **no** single CI step that diffs `information_schema` for all `main` manuscript tables against a frozen artifact on every PR.

- **Impossible values** are covered **domain-by-domain** (e.g., `val_lab_canonical_v1` in script 29; molecular bounds in 119; CI band checks on headline counts) but **not** as one catalog-driven suite in CI.

- **Append-only auditability** is **design-level** strong (`docs/motherduck_database_contract_v1.md`: `v2_stage`, `qa`, `release_*`, `load_inventory`, `note_extraction_runs`; `llm_extraction/run_telemetry.py` for local parquet run logs). **Enforcement** that every promotion path appends scorecard/queue rows is **implicit** in scripts, not asserted by a dedicated CI read-only audit in the default branch workflow.

**Verdict:** For the stated four pillars, **most building blocks exist**. The thinnest next step is **CI wiring and doc alignment** (wire existing offline tests; optionally add a **read-only** `29 --md` summary job or artifact on a schedule), **before** writing new validation engines.

---

## Pillar-by-pillar view

### 1) Schema drift

**Covered today**

- **Promotion / formalization:** `scripts/112_v2_domain_promotion_gate.py` G1–G3 (domain inventory, **core column** compliance, provenance presence rules); CI runs `116_md_stage_loader.py --md --dry-run` and `112 … --motherduck-check` and `119_md_formalization_validate.py --md` in `motherduck-formalization` (after `lint-and-syntax`).
- **MotherDuck contract doc:** `docs/motherduck_database_contract_v1.md` names schemas, staging vs `main`, and required provenance columns (normative for humans and operators).
- **CI `lint-and-syntax`:** “Check canonical tables exist” — **existence-only** smoke `SELECT 1 FROM {tbl}`.

**Gaps**

- No **automated** “all columns in contract §X match live `information_schema`” on **every** PR (119 approaches this for formalized release checks but is gated behind MD job + not the full manuscript-wide column registry).
- **`scripts/29_validation_engine.py`** assumes V2 episode tables exist; it does not replace a global schema contract test.

### 2) Impossible values / ranges

**Covered today**

- **CI:** “Canonical metric reproducibility” — **band checks** on headline SQL metrics (population sanity, not clinical physiological bounds).
- **`VAL_LAB_CANONICAL_SQL`** inside `scripts/29_validation_engine.py` — TG/PTH/Ca bounds, future-date counts, invalid `data_completeness_tier`.
- **`scripts/119_md_formalization_validate.py`** — e.g. molecular contract views: **allele_fraction bounds**, enum-like `variant_class`, checksum uniqueness (release-oriented).
- **`tests/test_lab_canonical.py`** — local/optional pytest over `longitudinal_lab_canonical_v1` (schema, tiers, date statuses, plausibility bounds) **if** `thyroid_master.duckdb` is present.

**Gaps**

- **Lab canonical pytest not in CI** — matrix row documents this.
- **No single “range registry”** driving both SQL validators and tests (duplicated bounds definitions risk drift between `tests/test_lab_canonical.py` and `VAL_LAB_CANONICAL_SQL`).

### 3) Linkage-prep inconsistencies

**Covered today**

- **`scripts/29_validation_engine.py`:** `val_chronology_anomalies`, `val_unlinked_linkable`, `val_missing_derivable`, adjudication confirmation `val_*`, refinement audits, provenance traceability, fact provenance.
- **Manual workflow** `motherduck_episode_pipeline.yml`: runs `22–25` then **`29_validation_engine.py --md`** (materializes validators on MotherDuck when operators run it).
- **CI offline:** `tests/test_imaging_fna_linkage_mm_v1.py` (script 129 behavior), `tests/test_multimodal_contract_mm_v1.py`, release gate tests; optional **`multimodal-md-contract-gate`** for strict `129→128` on MD (manual dispatch).
- **`tests/test_linkage_confidence.py`:** in-memory tier logic for imaging↔FNA and pathology↔RAI patterns.

**Gaps**

- **Default CI path does not run script 29** — linkage/chronology `val_*` are **operator/episode-workflow** or ad-hoc unless replicated elsewhere.
- **`test_linkage_confidence.py` not listed in CI pytest** — tier regression relies on local runs.

### 4) Append-only auditability

**Covered today**

- **Contract:** `migrate` narrative for `v2_stage` → promotion → `main`; `qa` promotion artifacts (`promotion_scorecard`, `manual_review_queue`, `release_manifest`, etc.); `load_inventory`; `note_extraction_runs` (§2–3 of contract doc).
- **`llm_extraction/run_telemetry.py`:** `append_note_extraction_run` appends to `processed/note_extraction_runs.parquet` with `run_id`, git commit, registry digest, counters — **local/processed audit**, MotherDuck sync via materialization path (see contract).
- **`scripts/120_review_queue_triage.py`:** **read-only** exports for `qa.manual_review_queue` with **PHI-conscious** truncation (no `review_reason` in worklists).

**Gaps**

- **CI does not verify** `note_extraction_runs` × `canonical_extracted_fact_long_v2` completeness (119 has related release-mode checks; not the same as a lightweight “non-null `extraction_run_id` fraction” on every push).
- **Append-only** is **policy**; physical enforcement (DB ACLs) is **outside** this repo’s CI.

---

## Files inspected (this pass)

- `AGENTS.md` (policy context)
- `.github/workflows/ci.yml`, `motherduck_episode_pipeline.yml`, `motherduck_optimize.yml`
- `scripts/112_v2_domain_promotion_gate.py`, `scripts/29_validation_engine.py`, `scripts/119_md_formalization_validate.py`, `scripts/120_review_queue_triage.py`
- `llm_extraction/run_telemetry.py`
- `docs/motherduck_database_contract_v1.md`
- `tests/test_lab_canonical.py`, `tests/test_linkage_confidence.py`, `tests/test_imaging_fna_linkage_mm_v1.py`, `tests/test_fact_provenance_contract.py`

---

## Related security / ops notes (non-goal but recorded)

- MotherDuck tokens belong in **gitignored** `motherduck.local.toml` (see `motherduck.local.toml.example`); CI uses secrets `MD_SA_TOKEN` / `MOTHERDUCK_TOKEN`. This assessment did **not** run live MotherDuck queries.
