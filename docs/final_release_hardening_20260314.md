# Final Release Hardening — 2026-03-14

**Scope:** Release-engineering pass to harden the THYROID_2026 pipeline for
stable production deployment.  All changes are additive and non-breaking.

---

## 1. MATERIALIZATION_MAP Deduplication

### Problem
`scripts/85_materialization_performance_audit.py` reported **6 MAP
duplicates**:

```
md_pathology_recon_review_v2
md_molecular_linkage_review_v2
md_rai_adjudication_review_v2
md_imaging_path_concordance_v2
md_op_path_recon_review_v2
md_lineage_audit_v1
```

### Root cause
`check_map_duplicates()` scanned the **entire script 26 file** using
`re.findall(r'"(md_[^"]+)"', ...)` — it picked up six SQL string-substitution
references in the cross-database materialization block (lines 598–629) that
share names with MAP entries but are used there only as `.replace()` target
literals, not as MAP entries.

### Resolution
- Fixed `check_map_duplicates()` in `scripts/85_materialization_performance_audit.py`
  to use a MAP-scoped parser (exits at the closing `]` of `MATERIALIZATION_MAP`).
- Created `scripts/94_map_dedup_validator.py`: authoritative, CI-safe validator
  with embedded before/after narrative (`--narrative` flag).
- True duplicate count: **0**.  MAP has **220 unique entries**.

---

## 2. Environment Promotion Workflow

### New script: `scripts/95_environment_promotion.py`

Implements a deterministic, gate-checked DEV → QA → PROD promotion workflow.

**Promotion paths allowed:**

| From | To   | Gate level  | Notes                              |
|------|------|-------------|------------------------------------|
| dev  | qa   | full        | Routine iteration                  |
| qa   | prod | full+share  | Production gated; shape + RO share |
| qa   | qa   | full        | Idempotent re-validation           |
| dev  | dev  | smoke       | Self-validation only               |

**Gates run:**
1. All CRITICAL_TABLES exist in source
2. 8 canonical metric bounds
3. No row multiplication (patient/episode dedup)
4. Core columns non-null
5. Hardening tables present
6. MAP dedup (runs script 94)
7. `prod_db_accessible` — prod DB (`thyroid_research_2026`) directly reachable (prod only)
8. `prod_ro_share_accessible` — publication RO share (`thyroid_share` catalog alias) reachable (prod only)

> **micro-hardening 2026-03-14**: former single `ro_share` gate split into gates 7 + 8
> so prod-DB vs share failures surface separately in the promotion manifest.

**Outputs:**
- Promotion manifest JSON in `exports/release_manifests/<promotion_id>.json`
- Promotion log entry in `promotion_log` table of the target database

**Usage:**
```bash
# dry-run from qa to prod
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/95_environment_promotion.py \
    --from qa --to prod --dry-run

# actual promotion with service-account token
MD_SA_TOKEN=... .venv/bin/python scripts/95_environment_promotion.py \
    --from qa --to prod --sa
```

---

## 3. Release Manifest Generator

### New script: `scripts/96_release_manifest.py`

Point-in-time release manifest capturing:

| Field               | Content                                    |
|---------------------|--------------------------------------------|
| `git_sha`           | Full + short commit SHA                    |
| `git_branch`        | Current branch name                        |
| `tagged_version`    | Git tag (if exact-match)                   |
| `generated_at`      | UTC ISO timestamp                          |
| `row_counts`        | Per-table counts for 14 CRITICAL_TABLES    |
| `metrics`           | 11 named metric bounds (in-range flag)     |
| `benchmarks`        | Query latency vs baseline CSV              |
| `gates`             | Pass/fail per gate                         |
| `overall_status`    | RELEASE_READY / BLOCKED / DRY_RUN          |

Output files:
- `exports/release_manifests/release_<sha7>_<timestamp>.json`
- `exports/release_manifests/LATEST_MANIFEST.json` (stable pointer)

**Usage:**
```bash
# Read from prod and write manifest  (default --env prod; no --md flag)
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/96_release_manifest.py

# Target a specific environment
MOTHERDUCK_TOKEN=... .venv/bin/python scripts/96_release_manifest.py --env qa

# Dry-run (print; do not write file)
.venv/bin/python scripts/96_release_manifest.py --dry-run

# Use service-account token
MD_SA_TOKEN=... .venv/bin/python scripts/96_release_manifest.py --sa
```

> **Note:** Script 96 uses `--env {dev|qa|prod}` (default: `prod`).
> It does **not** accept a `--md` flag.

---

## 4. Source-Limited Field Registry

### New file: `exports/final_release_hardening_20260314/source_limited_field_registry.csv`

Canonical CSV registry of all analytically important fields classified by
data availability.  **See `docs/source_limited_field_registry_20260314.md`
for full narrative.**

Fields:

| Status                  | Meaning                                                         |
|-------------------------|-----------------------------------------------------------------|
| `CANONICAL`             | Reliable; use as primary variable                              |
| `SOURCE_LIMITED`        | Structural data gap; use with caveat                           |
| `DERIVED_APPROXIMATE`   | Computed from imperfect inputs; precision limited              |
| `MANUAL_REVIEW_ONLY`    | Not suitable for statistical analysis without manual curation   |

Seed entries include: `esophageal_involvement_flag`, `rln_monitoring_flag`,
`rai_dose_mci`, `tirads_score`, `vascular_invasion_who_grade`,
`braf_positive_final`, `ete_grade_v9`, `age_at_surgery`, and 28 others.

---

## 5. Manuscript Output Hardening

### Modified script: `scripts/90_manuscript_freeze_rebuild.py`

Added prod-sourcing enforcement block before the validation phase:

```python
MANUSCRIPT_SOURCE_ENV = "prod"
PROD_DB_NAME = "thyroid_research_2026"
NON_PROD_DB_NAMES = {"thyroid_research_2026_dev", "thyroid_research_2026_qa"}
```

Behaviour:
- **prod (`thyroid_research_2026`)**: passes; status=VERIFIED_PROD
- **non-prod** without `--allow-nonprod`: exits 1 (BLOCKED)
- **non-prod** with `--allow-nonprod`: passes with WARN status; audit artifact
  written to `exports/manuscript_cohort_freeze/source_audit_<ts>.json`
- **local DuckDB**: passes with NOTE; warns to use `--md` for publication outputs

The source audit artifact is also copied into the bundle ZIP.

New flag: `--allow-nonprod` (for QA testing only; generates permanent warning record).

---

## 6. CI / QA Hardening

### Modified file: `.github/workflows/ci.yml`

Three additions:

#### 6.1 MAP dedup check in `lint-and-syntax` (no token required)
```yaml
- name: MAP dedup validation (no token needed)
  run: python scripts/94_map_dedup_validator.py
```
Fails the lint job if true MAP duplicates are found.  Runs on every push.

#### 6.2 Manuscript source-env verification in `motherduck-ci`
Verifies that `current_database()` returns `thyroid_research_2026` (prod).
Blocks if connected to dev/qa databases (would indicate CI misconfiguration).

#### 6.3 Release manifest freshness check in `motherduck-ci`
Reads `LATEST_MANIFEST.json`; warns (non-blocking) if:
- File does not exist (manual: run `scripts/96_release_manifest.py --md`)
- `overall_status == "BLOCKED"` before a tagged release

---

## Summary of New/Modified Files

| File | Type | Change |
|------|------|--------|
| `scripts/85_materialization_performance_audit.py` | Modified | Fixed MAP-scoped dedup parser |
| `scripts/90_manuscript_freeze_rebuild.py` | Modified | Added prod-sourcing enforcement + `--allow-nonprod` flag |
| `scripts/94_map_dedup_validator.py` | New | Authoritative MAP dedup validator with before/after narrative |
| `scripts/95_environment_promotion.py` | New | DEV→QA→PROD promotion workflow with gate-checks |
| `scripts/96_release_manifest.py` | New | Release manifest generator |
| `exports/.../source_limited_field_registry.csv` | New | 38-row field classification registry |
| `docs/final_release_hardening_20260314.md` | New | This document |
| `docs/motherduck_promotion_runbook_20260314.md` | New | Step-by-step promotion runbook |
| `docs/source_limited_field_registry_20260314.md` | New | Field registry narrative |
| `.github/workflows/ci.yml` | Modified | +3 new CI checks |
