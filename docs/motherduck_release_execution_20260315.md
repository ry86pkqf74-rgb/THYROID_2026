# MotherDuck Release Execution Guide

> **Date:** 2026-03-15  
> **Scope:** Exact command sequences for promotion gates, manifest generation, and readiness interpretation.  
> **Prerequisites:** Active MotherDuck token exported as `MOTHERDUCK_TOKEN` (personal) or `MD_SA_TOKEN` (service-account).

---

## 1. Quick Start (Makefile)

```bash
# Verify token is set
echo ${MOTHERDUCK_TOKEN:+set}${MD_SA_TOKEN:+set}   # should print "set"

# Dry-run DEV → QA promotion gates
make md-promote-dryrun-dev-qa

# Dry-run QA → PROD promotion gates
make md-promote-dryrun-qa-prod

# Generate (or refresh) the release manifest for PROD
make md-release-manifest-prod

# Check manifest freshness without MotherDuck
make md-manifest-status
```

---

## 2. Raw Commands (without Make)

### 2a. Promotion Gate — DEV → QA (dry-run)
```bash
.venv/bin/python scripts/95_environment_promotion.py \
    --from dev --to qa --dry-run
```
**What it checks:** 12 critical tables exist, 8 metric bounds in range, row-multiplication ≤ 3×, null-rate < 5%, 5 hardening tables present, MAP dedup validator (script 94), prod DB reachable.

### 2b. Promotion Gate — QA → PROD (dry-run)
```bash
.venv/bin/python scripts/95_environment_promotion.py \
    --from qa --to prod --dry-run
```
**Additional checks (prod):** RO share (`thyroid_share`) readable, post-promotion share visibility proof.

### 2c. Release Manifest — PROD
```bash
.venv/bin/python scripts/96_release_manifest.py --env prod
```
**Output:** `exports/release_manifests/release_<SHA>_<TIMESTAMP>.json` + `LATEST_MANIFEST.json` symlink.

### 2d. Service-Account Mode
Append `--sa` to any command above to use `MD_SA_TOKEN` instead of `MOTHERDUCK_TOKEN`:
```bash
.venv/bin/python scripts/95_environment_promotion.py \
    --from qa --to prod --dry-run --sa
```

---

## 3. Interpreting Results

### Promotion Gate (script 95) — Exit Codes

| Exit | Meaning |
|------|---------|
| `0`  | All gates PASS — safe to promote (or, in `--dry-run`, would be safe) |
| `1`  | One or more gates FAIL — review log output above |

### Promotion Gate — Per-Gate Outcomes

| Gate | PASS condition | FAIL condition |
|------|---------------|---------------|
| **table_existence** | All 12 critical tables exist | Any missing |
| **metric_bounds** | All 8 metrics within `[low, high]` | Any out of range |
| **row_multiplication** | No table exceeds 3× expected rows | Any multiplication detected |
| **null_check** | `research_id` null rate < 5% across critical tables | Exceeds threshold |
| **hardening_tables** | All 5 hardening tables present and non-empty | Any missing or empty |
| **map_dedup** | Script 94 reports 0 duplicates | Duplicates found |
| **prod_accessible** | Can query `thyroid_research_2026` | Connection fails |
| **share_readable** (prod only) | Can query `thyroid_share` | Connection or 0 rows |

### Release Manifest (script 96) — Status Values

| Status | Meaning | Action |
|--------|---------|--------|
| `RELEASE_READY` | All metrics in bounds, clean git tree, MAP dedup clean | Safe to tag a release |
| `BLOCKED` | One or more metrics out of bounds or dedup failure | Fix flagged metrics then re-run |
| `DIRTY_TREE` | Uncommitted changes present | Commit first, or re-run with `--allow-dirty` |

### Manifest SHA Freshness

The CI and `make md-manifest-status` both compare the manifest's `git_sha` against HEAD.  
If they differ, the manifest is **stale** — re-run `make md-release-manifest-prod` after committing.

---

## 4. Metric Reference

### Script 95 — Promotion Gate Bounds

| Metric | Low | High |
|--------|-----|------|
| `surgical_cohort` | 10,500 | 12,000 |
| `cancer_cohort` | 3,500 | 5,000 |
| `dedup_episodes` | 8,500 | 10,500 |
| `recurrence_patients` | 1,500 | 2,500 |
| `rai_episodes` | 1,500 | 2,200 |
| `motherduck_tables` | 580 | 700 |
| `rai_dose_pct` | 35.0 | 55.0 |
| `tirads_patients` | 3,000 | 4,000 |

### Script 96 — Manifest Metric Bounds (superset)

Includes all 8 above **plus**:

| Metric | Low | High |
|--------|-----|------|
| `lab_rows` | 35,000 | 50,000 |
| `refined_master_v12` | 12,000 | 14,000 |
| `molecular_tested` | 9,000 | 11,000 |

---

## 5. Typical Release Sequence

```
# 1. Ensure clean working tree
git status --short   # should be empty

# 2. Dry-run gates
make md-promote-dryrun-dev-qa
make md-promote-dryrun-qa-prod

# 3. Generate manifest
make md-release-manifest-prod

# 4. Check manifest
make md-manifest-status    # must print "fresh" + "RELEASE_READY"

# 5. Commit manifest + tag
git add exports/release_manifests/
git commit -m "release manifest $(date +%Y%m%d)"
git tag v2026.03.15-release
git push origin main --tags
```

---

## 6. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ERROR: Neither MOTHERDUCK_TOKEN nor MD_SA_TOKEN is set` | No token exported | `export MOTHERDUCK_TOKEN="..."` |
| Gate `table_existence` FAIL | Table dropped or not materialized | Re-run `scripts/26_motherduck_materialize_v2.py --md` |
| Metric `surgical_cohort` out of bounds | Data drift after new ingestion | Verify, then update bounds in scripts 91/95/96 together |
| Manifest `DIRTY_TREE` | Uncommitted edits | Commit or pass `--allow-dirty` |
| Manifest SHA stale | Commits after last manifest | Re-run `make md-release-manifest-prod` |
| CI `WARN: Manifest SHA ≠ HEAD` | Same as above | Non-blocking; re-run manifest before tagging |

---

## 7. File Index

| File | Purpose |
|------|---------|
| `scripts/95_environment_promotion.py` | Promotion gate runner |
| `scripts/96_release_manifest.py` | Release manifest generator |
| `scripts/91_promotion_gate.py` | Lightweight CI promotion gate |
| `scripts/94_map_dedup_validator.py` | MAP dedup integrity checker |
| `Makefile` | Convenience targets |
| `exports/release_manifests/LATEST_MANIFEST.json` | Current manifest pointer |
| `docs/motherduck_promotion_runbook_20260314.md` | Full runbook with rollback |
