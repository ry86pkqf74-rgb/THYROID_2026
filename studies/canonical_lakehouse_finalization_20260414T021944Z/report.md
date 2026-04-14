# Canonical Lakehouse Finalization — Execution Report

**Executed:** 2026-04-14 02:19–04:xx UTC  
**Operator:** Agent (Opus 4.6)  
**Branch:** `canonical-lakehouse-finalization-20260414T021944Z`

---

## 1. Phase 0 — Setup

- Created feature branch from `ac8642e` (main HEAD)
- Created working directory at `studies/canonical_lakehouse_finalization_20260414T021944Z/`
- Captured `before_state/git_head.txt`

## 2. Phase 1 — Before-state inventory

Captured:
- `before_state/repo_tree_summary.txt` — repo file structure
- `before_state/current_truth_files.txt` — identified canonical docs and contract
- `before_state/historical_study_files.txt` — located 11 historical study directories
- `before_state/review_input_files.txt` — searched for human review inputs (found none with real decisions)
- `before_state/canonical_candidates.txt` — canonical table/view references in docs
- `before_state/narrative_drift_grep.txt` — checked top-level docs for conflicting truth claims
- `before_state/release_manifests.txt` — identified checked-in and live manifest state

## 3. Phase 2a — Repo scripts (read-only)

| Script | Result |
|--------|--------|
| `144 --md` | SUCCESS — `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` generated |
| `119 --md --release-mode` | 39 PASS / 6 WARN / 2 FAIL (Check 11 missing `review_grain` cols) |
| `125 --md` | SUCCESS — views deployed with `review_grain`, `review_status_source`, `review_join_key` |
| `119 --md --release-mode` (re-run) | 40 PASS / 5 WARN / 0 FAIL |
| `120 --md` | Triage export completed (exports/ timestamped subdir) |

## 4. Phase 2b — Live SQL probes

Executed `sql/run_live_probes.py` with 10 probes:

| # | Probe | Result |
|---|-------|--------|
| 01 | Canonical fact count | 55,500 |
| 02 | Quarantine count | 199 |
| 03 | Distinct fact_id count | 55,500 (0 duplicates) |
| 04 | Lineage completeness | 100% on domain/run_id/method/timestamp/fact_id |
| 05 | Release tag alignment | `20260411` on all facts = latest manifest |
| 06 | MRQ grain analysis | 5,622 rows, 0 true human-reviewed |
| 07 | Promotion decisions | 6 batch-level decisions, all automation |
| 08 | Verified view parity | master=lineage=canonical=55,500; rollup=5,141 |
| 09 | Extraction linkage | 3 runs in note_extraction_runs; all 55,500 facts have run_id |
| 10 | Specimen/FHIR state | spec=10,139, tumor=11,103, genomic=10,370, fhir=10,139 |

## 5. Phase 3 — Canonicalization actions

### 3a. MANUSCRIPT_DATA_START_HERE.md
- Created new file at repo root
- Contents: live SSOT pointer, analyst-facing views, supporting tables, specimen/FHIR adjunct, row count citation rule, reviewer status caveat, manuscript-readiness answer, files to ignore, key scripts

### 3b. docs/final_source_of_truth_contract.md
- Added canonical scope inventory (main, qa, adjunct)
- Added allowed vs disallowed claims table
- Added validation definitions section
- Added analyst quick-start link

### 3c. scripts/144
- Added `note_extraction_runs` to core analytic surface probes
- Verified output includes the new count

### 3d. scripts/119
- Re-evaluated: already had Check 5b (publication governance), Check 11 (view schema), Check 11b (fact parity), duplicate probes, manifest comparison
- No changes required — robust as-is

### 3e. Top-level narrative
- README.md: added MANUSCRIPT_DATA_START_HERE.md pointer, updated date
- truth_sync_summary.md: added analyst quick-start link
- docs/REPO_STATUS.md: added analyst quick-start link
- RELEASE_NOTES.md: added canonical lakehouse finalization entry

### 3f. Historical labeling
- Bannered 21 files across 8 directories with "HISTORICAL / SUPERSEDED" prefix
- Directories: 20260407_tier_final_master_release, 20260408_first_full_release_evidence, 20260408_second_full_release_evidence, 20260409_final_master_release, manuscript_blocker_rebaseline_20260408T*, specimen_fhir_export_20260408_*, specimen_fhir_export_20260413_*, manuscript_human_review_release_20260413T*

### 3g. LLM extraction lineage audit
- `llm_extraction_lineage_audit.csv` — per-fact provenance sampling
- `llm_extraction_domain_coverage.csv` — per-domain breakdown
- `note_extraction_runs_full.csv` — full extraction orchestration log
- `llm_extraction_lineage_audit.md` — narrative summary (100% lineage completeness)

### 3h. Export bundle
- `exports/full_canonical_release_20260408r4/` with 10 artifacts + checksums
- Includes canonical_counts, lineage_completeness, schema_inventory, validation_summary, manifest, contract snapshot

### 3i. Governance attempt
- Searched for human review inputs: found only machine-generated worklist CSVs
- Confirmed 0 true human-reviewed MRQ rows
- Documented in `governance_blocker_dossier.md`

## 6. Phase 4 — Regression tests

- `tests/test_canonical_finalization.py` — 19 pytest tests
- Covers: MANUSCRIPT_DATA_START_HERE.md, contract doc, historical labeling, manifest role, export bundle, top-level doc pointers
- All tests pass locally

## 7. Phase 5 — Final validation

| Run | Result |
|-----|--------|
| `144 --md` | SUCCESS, refreshed CURRENT_MOTHERDUCK_REPO_STATE.md |
| `119 --md --release-mode` | 40 PASS / 5 WARN / 0 FAIL |
| `125 --md` | Views confirmed current |

5 WARNs:
1. note_extraction_runs local parquet absent (expected — remote-only)
2. Duplicate natural keys (documented multi-entity grain)
3. Molecular panel_version pairing (non-blocking)
4. Molecular assay dictionary match (non-blocking)
5. Specimen-adjacent review burden (10,155 open — documented)

## 8. Verdict

**single SSOT achieved with documented governance blocker**

Technical SSOT is complete: parity, lineage, deduplication, export, regression tests all pass. Governance blocker: zero true human-reviewed MRQ rows. This is documented transparently and does not prevent manuscript work — only prevents claiming "human-reviewed" status.
