# Before-state audit report — canonical finalization

**Audit timestamp:** 2026-04-14T03:30:55Z  
**Git HEAD:** 133f0a8b51f1819d9b9408b86ea27e38d6742a6b  
**Branch:** canonical-finalization-20260414T032810Z  
**MotherDuck connection:** SUCCESS (fail-closed mode)

---

## 1. What is the one canonical live source of truth right now?

**MotherDuck database `Thyroid 2026`, schemas `main` (analytics) and `qa` (governance).** This is already documented in `docs/final_source_of_truth_contract.md` and consistent across README, `truth_sync_summary.md`, `docs/REPO_STATUS.md`, and `MANUSCRIPT_DATA_START_HERE.md`.

## 2. Exact live tables/views analysts should use

| Object | Type | Rows | Status |
|--------|------|------|--------|
| `main.master_fact_long_verified_v1` | VIEW | 55,500 | canonical analyst surface |
| `main.master_patient_rollup_verified_v1` | VIEW | 5,141 | canonical analyst surface |
| `main.master_source_lineage_v1` | VIEW | 55,500 | canonical analyst surface |
| `main.canonical_extracted_fact_long_v2` | TABLE | 55,500 | upstream promoted facts |
| `main.canonical_fact_quarantine_v2` | TABLE | 199 | quarantined facts |
| `main.longitudinal_lab_canonical_v1` | TABLE | 77,960 | structured labs |
| `main.longitudinal_lab_deduped_v` | VIEW | 56,198 | deduplicated labs |

## 3. Parity confirmation

| Metric | Value | Status |
|--------|-------|--------|
| canonical facts | 55,500 | — |
| master verified facts | 55,500 | **MATCH** |
| source lineage rows | 55,500 | **MATCH** |
| distinct patients (master) | 5,141 | — |
| patient rollup rows | 5,141 | **MATCH** |
| fact:lineage ratio | 1:1 | **PASS** |

## 4. Lineage completeness

| Metric | Value |
|--------|-------|
| facts with source_object_id | 55,500 / 55,500 (100%) |
| facts with source_domain | 55,500 / 55,500 (100%) |
| facts with extraction_run_id | 55,500 / 55,500 (100%) |
| lineage with source_object_id | 55,500 / 55,500 (100%) |
| lineage with source_domain | 55,500 / 55,500 (100%) |

## 5. Duplicate audits

| Audit | Groups found | Status |
|-------|-------------|--------|
| Duplicate fact_id | 0 | **PASS** |
| Duplicate natural key | 200 | **EXPECTED** (multi-analyte rows; documented in contract) |
| Duplicate lineage | 200 | **EXPECTED** (mirrors natural key pattern) |

## 6. Review / governance state

| Metric | Value |
|--------|-------|
| MRQ total rows | 5,622 |
| MRQ pending (NULL status) | 0 |
| auto_accepted_standard | 3,081 |
| auto_accepted_critical_sample_ok | 1,646 |
| auto_accepted_informational | 893 |
| confirmed_correct | 2 |
| **true_human_reviewed** | **0** |
| Run labels | 1 (`20260407_tier_policy_review_gate`) |
| Stale/duplicate run labels | **0** (clean) |

**Review grain:** `research_id_domain` — propagated from MRQ at (research_id, domain) grain, not per-fact. The `review_grain`, `review_status_source`, and `review_join_key` columns exist on verified views and make this explicit.

## 7. Extraction run inventory

| Metric | Value |
|--------|-------|
| note_extraction_runs rows | 3 |
| Facts with NULL extraction_run_id | 0 |
| Orphan runs (no matching facts) | 3 |
| Build versions contributing to facts | 23 distinct extraction_run_ids in canonical facts |

The 3 orphan runs appear to be orchestration log entries that did not produce canonical output (possibly failed, retried, or superseded). This is benign.

## 8. Imaging / FNA linkage

| Surface | Rows | Status |
|---------|------|--------|
| `v_imaging_nodule_linkage_classification_v1` | 37,016 | populated |
| `v_fna_episode_bethesda_resolved_v1` | 8,119 | populated |
| FNA with NULL bethesda | 23 | unscorable |

Imaging linkage classifications: 6,359 primary-linked pairs; 8,395 patients with no dated FNA episode; 8,107 FNA beyond 90d window. These are expected given the surgical cohort composition.

## 9. Specimen / FHIR

| Surface | Rows | Status |
|---------|------|--------|
| `specimen_master_v1` | 10,139 | populated |
| `specimen_tumor_focus_v1` | 11,103 | populated |
| `specimen_genomic_assay_v1` | 10,370 | populated |
| `fhir_bundle_specimen_export_v1` | 10,139 | populated |
| Broken FHIR refs | 0 | **PASS** |
| Review burden rows | 1 | summary row |
| Genomic link review queue | 10,155 | review surface |

## 10. Release ledger

| Tag | SHA | Created |
|-----|-----|---------|
| 20260408r4 | d9b9dc9 | latest |
| 12 total release entries | 9 distinct SHAs | — |
| Duplicate tags | 0 | **PASS** |

## 11. Reconciliation (checked-in vs live)

**0 drift detected.** All 12 checked-in counts in `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` match live MotherDuck exactly.

## 12. Stale/historical artifacts in git

- `exports/release_manifests/LATEST_MANIFEST.json` — labeled `role: historical_checkpoint`, SHA `8c18892` (March 2026); live SSOT is `qa.release_manifest`
- `studies/20260407_*/`, `studies/20260408_*/`, `studies/20260409_*/` — historical evidence packs
- `studies/20260411_final_master_release/EVIDENCE_PACK.md` — most recent but still point-in-time
- March 2026 local manuscript freeze artifacts — historical baseline

## 13. Can the repo truthfully claim "100% validated" today?

**No.** Technical structural validation is complete (0 FAIL on `119 --release-mode`, 100% lineage, 0 duplicates, parity confirmed). However:

- **0 of 5,622 MRQ rows** are true human-reviewed (all are `auto_accepted_*` or `confirmed_correct` without named reviewer)
- Governance-grade manuscript validation requires named reviewer identity + timestamp on MRQ rows
- This is explicitly documented in the contract under "Allowed vs disallowed claims"

## 14. Exact blockers remaining

### Technical/data-shape
- None. Technical SSOT is clean and internally consistent.

### Governance/human-review
- **0 true human-reviewed MRQ rows** — manuscript-grade human validation not performed
- No real reviewed CSVs or gate directories with human reviewer inputs found in repo

### Source-limited coverage
- RAI dose: ~41% coverage ceiling (nuclear medicine reports absent)
- Recurrence dates: 88.8% unresolved (structural registry absent)
- Operative NLP: 8 fields at 0% (pipeline architecture gap)
- Non-Tg lab temporal truth: limited to NLP-inferred dates
