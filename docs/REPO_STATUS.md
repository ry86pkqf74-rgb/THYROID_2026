# THYROID_2026 — Current Repo Status

**As of:** 2026-03-13 (truth-sync pass)
**Overall verdict:** Manuscript-ready (with scoped caveats) | Approaching dataset-mature | Extraction pipeline complete

---

## What is verified

| Dimension | Status | Evidence |
|-----------|--------|----------|
| Manuscript readiness | **VERIFIED** | Resolved layer populated; 7/7 readiness gates PASS |
| Source/date traceability | **MOSTLY VERIFIED** | `provenance_enriched_events_v1` + `lineage_audit_v1` deployed; 0 error-severity issues |
| Extraction pipeline | **COMPLETE** | 13 phases, 11 engine versions, data quality 98/100 |
| Manuscript metrics | **VERIFIED** | 11 metrics, 0 cross-source mismatches |
| Database hardening | **VERIFIED** | 0 critical blocking, 0 row multiplication, 0 identity failures |
| Complication refinement | **VERIFIED** | 7 entities refined (3.3% raw NLP precision → confirmed/probable tiers) |
| Scoring systems | **VERIFIED** | AJCC8, ATA, MACIS, AGES, AMES all calculable for eligible patients |

## What remains to backfill

These are propagation gaps, not extraction gaps. Some were resolved during
the maturation/hardening pass; others remain as documented limitations.

| Gap | Status | Detail |
|-----|--------|--------|
| Operative NLP enrichment | **OPEN** — pipeline architecture gap | Extractor exists; COALESCE guards prevent UPDATE; 8 fields at 0% |
| RAI dose | **PARTIALLY CLOSED** — 41% coverage | 371/1,857 episodes via `scripts/76_canonical_gap_closure.py` |
| RAS flag | **CLOSED** — 325 episodes backfilled | Via `extracted_ras_patient_summary_v1` |
| Linkage IDs | **CLOSED** — 6 tables propagated | Via `scripts/76_canonical_gap_closure.py` Phase D |
| Imaging nodule master | **CLOSED** — 19,891 rows | `imaging_nodule_master_v1` populated via `scripts/75_dataset_maturation.py` |
| Recurrence dates | **OPEN** — structural sparsity | 1,764 unresolved; prioritized review queue deployed |

Items that **cannot** be resolved without new institutional data:
- Structured PTH/calcium/TSH lab table
- Nuclear medicine report text (0 notes in corpus)
- IHC BRAF (VE1) pathology addendums (not in `clinical_notes_long`)

## Audit document index (March 13)

| Document | Path |
|----------|------|
| Final verification report | [`docs/final_repo_verification_20260313.md`](final_repo_verification_20260313.md) |
| Database hardening audit | [`docs/database_hardening_audit_20260313.md`](database_hardening_audit_20260313.md) |
| Manuscript metric reconciliation | [`docs/manuscript_metric_reconciliation_20260313.md`](manuscript_metric_reconciliation_20260313.md) |
| Freeze alignment report | [`docs/manuscript_freeze_alignment_20260313.md`](manuscript_freeze_alignment_20260313.md) |
| Canonical backfill report | [`docs/canonical_backfill_report_20260313.md`](canonical_backfill_report_20260313.md) |
| Provenance date audit | [`docs/provenance_date_audit_20260313.md`](provenance_date_audit_20260313.md) |
| Operative NLP propagation | [`docs/operative_nlp_motherduck_propagation_20260313.md`](operative_nlp_motherduck_propagation_20260313.md) |
| Operative-path linkage audit | [`docs/operative_note_path_linkage_audit_20260313.md`](operative_note_path_linkage_audit_20260313.md) |
| H&P / discharge note audit | [`docs/hp_discharge_note_audit_20260313.md`](hp_discharge_note_audit_20260313.md) |
| Imaging nodule materialization | [`docs/imaging_nodule_materialization_20260313.md`](imaging_nodule_materialization_20260313.md) |

## Export bundles

| Bundle | Path | Contents |
|--------|------|----------|
| Final publication bundle | `exports/FINAL_PUBLICATION_BUNDLE_20260313/` | 62 files: Tables 1–3, Figures 1–5, cohort CSVs, readiness JSON, Phase 13 report |
| Hardening audit results | `exports/hardening_audit_20260313_0751/` | `check_results.json`, provenance coverage/gaps CSVs |
| Manuscript reconciliation | `exports/manuscript_reconciliation_20260313_0708/` | Metric definitions, SQL registry, review queues, patient cohort |

## Key tables on MotherDuck

| Table | Rows | Role |
|-------|------|------|
| `patient_analysis_resolved_v1` | 10,871 | Primary patient table (one per patient) |
| `episode_analysis_resolved_v1_dedup` | 9,368 | One per surgery episode (deduped) |
| `lesion_analysis_resolved_v1` | 11,851 | One per tumor/lesion |
| `manuscript_cohort_v1` | 10,871 | Frozen manuscript cohort (139 columns) |
| `patient_refined_master_clinical_v12` | 12,886 | FINAL master clinical (136 columns) |
| `thyroid_scoring_py_v1` | 10,871 | AJCC8/ATA/MACIS/AGES/AMES |
| `complication_phenotype_v1` | 5,928 | Phenotyped complication events |
| `longitudinal_lab_clean_v1` | 38,699 | Deduplicated lab timeline |
| `recurrence_event_clean_v1` | 1,946 | Source-linked recurrence events |
| `analysis_cancer_cohort_v1` | 4,136 | Analysis-eligible cancer subset |
