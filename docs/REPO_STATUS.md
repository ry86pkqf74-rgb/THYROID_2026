# THYROID_2026 — Current Repo Status

> **Historical / narrative note:** This file mixes **April 2026** cloud posture with **March 13** freeze-era tables. **Canonical contract** for live vs historical: [`final_source_of_truth_contract.md`](final_source_of_truth_contract.md). **Short headline** (keep in sync with README and [`truth_sync_summary.md`](../truth_sync_summary.md)): **Live MotherDuck `main` / `qa` are canonical; `119 --release-mode` can pass while governance (human-reviewed MRQ where policy requires) remains a separate concern.**

**As of:** 2026-04-14 (SSOT canonicalization); prior MotherDuck reruns remain cited in linked studies.  
**MotherDuck vs repo introspection:** [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../studies/CURRENT_MOTHERDUCK_REPO_STATE.md) — regenerate with `python scripts/144_md_repo_current_state_summary.py --md` (token via `motherduck_client.get_token()` / `motherduck.local.toml` / env per [`motherduck_client.py`](../motherduck_client.py)).

---

## Top-level posture (exactly one formulation)

| Layer | Meaning | Evidence |
|-------|---------|----------|
| **Live catalog SSOT** | MotherDuck **`main`** + **`qa`**; release ledger **`qa.release_manifest`**; analyst views e.g. **`main.master_fact_long_verified_v1`** | [`final_source_of_truth_contract.md`](final_source_of_truth_contract.md) |
| **Automation / validation** | Fresh `119 --release-mode` — PASS / WARN / FAIL per run; cite **timestamped** report under `studies/` | Example lineage audit: [`studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md) |
| **Governance / human review** | Manuscript sign-off **not** implied by automation alone; policy may require human-reviewed MRQ / promotion | [`studies/20260407_publication_signoff_live/final_verdict_memo.md`](../studies/20260407_publication_signoff_live/final_verdict_memo.md) |
| **Source-limited enrichment backlog** | Operative NLP materialization, recurrence date sparsity, RAI dose ceiling, **residual** non-Tg lab edge cases — **not** “missing institutional lab wave” | [`studies/20260411_final_master_release/EVIDENCE_PACK.md`](../studies/20260411_final_master_release/EVIDENCE_PACK.md); March freeze docs below |

**Not current:** “Blocked by synthetic MRQ **and** missing final lab wave” — the **`final_institutional_20260407`** ingest **closed** the wave-level blocker; cite **20260411** evidence for row counts and lab posture (counts may still **lag** live prod).

**Not claimed:** “Final human-reviewed manuscript signoff complete.”

---

## What was verified (local DuckDB / manuscript freeze — March 13 baseline)

> **Historical snapshot.** The table below is the **2026-03-13** truth-sync baseline (local `thyroid_master` / freeze package). It remains valid for **that artifact**; **MotherDuck cloud** promotion, MRQ histograms, and `119` checks evolved in **April 2026** — use the sections above + live audit folders for cloud narrative.

| Dimension | Status | Evidence |
|-----------|--------|----------|
| Manuscript readiness | **VERIFIED** (freeze-era) | Resolved layer populated; 7/7 readiness gates PASS at freeze |
| Source/date traceability | **MOSTLY VERIFIED** | `provenance_enriched_events_v1` + `lineage_audit_v1` deployed; 0 error-severity issues |
| Extraction pipeline | **COMPLETE** | 13 phases, 11 engine versions, data quality 98/100 |
| Manuscript metrics | **VERIFIED** | 11 metrics, 0 cross-source mismatches at freeze |
| Database hardening | **VERIFIED** | 0 critical blocking, 0 row multiplication, 0 identity failures |
| Complication refinement | **VERIFIED** | 7 entities refined (3.3% raw NLP precision → confirmed/probable tiers) |
| Scoring systems | **VERIFIED** | AJCC8, ATA, MACIS, AGES, AMES all calculable for eligible patients |

---

## What remains to backfill (freeze-era index; cloud may differ)

| Gap | Status | Detail |
|-----|--------|--------|
| Operative NLP enrichment | **OPEN** — pipeline architecture gap | Extractor exists; COALESCE guards prevent UPDATE; 8 fields at 0% |
| RAI dose | **PARTIALLY CLOSED** — ~41% coverage ceiling | Source-limited without NM feeds |
| RAS flag | **CLOSED** — 325 episodes backfilled | Via `extracted_ras_patient_summary_v1` |
| Linkage IDs | **CLOSED** — 6 tables propagated | Via `scripts/76_canonical_gap_closure.py` Phase D |
| Imaging nodule master | **CLOSED** — 19,891 rows | `imaging_nodule_master_v1` populated via `scripts/75_dataset_maturation.py` |
| Recurrence dates | **OPEN** — structural sparsity | 1,764 unresolved; prioritized review queue deployed |

Items that **cannot** be resolved without new institutional data (still true as **classes** of limitation):

- Nuclear medicine report text (0 notes in corpus — RAI dose ceiling)
- IHC BRAF (VE1) addendums not in `clinical_notes_long`
- Fine-grained non-Tg lab **temporal** truth where structured collection dates are absent

---

## Audit document index

### April 2026 — MotherDuck / publication gate

| Document | Path |
|----------|------|
| Live lineage + `119` (27-check) | [`studies/20260407_live_truth_and_lineage_contract_audit/`](../studies/20260407_live_truth_and_lineage_contract_audit/) |
| Publication signoff memos | [`studies/20260407_publication_signoff_live/`](../studies/20260407_publication_signoff_live/) |
| Final master evidence (20260411 tag) | [`studies/20260411_final_master_release/EVIDENCE_PACK.md`](../studies/20260411_final_master_release/EVIDENCE_PACK.md) |
| Historical master snapshot (20260409) | [`studies/20260409_final_master_release/EVIDENCE_PACK.md`](../studies/20260409_final_master_release/EVIDENCE_PACK.md) — **superseded** for row counts by 20260411 |
| Specimen/FHIR contract | [`docs/specimen_fhir_contract_review.md`](specimen_fhir_contract_review.md) |

### March 13, 2026 — local freeze / verification

| Document | Path |
|----------|------|
| Final verification report | [`docs/final_repo_verification_20260313.md`](final_repo_verification_20260313.md) |
| Database hardening audit | [`docs/database_hardening_audit_20260313.md`](database_hardening_audit_20260313.md) |
| Manuscript metric reconciliation | [`docs/manuscript_metric_reconciliation_20260313.md`](manuscript_metric_reconciliation_20260313.md) |
| Freeze alignment report | [`docs/manuscript_freeze_alignment_20260313.md`](manuscript_freeze_alignment_20260313.md) |
| Canonical backfill report | [`docs/canonical_backfill_report_20260313.md`](canonical_backfill_report_20260313.md) |
| Provenance date audit | [`docs/provenance_date_audit_20260313.md`](provenance_date_audit_20260313.md) |
| Operative NLP propagation | [`docs/operative_nlp_propagation_audit_20260315.md`](operative_nlp_propagation_audit_20260315.md) |
| Operative-path linkage audit | [`docs/operative_note_path_linkage_audit_20260313.md`](operative_note_path_linkage_audit_20260313.md) |
| H&P / discharge note audit | [`docs/hp_discharge_note_audit_20260313.md`](hp_discharge_note_audit_20260313.md) |
| Imaging nodule materialization | [`docs/imaging_nodule_materialization_20260313.md`](imaging_nodule_materialization_20260313.md) |

---

## Export bundles

| Bundle | Path | Contents |
|--------|------|----------|
| Final publication bundle | `exports/FINAL_PUBLICATION_BUNDLE_20260313/` | 62 files: Tables 1–3, Figures 1–5, cohort CSVs, readiness JSON, Phase 13 report |
| Hardening audit results | `exports/hardening_audit_20260313_0751/` | `check_results.json`, provenance coverage/gaps CSVs |
| Manuscript reconciliation | `exports/manuscript_reconciliation_20260313_0708/` | Metric definitions, SQL registry, review queues, patient cohort |

---

## Key tables on local DuckDB (freeze-era snapshot)

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
