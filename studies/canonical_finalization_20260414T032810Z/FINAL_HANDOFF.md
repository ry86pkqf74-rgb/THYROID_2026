# Canonical lakehouse finalization — FINAL HANDOFF

**Finalization timestamp:** 2026-04-14T03:30Z  
**Git branch:** `canonical-finalization-20260414T032810Z`  
**MotherDuck connection:** LIVE — fail-closed mode verified

---

## 1. Canonical source of truth

**Live MotherDuck database `Thyroid 2026`**, schemas `main` (analytics) and `qa` (governance). Local `thyroid_master.duckdb` is a developer artifact, not production SSOT. Contract: `docs/final_source_of_truth_contract.md`.

## 2. Exact live tables/views analysts should use

| View | Rows | Use |
|------|-----:|-----|
| `main.master_fact_long_verified_v1` | 55,500 | primary analyst surface |
| `main.master_patient_rollup_verified_v1` | 5,141 | per-patient aggregates |
| `main.master_source_lineage_v1` | 55,500 | provenance chain |
| `main.longitudinal_lab_canonical_v1` | 77,960 | structured labs |
| `main.longitudinal_lab_deduped_v` | 56,198 | deduplicated labs |

## 3. Exact GitHub artifacts analysts should use

| Artifact | Path |
|----------|------|
| Contract doc | `docs/final_source_of_truth_contract.md` |
| Analyst quick-start | `MANUSCRIPT_DATA_START_HERE.md` |
| Live state mirror | `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` (regenerate with `144 --md`) |
| Canonical counts | `exports/full_canonical_release_20260414/canonical_counts.json` |
| LLM extraction audit | `studies/canonical_finalization_20260414T032810Z/artifacts/llm_extraction_lineage_audit.md` |
| Important tables audit | `studies/canonical_finalization_20260414T032810Z/artifacts/important_tables_audit.md` |

## 4. Exact historical artifacts analysts should IGNORE

- `exports/release_manifests/LATEST_MANIFEST.json` — labeled `role: historical_checkpoint` (March 2026)
- `studies/20260407_*/` — superseded by later releases
- `studies/20260408_*/` — superseded (except `studies/20260408_data_contract_gate/`)
- `studies/20260409_final_master_release/` — superseded by `20260411` pack
- Any `EVIDENCE_PACK.md` under `studies/` — point-in-time snapshots, not live truth
- March 2026 local DuckDB freeze artifacts — local baseline only

## 5. Latest live row counts for canonical objects

| Object | Rows |
|--------|-----:|
| `canonical_extracted_fact_long_v2` | 55,500 |
| `canonical_fact_quarantine_v2` | 199 |
| `note_extraction_runs` | 3 |
| `master_fact_long_verified_v1` | 55,500 |
| `master_patient_rollup_verified_v1` | 5,141 |
| `master_source_lineage_v1` | 55,500 |
| `longitudinal_lab_canonical_v1` | 77,960 |
| `longitudinal_lab_deduped_v` | 56,198 |

## 6. Latest live row counts for adjunct linkage/data tables

| Object | Rows |
|--------|-----:|
| `imaging_nodule_master_v1` | 37,016 |
| `fna_episode_master_v2` | 8,119 |
| `v_fna_episode_bethesda_resolved_v1` | 8,119 |
| `v_imaging_nodule_linkage_classification_v1` | 37,016 |
| `specimen_master_v1` | 10,139 |
| `specimen_tumor_focus_v1` | 11,103 |
| `specimen_genomic_assay_v1` | 10,370 |
| `fhir_bundle_specimen_export_v1` | 10,139 |

## 7. Latest `qa.release_manifest` tags

12 total release entries across 9 distinct git SHAs. Latest tag: `20260408r4`. No duplicate tags.

## 8. Checked-in manifest status

`exports/release_manifests/LATEST_MANIFEST.json` is labeled `role: historical_checkpoint` with SHA `8c18892` (March 2026). It is **historical only** — live SSOT is `qa.release_manifest`.

## 9. Stale/duplicate MRQ run_labels

**None.** Single run label `20260407_tier_policy_review_gate` covers all 5,622 MRQ rows. No stale or duplicate snapshots.

## 10. Verified-view parity

**PASS.** canonical facts (55,500) = master facts (55,500) = lineage (55,500). Patient rollup (5,141) = distinct patients from master facts (5,141).

## 11. Reviewer status grain

**Propagated at `(research_id, domain)` grain** — NOT per-fact human review. The `review_grain` column on all three verified views reads `research_id_domain`, with `review_status_source` = `qa.manual_review_queue`. 0 of 5,622 MRQ rows are true human-reviewed; all are `auto_accepted_*` (5,620) or `confirmed_correct` without named reviewer (2).

## 12. LLM extraction output linkage

**All 23 canonical domains are linked and included.** 0 facts have NULL `extraction_run_id`. 3 orphan runs exist in `note_extraction_runs` (no matching facts — benign). Full domain-level audit: `studies/canonical_finalization_20260414T032810Z/artifacts/llm_extraction_lineage_audit.md`.

## 13. Important linkage/data tables assessed

**19 objects audited** across canonical (8), adjunct (8), and governance (3) scopes. All populated and internally consistent. Audit: `studies/canonical_finalization_20260414T032810Z/artifacts/important_tables_audit.md`.

## 14. Remaining gaps

### Technical/data-shape
- **None.** Technical SSOT is clean and internally consistent. 100% lineage, 0 duplicates, perfect parity.

### Governance/human-review
- **0 of 5,622 MRQ rows are true human-reviewed.** All verification statuses are automation-tier (`auto_accepted_standard`, `auto_accepted_critical_sample_ok`, `auto_accepted_informational`) or `confirmed_correct` without named reviewer identity.
- No real human-reviewed CSVs or gate directory inputs were found in the repo.
- Manuscript-grade human validation (named reviewer + timestamp + substantive clinical decision) has not been performed.

### Source-limited coverage
- RAI dose: ~41% coverage ceiling (nuclear medicine reports absent from corpus)
- Recurrence dates: ~88.8% unresolved (no structured recurrence registry)
- Operative NLP: 8 boolean fields at 0% (pipeline architecture / COALESCE guard limitation)
- Non-Tg lab temporal truth: limited to NLP-inferred dates for PTH/calcium
- IHC BRAF (VE1): pathology addendum reports not in `clinical_notes_long`

## 15. One-line manuscript readiness verdict

**Technically ready. Governance not ready.** Technical SSOT is complete (55,500 facts, 100% linked, 0 duplicates, structural validation passes). Manuscript-grade human validation has not been performed — all MRQ rows are automation-tier. If publication policy requires human chart review evidence, that blocker remains open.

---

## Final status

**single SSOT achieved with documented governance blocker**
