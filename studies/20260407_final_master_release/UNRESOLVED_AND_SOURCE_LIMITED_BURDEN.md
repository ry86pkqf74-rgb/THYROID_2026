# Unresolved and source-limited burden

## Manual review queue (`qa.manual_review_queue`)

After reconciling the RC sign-off hydrate bundle:

| Status | Rows | Meaning |
|--------|------:|---------|
| `SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF` | 5,620 | Automation / governance placeholder — **not** a claim of clinical manuscript sign-off (see playbook) |
| `confirmed_correct` | 2 | Human-confirmed discordance resolutions |

**Pending null `verification_status`:** **0** at strict release validation (required for `--release-mode`).

## Operational correction (governance)

An orchestrator re-run had hydrated a gate folder whose `run_label` resolved to the directory name `promotion_gate`, producing thousands of unsigned rows. Those rows were **removed**, and the queue was re-hydrated from `studies/20260409_final_master_release/mrq_hydrate_gate/` (signed-off CSVs). Duplicate `gate` vs `mrq_hydrate_gate` buckets were deduplicated to a **single** `mrq_hydrate_gate` cohort (5,622 rows).

## Patient rollup — review coverage metric

From `main.master_patient_rollup_verified_v1`:

- **Patients:** 5,574  
- **Mean `pct_reviewed`:** ~14.2%  
- **Patients with `pct_reviewed` &lt; 100%:** 5,331  

**Interpretation:** This metric reflects **overlap between the manual review queue keys and promoted facts**, not clinical “missing data” in the EHR. Most facts will not appear on the discordance queue by design. Statements about “percent reviewed” in manuscripts must use this field’s definition (per rollup DDL), not colloquial “chart review complete.”

## Source-limited missingness

- **Quarantine:** `canonical_fact_quarantine_v2` holds **199** rows (excluded from primary long table by design).
- **Lab QC:** See `processed/tg_lab_ingestion_qc_v1.json` — combo disambiguation, numeric parse rate, unmatched research_id tail (8 IDs listed in QC JSON).
- **Structured labs** do not supply narrative note text; **LLM note-derived labs** remain in canonical facts under note-entity domains — do not double-count without a reconciliation plan.

## Uncertainty language (manuscript-safe)

When reviewer status is null, report **provenance-complete but not discordance-adjudicated** at the fact level. When status is synthetic automation-only, **do not** characterize as attending physician verification. Use `release_20260407_final2` tables for frozen numbers tied to this release tag.
