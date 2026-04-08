# Specimen / FHIR reference integrity + genomics QA hardening

**Generated (UTC):** 2026-04-08  
**Git SHA:** not embedded (avoids amend drift); after checkout run `git log -1 --format=%H` on `main` — subject line contains `specimen-fhir`.

## Historical failure categories (`broken_fhir_refs=10,139`, 2026-04-07 early run)

Per `studies/20260407_specimen_fhir_blocker_remediation/remediation_report.md` and `studies/20260407_publication_signoff_live/final_verdict_memo.md`:

| Category | Mechanism |
|----------|-----------|
| `encounter_episode` (dominant) | Diagnostic joined `fhir_episode_of_care_v1` via `(patient_fhir_id, surgery_episode_id)` style rollup that did **not** match the **full** `EpisodeOfCare/{id}` string embedded in encounter `resource_json`, so **every** encounter row looked broken. |
| `procedure_subject` / `encounter_subject` / `specimen_*` | Additional branches surface Patient-reference normalization issues (stripping repeated `Patient/` prefixes). |

**After remediation (142 SQL fix + redeploy):** breakdown by `issue` was **empty**; `broken_fhir_refs=0`.

## Live verification (this session, prod catalog)

- `SELECT issue, COUNT(*) FROM qa.v_diag_specimen_fhir_broken_refs_v1 GROUP BY 1` returned **no rows** (0 broken refs).
- Root cause was **validator / diagnostic drift**, not missing upstream FHIR rows, once 138 tail and episode table were aligned.

## Changes in this commit

| Area | Change |
|------|--------|
| MotherDuck UA / session hint | Default **`specimen_fhir_ref_integrity_v2`** via `utils/md_pipeline_attribution.py` (`SPECIMEN_FHIR_RELEASE_TRUTH_*` constants). Env overrides unchanged. |
| Bundle / reconstruction JOIN | `scripts/sql/138_specimen_fhir_tail_ddl.sql` and `scripts/141_fhir_specimen_json_export.py`: `fhir_episode_of_care_v1` join adds **`fe.patient_fhir_id = fo.patient_fhir_id`** (defense in depth vs short-key collision). |
| QA diagnostics | `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`: `encounter_episode` resolves episode by **`fo.fhir_id = JSON reference`** only; new branch **`encounter_episode_patient_mismatch`** if id exists but patients differ. |
| Genomics validation | `scripts/140_md_specimen_genomics_binding.py` `run_validation`: replaces trivial ThyroSeq check with `high_tier_null_specimen_guard`, `specimen_focus_fk_when_populated`, `thyroseq_explode_ordinality_dense`, `thyroseq_fusion_array_parity` / `thyroseq_allele_array_parity` (when source table exists), `thyroseq_payload_fingerprint_unique_per_slice`. |
| Docs | `docs/specimen_fhir_contract_review.md` UA table updated. |
| Tests | `tests/test_specimen_fhir_qa_diagnostics.py`: missing episode row + patient mismatch; `tests/test_specimen_genomics_binding.py`: ordinality, fusion parity, focus FK regression tests. |

## Before / after (meaningful metrics)

| Metric | Before (early 2026-04-07) | After (142 fix + live) | After this PR (expect) |
|--------|---------------------------|--------------------------|-------------------------|
| `broken_fhir_refs` | 10,139 | 0 | 0 |
| Genomics `val_specimen_genomic_binding_v1` | Trivial `thyroseq` row | — | Strict parity / FK / ordinality gates |

## Operator actions

1. Redeploy QA views after pull: `scripts/138_md_specimen_fhir_layer.py --md` (full) or `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md` (142 only).
2. Re-materialize FHIR tail if bundle join semantics must match prod: `138 --md` (includes 138 tail).
3. Re-run `scripts/119_md_formalization_validate.py --md --release-mode`.
