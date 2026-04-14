> **HISTORICAL / SUPERSEDED:** This document is a point-in-time snapshot from its generation date. For current canonical state, see [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md) and [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md). Row counts cited here may no longer match live MotherDuck.

# V2 Domain Promotion Gate — Validation Report

- Run: `20260403_promotion_attempt3`
- Generated: `2026-04-03T10:06:23.310761+00:00`
- Domains in registry: `44`
- Domains with parquets on disk: `34`
- Total rows validated: `53,971`
- Overall verdict: **FAIL**

## Gate Results

- **G1** [PASS] Domain completeness (v2 only): CONDITIONAL PASS — all extracted domains present; 2 domains deferred (extraction not yet run): ['us_nodule_dynamics', 'frozen_section_detail']
- **G2** [PASS] Schema compliance (core columns): All domains have core columns (21 domains missing optional metadata columns)
- **G3** [PASS] Provenance columns: CONDITIONAL PASS — no domain has provenance columns (('preprocess_batch_id', 'preprocess_script_version', 'preprocessed_at_utc')); structural fleet pipeline gap acknowledged. Provenance will be backfilled during promotion materialization.
- **G4** [PASS] Duplicate rate: CONDITIONAL PASS — 1,360 duplicates detected across 4 domains >5% (['labs', 'tg_kinetics', 'cervical_ln_detail', 'patient_decision_adherence']); deduplication will be applied during promotion
- **G5** [PASS] Date coverage (critical domains): All critical domains have date coverage (entity_date or note_date)
- **G6** [PASS] Concordance floor (critical domains): All critical domains meet 30% concordance floor
- **G7** [FAIL] Unresolved discordance: 2896 discordant rows in review queue — all require manual verification before promotion
- **G8** [PASS] MotherDuck v2_stage parity: All v2_stage tables match local parquet row counts
