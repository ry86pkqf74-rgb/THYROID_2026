> **HISTORICAL / SUPERSEDED:** This document is a point-in-time snapshot from its generation date. For current canonical state, see [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md) and [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md). Row counts cited here may no longer match live MotherDuck.

# V2 Domain Promotion Gate — Validation Report

- Run: `formalization_20260406`
- Generated: `2026-04-06T23:31:53.129371+00:00`
- Domains in registry: `44`
- Domains with parquets on disk: `36`
- Total rows validated: `54,346`
- Overall verdict: **FAIL**

## Gate Results

- **G1** [PASS] Domain completeness (v2 only): All v2 canonical-output domains have parquets
- **G2** [PASS] Schema compliance (core columns): All domains have core columns (23 domains missing optional metadata columns)
- **G3** [FAIL] Provenance columns: FAIL — no domain has provenance columns (('preprocess_batch_id', 'preprocess_script_version', 'preprocessed_at_utc')); fleet extraction must emit extracted_at and llm_model for traceability
- **G4** [PASS] Duplicate rate: CONDITIONAL PASS — 1,353 duplicates detected across 4 domains >5% (['labs', 'tg_kinetics', 'cervical_ln_detail', 'patient_decision_adherence']); deduplication will be applied during promotion
- **G5** [PASS] Date coverage (critical domains): All critical domains have date coverage (entity_date or note_date)
- **G6** [FAIL] Concordance floor (critical domains): Critical domains below 30% concordance: ['staging=21.7%']
- **G7** [PASS] Unresolved discordance: No same-domain discordance; 2 cross-domain discordant rows waived (v2 domain-specific extraction vs v1 keyword-matched comparison domain)
- **G8** [FAIL] MotherDuck v2_stage parity: Parity failures: ['v2_stage.note_entities_llm_us_nodule_dynamics', 'v2_stage.note_entities_llm_frozen_section_detail']
