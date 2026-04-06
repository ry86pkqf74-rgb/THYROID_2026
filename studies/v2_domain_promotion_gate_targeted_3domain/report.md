# V2 Domain Promotion Gate — Validation Report

- Run: `targeted_3domain`
- Generated: `2026-04-03T14:21:20.630744+00:00`
- Domains in registry: `45`
- Domains with parquets on disk: `37`
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
- **G8** [PASS] MotherDuck v2_stage parity: Skipped (--motherduck-check not set or MOTHERDUCK_TOKEN missing)
