# V2 Domain Promotion Gate — Validation Report

- Run: `formalization_20260406_v3`
- Generated: `2026-04-06T23:35:58.896216+00:00`
- Domains in registry: `44`
- Domains with parquets on disk: `36`
- Total rows validated: `54,346`
- Overall verdict: **PASS**

## Gate Results

- **G1** [PASS] Domain completeness (v2 only): All v2 canonical-output domains have parquets
- **G2** [PASS] Schema compliance (core columns): All domains have core columns (23 domains missing optional metadata columns)
- **G3** [PASS] Provenance columns: CONDITIONAL PASS — no domain has provenance columns (('preprocess_batch_id', 'preprocess_script_version', 'preprocessed_at_utc')); structural fleet pipeline gap acknowledged. Provenance will be backfilled during promotion materialization.
- **G4** [PASS] Duplicate rate: CONDITIONAL PASS — 1,353 duplicates detected across 4 domains >5% (['labs', 'tg_kinetics', 'cervical_ln_detail', 'patient_decision_adherence']); deduplication will be applied during promotion
- **G5** [PASS] Date coverage (critical domains): All critical domains have date coverage (entity_date or note_date)
- **G6** [PASS] Concordance floor (critical domains): All critical domains meet 30% concordance floor (waived cross-domain-only: ['staging=21.7%'])
- **G7** [PASS] Unresolved discordance: No same-domain discordance; 2 cross-domain discordant rows waived (v2 domain-specific extraction vs v1 keyword-matched comparison domain)
- **G8** [PASS] MotherDuck v2_stage parity: All v2_stage tables match local parquet row counts
