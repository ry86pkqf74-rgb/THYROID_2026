-- mig_264 — Bethesda-2 × malignant cohort probes (read-only reference)
-- Decision pass: no DDL/DML. Execute via scripts/mig_264_bethesda2_false_neg_audit.py (MotherDuck connect_locked).
-- Cohort: canonical_patient_master.bethesda_final = 2 AND COALESCE(is_malignant,FALSE)=TRUE

-- Verification counts (live 2026-05-01): n_bethesda2_malig = 385; n_bethesda2_all = 2033

/*
§2a–§2f SQL bodies are maintained in scripts/mig_264_bethesda2_false_neg_audit.py
(schema-aware: canonical_fna_events_v1.bethesda_final_num, fna_date_resolved,
 safe VARCHAR research_id joins).

Outputs:
  scripts/output/mig_264_bethesda2_audit_*.md
  scripts/output/mig_264_disposition_table.csv

Carry-forward: CF-mig264-BETHESDA-LINKAGE-MISMAP — §2b showed linkage_source = surgery for all 385;
 multi-nodule / stale-Bethesda vs events remain primary mechanical hypotheses until Logan disposition.
*/
