# Cursor Prompt — mig_186 NIFTP + uncertain-malignancy exclusion sweep

**Date:** 2026-04-30
**Lane:** mig_186 / niftp_uncertain_exclusion_scoping
**Batch (proposed):** `mig_186_niftp_uncertain_exclusion_20260430`
**Trigger:** Logan flagged rid 12188 (NIFTP) during R1 size CSV review. NIFTP was reclassified as non-malignant by WHO 2017; rows for these patients should not be in `canonical_path_malignant_events_v1`.
**Posture:** **READ-ONLY scoping + skeleton SQL.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only)

---

## Mission

Identify all NIFTP and uncertain-malignancy entries on `canonical_path_malignant_events_v1`, classify them, and propose a Logan-ratifiable exclusion/reclassification rule.

**Live MD probe (Cowork 2026-04-30):**
- 213 events / 195 patients with NIFTP in `canonical_path_malignant_events_v1` (primary_histology='NIFTP' OR histology_variant='NIFTP' OR primary_histology ILIKE '%NIFTP%')
- 7 events with "uncertain" or "hurthle neoplasm of uncertain malignant potential"
- These shouldn't be on a "malignant events" table per WHO 2017 NIFTP reclassification

---

## Required scope

### §1 Full inventory + classification

```sql
SELECT
  primary_histology,
  histology_variant,
  COUNT(*) AS n_events,
  COUNT(DISTINCT research_id) AS n_pts,
  COUNT(*) FILTER (WHERE t_stage_ajcc8 IS NOT NULL) AS n_with_t_stage,
  COUNT(*) FILTER (WHERE n_stage_ajcc8 IS NOT NULL) AS n_with_n_stage
FROM main.canonical_path_malignant_events_v1
WHERE primary_histology ILIKE '%NIFTP%'
   OR histology_variant ILIKE '%NIFTP%'
   OR primary_histology ILIKE '%uncertain%'
   OR primary_histology ILIKE '%hurthle%neoplasm%'
   OR primary_histology ILIKE '%FT-UMP%'
   OR primary_histology ILIKE '%WDT-UMP%'
GROUP BY 1, 2 ORDER BY 3 DESC;
```

### §2 Cross-table impact: are NIFTP patients also in other malignant canonicals?

Check `canonical_invasion_events_v1`, `canonical_us_lymph_node_patient_rollup_v2`, `canonical_patient_master.histologic_types_all`, etc. — does the NIFTP misclassification cascade into other tables?

### §3 Cohort impact

How many of the 195 NIFTP patients are also flagged as malignant elsewhere (e.g., have a separate malignant tumor, or PM `is_malignant=TRUE`)? Surface:
- NIFTP-only patients (truly should be excluded from malignant cohort)
- Mixed: NIFTP + other malignant tumor (NIFTP entry should be excluded but patient kept)
- Edge cases: only NIFTP histology found but flagged malignant by other criteria

### §4 Propose disposition rules

| Rule | Approach |
|---|---|
| R-A | DELETE NIFTP rows from `canonical_path_malignant_events_v1` (clean exclusion) |
| R-B | Move NIFTP rows to a new `canonical_path_indeterminate_events_v1` table (preserve provenance) |
| R-C | Add `is_malignant_per_who_2017` BOOLEAN flag on existing rows; filter at query time |
| R-D | Hybrid: delete from canonical_path_malignant_events_v1, snapshot to archive, register exclusion in CF |

Recommend R-D (cleanest for manuscript, preserves audit trail). Logan ratifies.

### §5 Author placeholder skeleton apply SQL

`qc_framework_v1/migrations/186_niftp_uncertain_exclusion_TBD_20260430.sql` (placeholder; not for execution):
- §A pre-snapshot of all 220 (213 + 7) affected rows to archive
- §B DELETE per chosen rule (R-A/R-B/R-C/R-D — Logan ratifies)
- §C cascade impact: rebuild downstream canonicals if needed
- §D registry note appendix on affected tier-2 cols + open `CF-mig186-WHO-2017-NIFTP-RECLASS`

### §6 Audit/report

`qc_framework_v1/reports/mig_186_niftp_uncertain_exclusion_scoping_20260430.md`:
- §1 full histology inventory (213 NIFTP + 7 uncertain breakdown)
- §2 cross-table cascade analysis
- §3 cohort impact (NIFTP-only vs mixed)
- §4 R-A/R-B/R-C/R-D rule comparison + recommendation
- §5 manuscript implications (NIFTP cohort needs separate analysis or exclusion)
- §6 sample 10 rids for Logan spot-check (CSV at `exports/mig186_niftp_scoping_20260430/niftp_uncertain_inventory.csv`)

---

## Governance reminders

- Read-only investigation only. Author = `Logan Glosser <logan.glosser@gmail.com>`.

---

## Deliverables

1. `qc_framework_v1/migrations/186_niftp_uncertain_exclusion_TBD_20260430.sql`
2. `qc_framework_v1/reports/mig_186_niftp_uncertain_exclusion_scoping_20260430.md`
3. `exports/mig186_niftp_scoping_20260430/niftp_uncertain_inventory.csv`

Commit message: `qc: mig_186 NIFTP + uncertain-malignancy exclusion scoping (213+7 events; pending Logan rule ratification)`

---

End of prompt.
