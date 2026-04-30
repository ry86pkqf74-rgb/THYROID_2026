# Cursor Prompt — mig_177c LVI+VI derivative col reclean scoping (2,502 + 2,580 flippers)

**Date:** 2026-04-29 (very late evening)
**Lane:** mig_177c / lvi_vi_derivatives_reclean_scoping
**Batch (proposed):** `mig_177c_lvi_vi_derivatives_reclean_20260429`
**Predecessor:** mig_177b (CLOSED at `dd45a59` — re-derived `lvi_any_present_path` + `vi_any_present_path` + 2 rollup snapshot copies; opened CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN)
**Posture:** **READ-ONLY scoping only.** Cursor surfaces data + 2 design options. Logan ratifies. Apply SQL is authored AFTER ratification.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none yet (read-only scoping)

---

## Mission

After mig_177b, **2,502 patients** had `lvi_any_present_path` flip TRUE→FALSE and **2,580 patients** had `vi_any_present_path` flip TRUE→FALSE. Their derivative cols (grade/ordinal/n_tumors/vessel_count) still hold non-null values from the prior alias-bug-driven verification, creating internal inconsistency:

| TRUE→FALSE flippers (LVI 2,502 / VI 2,580) | Current state of derivative col |
|---|---|
| `lvi_grade` | 2,460 of 2,502 still have grade (98%) — STALE |
| `lvi_ordinal_worst` | 2,502 of 2,502 still have ordinal — STALE |
| `n_tumors_lvi_present` | 2,502 of 2,502 have count > 0 — STALE |
| `vasc_grade` / `vasc_grade_final_v13` / vasc_vessel_count_v13` / `vascular_invasion_final` / `vascular_invasion_grade` / `vascular_vessel_count` / `vascular_who_2022_grade` / `vi_ordinal_worst` / `vi_vessels_max` | similar STALE patterns on 2,580 patients |

**99 LVI + 60 VI patients flipped FALSE/NULL→TRUE** — they now lack derivative cols (no grade/ordinal/count for the new positives).

---

## Two design options for Logan to choose

### Option A — Clear-only (conservative)
For all TRUE→FALSE flippers: NULL out `lvi_grade`, set `lvi_ordinal_worst=0`, `n_tumors_lvi_present=0`. Same for VASC family. Fast, internally consistent, but loses the "vasc was present" information (which is now correctly captured in `vi_any_present_path` and `vasc_*` cols).

For FALSE/NULL→TRUE flippers: leave NULL — open `CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS` for follow-up extent extraction (159 patients across LVI+VI).

### Option B — Re-derive from refreshed events (canonical)
For each TRUE→FALSE flipper:
- Look up `canonical_invasion_events_v1` rows for that rid where invasion_type='lymphatic_microscopic' (for LVI) or 'vascular_microscopic' (for VI)
- If 0 PRESENT rows → `lvi_any_present_path=FALSE` confirmed; clear derivatives (lvi_grade=NULL, n_tumors_lvi_present=0, lvi_ordinal_worst=0)
- If PRESENT rows exist → derive grade/ordinal/n_tumors from `evidence_qualifier` / event count

For each FALSE/NULL→TRUE flipper: derive grade/ordinal/n_tumors from the new mig_179 supplemental events.

More complete; preserves clinical info; but requires custom parse of evidence_qualifier strings (e.g., `pattern=mig179_combined_lymphovascular|lymphatic_raw=focal|...`).

---

## Required scope

### §1 Surface current state per derivative col
For each of the 12 derivative cols affected (3 LVI + 9 VASC), produce a 4-bucket cross-tab:

```sql
WITH evts_lymph AS (SELECT DISTINCT research_id::BIGINT AS rid FROM main.canonical_invasion_events_v1 WHERE invasion_type='lymphatic_microscopic' AND finding_status='present'),
evts_vasc AS (SELECT DISTINCT research_id::BIGINT AS rid FROM main.canonical_invasion_events_v1 WHERE invasion_type='vascular_microscopic' AND finding_status='present')
SELECT
  CASE WHEN pm.lvi_any_present_path THEN 'lvi_T' ELSE 'lvi_F' END AS lvi_band,
  pm.lvi_grade IS NOT NULL AS grade_nonnull,
  pm.n_tumors_lvi_present > 0 AS count_gt0,
  COUNT(*) AS n_pts
FROM main.canonical_patient_master pm
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```

### §2 Sample evidence per design option

**Option A sample:** for 5 random TRUE→FALSE flippers, show what fields would be NULLed.

**Option B sample:** for the SAME 5 patients, look up their canonical_invasion_events_v1 rows and show which extent could be extracted (or NULL if no events). Then show what each derivative col would be re-derived to.

### §3 Re-derivation feasibility analysis

For each derivative col, can it be re-derived from refreshed events?
- `lvi_grade`, `vasc_grade`: from `evidence_qualifier`'s `lymphatic_raw=<extent>` token? Yes — partially possible
- `lvi_ordinal_worst`, `vi_ordinal_worst`: ordinal mapping (focal=1, extensive=2, etc.) per Logan's mig_154 ladder; recoverable from grade
- `n_tumors_lvi_present`, `vasc_vessel_count`: count distinct tumor_index from supplemental_events (or vessel_count where structured)
- `vasc_grade_final_v13`, `vascular_who_2022_grade`: WHO 2022 mapping; needs grade as input

### §4 Recommendation matrix

Cursor authors `qc_framework_v1/reports/mig_177c_derivative_reclean_scoping_20260429.md` with:
- §1 cross-tab table (current state)
- §2 sample comparison (Option A vs Option B for 5 patients)
- §3 feasibility per col
- §4 recommendation: which cols are best for Option A (clear) vs Option B (rederive); which need Logan ratification on specific extent rules

### §5 Pre-author placeholder apply SQL (not for execution)

`qc_framework_v1/migrations/177c_lvi_vi_derivatives_reclean_TBD_20260429.sql` — placeholder skeleton for both Option A and Option B variants, clearly marked as `-- LOGAN MUST RATIFY ONE OPTION BEFORE EXECUTION`.

---

## Governance reminders

- **Read-only investigation only.** No SQL apply in this lane.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits.
- The apply SQL produced in §5 is a placeholder; Logan ratifies; Cursor produces a final apply SQL in a follow-up `mig_177c_apply` lane.

---

## Deliverables

1. `qc_framework_v1/reports/mig_177c_derivative_reclean_scoping_20260429.md` — investigation + recommendation
2. `qc_framework_v1/migrations/177c_lvi_vi_derivatives_reclean_TBD_20260429.sql` — placeholder skeleton (NOT for execution)

Commit message: `qc: mig_177c LVI+VI derivative reclean scoping (2502+2580 flippers; Logan ratification needed)`

---

End of prompt.
