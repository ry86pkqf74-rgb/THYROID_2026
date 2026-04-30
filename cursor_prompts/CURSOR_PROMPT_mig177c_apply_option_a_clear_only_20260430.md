# Cursor Prompt — mig_177c_apply Option A (clear-only) for LVI+VI derivative reclean

**Date:** 2026-04-30
**Lane:** mig_177c_apply / lvi_vi_derivatives_clear_only
**Batch (proposed):** `mig_177c_apply_option_a_clear_only_20260430`
**Predecessor:** mig_177c (CLOSED at `7210f80` — read-only scoping; report at `qc_framework_v1/reports/mig_177c_lvi_vi_derivative_reclean_scope_20260429.md`)
**Posture:** Read-only audit + SQL authoring + commit + push. **DO NOT execute against MotherDuck.** Cowork applies via Path C.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** `main.canonical_patient_master` (data writes, **clear-only on flippers**) + `main.canonical_column_verification_registry_v1` (note appendix only) + provenance

---

## Logan ratification

**Logan ratified Option A (clear-only)** based on mig_177c scope report. Option B is blocked because `canonical_invasion_events_v1` lacks ordinal grade and vessel-count columns; that lineage extension is a separate future lane. Option A is the minimal-blast-radius internal-consistency cleanup.

---

## Mission

For the **5,082 TRUE→FALSE flippers** (2,502 LVI + 2,580 VI) created by mig_177b, clear the now-stale derivative cols on PM:

| Family | Flippers | Cols to clear | Cells to clear (non-null) |
|---|---:|---|---:|
| LVI | 2,502 | `lvi_grade` (NULL), `lvi_ordinal_worst` (NULL), `n_tumors_lvi_present` (0) | 7,464 |
| VI | 2,580 | `vasc_grade`, `vasc_grade_final_v13`, `vascular_invasion_final`, `vascular_invasion_grade`, `vascular_who_2022_grade`, `vi_ordinal_worst` (all NULL); `vasc_vessel_count_v13`, `vascular_vessel_count`, `vi_vessels_max` (NULL — already 0/0); `vasc_confidence_final_v13`, `vasc_source_final_v13` (NULL); `n_tumors_vi_present` (0) | 20,635 |
| **Total** | **5,082** | **15 cols** | **~28,099** |

Identification of flippers must use the same definition as mig_177b: `pre_true → post_false` in `lvi_any_present_path` / `vi_any_present_path`. The pre-snapshot from mig_177b at `archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_*` is the canonical pre-state. Use it OR re-derive flippers as `pm.<flag>=FALSE AND <prior_truth_for_that_flipper_set>`.

**Note**: also surface (but DO NOT modify) the 159 FALSE/NULL→TRUE flippers (99 LVI + 60 VI) — these now lack derivatives, opening `CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS` for future Option B work.

---

## Required scope

### §1 Re-confirm flipper identification against mig_177b pre-snapshot

```sql
-- LVI flippers from mig_177b: pre=TRUE, post=FALSE
SELECT COUNT(*) AS n_lvi_flippers
FROM main.canonical_patient_master pm
JOIN "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_pre_mig177b_<exact_snapshot_name> pre USING (research_id)
WHERE pre.lvi_any_present_path = TRUE AND pm.lvi_any_present_path = FALSE;
-- Expected: 2502

-- VI flippers
... 2580
```

If the exact pre-snapshot table name differs, locate via `information_schema.tables` filter on `lvi_vi_pre_mig177b%` or `pre_mig177b%`.

### §2 Pre-snapshot of affected slice

```sql
CREATE TABLE IF NOT EXISTS "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_lvi_vi_derivatives_pre_mig177c_apply_20260430 AS
SELECT pm.research_id,
       pm.lvi_any_present_path, pm.vi_any_present_path,
       pm.lvi_grade, pm.lvi_ordinal_worst, pm.n_tumors_lvi_present,
       pm.vasc_grade, pm.vasc_grade_final_v13, pm.vascular_invasion_final, pm.vascular_invasion_grade,
       pm.vascular_who_2022_grade, pm.vi_ordinal_worst,
       pm.vasc_vessel_count_v13, pm.vascular_vessel_count, pm.vi_vessels_max,
       pm.vasc_confidence_final_v13, pm.vasc_source_final_v13, pm.n_tumors_vi_present,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig177c_apply_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master pm
WHERE -- flippers OR stable_true OR FALSE/NULL→TRUE flippers — i.e., everyone with any derivative nonnull
  pm.lvi_grade IS NOT NULL OR pm.lvi_ordinal_worst IS NOT NULL OR pm.n_tumors_lvi_present > 0
  OR pm.vasc_grade IS NOT NULL OR pm.vasc_grade_final_v13 IS NOT NULL OR pm.vascular_invasion_final IS NOT NULL
  OR pm.vascular_invasion_grade IS NOT NULL OR pm.vascular_who_2022_grade IS NOT NULL OR pm.vi_ordinal_worst IS NOT NULL
  OR pm.vasc_vessel_count_v13 IS NOT NULL OR pm.vascular_vessel_count IS NOT NULL OR pm.vi_vessels_max IS NOT NULL
  OR pm.vasc_confidence_final_v13 IS NOT NULL OR pm.vasc_source_final_v13 IS NOT NULL OR pm.n_tumors_vi_present > 0;
```

(Choose stricter scope if total row count > ~3,500.)

### §3 Apply Option A clear-only

```sql
-- LVI clear (3 cols × 2,502 patients)
UPDATE main.canonical_patient_master
SET lvi_grade = NULL,
    lvi_ordinal_worst = NULL,
    n_tumors_lvi_present = 0
WHERE lvi_any_present_path = FALSE
  AND research_id IN (SELECT research_id FROM <flipper CTE>)
  AND (lvi_grade IS NOT NULL OR lvi_ordinal_worst IS NOT NULL OR n_tumors_lvi_present > 0);

-- VI clear (12 cols × 2,580 patients)
UPDATE main.canonical_patient_master
SET vasc_grade = NULL, vasc_grade_final_v13 = NULL,
    vascular_invasion_final = NULL, vascular_invasion_grade = NULL,
    vascular_who_2022_grade = NULL, vi_ordinal_worst = NULL,
    vasc_vessel_count_v13 = NULL, vascular_vessel_count = NULL, vi_vessels_max = NULL,
    vasc_confidence_final_v13 = NULL, vasc_source_final_v13 = NULL,
    n_tumors_vi_present = 0
WHERE vi_any_present_path = FALSE
  AND research_id IN (SELECT research_id FROM <flipper CTE>)
  AND (vasc_grade IS NOT NULL OR vasc_grade_final_v13 IS NOT NULL
       OR vascular_invasion_final IS NOT NULL OR vascular_invasion_grade IS NOT NULL
       OR vascular_who_2022_grade IS NOT NULL OR vi_ordinal_worst IS NOT NULL
       OR vasc_vessel_count_v13 IS NOT NULL OR vascular_vessel_count IS NOT NULL OR vi_vessels_max IS NOT NULL
       OR vasc_confidence_final_v13 IS NOT NULL OR vasc_source_final_v13 IS NOT NULL OR n_tumors_vi_present > 0);
```

Use either explicit flipper rid set from §1 OR the conjunction `<post-mig177b flag>=FALSE AND <pre-mig177b flag>=TRUE` — whichever produces a clean idempotent UPDATE.

### §4 Post-state probes

For each of the 15 cleared cols, post-counts on flippers should be 0/0 or 100%-null:

```sql
SELECT
  COUNT(*) FILTER (WHERE lvi_any_present_path = FALSE AND lvi_grade IS NOT NULL) AS lvi_grade_residual,
  COUNT(*) FILTER (WHERE lvi_any_present_path = FALSE AND lvi_ordinal_worst IS NOT NULL) AS lvi_ord_residual,
  COUNT(*) FILTER (WHERE lvi_any_present_path = FALSE AND n_tumors_lvi_present > 0) AS lvi_n_residual,
  -- VI similarly
  ...
  COUNT(*) AS pm_total,
  COUNT(DISTINCT research_id) AS pm_distinct_rids
FROM main.canonical_patient_master;
```

Pass criteria: all _residual = 0; pm_total = 10,871; pm_distinct_rids = 10,871.

### §5 CF closure notes (registry note appendix only)

For the 15 derivative col rows in `canonical_column_verification_registry_v1` (PM scope), append closure note:
- `mig_177c_apply CLOSED CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN via Option A clear-only on <N> flippers.`

Use idempotent `POSITION(...) > 0` guard.

Open NEW informational CF on the 15 col rows:
- `CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS: 99 LVI + 60 VI patients flipped FALSE/NULL→TRUE in mig_177b lack derivatives; future Option B lane needed (requires grade/count cols on canonical_invasion_events_v1).`

### §6 cpm_reconciliation_provenance_v1 row

Insert one row with `run_id='canonical_cleanup_mig177c_apply_option_a_clear_only_20260430'`, `phases_applied='pre_snapshot_lvi_clear_vi_clear_post_state_probe_registry_notes'`, `critical_findings_cleared='CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN'`, `held_for_adjudication='CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS'`.

### §7 Audit/report

`qc_framework_v1/reports/mig_177c_apply_option_a_audit_20260430.md`:
- §1 flipper re-confirmation against pre-snapshot
- §2 pre-snapshot row counts
- §3 per-col cells cleared (expected ≈ 28,099 total)
- §4 post-state probe results (all _residual=0)
- §5 informational CF opened (159 FALSE/NULL→TRUE flippers)
- §6 next: CF-mig177c-EXTENT-MISSING handed to a future "extend canonical_invasion_events_v1 with grade/count cols" lane (likely script 363 follow-up + Tier-2 extractor extension)

---

## Governance reminders

- Read-only audit + SQL authoring only. Cowork applies via Path C.
- Pre-snapshot mandatory before any UPDATE.
- DO NOT touch the 99 LVI + 60 VI FALSE/NULL→TRUE flippers — they need Option B treatment, not Option A.
- Author = `Logan Glosser <logan.glosser@gmail.com>` for all commits.
- Surgical git add only.
- No `BEGIN TRANSACTION;`/`COMMIT;`.

---

## Deliverables

1. `qc_framework_v1/migrations/177c_apply_option_a_clear_only_20260430.sql`
2. `qc_framework_v1/reports/mig_177c_apply_option_a_audit_20260430.md`

Commit message: `qc: mig_177c_apply Option A clear-only authoring (5,082 flippers; 28,099 derivative cells; CF-mig177b closure)`

---

End of prompt.
