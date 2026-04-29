# canonical_recurrence_v1 — mig_123 rebuild close-out (Protocol v2)

**Date:** 2026-04-29  
**Lane:** Cursor Lane 19 RESUME (spot-check Logan approval Option 1)  
**MotherDuck batch_id:** `mig_123_canonical_recurrence_v1_rebuild_signoff_20260429`  
**Repo migration file:** `qc_framework_v1/migrations/131_canonical_recurrence_v1_rebuild_signoff_20260429.sql`  
*(SQL file number **131** — number **123** is already `123_canonical_survival_followup_v1_signoff.sql` Lane 15.)*

## Builder

- **Script:** `scripts/203b_canonical_recurrence_harmonized_20260429.py`
- **Spine:** `canonical_operative_events_v1` + cohort `canonical_patient_master` (10,871)
- **Archive snapshot (pre-RW):** `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_recurrence_v1_pre_mig123_20260429`

## Spot-check filters (2026-04-29)

| Filter | Mechanism |
|--------|-----------|
| Legacy `structural_confirmed` | `sql_legacy_old_structural_*`: `INNER JOIN first_surg` and `CAST(recurrence_date AS DATE) > CAST(first_surgery_date AS DATE)` — drops TTR≤0 / negative-TTR legacy rows (incl. overlapping bad-year cases). |
| path_proven date hygiene | `PATH_PROVEN_DEFENSIVE_DATE_FILTER` + `probe_path_proven_date_outliers()` — **2** upstream rows outside **1990–2027** DATE band (audit only until CF-mig124 Tier-1 UNION). |
| Negative TTR clip | Retained; post-filter **0** negatives in dry-run gate. |

## Live probes (post `--write`)

| Probe | Value |
|-------|-------|
| Rows | 10,871 |
| DISTINCT research_id | 10,871 |
| `recurrence_confirmed = TRUE` | **514** |
| `structural_confirmed_legacy` | **16** |

Special case **rid 6674**: retained ~34d TTR — **CF-mig123-LEGACY-COMPLETION-CHECK-6674**.

## Registry

- **CLOSED:** `CF-mig122-RECURRENCE-203-REBUILD-PENDING`
- **OPEN:** `CF-mig123-UPSTREAM-DATE-202-TYPO`, `CF-mig123-NEGATIVE-TTR-9-PATIENTS`, `CF-mig123-LEGACY-COMPLETION-CHECK-6674`, `CF-mig124-RECURRENCE-PATH-CANONICAL-LINEAGE`
- Column verification: 11 derivation cols → `derivation_re_derivation_post_script_203b_harmonized_rebuild`, batch `mig_123_*`

## References

- Prior shell sign-off: `memory/project_canonical_recurrence_v1_mig_122_closeout.md`
- Harmonization context: `feedback_clinical_dates_calendar_only.md`, `feedback_motherduck_direct_check.md`
