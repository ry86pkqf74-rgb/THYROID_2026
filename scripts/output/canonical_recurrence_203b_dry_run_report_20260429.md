# Harmonized Script 203b — dry-run report (canonical_recurrence_v1)

**UTC:** 2026-04-29T03:22:14.672409+00:00  
**Mode:** dry-run (no MotherDuck table replace)

## Harmonization spine
- Operative backbone: `canonical_operative_events_v1` (replaces `operative_episode_detail_v2`).
- Cohort padding: `canonical_patient_master` (replaces `gold_master_patient_facts_v1`).

## Live table before run (MotherDuck)
```json
{
  "live_row_count": 10871,
  "recurrence_type_counts": {
    "none": 10871
  },
  "confirmed_true": 0,
  "confirmed_false": 10871,
  "distinct_cpm_research_ids": 10871
}
```

## Rebuilt summary (dry-run dataframe)
- Rows: **10871** (distinct `research_id`: **10871**)
- Confirmed TRUE: **528**

### `recurrence_type` distribution

- `none`: **9569**
- `persistent_biochemical_disease`: **649**
- `structural_confirmed`: **440**
- `biochemical_tg_rise`: **80**
- `fna_confirmed`: **58**
- `imaging_suspicious_unconfirmed`: **45**
- `structural_confirmed_legacy`: **30**

### Acceptance gate preview
```json
{
  "cohort_10871_expectation": true,
  "distinct_rids_equals_rows": true,
  "non_shell_confirmed_positive": true,
  "confirmed_have_evidence_when_true": true,
  "time_to_recurrence_negative_clipped_rows": 9,
  "none_category_null_recurrence_date": true
}
```

### Logan — pause gate (§3d)
Do **not** run `--write`, archive snapshot, or mig_123 until Logan approves RW rebuild.

- JSON: `/Users/ros/THyroid 2026/scripts/output/canonical_recurrence_203b_dry_run_report_20260429.json`
- Preview parquet: `/Users/ros/THyroid 2026/scripts/output/canonical_recurrence_v1_preview_203b.parquet`