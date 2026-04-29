# Harmonized Script 203b — dry-run report (canonical_recurrence_v1)

**UTC:** 2026-04-29T03:46:36.823841+00:00  
**Mode:** dry-run (no MotherDuck table replace)

## Harmonization spine
- Operative backbone: `canonical_operative_events_v1` (replaces `operative_episode_detail_v2`).
- Cohort padding: `canonical_patient_master` (replaces `gold_master_patient_facts_v1`).

## Live table before run (MotherDuck)
```json
{
  "live_row_count": 10871,
  "recurrence_type_counts": {
    "none": 9583,
    "persistent_biochemical_disease": 649,
    "structural_confirmed": 440,
    "biochemical_tg_rise": 80,
    "fna_confirmed": 58,
    "imaging_suspicious_unconfirmed": 45,
    "structural_confirmed_legacy": 16
  },
  "confirmed_true": 514,
  "confirmed_false": 10357,
  "distinct_cpm_research_ids": 10871
}
```

## Rebuilt summary (dry-run dataframe)
- Rows: **10871** (distinct `research_id`: **10871**)
- Confirmed TRUE: **514**

### `recurrence_type` distribution

- `none`: **9583**
- `persistent_biochemical_disease`: **649**
- `structural_confirmed`: **440**
- `biochemical_tg_rise`: **80**
- `fna_confirmed`: **58**
- `imaging_suspicious_unconfirmed`: **45**
- `structural_confirmed_legacy`: **16**

### Acceptance gate preview
```json
{
  "cohort_10871_expectation": true,
  "distinct_rids_equals_rows": true,
  "non_shell_confirmed_positive": true,
  "confirmed_have_evidence_when_true": true,
  "time_to_recurrence_negative_clipped_rows": 0,
  "none_category_null_recurrence_date": true
}
```

### Spot-check filters — Lane 19 RESUME (2026-04-29)

- `recurrence_event_clean_v1` legacy fallback: **`recurrence_date > first_surgery_date`** 
  (`structural_confirmed`) — excludes initial-diagnosis/completion-Thy mismaps (CF narrative in mig_123).
- **`manuscript_workspace.recurrence_path_proven_candidates_v1`**: predicate constant 
`PATH_PROVEN_DEFENSIVE_DATE_FILTER` (1990–2027 inclusive) retained for Tier-1 future UNION;
`path_proven_upstream_date_outliers` count = rows outside band (upstream clean-up deferred).

- JSON: `/Users/ros/THyroid 2026/scripts/output/canonical_recurrence_203b_dry_run_report_20260429.json`
- Preview parquet: `/Users/ros/THyroid 2026/scripts/output/canonical_recurrence_v1_preview_203b.parquet`