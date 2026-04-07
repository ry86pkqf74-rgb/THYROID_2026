# Validation report

Generated: `2026-04-07T13:26:57.031097+00:00`

## Counts

{
  "canonical_rows": 19891,
  "with_fna_date": 2380,
  "with_molecular_episode": 414,
  "with_surgery_date": 1256,
  "with_final_histology": 622,
  "manual_review_flagged": 608,
  "discordance_total_rows": 87,
  "candidate_pair_rows": 424,
  "manual_review_queue_rows": 608
}

## QC summary (upstream yields)

{
  "n_imaging_nodule_rows": 19891,
  "n_distinct_imaging_nodules": 19891,
  "n_us_fna_primary_links": 2380,
  "n_fna_molecular_rank1": 816,
  "n_preop_surgery_fna_rank1": 3129,
  "n_surgery_path_rank1": 8733
}

## Prior export comparison

n/a

## NIFTP sensitivity

Primary malignancy accounting excludes `niftp_flag=true` rows per policy;
include them in secondary sensitivity sets via `discordance_summary` + `niftp_flag`.
