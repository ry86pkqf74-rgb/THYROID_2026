# Specimen / genomic review queue export

Generated (UTC): `2026-04-13T20:38:28Z`

## Source

- `qa.specimen_genomic_link_review_v1` (detail + batched worklists)
- `qa.v_diag_specimen_review_burden_v1` (counts by queue_key / review_status)

## Safety

- No raw note text (not present on these objects).
- Truncation: conflict_summary ≤160, reason_codes ≤120, source_row_key ≤96 chars.

## Counts

- `specimen_genomic_link_review_v1` rows exported: **10,155**
- Batched worklist files: **4**
