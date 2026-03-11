# ThyroSeq Integration Report

**Generated:** 2026-03-11 00:20  
**Batch ID:** `1dd8e0e2-d8f`  
**Git SHA:** `ed16dc0`  

## Summary Metrics

| Metric | Count |
|--------|-------|
| Source rows ingested | 81 |
| High-confidence matches | 48 |
| Manual review required | 33 |
| Unmatched rows | 32 |
| Molecular enrichment rows | 49 |
| Follow-up lab rows | 125 |
| Follow-up event rows | 79 |
| Fill actions | 0 |
| Conflicts | 0 |
| Parse failures | 3 |

## Match Method Breakdown

| Method | Count |
|--------|-------|
| exact_mrn_dob_name | 48 |
| manual_review_required | 32 |
| mrn_with_discordance | 1 |

## Review Queue Summary

| Issue Type | Count |
|------------|-------|
| match_review | 33 |
| parse_failure | 3 |

## Output Tables

| Table | Description |
|-------|-------------|
| `stg_thyroseq_excel_raw` | Raw staging with all original columns + identifiers |
| `stg_thyroseq_match_results` | Patient matching results |
| `stg_thyroseq_parsed` | Parsed/normalized fields |
| `thyroseq_molecular_enrichment` | Molecular findings (long format) |
| `thyroseq_followup_labs` | Serial Tg/TgAb/TSH values (long format) |
| `thyroseq_followup_events` | Surgery/RAI/imaging events (long format) |
| `thyroseq_fill_actions` | Audit log of field fills |
| `thyroseq_review_queue` | Items requiring manual review |

## Export Directory

`exports/thyroseq_integration_20260311_0020/`
