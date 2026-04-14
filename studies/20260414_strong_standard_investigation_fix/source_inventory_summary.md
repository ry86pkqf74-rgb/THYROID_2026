# Source Inventory Summary — 2026-04-14

## Ultrasound Corpora

| Corpus | Raw Table | Raw Rows | Canonical Rows | Patients | TI-RADS Reported | TI-RADS ACR | ACR Features |
|--------|-----------|----------|----------------|----------|------------------|-------------|--------------|
| COMPLETE | raw_us_tirads_excel_v1 | 19,891 | 19,891 | 4,074 | 19,572 (98.4%) | 19,891 (100%) | YES (full) |
| Scored | raw_us_tirads_scored_v1 | 19,549 | 8,331 | ~10,862 | 8,331 (100%) | 0 (0%) | NO |
| Imaging_12 | raw_imaging_12_slots_v1 | 21,079 | 8,794 | ~4,000 | 0 (0%) | 0 (0%) | NO (n_criteria=0) |
| **Canonical** | **imaging_nodule_master_v1** | **—** | **37,016** | **~6,000** | **27,903 (75.4%)** | **19,891 (53.7%)** | — |

### Notes
- COMPLETE is the only corpus with full ACR TI-RADS feature data (composition, echogenicity, shape, margins, calcifications)
- Scored corpus has radiologist-reported TI-RADS but no ACR features for independent recalculation
- Imaging_12 has neither reported TI-RADS nor ACR features; all 21,079 raw rows have `n_criteria_available = 0`
- Raw→Canonical reduction is due to ±30d dedup policy in `scripts/50_multinodule_imaging.py`
- No additional US corpus discovered in raw/, DVC, or ingest scripts beyond these three

## FNA Sources

| Table | Rows | Distinct Patients |
|-------|------|-------------------|
| fna_history | 8,119 | ~3,000 |
| fna_cytology | 8,063 | ~3,000 |
| fna_episode_master_v2 | 8,119 | ~3,000 |

## Ultrasound LN Data

| Source | Rows | Has LN Content | Structured LN Columns |
|--------|------|----------------|----------------------|
| ultrasound_reports | 6,793 | 6,793 (100%) | `lymph_node_assessment` only (narrative) |

- **No per-level, per-laterality, or per-size structured LN columns exist in any source**
- 6,453 exams have negative/normal LN assessments
- 340 exams have other narrative content (some with level mentions)

## Serial Imaging US

| Table | Rows | Status |
|-------|------|--------|
| serial_imaging_us | 0 | Empty placeholder (schema only) |

- Created via `scripts/155_md_serial_imaging_us_placeholder.py`
- Institutional feed required to populate
