# Canonical Changelog

---

## v1_0 — 2026-04-16

**Type:** baseline (initial publication)

**MotherDuck DB:** `thyroid_canonical_publication_v1_0`

**Built by:** `scripts/223_ingest_and_publish.py` (pre-versioning), renamed to
`thyroid_canonical_publication_v1_0` on 2026-04-16 via
`scripts/225_promote_canonical_version.py` versioning convention adoption.

**Contents:**

- **10,871** thyroid surgery patients (research_id 100% populated, zero nulls)
- **1,377** columns in `canonical_patient_master` (100% commented)
- **110 tables** total:
  - 1 patient master (`canonical_patient_master`)
  - 15 patient-level summaries
  - 58 episode-level tables
  - 30 NLP entity tables
  - 3 data dictionaries
  - 2 utility tables (`__readme`, `data_dictionary_parquet_v221`)
- 6 newly-ingested tables (not present in source prior to Script 223):
  - `mri_imaging` — 715 MRI exams (PHI-scrubbed)
  - `nsqip_enrichment` — 1,275 perioperative records (DOB removed)
  - `nsqip_patient_summary` — 1,261 NSQIP summaries (DOB removed)
  - `patient_completion_oed_path_linkage_v1` — 11,506 completion linkage rows
  - `thyroid_weights` — 10,001 gland weight records (DOB + path text removed)
  - `thyroid_sizes` — 11,675 standardized size records

**Script 221c gap-fix state (verified pre-build):**

| Invariant | Value |
|-----------|-------|
| Total patients | 10,871 |
| `null research_id` | 0 |
| `followup_years > 0` | 4,038 |
| `prm_first_fna_date IS NOT NULL` | 5,212 |
| `first_tg_date IS NOT NULL` | 2,721 |

**Built from:**
- Source DB: `"Thyroid 2026 UPdated"` on eras MotherDuck account
- Script 221c gap fixes applied before build

**Known gaps (will be addressed in v1_1 when data lands):**
- Lab pull pending: TSH/PTH/Ca/VitD baseline values not yet ingested
- US exam dates incomplete: ~4,082 patients missing baseline US date
- Molecular test dates incomplete: ~809 patients
- `followup_years = 0` for 6,833 patients (still in active follow-up or very early cohort)
