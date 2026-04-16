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

---

## Canonical v1_0 finalization run (Scripts 237–247, started 2026-04-16)

Post-baseline fixup pass driven by coworker data-quality review. See
`CURSOR_PROMPT_224_CANONICAL_FIXES.md` for the source spec. Every script
carries a backup to `"Thyroid 2026 UPdated".archive_pub_v1_0` (where a
destructive op exists) and an assertion block. Scripts without a
pre-change backup are explicitly no-ops on data (documentation only).

### Script 237 — Document imaging↔FNA size concordance gap (no-op on data)
- **Type:** documentation-only (no row counts or cell values change).
- **Why:** `imaging_fna_linkage_v3.fna_size_cm` has no independent source in
  the canonical DB. The only candidate backfill path
  (`imaging_nodule_long_v2.size_cm_max` via `nodule_id`) is the same source
  `img_size_cm` already uses (verified: 9,911/9,911 byte-identical), so any
  derived `size_score` would be tautologically 1.0. Preserving the flat 0.5
  fallback is the correct v1_0 behavior until an independent FNA-side size
  extractor is built.
- **Changes:**
  - `COMMENT ON COLUMN imaging_fna_linkage_v3.fna_size_cm` / `.size_score`
    with v1_0 design intent + v1_1 TODO.
  - `UPDATE manuscript_workspace.detail_table_registry_v1` description for
    `imaging_fna_linkage_v3` to surface the gap.
  - `INSERT` 2 provisional rows into `data_dictionary_v240` (for
    `fna_size_cm` and `size_score`).
- **Assertions (10/10 PASS):** row counts unchanged; `canonical_patient_master`
  at 10,871; comments persisted; registry description updated; dictionary rows
  exactly 1 each.
- **Follow-up for v1_1:** build a targeted NLP pass over
  `note_entities_llm_us_nodule_dynamics` / `note_entities_llm_tirads_granular`
  to extract FNA-era nodule sizes, then re-run the scoring.
