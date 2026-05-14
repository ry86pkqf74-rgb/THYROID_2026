# Cursor handoff: INSERT Afirma rows for rids 8218 and 9154

**Recommendation: do this in Cursor.** Two patient-level INSERTs into `pub_canonical.canonical_molecular_genetics_v2` to close the mig_325 residual where ThyroSeq parser hallucinations were superseded but no Afirma row existed to receive the reclassified call.

## Goal

Insert two new rows into `canonical_molecular_genetics_v2` capturing the actual Afirma test results for rids 8218 and 9154, per the structured data confirmed in `THYROSEQ_AFIRMA_12_5.xlsx` (uploaded 2026-05-14).

## Data to INSERT

PHI-safe extracted structured fields only (raw narrative excluded from this prompt — see the upload directly if Cursor needs to verify against the source text, but do NOT commit any narrative quotes to git/Airtable):

| research_id | platform | overall_result_class | rom_descriptor | resolved_test_date | bethesda_category | source | notes |
|---|---|---|---|---|---|---|---|
| 8218 | Afirma | suspicious | NULL (Afirma binary call) | 2024-05-01 (approx — source says "5/24") | (not in source) | manual_insert_from_THYROSEQ_AFIRMA_12_5_xlsx | RESULT_1 = "Suspicious"; from clinical narrative |
| 9154 | Afirma | suspicious | NULL | 2020-10-28 | 3 | manual_insert_from_THYROSEQ_AFIRMA_12_5_xlsx | RESULT_1 = "high risk" + AUS Bethesda 3; MUTATION_1 = "report unavailable" |

Both should map to **Afirma test-positive** in the Table 3 v3 SQL (since overall_result_class IN ('suspicious','positive')).

## Hard rules

1. **Snapshot first**: `CREATE OR REPLACE TABLE pub_archive.canonical_molecular_genetics_v2_pre_manual_insert_<YYYYMMDD> AS SELECT * FROM pub_canonical.canonical_molecular_genetics_v2;`
2. **Pre-edit DFL row** (`appJYOnUb7KrHKwpV.tblsiYKJtKcktkzze`): `change_type=data_correction`, target = `canonical_molecular_genetics_v2`, your_request_summary = "Manual Afirma INSERT for rids 8218, 9154 to close mig_325 residual; data extracted from THYROSEQ_AFIRMA_12_5.xlsx".
3. **No PHI in DFL or MFL**. Use the structured fields only; do NOT paste narrative quotes.
4. **MFL after the INSERT** (`recqp9bdMTqNfILhJ`'s sibling): `MFL-<YYYYMMDD>-EXT2-4-AFIRMA-MANUAL-INSERT`, linked to EXT2-4 (`rec1GJyrmKdKxjlaY`), summarize the INSERT and the manuscript impact.
5. Update VC-MOL-PLATFORM-001 (`recPnjqNfMaE1AS9H`) `fix_action` text noting the 2 residual rows are now closed.

## SQL skeleton

```sql
-- Snapshot
CREATE OR REPLACE TABLE
  `pub_archive.canonical_molecular_genetics_v2_pre_manual_insert_<YYYYMMDD>` AS
SELECT * FROM `pub_canonical.canonical_molecular_genetics_v2`;

-- Generate molecular_episode_id_v2 (deterministic hash, matches mig_324 convention)
WITH new_rows AS (
  SELECT
    "8218" AS research_id,
    "Afirma" AS platform,
    "suspicious" AS overall_result_class,
    CAST(NULL AS STRING) AS rom_descriptor,
    CAST(NULL AS FLOAT64) AS rom_percent_point,
    DATE "2024-05-01" AS resolved_test_date,
    CAST(NULL AS INT64) AS bethesda_category,
    "manual_insert_from_THYROSEQ_AFIRMA_12_5_xlsx_20260514" AS report_source_table,
    "manual_insert_v1" AS band_source,
    "afirma_result_field" AS parathyroid_weight_source,  -- unused for Afirma
    CURRENT_TIMESTAMP() AS band_backfill_applied_at,
    "manual_insert_<run_id>" AS band_backfill_run_id

  UNION ALL

  SELECT
    "9154", "Afirma", "suspicious", CAST(NULL AS STRING), CAST(NULL AS FLOAT64),
    DATE "2020-10-28", 3,
    "manual_insert_from_THYROSEQ_AFIRMA_12_5_xlsx_20260514",
    "manual_insert_v1",
    "afirma_result_field",
    CURRENT_TIMESTAMP(),
    "manual_insert_<run_id>"
)
INSERT INTO `pub_canonical.canonical_molecular_genetics_v2`
  (research_id, platform, overall_result_class, rom_descriptor, rom_percent_point,
   resolved_test_date, bethesda_category, report_source_table, band_source,
   band_backfill_applied_at, band_backfill_run_id,
   molecular_episode_id_v2, test_dedup_key)
SELECT
  research_id, platform, overall_result_class, rom_descriptor, rom_percent_point,
  resolved_test_date, bethesda_category, report_source_table, band_source,
  band_backfill_applied_at, band_backfill_run_id,
  FARM_FINGERPRINT(CONCAT(research_id, '|',
                          CAST(IFNULL(resolved_test_date, DATE '1900-01-01') AS STRING), '|',
                          IFNULL(platform, '_unknown'), '|',
                          IFNULL(report_source_table, '_unknown'))) AS molecular_episode_id_v2,
  FARM_FINGERPRINT(CONCAT(research_id, '|',
                          CAST(IFNULL(resolved_test_date, DATE '1900-01-01') AS STRING), '|',
                          IFNULL(platform, '_unknown'))) AS test_dedup_key
FROM new_rows;
```

Adjust the column list to whatever the live schema requires (per mig_324 it should include `band_backfill_*` audit columns; per mig_323 the `platform_reclass_status` should be NULL for these new rows since they're not a reclassification).

## Verification

After INSERT:

```sql
SELECT research_id, platform, overall_result_class, rom_descriptor, resolved_test_date,
       report_source_table, band_source
FROM `pub_canonical.canonical_molecular_genetics_v2`
WHERE research_id IN ("8218", "9154")
ORDER BY research_id, platform;
```

Expected: each rid has TWO rows now — one superseded ThyroSeq row (from mig_325) and one new Afirma row (from this INSERT). Both should appear; the superseded ThyroSeq row should still have `platform_reclass_status='superseded_by_afirma_row'` so the Table 3 SQL excludes it; the new Afirma row should slot into the Afirma B3+B4 evaluable subset.

Re-run `sql/04b_table3_v2_actual_reported_call.sql` and confirm:
- Afirma B3+B4 n: 90 → **92** (+2 from the INSERT)
- Both new rows show up as `positive` reported_call
- For rid 9154: positive call, B3 cytology, no histology resolved → benign (since `histology_final IS NULL`) → counts as FP if histology stays NULL, or TP if histology is later resolved as malignant. Spot-check.

## Acceptance

- [ ] 2 new rows present in `canonical_molecular_genetics_v2` with research_id IN (8218, 9154) and platform=Afirma
- [ ] Snapshot at `pub_archive.canonical_molecular_genetics_v2_pre_manual_insert_<YYYYMMDD>`
- [ ] DFL + MFL filed; VC-MOL-PLATFORM-001 fix_action updated
- [ ] Table 3 v3 Afirma B3+B4 n grew by exactly 2

## Manuscript impact

Marginal — Afirma cohort grows from 90 → 92. Wilson CIs barely shift. No prose changes needed; just refresh `build_figures_v2.py` forest_rows if a perfect-consistency rebuild is desired.

## Source file note

`THYROSEQ_AFIRMA_12_5.xlsx` is a PHI-containing institutional spreadsheet (column "Thyroseq/Afirma_1" contains clinical narrative). DO NOT commit the upload to git or copy any narrative text into Airtable. Only structured fields (research_id, platform, result class, date, Bethesda) cross the boundary.
