# Cursor Composer Dispatch — mig_281: SF→MD NLP canonical promotion (smoking + family_hx + vasc invasion)

**Generated:** 2026-05-03 by Cowork at HEAD `4aedc94`.
**Lane:** mig_281 — Promote Snowflake AI_CLASSIFY NLP results back to MotherDuck canonical NLP tables + repoint CPM rollup cols. Replaces the deprecated mig_272 H200/Vast.ai path per Logan-ratified `feedback_nlp_refresh_on_snowflake.md`.

**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — needs walking through the SF→MD bridge logic; promotion itself is mechanical.
**Estimated runtime:** 90 min (waits for Cowork to land full-scale SF runs first).
**Triggered by:** Cowork SF AI_CLASSIFY pilots (N1 smoking 93% / N2 family-hx 84% / N3 vasc invasion 49%) + Logan greenlight to promote.
**Severity:** HIGH for M032 (smoking prevalence in Table 1) + M037 (family-hx as LN predictor) + M044 (smoking confounder).
**Closes carry-forward:** CF-VASC-INVASION-749-UNDERFIRES + CF-SMOKING-COVERAGE-GAP + CF-FAMILY-HX-COVERAGE-GAP at canonical level.

---

## §0 — First message to paste into Cursor Chat

> mig_281 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_281_NLP_SF_TO_MD_PROMOTION_20260503.md` end-to-end. Cowork has run AI_CLASSIFY against full SF corpus and dropped results into 3 SF tables. Your job is the SF→MD bridge: pull SF results via the duckdb `md:` connection or a one-shot Parquet roundtrip, append to MD `note_entities_llm_pmhx` / `note_entities_llm_invasion_v2`, then rebuild CPM cols.
>
> MotherDuck DB is `thyroid_canonical_publication_v1_0`. SF source tables in `THYROID_VALIDATION.PUBLIC.NLP_*_RESULTS_v1`. Pre-snapshot any rebuilt MD object to `"Thyroid 2026 UPdated".archive_pub_v1_0`.
>
> **PRECONDITION:** Cowork must confirm full-scale SF runs landed before you start. Look for `THYROID_VALIDATION.PUBLIC.NLP_SMOKING_FULL_RESULTS_v1` (~3,541 rows) + `NLP_FAMILY_HX_THYROID_FULL_RESULTS_v1` + (optional) `NLP_VASC_INVASION_FULL_RESULTS_v1`. If any are missing or pilot-only, surface to Logan before proceeding.

---

## §1 — SF source inventory (pre-task probe)

```sql
-- Run on Snowflake (via _sf_client.py or SSO snowsql):
SHOW TABLES LIKE 'NLP_%_RESULTS_v1' IN THYROID_VALIDATION.PUBLIC;
SELECT COUNT(*) FROM THYROID_VALIDATION.PUBLIC.NLP_SMOKING_FULL_RESULTS_v1;
SELECT COUNT(*) FROM THYROID_VALIDATION.PUBLIC.NLP_FAMILY_HX_THYROID_FULL_RESULTS_v1;
SELECT COUNT(*) FROM THYROID_VALIDATION.PUBLIC.NLP_VASC_INVASION_FULL_RESULTS_v1;
```

Schema (from N1/N2/N3 pilots):
| Col | Type |
|---|---|
| RESEARCH_ID | VARCHAR |
| NOTE_TYPE | VARCHAR |
| NOTE_INDEX | INTEGER |
| classification_raw | VARIANT (raw AI_CLASSIFY response) |
| smoking_status / family_hx_status / vasc_invasion_status | VARCHAR (decision label) |
| classified_at | TIMESTAMP |
| llm_model | VARCHAR (= 'AI_CLASSIFY_default_model') |

---

## §2 — SF→MD transport

Three transport options in increasing complexity; pick simplest that works:

### Option A — DuckDB direct attach (preferred)
DuckDB supports the Snowflake extension. Connect both then INSERT … SELECT:

```python
import duckdb, os
md = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")
md.execute("INSTALL snowflake; LOAD snowflake;")
md.execute(f"""
ATTACH 'account=qcc02515.us-east-1;user=LGLOSSE13;password={os.environ['SNOWFLAKE_PAT']};database=THYROID_VALIDATION;schema=PUBLIC' AS sf (TYPE SNOWFLAKE)
""")
md.execute("CREATE OR REPLACE TABLE main.nlp_smoking_sf_pull_20260503 AS SELECT * FROM sf.PUBLIC.NLP_SMOKING_FULL_RESULTS_v1")
```

If extension unavailable in your DuckDB version → fallback B.

### Option B — Parquet roundtrip
1. SF: `COPY INTO @COWORK_STAGE/promote/ FROM NLP_SMOKING_FULL_RESULTS_v1 FILE_FORMAT=(TYPE=PARQUET)`
2. SF: `GET @COWORK_STAGE/promote/ file:///tmp/nlp_promote/`
3. MD: `INSERT INTO main.nlp_smoking_sf_pull_20260503 SELECT * FROM read_parquet('/tmp/nlp_promote/*.parquet')`

### Option C — CSV via cursor + DuckDB ingest
Last resort. Skip if A/B work.

---

## §3 — MD schema reconciliation

Existing MD targets:
- `main.note_entities_llm_pmhx` — for smoking + family_hx (set `category` discriminator)
- `main.note_entities_llm_invasion_v2` — for vasc invasion residual

Probe expected schema:
```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='main' AND table_name='note_entities_llm_pmhx' ORDER BY ordinal_position;

SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='main' AND table_name='note_entities_llm_invasion_v2' ORDER BY ordinal_position;
```

Map SF cols → MD cols (case-insensitively), filling in:
- `category` = 'smoking' / 'family_hx_thyroid' / 'vasc_invasion' as appropriate
- `llm_model` = 'AI_CLASSIFY_snowflake_cortex_20260503'
- `extracted_at` = SF `classified_at`

---

## §4 — Apply (Protocol v2)

### §4a — Pre-snapshot

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.note_entities_llm_pmhx_pre_mig281_20260503 AS
SELECT * FROM main.note_entities_llm_pmhx;

CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.note_entities_llm_invasion_v2_pre_mig281_20260503 AS
SELECT * FROM main.note_entities_llm_invasion_v2;

CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_nlp_cols_pre_mig281_20260503 AS
SELECT research_id,
       pmhx_nlp_smoking_status, nsqip_smoker,
       pmhx_nlp_family_hx_thyroid, pmhx_nlp_family_hx_cancer,
       vascular_invasion_final
FROM main.canonical_patient_master;
```

### §4b — Append SF results to MD canonicals

```sql
INSERT INTO main.note_entities_llm_pmhx BY NAME
SELECT
  research_id, note_type, note_index,
  'smoking' AS category,
  smoking_status AS extracted_value,
  classification_raw AS extracted_raw,
  classified_at AS extracted_at,
  'AI_CLASSIFY_snowflake_cortex_20260503' AS llm_model,
  -- map any other required columns (confidence, error, etc.) per the schema you observed
  ...
FROM main.nlp_smoking_sf_pull_20260503;

-- Repeat for family_hx and vasc_invasion (vasc into invasion_v2 instead of pmhx)
```

### §4c — Rebuild CPM rollup cols

For smoking:
```sql
UPDATE main.canonical_patient_master pm
SET pmhx_nlp_smoking_status = (
  SELECT
    -- If any extraction = 'current_smoker', current; else former; else never; else NULL
    CASE
      WHEN COUNT(*) FILTER (WHERE extracted_value='current_smoker') > 0 THEN 'current'
      WHEN COUNT(*) FILTER (WHERE extracted_value='former_smoker')  > 0 THEN 'former'
      WHEN COUNT(*) FILTER (WHERE extracted_value='never_smoker')   > 0 THEN 'never'
      ELSE NULL
    END
  FROM main.note_entities_llm_pmhx p
  WHERE p.research_id = pm.research_id
    AND p.category = 'smoking'
    AND p.extracted_value IN ('current_smoker','former_smoker','never_smoker')
);
```

Mirror for `pmhx_nlp_family_hx_thyroid` (BOOL: TRUE if any present, FALSE if any absent w/o present, NULL if all unknown).

For vasc invasion: only update `vascular_invasion_final` where currently NULL AND the SF extraction is non-unknown (preserve existing CAP-derived values).

### §4d — Verify

```sql
-- Coverage uplift
SELECT
  'smoking' AS slice,
  COUNT_IF(pmhx_nlp_smoking_status IS NOT NULL) AS n_known_post,
  (SELECT COUNT_IF(pmhx_nlp_smoking_status IS NOT NULL) FROM "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_nlp_cols_pre_mig281_20260503) AS n_known_pre
FROM main.canonical_patient_master
UNION ALL
SELECT 'family_hx_thyroid',
  COUNT_IF(pmhx_nlp_family_hx_thyroid IS NOT NULL),
  (SELECT COUNT_IF(pmhx_nlp_family_hx_thyroid IS NOT NULL) FROM "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_nlp_cols_pre_mig281_20260503)
FROM main.canonical_patient_master
UNION ALL
SELECT 'vasc_invasion',
  COUNT_IF(vascular_invasion_final IS NOT NULL),
  (SELECT COUNT_IF(vascular_invasion_final IS NOT NULL) FROM "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_nlp_cols_pre_mig281_20260503)
FROM main.canonical_patient_master;
```

Expected uplift (pilot extrapolation):
- Smoking: 13 → ~3,300 (×254)
- Family_hx_thyroid: 163 → ~3,000 (×18)
- Vasc invasion: stays ~1,172 + ~600 new = ~1,750 (49% pilot yield × 1,200 NULL)

### §4e — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_281', CURRENT_TIMESTAMP, 'cursor_composer_mig281',
 'mig_281: Promoted SF AI_CLASSIFY NLP results to MD canonicals. Appended Nv smoking + Ns family_hx + Nf vasc to note_entities_llm_pmhx + invasion_v2. Rebuilt CPM cols pmhx_nlp_smoking_status / pmhx_nlp_family_hx_thyroid / vascular_invasion_final. Coverage uplift: smoking 13→Nv, family_hx 163→Ns, vasc 1172→Nf+1172. llm_model=AI_CLASSIFY_snowflake_cortex_20260503. Closes CF-VASC-INVASION-749-UNDERFIRES + CF-SMOKING-COVERAGE-GAP + CF-FAMILY-HX-COVERAGE-GAP at canonical level.');
```

---

## §5 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-VASC-INVASION-749-UNDERFIRES | **CLOSED on apply** (canonical level) | Was already CLOSED at extraction level; canonical close completes |
| CF-SMOKING-COVERAGE-GAP | **CLOSED on apply** | Coverage 0.3% → ~70% on malig |
| CF-FAMILY-HX-COVERAGE-GAP | **CLOSED on apply** | Coverage 4% → ~60% on malig |
| CF-mig281-VASC-PILOT-YIELD-LOW | **OPEN if vasc included** | N3 pilot was only 49% actionable (vs 70% gate). Logan-decision: include partial extraction OR re-tune prompt with structured AI_COMPLETE extraction OR scope to path_synoptic-class only |
| CF-mig281-LLM-VERSION-CHANGE | **OPEN** | If we re-run later with updated SF model, need to keep `llm_model` tag to distinguish vintages per `feedback_llm_model_tag` |

---

## §6 — Surgical git add

```
qc_framework_v1/migrations/281_nlp_sf_to_md_promotion_20260503.sql
scripts/mig_281_nlp_promotion.py
scripts/output/mig_281_apply_log.txt
scripts/output/mig_281_coverage_uplift_report.csv
cursor_prompts/CURSOR_PROMPT_MIG_281_NLP_SF_TO_MD_PROMOTION_20260503.md
```

Commit message:
```
feat(md): mig_281 SF→MD NLP canonical promotion (smoking + family_hx + vasc invasion)

- Pulled SF AI_CLASSIFY results from THYROID_VALIDATION.PUBLIC.NLP_*_RESULTS_v1
- Appended to MD note_entities_llm_pmhx + note_entities_llm_invasion_v2
- Rebuilt CPM cols pmhx_nlp_smoking_status, pmhx_nlp_family_hx_thyroid, vascular_invasion_final
- Coverage uplift: smoking 13→~3,300; family_hx 163→~3,000; vasc 1,172→~1,750
- llm_model = AI_CLASSIFY_snowflake_cortex_20260503 (Cowork pipeline)
- Closes CF-VASC-INVASION-749-UNDERFIRES + CF-SMOKING-COVERAGE-GAP + CF-FAMILY-HX-COVERAGE-GAP at canonical level
- Replaces deprecated mig_272 H200/Vast.ai path per feedback_nlp_refresh_on_snowflake.md
```

---

**End of mig_281 dispatch.**
