# Snowflake AI Infrastructure — Deployed Components

**Deployed by Cowork:** 2026-05-04
**Deploy script:** `snowflake_trial/scripts/sf_infrastructure_deploy_v2.py`
**Account:** `qcc02515.us-east-1` / DB `THYROID_VALIDATION` / Schema `PUBLIC`

---

## What's running on Snowflake (15 components)

### Flat-projection views (cohort + CPM)
- `CANONICAL_PATIENT_MASTER_FLAT` — VARIANT $1 → flat (10,871 rows)
- `COHORT_M044_AJCC_ETE_V1_FLAT` — 4,013 / 35 cols
- `COHORT_M037_LN_METASTASIS_V1_FLAT` — 2,234 / 43 cols
- `COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT` — 3,375 / 23 cols
- `COHORT_M032_DESCRIPTIVE_25YR_V1_FLAT` — 10,871 / 37 cols
- `COHORT_M038_MASSIVE_GOITER_V1_FLAT` — 10,871 / 31 cols
- All canonical_*_v1 events tables (8 tables) — see CANONICAL_*_FLAT views

### NLP results (AI_CLASSIFY full corpus)
- `NLP_SMOKING_FULL_RESULTS_v1` — 3,541 notes, 92.7% actionable
- `NLP_FAMILY_HX_THYROID_FULL_RESULTS_v1` — 3,534 notes, 84% actionable
- `NLP_VASC_INVASION_FULL_RESULTS_v1` — 806 notes, 52% actionable

### Validation infrastructure
- `VALIDATION_RUN_LOG_v1` (table) — audit log with run history
- `VALIDATE_ALL_COHORTS()` (procedure) — auto-validate 10 cross-manuscript cells; INSERT to log + RETURN results
- `COHORT_SUMMARY_DASHBOARD` (view) — cross-manuscript cohort sizes at a glance

### Search + AI
- `THYROID_NOTES_SEARCH` (Cortex Search service) — semantic search over 1,000-note sample of clinical notes (expandable)
- `CLINICAL_NOTES_SEARCH_V1` (table) — backing data for search service
- `@SEMANTIC_MODELS/thyroid_2026_semantic_model.yaml` — Cortex Analyst semantic model staged; bind via Snowsight UI

### Pipeline manifest
- `COWORK_PIPELINE_REGISTRY_V1` (table) — registry of all Cowork-deployed components

---

## How Logan uses these

### 1. Cortex Analyst (talk-to-your-data)
**Setup (one-time, ~3 min):**
1. Open Snowsight → AI & ML → Cortex Analyst
2. Click **+ New semantic model** → from stage
3. Pick `@SEMANTIC_MODELS/thyroid_2026_semantic_model.yaml`
4. Name it `Thyroid 2026 Publication Model`

**Then:** Type questions like "What's the malignancy rate by Bethesda category?" or "How does ETE relate to recurrence?" — get back validated SQL + result tables.

### 2. Cohort Summary Dashboard
```sql
SELECT * FROM THYROID_VALIDATION.PUBLIC.COHORT_SUMMARY_DASHBOARD;
```
Sub-second answer: cohort sizes + event counts for all 5 manuscripts + the full publication cohort.

### 3. Repeatable validation
```sql
CALL THYROID_VALIDATION.PUBLIC.VALIDATE_ALL_COHORTS();
```
Re-runs 10 cross-manuscript denominator checks. Returns table of PASS/FAIL + writes to `VALIDATION_RUN_LOG_v1` for audit history. Run anytime as a sanity check after MD migration rounds.

### 4. Cortex Search (semantic note search)
```sql
SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'THYROID_NOTES_SEARCH',
  '{"query": "lateral neck dissection lymph node positive", "limit": 5}'
));
```
Returns top-N most semantically similar notes from the indexed corpus.

### 5. Pipeline registry
```sql
SELECT * FROM THYROID_VALIDATION.PUBLIC.COWORK_PIPELINE_REGISTRY_V1 ORDER BY KIND, COMPONENT;
```
Quick "what did Cowork deploy?" lookup.

---

## Cost notes

- Cortex AI SQL functions (AI_CLASSIFY): ~$0.0001 per row, batched per table
- Cortex Search query: ~$0.001-0.01 per query (LLM tokens)
- Cortex Analyst query: ~$0.05-0.20 per question (LLM tokens for Q→SQL translation)
- Stored procedures + views: free at our scale

Trial credit budget remaining sufficient for full M032/M037 packages + ongoing validation runs.

---

## Maintenance

The infrastructure is reproducible from `sf_infrastructure_deploy_v2.py`. After any MD migration round that changes cohort views or canonical schemas, re-run:
```bash
cd "/Users/ros/THyroid 2026"
source .venv/bin/activate
export SNOWFLAKE_PAT=...
export MOTHERDUCK_TOKEN=$(grep MD_SA_TOKEN motherduck.local.toml | sed 's/.*"\(.*\)".*/\1/')
python snowflake_trial/scripts/01_export_md_to_parquet.py
python snowflake_trial/scripts/02_load_to_snowflake.py
python snowflake_trial/scripts/04_build_flat_views.py
python snowflake_trial/scripts/sf_infrastructure_deploy_v2.py
# Then sanity check:
# CALL VALIDATE_ALL_COHORTS();
```

---

**Status:** ALL 15 COMPONENTS DEPLOYED + VERIFIED 2026-05-04.
