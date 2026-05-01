# Snowflake Cortex Trial — Operational Scaffold

**Status (2026-05-01):** Foundation working end-to-end. CPM + 8 detail tables loaded; AI_COMPLETE/AI_CLASSIFY/AI_FILTER confirmed working on real cohort data.

## What is set up

### Snowflake account
- Account: `qcc02515.us-east-1` (region: AWS_US_EAST_1)
- User: `LGLOSSE13` (default role ACCOUNTADMIN, default warehouse COMPUTE_WH)
- Database: `THYROID_VALIDATION` (created via Snowsight worksheet)
- Schema: `PUBLIC`
- Stage: `THYROID_VALIDATION.PUBLIC.thyroid_stage` (internal; Parquet uploads land here)
- File format: `THYROID_VALIDATION.PUBLIC.parquet_fmt` (Parquet, vectorized scanner ON)
- Authentication policy: `THYROID_VALIDATION.PUBLIC.allow_pat_policy` assigned to LGLOSSE13 — allows PAT auth alongside password/SAML/OAuth/keypair for SNOWFLAKE_UI/DRIVERS/SNOWSQL clients

### Loaded tables
All 9 tables in `THYROID_VALIDATION.PUBLIC` loaded as VARIANT $1 column (CTAS from staged Parquet preserves Parquet types but not column projection).

| Table | Rows |
|---|---|
| CANONICAL_PATIENT_MASTER | 10,871 |
| CANONICAL_FNA_EVENTS_V1 | 8,050 |
| CANONICAL_INVASION_EVENTS_V1 | 58,582 |
| CANONICAL_LABS_THYROGLOBULIN_V1 | 53,006 |
| CANONICAL_MOLECULAR_GENETICS_V2 | 1,384 |
| CANONICAL_OPERATIVE_EVENTS_V1 | 11,773 |
| CANONICAL_PATH_GLAND_EVENTS_V1 | 28,724 |
| CANONICAL_PATH_MALIGNANT_EVENTS_V1 | 6,469 |
| CANONICAL_COMPLICATIONS_EVENTS_V1 | 5,050 |

Field access pattern: `$1:column_name::TYPE`, e.g. `$1:age_at_surgery::INT`, `$1:histology_final::VARCHAR`, `$1:is_malignant::BOOLEAN`.

## Critical workarounds discovered

### 1. PAT auth requires authentication policy
Trial accounts default to MFA-enforced auth which rejects PATs. Must run (Snowsight worksheet, ACCOUNTADMIN):
```sql
CREATE OR REPLACE AUTHENTICATION POLICY allow_pat_policy
  AUTHENTICATION_METHODS = ('PASSWORD','PROGRAMMATIC_ACCESS_TOKEN','SAML','OAUTH','KEYPAIR')
  CLIENT_TYPES = ('SNOWFLAKE_UI','DRIVERS','SNOWSQL');
ALTER USER LGLOSSE13 SET AUTHENTICATION POLICY allow_pat_policy;
```

### 2. Python connector v4.4.0 has a PAT auth bug
`PROGRAMMATIC_ACCESS_TOKEN` authenticator leaves `TOKEN` body field empty AND strips region from `ACCOUNT_NAME`. Without patching, all PAT auth fails as "invalid token". Fix in `scripts/02_load_to_snowflake.py`:

```python
import snowflake.connector.network as _net
_orig_post = _net.SnowflakeRestful._post_request
def _patched(self, url, headers, body, *args, **kwargs):
    if "/session/v1/login-request" in url:
        d = json.loads(body) if isinstance(body, str) else json.loads(body.decode())
        d["data"]["ACCOUNT_NAME"] = "qcc02515.us-east-1"  # restore region
        if not d["data"].get("TOKEN"):
            d["data"]["TOKEN"] = PAT  # the connector left it empty
        body = json.dumps(d)
    return _orig_post(self, url, headers, body, *args, **kwargs)
_net.SnowflakeRestful._post_request = _patched
```

### 3. `cortex --print` blocked on trial accounts
Cortex Code CLI's headless print mode is paid-tier only. AI SQL functions (`AI_COMPLETE`, `AI_CLASSIFY`, `AI_FILTER`, `AI_AGG`) **work fine on trial** when invoked via SQL through the connector or Snowsight. The original plan's bulk-validation strategy survives by running AI as SQL, not via cortex CLI.

### 4. CTAS from stage is the right load path
INFER_SCHEMA + USING TEMPLATE silently drops rows on type-cast errors. Direct `CREATE TABLE foo AS SELECT * FROM @stage/file.parquet (FILE_FORMAT => 'parquet_fmt')` preserves all rows in a VARIANT column. To get flat columns, project explicitly per query.

## Cortex AI Studio paths (Logan-facing, no CLI needed)

All accessible from Snowsight at https://app.snowflake.com/us-east-1/qcc02515/

### Cortex Playground
*Use for:* ad-hoc LLM prompts on a small piece of data — recurrence text extraction trial, plausibility checks of edge cases, manuscript abstract drafting.
*Path:* AI & ML → AI Studio → Cortex Playground
*Why for us:* the lowest-friction way to iterate on prompts before scaling them to SQL.

### Cortex Analyst
*Use for:* "talk to your data" — co-PIs ask plain-English questions and get SQL + results. Maps directly to Plan §4.4.
*Path:* AI & ML → AI Studio → Cortex Analyst
*Setup:* upload `thyroid_2026_semantic_model.yaml` (template in Plan §4.4) defining tables, dimensions, measures, verified queries.
*Why for us:* the deliverable in the plan that lets non-technical collaborators (Logan, co-PIs, manuscript reviewers) explore the cohort.

### Cortex Search
*Use for:* keyword + semantic search over text columns (NLP entity text, operative notes, pathology free-text).
*Path:* AI & ML → AI Studio → Cortex Search
*Why for us:* faster lookup over the 5 integrated NLP domain text columns than full table scans for ad-hoc clinical questions.

### Cortex Agents
*Use for:* an agent that can chain Cortex Analyst + Cortex Search + AI_COMPLETE to answer multi-step clinical questions.
*Path:* AI & ML → AI Studio → Cortex Agents
*Why for us:* Phase 4 deliverable — a manuscript-feasibility agent that scans the 83 manuscripts and reports which are publication-ready.

### Document Processing Playground
*Use for:* AI_EXTRACT and AI_PARSE_DOCUMENT trial runs on PDFs/scans.
*Path:* AI & ML → AI Studio → Document Processing Playground
*Why for us:* if any pathology reports remain as scanned PDFs, this is the cheapest way to OCR + extract structured fields.

### Cortex Playground vs. AI SQL functions
- **Playground** = exploratory, one prompt at a time, good for prompt design.
- **AI SQL functions** (`AI_COMPLETE`, `AI_CLASSIFY`, `AI_FILTER`, `AI_AGG`) = production, run across all rows, results are tables. Use Playground to design the prompt, then move it into SQL for the cohort run.

## Scripts

| Script | Purpose | Status |
|---|---|---|
| `scripts/01_export_md_to_parquet.py` | Pull canonical tables from MotherDuck → local Parquet | Working (9/12 tables; 3 skipped due to name mismatches) |
| `scripts/02_load_to_snowflake.py` | PUT + CTAS load into Snowflake | Working — auth-patched |
| `scripts/03_run_validation_prompt1.py` | Demographics + Table 1 + AI_CLASSIFY + AI_FILTER on CPM | Working — see `reports/01_demographics_validation.md` |

## Validation prompt status

Maps to Plan §2 (data quality) and §3 (cross-validation).

| # | Prompt | Status | Output |
|---|---|---|---|
| 1 | Demographics + Table 1 | DONE | `reports/01_demographics_validation.md` |
| 2 | Molecular testing audit | TEMPLATE READY (extend script 03) | — |
| 3 | Survival/recurrence integrity | TEMPLATE READY | — |
| 4 | RAI/Tg kinetics + FORECAST | NEEDS Tg time-series prep | — |
| 5 | LN/staging cross-validation | TEMPLATE READY | — |
| 6 | ETE/vascular invasion | TEMPLATE READY | — |
| 7 | TIRADS/FNA/Bethesda | TEMPLATE READY | — |
| 8 | Complication rates + AI_AGG | TEMPLATE READY | — |
| 9 | Voice/NLP cross-validation | NEEDS NLP cols | — |
| 10 | Imaging multi-modal | NEEDS imaging tables | — |
| 11 | Comorbidity/PMH | TEMPLATE READY | — |
| 12 | Synoptic pathology | TEMPLATE READY | — |
| 13–21 | Detail-table cross-validation | DEPENDS on schema flatten | — |

## Outstanding work (Logan punch list)

1. **Schema flatten** — current tables are VARIANT $1; to enable Cortex Analyst semantic model and cleaner queries, run a CTAS pass that projects each $1:field::TYPE into a named column. Templates in Plan §4.4 assume flat columns.
2. **Add 3 missing tables** — `canonical_rai_episodes_v1`, `canonical_us_exam_events_v1`, `canonical_recurrence_events_v1` weren't found by their guessed names. Discover the actual names with:
   ```python
   con.sql("SELECT table_name FROM duckdb_tables() WHERE database_name='thyroid_canonical_publication_v1_0' AND table_name ILIKE '%recurrence%'").fetchall()
   ```
   then add to `TABLES` list in `01_export_md_to_parquet.py` and re-run.
3. **Build remaining validation prompts 2–12** — copy the pattern in `03_run_validation_prompt1.py`. Each prompt is a SELECT + `AI_*(...)` + markdown writer.
4. **Cortex Analyst semantic model** — author `semantic_model.yaml` per Plan §4.4 and upload via Snowsight UI. Logan + co-PIs can then query in plain English.
5. **MFA/security** — once trial wraps, decide whether to drop `allow_pat_policy` or formalize a PAT-based service-account workflow.
6. **Cancel before Day 30** — trial converts to ~$25/mo subscription. To cancel: Admin → Account → Manage Subscription. Set a calendar reminder for 2026-05-29.

## Auth credentials note

PAT used in scripts: `CLAUDE_SANDBOX_PAT` (created 2026-05-01, expires 2026-05-08). For longer-lived auth, generate a 30-day PAT via Snowsight (Admin → Users & Roles → LGLOSSE13 → Generate token) and update `SNOWFLAKE_PAT` env var. **PAT secret is never persisted in the repo** — pass via env var only.

## Cost tracking

To monitor credit consumption:
```sql
SELECT DATE(START_TIME) AS day,
       SERVICE_TYPE,
       SUM(CREDITS_USED) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE START_TIME > DATEADD(day, -7, CURRENT_TIMESTAMP)
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
```
Run via Snowsight worksheet or by adding to a script.
