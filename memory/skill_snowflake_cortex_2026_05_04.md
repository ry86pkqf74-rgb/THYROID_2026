# Snowflake Cortex stack — usage skill (Logan, 2026-05-04)

**Trigger:** any task involving Snowflake AI features (Analyst, Search, Agents, Code), running NL→SQL on the publication semantic model, validating cohort numbers across MD↔SF, or running long-running data work where Logan wants Claude-style AI assistance directly inside Snowflake.

**Account:** `qcc02515.us-east-1` / DB `THYROID_VALIDATION` / Schema `PUBLIC` / WH `COMPUTE_WH`
**User:** `LGLOSSE13` / Role `ACCOUNTADMIN`
**Auth:** Programmatic Access Token (PAT). Generated in Snowsight → User profile → Settings → Programmatic access tokens. Cortex-only PATs CAN'T do generic warehouse SQL — they'll 250001-fail on `snow connection test`. Generic-scope PATs work everywhere.

## Local CLIs (installed 2026-05-04)
- `snow` (Snowflake CLI 3.16.0) at `/usr/local/bin/snow` — generic SQL + connection mgmt. Config at `~/.snowflake/config.toml`, default conn `[connections.thyroid_2026]`.
- `cortex` (Cortex Code 1.0.73+180523) at `~/.local/bin/cortex` — Claude-Opus/Sonnet 4.6 backed agent. Path is appended to `.zshrc` and `.bash_profile`.

## Cortex Code CLI surface (the high-value parts)

| subcommand | what it does | when to use |
|---|---|---|
| `cortex` | one-shot or interactive chat against Cortex models | quick SQL synthesis, doc Q&A, repo nav |
| `cortex analyst` | NL→SQL against staged semantic models | answering "what's the malignancy rate by Bethesda?" — needs a model bound in Snowsight first |
| `cortex search` | semantic search over Snowflake objects + Cortex Search services | finding clinical-note evidence; we have `THYROID_NOTES_SEARCH` over the full 11,050-note corpus |
| `cortex agents` | list/use Cortex Agents (built in Snowflake AI Studio) | running multi-step reasoning agents on Snowflake-side data |
| `cortex mcp` | manage MCP servers (Cortex Code IS an MCP server itself) | wire Cortex into Cursor/Claude Code as an MCP |
| `cortex acp` | Agent Client Protocol — interop with other AI clients | Cursor↔Cortex bridge |
| `cortex skill` | manage skill directories | install/load skill bundles for cortex |
| `cortex ctx` | long-term AI memory + task management | persistent project context across sessions |
| `cortex worktree` | manage git worktrees | parallel branches |
| `cortex resume [--last]` | resume previous session | continue a prior cortex chat thread |
| `cortex airflow` | Airflow orchestration | not used here yet |
| `cortex create-ui-launcher` | build the macOS desktop UI launcher | optional GUI |

## Snowflake AI infrastructure currently deployed (per `SF_INFRASTRUCTURE_REGISTRY.md`)

- **Cortex Search service:** `THYROID_NOTES_SEARCH` — full 11,050-note corpus indexed for semantic search.
- **Semantic model:** `@SEMANTIC_MODELS/thyroid_2026_semantic_model.yaml` — staged but **needs Snowsight UI bind** before Cortex Analyst can use it. After binding, `cortex analyst -m thyroid_2026 "what's the M025 malignancy rate by TR category?"` should work.
- **Validation SP:** `CALL VALIDATE_ALL_COHORTS()` — sub-second; runs 17 baseline checks (M025/M037/M044/M032/M038/CPM denominators + NLP coverage + manuscript cells + NLP scale). Writes to `VALIDATION_RUN_LOG_v1` (mirrored to MD as `main.cowork_sf_validation_log_v1` via mig_293b).
- **Dashboard view:** `COHORT_SUMMARY_DASHBOARD` — at-a-glance cohort sizes.
- **Pipeline registry:** `COWORK_PIPELINE_REGISTRY_V1` — which Cowork-built components live where.
- **NLP results from Snowflake AI_CLASSIFY (mig_281):** `NLP_SMOKING_FULL_RESULTS_v1` (3,541), `NLP_FAMILY_HX_THYROID_FULL_RESULTS_v1` (3,534), `NLP_VASC_INVASION_FULL_RESULTS_v1` (806). This pattern (AI_CLASSIFY in SF, then promote to MD canonicals) is the standard NLP path going forward — supersedes the deprecated mig_272 Vast.ai/H200 batch.

## Reusable patterns

### Pattern A: cross-source validation (MD ↔ SF)
After any MD migration that touches a cohort:
```
# 1. Refresh SF flat views
python snowflake_trial/scripts/01_export_md_to_parquet.py
python snowflake_trial/scripts/02_load_to_snowflake.py
python snowflake_trial/scripts/04_build_flat_views.py
python snowflake_trial/scripts/sf_infrastructure_deploy_v2.py

# 2. Run baseline (must be 17/17 PASS)
snow sql -c thyroid_2026 -q "CALL VALIDATE_ALL_COHORTS();"

# 3. Mirror the run log back to MD
python snowflake_trial/scripts/35_pull_sf_validation_log.py --md
```

### Pattern B: Cortex Analyst NL→SQL for ad-hoc manuscript questions
After binding the semantic model in Snowsight, instead of writing SQL by hand:
```
cortex analyst "What is the malignancy rate by ACR TI-RADS category in the M025 cohort, stratified by Bethesda?"
```
Returns SQL + executes against the warehouse + returns table. Use for figure exploration before writing the canonical SQL into a build script.

### Pattern C: Cortex Search for clinical-note evidence retrieval
For any manuscript that needs note-level evidence (e.g., "find notes describing extrathyroidal extension of cricoid"):
```sql
SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'THYROID_NOTES_SEARCH',
  '{"query": "<your query>", "limit": 10}'
));
```
Returns top-N semantically similar notes. Use to back manuscript claims with citations and to discover edge cases.

### Pattern D: AI_CLASSIFY for NLP gap closure
When a manuscript needs an NLP-derived field that's NULL on most patients:
```sql
CREATE OR REPLACE TABLE NLP_<DOMAIN>_FULL_RESULTS_v1 AS
SELECT note_id, research_id,
       AI_CLASSIFY(note_text, ['<class_1>', '<class_2>', '<class_3>']) AS classification
FROM CLINICAL_NOTES_SEARCH_V1
WHERE <filter_relevant_notes>;
```
Then promote to MD canonical with a signed mig (pattern: mig_281, 287, 298).

## Auth troubleshooting (saw 2026-05-04)
- `250001 (08001) Programmatic access token is invalid` — token expired/rotated/wrong-user. Regenerate.
- `290404 (08001) 404 Not Found ... session/v1/login-request` — account name missing region. Use `qcc02515.us-east-1` not `qcc02515`.
- Cortex-only PATs: silently 250001 on warehouse SQL. To use both warehouse SQL and Cortex, generate the PAT with the role `ACCOUNTADMIN` (or a custom role with USAGE on WH + DB + SCHEMA).

## Known carry-forwards
- **CF-NODULE-FNA-V2-KEYS** (this session): `imaging_fna_linkage_v3` uses legacy nodule_id format (`4131-US-1-2`) but `canonical_us_nodule_v2` uses MD5 hashes — zero overlap. Bridge via (research_id + exam_date ±30d + laterality). Recovers ~70% of FNA links.
- **CF-FNA-SIZE-CM-NULL**: `imaging_fna_linkage_v3.fna_size_cm` is NULL by design in v1_0; size_score is a flat 0.5 prior. v1_1 task: NLP-extract from `note_entities_llm_us_nodule_dynamics` / `note_entities_llm_tirads_granular`.
- **CF-CORTEX-ANALYST-NEEDS-BIND**: semantic model staged but not yet bound in Snowsight UI; can't use `cortex analyst` until done.
- **CF-MIG_305**: `VALIDATE_ALL_COHORTS()` SP v3 hangs on `information_schema.columns` sentinel inside SP context. Currently SP at v1 (10 checks) — running v2 (17 checks). Fix: remove information_schema check from inside SP body; run it as a side query.

## File pointers
- Repo root: `/Users/loganglosser/THYROID_2026`
- `snowflake_trial/scripts/_sf_client.py` — shared PAT-auth client (handles connector v4.4.0 quirk)
- `snowflake_trial/SF_INFRASTRUCTURE_REGISTRY.md` — what's deployed on SF
- `snowflake_trial/scripts/27_m025_tirads_performance.py` — the M025 build script
- `~/.snowflake/config.toml` — snow CLI default conn `[connections.thyroid_2026]`
- `.env` — `SNOWFLAKE_PAT=...` and `MOTHERDUCK_TOKEN=...`
