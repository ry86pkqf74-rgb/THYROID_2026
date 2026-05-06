# MIG-003 — VC paresis re-validation (2026-05-06) — **COMPLETE (repopulation path)**

## Governance

- **DFL:** DFL-20260506-082 (before BQ DML).
- **BQ migration log:** `mig_081_mig003_vc_paresis_ai_classify_bq_20260506` (supersedes incomplete Step 1 log `mig_080`).
- **SQL artifact (repo):** `sql/mig_081_mig003_vc_paresis_bq_update_20260506.sql`.
- **No PHI** in this file (no note bodies; `research_id` only where needed for audit).

## Findings

### Cortex `SEARCH_PREVIEW`

`SEARCH_PREVIEW` on `THYROID_VALIDATION.PUBLIC.THYROID_NOTES_SEARCH` returns **only** `NOTE_TEXT` + scores — **no `research_id` in results** despite service `ATTRIBUTES`. All screening for patient keys used **`CLINICAL_NOTES_SEARCH_V1`**.

### Note corpus (Snowflake SQL, LIKE screens)

| Metric | Value |
|--------|------:|
| Distinct patients with paresis/paretic + (vocal/fold/cord/laryngeal) in any note | 65 |
| Distinct patients with explicit “without / no / not … paralysis” + paresis language | 13 |
| Of those 13, `comp_vc_paralysis_confirmed = TRUE` on BigQuery CPM | 1 (`research_id` **9012**) — excluded from AI_CLASSIFY promotion batch |

### AI_CLASSIFY (Snowflake Cortex)

**Scope:** Notes matching contrast-language filter for **12** patients with **no** CPM paralysis (`research_id` ∈ {7175, 7290, 8088, 8159, 8616, 8692, 8894, 9119, 9764, 9905, 11108, 11915}).

Classes: `clinical_paresis_distinct_from_paralysis`, `consent_template_or_risk_list`, `describes_paralysis_not_paresis`, `unclear`.

| Label | Note rows |
|-------|----------:|
| `describes_paralysis_not_paresis` | 12 |
| `clinical_paresis_distinct_from_paralysis` | 1 |

**Distinct patient with clinical paresis (AI_CLASSIFY):** **1** → `research_id` **8616**.

### BigQuery DML

- **Dry-run** (`UPDATE` one row): upper bound **~46,095,307** bytes processed.
- **Applied:** `comp_vc_paresis_confirmed = TRUE`, `comp_vc_paresis_evidence_tier = 2` where `research_id = '8616'` and paralysis not confirmed.
- **Post-state:** `COUNTIF(comp_vc_paresis_confirmed)` = **1** on full CPM (10,871 rows).

### Decision

**Step 2B (repopulation)** — not deprecation. Standing-rule memory and H2 v2 Limitations §8 updated accordingly.

## Artifacts (aggregate JSON, repo)

- `_scripts/mig003_sf_note_agg.json` — early LIKE aggregation (superseded by refined file).
- `_scripts/mig003_sf_note_agg_refined.json` — consent-heuristic refinement.
- `_scripts/mig003_ai_classify_agg.json` — final label counts + clinical rid list.

## Operator note — authentication

Snowflake access used **`SNOWFLAKE_PAT_FILE`** (local one-line PAT, not committed). Do not store PAT paths or token values in git or Airtable notes.
