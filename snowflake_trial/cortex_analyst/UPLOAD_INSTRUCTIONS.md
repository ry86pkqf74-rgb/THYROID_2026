# Cortex Analyst — Upload Instructions

This is a "talk-to-your-data" semantic model that lets you (and co-PIs) ask plain-English clinical questions and get publication-quality SQL + result tables back. Maps to Plan §4.4 deliverable.

## What this gets you

After uploading, you can ask Cortex Analyst questions like:
- "What's the malignancy rate by Bethesda category?"
- "How many Graves patients had PTC?"
- "Show recurrence rates by ETE group, stratified by surgery type"
- "What percentage of BRAF-positive patients received RAI?"
- "Compare tumor size between hemithyroidectomy and total thyroidectomy"
- "What's the LN positivity rate by tumor size bucket?"
- "Show BRAF positivity over time"

…and get back validated SQL + result tables, no Python or SQL knowledge needed.

## How to upload (one-time setup)

1. Open Snowsight: https://app.snowflake.com/us-east-1/qcc02515/
2. Navigate: **AI & ML → AI Studio → Cortex Analyst → Try**
3. Click **+ New semantic model** (or **Create from YAML** if available)
4. Upload `thyroid_2026_semantic_model.yaml` from this directory
5. Snowsight will validate + show any schema mismatches
6. If validation passes: name it **Thyroid 2026 Publication Model** and save

## How to use it after upload

- Click **Test in chat** in the Cortex Analyst UI
- Type a question — Analyst returns SQL + a result table
- Click the SQL panel to copy/paste into a worksheet for further refinement
- "Verified queries" (the ones below the dimensions/measures in the YAML) are auto-suggested when the question matches

## Semantic-model design notes

**Dimensions** are fields you can group/filter by — sex, race, histology_group, ajcc8_stage_group, surgery_type, ete_grade, surgery_era, autoimmune_type, ln_status_source, etc.

**Measures** are pre-computed metrics — total_patients, malignancy_rate, mean_age, recurrence_rate, rai_rate, braf_positivity_rate, etc.

**Verified queries** are pre-approved SQL for common questions; Analyst uses them as templates and adapts them to user phrasing.

**Cohort views** (`cohort_m004_*`, `cohort_m032_*`, `cohort_m037_*`) are pre-filtered subsets — you can ask "in M037 cohort, what's the LN positivity rate?" and Analyst will route through that view automatically.

## Re-uploading after data changes

If you re-export from MotherDuck → Snowflake (e.g. after a new mig lands), the YAML doesn't need to change unless you've added/removed columns. Re-validate via Snowsight if you want to confirm. The verified queries reference column names directly, so a column rename in CPM = update the YAML.

## When to update this YAML

- After mig_260 lands (TIRADS column re-point) → update `tirads_max_category` to point to `canonical_us_patient_master_VIEW_v2.max_tirads_category_ever`
- After mig_263 lands (AJCC overlay re-derive Option A) → update `ajcc8_stage_group` description if IVA/IVC become available
- When new manuscript cohorts get views in Snowflake → add a new `tables:` entry

## Cost

Cortex Analyst billed per query (LLM tokens for question→SQL translation). Plan budget: ~$0.05–0.20 per question on Llama 3.1 70B (default). Set `--model llama3.1-8b` if you need to drop cost; `claude-sonnet-4` if you need stronger reasoning on a complex question.
