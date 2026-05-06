# Manuscript Jupyter notebooks (BigQuery)

This folder holds reusable notebook templates that pull **parameterized** cohort analytics from the publication BigQuery project **`thyroid-canonical-pub-2026`**, using the same service-account pattern as the MotherDuck→GCP migration.

## Prerequisites

1. **Python env:** repo `.venv` with `pandas`, `matplotlib`, `pyarrow`, and `google-cloud-bigquery` (see root `requirements.txt`).
2. **Service account key:** JSON for `thyroid-pub-loader@thyroid-canonical-pub-2026.iam.gserviceaccount.com`, stored under the **migration** repo as `_creds/thyroid-pub-loader-key.json` (not copied into THYROID_2026).
3. **Authentication:** either:
   - `export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/.../migration/_creds/thyroid-pub-loader-key.json"` before starting Jupyter, or  
   - Rely on the template default path if your migration checkout matches `~/Desktop/Thyroid Motherduck To GC migration/` (adjust in notebook cell 1 if not).

## Template: `manuscript_notebook_v1.ipynb`

**Default manuscript:** `M025` (TI-RADS cohort). Intended to **Run All** with no edits when credentials resolve.

**What it does**

| Step | Description |
|------|-------------|
| Setup | BigQuery client, `MANUSCRIPT_CODE`, PHI-safe output paths under `studies/<CODE>/` |
| Metadata | Join `pub_workspace.manuscript_feasibility_v1` × `pub_workspace.manuscript_dive_map_v1` |
| Cohort | `SELECT *` from `pub_legacy_source_20260416.<cohort_view_name>` |
| Summary | Row counts, distinct `research_id`, categorical demographics (counts only) |
| Missingness | Matplotlib heatmap (top N columns by missing fraction) → `studies/<CODE>/figures/` |
| Outcomes | Incidence tables for `recurrence_status_final` and/or `death_date` when present |
| Snapshot | Append-only JSON line in `studies/<CODE>/cohort_snapshots.jsonl` |

**PHI rules baked in:** only `research_id` as patient key in outputs; no names, no operative narrative, no listing of calendar dates—only coarse summaries (e.g. optional year ranges from numeric year columns, outcome counts).

## Forking for a new manuscript

1. **Copy** `templates/manuscript_notebook_v1.ipynb` → e.g. `studies/M044/M044_bq_eda_v1.ipynb` (keep a clean master copy in `templates/`).
2. Change **`MANUSCRIPT_CODE`** in the first code cell (e.g. `"M044"`). `M###` maps to `manuscript_id` via the numeric suffix (`M025` → `25`).
3. **Run from repo root** (so `studies/<CODE>/...` resolves correctly), or set working directory to repo root in Jupyter.
4. If the cohort view moves out of the frozen legacy dataset, update **`BQ_DATASET_LEGACY`** and the cohort query cell to match the governed location (see `pub_signoff` / migration notes).

## BigQuery object reference

| Purpose | Table / view |
|--------|----------------|
| Feasibility row | `` `thyroid-canonical-pub-2026.pub_workspace.manuscript_feasibility_v1` `` |
| Cohort view name | `` `thyroid-canonical-pub-2026.pub_workspace.manuscript_dive_map_v1` `` |
| Frozen cohort slice | `` `thyroid-canonical-pub-2026.pub_legacy_source_20260416.<cohort_view_name>` `` |

M025 expected view name (audit reference): `cohort_m025_tirads_performance_v1`.

## Troubleshooting

- **`DefaultCredentialsError`:** set `GOOGLE_APPLICATION_CREDENTIALS` to the absolute JSON path or fix `_DEFAULT_SA_KEY` in the notebook.
- **Empty metadata:** `manuscript_id` must exist in **both** feasibility and dive-map tables.
- **`404 Not found` on cohort:** cohort view may not be mirrored into `pub_legacy_source_20260416`; check `pub_workspace` / `pub_views_readable` and migration logs.
