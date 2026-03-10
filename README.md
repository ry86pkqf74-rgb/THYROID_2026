# THYROID_2026

## Publication-Ready Release (v2026.03.10)

- **Scripts 13-15 executed:** optimized MVs, sub-second dashboard tabs, full publication bundles
- **Local DuckDB backup:** `thyroid_master_local.duckdb` (use if downgrading MotherDuck)
- **Manuscript tables ready:** `Table1_Cohort_Demographics.csv`, `Table2_Risk_Stratification.csv`, `Table3_Complications.csv`, `Table4_Timeline_Summary.csv`
- **Studies folder:** `studies/proposal2_ete_staging/` (AJCC 8th + ETE + recurrence PSM)
- **Publication bundle:** `exports/THYROID_2026_PUBLICATION_BUNDLE_20260310_0414/`
- **Final release:** `exports/FINAL_RELEASE_v2026.03.10_20260310_0529/`
- **Tag:** [`v2026.03.10-publication-ready`](../../releases/tag/v2026.03.10-publication-ready)

---

Thyroid cancer research lakehouse — 11,673 patients across 13 base tables,
8+ analytic views, and a fully interactive Streamlit dashboard backed by
[MotherDuck](https://motherduck.com) cloud DuckDB.

## Live Dashboard

**[thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app](https://thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app/)**

> The deployed app connects to a read-only MotherDuck share. If the app
> asks you to sign in, the owner must set **Settings > Sharing > Public**
> in the Streamlit Cloud dashboard to allow unauthenticated access.

## Repository layout

```
.
├── dashboard.py              # Streamlit dashboard (main entry point)
├── motherduck_client.py      # MotherDuck connection helper
├── requirements.txt          # Python dependencies
├── runtime.txt               # Python 3.11 pin for Streamlit Cloud
├── .streamlit/
│   ├── config.toml           # Server, theme, and browser settings
│   ├── secrets.toml.example  # Template — copy to secrets.toml
│   └── secrets.toml          # (gitignored) your real token
├── .github/workflows/
│   └── ci.yml                # CI: syntax + MotherDuck smoke test
├── scripts/                  # ETL and view-creation scripts
├── notebooks/                # Jupyter exploration notebooks
├── exports/                  # Publication-ready CSV exports
├── processed/                # DVC-tracked parquet files
├── studies/                  # Per-proposal analysis folders
├── docs/                     # Documentation (QA report, architecture)
├── data_dictionary.md        # Full schema documentation
└── RELEASE_NOTES.md          # Publication release notes
```

## Quick start (local)

```bash
# 1. Clone
git clone https://github.com/ry86pkqf74-rgb/THYROID_2026.git
cd THYROID_2026

# 2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set your MotherDuck token (one of two ways)

#    Option A — environment variable:
export MOTHERDUCK_TOKEN='your_motherduck_token'

#    Option B — Streamlit secrets file:
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
#    then edit .streamlit/secrets.toml and paste your real token

# 5. Launch
streamlit run dashboard.py
```

> **After any view change**, run `python scripts/03_research_views.py` locally
> before pushing so that MotherDuck has the updated views.

Open **http://localhost:8501** in your browser.

## MotherDuck data source

| Property           | Value |
|--------------------|-------|
| Database           | `thyroid_research_2026` |
| Read-only share    | `md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c` |
| Patients           | 11,673 |
| Base tables        | 13 |
| Analytic views     | 8+ (ptc_cohort, recurrence_risk_cohort, advanced_features_view, etc.) |

Anyone with a valid MotherDuck token can connect to the read-only share.
The share is SELECT-only — data cannot be modified through it.

## Dashboard features

- **Overview** — 12 key metrics + data completeness by surgery year + date rescue KPI
- **Data Explorer** — full `advanced_features_v3` with column selector, sidebar
  filters (histology, BRAF, parathyroid, sex, age), and CSV download
- **Visualizations** — interactive Plotly charts: histology distribution,
  AJCC 8th stage, sex distribution, parathyroid findings
- **Advanced** — mutation flags (BRAF/RAS/RET/TERT/NTRK/ALK), RAI avidity
  breakdown, benign phenotypes, recurrence risk bands
- **Extraction Completeness** — V2 extractor field fill rates across domains
- **Molecular Episodes** — canonical molecular test analytics with linkage quality
- **RAI Episodes** — RAI treatment analytics with assertion status and interval classes
- **Imaging & Nodules** — nodule-level analytics with imaging-pathology concordance
- **Operative Detail** — operative episode enrichment from V2 extractors
- **QA & Adjudication** — cross-domain QA summary, linkage quality, date completeness
- **Validation Engine** — adjudication confirmations, chronology anomalies,
  completeness scorecard, combined review queue, domain-level downloads

## CI / CD

| Component | Detail |
|-----------|--------|
| GitHub Actions | `.github/workflows/ci.yml` — syntax check + MotherDuck smoke test |
| GitHub Secret | `MOTHERDUCK` — used by CI for live query validation |
| Streamlit Cloud | Auto-deploys from `main` branch on push |
| Runtime | Python 3.11 (pinned in `runtime.txt`) |

## Streamlit Cloud deployment (already done)

The app is deployed at
**[thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app](https://thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app/)**.

To redeploy or reconfigure:

1. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub.
2. Find the app in your dashboard.
3. **Settings > Secrets** — update `MOTHERDUCK_TOKEN` if the token rotates.
4. **Settings > Sharing** — set to **Public** if you want unauthenticated access.
5. Pushes to `main` auto-redeploy the app.

## Making the app public

By default, apps from private repos require Streamlit Cloud sign-in to view.
To allow anyone to access the dashboard without signing in:

1. Open [share.streamlit.io](https://share.streamlit.io)
2. Click the **three-dot menu** on your app
3. Select **Settings > Sharing**
4. Change from **Only people with access** to **Public**
5. Save

## New Dashboard Features (enabled during MotherDuck trial)

Five new tabs added by `scripts/12_update_streamlit_dashboard.py`:

| Tab | Description |
|-----|-------------|
| **Patient Timeline Explorer** | Per-patient surgery timeline, Tg/TSH trend with surgery markers, all clinical events anchored by relative days |
| **Extracted Clinical Events** | Searchable table of labs, meds, PMH, RAI, recurrence from `extracted_clinical_events_v4` with download |
| **QA Dashboard** | Summary metrics from `qa_issues`, severity/check distribution, drill-down table |
| **Risk & Survival** | Kaplan-Meier recurrence-free survival with stratification by stage, histology, BRAF; risk feature summary |
| **Advanced Features v3** | Full column selector across all 60+ engineered features |

**New sidebar filters:** Surgery count, QA status (clean / flagged),
days-since-nearest-surgery range (<30d / 30-90d / 90-365d / >1y).

**Performance controls:** MotherDuck compute tier display, Jumbo instance toggle.

**Publication tools:** "Publication Snapshot" button exports all materialized
views to a dated `exports/snapshot_YYYYMMDD/` folder as CSV + Parquet.
Multi-format download buttons (CSV / Excel / Parquet) on all QA tables.

### Script 11.5 — Cross-File Validation

Run `python scripts/11.5_cross_file_validation.py` after script 11 to create
three cross-file consistency tables:

| Table | Check |
|-------|-------|
| `qa_laterality_mismatches` | Operative vs pathology laterality consistency |
| `qa_report_matching` | FNA↔Pathology and US↔Operative linkage rates |
| `qa_missing_demographics` | Patients missing age, sex, or race |

These tables are displayed in the QA Dashboard tab and their issues are
inserted into `qa_issues` with `check_id` prefix `xfile_`.

Requires `lifelines` (Kaplan-Meier) and `openpyxl` (Excel export).
Install: `pip install -r requirements.txt`.

## Data dictionary

See [data_dictionary.md](data_dictionary.md) for full schema documentation
of all 13 tables and 8+ views.

## License

Private research data — do not redistribute without permission.


## V3 Dashboard Materialization

After deploying the full pipeline (scripts 15–20, 22–27), materialize tables
for the Streamlit dashboard:

```bash
# Materialize all v2/v3 tables to MotherDuck
python scripts/26_motherduck_materialize_v2.py --md

# Run validation engine (creates val_* tables)
python scripts/29_validation_engine.py --md

# Run combined validation + export
python scripts/29_validation_runner.py --md

# Check readiness (exits non-zero if critical tables missing)
python scripts/30_readiness_check.py --md
```

## v2026.03.10 - Publication Release

See [RELEASE_NOTES.md](RELEASE_NOTES.md) for full details.
QA reconciliation report: [docs/QA_report.md](docs/QA_report.md).
