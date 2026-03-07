# THYROID_2026

Thyroid cancer research lakehouse — 11,673 patients across 13 base tables,
8+ analytic views, and a fully interactive Streamlit dashboard backed by
[MotherDuck](https://motherduck.com) cloud DuckDB.

## Repository layout

```
.
├── dashboard.py              # Streamlit dashboard (main entry point)
├── motherduck_client.py      # MotherDuck connection helper
├── requirements.txt          # Python dependencies
├── .streamlit/
│   ├── secrets.toml.example  # Template — copy to secrets.toml
│   └── secrets.toml          # (gitignored) your real token
├── scripts/                  # ETL and view-creation scripts
├── notebooks/                # Jupyter exploration notebooks
├── exports/                  # Publication-ready CSV exports
├── processed/                # DVC-tracked parquet files
├── data_dictionary.md        # Full schema documentation
└── QA_report.md              # Quality assurance report
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

- **Overview** — 12 key metrics + data completeness by surgery year
- **Data Explorer** — full `advanced_features_view` with column selector, sidebar
  filters (histology, BRAF, parathyroid, sex, age), and CSV download
- **Visualizations** — interactive Plotly charts: histology distribution,
  AJCC 8th stage, sex distribution, parathyroid findings
- **Advanced** — mutation flags (BRAF/RAS/RET/TERT/NTRK/ALK), RAI avidity
  breakdown, benign phenotypes, recurrence risk bands

## Deploy to Streamlit Community Cloud (free)

1. **Push this repo to GitHub** (already done — `origin` points to
   `https://github.com/ry86pkqf74-rgb/THYROID_2026.git`).

2. **Go to [share.streamlit.io](https://share.streamlit.io)** and sign in
   with GitHub.

3. **Click "New app"**, then:
   - Repository: `ry86pkqf74-rgb/THYROID_2026`
   - Branch: `main`
   - Main file path: `dashboard.py`

4. **Add secrets** — in the app's **Settings > Secrets**, paste:
   ```toml
   MOTHERDUCK_TOKEN = "your_real_motherduck_token"
   ```

5. **Save & reboot.** The dashboard will be publicly accessible at
   `https://<your-app>.streamlit.app`.

> **Security note:** use a read-only MotherDuck token or share URL for the
> deployed app. The GitHub secret `MOTHERDUCK` is available for CI workflows
> but is **not** automatically available to Streamlit Cloud — you must add it
> through the Streamlit UI.

## GitHub secret

A repository secret named `MOTHERDUCK` is configured for CI/CD use. Access
it in GitHub Actions workflows as `${{ secrets.MOTHERDUCK }}`.

## Data dictionary

See [data_dictionary.md](data_dictionary.md) for full schema documentation
of all 13 tables and 8+ views.

## License

Private research data — do not redistribute without permission.
