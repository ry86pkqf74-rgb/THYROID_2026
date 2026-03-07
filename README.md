# THYROID_2026

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

## Data dictionary

See [data_dictionary.md](data_dictionary.md) for full schema documentation
of all 13 tables and 8+ views.

## License

Private research data — do not redistribute without permission.
