# Lobectomy vs total thyroidectomy (2–4 cm, imaging-defined N0)

Retrospective cohort extraction and statistics from the THYROID_2026 MotherDuck lakehouse (`thyroid_research_2026`).

## Prerequisites

- Python 3.11+ (repo uses `.venv` with DuckDB ≤1.4.x per `AGENTS.md`)
- `MOTHERDUCK_TOKEN` or `MD_SA_TOKEN` in the environment, **or** `.streamlit/secrets.toml` with the same keys
- Run commands from repository root: `THYROID_2026/` (this directory’s parent’s parent)

## Reproduce cohort + tables + figures

```bash
cd /path/to/THYROID_2026
export MOTHERDUCK_TOKEN="your_token_here"   # or rely on secrets.toml
.venv/bin/python studies/lobectomy_molecular_202603/run_pipeline.py
```

Artifacts land in `outputs/lobectomy_molecular_202603/`:

| Output | Description |
|--------|-------------|
| `cohort_analytic_v1.csv` | Raw MotherDuck cohort pull |
| `analytic_ready_v1.csv` | Engineered variables |
| `cohort_summary.json` | Ns and flow hints |
| `tables/table1.md` | Table 1 (overall + by surgery type) |
| `tables/logistic_multivariable_main.csv` | Multivariable ORs |
| `tables/logistic_multivariable_main.tex` | LaTeX OR table |
| `tables/kappa_by_platform.csv` | Concordance κ by platform |
| `tables/univariable_tests.csv` | FDR-adjusted univariable tests |
| `figures/forest_multivariable.png` | Forest plot (multivariable ORs) |
| `figures/sankey_genetics_surgery_completion.html` | Sankey (interpret cautiously; see data quality notes) |
| `data_quality_issues.md` | Known gaps |

## Ad hoc MotherDuck SQL

```bash
cd /path/to/THYROID_2026
.venv/bin/python -c "
from pathlib import Path
from motherduck_client import MotherDuckClient
sql = Path('studies/lobectomy_molecular_202603/sql/01_cohort_base.sql').read_text()
con = MotherDuckClient.for_env('prod').connect_rw()
print(con.execute(sql).fetchdf().head())
con.close()
"
```

**Note:** The project discourages `CREATE TABLE` writes on MotherDuck without explicit approval; this pipeline is **read-only**.

## Optional multiple imputation (MICE)

The shared `ThyroidStatisticalAnalyzer` class (`utils/statistical_analysis.py`) implements `mice_impute()` and `pool_logistic_rubins()`. This study’s **primary** analysis is complete-case multivariable logistic regression because preoperative molecular coverage is sparse (n=21) and MICE does not recover information that is structurally missing from the source tables. Investigators may extend `run_pipeline.py` if prospective sensitivity analyses are required.

## Manuscript draft

See [`manuscript_draft.md`](manuscript_draft.md). Word export (requires [pandoc](https://pandoc.org)):

```bash
pandoc studies/lobectomy_molecular_202603/manuscript_draft.md \
  -o outputs/lobectomy_molecular_202603/manuscript_draft.docx
```

## Citation / provenance

- **Zenodo (code + bundle):** [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510)  
- **Git tag:** `v2026.03.10-publication-ready`  
- **MotherDuck:** database `thyroid_research_2026`; read-only share path defined in `motherduck_client.py`
