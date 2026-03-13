# THYROID_2026

## Dataset Maturation Layer (v2026.03.13)

**Status:** Manuscript-ready | Approaching dataset-mature | Extraction pipeline complete

A full engineering-grade verification pass on 2026-03-13 audited 531 MotherDuck
tables, 34 `val_*` validation tables, and 18 prior audit documents. The
**analysis-resolved layer** is populated and all 7 readiness gates pass. The
extraction pipeline is complete (13 phases, 11 engine versions, data quality
98/100). A subsequent **dataset maturation pass** (`scripts/75_dataset_maturation.py`)
resolved the remaining canonical-table propagation gaps.

### Key references

| Artifact | Location |
|----------|----------|
| Definitive verification report | [`docs/final_repo_verification_20260313.md`](docs/final_repo_verification_20260313.md) |
| Database hardening audit | [`docs/database_hardening_audit_20260313.md`](docs/database_hardening_audit_20260313.md) |
| Manuscript metric reconciliation | [`docs/manuscript_metric_reconciliation_20260313.md`](docs/manuscript_metric_reconciliation_20260313.md) |
| Freeze alignment report | [`docs/manuscript_freeze_alignment_20260313.md`](docs/manuscript_freeze_alignment_20260313.md) |
| Canonical backfill report | [`docs/canonical_backfill_report_20260313.md`](docs/canonical_backfill_report_20260313.md) |
| Publication bundle (62 files) | `exports/FINAL_PUBLICATION_BUNDLE_20260313/` |
| Readiness assessment (7/7 PASS) | `exports/FINAL_PUBLICATION_BUNDLE_20260313/readiness_assessment.json` |
| Zenodo DOI | [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510) |
| Git tag | [`v2026.03.10-publication-ready`](../../releases/tag/v2026.03.10-publication-ready) |

### What "manuscript-ready" means

The manuscript cohort (`manuscript_cohort_v1`, 10,871 patients, 139 columns), the
analysis-eligible cancer subcohort (N=4,136), episode-level dedup table, scoring
systems (AJCC8/ATA/MACIS/AGES/AMES), Tables 1–3, and Figures 1–5 are generated
and verified. 11 canonical metrics pass cross-source consistency checks.

### Dataset Verification Status (March 13 2026)

The dataset maturation pass resolved the following:

1. **Operative CND/LND flags** — wired from structured `path_synoptics` fields;
   CND: 0 -> 2,497 TRUE (26.6%); LND: 0 -> 241 TRUE (2.6%)
2. **Operative note dates** — 9,366 of 9,371 episodes now have resolved dates
3. **Imaging layer** — `imaging_nodule_master_v1` (19,891 rows) is now canonical;
   `imaging_nodule_long_v2` deprecated (schema stub)
4. **Provenance columns** — unified `source_table`, `source_script`, `provenance_note`,
   `resolved_layer_version` added to all 4 analysis tables
5. **Chronology anomalies** — 626 classified (102 benign, 14 extraction errors, 510 true conflicts)
6. **MotherDuck optimization** — ANALYZE TABLE run on 10 canonical tables
7. **Health monitoring** — 3 dashboard tables deployed (`val_dataset_integrity_summary_v1`,
   `val_provenance_completeness_v2`, `val_episode_linkage_completeness_v1`)
8. **Canonical gap closure** (`scripts/76_canonical_gap_closure.py`) — RAI dose
   provenance (20% -> 41%), RAS subtype propagation (325 rows), linkage ID
   propagation (6 tables), recurrence date hardening (4 tiers)
9. **Lab canonical layer** (`scripts/77_lab_canonical_layer.py`) —
   `longitudinal_lab_canonical_v1` (45,954 rows, 5 analytes, 3,349 patients)
   with forward-compatible schema for future institutional lab extract
10. **Workflow dashboard refactor** — 39 flat tabs reorganized into 6
    workflow-first sections; new QA workbench and manual review workbench modules
11. **Final hardening** (`scripts/78_final_hardening.py`) — recurrence review
    queue, imaging-FNA linkage fix, RAI missingness classification, lab
    contract validation

### Remaining Source-Limited Gaps

- Non-Tg lab dates (TSH/PTH/Ca/vitD) at 0% — requires institutional lab extract
- Zero nuclear medicine notes in corpus — RAI dose recovery capped at ~41%
- 87% vascular invasion remains `present_ungraded` — synoptic template limitation
- Pre-2019 operative notes absent — institutional data limitation
- 1,764 recurrence dates unresolved — requires manual chart review

Remaining source-limited gaps (not fixable without new institutional data):
- Non-Tg lab dates (TSH/PTH/Ca/vitD) at 0%
- Nuclear medicine report text absent from corpus
- Vascular invasion 87% present_ungraded (synoptic template limitation)

### Current repo status

See [`docs/REPO_STATUS.md`](docs/REPO_STATUS.md) for a navigable index of all
March 13 audit documents, export bundles, and open backfill items.

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

## Traceability & Date Accuracy Guarantee (v2026.03.12)

Every data point in THYROID_2026 is traceable to its **direct source** (raw file,
sheet, row_id, text span, extraction method) and uses the **accurate associated date**.

### Strict Lab Date Precedence

Lab collection dates always take precedence over note encounter dates:

```
specimen_collect_dt (explicit lab date, confidence 1.0)
  └─▶ entity_date (NLP-extracted near-entity date, confidence 0.7)
       └─▶ note_date (encounter date, fallback only — error for labs)
```

This is enforced in `provenance_enriched_events_v1` and validated by `val_provenance_traceability`.

### Direct Source Linking

Every event in `provenance_enriched_events_v1` has:
- `direct_source_link` = `source_column|research_id|event_subtype|evidence_snippet`
- `date_status_final` = `LAB_DATE_USED` / `ENTITY_DATE_USED` / `NOTE_DATE_FALLBACK`

### Lineage Audit

`lineage_audit_v1` traces the complete 4-tier data lineage per patient:

```
Tier 1: Raw structured source (path_synoptics, thyroglobulin_labs)
Tier 2: Note-level anchor (clinical_notes_long)
Tier 3: Extracted entities (note_entities_*)
Tier 4: Final analytic cohort (patient_refined_master_clinical_v9)
```

### Validation

`val_provenance_traceability` (16th `val_*` table) enforces:
- Zero tolerance for `direct_source_link IS NULL`
- Zero tolerance for `NOTE_DATE_FALLBACK` on lab events
- Warning for any lab with no date at all
- Warning for untraced patients in `lineage_audit_v1`

```bash
# Run full provenance audit
.venv/bin/python scripts/46_provenance_audit.py --md

# Reports generated:
# docs/provenance_coverage_report.md
# docs/date_accuracy_verification_report_YYYYMMDD.md
```

---

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

## Dashboard (6 Workflow Sections)

The Streamlit dashboard is organized into 6 workflow-first sections:

1. **Overview** — cohort KPIs, data completeness by surgery year, date rescue
   rate, dataset health monitoring, linkage/provenance completeness, caveats
2. **Patient Explorer** — per-patient timeline with date-status legend and
   eligibility badges, patient audit, data explorer, visualizations
3. **Data Quality** — QA workbench (integrity, provenance, imaging-FNA linkage
   status, chained molecular metrics, RAI missingness, recurrence date
   resolution, lab coverage), manual review workbench (chronology conflicts,
   extraction errors, linkage ambiguities, unresolved recurrence prioritized
   queue), validation engine, diagnostics, cohort QC
4. **Linkage & Episodes** — extraction completeness, molecular/RAI/imaging/
   operative episode analytics, QA & adjudication, features explorer, timeline
5. **Outcomes & Analytics** — survival, advanced survival, statistical analysis,
   predictive analytics (model comparison, competing risks, ML nomograms,
   cure calculator), advanced analytics, cure probability
6. **Manuscript & Export** — genetics, specimen, complications, imaging,
   ThyroSeq integration, review queues (histology, molecular, RAI)

## Interactive Stats & Modeling

The `ThyroidStatisticalAnalyzer` class (`utils/statistical_analysis.py`) provides a publication-ready statistical engine:

```python
from utils.statistical_analysis import ThyroidStatisticalAnalyzer
analyzer = ThyroidStatisticalAnalyzer(con)

# Table 1 with SMD
t1_df, meta = analyzer.generate_table_one(data=df, groupby_col="braf_positive")

# FDR-corrected hypothesis tests
results = analyzer.run_hypothesis_tests(df, "event_occurred", features, correction="fdr_bh")

# Logistic regression with clinical snippet
result = analyzer.fit_logistic_regression("event_occurred", predictors, data=df)
snippet = ThyroidStatisticalAnalyzer.format_clinical_snippet(result, model_type="OR")

# Longitudinal Tg mixed-effects
long = analyzer.longitudinal_summary(marker="tg")

# Power analysis
n = ThyroidStatisticalAnalyzer.power_two_proportions(p1=0.15, p2=0.05)
```

**CLI demo** (outputs to `studies/statistical_analysis_examples/`):
```bash
.venv/bin/python scripts/36_statistical_analysis_examples.py --md
```

**Notebook**: `notebooks/36_statistical_analysis_examples.ipynb` (10 sections, 35 cells)

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
| **Risk & Survival** | Kaplan-Meier recurrence-free survival with stratification by stage, histology, BRAF; risk feature summary; **Latent Disease Burden (PTCM)** + **Unified Cure Modeling Dashboard** (MCM vs PTCM head-to-head) sub-sections |
| **Advanced Features v3** | Full column selector across all 60+ engineered features |

### Risk & Survival — Promotion Time Cure Model (PTCM)

The **Risk & Survival** tab includes a "Latent Disease Burden (Promotion Time Cure Model)" sub-section. Unlike the mixture cure model (which treats cure as a binary latent variable), the PTCM (Chen, Ibrahim & Sinha 1999) provides a biologically mechanistic interpretation: the number of cancer cells capable of promoting recurrence follows Poisson(θ(x)), where θ(x) = exp(xᵀβ) is the covariate-driven promotion intensity. If the Poisson count is zero, the patient is operationally cured, giving cure probability π(x) = exp(−θ(x)). The Weibull baseline captures the shape of the promotion-time hazard. The sub-section shows model KPIs (cure fraction π̄, 10-year plateau rate, AIC, Weibull shape κ), a covariate effects table with bootstrap 95% CIs, patient-level cure probability distribution, Weibull vs Kaplan-Meier overlay, and a self-contained biological interpretation HTML report.

Run the mechanistic cure analysis:

```bash
python scripts/26_motherduck_materialize_v2.py --md
python scripts/39_promotion_time_cure_models.py --md
streamlit run dashboard.py
```

Outputs land in `exports/promotion_cure_results/`.

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

## Release history

**Current:** v2026.03.13 audit/verification wave — see [RELEASE_NOTES.md](RELEASE_NOTES.md)
**Zenodo DOI:** [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510)
**QA reconciliation:** [docs/QA_report.md](docs/QA_report.md)

**Legacy compatibility:** If the dashboard shows a message about missing legacy tables
(`molecular_episode_v3`, `rai_episode_v3`, `validation_failures_v3`, `tumor_episode_master_v2`,
`linkage_summary_v2`), run once:

```bash
.venv/bin/python scripts/27_fix_legacy_episode_compatibility.py
```

This creates the five tables as views on top of the modern stack (no data duplication).
Restart the Streamlit app after running. See [data_dictionary.md](data_dictionary.md) § Legacy Compatibility Layer.

## Daily Refresh / Nightly Automation

Run the full pipeline chain locally:

```bash
.venv/bin/python scripts/36_daily_refresh.py --md
```

Or use the Publication Export pipeline for manuscript-ready outputs:

```bash
.venv/bin/python scripts/37_publication_export.py --md
```

### Advanced Survival Analysis

Run publication-grade survival models (KM, Cox PH, RMST, CIF, PSM, RSF + SHAP, DeepSurv):

```bash
# 1. Build survival_cohort_enriched + survival_kpis tables
.venv/bin/python scripts/26_motherduck_materialize_v2.py --md

# 2. Run full analysis — outputs to exports/survival_results/
.venv/bin/python scripts/38_advanced_survival_analysis.py --md

# 3. View in dashboard (Advanced Survival tab)
streamlit run dashboard.py
```

Requires: `pip install lifelines scikit-survival shap torch plotly kaleido`

### Unified Cure Modeling Platform

The project provides two complementary cure models and a head-to-head comparison framework:

| Model | Script | Interpretation | Key Output |
|-------|--------|----------------|------------|
| **Mixture Cure Model (MCM)** | `38_mixture_cure_models.py` | Population split: π(x) = logistic(xᵀγ) partitions patients into cured/susceptible | Incidence ORs, Weibull latency, patient π(x) |
| **Promotion Time Cure Model (PTCM)** | `39_promotion_time_cure_models.py` | Mechanistic: θ(x) = exp(xᵀβ) Poisson promotion intensity, π(x) = exp(−θ(x)) | Promotion β, Weibull baseline, patient θ(x) |
| **Head-to-Head Comparison** | `40_cure_model_comparison.py` | Unified table: cure fraction, AIC, top predictors, 10y RMST, forest plots | HTML report, CSV, side-by-side figures |

Run the full cure analysis pipeline:

```bash
# 1. Materialize cohorts (builds mixture_cure_cohort + promotion_cure_cohort)
python scripts/26_motherduck_materialize_v2.py --md

# 2. Run Mixture Cure Model (MCM) — exports to exports/mixture_cure_results/
python scripts/38_mixture_cure_models.py --md

# 3. Run Promotion Time Cure Model (PTCM) — exports to exports/promotion_cure_results/
python scripts/39_promotion_time_cure_models.py --md

# 4. Head-to-head comparison — exports to exports/cure_comparison/
python scripts/40_cure_model_comparison.py --md

# 5. View in dashboard (Risk & Survival → Unified Cure Modeling Dashboard)
streamlit run dashboard.py
```

The **Risk & Survival** tab includes a "Unified Cure Modeling Dashboard" sub-section with three tabs (Mixture Cure | Promotion Time Cure | Head-to-Head Comparison), KPI cards from both models, an interactive patient calculator showing π from both models, side-by-side Weibull curves, and covariate forest plots. The sidebar includes a "Cure Model Comparison KPIs" expander.

<!-- GitHub Actions nightly refresh (add to .github/workflows/nightly-refresh.yml):

name: Nightly Pipeline Refresh
on:
  schedule:
    - cron: '0 6 * * *'    # 6 AM UTC daily
  workflow_dispatch:        # manual trigger

jobs:
  refresh:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python scripts/36_daily_refresh.py --md
        env:
          MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}
      - run: python scripts/37_publication_export.py --md
        env:
          MOTHERDUCK_TOKEN: ${{ secrets.MOTHERDUCK_TOKEN }}
-->
