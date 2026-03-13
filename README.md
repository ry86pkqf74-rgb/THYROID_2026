# THYROID_2026

## Dataset Maturation Layer (v2026.03.13)

**Status:** Manuscript-ready (with scoped caveats) | Extraction pipeline complete | 578 MotherDuck tables

A final manuscript-readiness hardening pass on 2026-03-13 audited 578 MotherDuck
tables, 16 `val_*` validation tables, and all prior audit documents. The
**analysis-resolved layer** is populated and all 7 readiness gates pass. The
extraction pipeline is complete (13 phases, 11 engine versions). A subsequent
**hardening pass** fixed 3 missing Streamlit tables, ran ANALYZE on 17 key tables,
and verified all dashboard data dependencies against live MotherDuck state.

**Honest assessment:** ~50% of patients have clinical notes; operative NLP enrichment
fields (berry ligament, frozen section, EBL) remain at 0% due to pipeline architecture;
88.8% of recurrence dates are unresolved; RAI dose coverage is 41%. These are
documented source limitations, not data quality failures.

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
- 1,764 recurrence dates unresolved — requires manual chart review (prioritized queue deployed)
- 8 operative V2 NLP enrichment fields at 0% — extractor exists but outputs not materialized

### Current repo status

See [`docs/FINAL_REPO_STATUS_20260313.md`](docs/FINAL_REPO_STATUS_20260313.md) for the
definitive single source of truth (readiness, maturity, safe/unsafe claims).
See [`docs/REPO_STATUS.md`](docs/REPO_STATUS.md) for the navigable index of all
audit documents, export bundles, and open backfill items.

---

Thyroid cancer research lakehouse — 11,673 patients across 13 base tables,
8+ analytic views, and a fully interactive Streamlit dashboard backed by
[MotherDuck](https://motherduck.com) cloud DuckDB.

## Live Dashboard

**[thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app](https://thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app/)**

> The deployed app connects to a read-only MotherDuck share.

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

## Traceability & Date Accuracy (v2026.03.13)

The manuscript cohort and analysis-resolved layer have source/date provenance
for all manuscript-critical workflows. Structured data domains (demographics,
surgery, pathology, thyroglobulin labs) have direct source links with high
date accuracy. NLP-derived domains (complications, staging entities) have
source links but variable date precision.

**Known provenance limits:**
- Non-thyroglobulin lab dates (TSH, PTH, calcium, vitamin D) have 0% structured
  collection dates — NLP-extracted mentions lack specimen timestamps
- ~50% of patients have clinical notes; the remainder are represented by
  structured data only (path_synoptics, tumor_pathology)
- 88.8% of recurrence dates are unresolved (flagged, not fabricated)

### Strict Lab Date Precedence

Lab collection dates always take precedence over note encounter dates:

```
specimen_collect_dt (explicit lab date, confidence 1.0)
  └─▶ entity_date (NLP-extracted near-entity date, confidence 0.7)
       └─▶ note_date (encounter date, fallback only — error for labs)
```

This is enforced in `provenance_enriched_events_v1` and validated by `val_provenance_traceability`.

### Direct Source Linking

Events in `provenance_enriched_events_v1` (50,297 rows) carry:
- `direct_source_link` = `source_column|research_id|event_subtype|evidence_snippet`
- `date_status_final` = `LAB_DATE_USED` / `ENTITY_DATE_USED` / `NOTE_DATE_FALLBACK`

### Lineage Audit

`lineage_audit_v1` traces the 4-tier data lineage per patient:

```
Tier 1: Raw structured source (path_synoptics, thyroglobulin_labs)
Tier 2: Note-level anchor (clinical_notes_long)
Tier 3: Extracted entities (note_entities_*)
Tier 4: Final analytic cohort (patient_refined_master_clinical_v12)
```

### Validation

`val_provenance_traceability` enforces:
- Zero tolerance for `direct_source_link IS NULL`
- Zero tolerance for `NOTE_DATE_FALLBACK` on lab events
- Warning (not error) for non-Tg lab events with no date (6,801 — institutional data limitation)

```bash
# Run full provenance audit
.venv/bin/python scripts/46_provenance_audit.py --md
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

## Streamlit Cloud deployment

The app auto-deploys from `main` at
**[thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app](https://thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app/)**.

### Deployment access modes

| Mode | Data access | Auth required | How to enable |
|------|------------|---------------|---------------|
| **Local + token** | MotherDuck RW or RO share | `MOTHERDUCK_TOKEN` env var or `.streamlit/secrets.toml` | Default local setup |
| **Cloud — private** | MotherDuck RO share via Streamlit secret | Streamlit Cloud login | Default cloud deploy |
| **Cloud — public (share enabled)** | MotherDuck RO share via Streamlit secret | None (unauthenticated) | Enable sharing at [share.streamlit.io](https://share.streamlit.io) |
| **Cloud — sign-in required** | MotherDuck RO share via Streamlit secret | Viewer authentication | Configure viewer auth in Streamlit Cloud settings |

The deployed app reads from the **read-only MotherDuck share** (`thyroid_share`).
Data cannot be modified through the share. Write operations (Review Mode) require
a local connection with the RW token.

### Cure Modeling (PTCM + MCM)

The **Outcomes & Analytics** section includes Promotion Time Cure Model (PTCM)
and Mixture Cure Model (MCM) sub-sections with head-to-head comparison.

```bash
python scripts/26_motherduck_materialize_v2.py --md
python scripts/39_promotion_time_cure_models.py --md
python scripts/38_mixture_cure_models.py --md
streamlit run dashboard.py
```

## Data dictionary

See [data_dictionary.md](data_dictionary.md) for full schema documentation
of all 13 tables and 8+ views.

## License

Private research data — do not redistribute without permission.


## Pipeline & Deployment

### Materialize to MotherDuck

```bash
python scripts/26_motherduck_materialize_v2.py --md   # 131+ tables
python scripts/29_validation_engine.py --md            # val_* tables
python scripts/78_final_hardening.py --md              # hardening pass
python scripts/30_readiness_check.py --md              # readiness audit
```

### Daily refresh

```bash
.venv/bin/python scripts/36_daily_refresh.py --md
```

## Release history

**Current:** v2026.03.13 (truth-sync) — see [RELEASE_NOTES.md](RELEASE_NOTES.md)
**Dashboard:** v3.3.0-2026.03.13
**Zenodo DOI:** [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510)

Requires: `pip install -r requirements.txt` (includes lifelines, scikit-survival, shap, etc.)
