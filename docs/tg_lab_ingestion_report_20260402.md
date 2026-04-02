# Thyroglobulin Lab Ingestion Report

**Generated**: 2026-04-02 17:26
**Script**: `scripts/113_tg_lab_ingestion.py`
**Source**: `Thyroid Thyroglobulin Lab_20251120.csv`

## Source File Metadata

| Field | Value |
|-------|-------|
| File | `Thyroid Thyroglobulin Lab_20251120.csv` |
| Date received | 2025-11-20 |
| Raw rows | 78,112 |
| Columns | 17 |

## Row Count Waterfall

| Stage | Rows |
|-------|------|
| Raw input | 78,112 |
| After deduplication | 78,006 |
| Assigned (canonical) | 76,971 |
| Review queue | 1,035 |

## Analyte Breakdown

| analyte   | assay_method   |   count |
|:----------|:---------------|--------:|
| Tg        | IMA            |      83 |
| Tg        | LC-MS/MS       |     368 |
| Tg        | RIA            |       8 |
| Tg        | comprehensive  |      57 |
| Tg        | immunoassay    |   37450 |
| TgAb      | IgG            |       4 |
| TgAb      | combo_panel    |      14 |
| TgAb      | immunoassay    |   38982 |
| TgAb      | reflex         |       5 |

| Analyte | Rows | Patients |
|---------|------|----------|
| Tg | 37,966 | 3,057 |
| TgAb | 39,005 | 3,170 |
| **Total** | **76,971** | **3,258** |

Patients with both Tg and TgAb: 2,969

## Combo Panel Disambiguation

| Metric | Count |
|--------|-------|
| Total combo pairs | 17,162 |
| Heuristic-resolved (detection limits) | 16,173 |
| Cross-reference-resolved | 561 |
| Ambiguous → review queue | 607 |

Heuristic accuracy: 99.2% (validated on 7,622 ground-truth pairs).

## Result Parsing

| Metric | Value |
|--------|-------|
| Numeric parse rate | 97.4% |
| Date coverage | 100.0% |
| Date range | 2001-01-04 10:20:00 — 2025-11-19 19:41:00 |

## Temporal Distribution

| Window | Count |
|--------|-------|
| early_postop | 9,120 |
| long_term | 22,960 |
| perioperative | 1,543 |
| pre_surgery | 3,032 |
| surveillance_1y | 7,520 |
| surveillance_5y | 32,796 |

## Unmatched Research IDs

8 research IDs not in master cohort: {20038, 20040, 20041, 20044, 20045, 20048, 20049, 20054}

**Recommendation**: These 8 IDs (20038, 20040, 20041, 20044, 20045, 20048, 20049, 20054)
should be verified against the master cohort file and either added or excluded.

## Spot Checks (10 Random Patients — Tg Trajectory)

| Research ID | First 5 Tg Values |
|-------------|-------------------|
| 5499 | 2.2, 2.2 |
| 512 | 0.6, 0.6 |
| 2770 | 15.5 |
| 8232 | 1.7, 1.7, 2.6, 2.6, 1.1 |
| 11084 | 2.4 |
| 967 | 518.0, 141.0, 23.0, 101.0 |
| 10886 | 8.7 |
| 1463 | <0.1, 0.1, 0.1, 0.1, 0.2 |
| 7911 | 0.2, 0.2 |
| 7383 | 4.6, 4.6, 3228.0, >9000.0, 1051.7 |

## Methods Paragraph (Pre-Written)

Serum thyroglobulin (Tg) and thyroglobulin antibody (TgAb) levels were obtained
from institutional laboratory information system records. A total of 76,971
laboratory results from 3,258 patients were available, spanning
2001–2025.
Results obtained via immunometric assay (IMA), liquid chromatography–tandem mass
spectrometry (LC-MS/MS), and radioimmunoassay (RIA) were preserved with assay method
annotations. Panel orders combining Tg and TgAb in a single test entry
(34,324 of 78,112 results) were disambiguated using detection
limit pattern matching (validated accuracy 99.2% against 7,622 independently labeled
ground-truth pairs). 1,035 results (1.3%) with
ambiguous analyte assignment were excluded from primary analyses and routed to manual
review.
