# Lymph Node Extraction Completeness Audit

**Date:** 2026-03-27  
**Scope:** THYROID_2026 repository — pathology lymph node (LN) information from extraction through analytic/manuscript layers.  
**Method:** Code-path trace, pipeline review, and quantified checks on frozen export `exports/FINAL_PUBLICATION_BUNDLE_20260313/manuscript_cohort_v1.csv` (N=10,871). local DuckDB was not connected in the verification environment; claims below are backed by repository artifacts and source files.

---

## 1. Executive conclusion

**NOT COMPLETE**

Structured LN counts are **not** guaranteed for every pathology report row or every patient in the final analytic tables. Primary ingestion is **synoptic/structured columns** (`path_synoptics`, `tumor_pathology`), not systematic full-text parsing of every pathology report narrative. Final manuscript-layer columns show **~71%** non-null examined counts and **~33–37%** non-null positive/final counts on the full surgical cohort export. There is **no** repository-level proof that each narrative report was scanned end-to-end for LN language.

The manuscript / publication package **must not** claim that node burden is fully captured from all pathology reports without heavy qualification. Safe language: “available where documented in structured synoptic pathology fields; substantial missingness; narrative not systematically parsed in production.”

---

## 2. Evidence from code and file paths

### 2.1 Canonical tumor episode table (first hop from pathology)

File: `scripts/22_canonical_episodes_v2.py`

- `tumor_episode_master_v2` copies nodal counts **only** from `path_synoptics`:

```109:110:scripts/22_canonical_episodes_v2.py
        tumor_1_ln_examined AS ps_nodes_total,
        tumor_1_ln_involved AS ps_nodes_positive,
```

```235:237:scripts/22_canonical_episodes_v2.py
    -- nodal disease
    TRY_CAST(ps.ps_nodes_positive AS INTEGER) AS nodal_disease_positive_count,
    TRY_CAST(ps.ps_nodes_total AS INTEGER) AS nodal_disease_total_count,
```

- The optional `tumor_pathology` join in the same SQL **does not** supply LN counts (no `histology_1_ln_*` in that CTE). So episode-level nodal fields are **synoptic-column-bound**.

### 2.2 V2 note enrichment (what is *not* wired)

File: `scripts/22_canonical_episodes_v2.py`, function `enrich_from_v2_extractors`

- Loads RAI, Operative, Molecular, and Imaging extractors only. **`HistologyDetailExtractor` is not imported or run** in this enrichment loop, so **no** production merge from that extractor into `tumor_episode_master_v2`.

```853:869:scripts/22_canonical_episodes_v2.py
    from llm_extraction.extract_rai_v2 import RAIDetailExtractor
    from llm_extraction.extract_operative_v2 import OperativeDetailExtractor
    from llm_extraction.extract_molecular_v2 import MolecularDetailExtractor
    from llm_extraction.extract_imaging_v2 import ImagingNoduleExtractor
```

### 2.3 Histology / pathology narrative extractor (library exists; not production spine)

File: `llm_extraction/extract_histology_v2.py`

- Declares extraction from “pathology reports, synoptic text, and clinical notes.”
- LN pattern is **narrow**: `(\d+) (of|out of|/) (\d+) (lymph )?nodes? (positive|involved|with metast…)` — it does **not** cover many common negation/“no nodal mets” sentences unless they match this shape.

```118:123:llm_extraction/extract_histology_v2.py
_LN_COUNT = re.compile(
    r"\b(\d+)\s+(?:of|out\s+of|/)\s*(\d+)\s+"
    r"(?:lymph\s+nodes?|nodes?)\s+"
    r"(?:positive|involved|with\s+(?:metasta|tumor|carcinoma))", re.I,
)
```

Tests: `tests/test_histology_parser.py` (`TestLymphNodeCounts`).

### 2.4 Phase 6 LN yield (structured + location text)

File: `llm_extraction/extraction_audit_engine_v4.py`

- Class `LNYieldCalculator` parses **`path_synoptic`** fields (`raw_examined`, `raw_involved`, location strings). `'x'` in involved is treated as **positive without numeric count**.
- `build_ln_yield_sql()` builds `extracted_ln_yield_v1` from **`path_synoptics`** only (not from free-text CAP narrative tables).

### 2.5 Views combining cohort and risk features

File: `scripts/03_research_views.py` — `advanced_features_v3` uses `path_synoptics` for `ln_examined` / `ln_positive`:

```413:420:scripts/03_research_views.py
    TRY_CAST(ps.tumor_1_ln_examined AS DOUBLE) AS ln_examined,
    TRY_CAST(ps.tumor_1_ln_involved AS DOUBLE) AS ln_positive,
    CASE
        WHEN TRY_CAST(ps.tumor_1_ln_examined AS DOUBLE) > 0
        THEN ROUND(TRY_CAST(ps.tumor_1_ln_involved AS DOUBLE) /
                   TRY_CAST(ps.tumor_1_ln_examined AS DOUBLE), 4)
```

File: `scripts/10_maximize_local DuckDB_trial.py` — `recurrence_risk_features_mv` pulls LN counts from **`tumor_pathology`**:

```457:459:scripts/10_maximize_local DuckDB_trial.py
        TRY_CAST(tp.histology_1_ln_positive AS INT) AS ln_positive,
        TRY_CAST(tp.histology_1_ln_examined AS INT) AS ln_examined,
```

So **different downstream surfaces** can use **different primary tables** (path_synoptics vs tumor_pathology), which is a source of **inconsistency** unless reconciled in later resolved layers.

### 2.6 Scoring / AJCC8 N (computed from counts when available)

File: `scripts/51b_thyroid_scoring_python.py`

- Pulls `nodal_disease_*` from `tumor_episode_master_v2` and merges `ln_total_*` / `ln_positive_v6` from `patient_refined_master_clinical_v12`, then **fillna** merges.
- `compute_n_stage` returns **None** when `ln_positive` is null — so **N stage is not imputed from narrative** here.

```88:96:scripts/51b_thyroid_scoring_python.py
def compute_n_stage(row) -> str | None:
    ln_pos = row.get("ln_positive")
    ...
    if pd.isna(ln_pos) or ln_pos is None:
        return None
```

### 2.7 ThyroSeq helper (external Excel, not pathology report corpus)

File: `utils/thyroseq_helpers.py` — `normalize_lymph_nodes` parses ThyroSeq-style text; not the main pathology synoptic pipeline.

---

## 3. Final columns / tables carrying LN data (manuscript-aligned)

| Layer | Table / artifact | Columns (examples) | Derivation (summary) |
|-------|------------------|--------------------|----------------------|
| Episode | `tumor_episode_master_v2` | `nodal_disease_positive_count`, `nodal_disease_total_count` | `path_synoptics.tumor_1_ln_involved` / `tumor_1_ln_examined` |
| Refined | `extracted_ln_yield_v1` | `ln_total_examined`, `ln_total_positive`, … | SQL over `path_synoptics` |
| Master | `patient_refined_master_clinical_v12` | `ln_total_examined`, `ln_total_positive`, `ln_positive_v6`, … | Phase merges |
| Scoring | `thyroid_scoring_py_v1` / `thyroid_scoring_systems_v1` | `ln_positive`, `ln_examined`, `ln_ratio`, … | Merge episode + mcv12 |
| Manuscript export | `manuscript_cohort_v1.csv` | `path_ln_examined_raw`, `path_ln_positive_raw`, `ln_positive_final`, `ln_ratio`, `ajcc8_n_stage` | Resolved layer / script 57+ |

**Semantics of NULL (not uniquely typed in schema):**

- Often means **not populated in source structured fields** (unknown / not extracted), **not** a proven “not scanned.”
- There is **no** single column in these exports that reliably distinguishes “report had no LN section” vs “field not filled” vs “narrative-only data not ingested.”

---

## 4. Pipeline guarantee assessment (tasks A4–A5)

| Required outcome | Met? |
|------------------|------|
| Every pathology report → numeric LN data | **No** — only where synoptic/pathology table fields exist and cast cleanly |
| Explicit zero / no-node encoding everywhere | **No** — zeros appear when coded as `0` in source; absence often remains NULL |
| Explicit missing / unresolved status | **Partial** — some validation tables; not a uniform per-report status flag |
| Full narrative scanned | **No** — `enrich_from_v2_extractors` does not run histology/path narrative LN pass at scale |

### 4.1 Silent / partial failure modes (observed in design)

- **Narrative not merged:** Pathology CAP narrative in `clinical_notes_long` is **not** systematically merged into nodal counts via script 22 enrichment.
- **Synoptic-only path:** Counts depend on Excel/synoptic column completeness; benign or non-cancer rows may lack LN fields by design.
- **`x` placeholder:** Positive involvement without numeric count (`tumor_1_ln_involved` = `x`) — positive flag without numerator in some SQL paths.
- **Multi-row / aggregation:** `extracted_ln_yield_v1` aggregates per patient with SUM and filters; detail can be lost across multiple specimens.
- **Table divergence:** `recurrence_risk_features_mv` uses `tumor_pathology` LN columns; `advanced_features_v3` uses `path_synoptics` — reconciliation depends on later scripts.

---

## 5. Quantified missingness and checks (frozen export)

**Source:** `exports/FINAL_PUBLICATION_BUNDLE_20260313/manuscript_cohort_v1.csv` (N=10,871).  
**Machine summary:** `studies/proposal2_ete_staging/outputs/pathology_ln_audit_summary.json` (regenerate: `.venv/bin/python studies/proposal2_ete_staging/run_pathology_ln_audit.py`).

| Metric | Value |
|--------|------:|
| `path_ln_examined_raw` non-null | 7,730 (**71.11%**) |
| `path_ln_positive_raw` non-null | 3,603 (**33.14%**) |
| `ln_positive_final` non-null | 4,060 (**37.35%**) |
| Both path raw non-null | 3,493 |
| AJCC8 N in N1 family (regex `N1a?b?`) | 2,640 |
| N1 family with `ln_positive_final` NULL or 0 | **1,519** |
| `path_ln_positive_raw` > `path_ln_examined_raw` | **1** |
| `path_ln_examined_raw` == 0 | 4,231 |

**Stratified missingness (`path_ln_examined_raw`) by surgery year (approximate eras):**

| Period | N | % non-null examined |
|--------|---:|--------------------:|
| 2005–2009 | 1,191 | 80.4 |
| 2010–2014 | 1,885 | 95.9 |
| 2015–2019 | 2,935 | 78.7 |
| 2020–2024 | 1,817 | **52.9** |

**Proposal 2 ETE snapshot (`studies/proposal2_ete_staging/tables/analytic_cohort.csv`, N=596 PTC subset):**  
~98.8% `ln_examined`, ~88.6% `ln_positive` non-null; **430 / 589** examined values equal **1** — consistent with prior audit concern that `ln_examined` can behave like a coarse indicator (see `audit_report.md` / `audit_reproduce.py`).

**Logical tension:** Over **1,500** rows sit in an N1-class AJCC8 bucket while `ln_positive_final` is missing or zero — staging and numeric LN burden are **not** always co-derived from the same resolved numeric LN fields. This blocks any claim of “complete, consistent node burden extraction.”

---

## 6. Manuscript / package safety

- **Do not** state that all pathology reports were scanned for LN data.  
- **Do** report *denominators* and *missingness* for LN examined/positive, and separate **pathologic N stage** from **numeric LN counts** where they disagree.  
- **Cite** structured sources (`path_synoptics`, `tumor_pathology`, resolved layer) and acknowledge **narrative gap** unless/until a narrative pass is materialized and validated.

---

## 7. Remediation (summary pointer)

Implemented in-repo utilities and audit runner; full local DuckDB materialization of narrative-augmented columns is **out of scope** for the offline verification run. See:

- `studies/proposal2_ete_staging/LYMPH_NODE_EXTRACTION_REMEDIATION_SUMMARY.md`
- `utils/pathology_ln_narrative_extract.py`
- `studies/proposal2_ete_staging/run_pathology_ln_audit.py`

---

## 8. Verification commands (repeatable)

```bash
.venv/bin/python -m pytest tests/test_pathology_ln_narrative_extract.py -q
.venv/bin/python studies/proposal2_ete_staging/run_pathology_ln_audit.py
```

Optional narrative sample (requires `LOCAL_DB_PATH`):

```bash
export LOCAL_DB_PATH="…"
.venv/bin/python studies/proposal2_ete_staging/run_pathology_ln_audit.py --md
```
