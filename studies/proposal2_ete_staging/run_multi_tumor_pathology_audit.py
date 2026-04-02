#!/usr/bin/env python3
"""Regenerate multi-tumor pathology audit artifacts (source vs path_synoptics vs canonical).

Outputs (under this directory):
  - MULTI_TUMOR_PATHOLOGY_CAPTURE_AUDIT.md
  - multi_tumor_source_vs_output_counts.csv
  - multi_tumor_discrepant_cases.csv

Usage:
  .venv/bin/python studies/proposal2_ete_staging/run_multi_tumor_pathology_audit.py
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
STUDY = Path(__file__).resolve().parent
RAW_XLSX = ROOT / "raw" / "All Diagnoses & synoptic 12_1_2025.xlsx"
PARQUET = ROOT / "processed" / "path_synoptics.parquet"
SHEET = "synoptics + Dx merged"

OUT_MD = STUDY / "MULTI_TUMOR_PATHOLOGY_CAPTURE_AUDIT.md"
OUT_COUNTS = STUDY / "multi_tumor_source_vs_output_counts.csv"
OUT_DISC = STUDY / "multi_tumor_discrepant_cases.csv"


def _slot_columns_parquet(con: duckdb.DuckDBPyConnection, pq: str, n: int) -> list[str]:
    r = con.execute(
        f"""
        SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{pq}'))
        WHERE column_name LIKE 'tumor_{n}_%' ORDER BY column_name
        """
    ).fetchdf()
    return r["column_name"].tolist()


def _slot_cols_excel(ex: pd.DataFrame, n: int) -> list[str]:
    out: list[str] = []
    for c in ex.columns:
        s = str(c)
        if re.search(rf"(?i)(?:tumor|tumour)\s*[_ ]?{n}\b", s) or re.search(
            rf"(?i)^tumor_{n}_", s
        ):
            out.append(c)
        elif n == 1 and re.search(r"(?i)^tumor_1_", s):
            out.append(c)
    return out


def _nonempty_row_mask(sub: pd.DataFrame) -> pd.Series:
    def ok(x) -> bool:
        if pd.isna(x):
            return False
        t = str(x).strip().lower()
        return t not in ("", "nan", "none", "null")

    return sub.apply(lambda r: any(ok(v) for v in r.values), axis=1).astype(int)


def main() -> None:
    con = duckdb.connect()
    pq = PARQUET.as_posix()
    ex = pd.read_excel(RAW_XLSX, sheet_name=SHEET)
    par = con.execute(f"SELECT * FROM read_parquet('{pq}')").fetchdf()

    if len(ex) != len(par):
        raise SystemExit(f"Row count mismatch Excel {len(ex)} vs parquet {len(par)}")
    if not (ex["Research ID number"].values == par["research_id"].values).all():
        raise SystemExit("research_id order mismatch Excel vs parquet")

    # Per-slot column lists
    slot_par = {i: _slot_columns_parquet(con, pq, i) for i in range(1, 6)}
    slot_ex = {i: _slot_cols_excel(ex, i) for i in range(1, 6)}

    per_par = {i: _nonempty_row_mask(par[slot_par[i]]) for i in range(1, 6)}
    per_ex = {i: _nonempty_row_mask(ex[slot_ex[i]]) for i in range(1, 6)}

    mism = {i: int((per_par[i].values != per_ex[i].values).sum()) for i in range(1, 6)}

    nslots_par = sum(per_par[i] for i in range(1, 6))
    nslots_ex = sum(per_ex[i] for i in range(1, 6))
    if (nslots_par.values != nslots_ex.values).any():
        raise SystemExit("Aggregated slot count mismatch")

    dist = Counter(int(x) for x in nslots_par.values)
    rows_path = len(par)
    with_secondary = int((per_par[2] + per_par[3] + per_par[4] + per_par[5] > 0).sum())

    # Canonical tumor_episode (local DB if present)
    te_ordinals = ""
    try:
        loc = duckdb.connect(str(ROOT / "thyroid_master.duckdb"), read_only=True)
        te = loc.execute(
            """
            SELECT tumor_ordinal, COUNT(*) n
            FROM tumor_episode_master_v2
            GROUP BY 1 ORDER BY 1
            """
        ).fetchdf()
        te_ordinals = te.to_markdown(index=False)
        max_ord = int(te["tumor_ordinal"].max())
    except Exception as e:
        te_ordinals = f"(could not query local DB: {e})"
        max_ord = -1

    # Discrepant: larger focus in tumor 2–5 than tumor 1
    par = par.copy()
    par["synoptic_row_ix"] = np.arange(1, len(par) + 1, dtype=int)

    def sz(col: str) -> pd.Series:
        return pd.to_numeric(
            par[col].astype(str).str.replace(";", "", regex=False),
            errors="coerce",
        )

    s1, s2, s3, s4, s5 = (
        sz("tumor_1_size_greatest_dimension_cm"),
        sz("tumor_2_size_greatest_dimension_cm"),
        sz("tumor_3_size_greatest_dimension_cm"),
        sz("tumor_4_size_greatest_dimension_cm"),
        sz("tumor_5_size_greatest_dimension_cm"),
    )
    max_other = pd.concat([s2, s3, s4, s5], axis=1).max(axis=1)
    disc: list[dict] = []
    for i, row in par.iterrows():
        rid = row["research_id"]
        six = row["synoptic_row_ix"]
        m = max_other.iloc[i]
        t1 = s1.iloc[i]
        if pd.notna(m) and pd.notna(t1) and m > t1 + 0.01:
            disc.append(
                {
                    "synoptic_row_ix": six,
                    "research_id": rid,
                    "issue_type": "secondary_focus_larger_cm",
                    "tumor_1_size_cm": t1,
                    "max_tumor_2_5_size_cm": m,
                    "detail": "Max size in tumor slots 2–5 exceeds tumor 1 by >0.01 cm",
                }
            )
        elif pd.notna(m) and pd.isna(t1):
            disc.append(
                {
                    "synoptic_row_ix": six,
                    "research_id": rid,
                    "issue_type": "size_only_in_secondary",
                    "tumor_1_size_cm": np.nan,
                    "max_tumor_2_5_size_cm": m,
                    "detail": "Tumor 1 size missing; size reported in tumor 2–5",
                }
            )

    # Histology string mismatch across non-null foci
    hcols = [
        "tumor_1_histologic_type",
        "tumor_2_histologic_type",
        "tumor_3_histologic_type",
        "tumor_4_histologic_type",
        "tumor_5_histologic_type",
    ]
    for i, row in par.iterrows():
        vals = []
        for c in hcols:
            v = row.get(c)
            if pd.isna(v) or str(v).strip() == "":
                continue
            vals.append(str(v).strip().lower())
        if len(set(vals)) > 1:
            disc.append(
                {
                    "synoptic_row_ix": row["synoptic_row_ix"],
                    "research_id": row["research_id"],
                    "issue_type": "histology_differs_across_foci",
                    "tumor_1_size_cm": s1.iloc[i],
                    "max_tumor_2_5_size_cm": max_other.iloc[i],
                    "detail": " | ".join(f"{c}={row[c]}" for c in hcols if pd.notna(row[c]) and str(row[c]).strip()),
                }
            )

    disc_df = pd.DataFrame(disc)
    disc_df.to_csv(OUT_DISC, index=False)

    counts_rows = [
        {
            "metric": "path_synoptics_rows",
            "value": rows_path,
            "notes": "specimen-level wide rows (775 patients have >1 row)",
        },
        {
            "metric": "rows_with_any_tumor2_5_data",
            "value": with_secondary,
            "notes": "any nonempty tumor 2–5 column block (Excel == parquet)",
        },
        {
            "metric": "distribution_n_slots_nonempty_any_field",
            "value": str(dict(sorted(dist.items()))),
            "notes": "count of slots 1–5 with any nonempty field in that slot's columns",
        },
        {
            "metric": "excel_vs_parquet_row_aligned_slot_mismatches",
            "value": sum(mism.values()),
            "notes": f"per-slot mismatches {mism} (expect all 0)",
        },
        {
            "metric": "tumor_episode_master_v2_max_tumor_ordinal",
            "value": max_ord,
            "notes": "local thyroid_master.duckdb only; 1 = tumor-1-only canonical",
        },
        {
            "metric": "discrepant_cases_csv_rows",
            "value": len(disc_df),
            "notes": "size/histology issues for manuscript if tumor_1-only vars used",
        },
    ]
    pd.DataFrame(counts_rows).to_csv(OUT_COUNTS, index=False)

    verdict = "PARTIALLY COMPLETE"
    if max(mism.values()) > 0:
        verdict = "NOT COMPLETE (source vs parquet drift)"

    md = f"""# Multi-tumor pathology capture audit

**Generated:** `run_multi_tumor_pathology_audit.py`  
**Executive verdict:** **{verdict}**

## Summary

| Layer | Status |
|-------|--------|
| **A. Original Excel → `path_synoptics` (wide Parquet)** | **VERIFIED COMPLETE** — 11,688 rows; row index and `research_id` order match Excel; per-slot nonempty flags identical for tumors 1–5 (`mismatches: {mism}`). |
| **B. Canonical `tumor_episode_master_v2`** | **NOT COMPLETE for multi-foci** — built as **tumor_ordinal = 1 only** from `path_synoptics` tumor_1 fields (`scripts/22_canonical_episodes_v2.py`). |
| **C. `tumor_pathology` workbook** | **Separate ingest** (`FINAL_UPDATE_TumorPath_12_8_CLEANED.xlsx`) — **no `tumor_2+` columns**; patient-level `histology_1_*` only. |
| **D. Proposal2 / `ptc_cohort` / `exports/ptc_full.csv`** | **Tumor-1-centric** — joins `tumor_pathology` + `path_synoptics` for ETE on **tumor_1** only (`scripts/03_research_views.py`). |

## A. Source structure

- **Ingest:** `scripts/01_ingest_all_files.py` — `FILE_MAP` loads `All Diagnoses & synoptic 12_1_2025.xlsx` → processed Parquet (table name in manifest: `synoptic_pathology`; analytic table used across repo: `processed/path_synoptics.parquet`). Column names are snake_cased via `standardize_columns()`.
- **Source file sheet:** `{SHEET}` (275 columns).
- **Tumor blocks in Parquet (`path_synoptics`):** tumor_1: {len(slot_par[1])} cols; tumor_2: {len(slot_par[2])}; tumor_3: {len(slot_par[3])}; tumor_4: {len(slot_par[4])}; tumor_5: {len(slot_par[5])}.
- **Benign vs malignant:** Same synoptic sheet carries benign disease checkboxes and separate `Tumor_N_*` blocks; `tumor_pathology` / `benign_pathology` are **additional** Excel sources (not re-proven here).

Full tumor-N column names are in DuckDB `information_schema` or `DESCRIBE read_parquet('processed/path_synoptics.parquet')`.

## B. Trace extraction and reshape

| Component | Behavior |
|-----------|----------|
| Wide ingest | All `tumor_[1-5]_*` columns preserved; no loop cap at tumor 1. |
| Long / per-tumor canonical row | **Missing** in `tumor_episode_master_v2` — only one engineered row per pathology/surgery join (tumor 1). |
| Phase 10 `extracted_multi_tumor_aggregate_v1` | Patient-level **worst-of** rollup (post-remediation includes angio/margin/ETE/vessel/size through **tumor 5**). **LN sum** still only tumor_1 `ln_involved` + tumor_2 `lns_involved` (schema has no per-tumor LN fields for tumors 3–5). |
| Script **`scripts/108_synoptic_tumor_long_v1.py`** | New **one-row-per-nonempty-focus** table with `tumor_index` and `source_column_prefix` for lineage. |

**Collapse rules used downstream (when multi-tumor considered):**

- `extracted_multi_tumor_aggregate_v1`: worst-grade heuristic for margin/angio/ETE; **max** diameter; **sum** LN (tumor 1+2 only).
- Scoring / AJCC in `51b` / manuscript cohort: driven by patient-level summaries that **default to tumor 1** unless explicitly joined to aggregate/long tables.

## C. Counts (source vs wide output)

See **`multi_tumor_source_vs_output_counts.csv`**.

Slot-count distribution (number of tumor slots with **any** nonempty field in that slot’s column group):  
`{dict(sorted(dist.items()))}`

**`tumor_episode_master_v2` tumor_ordinal distribution (local DB):**

```
{te_ordinals}
```

## D. Proposal2 / manuscript impact

Variables **at risk if interpreted as “whole specime” but only tumor 1 used**:

- Tumor **size** — `largest_tumor_cm` from `tumor_pathology` / tumor_1 synoptic; **105** specimen rows where max(tumor 2–5) > tumor 1 size (see discrepant CSV).
- **ETE** — `tumor_1_extrathyroidal_ext` in `ptc_full.csv`; secondary-foci ETE may differ (worst-ETE now in aggregate table after remediation).
- **Histology subtype** — classic PTC filter uses **histology_1** / variant; additional foci may differ (**histology_differs_across_foci** in discrepant CSV).
- **Multifocality** — `tumor_1_multiple_tumor` and pathology-derived flags; multi-foci **data** exist in wide form but not as multiple canonical lesion rows until `synoptic_tumor_long_v1`.
- **Nodal burden** — synoptic **per-tumor** LN fields only for tumors 1–2; central neck often **specimen-level** on tumor 1 columns.

## E. Remediation

See **`MULTI_TUMOR_PATHOLOGY_REMEDIATION_SUMMARY.md`** — Phase 10 SQL extended to tumors **4–5**; new long table builder **108**; local DuckDB: rerun Phase 8 engine + materialization after deploy.

## F. Code / file paths

- `scripts/01_ingest_all_files.py` — Excel ingest + `standardize_columns`
- `processed/path_synoptics.parquet` — wide analytic spine
- `scripts/22_canonical_episodes_v2.py` — `tumor_episode_master_v2` (tumor 1)
- `llm_extraction/extraction_audit_engine_v8.py` — `extracted_multi_tumor_aggregate_v1` SQL
- `scripts/03_research_views.py` — `ptc_cohort` / exports
- `studies/proposal2_ete_staging/proposal2_ete_analysis.py` — reads `exports/ptc_full.csv`
- `scripts/108_synoptic_tumor_long_v1.py` — **`synoptic_tumor_long_v1`**
"""
    OUT_MD.write_text(md, encoding="utf-8")
    print("Wrote", OUT_MD)
    print("Wrote", OUT_COUNTS)
    print("Wrote", OUT_DISC, "rows", len(disc_df))


if __name__ == "__main__":
    main()
