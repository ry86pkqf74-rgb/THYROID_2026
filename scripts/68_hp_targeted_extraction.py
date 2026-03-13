#!/usr/bin/env python3
"""
Script 68: Targeted H&P Extraction — Smoking Status + BMI

Runs SmokingStatusExtractor and BMIExtractor against clinical_notes_long
(h_p and endocrine_note types), creates sidecar extraction tables on
MotherDuck, validates precision, and exports review CSVs.

Tables created:
  - extracted_smoking_status_v1       (long-format, per-mention)
  - extracted_bmi_v1                  (long-format, per-mention)
  - patient_smoking_status_summary_v1 (one row per patient)
  - patient_bmi_summary_v1           (one row per patient)
  - review_smoking_ambiguous_v1      (uncertain/conflicting cases)
  - review_bmi_outlier_v1            (outlier BMI values)
  - val_hp_targeted_extraction_v1    (validation summary)

Usage:
  .venv/bin/python scripts/68_hp_targeted_extraction.py --md
  .venv/bin/python scripts/68_hp_targeted_extraction.py --local --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notes_extraction.base import EntityMatch
from notes_extraction.extract_hp_targeted import (
    BMIExtractor,
    SmokingStatusExtractor,
)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
EXPORT_DIR = Path(f"exports/hp_targeted_extraction_{TIMESTAMP}")
PHI_SNIPPET_LEN = 80

random.seed(42)


def get_connection(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        token = os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            import toml
            token = toml.load(".streamlit/secrets.toml")["MOTHERDUCK_TOKEN"]
            os.environ["MOTHERDUCK_TOKEN"] = token
        return duckdb.connect("md:thyroid_research_2026")
    return duckdb.connect("thyroid_master.duckdb")


def load_notes(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load H&P + endocrine notes for extraction."""
    print("Loading clinical notes (h_p, endocrine_note)...")
    df = con.execute("""
        SELECT
            CAST(note_row_id AS VARCHAR) AS note_row_id,
            CAST(research_id AS INTEGER) AS research_id,
            note_type,
            note_text,
            note_date
        FROM clinical_notes_long
        WHERE note_type IN ('h_p', 'endocrine_note')
          AND note_text IS NOT NULL
          AND LENGTH(TRIM(note_text)) > 50
        ORDER BY research_id, note_type, note_index
    """).df()
    print(f"  Loaded {len(df):,} notes from {df['research_id'].nunique():,} patients")
    return df


def run_extractors(notes_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run smoking + BMI extractors, return separate DataFrames."""
    smoking_ext = SmokingStatusExtractor()
    bmi_ext = BMIExtractor()

    smoking_rows: list[dict] = []
    bmi_rows: list[dict] = []

    total = len(notes_df)
    for i, (_, row) in enumerate(notes_df.iterrows()):
        if (i + 1) % 500 == 0:
            print(f"  Processing note {i+1:,}/{total:,}...")

        note_row_id = str(row["note_row_id"])
        research_id = int(row["research_id"])
        note_type = str(row["note_type"])
        note_text = str(row["note_text"])
        note_date = row.get("note_date")
        if pd.isna(note_date):
            note_date = None
        else:
            note_date = str(note_date)

        for match in smoking_ext.extract(
            note_row_id, research_id, note_type, note_text, note_date
        ):
            smoking_rows.append(match.to_dict())

        for match in bmi_ext.extract(
            note_row_id, research_id, note_type, note_text, note_date
        ):
            bmi_rows.append(match.to_dict())

    smoking_df = pd.DataFrame(smoking_rows) if smoking_rows else pd.DataFrame(
        columns=list(EntityMatch.__dataclass_fields__.keys())
    )
    bmi_df = pd.DataFrame(bmi_rows) if bmi_rows else pd.DataFrame(
        columns=list(EntityMatch.__dataclass_fields__.keys())
    )

    print("\n=== EXTRACTION RESULTS ===")
    print(f"  Smoking: {len(smoking_df):,} mentions from {smoking_df['research_id'].nunique() if len(smoking_df) else 0:,} patients")
    print(f"  BMI:     {len(bmi_df):,} mentions from {bmi_df['research_id'].nunique() if len(bmi_df) else 0:,} patients")

    return smoking_df, bmi_df


def build_patient_smoking_summary(smoking_df: pd.DataFrame) -> pd.DataFrame:
    """Roll up to one row per patient with best smoking status."""
    if smoking_df.empty:
        return pd.DataFrame()

    status_rows = smoking_df[smoking_df["entity_type"] == "smoking_status"].copy()
    pack_year_rows = smoking_df[smoking_df["entity_type"] == "pack_years"].copy()
    ppd_rows = smoking_df[smoking_df["entity_type"] == "packs_per_day"].copy()

    priority = {"current_smoker": 0, "former_smoker": 1, "passive_exposure": 2, "never_smoker": 3, "unknown": 4}

    patient_rows: list[dict] = []
    for rid, grp in status_rows.groupby("research_id"):
        norms = grp["entity_value_norm"].unique().tolist()
        best = min(norms, key=lambda x: priority.get(x, 99))

        conflicting = len(set(norms) - {"unknown"}) > 1
        n_mentions = len(grp)
        best_conf = float(grp.loc[grp["entity_value_norm"] == best, "confidence"].max())

        py_grp = pack_year_rows[pack_year_rows["research_id"] == rid]
        pack_years = None
        if not py_grp.empty:
            try:
                pack_years = float(py_grp["entity_value_norm"].astype(float).max())
            except (ValueError, TypeError):
                pass

        ppd_grp = ppd_rows[ppd_rows["research_id"] == rid]
        ppd_val = None
        if not ppd_grp.empty:
            try:
                ppd_val = float(ppd_grp["entity_value_norm"].astype(float).max())
            except (ValueError, TypeError):
                pass

        patient_rows.append({
            "research_id": int(rid),
            "smoking_status_final": best,
            "smoking_status_conflicting": conflicting,
            "smoking_mention_count": n_mentions,
            "smoking_confidence": best_conf,
            "pack_years": pack_years,
            "packs_per_day": ppd_val,
            "all_statuses_found": "|".join(sorted(set(norms))),
        })

    return pd.DataFrame(patient_rows)


def build_patient_bmi_summary(bmi_df: pd.DataFrame) -> pd.DataFrame:
    """Roll up to one row per patient with BMI value."""
    if bmi_df.empty:
        return pd.DataFrame()

    val_rows = bmi_df[bmi_df["entity_type"] == "bmi_value"].copy()
    cat_rows = bmi_df[bmi_df["entity_type"] == "bmi_category"].copy()

    patient_rows: list[dict] = []
    all_rids = set(val_rows["research_id"].unique()) | set(cat_rows["research_id"].unique())

    for rid in sorted(all_rids):
        v_grp = val_rows[val_rows["research_id"] == rid]
        c_grp = cat_rows[cat_rows["research_id"] == rid]

        bmi_value = None
        bmi_confidence = 0.0
        if not v_grp.empty:
            try:
                vals = v_grp["entity_value_norm"].astype(float)
                bmi_value = float(vals.iloc[0])
                bmi_confidence = float(v_grp["confidence"].max())
            except (ValueError, TypeError):
                pass

        bmi_category = None
        if not c_grp.empty:
            bmi_category = c_grp["entity_value_norm"].iloc[0]
            if bmi_confidence == 0:
                bmi_confidence = float(c_grp["confidence"].max())

        if bmi_value is None and bmi_category is None:
            continue

        is_outlier = bmi_value is not None and (bmi_value < 15.0 or bmi_value > 65.0)

        patient_rows.append({
            "research_id": int(rid),
            "bmi_value": bmi_value,
            "bmi_category": bmi_category,
            "bmi_confidence": bmi_confidence,
            "bmi_is_outlier": is_outlier,
            "bmi_mention_count": len(v_grp) + len(c_grp),
        })

    return pd.DataFrame(patient_rows)


def build_review_queues(
    smoking_df: pd.DataFrame,
    bmi_df: pd.DataFrame,
    smoking_summary: pd.DataFrame,
    bmi_summary: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build review queues for ambiguous smoking and outlier BMI."""
    smoking_review = pd.DataFrame()
    if not smoking_summary.empty:
        ambig = smoking_summary[
            smoking_summary["smoking_status_conflicting"]
            | (smoking_summary["smoking_status_final"] == "unknown")
        ].copy()
        if not ambig.empty:
            ambig_details = []
            for _, row in ambig.iterrows():
                rid = row["research_id"]
                mentions = smoking_df[smoking_df["research_id"] == rid]
                for _, m in mentions.iterrows():
                    ambig_details.append({
                        "research_id": int(rid),
                        "entity_type": m["entity_type"],
                        "entity_value_norm": m["entity_value_norm"],
                        "evidence_span": str(m["evidence_span"])[:PHI_SNIPPET_LEN],
                        "note_type": m["note_type"],
                        "confidence": m["confidence"],
                        "review_reason": "conflicting" if row["smoking_status_conflicting"] else "unknown_status",
                    })
            smoking_review = pd.DataFrame(ambig_details)

    bmi_review = pd.DataFrame()
    if not bmi_summary.empty:
        outlier = bmi_summary[bmi_summary["bmi_is_outlier"]].copy()
        if not outlier.empty:
            outlier_details = []
            for _, row in outlier.iterrows():
                rid = row["research_id"]
                mentions = bmi_df[bmi_df["research_id"] == rid]
                for _, m in mentions.iterrows():
                    outlier_details.append({
                        "research_id": int(rid),
                        "entity_type": m["entity_type"],
                        "entity_value_norm": m["entity_value_norm"],
                        "evidence_span": str(m["evidence_span"])[:PHI_SNIPPET_LEN],
                        "note_type": m["note_type"],
                        "confidence": m["confidence"],
                        "review_reason": "bmi_outlier",
                    })
            bmi_review = pd.DataFrame(outlier_details)

    return smoking_review, bmi_review


def precision_sample(df: pd.DataFrame, entity_col: str, n: int = 50) -> pd.DataFrame:
    """Draw a random precision-review sample."""
    if df.empty or len(df) < n:
        return df.copy()
    return df.sample(n=n, random_state=42)


def validate_and_report(
    smoking_df: pd.DataFrame,
    bmi_df: pd.DataFrame,
    smoking_summary: pd.DataFrame,
    bmi_summary: pd.DataFrame,
    smoking_review: pd.DataFrame,
    bmi_review: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """Build validation table and markdown report."""
    rows = []

    sm_status = smoking_df[smoking_df["entity_type"] == "smoking_status"] if not smoking_df.empty else pd.DataFrame()
    sm_py = smoking_df[smoking_df["entity_type"] == "pack_years"] if not smoking_df.empty else pd.DataFrame()
    sm_ppd = smoking_df[smoking_df["entity_type"] == "packs_per_day"] if not smoking_df.empty else pd.DataFrame()
    bmi_val = bmi_df[bmi_df["entity_type"] == "bmi_value"] if not bmi_df.empty else pd.DataFrame()
    bmi_cat = bmi_df[bmi_df["entity_type"] == "bmi_category"] if not bmi_df.empty else pd.DataFrame()

    rows.append({
        "variable": "smoking_status",
        "total_mentions": len(sm_status),
        "unique_patients": sm_status["research_id"].nunique() if not sm_status.empty else 0,
        "conflicting_patients": int(smoking_summary["smoking_status_conflicting"].sum()) if not smoking_summary.empty else 0,
        "review_queue_size": len(smoking_review),
    })
    rows.append({
        "variable": "pack_years",
        "total_mentions": len(sm_py),
        "unique_patients": sm_py["research_id"].nunique() if not sm_py.empty else 0,
        "conflicting_patients": 0,
        "review_queue_size": 0,
    })
    rows.append({
        "variable": "packs_per_day",
        "total_mentions": len(sm_ppd),
        "unique_patients": sm_ppd["research_id"].nunique() if not sm_ppd.empty else 0,
        "conflicting_patients": 0,
        "review_queue_size": 0,
    })
    rows.append({
        "variable": "bmi_value",
        "total_mentions": len(bmi_val),
        "unique_patients": bmi_val["research_id"].nunique() if not bmi_val.empty else 0,
        "conflicting_patients": 0,
        "review_queue_size": len(bmi_review),
    })
    rows.append({
        "variable": "bmi_category",
        "total_mentions": len(bmi_cat),
        "unique_patients": bmi_cat["research_id"].nunique() if not bmi_cat.empty else 0,
        "conflicting_patients": 0,
        "review_queue_size": 0,
    })

    val_df = pd.DataFrame(rows)

    sm_dist = {}
    if not smoking_summary.empty:
        sm_dist = smoking_summary["smoking_status_final"].value_counts().to_dict()

    bmi_stats: dict[str, float] = {}
    if not bmi_summary.empty and bmi_summary["bmi_value"].notna().any():
        bv = bmi_summary["bmi_value"].dropna()
        bmi_stats = {
            "mean": round(float(bv.mean()), 1),
            "median": round(float(bv.median()), 1),
            "min": round(float(bv.min()), 1),
            "max": round(float(bv.max()), 1),
            "std": round(float(bv.std()), 1),
        }

    report = f"""# Targeted H&P Extraction Report: Smoking Status + BMI

**Date**: {datetime.now().strftime("%Y-%m-%d %H:%M")}
**Source notes**: h_p + endocrine_note from clinical_notes_long

---

## Extraction Summary

| Variable | Mentions | Patients | Conflicting | Review Queue |
|----------|----------|----------|-------------|--------------|
"""
    for _, r in val_df.iterrows():
        report += f"| {r['variable']} | {r['total_mentions']:,} | {r['unique_patients']:,} | {r['conflicting_patients']} | {r['review_queue_size']} |\n"

    report += """
---

## Smoking Status Distribution

| Status | Patients | % |
|--------|----------|---|
"""
    sm_total = sum(sm_dist.values()) if sm_dist else 1
    for status in ["current_smoker", "former_smoker", "never_smoker", "passive_exposure", "unknown"]:
        n = sm_dist.get(status, 0)
        report += f"| {status} | {n:,} | {100*n/max(sm_total,1):.1f}% |\n"

    report += f"""
**Pack-years extracted**: {sm_py['research_id'].nunique() if not sm_py.empty else 0} patients
**PPD extracted**: {sm_ppd['research_id'].nunique() if not sm_ppd.empty else 0} patients

---

## BMI Statistics

| Metric | Value |
|--------|-------|
"""
    for k, v in bmi_stats.items():
        report += f"| {k} | {v} |\n"

    if not bmi_summary.empty:
        report += f"""
**BMI patients**: {len(bmi_summary):,} (numeric: {bmi_summary['bmi_value'].notna().sum():,}, category-only: {(bmi_summary['bmi_value'].isna() & bmi_summary['bmi_category'].notna()).sum():,})
**Outlier BMI (< 15 or > 65)**: {bmi_summary['bmi_is_outlier'].sum()} patients

---

## BMI Category Distribution

| Category | Patients |
|----------|----------|
"""
        if bmi_summary["bmi_category"].notna().any():
            for cat, n in bmi_summary["bmi_category"].dropna().value_counts().items():
                report += f"| {cat} | {n:,} |\n"

    report += f"""
---

## Precision Review

### Smoking Status — Random Sample (n=50)

Evidence spans are truncated to {PHI_SNIPPET_LEN} chars for PHI safety.

"""
    sm_sample = precision_sample(sm_status, "entity_value_norm", 50) if not sm_status.empty else pd.DataFrame()
    if not sm_sample.empty:
        report += "| research_id | norm | evidence (truncated) | confidence |\n"
        report += "|-------------|------|----------------------|------------|\n"
        for _, r in sm_sample.head(20).iterrows():
            ev = str(r.get("evidence_span", ""))[:PHI_SNIPPET_LEN].replace("|", "/")
            report += f"| {r['research_id']} | {r['entity_value_norm']} | {ev} | {r['confidence']:.2f} |\n"
        report += f"\n*Showing 20 of {len(sm_sample)} sampled rows.*\n"

    report += """
### BMI — Random Sample (n=50)

"""
    bmi_sample = precision_sample(bmi_val, "entity_value_norm", 50) if not bmi_val.empty else pd.DataFrame()
    if not bmi_sample.empty:
        report += "| research_id | value | evidence (truncated) | confidence |\n"
        report += "|-------------|-------|----------------------|------------|\n"
        for _, r in bmi_sample.head(20).iterrows():
            ev = str(r.get("evidence_span", ""))[:PHI_SNIPPET_LEN].replace("|", "/")
            report += f"| {r['research_id']} | {r['entity_value_norm']} | {ev} | {r['confidence']:.2f} |\n"
        report += f"\n*Showing 20 of {len(bmi_sample)} sampled rows.*\n"

    report += f"""
---

## Deliverables

### MotherDuck Tables
1. `extracted_smoking_status_v1` — per-mention smoking extraction
2. `extracted_bmi_v1` — per-mention BMI extraction
3. `patient_smoking_status_summary_v1` — one row per patient
4. `patient_bmi_summary_v1` — one row per patient
5. `review_smoking_ambiguous_v1` — conflicting/unknown cases
6. `review_bmi_outlier_v1` — outlier BMI values for review
7. `val_hp_targeted_extraction_v1` — validation summary

### Export Bundle
`{EXPORT_DIR}/`
"""
    return val_df, report


def create_tables(
    con: duckdb.DuckDBPyConnection,
    smoking_df: pd.DataFrame,
    bmi_df: pd.DataFrame,
    smoking_summary: pd.DataFrame,
    bmi_summary: pd.DataFrame,
    smoking_review: pd.DataFrame,
    bmi_review: pd.DataFrame,
    val_df: pd.DataFrame,
) -> None:
    """Create sidecar tables on MotherDuck."""
    print("\n=== CREATING MOTHERDUCK TABLES ===")

    table_map = {
        "extracted_smoking_status_v1": smoking_df,
        "extracted_bmi_v1": bmi_df,
        "patient_smoking_status_summary_v1": smoking_summary,
        "patient_bmi_summary_v1": bmi_summary,
        "review_smoking_ambiguous_v1": smoking_review,
        "review_bmi_outlier_v1": bmi_review,
        "val_hp_targeted_extraction_v1": val_df,
    }

    for tbl_name, df in table_map.items():
        if df.empty:
            print(f"  Skipping {tbl_name} (empty)")
            continue
        try:
            tmp = Path(f"/tmp/{tbl_name}.parquet")
            df.to_parquet(tmp, index=False)
            con.execute(f"DROP TABLE IF EXISTS {tbl_name}")
            con.execute(f"CREATE TABLE {tbl_name} AS SELECT * FROM read_parquet('{tmp}')")
            row_count = con.execute(f"SELECT COUNT(*) FROM {tbl_name}").fetchone()[0]
            print(f"  Created {tbl_name}: {row_count:,} rows")
            tmp.unlink(missing_ok=True)
        except Exception as e:
            print(f"  ERROR creating {tbl_name}: {e}")


def export_csvs(
    smoking_df: pd.DataFrame,
    bmi_df: pd.DataFrame,
    smoking_summary: pd.DataFrame,
    bmi_summary: pd.DataFrame,
    smoking_review: pd.DataFrame,
    bmi_review: pd.DataFrame,
    val_df: pd.DataFrame,
    report: str,
) -> None:
    """Export all artifacts to CSV."""
    print(f"\n=== EXPORTING TO {EXPORT_DIR} ===")
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    if not smoking_df.empty:
        out = smoking_df.copy()
        out["evidence_span"] = out["evidence_span"].str[:PHI_SNIPPET_LEN]
        out.to_csv(EXPORT_DIR / "extracted_smoking_status_v1.csv", index=False)
    if not bmi_df.empty:
        out = bmi_df.copy()
        out["evidence_span"] = out["evidence_span"].str[:PHI_SNIPPET_LEN]
        out.to_csv(EXPORT_DIR / "extracted_bmi_v1.csv", index=False)
    if not smoking_summary.empty:
        smoking_summary.to_csv(EXPORT_DIR / "patient_smoking_status_summary_v1.csv", index=False)
    if not bmi_summary.empty:
        bmi_summary.to_csv(EXPORT_DIR / "patient_bmi_summary_v1.csv", index=False)
    if not smoking_review.empty:
        smoking_review.to_csv(EXPORT_DIR / "review_smoking_ambiguous_v1.csv", index=False)
    if not bmi_review.empty:
        bmi_review.to_csv(EXPORT_DIR / "review_bmi_outlier_v1.csv", index=False)
    if not val_df.empty:
        val_df.to_csv(EXPORT_DIR / "val_hp_targeted_extraction_v1.csv", index=False)

    manifest = {
        "timestamp": TIMESTAMP,
        "smoking_mentions": len(smoking_df),
        "smoking_patients": int(smoking_summary["research_id"].nunique()) if not smoking_summary.empty else 0,
        "bmi_mentions": len(bmi_df),
        "bmi_patients": int(bmi_summary["research_id"].nunique()) if not bmi_summary.empty else 0,
        "smoking_review_queue": len(smoking_review),
        "bmi_review_queue": len(bmi_review),
        "files": [f.name for f in EXPORT_DIR.iterdir() if f.suffix == ".csv"],
    }
    (EXPORT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"  Exported {len(manifest['files'])} CSV files + manifest")


def main() -> None:
    parser = argparse.ArgumentParser(description="Targeted H&P extraction: smoking + BMI")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Extract only, skip table creation")
    args = parser.parse_args()

    use_md = args.md or not args.local
    con = get_connection(use_md)
    print(f"Connected to {'MotherDuck' if use_md else 'local DuckDB'}")

    notes_df = load_notes(con)
    smoking_df, bmi_df = run_extractors(notes_df)

    smoking_summary = build_patient_smoking_summary(smoking_df)
    bmi_summary = build_patient_bmi_summary(bmi_df)
    smoking_review, bmi_review = build_review_queues(
        smoking_df, bmi_df, smoking_summary, bmi_summary
    )

    val_df, report = validate_and_report(
        smoking_df, bmi_df, smoking_summary, bmi_summary, smoking_review, bmi_review,
    )

    if not args.dry_run:
        create_tables(
            con, smoking_df, bmi_df, smoking_summary, bmi_summary,
            smoking_review, bmi_review, val_df,
        )
        export_csvs(
            smoking_df, bmi_df, smoking_summary, bmi_summary,
            smoking_review, bmi_review, val_df, report,
        )

    report_path = Path(f"docs/hp_targeted_extraction_{datetime.now().strftime('%Y%m%d')}.md")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report)
    print(f"\n  Report written to {report_path}")

    print("\n" + report)
    con.close()


if __name__ == "__main__":
    main()
