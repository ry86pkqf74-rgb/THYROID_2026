#!/usr/bin/env python3
"""
103_fact_lineage_materialize.py — Canonical long-table of note-entity facts + episode inference

Creates:
  - processed/canonical_extracted_fact_long_v1.parquet (clean facts)
  - processed/canonical_fact_quarantine_v1.parquet (quarantined rows + reason)
  - DuckDB tables for the above + optional note_extraction_runs

Episode linkage: nearest operative_episode_detail_v2 row by |days| from
COALESCE(entity_date, clin_note_date) when that table exists (Parquet or DuckDB).

Usage:
  .venv/bin/python scripts/103_fact_lineage_materialize.py
  .venv/bin/python scripts/103_fact_lineage_materialize.py --dry-run
  .venv/bin/python scripts/103_fact_lineage_materialize.py --md
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.vocab import CANONICAL_FACT_CONTRACT_DTYPES  # noqa: E402
from utils.md_connect import connect_md_or_file  # noqa: E402
from utils.provenance import (  # noqa: E402
    LOW_LLM_DATE_CONF,
    MULTI_SURGERY_EP_DIST_THRESH_DAYS,
    TEMPORAL_CONFLICT_DAYS,
    apply_provenance_contract_columns,
    quarantine_masks,
    split_quarantine,
)
from utils.text_helpers import save_parquet  # noqa: E402

DB_PATH = ROOT / "thyroid_master.duckdb"
PROCESSED = ROOT / "processed"

ENTITY_DOMAIN_MAP: list[tuple[str, str]] = [
    ("note_entities_staging", "staging"),
    ("note_entities_genetics", "genetics"),
    ("note_entities_procedures", "procedures"),
    ("note_entities_operative_detail", "operative_detail"),
    ("note_entities_complications", "complications"),
    ("note_entities_medications", "medications"),
    ("note_entities_problem_list", "problem_list"),
    ("note_entities_llm", "llm"),
]

def multi_surgery_research_ids(op: pd.DataFrame | None) -> set[int]:
    if op is None or op.empty or "research_id" not in op.columns:
        return set()
    rid = pd.to_numeric(op["research_id"], errors="coerce").dropna().astype(int)
    vc = rid.value_counts()
    return {int(k) for k, n in vc.items() if n > 1}


def add_contract_columns(uni: pd.DataFrame, multi_surgery_rids: set[int]) -> pd.DataFrame:
    """Full provenance contract columns on the long frame (delegates to utils.provenance)."""
    return apply_provenance_contract_columns(uni, multi_surgery_rids)


def _load_op_episodes() -> pd.DataFrame | None:
    pq = PROCESSED / "operative_episode_detail_v2.parquet"
    if pq.exists():
        return pd.read_parquet(pq)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        con.execute("SELECT 1 FROM operative_episode_detail_v2 LIMIT 1")
    except Exception:
        con.close()
        return None
    df = con.execute(
        "SELECT research_id, surgery_episode_id, surgery_date FROM operative_episode_detail_v2"
    ).fetchdf()
    con.close()
    return df


def _infer_episodes(uni: pd.DataFrame, op: pd.DataFrame) -> pd.DataFrame:
    op = op.copy()
    op["research_id"] = pd.to_numeric(op["research_id"], errors="coerce").astype("Int64")
    op["_sd"] = pd.to_datetime(op["surgery_date"], errors="coerce")

    inferred_ep: list[int | None] = []
    inferred_dt: list[object | None] = []
    ep_dist: list[int | None] = []
    surgery_keys: list[str] = []

    for _, row in uni.iterrows():
        rid = pd.to_numeric(row.get("research_id"), errors="coerce")
        if pd.isna(rid):
            inferred_ep.append(None)
            inferred_dt.append(None)
            ep_dist.append(None)
            surgery_keys.append(str(row.get("research_id", "")))
            continue
        ref = row.get("entity_date")
        if ref is None or (isinstance(ref, float) and pd.isna(ref)):
            ref = row.get("clin_note_date")
        rt = pd.to_datetime(ref, errors="coerce")
        if pd.isna(rt):
            rt = pd.Timestamp("1900-01-01")
        sub = op[op["research_id"] == int(rid)]
        if sub.empty:
            inferred_ep.append(None)
            inferred_dt.append(None)
            ep_dist.append(None)
            surgery_keys.append(str(int(rid)))
            continue
        dlt = (sub["_sd"] - rt).abs().dt.days
        j = dlt.idxmin()
        best = sub.loc[j]
        se = best.get("surgery_episode_id")
        inferred_ep.append(int(se) if pd.notna(se) else None)
        inferred_dt.append(best["_sd"].date() if pd.notna(best["_sd"]) else None)
        ep_dist.append(int(dlt.loc[j]) if pd.notna(dlt.loc[j]) else None)
        surgery_keys.append(f"{int(rid)}:{int(se)}" if pd.notna(se) else str(int(rid)))

    uni = uni.copy()
    uni["inferred_surgery_episode_id"] = inferred_ep
    uni["inferred_surgery_date"] = inferred_dt
    uni["ep_distance_days"] = ep_dist
    uni["surgery_key"] = surgery_keys
    return uni


def _apply_contract_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Cast provenance contract columns for stable parquet types (best-effort)."""
    out = df
    for col, dtype in CANONICAL_FACT_CONTRACT_DTYPES.items():
        if col in out.columns:
            try:
                out[col] = out[col].astype(dtype)
            except (TypeError, ValueError):
                pass
    return out


def fact_id_for(i: int, row: pd.Series) -> str:
    key = "|".join(
        [
            str(row.get("research_id", "")),
            str(row.get("note_row_id", "")),
            str(row.get("entity_type", "")),
            str(row.get("entity_value_raw", "")),
            str(row.get("fact_domain", "")),
            str(i),
        ]
    )
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--md",
        action="store_true",
        help="Open DuckDB via MotherDuck when token/env is available (else local file)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("  103 — canonical_extracted_fact_long_v1 + quarantine")
    print("=" * 70)

    frames: list[pd.DataFrame] = []
    for stem, domain in ENTITY_DOMAIN_MAP:
        pq = PROCESSED / f"{stem}.parquet"
        if not pq.exists():
            print(f"  skip (no parquet): {stem}")
            continue
        df = pd.read_parquet(pq).copy()
        df["fact_domain"] = domain
        frames.append(df)
        print(f"  loaded {stem}: {len(df):,} rows")

    if not frames:
        print("  ERROR: no note_entities_*.parquet under processed/")
        sys.exit(1)

    all_cols: set[str] = set()
    for df in frames:
        all_cols |= set(df.columns)
    aligned = []
    for df in frames:
        for c in all_cols - set(df.columns):
            df[c] = None
        aligned.append(df[sorted(all_cols)])

    uni = pd.concat(aligned, ignore_index=True)
    uni["_fact_rn"] = range(len(uni))

    notes_path = PROCESSED / "clinical_notes_long.parquet"
    if notes_path.exists():
        notes = pd.read_parquet(notes_path)
        nc = [
            c
            for c in (
                "note_row_id",
                "note_date",
                "source_sheet",
                "source_column",
                "source_workbook",
                "excel_row_0based",
            )
            if c in notes.columns
        ]
        notes = notes[nc].drop_duplicates(subset=["note_row_id"])
        notes = notes.rename(
            columns={
                "note_date": "clin_note_date",
                "source_sheet": "clin_source_sheet",
                "source_column": "clin_source_column",
                "source_workbook": "clin_source_workbook",
                "excel_row_0based": "clin_excel_row_0based",
            }
        )
        uni = uni.merge(notes, on="note_row_id", how="left")
        print("  merged clinical_notes_long")
    else:
        uni["clin_note_date"] = None
        uni["clin_source_sheet"] = None
        uni["clin_source_column"] = None
        uni["clin_source_workbook"] = None
        uni["clin_excel_row_0based"] = None

    op = _load_op_episodes()
    multi_ids = multi_surgery_research_ids(op)
    print(f"  multi-surgery patients (operative_episode_detail_v2): {len(multi_ids):,}")

    if op is not None and "surgery_episode_id" in op.columns:
        uni = _infer_episodes(uni, op)
        print("  inferred surgery_episode_id / surgery_key")
    else:
        uni["inferred_surgery_episode_id"] = None
        uni["inferred_surgery_date"] = None
        uni["ep_distance_days"] = None
        uni["surgery_key"] = uni["research_id"].astype(str)

    uni = add_contract_columns(uni, multi_ids)

    uni["fact_id"] = [fact_id_for(i, uni.iloc[i]) for i in range(len(uni))]

    clean, quar = split_quarantine(uni, multi_ids)
    print(f"  quarantine split: clean={len(clean):,} quarantined={len(quar):,}")

    out_pq = PROCESSED / "canonical_extracted_fact_long_v1.parquet"
    out_q = PROCESSED / "canonical_fact_quarantine_v1.parquet"
    if args.dry_run:
        print(f"  dry-run: would write {len(clean):,} rows → {out_pq}")
        print(f"  dry-run: would write {len(quar):,} rows → {out_q}")
        return

    clean = _apply_contract_dtypes(clean)
    quar = _apply_contract_dtypes(quar)
    save_parquet(clean, out_pq)
    save_parquet(quar, out_q)

    con = connect_md_or_file(DB_PATH, md=args.md)
    con.execute(
        f"CREATE OR REPLACE TABLE canonical_extracted_fact_long_v1 AS "
        f"SELECT * FROM read_parquet('{out_pq}')"
    )
    con.execute(
        f"CREATE OR REPLACE TABLE canonical_fact_quarantine_v1 AS "
        f"SELECT * FROM read_parquet('{out_q}')"
    )
    runs_pq = PROCESSED / "note_extraction_runs.parquet"
    if runs_pq.exists():
        con.execute(
            f"CREATE OR REPLACE TABLE note_extraction_runs AS "
            f"SELECT * FROM read_parquet('{runs_pq}')"
        )
        rn = con.execute("SELECT COUNT(*) FROM note_extraction_runs").fetchone()[0]
        print(f"  DuckDB note_extraction_runs rows: {rn:,}")

    n = con.execute("SELECT COUNT(*) FROM canonical_extracted_fact_long_v1").fetchone()[0]
    nq = con.execute("SELECT COUNT(*) FROM canonical_fact_quarantine_v1").fetchone()[0]
    con.close()
    print(f"  wrote {out_pq.name}; DuckDB canonical rows: {n:,}")
    print(f"  wrote {out_q.name}; DuckDB quarantine rows: {nq:,}")
    print("=" * 70)


if __name__ == "__main__":
    main()
