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
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

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

# Quarantine: multi-surgery patients with weak episode match (days) or missing distance
MULTI_SURGERY_EP_DIST_THRESH_DAYS = 90
LOW_LLM_DATE_CONF = 0.35
TEMPORAL_CONFLICT_DAYS = 730


def multi_surgery_research_ids(op: pd.DataFrame | None) -> set[int]:
    if op is None or op.empty or "research_id" not in op.columns:
        return set()
    rid = pd.to_numeric(op["research_id"], errors="coerce").dropna().astype(int)
    vc = rid.value_counts()
    return {int(k) for k, n in vc.items() if n > 1}


def _hash_evidence_span(val: object) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def add_contract_columns(uni: pd.DataFrame, multi_surgery_rids: set[int]) -> pd.DataFrame:
    """Full provenance contract columns on the long frame."""
    out = uni.copy()
    out["source_file_id"] = (
        out["clin_source_workbook"] if "clin_source_workbook" in out.columns else None
    )
    out["canonical_domain"] = out["fact_domain"]
    out["canonical_fact_type"] = out["entity_type"]

    eg0 = pd.to_numeric(out["evidence_global_start"], errors="coerce") if "evidence_global_start" in out.columns else None
    eg1 = pd.to_numeric(out["evidence_global_end"], errors="coerce") if "evidence_global_end" in out.columns else None
    out["source_text_span_start"] = eg0
    out["source_text_span_end"] = eg1

    if "evidence_span" in out.columns:
        out["source_text_hash"] = out["evidence_span"].apply(_hash_evidence_span)
    else:
        out["source_text_hash"] = None

    em = out["extraction_method"].astype(str)
    dc = pd.to_numeric(out.get("date_confidence"), errors="coerce")
    dst: list[str] = []
    for i in range(len(out)):
        method = em.iloc[i]
        dcv = dc.iloc[i] if len(dc) else np.nan
        if not str(method).startswith("llm"):
            dst.append("regex_extractor")
        elif pd.isna(dcv) or float(dcv) <= 0:
            dst.append("unknown")
        elif float(dcv) >= 0.99:
            dst.append("explicit_lab")
        elif float(dcv) >= 0.5:
            dst.append("note_body")
        else:
            dst.append("encounter_fallback")
    out["date_source_type"] = dst

    epd = (
        pd.to_numeric(out["ep_distance_days"], errors="coerce")
        if "ep_distance_days" in out.columns
        else pd.Series(pd.NA, index=out.index, dtype="Float64")
    )
    rid = pd.to_numeric(out["research_id"], errors="coerce")
    has_ep = (
        out["inferred_surgery_episode_id"].notna()
        if "inferred_surgery_episode_id" in out.columns
        else pd.Series(False, index=out.index)
    )
    lc: list[float] = []
    for i in range(len(out)):
        r = int(rid.iloc[i]) if pd.notna(rid.iloc[i]) else -1
        multi = r in multi_surgery_rids
        he = bool(has_ep.iloc[i])
        d_raw = epd.iloc[i]
        d = float(d_raw) if pd.notna(d_raw) else None
        if not he:
            lc.append(0.0)
            continue
        if d is None:
            lc.append(round(0.35 if multi else 0.55, 4))
            continue
        score = max(0.0, 1.0 - min(d, 180.0) / 180.0)
        if multi and d > MULTI_SURGERY_EP_DIST_THRESH_DAYS:
            score *= 0.65
        if multi and d > 120:
            score *= 0.85
        lc.append(round(float(score), 4))
    out["linkage_confidence"] = lc
    return out


def quarantine_masks(
    uni: pd.DataFrame,
    multi_surgery_rids: set[int],
    *,
    multi_ep_dist_days: int = MULTI_SURGERY_EP_DIST_THRESH_DAYS,
    low_llm_date_conf: float = LOW_LLM_DATE_CONF,
    temporal_conflict_days: int = TEMPORAL_CONFLICT_DAYS,
) -> tuple[pd.Series, pd.Series]:
    """
    Return (quarantine_bool_series, reason_series) aligned to uni.index.
    First matching rule wins for reason text.
    """
    q = pd.Series(False, index=uni.index)
    reason = pd.Series("", index=uni.index, dtype=object)

    rid = pd.to_numeric(uni["research_id"], errors="coerce")
    multi_mask = rid.apply(lambda x: int(x) in multi_surgery_rids if pd.notna(x) else False)
    epd = pd.to_numeric(uni["ep_distance_days"], errors="coerce")

    m1 = multi_mask & (epd.isna() | (epd > multi_ep_dist_days))
    q = q | m1
    reason = reason.where(~m1, "multi_surgery_episode_ambiguous")

    em = uni["extraction_method"].astype(str)
    dc = pd.to_numeric(uni.get("date_confidence"), errors="coerce")
    has_ed = uni["entity_date"].notna() if "entity_date" in uni.columns else pd.Series(False, index=uni.index)
    m2 = em.str.startswith("llm") & has_ed & dc.notna() & (dc < low_llm_date_conf)
    m2 = m2 & ~m1
    q = q | m2
    reason = reason.where(~m2, "low_confidence_llm_date")

    ed = pd.to_datetime(uni["entity_date"], errors="coerce") if "entity_date" in uni.columns else pd.Series(pd.NaT, index=uni.index)
    sd = pd.to_datetime(uni["inferred_surgery_date"], errors="coerce") if "inferred_surgery_date" in uni.columns else pd.Series(pd.NaT, index=uni.index)
    delta = (ed - sd).abs().dt.days
    m3 = ed.notna() & sd.notna() & delta.notna() & (delta > temporal_conflict_days)
    m3 = m3 & ~q
    q = q | m3
    reason = reason.where(~m3, "temporal_conflict_entity_vs_surgery")

    return q, reason


def split_quarantine(uni: pd.DataFrame, multi_surgery_rids: set[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    q, reason = quarantine_masks(uni, multi_surgery_rids)
    qdate = datetime.now(timezone.utc).date().isoformat()
    quar = uni.loc[q].copy()
    quar["quarantine_reason"] = reason.loc[q]
    quar["quarantine_date"] = qdate
    clean = uni.loc[~q].copy()
    return clean, quar


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

    save_parquet(clean, out_pq)
    save_parquet(quar, out_q)

    con = duckdb.connect(str(DB_PATH))
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
