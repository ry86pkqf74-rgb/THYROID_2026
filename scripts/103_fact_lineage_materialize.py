#!/usr/bin/env python3
"""
103_fact_lineage_materialize.py — Canonical long-table of note-entity facts + episode inference

Registry-driven: reads config/extraction_domain_registry.yaml to discover all
fact-capable domains.  Falls back to hardcoded list when the registry is unavailable.

Creates:
  v1 (preserved, unchanged schema):
    - processed/canonical_extracted_fact_long_v1.parquet
    - processed/canonical_fact_quarantine_v1.parquet
  v2 (all domains, family-specific linkage):
    - processed/canonical_extracted_fact_long_v2.parquet
    - processed/canonical_fact_quarantine_v2.parquet
  DuckDB tables for all four + optional note_extraction_runs
  QC report: exports/fact_lineage_qc/qc_report_YYYYMMDD_HHMM.md

Episode linkage rules by domain family:
  surgery_anchored  — nearest operative_episode_detail_v2 row
  pathology_anchored — nearest tumor_episode_master_v2 row
  imaging_anchored  — nearest imaging_exam_summary_v2 row
  longitudinal/outcome — nearest surgery but relaxed window (365d)

Ambiguous or multi-episode conflicts are routed into quarantine/manual review.

Usage:
  .venv/bin/python scripts/103_fact_lineage_materialize.py
  .venv/bin/python scripts/103_fact_lineage_materialize.py --dry-run
  .venv/bin/python scripts/103_fact_lineage_materialize.py --md
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

from llm_extraction.vocab import CANONICAL_FACT_CONTRACT_DTYPES  # noqa: E402
from utils.md_connect import connect_md_or_file  # noqa: E402
from utils.provenance import (  # noqa: E402
    apply_provenance_contract_columns,
    quarantine_masks,
)
from utils.text_helpers import save_parquet  # noqa: E402

DB_PATH = ROOT / "thyroid_master.duckdb"
PROCESSED = ROOT / "processed"
QC_DIR = ROOT / "exports" / "fact_lineage_qc"

# ---------------------------------------------------------------------------
# Linkage anchor family → episode source mapping
# ---------------------------------------------------------------------------
FAMILY_EPISODE_SOURCE = {
    "operative": "operative_episode_detail_v2",
    "pathology": "tumor_episode_master_v2",
    "molecular": "tumor_episode_master_v2",
    "imaging": "imaging_exam_summary_v2",
    "rai": "operative_episode_detail_v2",
    "followup": "operative_episode_detail_v2",
    "demographics": "operative_episode_detail_v2",
    "audit": "operative_episode_detail_v2",
}

FAMILY_MAX_DAYS = {
    "operative": 90,
    "pathology": 90,
    "molecular": 180,
    "imaging": 365,
    "rai": 365,
    "followup": 365,
    "demographics": 365,
    "audit": 365,
}

# ---------------------------------------------------------------------------
# Registry-driven domain map
# ---------------------------------------------------------------------------
_REGISTRY_LOADED = False
_LINKAGE_FAMILY_MAP: dict[str, list[str]] = {}

try:
    from llm_extraction.registry import load_registry as _load_registry

    _reg = _load_registry()
    ENTITY_DOMAIN_MAP: list[tuple[str, str, str]] = [
        (spec.parquet_stem, name, spec.linkage_anchor_family)
        for name, spec in _reg.domains.items()
        if spec.canonical_output
    ]
    _LINKAGE_FAMILY_MAP = _reg.linkage_family_map()
    _REGISTRY_LOADED = True
except Exception:
    ENTITY_DOMAIN_MAP = [
        ("note_entities_staging", "staging", "pathology"),
        ("note_entities_genetics", "genetics", "molecular"),
        ("note_entities_procedures", "procedures", "operative"),
        ("note_entities_operative_detail", "operative_detail", "operative"),
        ("note_entities_complications", "complications", "operative"),
        ("note_entities_medications", "medications", "followup"),
        ("note_entities_problem_list", "problem_list", "demographics"),
        ("note_entities_llm", "llm", "audit"),
    ]

V1_DOMAIN_NAMES: set[str] = set()
try:
    V1_DOMAIN_NAMES = {name for name, spec in _reg.domains.items() if spec.is_v1}
except Exception:
    V1_DOMAIN_NAMES = {
        "staging", "genetics", "procedures", "operative_detail",
        "complications", "medications", "problem_list", "llm",
    }


# ---------------------------------------------------------------------------
# Episode source loaders
# ---------------------------------------------------------------------------
def _ensure_surgery_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """operative_episode_detail_v2 uses resolved_surgery_date + surgery_date_native."""
    if "surgery_date" in df.columns and df["surgery_date"].notna().any():
        return df
    out = df.copy()
    rs = (
        pd.to_datetime(out["resolved_surgery_date"], errors="coerce")
        if "resolved_surgery_date" in out.columns
        else pd.Series(pd.NaT, index=out.index)
    )
    sn = (
        pd.to_datetime(out["surgery_date_native"], errors="coerce")
        if "surgery_date_native" in out.columns
        else pd.Series(pd.NaT, index=out.index)
    )
    out["surgery_date"] = rs.fillna(sn)
    return out


def _load_episode_source(table_name: str) -> pd.DataFrame | None:
    """Load an episode source from parquet or DuckDB."""
    pq = PROCESSED / f"{table_name}.parquet"
    if pq.exists():
        df = pd.read_parquet(pq)
        if table_name == "operative_episode_detail_v2":
            df = _ensure_surgery_date_column(df)
        return df

    if not DB_PATH.exists():
        return None
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except Exception:
        con.close()
        return None

    date_col = "surgery_date"
    ep_id_col = "surgery_episode_id"
    if table_name == "operative_episode_detail_v2":
        date_col = "COALESCE(TRY_CAST(resolved_surgery_date AS DATE), surgery_date_native)"
    elif table_name == "tumor_episode_master_v2":
        date_col = "COALESCE(TRY_CAST(resolved_surgery_date AS DATE), surgery_date_native)"
    elif table_name == "imaging_exam_summary_v2":
        date_col = "exam_date"
        ep_id_col = "exam_id"

    try:
        df = con.execute(
            f"SELECT research_id, {ep_id_col} AS episode_id, "
            f"{date_col} AS episode_date FROM {table_name}"
        ).fetchdf()
    except Exception:
        con.close()
        return None
    con.close()
    return df


_EPISODE_CACHE: dict[str, pd.DataFrame | None] = {}


def _get_episode_source(table_name: str) -> pd.DataFrame | None:
    if table_name not in _EPISODE_CACHE:
        _EPISODE_CACHE[table_name] = _load_episode_source(table_name)
    return _EPISODE_CACHE[table_name]


def _normalize_episode_df(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Normalize to (research_id, episode_id, episode_date) columns."""
    out = df.copy()
    out["research_id"] = pd.to_numeric(out["research_id"], errors="coerce").astype("Int64")

    if "episode_id" not in out.columns:
        if "surgery_episode_id" in out.columns:
            out["episode_id"] = out["surgery_episode_id"]
        elif "exam_id" in out.columns:
            out["episode_id"] = out["exam_id"]
        else:
            out["episode_id"] = 1

    if "episode_date" not in out.columns:
        if "surgery_date" in out.columns:
            out["episode_date"] = pd.to_datetime(out["surgery_date"], errors="coerce")
        elif "exam_date" in out.columns:
            out["episode_date"] = pd.to_datetime(out["exam_date"], errors="coerce")
        else:
            out["episode_date"] = pd.NaT

    out["episode_date"] = pd.to_datetime(out["episode_date"], errors="coerce")
    return out[["research_id", "episode_id", "episode_date"]].dropna(subset=["research_id"])


# ---------------------------------------------------------------------------
# Multi-surgery detection
# ---------------------------------------------------------------------------
def multi_surgery_research_ids(op: pd.DataFrame | None) -> set[int]:
    if op is None or op.empty or "research_id" not in op.columns:
        return set()
    rid = pd.to_numeric(op["research_id"], errors="coerce").dropna().astype(int)
    vc = rid.value_counts()
    return {int(k) for k, n in vc.items() if n > 1}


# ---------------------------------------------------------------------------
# Family-specific episode linkage
# ---------------------------------------------------------------------------
def _infer_episodes_by_family(
    uni: pd.DataFrame,
    family_col: str = "linkage_anchor_family",
) -> pd.DataFrame:
    """Infer episode linkage per-row using the domain's linkage_anchor_family."""
    inferred_ep: list[object] = []
    inferred_dt: list[object] = []
    ep_dist: list[object] = []
    surgery_keys: list[str] = []
    ep_source_names: list[str] = []

    for _, row in uni.iterrows():
        rid_raw = pd.to_numeric(row.get("research_id"), errors="coerce")
        if pd.isna(rid_raw):
            inferred_ep.append(None)
            inferred_dt.append(None)
            ep_dist.append(None)
            surgery_keys.append(str(row.get("research_id", "")))
            ep_source_names.append("")
            continue

        rid = int(rid_raw)
        family = str(row.get(family_col, "audit"))
        source_table = FAMILY_EPISODE_SOURCE.get(family, "operative_episode_detail_v2")
        max_days = FAMILY_MAX_DAYS.get(family, 365)
        raw_ep = _get_episode_source(source_table)

        if raw_ep is None or raw_ep.empty:
            inferred_ep.append(None)
            inferred_dt.append(None)
            ep_dist.append(None)
            surgery_keys.append(str(rid))
            ep_source_names.append(source_table)
            continue

        ep = _normalize_episode_df(raw_ep, source_table)
        sub = ep[ep["research_id"] == rid]
        if sub.empty:
            inferred_ep.append(None)
            inferred_dt.append(None)
            ep_dist.append(None)
            surgery_keys.append(str(rid))
            ep_source_names.append(source_table)
            continue

        ref = row.get("entity_date")
        if ref is None or (isinstance(ref, float) and pd.isna(ref)):
            ref = row.get("clin_note_date")
        rt = pd.to_datetime(ref, errors="coerce")
        if pd.isna(rt):
            rt = pd.Timestamp("1900-01-01")

        dlt = (sub["episode_date"] - rt).abs().dt.days
        valid = dlt[dlt.notna()]
        if valid.empty:
            inferred_ep.append(None)
            inferred_dt.append(None)
            ep_dist.append(None)
            surgery_keys.append(str(rid))
            ep_source_names.append(source_table)
            continue

        j = valid.idxmin()
        best = sub.loc[j]
        best_dist = int(valid.loc[j])

        if best_dist > max_days:
            inferred_ep.append(None)
            inferred_dt.append(None)
            ep_dist.append(best_dist)
            surgery_keys.append(str(rid))
            ep_source_names.append(source_table)
            continue

        se = best.get("episode_id")
        inferred_ep.append(int(se) if pd.notna(se) else None)
        inferred_dt.append(
            best["episode_date"].date()
            if pd.notna(best["episode_date"]) else None
        )
        ep_dist.append(best_dist)
        surgery_keys.append(
            f"{rid}:{int(se)}" if pd.notna(se) else str(rid)
        )
        ep_source_names.append(source_table)

    uni = uni.copy()
    uni["inferred_surgery_episode_id"] = inferred_ep
    uni["inferred_surgery_date"] = inferred_dt
    uni["ep_distance_days"] = ep_dist
    uni["surgery_key"] = surgery_keys
    uni["ep_source_table"] = ep_source_names
    return uni


# ---------------------------------------------------------------------------
# Fact ID generation
# ---------------------------------------------------------------------------
def fact_id_for(i: int, row: pd.Series) -> str:
    key = "|".join([
        str(row.get("research_id", "")),
        str(row.get("note_row_id", "")),
        str(row.get("entity_type", "")),
        str(row.get("entity_value_raw", "")),
        str(row.get("fact_domain", "")),
        str(i),
    ])
    return hashlib.md5(key.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# V2 quarantine: extends v1 with family-aware rules
# ---------------------------------------------------------------------------
def split_quarantine_v2(
    uni: pd.DataFrame,
    multi_surgery_rids: set[int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split into clean + quarantine using v1 masks plus family-specific rules."""
    quarantine_date = datetime.now(timezone.utc).date().isoformat()
    q, reason = quarantine_masks(uni, multi_surgery_rids)

    epd = pd.to_numeric(
        uni["ep_distance_days"], errors="coerce"
    ) if "ep_distance_days" in uni.columns else pd.Series(np.nan, index=uni.index)

    family = (
        uni["linkage_anchor_family"].fillna("audit")
        if "linkage_anchor_family" in uni.columns
        else pd.Series("audit", index=uni.index)
    )
    family_max = family.map(FAMILY_MAX_DAYS).fillna(365).astype(float)
    m_family = epd.notna() & (epd > family_max) & ~q
    q = q | m_family
    reason = reason.where(~m_family, "family_window_exceeded")

    has_ep = (
        uni["inferred_surgery_episode_id"].notna()
        if "inferred_surgery_episode_id" in uni.columns
        else pd.Series(False, index=uni.index)
    )
    m_no_ep = ~has_ep & ~q
    q = q | m_no_ep
    reason = reason.where(~m_no_ep, "no_episode_linkage")

    quar = uni.loc[q].copy()
    quar["quarantine_reason"] = reason.loc[q]
    quar["quarantine_date"] = quarantine_date
    clean = uni.loc[~q].copy()
    return clean, quar


# ---------------------------------------------------------------------------
# Contract dtype enforcement
# ---------------------------------------------------------------------------
def _apply_contract_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df
    for col, dtype in CANONICAL_FACT_CONTRACT_DTYPES.items():
        if col in out.columns:
            try:
                out[col] = out[col].astype(dtype)
            except (TypeError, ValueError):
                pass
    return out


# ---------------------------------------------------------------------------
# QC report
# ---------------------------------------------------------------------------
def _generate_qc_report(
    clean_v1: pd.DataFrame,
    quar_v1: pd.DataFrame,
    clean_v2: pd.DataFrame,
    quar_v2: pd.DataFrame,
) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# Fact Lineage QC Report",
        f"\nGenerated: {ts}",
        "\n## Row Counts",
        "\n| Output | Rows |",
        "|--------|------|",
        f"| canonical_extracted_fact_long_v1 | {len(clean_v1):,} |",
        f"| canonical_fact_quarantine_v1 | {len(quar_v1):,} |",
        f"| canonical_extracted_fact_long_v2 | {len(clean_v2):,} |",
        f"| canonical_fact_quarantine_v2 | {len(quar_v2):,} |",
    ]

    lines.append("\n## V2 Clean Facts by Domain\n")
    lines.append("| Domain | Rows | Linked % |")
    lines.append("|--------|------|----------|")
    if "fact_domain" in clean_v2.columns:
        for dom in sorted(clean_v2["fact_domain"].dropna().unique()):
            sub = clean_v2[clean_v2["fact_domain"] == dom]
            linked = (
                sub["inferred_surgery_episode_id"].notna().sum()
                if "inferred_surgery_episode_id" in sub.columns else 0
            )
            pct = f"{100 * linked / len(sub):.1f}" if len(sub) > 0 else "0.0"
            lines.append(f"| {dom} | {len(sub):,} | {pct}% |")

    lines.append("\n## V2 Quarantine Reasons\n")
    lines.append("| Reason | Count |")
    lines.append("|--------|-------|")
    if "quarantine_reason" in quar_v2.columns and len(quar_v2) > 0:
        for reason, cnt in (
            quar_v2["quarantine_reason"].value_counts().items()
        ):
            lines.append(f"| {reason} | {cnt:,} |")
    else:
        lines.append("| (none) | 0 |")

    lines.append("\n## V2 Unresolved Episode Linkage\n")
    if "inferred_surgery_episode_id" in clean_v2.columns:
        unlinked = clean_v2["inferred_surgery_episode_id"].isna().sum()
        lines.append(f"Unlinked rows in clean v2: {unlinked:,}")
    else:
        lines.append("Episode linkage column not present.")

    lines.append("\n## Duplicate Facts\n")
    dedup_cols = ["research_id", "note_row_id", "entity_type", "entity_value_raw", "fact_domain"]
    present = [c for c in dedup_cols if c in clean_v2.columns]
    if len(present) == len(dedup_cols):
        n_dupes = clean_v2.duplicated(subset=present).sum()
        lines.append(f"Duplicate rows (on dedup key): {n_dupes:,}")
    else:
        lines.append(f"Cannot check duplicates — missing columns: {set(dedup_cols) - set(present)}")

    lines.append("\n## Linkage Family Distribution (V2 clean)\n")
    lines.append("| Family | Domains | Rows |")
    lines.append("|--------|---------|------|")
    if "linkage_anchor_family" in clean_v2.columns:
        for fam in sorted(clean_v2["linkage_anchor_family"].dropna().unique()):
            sub = clean_v2[clean_v2["linkage_anchor_family"] == fam]
            dom_count = sub["fact_domain"].nunique() if "fact_domain" in sub.columns else 0
            lines.append(f"| {fam} | {dom_count} | {len(sub):,} |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--md", action="store_true",
        help="Open DuckDB via MotherDuck when token/env is available",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("  103 — canonical_extracted_fact_long (v1 + v2) + quarantine")
    print(f"  Registry: {'YAML-driven' if _REGISTRY_LOADED else 'hardcoded fallback'}")
    print(f"  Domains: {len(ENTITY_DOMAIN_MAP)}")
    print("=" * 70)

    # ── Load all domain parquets ──────────────────────────────────────────
    frames: list[pd.DataFrame] = []
    for entry in ENTITY_DOMAIN_MAP:
        stem, domain = entry[0], entry[1]
        family = entry[2] if len(entry) > 2 else "audit"
        pq = PROCESSED / f"{stem}.parquet"
        if not pq.exists():
            print(f"  skip (no parquet): {stem}")
            continue
        df = pd.read_parquet(pq).copy()
        df["fact_domain"] = domain
        df["linkage_anchor_family"] = family
        frames.append(df)
        print(f"  loaded {stem}: {len(df):,} rows  [family={family}]")

    if not frames:
        print("  ERROR: no note_entities_*.parquet under processed/")
        sys.exit(1)

    # ── Align columns ─────────────────────────────────────────────────────
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

    # ── Merge clinical note provenance ────────────────────────────────────
    notes_path = PROCESSED / "clinical_notes_long.parquet"
    if notes_path.exists():
        notes = pd.read_parquet(notes_path)
        nc = [
            c for c in (
                "note_row_id", "note_date", "source_sheet",
                "source_column", "source_workbook", "excel_row_0based",
            ) if c in notes.columns
        ]
        notes = notes[nc].drop_duplicates(subset=["note_row_id"])
        notes = notes.rename(columns={
            "note_date": "clin_note_date",
            "source_sheet": "clin_source_sheet",
            "source_column": "clin_source_column",
            "source_workbook": "clin_source_workbook",
            "excel_row_0based": "clin_excel_row_0based",
        })
        uni = uni.merge(notes, on="note_row_id", how="left")
        print("  merged clinical_notes_long")
    else:
        for c in ("clin_note_date", "clin_source_sheet", "clin_source_column",
                   "clin_source_workbook", "clin_excel_row_0based"):
            uni[c] = None

    # ── Family-specific episode linkage ───────────────────────────────────
    op_raw = _get_episode_source("operative_episode_detail_v2")
    multi_ids = multi_surgery_research_ids(op_raw)
    print(f"  multi-surgery patients: {len(multi_ids):,}")

    uni = _infer_episodes_by_family(uni)
    print("  inferred episode linkage (family-specific)")

    # ── Provenance contract columns ───────────────────────────────────────
    uni = apply_provenance_contract_columns(uni, multi_ids)
    uni["fact_id"] = [fact_id_for(i, uni.iloc[i]) for i in range(len(uni))]

    # ── V1 split (preserved unchanged) ────────────────────────────────────
    from utils.provenance import split_quarantine  # noqa: E402

    v1_mask = uni["fact_domain"].isin(V1_DOMAIN_NAMES)
    uni_v1 = uni[v1_mask].copy()
    clean_v1, quar_v1 = split_quarantine(uni_v1, multi_ids)
    print(f"  v1 split: clean={len(clean_v1):,}  quarantined={len(quar_v1):,}")

    # ── V2 split (all domains, family-specific quarantine) ────────────────
    clean_v2, quar_v2 = split_quarantine_v2(uni, multi_ids)
    print(f"  v2 split: clean={len(clean_v2):,}  quarantined={len(quar_v2):,}")

    # ── Output paths ──────────────────────────────────────────────────────
    out_v1 = PROCESSED / "canonical_extracted_fact_long_v1.parquet"
    outq_v1 = PROCESSED / "canonical_fact_quarantine_v1.parquet"
    out_v2 = PROCESSED / "canonical_extracted_fact_long_v2.parquet"
    outq_v2 = PROCESSED / "canonical_fact_quarantine_v2.parquet"

    # ── QC report ─────────────────────────────────────────────────────────
    qc_text = _generate_qc_report(clean_v1, quar_v1, clean_v2, quar_v2)

    if args.dry_run:
        print(f"  dry-run: v1 clean={len(clean_v1):,} → {out_v1.name}")
        print(f"  dry-run: v1 quarantine={len(quar_v1):,} → {outq_v1.name}")
        print(f"  dry-run: v2 clean={len(clean_v2):,} → {out_v2.name}")
        print(f"  dry-run: v2 quarantine={len(quar_v2):,} → {outq_v2.name}")
        print("\n" + qc_text)
        return

    # ── Write parquets ────────────────────────────────────────────────────
    for label, df, path in [
        ("v1", clean_v1, out_v1),
        ("v1q", quar_v1, outq_v1),
        ("v2", clean_v2, out_v2),
        ("v2q", quar_v2, outq_v2),
    ]:
        df = _apply_contract_dtypes(df)
        save_parquet(df, path)

    # ── Write QC report ───────────────────────────────────────────────────
    QC_DIR.mkdir(parents=True, exist_ok=True)
    ts_slug = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    qc_path = QC_DIR / f"qc_report_{ts_slug}.md"
    qc_path.write_text(qc_text, encoding="utf-8")
    print(f"  QC report: {qc_path}")

    # ── DuckDB ────────────────────────────────────────────────────────────
    con = connect_md_or_file(DB_PATH, md=args.md)
    for tbl, pq in [
        ("canonical_extracted_fact_long_v1", out_v1),
        ("canonical_fact_quarantine_v1", outq_v1),
        ("canonical_extracted_fact_long_v2", out_v2),
        ("canonical_fact_quarantine_v2", outq_v2),
    ]:
        con.execute(
            f"CREATE OR REPLACE TABLE {tbl} AS "
            f"SELECT * FROM read_parquet('{pq}')"
        )
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  DuckDB {tbl}: {cnt:,} rows")

    runs_pq = PROCESSED / "note_extraction_runs.parquet"
    if runs_pq.exists():
        con.execute(
            f"CREATE OR REPLACE TABLE note_extraction_runs AS "
            f"SELECT * FROM read_parquet('{runs_pq}')"
        )
        rn = con.execute("SELECT COUNT(*) FROM note_extraction_runs").fetchone()[0]
        print(f"  DuckDB note_extraction_runs: {rn:,}")

    con.close()
    print("=" * 70)
    print("  DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
