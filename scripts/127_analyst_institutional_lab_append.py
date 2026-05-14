#!/usr/bin/env python3
"""Append analyst-delivered institutional lab rows to the per-analyte
canonical lab tables introduced by Script 347.

DEPRECATED MOTHERDUCK OPERATIONS — 2026-05-14: Institutional / analyst lab
staging on MotherDuck is no longer the publication build path for Tg refreshes.
Thyroglobulin/TgAb canonical rows from the authoritative analyst EHR pull are rebuilt
directly in BigQuery via mig_340
(``qc_framework_v1/migrations/mig_340_thyroglobulin_analyst_bq_rebuild.py``).
Continue using Script 127 only for historical reproducibility unless explicitly
directed otherwise.

Refactored by Script 348 (2026-04-21). The legacy target
``main.longitudinal_lab_canonical_v1`` was dropped by Script 347; rows
are now routed by ``lab_name_standardized`` to the matching per-analyte
canonical:

    lab_name_standardized               target table
    ---------------------------------   --------------------------------------
    thyroglobulin / Tg / tg            -> main.canonical_labs_thyroglobulin_v1
    anti_thyroglobulin / TgAb / tgab   -> main.canonical_labs_thyroglobulin_v1
    tsh                                -> main.canonical_labs_tsh_v1
    pth                                -> main.canonical_labs_pth_v1
    calcium / total_calcium /
       corrected_calcium / ionized_calcium -> main.canonical_labs_calcium_v1
    vitamin_d / 25_oh_vit_d / vitd     -> main.canonical_labs_vitamin_d_v1

Every appended row carries ``source = 'institutional_append'`` (the
highest cross-wave dedup precedence per Script 347). Idempotent replace
of the wave is keyed on the wave label embedded in
``value_correction_note`` as ``ingestion_wave_tag=<wave>`` (the per-
analyte canonical schema does not carry an ``ingestion_wave`` column).

All value normalisation is delegated to
``scripts/_lab_value_normalizer.py`` (uniform 2A–2F pipeline +
``convert_to_canonical_unit``). Cross-wave dedup is applied INLINE at
write time per affected per-analyte table.

No raw clinical note text is read or written. MotherDuck target uses
fail-closed --md.

Usage:
  .venv/bin/python scripts/127_analyst_institutional_lab_append.py --md \\
      --input exports/incoming/final_lab_extract_YYYYMMDD.csv \\
      --ingestion-wave final_institutional_20260407

Expected CSV columns (headers):
  research_id (required)
  lab_date (required, ISO date) OR lab_datetime (ISO datetime; preferred)
  lab_name_standardized OR lab_name_raw (required)
  value_raw (required)
  source_lineage_key (required) — institutional order/result id, hash, or
    stable composite
  unit_raw (optional) — used for conversion when present and recognised
  unit_standardized (optional) — informational; unit normalisation is
    re-derived
  provenance_note (optional)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from _lab_value_normalizer import (  # noqa: E402
    CANONICAL_UNIT,
    convert_to_canonical_unit,
    normalize_lab_value,
)

DEFAULT_DB = ROOT / "thyroid_master.duckdb"
SCRIPT_TAG = "127_analyst_institutional_lab_append.py"
PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
SOURCE_TAG = "institutional_append"

LAB_APPEND_FAIL_AFTER_ENV = "LAB_APPEND_FAIL_AFTER"

# Map every accepted lab_name_standardized synonym to (canonical_key, target_table).
ANALYTE_ROUTING: dict[str, tuple[str, str]] = {
    # Tg / TgAb
    "thyroglobulin":      ("thyroglobulin",      "canonical_labs_thyroglobulin_v1"),
    "tg":                 ("thyroglobulin",      "canonical_labs_thyroglobulin_v1"),
    "anti_thyroglobulin": ("anti_thyroglobulin", "canonical_labs_thyroglobulin_v1"),
    "tgab":               ("anti_thyroglobulin", "canonical_labs_thyroglobulin_v1"),
    "tg_antibody":        ("anti_thyroglobulin", "canonical_labs_thyroglobulin_v1"),
    # TSH
    "tsh":                ("tsh",                "canonical_labs_tsh_v1"),
    # PTH
    "pth":                ("pth",                "canonical_labs_pth_v1"),
    # Calcium (and label variants observed in institutional deliverables)
    "calcium":            ("calcium",            "canonical_labs_calcium_v1"),
    "total_calcium":      ("calcium",            "canonical_labs_calcium_v1"),
    "corrected_calcium":  ("calcium",            "canonical_labs_calcium_v1"),
    "ionized_calcium":    ("calcium",            "canonical_labs_calcium_v1"),
    "ca":                 ("calcium",            "canonical_labs_calcium_v1"),
    # Vitamin D
    "vitamin_d":          ("vitamin_d",          "canonical_labs_vitamin_d_v1"),
    "25_oh_vit_d":        ("vitamin_d",          "canonical_labs_vitamin_d_v1"),
    "vitd":               ("vitamin_d",          "canonical_labs_vitamin_d_v1"),
}

ALL_TARGETS = sorted({t for _, t in ANALYTE_ROUTING.values()})

REQUIRED_COLS = {"research_id", "value_raw", "source_lineage_key"}

# Tg/TgAb table carries `analyte` column (Tg|TgAb); the others don't.
THY_ANALYTE_LABEL = {"thyroglobulin": "Tg", "anti_thyroglobulin": "TgAb"}

# Cross-wave dedup priority, identical to Scripts 347 / 113.
DEDUP_RANK_CASE = """
    CASE source
        WHEN 'institutional_append' THEN 0
        WHEN 'structured_ehr_tg'    THEN 1
        WHEN 'analyst_ehr_tg'       THEN 1
        WHEN 'postop_structured'    THEN 2
        WHEN 'clinical_note'        THEN 3
        ELSE 9
    END
"""


def _scalar_int(con: duckdb.DuckDBPyConnection, sql: str,
                params: list[object] | None = None) -> int:
    row = con.execute(sql, params or []).fetchone()
    if row is None:
        raise RuntimeError(f"unexpected empty result: {sql!r}")
    return int(row[0])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument(
        "--md-sa", action="store_true",
        help="Prefer MD_SA_TOKEN over MOTHERDUCK_TOKEN when connecting.",
    )
    p.add_argument(
        "--db-path", default=str(DEFAULT_DB),
        help="Local DuckDB path (unused when --md).",
    )
    p.add_argument("--input", type=Path, required=True, help="Analyst lab CSV path.")
    p.add_argument(
        "--ingestion-wave", required=True,
        help="Unique wave label, e.g. final_institutional_20260407 "
             "(used for idempotent replace via value_correction_note tag).",
    )
    p.add_argument("--dry-run", action="store_true", help="Validate only; no writes.")
    return p.parse_args()


def connect(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    if args.md:
        from utils.md_connect import connect_md_or_file
        return connect_md_or_file(
            Path(args.db_path), md=True, fail_closed=True,
            prefer_service_account=args.md_sa,
            custom_user_agent=os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT"),
            motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT"),
        )
    print("  FATAL: This script requires --md (no silent local fallback for "
          "institutional append).")
    sys.exit(1)


def cpm_invariant(con: duckdb.DuckDBPyConnection, label: str) -> None:
    r = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) "
        f"FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()
    print(f"  CPM invariant ({label}): rows={r[0]} dist_rid={r[1]} null_fna={r[2]}")
    if (r[0], r[1], r[2]) != (10871, 10871, 0):
        raise SystemExit(
            f"CPM INVARIANT FAIL ({label}): expected (10871,10871,0); got {tuple(r)}"
        )


def build_frames(path: Path, ingestion_wave: str) -> dict[str, pd.DataFrame]:
    """Read the analyst CSV, normalise per row, and split into per-target frames.

    Returns ``{target_table: per_target_df}`` where each df matches the
    canonical schema of that table.
    """
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise SystemExit(f"  FATAL: CSV missing required columns: {sorted(missing)}")
    if "lab_name_standardized" not in df.columns and "lab_name_raw" not in df.columns:
        raise SystemExit("  FATAL: need lab_name_standardized or lab_name_raw")
    if "lab_date" not in df.columns and "lab_datetime" not in df.columns:
        raise SystemExit("  FATAL: need lab_date or lab_datetime")

    # Validate source_lineage_key uniqueness up-front.
    key = df["source_lineage_key"].astype(str)
    blank_key = key.str.strip().eq("") | key.str.lower().isin(("nan", "none"))
    if blank_key.any():
        raise SystemExit(
            f"  FATAL: {int(blank_key.sum())} row(s) have empty/invalid "
            "source_lineage_key"
        )
    if key.str.strip().duplicated(keep=False).any():
        n_dup = int(key.str.strip().duplicated(keep=False).sum())
        raise SystemExit(
            f"  FATAL: duplicate source_lineage_key in CSV ({n_dup} row(s)); "
            "keys must be unique for deterministic lineage"
        )

    rid = pd.to_numeric(df["research_id"], errors="coerce").astype("Int64")
    if rid.isna().any():
        raise SystemExit(
            f"  FATAL: {int(rid.isna().sum())} row(s) failed research_id coercion"
        )

    # lab_datetime: prefer explicit; otherwise midnight of lab_date.
    if "lab_datetime" in df.columns:
        lab_dt = pd.to_datetime(df["lab_datetime"], errors="coerce")
    else:
        lab_dt = pd.to_datetime(df["lab_date"], errors="coerce")
    if lab_dt.isna().any():
        raise SystemExit(
            f"  FATAL: {int(lab_dt.isna().sum())} row(s) failed lab date/time coercion"
        )

    name = (
        df["lab_name_standardized"].fillna(df.get("lab_name_raw"))
        if "lab_name_standardized" in df.columns
        else df["lab_name_raw"]
    ).astype(str).str.strip().str.lower()

    unmapped_names = sorted(set(name) - set(ANALYTE_ROUTING.keys()))
    if unmapped_names:
        raise SystemExit(
            f"  FATAL: unknown lab_name_standardized values: {unmapped_names}; "
            "extend ANALYTE_ROUTING to add support."
        )

    note_in = df["provenance_note"] if "provenance_note" in df.columns else pd.Series(
        [""] * len(df)
    )

    discordances: list[dict] = []
    out_rows: dict[str, list[dict]] = {t: [] for t in ALL_TARGETS}
    now_utc = datetime.now(timezone.utc)

    for i in range(len(df)):
        canon_key, target = ANALYTE_ROUTING[name.iat[i]]
        v_raw = df["value_raw"].iat[i]
        if isinstance(v_raw, float) and v_raw != v_raw:  # NaN
            v_raw = None

        v_num, is_cens, n_note = normalize_lab_value(v_raw, canon_key)

        unit_src = (
            df["unit_raw"].iat[i] if "unit_raw" in df.columns else None
        )
        try:
            v_num, unit_std, u_note = convert_to_canonical_unit(
                v_num, unit_src, canon_key
            )
        except ValueError as e:
            discordances.append({
                "research_id": int(rid.iat[i]),
                "analyte": canon_key,
                "value_raw": v_raw,
                "unit_raw": unit_src,
                "error": str(e),
            })
            unit_std = CANONICAL_UNIT[canon_key]
            u_note = f"unit_unknown_{unit_src}_aborted"

        notes = []
        notes.append(f"ingestion_wave_tag={ingestion_wave}")
        notes.append(f"lineage_key={str(key.iat[i]).strip()}")
        if n_note:
            notes.append(n_note)
        if u_note:
            notes.append(u_note)
        prov = note_in.iat[i] if i < len(note_in) else None
        if isinstance(prov, str) and prov.strip():
            notes.append(prov.strip())
        full_note = " | ".join(notes)

        row: dict = {
            "research_id":     int(rid.iat[i]),
            "lab_datetime":    lab_dt.iat[i].to_pydatetime(),
            "value_raw":       v_raw,
            "value_numeric":   v_num,
            "is_censored":     bool(is_cens),
            "value_correction_note": full_note,
            "unit_standardized":     unit_std,
            "source":          SOURCE_TAG,
            "is_in_canonical_cancer_cohort": False,  # backfilled in finalize step
            "ingestion_date":  now_utc,
        }
        if "thyroglobulin" in target:
            row["analyte"] = THY_ANALYTE_LABEL[canon_key]
            row["assay_method"] = None
            row["analyte_assignment_method"] = "explicit_institutional_append"
        out_rows[target].append(row)

    if discordances:
        _write_discordances(discordances)
        raise SystemExit(
            f"  ABORT: {len(discordances)} unrecognised source-unit row(s); "
            "see studies/lab_ingestion_refactor_20260421/discordance_review.md"
        )

    out: dict[str, pd.DataFrame] = {}
    for target, rows in out_rows.items():
        if not rows:
            continue
        d = pd.DataFrame.from_records(rows)
        d["lab_datetime"] = pd.to_datetime(d["lab_datetime"])
        d["ingestion_date"] = pd.to_datetime(
            d["ingestion_date"], utc=True
        ).dt.tz_localize(None)
        d["value_numeric"] = d["value_numeric"].astype("float64")
        d["research_id"] = d["research_id"].astype("int64")
        out[target] = d
    return out


def _write_discordances(rows: list[dict]) -> None:
    out_dir = ROOT / "studies" / "lab_ingestion_refactor_20260421"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "discordance_review.md"
    with path.open("a") as f:
        f.write(f"\n## Script 127 unit discordances — "
                f"{datetime.now(timezone.utc).isoformat()}\n\n")
        f.write("| research_id | analyte | value_raw | unit_raw | error |\n")
        f.write("|---|---|---|---|---|\n")
        for d in rows:
            f.write(
                f"| {d['research_id']} | {d['analyte']} | "
                f"{(d['value_raw'] or '')[:80]} | {d.get('unit_raw') or ''} | "
                f"{d['error']} |\n"
            )


def _injected_fail(stage: str) -> None:
    if os.environ.get(LAB_APPEND_FAIL_AFTER_ENV) == stage:
        raise RuntimeError(f"injected failure ({LAB_APPEND_FAIL_AFTER_ENV}={stage})")


def _replace_wave_per_table(
    con: duckdb.DuckDBPyConnection,
    target: str,
    frame: pd.DataFrame,
    ingestion_wave: str,
) -> tuple[int, int, int]:
    """Replace the institutional-append slice for ``ingestion_wave`` in
    ``main.<target>``. Inline cross-wave dedup is applied via a CREATE OR
    REPLACE TABLE rebuild from the union of (kept rows + new wave rows).

    Returns (pre_total, post_total, wave_n_inserted).
    """
    fq = f"main.{target}"
    has_analyte = "thyroglobulin" in target

    pre_total = _scalar_int(con, f"SELECT COUNT(*) FROM {fq}")
    wave_tag = f"ingestion_wave_tag={ingestion_wave}"
    pre_wave = _scalar_int(
        con,
        f"SELECT COUNT(*) FROM {fq} WHERE source = ? "
        "AND COALESCE(value_correction_note,'') LIKE ?",
        [SOURCE_TAG, f"%{wave_tag}%"],
    )

    # Snapshot the rows we INTEND to keep (everything not in this wave).
    keep_sql = (
        f"SELECT * FROM {fq} WHERE NOT (source = '{SOURCE_TAG}' "
        f"AND COALESCE(value_correction_note,'') LIKE '%{wave_tag}%')"
    )

    # Schema-aware column list (must match exactly).
    if has_analyte:
        cols = [
            "research_id", "analyte", "assay_method", "lab_datetime",
            "value_raw", "value_numeric", "is_censored",
            "value_correction_note", "unit_standardized", "source",
            "is_in_canonical_cancer_cohort", "ingestion_date",
            "analyte_assignment_method",
        ]
    else:
        cols = [
            "research_id", "lab_datetime", "value_raw", "value_numeric",
            "is_censored", "value_correction_note", "unit_standardized",
            "source", "is_in_canonical_cancer_cohort", "ingestion_date",
        ]
    col_list = ", ".join(cols)

    # Ensure ``frame`` has every column the table requires.
    for c in cols:
        if c not in frame.columns:
            raise RuntimeError(
                f"Per-target frame missing required column {c!r} for {target}"
            )
    df_send = frame[cols].copy()

    partition_extra = ", analyte" if has_analyte else ""

    # Backfill is_in_canonical_cancer_cohort from existing rows (per rid).
    con.register("_lab_127_new", df_send)
    backfill_sql = f"""
        SELECT
            n.research_id,
            {('n.analyte,' if has_analyte else '')}
            {('n.assay_method,' if has_analyte else '')}
            n.lab_datetime, n.value_raw, n.value_numeric, n.is_censored,
            n.value_correction_note, n.unit_standardized, n.source,
            COALESCE(
                (SELECT BOOL_OR(t.is_in_canonical_cancer_cohort) FROM {fq} t
                 WHERE t.research_id = n.research_id),
                FALSE
            ) AS is_in_canonical_cancer_cohort,
            n.ingestion_date
            {(', n.analyte_assignment_method' if has_analyte else '')}
        FROM _lab_127_new n
    """

    rebuild_sql = f"""
    CREATE OR REPLACE TABLE {fq} AS
    WITH unioned AS (
        SELECT {col_list} FROM ({keep_sql})
        UNION ALL
        SELECT {col_list} FROM ({backfill_sql})
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY research_id{partition_extra},
                             CAST(lab_datetime AS DATE),
                             COALESCE(CAST(value_numeric AS VARCHAR), value_raw)
                ORDER BY {DEDUP_RANK_CASE}, ingestion_date DESC
            ) AS rn
        FROM unioned
    )
    SELECT {col_list} FROM ranked WHERE rn = 1
    """

    try:
        con.execute(rebuild_sql)
        _injected_fail("after_insert")
    finally:
        con.unregister("_lab_127_new")

    post_total = _scalar_int(con, f"SELECT COUNT(*) FROM {fq}")
    post_wave = _scalar_int(
        con,
        f"SELECT COUNT(*) FROM {fq} WHERE source = ? "
        "AND COALESCE(value_correction_note,'') LIKE ?",
        [SOURCE_TAG, f"%{wave_tag}%"],
    )
    return pre_total, post_total, post_wave - pre_wave


def main() -> None:
    args = parse_args()
    if not args.input.is_file():
        print(f"  FATAL: --input not found: {args.input}")
        sys.exit(1)

    wave = args.ingestion_wave.strip()
    frames = build_frames(args.input, wave)
    total_rows = sum(len(d) for d in frames.values())
    print(f"  Prepared {total_rows:,} lab row(s) across {len(frames)} target table(s) "
          f"for wave {wave}")
    for t, d in frames.items():
        print(f"    {t}: {len(d):,} row(s)")

    if args.dry_run:
        print("  [dry-run] stopping before database write")
        return

    con = connect(args)
    try:
        cpm_invariant(con, "pre")

        # Verify all target tables exist.
        for t in frames:
            if not _scalar_int(
                con,
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_name = ?",
                [t],
            ):
                raise SystemExit(f"  FATAL: main.{t} does not exist")

        # All writes inside a single transaction across the affected tables.
        con.execute("BEGIN TRANSACTION")
        results: list[dict] = []
        try:
            for target, frame in frames.items():
                pre, post, n_ins = _replace_wave_per_table(con, target, frame, wave)
                print(f"  [lab] main.{target}: {pre:,} → {post:,} (wave inserted: "
                      f"{n_ins:,})")
                results.append({"table": target, "pre": pre, "post": post,
                                "wave_inserted": n_ins})
                _injected_fail("after_insert")
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise

        cpm_invariant(con, "post")

        qc_dir = Path("studies/lab_ingestion_refactor_20260421")
        qc_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        qc_path = qc_dir / f"127_run_{wave}_{ts}.json"
        qc_path.write_text(
            json.dumps({
                "script": SCRIPT_TAG,
                "ingestion_wave": wave,
                "results": results,
            }, indent=2, default=str),
            encoding="utf-8",
        )
        print(f"  [qc] wave summary written {qc_path}")
    finally:
        con.close()

    print(f"  [done] {SCRIPT_TAG}")


if __name__ == "__main__":
    main()
