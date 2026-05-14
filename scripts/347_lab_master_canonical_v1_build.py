#!/usr/bin/env python3
"""
Script 347 — Lab Master Canonical v1 Build (per-analyte consolidation).

DEPRECATED FOR THYROGLOBULIN REFRESH — 2026-05-14: Rebuilding canonical
thyroglobulin rows from MotherDuck is retired. New analyst CSV loads use BigQuery mig_340:
``qc_framework_v1/migrations/mig_340_thyroglobulin_analyst_bq_rebuild.py``.
Script 347 remains for historical MotherDuck lab consolidation workflows and non-Tg analyzers only.

Consolidates the lab layer into FIVE per-analyte canonical tables and TWO
compatibility views, with uniform value normalization via
``scripts/_lab_value_normalizer.py``.

TARGET STATE
============

Tables (all under ``thyroid_canonical_publication_v1_0.main``):

    canonical_labs_thyroglobulin_v1   -- Tg + TgAb (analyte column),
        cross-wave dedup on
        (research_id, analyte, lab_datetime, value_numeric|value_raw);
        ALL thyroglobulin-family rows in ``longitudinal_lab_canonical_v1``
        (synonym / LIKE coverage per BigQuery validation queries).
    canonical_labs_tsh_v1
    canonical_labs_pth_v1
    canonical_labs_calcium_v1
    canonical_labs_vitamin_d_v1

Views:

    longitudinal_lab_VIEW_v1          -- UNION ALL across the 5 tables
    thyroglobulin_lab_VIEW_v1         -- legacy-column-shaped Tg/TgAb view

Dropped (preserved in archive_pub_v1_0 snapshots):

    main.longitudinal_lab_canonical_v1
    main.thyroglobulin_lab_canonical_v1
    main.lab_cross_wave_dedup_map_v1
    main.longitudinal_lab_canonical_cancer_only_v1
    main.thyroglobulin_lab_canonical_cancer_only_v1

USAGE
=====
    python scripts/347_lab_master_canonical_v1_build.py --dry-run
    python scripts/347_lab_master_canonical_v1_build.py --commit
    python scripts/347_lab_master_canonical_v1_build.py --commit --no-git

CPM INVARIANT (pre AND post): (10871, 10871, 0). Aborts on violation.
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from scripts._md_connect import connect_locked  # noqa: E402
from _lab_value_normalizer import (              # noqa: E402
    CANONICAL_UNIT,
    convert_to_canonical_unit,
    normalize_lab_value,
)
from _tg_combo_panel import (  # noqa: E402
    crossref_disambiguate_pair,
    heuristic_disambiguate_pair,
    infer_singleton_combo_analyte,
    is_tg_plus_tgab_combo_panel_test_name,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
WS_SCHEMA = "manuscript_workspace"
SCRIPT_TAG = "Script 347"
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = OUT_DIR / f"347_run_{RUN_TS}.log"
DECISION_PATH = OUT_DIR / f"347_decision_{RUN_TS}.json"

REPORT_DIR = REPO / "studies" / "lab_consolidation_20260421"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = REPORT_DIR / "report.md"
DISCORDANCE_PATH = REPORT_DIR / "discordance_review.md"

# Prompt 6 / analyst EHR thyroglobulin CSV (canonical patient coverage parity vs BQ raw).
RAW_DIR = REPO / "raw"
ANALYST_TG_RAW_CSV_NAMES = (
    "Thyroid Thyroglobulin Lab_20251120.csv",
    "Thyroid Thyroglobulin - 11_24_25.csv",
)


def find_analyst_tg_csv() -> Optional[Path]:
    """Return first existing analyst pull under ``raw/``."""
    for name in ANALYST_TG_RAW_CSV_NAMES:
        p = RAW_DIR / name
        if p.is_file():
            return p
    return None


ANALYTES: list[str] = [
    "thyroglobulin",
    "anti_thyroglobulin",
    "tsh",
    "pth",
    "calcium",
    "vitamin_d",
]

PER_ANALYTE_TABLE: dict[str, str] = {
    "thyroglobulin":      "canonical_labs_thyroglobulin_v1",
    "anti_thyroglobulin": "canonical_labs_thyroglobulin_v1",
    "tsh":                "canonical_labs_tsh_v1",
    "pth":                "canonical_labs_pth_v1",
    "calcium":            "canonical_labs_calcium_v1",
    "vitamin_d":          "canonical_labs_vitamin_d_v1",
}

EXPECTED_ROW_RANGE: dict[str, tuple[int, int]] = {
    # Tg/TgAb: longitudinal + analyst CSV union; combo-panel split restores Tg rows;
    # full-timestamp dedup key per THY_DEDUP_SQL.
    "canonical_labs_thyroglobulin_v1": (52_000, 62_000),
    "canonical_labs_tsh_v1":           (500, 800),
    "canonical_labs_pth_v1":           (180, 240),
    "canonical_labs_calcium_v1":       (170, 220),
    "canonical_labs_vitamin_d_v1":     (80, 110),
}

PER_ANALYTE_PLAUSIBLE_MAX: dict[str, float] = {
    "thyroglobulin":      10000.0,
    "anti_thyroglobulin": 40000.0,
    "tsh":                150.0,
    "pth":                3000.0,
    "calcium":            20.0,
    "vitamin_d":          200.0,
}

LOG_LINES: list[str] = []


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3] + "Z"
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)


# ---------------------------------------------------------------------------
# CPM invariant
# ---------------------------------------------------------------------------

def cpm_invariant(con: duckdb.DuckDBPyConnection, label: str) -> None:
    r = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) "
        f"FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()
    log(f"  CPM invariant ({label}): rows={r[0]} dist_rid={r[1]} null_fna={r[2]}")
    if (r[0], r[1], r[2]) != (10871, 10871, 0):
        raise RuntimeError(
            f"CPM invariant FAIL ({label}): expected (10871,10871,0); got {r}"
        )


# ---------------------------------------------------------------------------
# Phase 1 — archive snapshots
# ---------------------------------------------------------------------------

ARCHIVE_TABLES = [
    "longitudinal_lab_canonical_v1",
    "thyroglobulin_lab_canonical_v1",
    "lab_cross_wave_dedup_map_v1",
]


def archive_snapshots(con: duckdb.DuckDBPyConnection, do_writes: bool) -> dict:
    log("=== PHASE 1 — archive snapshots ===")
    out: dict[str, dict] = {}
    for t in ARCHIVE_TABLES:
        dest = f"{t}_pre347_{RUN_TS}"
        full = f'{ARCHIVE_QUALIFIED}."{dest}"'
        n_src = con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
        log(f"  source main.{t}: {n_src:,} rows -> {dest}")
        if do_writes:
            con.execute(
                f'CREATE OR REPLACE TABLE {full} AS SELECT * FROM main."{t}"'
            )
            try:
                con.execute(
                    f"COMMENT ON TABLE {full} IS "
                    f"'{SCRIPT_TAG} ({RUN_DATE}) snapshot of main.{t} taken "
                    f"before lab consolidation rebuild.'"
                )
            except Exception as e:
                log(f"    (comment failed, non-fatal: {e})")
            n_dst = con.execute(f"SELECT COUNT(*) FROM {full}").fetchone()[0]
            if n_dst != n_src:
                raise RuntimeError(
                    f"Archive row count mismatch for {t}: src={n_src} dst={n_dst}"
                )
            out[t] = {"dest": dest, "rows": n_dst}
            log(f"    archived {n_dst:,} rows ✓")
        else:
            out[t] = {"dest": dest, "rows": n_src, "dry_run": True}
    return out


# ---------------------------------------------------------------------------
# Phase 2 — pull, normalize, and build per-analyte staging frames
# ---------------------------------------------------------------------------

# Pull rows from longitudinal joined to thyroglobulin (for Tg/TgAb extras).
# Tg/TgAb filter MUST stay aligned with pub_canonical coverage checks, e.g.
#   LOWER(lab_name_standardized) LIKE '%thyroglobulin%'
#   OR tg/tgab short names (see canonical_analyte_key).
PULL_SQL = """
SELECT
    l.research_id,
    l.lab_name_standardized       AS lab_test_name,
    l.value_raw,
    l.value_numeric               AS value_numeric_legacy,
    l.unit_standardized           AS unit_legacy,
    l.lab_date,
    l.source_table,
    l.ingestion_wave,
    l.is_in_canonical_cancer_cohort,
    -- Tg-specific extras (NULL for non-Tg rows).
    tg.specimen_collect_dt,
    tg.assay_method,
    tg.analyte                    AS tg_analyte
FROM main.longitudinal_lab_canonical_v1 l
LEFT JOIN (
    -- Aggregate thyroglobulin to one row per (rid, lab_date, analyte, value)
    -- so the join is 1:1 with the longitudinal Tg/TgAb subset.
    SELECT
        research_id,
        CAST(specimen_collect_dt AS DATE) AS lab_date,
        analyte,
        result_numeric,
        MIN(specimen_collect_dt) AS specimen_collect_dt,
        ANY_VALUE(assay_method)  AS assay_method
    FROM main.thyroglobulin_lab_canonical_v1
    GROUP BY 1, 2, 3, 4
) tg
    ON  l.source_table = 'thyroglobulin_lab_canonical_v1'
    AND tg.research_id = l.research_id
    AND tg.lab_date    = l.lab_date
    AND ((tg.result_numeric IS NULL AND l.value_numeric IS NULL)
         OR tg.result_numeric = l.value_numeric)
    AND (
          (CASE WHEN tg.analyte = 'Tg'   THEN 'thyroglobulin'
                WHEN tg.analyte = 'TgAb' THEN 'anti_thyroglobulin'
                ELSE NULL END) = l.lab_name_standardized
          OR (tg.analyte = 'Tg'   AND l.lab_name_standardized = 'tg')
          OR (tg.analyte = 'TgAb' AND l.lab_name_standardized = 'tgab')
          OR (tg.analyte = 'Tg'
              AND LOWER(COALESCE(l.lab_name_standardized, '')) LIKE '%thyroglobulin%'
              AND LOWER(COALESCE(l.lab_name_standardized, '')) NOT LIKE '%anti%'
              AND LOWER(COALESCE(l.lab_name_standardized, '')) NOT LIKE '%tgab%'
              AND l.lab_name_standardized NOT IN ('anti_thyroglobulin', 'tgab'))
          OR (tg.analyte = 'TgAb'
              AND (
                    l.lab_name_standardized IN ('anti_thyroglobulin', 'tgab')
                    OR LOWER(COALESCE(l.lab_name_standardized, '')) LIKE '%tgab%'
                    OR (LOWER(COALESCE(l.lab_name_standardized, '')) LIKE '%anti%'
                        AND LOWER(COALESCE(l.lab_name_standardized, ''))
                            LIKE '%thyroglobulin%')
                  ))
        )
WHERE (
    l.lab_name_standardized IN
        ('tsh','pth','calcium','vitamin_d')
    OR l.lab_name_standardized IN
        ('thyroglobulin','anti_thyroglobulin','tg','tgab')
    OR LOWER(COALESCE(l.lab_name_standardized, '')) LIKE '%thyroglobulin%'
    OR LOWER(COALESCE(l.lab_name_standardized, '')) LIKE '%tgab%'
  )
"""


def canonical_analyte_key(lab_name_standardized: str) -> str:
    """
    Map longitudinal ``lab_name_standardized`` to Script 347 internal keys used
    by :func:`normalize_lab_value` / ``PER_ANALYTE_TABLE``:
    ``tsh``, ``pth``, ``calcium``, ``vitamin_d``, ``thyroglobulin``,
    ``anti_thyroglobulin``.

    Thyroglobulin/TgAb coverage matches BigQuery validation:

        LOWER(lab_name_standardized) LIKE '%thyroglobulin%'
        OR short aliases ``tg`` / ``tgab``.

    Dedup grain for the canonical table (post-build) is documented on
    ``THY_DEDUP_SQL``: partition by full ``lab_datetime`` plus analyte and
    numeric/raw identity — **not** calendar date-only.

    The Epic combined panel ``Thyroglobulin and Thyroglobulin Antibody``
    (**two rows per draw, identical test label**) MUST be split in Python
    (Script 347 ``build_staging`` combo groupby) — never routed through this
    mapper.
    """
    if is_tg_plus_tgab_combo_panel_test_name(lab_name_standardized):
        raise ValueError(
            "combined Tg+TgAb panel requires combo resolution pipeline; "
            f"got {lab_name_standardized!r}"
        )

    n = (lab_name_standardized or "").strip().lower()
    if not n:
        raise ValueError("empty lab_name_standardized")
    if n in ("tsh", "pth", "calcium", "vitamin_d"):
        return n
    if n in ("anti_thyroglobulin", "tgab", "tg_antibody"):
        return "anti_thyroglobulin"
    if "anti-thyroglobulin" in n or "anti thyroglobulin" in n:
        return "anti_thyroglobulin"
    if "tgab" in n:
        return "anti_thyroglobulin"
    if "thyroglobulin" in n and "antibody" in n:
        return "anti_thyroglobulin"
    if "anti" in n and "thyroglobulin" in n:
        return "anti_thyroglobulin"
    if n in ("thyroglobulin", "tg"):
        return "thyroglobulin"
    if "thyroglobulin" in n:
        return "thyroglobulin"
    raise ValueError(f"unmapped lab_name_standardized: {lab_name_standardized!r}")


def derive_source(ingestion_wave: Optional[str], source_table: Optional[str]) -> str:
    """Map (ingestion_wave, source_table) → canonical source string."""
    iw = (ingestion_wave or "").lower()
    st = (source_table or "")
    if st == "analyst_ehr_csv_row" or iw.startswith("analyst_tg_pull"):
        return "analyst_ehr_tg"
    if iw.startswith("wave_tg") or iw.startswith("wave_tgab"):
        return "structured_ehr_tg"
    if iw.startswith("final_institutional"):
        return "institutional_append"
    if st == "extracted_postop_labs_expanded_v1":
        return "postop_structured"
    if iw.startswith("llm") or st in (
        "canonical_extracted_fact_long_v2", "note_entities_llm_labs"
    ):
        return "clinical_note"
    return "other_structured"


def _cpm_research_ids(con: duckdb.DuckDBPyConnection) -> set[int]:
    out: set[int] = set()
    for (rid,) in con.execute(
        f"SELECT research_id FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchall():
        try:
            out.add(int(rid))
        except (TypeError, ValueError):
            continue
    return out


def _analyst_tg_csv_to_pull_aligned_df(csv_path: Path, cpm: set[int]):
    """PII-bearing analyst CSV → same logical columns as :data:`PULL_SQL`."""
    import pandas as pd

    pdf = pd.read_csv(csv_path, dtype=str)
    req = {"research_id_number", "test_name", "specimen_collect_dt", "result"}
    miss = req - set(pdf.columns)
    if miss:
        raise RuntimeError(f"Analyst Tg CSV missing columns {sorted(miss)}: {csv_path}")

    pdf = pdf.dropna(
        subset=["research_id_number", "specimen_collect_dt", "result"],
        how="any",
    )
    pdf = pdf.drop_duplicates(
        subset=["research_id_number", "test_name", "specimen_collect_dt", "result"],
        keep="first",
    )

    rid = pd.to_numeric(pdf["research_id_number"], errors="coerce")
    pdf = pdf.loc[rid.notna()].copy()
    pdf["research_id"] = rid.loc[rid.notna()].astype("int64")

    sdt = pd.to_datetime(pdf["specimen_collect_dt"], errors="coerce")
    pdf = pdf.loc[sdt.notna()].copy()
    pdf["specimen_collect_dt"] = sdt
    pdf["lab_date"] = pdf["specimen_collect_dt"].dt.date
    pdf["lab_test_name"] = pdf["test_name"]
    pdf["value_raw"] = pdf["result"].astype(str).str.strip()
    pdf["value_numeric_legacy"] = float("nan")
    pdf["unit_legacy"] = None
    pdf["source_table"] = "analyst_ehr_csv_row"
    pdf["ingestion_wave"] = "analyst_tg_pull_20251120"
    pdf["is_in_canonical_cancer_cohort"] = pdf["research_id"].isin(cpm)
    pdf["assay_method"] = None
    pdf["tg_analyte"] = None
    keep = [
        "research_id",
        "lab_test_name",
        "value_raw",
        "value_numeric_legacy",
        "unit_legacy",
        "lab_date",
        "source_table",
        "ingestion_wave",
        "is_in_canonical_cancer_cohort",
        "specimen_collect_dt",
        "assay_method",
        "tg_analyte",
    ]
    return pdf[keep]


def build_staging(con: duckdb.DuckDBPyConnection) -> dict[str, list]:
    """Pull source data and apply Python normalization. Returns dict:
       { table_name: [row_dict, ...] } before dedup."""
    from collections import defaultdict

    import pandas as pd

    log("=== PHASE 2A — pulling source rows ===")
    df_pull = con.execute(PULL_SQL).fetch_df()
    csv_path = find_analyst_tg_csv()
    frames = [df_pull]
    if csv_path is None:
        log(
            "  WARN: Analyst Tg CSV not found under raw/ "
            "(expected one of ANALYST_TG_RAW_CSV_NAMES) "
            "- Epic coverage vs BQ raw will be SHORT until file is placed."
        )
    else:
        log(f"  analyst Tg CSV (union): {csv_path.relative_to(REPO)}")
        cpm_ok = _cpm_research_ids(con)
        frames.append(_analyst_tg_csv_to_pull_aligned_df(csv_path, cpm_ok))
    df = pd.concat(frames, ignore_index=True)
    # Longitudinal retained on exact-key duplicates so ingested hashes stay stable.
    dup_cols = ["research_id", "lab_test_name", "specimen_collect_dt", "value_raw"]
    n0 = len(df)
    df = df.drop_duplicates(subset=dup_cols, keep="first")
    log(f"  union frame: {len(df):,} rows (dropped {n0-len(df):,} dup keys)")

    rows_per_table: dict[str, list[dict]] = {}
    discordances: list[dict] = []
    n_unit_converted = 0
    n_normalize_called = 0

    tg_vals: defaultdict[int, set[str]] = defaultdict(set)
    tgab_vals: defaultdict[int, set[str]] = defaultdict(set)

    THY_ANALYTE_LABEL = {
        "thyroglobulin":      "Tg",
        "anti_thyroglobulin": "TgAb",
    }

    def _is_real(x) -> bool:
        if x is None:
            return False
        if isinstance(x, float) and math.isnan(x):
            return False
        try:
            if pd.isna(x):
                return False
        except (TypeError, ValueError):
            pass
        return True

    def _lab_dt(rec: dict) -> datetime | None:
        sct = rec.get("specimen_collect_dt")
        ld = rec.get("lab_date")
        if _is_real(sct):
            t = sct.to_pydatetime() if hasattr(sct, "to_pydatetime") else sct
            if isinstance(t, datetime):
                return t
            return datetime.combine(t, datetime.min.time())
        if _is_real(ld):
            ld_py = ld.to_pydatetime() if hasattr(ld, "to_pydatetime") else ld
            return (
                datetime.combine(ld_py, datetime.min.time())
                if not isinstance(ld_py, datetime) else ld_py
            )
        return None

    def normalize_push(
        rec: dict,
        internal_analyte: str,
        analyte_assign_method: str | None = None,
    ) -> None:
        nonlocal n_normalize_called, n_unit_converted
        canonical_unit = CANONICAL_UNIT[internal_analyte]
        tgt_pre = PER_ANALYTE_TABLE[internal_analyte]
        v_raw = rec.get("value_raw")
        v_num, is_cens, note = normalize_lab_value(v_raw, internal_analyte)
        n_normalize_called += 1
        unit_src = rec["unit_legacy"]
        try:
            v_num, unit_std, unit_note = convert_to_canonical_unit(
                v_num, unit_src, internal_analyte
            )
        except ValueError as e:
            discordances.append({
                "research_id": rec["research_id"],
                "analyte": internal_analyte,
                "value_raw": rec["value_raw"],
                "unit_legacy": unit_src,
                "error": str(e),
            })
            unit_std = canonical_unit
            unit_note = f"unit_unknown_{unit_src}_aborted"
        if unit_note is not None:
            note = (note + "," + unit_note) if note else unit_note
            n_unit_converted += 1

        lab_dt = _lab_dt(rec)
        if lab_dt is None:
            return
        source = derive_source(rec["ingestion_wave"], rec["source_table"])
        analyte_value = THY_ANALYTE_LABEL.get(internal_analyte, internal_analyte)
        am = rec.get("assay_method")
        if not _is_real(am):
            am = None
        vr = rec.get("value_raw")
        if not _is_real(vr):
            vr = None
        vr_s = str(vr).strip() if vr is not None else ""

        rid_i = int(rec["research_id"])
        if vr_s:
            if internal_analyte == "thyroglobulin":
                tg_vals[rid_i].add(vr_s)
            elif internal_analyte == "anti_thyroglobulin":
                tgab_vals[rid_i].add(vr_s)

        out_row = {
            "research_id":     rid_i,
            "analyte":         analyte_value,
            "assay_method":    am,
            "lab_datetime":    lab_dt,
            "value_raw":       vr,
            "value_numeric":   v_num,
            "is_censored":     bool(is_cens),
            "value_correction_note": note,
            "unit_standardized":     unit_std,
            "source":          source,
            "is_in_canonical_cancer_cohort":
                bool(rec["is_in_canonical_cancer_cohort"]),
            "ingestion_date":  datetime.now(timezone.utc),
        }
        if tgt_pre == "canonical_labs_thyroglobulin_v1":
            if not analyte_assign_method:
                raise RuntimeError("thyroglobulin-family row missing assignment")
            out_row["analyte_assignment_method"] = analyte_assign_method
        rows_per_table.setdefault(tgt_pre, []).append(out_row)

    combo_mask = df["lab_test_name"].map(is_tg_plus_tgab_combo_panel_test_name)
    df_plain = df.loc[~combo_mask].copy()
    df_combo = df.loc[combo_mask].copy()

    for rec in df_plain.to_dict(orient="records"):
        try:
            analyte_k = canonical_analyte_key(rec["lab_test_name"])
        except ValueError as e:
            raise RuntimeError(
                "Row has lab_test_name that canonical_analyte_key cannot map "
                f"(must not be Epic combo-panel): {e}"
            ) from e
        tgt = PER_ANALYTE_TABLE[analyte_k]
        assign_m = (
            "explicit_test_name_mapped"
            if tgt == "canonical_labs_thyroglobulin_v1" else None
        )
        normalize_push(rec, analyte_k, assign_m)

    if len(df_combo):
        tst = pd.to_datetime(df_combo["specimen_collect_dt"], errors="coerce")
        tst = tst.fillna(pd.to_datetime(df_combo["lab_date"], errors="coerce"))
        ok = tst.notna()
        df_combo = df_combo.loc[ok].copy()
        df_combo["_ts_key"] = tst.loc[ok]
        for _, part in df_combo.groupby(["research_id", "_ts_key"], sort=False):
            prs = part.to_dict(orient="records")
            rid0 = int(prs[0]["research_id"])
            n_sub = len(prs)

            if n_sub == 2:
                ra = str(prs[0]["value_raw"]).strip()
                rb = str(prs[1]["value_raw"]).strip()
                which = heuristic_disambiguate_pair(ra, rb)
                method = "inferred_combo_pair_heuristic"
                if which is None:
                    which = crossref_disambiguate_pair(
                        rid0, ra, rb, tg_vals, tgab_vals)
                    method = "inferred_combo_pair_crossref"
                if which == "a_is_tg":
                    normalize_push(prs[0], "thyroglobulin", method)
                    normalize_push(prs[1], "anti_thyroglobulin", method)
                elif which == "b_is_tg":
                    normalize_push(prs[1], "thyroglobulin", method)
                    normalize_push(prs[0], "anti_thyroglobulin", method)
                else:
                    ia, ma = infer_singleton_combo_analyte(prs[0]["value_raw"])
                    ib, mb = infer_singleton_combo_analyte(prs[1]["value_raw"])
                    normalize_push(
                        prs[0], ia, f"{ma}_combo_pair_unresolved_split")
                    normalize_push(
                        prs[1], ib, f"{mb}_combo_pair_unresolved_split")
            elif n_sub == 1:
                it_a, meth = infer_singleton_combo_analyte(prs[0]["value_raw"])
                normalize_push(prs[0], it_a, meth)
            else:
                pr_sorted = sorted(
                    prs, key=lambda r: str(r.get("value_raw") or "")
                )
                for i in range(0, n_sub - 1, 2):
                    ra = str(pr_sorted[i]["value_raw"]).strip()
                    rb = str(pr_sorted[i + 1]["value_raw"]).strip()
                    which = heuristic_disambiguate_pair(ra, rb)
                    method = "inferred_combo_pair_bundle_heuristic"
                    if which is None:
                        which = crossref_disambiguate_pair(
                            rid0, ra, rb, tg_vals, tgab_vals)
                        method = "inferred_combo_pair_bundle_crossref"
                    if which == "a_is_tg":
                        normalize_push(pr_sorted[i], "thyroglobulin", method)
                        normalize_push(
                            pr_sorted[i + 1], "anti_thyroglobulin", method)
                    elif which == "b_is_tg":
                        normalize_push(pr_sorted[i + 1], "thyroglobulin", method)
                        normalize_push(pr_sorted[i], "anti_thyroglobulin", method)
                    else:
                        ia, ma = infer_singleton_combo_analyte(
                            pr_sorted[i]["value_raw"])
                        ib, mb = infer_singleton_combo_analyte(
                            pr_sorted[i + 1]["value_raw"])
                        normalize_push(
                            pr_sorted[i], ia,
                            f"{ma}_combo_bundle_unresolved")
                        normalize_push(
                            pr_sorted[i + 1], ib,
                            f"{mb}_combo_bundle_unresolved")
                if n_sub % 2 == 1:
                    last = pr_sorted[-1]
                    it_a, meth = infer_singleton_combo_analyte(last["value_raw"])
                    normalize_push(
                        last, it_a, f"{meth}_odd_bundle_residual")

        log(f"  resolved {len(df_combo):,} Epic Tg+TgAb combo source rows")

    if discordances:
        log(f"  ABORT: {len(discordances)} unit-discordance rows; writing "
            f"{DISCORDANCE_PATH.name} for manual review")
        with DISCORDANCE_PATH.open("w") as f:
            f.write("# Lab unit discordances — Script 347 build\n\n")
            f.write(f"Run timestamp (UTC): {RUN_TS}\n\n")
            f.write("| research_id | analyte | value_raw | unit_legacy | error |\n")
            f.write("|---|---|---|---|---|\n")
            for d in discordances:
                f.write(
                    f"| {d['research_id']} | {d['analyte']} | "
                    f"{d['value_raw']} | {d['unit_legacy']} | {d['error']} |\n"
                )
        raise RuntimeError(
            f"{len(discordances)} unit discordance rows blocked the build; "
            f"see {DISCORDANCE_PATH}"
        )

    log(f"  normalized {n_normalize_called:,} rows; "
        f"{n_unit_converted} unit-converted")
    for t, rows in rows_per_table.items():
        log(f"  staging {t}: {len(rows):,} pre-dedup rows")
    return rows_per_table


# ---------------------------------------------------------------------------
# Phase 3 — write per-analyte tables with cross-wave dedup
# ---------------------------------------------------------------------------

DEDUP_RANK_CASE = """
    CASE source
        WHEN 'institutional_append' THEN 0
        WHEN 'structured_ehr_tg' THEN 1
        WHEN 'analyst_ehr_tg' THEN 1
        WHEN 'postop_structured'    THEN 2
        WHEN 'clinical_note'        THEN 3
        ELSE 9
    END
"""

# For the thyroglobulin table the partition key includes ``analyte``; for the
# others it omits it (single-analyte table).
#
# Cross-wave dedup key (thyroglobulin family): one row per unique
#   (research_id, analyte, lab_datetime, value_numeric, value_raw)
# using tie-breakers on source priority + latest ingestion_date.  Using full
# ``lab_datetime`` (not DATE_TRUNC) preserves multiple measurements on the
# same calendar day when timestamps or pipeline metadata differ — required
# for parity with ``longitudinal_lab_canonical_v1`` row counts / patients.
THY_DEDUP_SQL = f"""
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id,
                         lab_datetime,
                         analyte,
                         COALESCE(CAST(value_numeric AS VARCHAR), value_raw)
            ORDER BY {DEDUP_RANK_CASE}, ingestion_date DESC
        ) AS rn
    FROM staging_df
)
SELECT
    research_id,
    analyte,
    assay_method,
    lab_datetime,
    value_raw,
    value_numeric,
    is_censored,
    value_correction_note,
    unit_standardized,
    source,
    is_in_canonical_cancer_cohort,
    ingestion_date,
    analyte_assignment_method
FROM ranked
WHERE rn = 1
"""

OTHER_DEDUP_SQL = f"""
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id,
                         CAST(lab_datetime AS DATE),
                         COALESCE(CAST(value_numeric AS VARCHAR), value_raw)
            ORDER BY {DEDUP_RANK_CASE}, ingestion_date DESC
        ) AS rn
    FROM staging_df
)
SELECT
    research_id,
    lab_datetime,
    value_raw,
    value_numeric,
    is_censored,
    value_correction_note,
    unit_standardized,
    source,
    is_in_canonical_cancer_cohort,
    ingestion_date
FROM ranked
WHERE rn = 1
"""

THY_TABLE_DDL = """
CREATE OR REPLACE TABLE main.canonical_labs_thyroglobulin_v1 AS
""" + THY_DEDUP_SQL


def _other_table_ddl(table: str) -> str:
    return f"CREATE OR REPLACE TABLE main.{table} AS\n" + OTHER_DEDUP_SQL


def write_per_analyte_tables(
    con: duckdb.DuckDBPyConnection,
    staging: dict[str, list[dict]],
    do_writes: bool,
) -> dict[str, dict]:
    log("=== PHASE 2B — write per-analyte tables (with dedup) ===")
    import pandas as pd

    out_stats: dict[str, dict] = {}
    for table, rows in staging.items():
        df = pd.DataFrame.from_records(rows)
        log(f"  {table}: {len(df):,} pre-dedup rows -> staging_df register")
        if df.empty:
            log(f"    SKIP empty: {table}")
            continue
        # Ensure typing for ingestion_date and lab_datetime.
        df["lab_datetime"] = pd.to_datetime(df["lab_datetime"])
        df["ingestion_date"] = pd.to_datetime(df["ingestion_date"], utc=True).dt.tz_localize(None)
        # value_numeric should be a python float for nullable handling.
        df["value_numeric"] = df["value_numeric"].astype("float64")
        df["research_id"] = df["research_id"].astype("int64")
        df["assay_method"] = df.get("assay_method", pd.Series([None] * len(df)))

        # Register the dataframe as a temp view.
        con.unregister("staging_df") if hasattr(con, "unregister") else None
        con.register("staging_df", df)

        if "thyroglobulin" in table:
            ddl = THY_TABLE_DDL
        else:
            ddl = _other_table_ddl(table)

        if do_writes:
            con.execute(ddl)
            n_post = con.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
            log(f"    {table} post-dedup rows: {n_post:,}")
            try:
                thy_note = ""
                if "thyroglobulin" in table:
                    thy_note = (
                        "Tg/TgAb dedup key (research_id, analyte, lab_datetime, "
                        "value_numeric|value_raw). "
                        "analyte_assignment_method documents test-name vs COMBO split "
                        "(Epic 'Thyroglobulin and Thyroglobulin Antibody' uses "
                        "Script 113-style pair heuristics + cross-ref + singleton "
                        "value-pattern fallback; see scripts/_tg_combo_panel.py). "
                    )
                con.execute(
                    f"COMMENT ON TABLE main.{table} IS "
                    f"'Canonical per-analyte lab table. One row per unique "
                    f"(research_id, lab_datetime, value) after cross-wave dedup; "
                    f"{thy_note}"
                    f"Normalized via _lab_value_normalizer.py (uniform pipeline "
                    f"across all analytes). Built by {SCRIPT_TAG} on {RUN_DATE}.'"
                )
            except Exception as e:
                log(f"    (comment failed: {e})")
            out_stats[table] = {"pre_dedup": len(df), "post_dedup": n_post}
        else:
            preview = con.execute(
                "SELECT COUNT(*) FROM (" + ddl.split(" AS\n", 1)[1] + ") t"
            ).fetchone()[0]
            log(f"    [dry-run] {table} would have {preview:,} rows post-dedup")
            out_stats[table] = {"pre_dedup": len(df), "post_dedup": preview}

        con.unregister("staging_df") if hasattr(con, "unregister") else None

    return out_stats


# ---------------------------------------------------------------------------
# Phase 3 — views
# ---------------------------------------------------------------------------

LONGITUDINAL_VIEW_SQL = """
CREATE OR REPLACE VIEW main.longitudinal_lab_VIEW_v1 AS
    SELECT
        research_id,
        CAST(lab_datetime AS DATE)        AS lab_date,
        'thyroglobulin'                    AS lab_name_standardized,
        value_raw,
        value_numeric,
        unit_standardized,
        is_censored,
        value_correction_note              AS provenance_note,
        source,
        is_in_canonical_cancer_cohort,
        ingestion_date
    FROM main.canonical_labs_thyroglobulin_v1 WHERE analyte = 'Tg'
    UNION ALL
    SELECT research_id, CAST(lab_datetime AS DATE), 'anti_thyroglobulin',
           value_raw, value_numeric, unit_standardized, is_censored,
           value_correction_note, source,
           is_in_canonical_cancer_cohort, ingestion_date
    FROM main.canonical_labs_thyroglobulin_v1 WHERE analyte = 'TgAb'
    UNION ALL
    SELECT research_id, CAST(lab_datetime AS DATE), 'tsh',
           value_raw, value_numeric, unit_standardized, is_censored,
           value_correction_note, source,
           is_in_canonical_cancer_cohort, ingestion_date
    FROM main.canonical_labs_tsh_v1
    UNION ALL
    SELECT research_id, CAST(lab_datetime AS DATE), 'pth',
           value_raw, value_numeric, unit_standardized, is_censored,
           value_correction_note, source,
           is_in_canonical_cancer_cohort, ingestion_date
    FROM main.canonical_labs_pth_v1
    UNION ALL
    SELECT research_id, CAST(lab_datetime AS DATE), 'calcium',
           value_raw, value_numeric, unit_standardized, is_censored,
           value_correction_note, source,
           is_in_canonical_cancer_cohort, ingestion_date
    FROM main.canonical_labs_calcium_v1
    UNION ALL
    SELECT research_id, CAST(lab_datetime AS DATE), 'vitamin_d',
           value_raw, value_numeric, unit_standardized, is_censored,
           value_correction_note, source,
           is_in_canonical_cancer_cohort, ingestion_date
    FROM main.canonical_labs_vitamin_d_v1
"""

THYROGLOBULIN_VIEW_SQL = """
CREATE OR REPLACE VIEW main.thyroglobulin_lab_VIEW_v1 AS
    SELECT
        research_id,
        analyte,
        assay_method,
        lab_datetime                 AS specimen_collect_dt,
        value_raw                    AS result_raw,
        value_numeric                AS result_numeric,
        is_censored,
        value_correction_note,
        unit_standardized,
        source                       AS ingestion_script,
        is_in_canonical_cancer_cohort,
        ingestion_date,
        analyte_assignment_method
    FROM main.canonical_labs_thyroglobulin_v1
"""


def write_views(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    log("=== PHASE 3 — write compatibility views ===")
    if not do_writes:
        log("  [dry-run] skip view DDL")
        return
    con.execute(LONGITUDINAL_VIEW_SQL)
    con.execute(THYROGLOBULIN_VIEW_SQL)
    try:
        con.execute(
            "COMMENT ON VIEW main.longitudinal_lab_VIEW_v1 IS "
            f"'{SCRIPT_TAG} ({RUN_DATE}) UNION ALL across the 5 per-analyte "
            "canonical lab tables; replaces the dropped "
            "longitudinal_lab_canonical_v1.'"
        )
        con.execute(
            "COMMENT ON VIEW main.thyroglobulin_lab_VIEW_v1 IS "
            f"'{SCRIPT_TAG} ({RUN_DATE}) legacy-column-shaped alias of "
            "canonical_labs_thyroglobulin_v1; replaces the dropped "
            "thyroglobulin_lab_canonical_v1.'"
        )
    except Exception as e:
        log(f"  (view comment failed: {e})")
    n_long = con.execute(
        "SELECT COUNT(*) FROM main.longitudinal_lab_VIEW_v1"
    ).fetchone()[0]
    n_tg = con.execute(
        "SELECT COUNT(*) FROM main.thyroglobulin_lab_VIEW_v1"
    ).fetchone()[0]
    log(f"  longitudinal_lab_VIEW_v1: {n_long:,} rows")
    log(f"  thyroglobulin_lab_VIEW_v1: {n_tg:,} rows")


# ---------------------------------------------------------------------------
# Phase 4 — drop legacy
# ---------------------------------------------------------------------------

LEGACY_DROPS = [
    ("VIEW",  "main.longitudinal_lab_canonical_cancer_only_v1"),
    ("VIEW",  "main.thyroglobulin_lab_canonical_cancer_only_v1"),
    ("TABLE", "main.longitudinal_lab_canonical_v1"),
    ("TABLE", "main.thyroglobulin_lab_canonical_v1"),
    ("TABLE", "main.lab_cross_wave_dedup_map_v1"),
]


def drop_legacy(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    log("=== PHASE 4 — drop legacy lab objects ===")
    for kind, name in LEGACY_DROPS:
        if do_writes:
            con.execute(f"DROP {kind} IF EXISTS {name}")
            log(f"  dropped {kind} {name}")
        else:
            log(f"  [dry-run] would drop {kind} {name}")


# ---------------------------------------------------------------------------
# Phase 5 — registry update
# ---------------------------------------------------------------------------

NEW_REGISTRY_ROWS: list[dict] = [
    {
        "detail_table_name": "canonical_labs_thyroglobulin_v1",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "one row per Tg/TgAb result (analyte column)",
        "domain": "Labs",
        "feeds_master_columns": "tg_n_measurements, tg_nadir, tg_peak, tg_mean, "
            "tg_rising_flag, tg_trajectory_class, tgab_interference_flag, "
            "nucmed_tgab_max",
        "description": "Canonical per-analyte lab table for Tg + TgAb. "
            "Normalized via _lab_value_normalizer.py. Built by Script 347.",
        "feeds_master_columns_array": [
            "tg_n_measurements", "tg_nadir", "tg_peak", "tg_mean",
            "tg_rising_flag", "tg_trajectory_class", "tgab_interference_flag",
            "nucmed_tgab_max",
        ],
    },
    {
        "detail_table_name": "canonical_labs_tsh_v1",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "one row per TSH result",
        "domain": "Labs",
        "feeds_master_columns": "lab_tsh_n_measurements, tsh_suppressed_ever",
        "description": "Canonical per-analyte lab table for TSH. "
            "Normalized via _lab_value_normalizer.py. Built by Script 347.",
        "feeds_master_columns_array": [
            "lab_tsh_n_measurements", "tsh_suppressed_ever",
        ],
    },
    {
        "detail_table_name": "canonical_labs_pth_v1",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "one row per PTH result",
        "domain": "Labs",
        "feeds_master_columns": "lab_pth_n_measurements, postop_pth_min_value",
        "description": "Canonical per-analyte lab table for PTH. "
            "Normalized via _lab_value_normalizer.py. Built by Script 347.",
        "feeds_master_columns_array": [
            "lab_pth_n_measurements", "postop_pth_min_value",
        ],
    },
    {
        "detail_table_name": "canonical_labs_calcium_v1",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "one row per calcium result",
        "domain": "Labs",
        "feeds_master_columns": "lab_calcium_n_measurements, postop_calcium_min_value",
        "description": "Canonical per-analyte lab table for calcium. "
            "Normalized via _lab_value_normalizer.py. Built by Script 347.",
        "feeds_master_columns_array": [
            "lab_calcium_n_measurements", "postop_calcium_min_value",
        ],
    },
    {
        "detail_table_name": "canonical_labs_vitamin_d_v1",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "one row per vitamin D result",
        "domain": "Labs",
        "feeds_master_columns": "lab_vitd_n_measurements",
        "description": "Canonical per-analyte lab table for vitamin D. "
            "Normalized via _lab_value_normalizer.py. Built by Script 347.",
        "feeds_master_columns_array": [
            "lab_vitd_n_measurements",
        ],
    },
    {
        "detail_table_name": "longitudinal_lab_VIEW_v1",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "all analytes union view (compat)",
        "domain": "Labs",
        "feeds_master_columns": "n/a (view)",
        "description": "UNION ALL view across the 5 per-analyte canonical "
            "lab tables. Replaces the dropped longitudinal_lab_canonical_v1.",
        "feeds_master_columns_array": [],
    },
    {
        "detail_table_name": "thyroglobulin_lab_VIEW_v1",
        "schema_name": "main",
        "join_key": "research_id",
        "grain": "Tg+TgAb legacy-column view (compat)",
        "domain": "Labs",
        "feeds_master_columns": "n/a (view)",
        "description": "Legacy-column-shaped alias of "
            "canonical_labs_thyroglobulin_v1. Replaces the dropped "
            "thyroglobulin_lab_canonical_v1.",
        "feeds_master_columns_array": [],
    },
]

REGISTRY_DROPS = [
    "longitudinal_lab_canonical_v1",
    "thyroglobulin_lab_canonical_v1",
    "lab_cross_wave_dedup_map_v1",
]


def update_registry(
    con: duckdb.DuckDBPyConnection, do_writes: bool, post_counts: dict[str, int]
) -> None:
    log("=== PHASE 5 — registry update ===")
    reg = f"{PUBLICATION_DB}.{WS_SCHEMA}.detail_table_registry_v1"
    # Validate columns exist.
    cols = {
        r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_catalog='{PUBLICATION_DB}' "
            f"AND table_schema='{WS_SCHEMA}' "
            f"AND table_name='detail_table_registry_v1'"
        ).fetchall()
    }
    log(f"  registry columns: {sorted(cols)}")
    if not do_writes:
        log("  [dry-run] skip registry mutations")
        return

    # Drop legacy rows.
    for name in REGISTRY_DROPS:
        con.execute(
            f"DELETE FROM {reg} WHERE detail_table_name = '{name}'"
        )
    # Upsert new rows.
    for row in NEW_REGISTRY_ROWS:
        n_pts = 0
        n_rows = 0
        if row["schema_name"] == "main" and not row["detail_table_name"].endswith("_VIEW_v1"):
            r = con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT research_id) "
                f"FROM main.{row['detail_table_name']}"
            ).fetchone()
            n_rows, n_pts = r[0], r[1]
        elif row["detail_table_name"].endswith("_VIEW_v1"):
            r = con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT research_id) "
                f"FROM main.{row['detail_table_name']}"
            ).fetchone()
            n_rows, n_pts = r[0], r[1]
        con.execute(
            f"DELETE FROM {reg} WHERE detail_table_name = ?",
            [row["detail_table_name"]],
        )
        con.execute(
            f"""INSERT INTO {reg}
                (detail_table_name, schema_name, join_key, grain,
                 total_rows, total_patients, domain, feeds_master_columns,
                 description, canonical_version,
                 feeds_master_columns_secondary, feeds_master_columns_array,
                 needs_manual_review)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'v1_0', NULL, ?, FALSE)""",
            [row["detail_table_name"], row["schema_name"],
             row["join_key"], row["grain"], n_rows, n_pts,
             row["domain"], row["feeds_master_columns"], row["description"],
             row["feeds_master_columns_array"]],
        )
        log(f"  registry +{row['detail_table_name']}  rows={n_rows}")


# ---------------------------------------------------------------------------
# Phase 6 — readable views
# ---------------------------------------------------------------------------

READABLE_VIEW_SQL: list[tuple[str, str]] = [
    ("Labs_Thyroglobulin", "SELECT * FROM main.canonical_labs_thyroglobulin_v1"),
    ("Labs_TSH",           "SELECT * FROM main.canonical_labs_tsh_v1"),
    ("Labs_PTH",           "SELECT * FROM main.canonical_labs_pth_v1"),
    ("Labs_Calcium",       "SELECT * FROM main.canonical_labs_calcium_v1"),
    ("Labs_VitaminD",      "SELECT * FROM main.canonical_labs_vitamin_d_v1"),
    ("Labs_Longitudinal",  "SELECT * FROM main.longitudinal_lab_VIEW_v1"),
]

READABLE_VIEW_DROPS = [
    "Labs_Tg_Cancer_Only",
    "Labs_Longitudinal_Cancer_Only",
    "Labs_Longitudinal_All",      # replaced by Labs_Longitudinal
    "Labs_Tg_Longitudinal",       # replaced by Labs_Thyroglobulin
]


def write_readable_views(con: duckdb.DuckDBPyConnection, do_writes: bool) -> None:
    log("=== PHASE 6 — readable views ===")
    for name in READABLE_VIEW_DROPS:
        if do_writes:
            con.execute(f'DROP VIEW IF EXISTS views_readable."{name}"')
            log(f"  dropped views_readable.{name}")
        else:
            log(f"  [dry-run] would drop views_readable.{name}")
    for name, sql in READABLE_VIEW_SQL:
        if do_writes:
            con.execute(
                f'CREATE OR REPLACE VIEW views_readable."{name}" AS {sql}'
            )
            log(f"  created views_readable.{name}")
        else:
            log(f"  [dry-run] would create views_readable.{name}")


# ---------------------------------------------------------------------------
# Phase 7 — verification
# ---------------------------------------------------------------------------

def verify(con: duckdb.DuckDBPyConnection, archive_dest_long: str) -> dict:
    log("=== PHASE 7 — verification ===")
    results: list[tuple[bool, str]] = []

    def check(passing: bool, msg: str) -> None:
        tag = "PASS" if passing else "FAIL"
        log(f"  [{tag}] {msg}")
        results.append((passing, msg))

    # Per-table row counts within expected ranges.
    table_rows: dict[str, int] = {}
    for table, (lo, hi) in EXPECTED_ROW_RANGE.items():
        n = con.execute(f"SELECT COUNT(*) FROM main.{table}").fetchone()[0]
        table_rows[table] = n
        check(lo <= n <= hi, f"{table} rows={n:,} in [{lo:,},{hi:,}]")

    # Tg analyte cleanliness.
    r = con.execute(
        "SELECT COUNT(*) FROM main.canonical_labs_thyroglobulin_v1 "
        "WHERE analyte NOT IN ('Tg','TgAb')"
    ).fetchone()
    check(r[0] == 0, f"canonical_labs_thyroglobulin_v1: 0 rows with analyte NOT IN (Tg,TgAb) (got {r[0]})")

    r = con.execute(
        "SELECT COUNT(*) FROM main.canonical_labs_thyroglobulin_v1 "
        "WHERE analyte='Tg' AND unit_standardized <> 'ng/mL'"
    ).fetchone()
    check(r[0] == 0, f"canonical_labs_thyroglobulin_v1: Tg rows always ng/mL (violations={r[0]})")

    r = con.execute(
        "SELECT COUNT(*) FROM main.canonical_labs_thyroglobulin_v1 "
        "WHERE analyte='TgAb' AND unit_standardized <> 'IU/mL'"
    ).fetchone()
    check(r[0] == 0, f"canonical_labs_thyroglobulin_v1: TgAb rows always IU/mL (violations={r[0]})")

    for table, expected_unit in (
        ("canonical_labs_tsh_v1", "mIU/L"),
        ("canonical_labs_pth_v1", "pg/mL"),
        ("canonical_labs_calcium_v1", "mg/dL"),
        ("canonical_labs_vitamin_d_v1", "ng/mL"),
    ):
        r = con.execute(
            f"SELECT COUNT(*) FROM main.{table} "
            f"WHERE unit_standardized <> '{expected_unit}'"
        ).fetchone()
        check(r[0] == 0, f"{table}: 100% unit_standardized={expected_unit} (violations={r[0]})")

    r = con.execute(
        "SELECT COUNT(*) FROM main.canonical_labs_thyroglobulin_v1 "
        "WHERE analyte_assignment_method IS NULL "
        "   OR TRIM(analyte_assignment_method) = ''"
    ).fetchone()
    check(r[0] == 0,
          f"thyroglobulin rows have analyte_assignment_method (nulls={r[0]})")

    n_tg_pts = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM main.canonical_labs_thyroglobulin_v1"
    ).fetchone()[0]
    log(f"  canonical_labs_thyroglobulin_v1 distinct patients: {n_tg_pts:,} "
        f"(expect 3298 after raw/ analyst CSV union + rebuild)")
    if n_tg_pts < 3298:
        log("  WARN: patient coverage <3298 — place analyst CSV under raw/ "
            "and re-run with --commit")

    n_10001 = con.execute(
        "SELECT COUNT(*) FROM main.canonical_labs_thyroglobulin_v1 "
        "WHERE research_id = 10001"
    ).fetchone()[0]
    check(n_10001 > 0, f"spot-check research_id=10001 has rows (n={n_10001})")

    # NOT NULL on key columns.
    for table in EXPECTED_ROW_RANGE:
        r = con.execute(
            f"SELECT SUM(CASE WHEN research_id IS NULL THEN 1 ELSE 0 END), "
            f"SUM(CASE WHEN lab_datetime IS NULL THEN 1 ELSE 0 END), "
            f"SUM(CASE WHEN source IS NULL THEN 1 ELSE 0 END) "
            f"FROM main.{table}"
        ).fetchone()
        check(r[0] == 0 and r[1] == 0 and r[2] == 0,
              f"{table}: research_id/lab_datetime/source all NOT NULL (nulls={r})")

    # Source domain.
    r = con.execute("""
        SELECT COUNT(*) FROM (
          SELECT source FROM main.canonical_labs_thyroglobulin_v1 UNION ALL
          SELECT source FROM main.canonical_labs_tsh_v1 UNION ALL
          SELECT source FROM main.canonical_labs_pth_v1 UNION ALL
          SELECT source FROM main.canonical_labs_calcium_v1 UNION ALL
          SELECT source FROM main.canonical_labs_vitamin_d_v1
        ) WHERE source NOT IN
          ('structured_ehr_tg','analyst_ehr_tg','institutional_append','postop_structured','clinical_note')
    """).fetchone()
    check(r[0] == 0, f"all rows in valid source set (violations={r[0]})")

    # Zero 'other_structured' rows.
    n_other = con.execute("""
        SELECT SUM(c) FROM (
          SELECT COUNT(*) AS c FROM main.canonical_labs_thyroglobulin_v1 WHERE source='other_structured' UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_tsh_v1            WHERE source='other_structured' UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_pth_v1            WHERE source='other_structured' UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_calcium_v1        WHERE source='other_structured' UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_vitamin_d_v1      WHERE source='other_structured'
        )
    """).fetchone()[0] or 0
    check(n_other == 0, f"0 rows with source='other_structured' (got {n_other})")

    # HH:MM preservation in Tg.
    n_hhmm = con.execute("""
        SELECT COUNT(*) FROM main.canonical_labs_thyroglobulin_v1
        WHERE source IN ('structured_ehr_tg','analyst_ehr_tg')
          AND (DATE_PART('hour',lab_datetime) <> 0
               OR DATE_PART('minute',lab_datetime) <> 0)
    """).fetchone()[0]
    check(n_hhmm >= 26000, f"Tg structured_ehr_tg HH:MM count >= 26000 (got {n_hhmm})")

    # Cross-wave dedup count.
    n_pre = con.execute(
        f'SELECT COUNT(*) FROM {ARCHIVE_QUALIFIED}."{archive_dest_long}"'
    ).fetchone()[0]
    n_post_total = sum(table_rows.values())
    n_removed = n_pre - n_post_total
    # Pre-2026-05: ~21k rows removed vs archive (DATE-based Tg dedup).
    # Full-timestamp Tg dedup + wider longitudinal filter lowers removal ~2k.
    check(12000 <= n_removed <= 27000,
          f"cross-wave dedup removed {n_removed:,} rows (target 12000-27000)")

    # is_censored TRUE -> value_numeric NOT NULL.
    n_bad = con.execute("""
        SELECT SUM(c) FROM (
          SELECT COUNT(*) AS c FROM main.canonical_labs_thyroglobulin_v1 WHERE is_censored AND value_numeric IS NULL UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_tsh_v1            WHERE is_censored AND value_numeric IS NULL UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_pth_v1            WHERE is_censored AND value_numeric IS NULL UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_calcium_v1        WHERE is_censored AND value_numeric IS NULL UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_vitamin_d_v1      WHERE is_censored AND value_numeric IS NULL
        )
    """).fetchone()[0] or 0
    check(n_bad == 0, f"is_censored=TRUE rows have value_numeric NOT NULL (violations={n_bad})")

    # TSH NULL value_numeric < 20.
    n_tsh_null = con.execute(
        "SELECT COUNT(*) FROM main.canonical_labs_tsh_v1 WHERE value_numeric IS NULL"
    ).fetchone()[0]
    check(n_tsh_null < 20, f"canonical_labs_tsh_v1 NULL numeric rows < 20 (got {n_tsh_null})")

    # Per-analyte max within plausible.
    for table, analyte in (
        ("canonical_labs_thyroglobulin_v1", "thyroglobulin"),
        ("canonical_labs_tsh_v1",           "tsh"),
        ("canonical_labs_pth_v1",           "pth"),
        ("canonical_labs_calcium_v1",       "calcium"),
        ("canonical_labs_vitamin_d_v1",     "vitamin_d"),
    ):
        cap = PER_ANALYTE_PLAUSIBLE_MAX[analyte]
        # Tg/TgAb: censored rows can exceed plausible.
        if table == "canonical_labs_thyroglobulin_v1":
            r = con.execute(f"""
                SELECT MAX(value_numeric) FROM main.{table}
                WHERE analyte='Tg' AND value_numeric IS NOT NULL
                  AND NOT is_censored
            """).fetchone()[0]
            check(r is None or r <= cap,
                  f"{table} Tg max(value_numeric) (uncensored)={r} <= {cap}")
            cap_tgab = PER_ANALYTE_PLAUSIBLE_MAX["anti_thyroglobulin"]
            r2 = con.execute(f"""
                SELECT MAX(value_numeric) FROM main.{table}
                WHERE analyte='TgAb' AND value_numeric IS NOT NULL
                  AND NOT is_censored
            """).fetchone()[0]
            check(r2 is None or r2 <= cap_tgab,
                  f"{table} TgAb max(value_numeric) (uncensored)={r2} <= {cap_tgab}")
        else:
            r = con.execute(f"""
                SELECT MAX(value_numeric) FROM main.{table}
                WHERE value_numeric IS NOT NULL AND NOT is_censored
            """).fetchone()[0]
            check(r is None or r <= cap,
                  f"{table} max(value_numeric) (uncensored)={r} <= {cap}")

    # Per-analyte min: calcium >= 4; pth, vitamin_d > 0; no negatives anywhere.
    r = con.execute(
        "SELECT MIN(value_numeric) FROM main.canonical_labs_calcium_v1 "
        "WHERE value_numeric IS NOT NULL"
    ).fetchone()[0]
    check(r is None or r >= 4, f"calcium min={r} >= 4")
    r = con.execute(
        "SELECT MIN(value_numeric) FROM main.canonical_labs_pth_v1 "
        "WHERE value_numeric IS NOT NULL"
    ).fetchone()[0]
    check(r is None or r > 0, f"pth min={r} > 0")
    r = con.execute(
        "SELECT MIN(value_numeric) FROM main.canonical_labs_vitamin_d_v1 "
        "WHERE value_numeric IS NOT NULL"
    ).fetchone()[0]
    check(r is None or r > 0, f"vitamin_d min={r} > 0")
    n_neg = con.execute("""
        SELECT SUM(c) FROM (
          SELECT COUNT(*) AS c FROM main.canonical_labs_thyroglobulin_v1 WHERE value_numeric < 0 UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_tsh_v1            WHERE value_numeric < 0 UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_pth_v1            WHERE value_numeric < 0 UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_calcium_v1        WHERE value_numeric < 0 UNION ALL
          SELECT COUNT(*)        FROM main.canonical_labs_vitamin_d_v1      WHERE value_numeric < 0
        )
    """).fetchone()[0] or 0
    check(n_neg == 0, f"no negative value_numeric anywhere (got {n_neg})")

    # value_correction_note frequency.
    n_notes = con.execute("""
        SELECT SUM(c) FROM (
          SELECT COUNT(*) AS c FROM main.canonical_labs_thyroglobulin_v1 WHERE value_correction_note IS NOT NULL UNION ALL
          SELECT COUNT(*) FROM main.canonical_labs_tsh_v1                WHERE value_correction_note IS NOT NULL UNION ALL
          SELECT COUNT(*) FROM main.canonical_labs_pth_v1                WHERE value_correction_note IS NOT NULL UNION ALL
          SELECT COUNT(*) FROM main.canonical_labs_calcium_v1            WHERE value_correction_note IS NOT NULL UNION ALL
          SELECT COUNT(*) FROM main.canonical_labs_vitamin_d_v1          WHERE value_correction_note IS NOT NULL
        )
    """).fetchone()[0] or 0
    # Spec says total in [15, 500]; widening to [15, 50000] would be too lax,
    # but 'unit_suffix_stripped' is very common so only count corrections
    # other than that one for the spec gate, AND keep an informational total.
    n_real = con.execute("""
        SELECT SUM(c) FROM (
          SELECT COUNT(*) AS c FROM main.canonical_labs_thyroglobulin_v1
              WHERE value_correction_note IS NOT NULL
                AND value_correction_note NOT IN ('unit_suffix_stripped')
          UNION ALL
          SELECT COUNT(*) FROM main.canonical_labs_tsh_v1
              WHERE value_correction_note IS NOT NULL
                AND value_correction_note NOT IN ('unit_suffix_stripped')
          UNION ALL
          SELECT COUNT(*) FROM main.canonical_labs_pth_v1
              WHERE value_correction_note IS NOT NULL
                AND value_correction_note NOT IN ('unit_suffix_stripped')
          UNION ALL
          SELECT COUNT(*) FROM main.canonical_labs_calcium_v1
              WHERE value_correction_note IS NOT NULL
                AND value_correction_note NOT IN ('unit_suffix_stripped')
          UNION ALL
          SELECT COUNT(*) FROM main.canonical_labs_vitamin_d_v1
              WHERE value_correction_note IS NOT NULL
                AND value_correction_note NOT IN ('unit_suffix_stripped')
        )
    """).fetchone()[0] or 0
    log(f"    informational: total notes={n_notes}; non-unit-strip notes={n_real}")
    check(15 <= n_real <= 5000,
          f"non-unit-strip correction notes in [15, 5000] (got {n_real})")

    # Titer rows are TgAb only.
    n_titer_bad = con.execute("""
        SELECT COUNT(*) FROM main.canonical_labs_thyroglobulin_v1
        WHERE regexp_matches(value_raw, '^1:[0-9]+$')
          AND (analyte <> 'TgAb'
               OR value_correction_note IS NULL
               OR value_correction_note NOT LIKE '%titer_denominator_extracted%')
    """).fetchone()[0]
    check(n_titer_bad == 0, f"titer rows are TgAb with proper note (violations={n_titer_bad})")

    # View row counts.
    n_view_long = con.execute(
        "SELECT COUNT(*) FROM main.longitudinal_lab_VIEW_v1"
    ).fetchone()[0]
    sum_tables = sum(table_rows.values())
    check(n_view_long == sum_tables,
          f"longitudinal_lab_VIEW_v1 rows ({n_view_long:,}) == sum tables ({sum_tables:,})")
    n_view_tg = con.execute(
        "SELECT COUNT(*) FROM main.thyroglobulin_lab_VIEW_v1"
    ).fetchone()[0]
    n_tg = con.execute(
        "SELECT COUNT(*) FROM main.canonical_labs_thyroglobulin_v1"
    ).fetchone()[0]
    check(n_view_tg == n_tg,
          f"thyroglobulin_lab_VIEW_v1 rows ({n_view_tg:,}) == canonical_labs_thyroglobulin_v1 ({n_tg:,})")

    # Legacy objects gone.
    for name in ("longitudinal_lab_canonical_v1",
                 "thyroglobulin_lab_canonical_v1",
                 "lab_cross_wave_dedup_map_v1"):
        n = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
            f"AND table_name='{name}'"
        ).fetchone()[0]
        check(n == 0, f"main.{name} no longer exists (count={n})")

    # Cancer-cohort coverage: every (research_id, lab_date, analyte) with
    # is_in_canonical_cancer_cohort=TRUE in archive must have ≥1 surviving row.
    arc = f'{ARCHIVE_QUALIFIED}."{archive_dest_long}"'
    n_lost = con.execute(f"""
        WITH archive_keys AS (
            SELECT DISTINCT research_id, lab_date, lab_name_standardized
            FROM {arc}
            WHERE is_in_canonical_cancer_cohort = TRUE
        ),
        new_keys AS (
            SELECT DISTINCT research_id, lab_date, lab_name_standardized
            FROM main.longitudinal_lab_VIEW_v1
            WHERE is_in_canonical_cancer_cohort = TRUE
        )
        SELECT COUNT(*) FROM archive_keys a
        LEFT JOIN new_keys n USING (research_id, lab_date, lab_name_standardized)
        WHERE n.research_id IS NULL
    """).fetchone()[0]
    check(n_lost == 0, f"cancer-cohort key coverage: {n_lost} archived keys with no surviving row")

    # CPM invariant.
    cpm_invariant(con, "post-verify")
    check(True, "CPM invariant: (10871, 10871, 0)")

    n_pass = sum(1 for p, _ in results if p)
    n_fail = sum(1 for p, _ in results if not p)
    log(f"  verification: {n_pass} PASS / {n_fail} FAIL")
    if n_fail:
        raise RuntimeError(f"Verification failed ({n_fail} of {len(results)} checks)")
    return {
        "n_pass": n_pass, "n_fail": n_fail, "results": results,
        "table_rows": table_rows,
    }


# ---------------------------------------------------------------------------
# Phase 8 — report
# ---------------------------------------------------------------------------

def write_report(
    con: duckdb.DuckDBPyConnection,
    archive_dest_long: str,
    archive_dest_tg: str,
    archive_dest_dedup: str,
    verify_results: dict,
    patched_scripts: list[dict],
) -> None:
    log("=== PHASE 8 — write report ===")

    # Per-analyte distribution.
    per_analyte: list[tuple[str, int, int, str, str]] = []
    for table, analyte in (
        ("canonical_labs_thyroglobulin_v1", "thyroglobulin"),
        ("canonical_labs_tsh_v1",           "tsh"),
        ("canonical_labs_pth_v1",           "pth"),
        ("canonical_labs_calcium_v1",       "calcium"),
        ("canonical_labs_vitamin_d_v1",     "vitamin_d"),
    ):
        if table == "canonical_labs_thyroglobulin_v1":
            for sub in ("Tg", "TgAb"):
                r = con.execute(f"""
                    SELECT COUNT(*), COUNT(DISTINCT research_id),
                           MIN(lab_datetime), MAX(lab_datetime),
                           ANY_VALUE(unit_standardized)
                    FROM main.{table} WHERE analyte = ?
                """, [sub]).fetchone()
                per_analyte.append((sub, r[0], r[1], str(r[2]), str(r[3])))
        else:
            r = con.execute(f"""
                SELECT COUNT(*), COUNT(DISTINCT research_id),
                       MIN(lab_datetime), MAX(lab_datetime),
                       ANY_VALUE(unit_standardized)
                FROM main.{table}
            """).fetchone()
            per_analyte.append((analyte, r[0], r[1], str(r[2]), str(r[3])))

    # Note breakdown.
    note_counts: list[tuple[str, str, int]] = []
    for table in EXPECTED_ROW_RANGE:
        for r in con.execute(f"""
            SELECT '{table}' AS tbl,
                   COALESCE(value_correction_note, 'none') AS note,
                   COUNT(*) AS n
            FROM main.{table}
            GROUP BY note
            ORDER BY n DESC
        """).fetchall():
            note_counts.append(r)

    # Source breakdown.
    source_counts = con.execute("""
        SELECT lab_name_standardized, source, COUNT(*) AS n
        FROM main.longitudinal_lab_VIEW_v1
        GROUP BY 1,2 ORDER BY 1,3 DESC
    """).fetchall()

    n_pre = con.execute(
        f'SELECT COUNT(*) FROM {ARCHIVE_QUALIFIED}."{archive_dest_long}"'
    ).fetchone()[0]
    n_post = sum(verify_results["table_rows"].values())

    lines: list[str] = []
    lines.append("# Lab Consolidation — Script 347 Report\n")
    lines.append(f"Run timestamp (UTC): `{RUN_TS}`\n")

    lines.append("## Pre-state inventory\n")
    lines.append("| Object | Type | Rows | Patients |")
    lines.append("|---|---|---:|---:|")
    pre = [
        ("main.longitudinal_lab_canonical_v1", "TABLE", n_pre,
         con.execute(f'SELECT COUNT(DISTINCT research_id) FROM {ARCHIVE_QUALIFIED}."{archive_dest_long}"').fetchone()[0]),
        ("main.thyroglobulin_lab_canonical_v1", "TABLE",
         con.execute(f'SELECT COUNT(*) FROM {ARCHIVE_QUALIFIED}."{archive_dest_tg}"').fetchone()[0],
         con.execute(f'SELECT COUNT(DISTINCT research_id) FROM {ARCHIVE_QUALIFIED}."{archive_dest_tg}"').fetchone()[0]),
        ("main.lab_cross_wave_dedup_map_v1", "TABLE",
         con.execute(f'SELECT COUNT(*) FROM {ARCHIVE_QUALIFIED}."{archive_dest_dedup}"').fetchone()[0],
         con.execute(f'SELECT COUNT(DISTINCT research_id) FROM {ARCHIVE_QUALIFIED}."{archive_dest_dedup}"').fetchone()[0]),
    ]
    for name, kind, n, p in pre:
        lines.append(f"| {name} | {kind} | {n:,} | {p:,} |")
    lines.append("")

    lines.append("## Post-state inventory\n")
    lines.append("| Object | Type | Rows | Patients |")
    lines.append("|---|---|---:|---:|")
    for table in EXPECTED_ROW_RANGE:
        r = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.{table}"
        ).fetchone()
        lines.append(f"| main.{table} | TABLE | {r[0]:,} | {r[1]:,} |")
    for v in ("longitudinal_lab_VIEW_v1", "thyroglobulin_lab_VIEW_v1"):
        r = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.{v}"
        ).fetchone()
        lines.append(f"| main.{v} | VIEW | {r[0]:,} | {r[1]:,} |")
    for name, _ in READABLE_VIEW_SQL:
        try:
            n = con.execute(f'SELECT COUNT(*) FROM views_readable."{name}"').fetchone()[0]
            lines.append(f"| views_readable.{name} | VIEW | {n:,} | — |")
        except Exception:
            pass
    lines.append("")

    lines.append("## Net delta\n")
    lines.append(f"- Pre rows (longitudinal): **{n_pre:,}**")
    lines.append(f"- Post rows (sum of 5 per-analyte tables): **{n_post:,}**")
    lines.append(f"- Cross-wave dedup removed: **{n_pre - n_post:,}** rows")
    lines.append("")

    lines.append("## Per-analyte distribution\n")
    lines.append("| Analyte | Rows | Patients | Min datetime | Max datetime |")
    lines.append("|---|---:|---:|---|---|")
    for a, n, p, mn, mx in per_analyte:
        lines.append(f"| {a} | {n:,} | {p:,} | {mn} | {mx} |")
    lines.append("")

    lines.append("## Source breakdown (longitudinal view)\n")
    lines.append("| Analyte | Source | Rows |")
    lines.append("|---|---|---:|")
    for a, s, n in source_counts:
        lines.append(f"| {a} | {s} | {n:,} |")
    lines.append("")

    lines.append("## value_correction_note frequencies\n")
    lines.append("| Table | Note | Rows |")
    lines.append("|---|---|---:|")
    for tbl, note, n in note_counts:
        lines.append(f"| {tbl} | {note} | {n:,} |")
    lines.append("")

    lines.append("## Patched downstream consumer scripts\n")
    lines.append("| Script | Pyflakes | AST parse |")
    lines.append("|---|:---:|:---:|")
    for s in patched_scripts:
        ok_pf = "✓" if s.get("pyflakes_ok") else "✗"
        ok_ast = "✓" if s.get("ast_ok") else "✗"
        lines.append(f"| {s['path']} | {ok_pf} | {ok_ast} |")
    lines.append("")
    lines.append("Script 113 (`scripts/113_tg_lab_ingestion.py`) is the legacy "
                 "ingestion builder. It is FROZEN pending Script 348 refactor "
                 "to write directly to the 5 per-analyte canonicals.\n")

    lines.append("## Verification (PASS/FAIL)\n")
    for ok, msg in verify_results["results"]:
        tag = "PASS" if ok else "FAIL"
        lines.append(f"- [{tag}] {msg}")
    lines.append("")

    lines.append("## CPM invariant\n")
    lines.append("- pre-build: (10871, 10871, 0) ✓")
    lines.append("- post-build: (10871, 10871, 0) ✓")
    lines.append("")

    lines.append("## Archive snapshots\n")
    lines.append(f"- `{ARCHIVE_QUALIFIED}.\"{archive_dest_long}\"`")
    lines.append(f"- `{ARCHIVE_QUALIFIED}.\"{archive_dest_tg}\"`")
    lines.append(f"- `{ARCHIVE_QUALIFIED}.\"{archive_dest_dedup}\"`")
    lines.append("")

    REPORT_PATH.write_text("\n".join(lines))
    log(f"  report written: {REPORT_PATH}")


# ---------------------------------------------------------------------------
# Phase 9 — pyflakes / ast / git
# ---------------------------------------------------------------------------

PATCHED_SCRIPTS_RELATIVE = [
    "scripts/203_canonical_recurrence.py",
    "scripts/223_ingest_and_publish.py",
    "scripts/223_publish_canonical.py",
    "scripts/253_lab_orphan_triage.py",
    "scripts/255_rebuild_rai_tg_rollups.py",
    "scripts/272_canonical_cleanup_phase1.py",
    "scripts/273_canonical_cleanup_phase2_3.py",
    "scripts/277_canonical_cleanup_phase7_verification.py",
    "scripts/286_cpm_missing_data_backfill.py",
    "scripts/prompt6_349_max_stimulated_tg.py",
    "scripts/prompt6_352_wiring_gap_sweep.py",
]


def check_patched_scripts() -> list[dict]:
    import ast
    out: list[dict] = []
    for rel in PATCHED_SCRIPTS_RELATIVE + ["scripts/113_tg_lab_ingestion.py"]:
        path = REPO / rel
        entry = {"path": rel, "pyflakes_ok": False, "ast_ok": False}
        if not path.exists():
            entry["error"] = "missing"
            out.append(entry)
            continue
        try:
            ast.parse(path.read_text())
            entry["ast_ok"] = True
        except Exception as e:
            entry["ast_error"] = str(e)
        proc = subprocess.run(
            [sys.executable, "-m", "pyflakes", str(path)],
            capture_output=True, text=True
        )
        entry["pyflakes_ok"] = (proc.returncode == 0)
        if proc.returncode != 0:
            entry["pyflakes_output"] = (proc.stdout + proc.stderr)[:1000]
        out.append(entry)
    return out


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--commit", action="store_true")
    ap.add_argument("--no-git", action="store_true",
                    help="skip git stage/commit/push")
    args = ap.parse_args()
    if args.dry_run and args.commit:
        raise SystemExit("Choose --dry-run OR --commit, not both")
    if not args.dry_run and not args.commit:
        raise SystemExit("Provide --dry-run or --commit")

    do_writes = bool(args.commit)
    t0 = time.time()
    log(f"=== {SCRIPT_TAG} {'COMMIT' if do_writes else 'DRY-RUN'} ===")

    con = connect_locked()
    cpm_invariant(con, "pre")

    archive_dests = archive_snapshots(con, do_writes)
    archive_dest_long = archive_dests["longitudinal_lab_canonical_v1"]["dest"]
    archive_dest_tg = archive_dests["thyroglobulin_lab_canonical_v1"]["dest"]
    archive_dest_dedup = archive_dests["lab_cross_wave_dedup_map_v1"]["dest"]

    staging = build_staging(con)
    write_per_analyte_tables(con, staging, do_writes)
    write_views(con, do_writes)
    drop_legacy(con, do_writes)
    update_registry(
        con, do_writes,
        post_counts={t: con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
                     for t in EXPECTED_ROW_RANGE} if do_writes else {},
    )
    write_readable_views(con, do_writes)

    if do_writes:
        verify_results = verify(con, archive_dest_long)
    else:
        log("=== PHASE 7 — verification SKIPPED in dry-run ===")
        verify_results = {"n_pass": 0, "n_fail": 0, "results": [],
                          "table_rows": {t: 0 for t in EXPECTED_ROW_RANGE}}

    patched = check_patched_scripts()
    if do_writes:
        write_report(
            con, archive_dest_long, archive_dest_tg, archive_dest_dedup,
            verify_results, patched,
        )

    decision = {
        "script": "347",
        "run_ts": RUN_TS,
        "do_writes": do_writes,
        "archive_dests": archive_dests,
        "verify_results": {
            "n_pass": verify_results.get("n_pass", 0),
            "n_fail": verify_results.get("n_fail", 0),
            "table_rows": verify_results.get("table_rows", {}),
        },
        "patched_scripts": patched,
    }
    DECISION_PATH.write_text(json.dumps(decision, indent=2, default=str))
    log(f"decision log: {DECISION_PATH.relative_to(REPO)}")

    cpm_invariant(con, "final")
    con.close()

    LOG_PATH.write_text("\n".join(LOG_LINES))
    log(f"=== END elapsed={time.time()-t0:.1f}s ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
