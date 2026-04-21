#!/usr/bin/env python3
"""
113_tg_lab_ingestion.py — Thyroglobulin & TgAb Lab Ingestion (per-analyte canonical).

Refactored by Script 348 (2026-04-21) to write directly to the per-analyte
canonical lab table introduced by Script 347. The legacy targets
``main.thyroglobulin_lab_canonical_v1`` /
``main.longitudinal_lab_canonical_v1`` /
``main.lab_cross_wave_dedup_map_v1`` are no longer in the publication
schema; this script writes ONLY to:

    main.canonical_labs_thyroglobulin_v1   (Tg + TgAb, analyte column)

All value normalization is delegated to ``scripts/_lab_value_normalizer.py``
(uniform 2A–2F pipeline + ``convert_to_canonical_unit``). Cross-wave dedup
is applied INLINE at write time (no separate dedup map).

Write strategy: FULL REBUILD.
    CREATE OR REPLACE TABLE main.canonical_labs_thyroglobulin_v1 AS <SELECT>
This is atomic, idempotent for repeated re-runs from the same source
data, and matches Script 347 semantics. Rationale: this script is the
sole owner of ``source = 'structured_ehr_tg'`` rows (100 % of the
current 53,006 rows in main.canonical_labs_thyroglobulin_v1 carry that
source). 127 owns the institutional-append slice; the two never overlap
on the dedup key.

Pipeline (CSV mode):
    A — Load & validate CSV
    B — PII stripping
    C — Exact-match dedup
    D — Test-name normalization (Tg / TgAb / COMBO)
    E — Combo-panel disambiguation
    F — (legacy result-string parsing — kept for QC visibility only;
          value_numeric / is_censored / value_correction_note are
          re-derived in Phase H from value_raw via normalize_lab_value)
    G — Temporal linkage
    H — Build canonical frame (per-analyte schema)
    I — Write main.canonical_labs_thyroglobulin_v1 (FULL REBUILD with
        inline cross-wave dedup)
    K — Validation (waterfall, patient coverage, Tg vs TgAb counts)
    L — Markdown ingestion report
    P — Machine-readable QC artifact (JSON)

Pipeline (--rebuild-from-archive mode):
    Reads the pre347 snapshot
    "Thyroid 2026 UPdated".archive_pub_v1_0.thyroglobulin_lab_canonical_v1_pre347_<UTC>
    (which already carries `analyte`, `assay_method`, `specimen_collect_dt`,
    `result_raw`, `is_in_canonical_cancer_cohort`), applies normalize_lab_value
    to the raw value, and writes via Phase I. Used by Script 348 for drift
    verification when the source CSV is not available.

CLI:
    python scripts/113_tg_lab_ingestion.py --input <csv_path> --md
    python scripts/113_tg_lab_ingestion.py --rebuild-from-archive --md
    python scripts/113_tg_lab_ingestion.py --input <csv_path> --md --dry-run
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
PROCESSED = ROOT / "processed"
DOCS = ROOT / "docs"

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from _lab_value_normalizer import (  # noqa: E402
    CANONICAL_UNIT,
    convert_to_canonical_unit,
    normalize_lab_value,
)

TIMESTAMP = datetime.now().strftime("%Y%m%d")
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
SCRIPT_NAME = "scripts/113_tg_lab_ingestion.py"
SEED = 42

PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
TARGET_TABLE = "main.canonical_labs_thyroglobulin_v1"
SOURCE_TAG = "structured_ehr_tg"

# Cross-wave dedup priority — matches Script 347 (and the inline dedup in 127).
DEDUP_RANK_CASE = """
    CASE source
        WHEN 'institutional_append' THEN 0
        WHEN 'structured_ehr_tg'    THEN 1
        WHEN 'postop_structured'    THEN 2
        WHEN 'clinical_note'        THEN 3
        ELSE 9
    END
"""

PII_COLUMNS = [
    "patient_first_nm",
    "patient_last_nm",
    "PrimaryMrn",
    "euh_mrn",
    "tec_mrn",
    "dob",
    "surgeon_first_nm",
]

KEEP_COLUMNS = [
    "research_id_number",
    "race",
    "gender",
    "surg_date",
    "age",
    "thyroid_procedure",
    "test_name",
    "specimen_collect_dt",
    "order_dt",
    "result",
]

TEST_NAME_MAP: dict[str, tuple[str, str]] = {
    "THYROGLOBULINLEVEL": ("Tg", "immunoassay"),
    "THYROGLOBULIN": ("Tg", "immunoassay"),
    "Thyroglobulin": ("Tg", "immunoassay"),
    "THYROGLOBULIN BY IMA": ("Tg", "IMA"),
    "COMPREHENSIVE THYROGLOBULIN": ("Tg", "comprehensive"),
    "Thyroglobulin by LC-MS/MS": ("Tg", "LC-MS/MS"),
    "THYROGLOBULIN BY LCMS": ("Tg", "LC-MS/MS"),
    "Thyroglobulin by Reflex LC-MS/MS or CIA": ("Tg", "LC-MS/MS"),
    "THYROGLOBULIN BY RIA": ("Tg", "RIA"),
    "Thyroglobulin, RIA": ("Tg", "RIA"),
    "THYROGLOBULINANTIBODY": ("TgAb", "immunoassay"),
    "Thyroglobulin Antibody": ("TgAb", "immunoassay"),
    "ANTITHYROGLOBULIN": ("TgAb", "immunoassay"),
    "ANTITHYROGLOBULINANTIBODY": ("TgAb", "immunoassay"),
    "Anti Thyroglobulin Antibody": ("TgAb", "immunoassay"),
    "ANTITHYROGLOBULINIGG": ("TgAb", "IgG"),
    "Thyroglobulin and Thyroglobulin Antibody": ("COMBO", "immunoassay"),
    "THYROID PEROXIDASE AND THYROGLOBULIN ANTIBODIES": ("TgAb", "combo_panel"),
    "THYROGLOBULIN ANTIBODY AND THYROGLOBULIN, IMA OR LC/MS-MS": ("TgAb", "reflex"),
}

# Map analyte label (Tg / TgAb) to the canonical lab_test_name key
# accepted by normalize_lab_value().
_ANALYTE_TO_CANONICAL_KEY = {
    "Tg":   "thyroglobulin",
    "TgAb": "anti_thyroglobulin",
}

TEMPORAL_WINDOWS = [
    (-999999, -1, "pre_surgery"),
    (0, 30, "perioperative"),
    (31, 180, "early_postop"),
    (181, 365, "surveillance_1y"),
    (366, 1825, "surveillance_5y"),
    (1826, 999999, "long_term"),
]


# ---------------------------------------------------------------------------
# Connection / utility
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n{'=' * 76}")
    print(f"  {title}")
    print(f"{'=' * 76}")


def connect_md_locked():
    """Connect to MotherDuck publication DB with the search path locked.

    Used for both --md writes and --rebuild-from-archive reads (which
    cross-database to "Thyroid 2026 UPdated".archive_pub_v1_0).
    """
    from _md_connect import connect_locked  # noqa: E402

    return connect_locked()


def connect_local():
    """Local DuckDB fallback (used by --duckdb mode)."""
    from utils.md_connect import connect_md_or_file  # noqa: E402

    return connect_md_or_file(DB_PATH, md=False, fail_closed=False)


def cpm_invariant(con, label: str) -> None:
    """Abort if canonical_patient_master is not (10871, 10871, 0)."""
    r = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) "
        f"FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()
    print(f"  CPM invariant ({label}): rows={r[0]} dist_rid={r[1]} null_fna={r[2]}")
    if (r[0], r[1], r[2]) != (10871, 10871, 0):
        raise SystemExit(
            f"CPM INVARIANT FAIL ({label}): expected (10871, 10871, 0); got {tuple(r)}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Phase A: Load & Validate
# ─────────────────────────────────────────────────────────────────────────────
def phase_a_load(input_path: str) -> pd.DataFrame:
    section("Phase A — Load & Validate")
    df = pd.read_csv(input_path, dtype=str)
    print(f"  Loaded {len(df):,} rows, {df.columns.tolist()}")

    null_rid = df["research_id_number"].isna().sum()
    if null_rid > 0:
        print(f"  WARNING: {null_rid} rows with null research_id_number — dropping")
        df = df.dropna(subset=["research_id_number"])

    df["research_id_number"] = pd.to_numeric(df["research_id_number"], errors="coerce")
    bad_ids = df["research_id_number"].isna().sum()
    if bad_ids > 0:
        print(f"  WARNING: {bad_ids} rows with non-integer research_id — dropping")
        df = df.dropna(subset=["research_id_number"])
    df["research_id_number"] = df["research_id_number"].astype(int)

    df["specimen_collect_dt_parsed"] = pd.to_datetime(
        df["specimen_collect_dt"], errors="coerce"
    )
    bad_dates = df["specimen_collect_dt_parsed"].isna().sum()
    print(f"  Date parse failures: {bad_dates} / {len(df)}")

    n_patients = df["research_id_number"].nunique()
    date_range = (
        df["specimen_collect_dt_parsed"].min(),
        df["specimen_collect_dt_parsed"].max(),
    )
    print(f"  Patients: {n_patients:,}")
    print(f"  Date range: {date_range[0]} — {date_range[1]}")
    print(f"  Test names: {df['test_name'].nunique()} distinct")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Phase B: PII Stripping
# ─────────────────────────────────────────────────────────────────────────────
def phase_b_strip_pii(df: pd.DataFrame) -> pd.DataFrame:
    section("Phase B — PII Stripping")
    present_pii = [c for c in PII_COLUMNS if c in df.columns]
    df = df.drop(columns=present_pii)
    print(f"  Dropped {len(present_pii)} PII columns: {present_pii}")

    remaining_pii = [c for c in PII_COLUMNS if c in df.columns]
    assert len(remaining_pii) == 0, f"PII leak: {remaining_pii}"

    df = df.rename(columns={"research_id_number": "research_id"})
    print("  Renamed research_id_number → research_id")
    print(f"  Remaining columns: {df.columns.tolist()}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Phase C: Deduplication
# ─────────────────────────────────────────────────────────────────────────────
def phase_c_dedup(df: pd.DataFrame) -> pd.DataFrame:
    section("Phase C — Deduplication")
    n_before = len(df)
    dedup_cols = ["research_id", "test_name", "specimen_collect_dt", "result"]
    df = df.drop_duplicates(subset=dedup_cols, keep="first")
    n_removed = n_before - len(df)
    print(f"  Removed {n_removed:,} exact duplicates on {dedup_cols}")
    print(f"  Rows after dedup: {len(df):,}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Phase D: Test Name Normalization
# ─────────────────────────────────────────────────────────────────────────────
def phase_d_normalize(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    section("Phase D — Test Name Normalization")
    review_rows = []

    df["test_name_raw"] = df["test_name"]
    df["analyte"] = None
    df["assay_method"] = None

    for test_name, (analyte, method) in TEST_NAME_MAP.items():
        mask = df["test_name"] == test_name
        df.loc[mask, "analyte"] = analyte
        df.loc[mask, "assay_method"] = method

    unmapped = df[df["analyte"].isna()]
    if len(unmapped) > 0:
        print(f"  WARNING: {len(unmapped)} rows with unmapped test_name:")
        for tn, cnt in unmapped["test_name"].value_counts().items():
            print(f"    '{tn}': {cnt}")
        review_unmapped = unmapped.copy()
        review_unmapped["review_reason"] = "unmapped_test_name"
        review_rows.append(review_unmapped)
        df = df[df["analyte"].notna()].copy()

    vc = df["analyte"].value_counts()
    print("  Analyte distribution:")
    for a, c in vc.items():
        print(f"    {a}: {c:,}")

    review_df = (
        pd.concat(review_rows, ignore_index=True) if review_rows else pd.DataFrame()
    )
    return df, review_df


# ─────────────────────────────────────────────────────────────────────────────
# Phase E: Combo Panel Disambiguation
# ─────────────────────────────────────────────────────────────────────────────
def phase_e_disambiguate_combos(
    df: pd.DataFrame, review_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    section("Phase E — Combo Panel Disambiguation")
    combo_mask = df["analyte"] == "COMBO"
    non_combo = df[~combo_mask].copy()
    non_combo["disambiguation_method"] = "direct_label"
    non_combo["disambiguation_confidence"] = 1.0

    combo = df[combo_mask].copy()
    print(f"  Combo rows to disambiguate: {len(combo):,}")

    if len(combo) == 0:
        stats = {"pairs_total": 0, "heuristic": 0, "crossref": 0, "ambiguous": 0}
        return non_combo, review_df, stats

    combo["_group_key"] = (
        combo["research_id"].astype(str) + "|" + combo["specimen_collect_dt"]
    )

    combo["analyte"] = None
    combo["disambiguation_method"] = None
    combo["disambiguation_confidence"] = np.nan

    group_keys = combo["_group_key"].unique()
    n_pairs = 0
    n_heuristic = 0
    n_crossref = 0
    n_ambiguous = 0
    ambiguous_indices = []

    tg_patient_values: dict[int, set[str]] = {}
    tgab_patient_values: dict[int, set[str]] = {}
    for _, row in non_combo.iterrows():
        rid = row["research_id"]
        res = str(row["result"]).strip() if pd.notna(row["result"]) else ""
        if row["analyte"] == "Tg":
            tg_patient_values.setdefault(rid, set()).add(res)
        elif row["analyte"] == "TgAb":
            tgab_patient_values.setdefault(rid, set()).add(res)

    for gk in group_keys:
        pair = combo[combo["_group_key"] == gk]
        if len(pair) != 2:
            ambiguous_indices.extend(pair.index.tolist())
            n_ambiguous += len(pair)
            continue

        n_pairs += 1
        idx_a, idx_b = pair.index[0], pair.index[1]
        res_a = str(pair.loc[idx_a, "result"]).strip()
        res_b = str(pair.loc[idx_b, "result"]).strip()

        assigned = _heuristic_disambiguate(res_a, res_b)
        if assigned is not None:
            tg_idx, tgab_idx = (
                (idx_a, idx_b) if assigned == "a_is_tg" else (idx_b, idx_a)
            )
            combo.loc[tg_idx, "analyte"] = "Tg"
            combo.loc[tgab_idx, "analyte"] = "TgAb"
            combo.loc[tg_idx, "disambiguation_method"] = "combo_heuristic"
            combo.loc[tgab_idx, "disambiguation_method"] = "combo_heuristic"
            combo.loc[tg_idx, "disambiguation_confidence"] = 0.99
            combo.loc[tgab_idx, "disambiguation_confidence"] = 0.99
            n_heuristic += 1
            continue

        rid = pair.loc[idx_a, "research_id"]
        crossref_result = _crossref_disambiguate(
            rid, res_a, res_b, tg_patient_values, tgab_patient_values
        )
        if crossref_result is not None:
            tg_idx, tgab_idx = (
                (idx_a, idx_b) if crossref_result == "a_is_tg" else (idx_b, idx_a)
            )
            combo.loc[tg_idx, "analyte"] = "Tg"
            combo.loc[tgab_idx, "analyte"] = "TgAb"
            combo.loc[tg_idx, "disambiguation_method"] = "combo_crossref"
            combo.loc[tgab_idx, "disambiguation_method"] = "combo_crossref"
            combo.loc[tg_idx, "disambiguation_confidence"] = 0.80
            combo.loc[tgab_idx, "disambiguation_confidence"] = 0.80
            n_crossref += 1
            continue

        ambiguous_indices.extend([idx_a, idx_b])
        n_ambiguous += 1

    if ambiguous_indices:
        ambig = combo.loc[ambiguous_indices].copy()
        ambig["review_reason"] = "combo_ambiguous"
        review_df = pd.concat([review_df, ambig], ignore_index=True)
        combo = combo.drop(index=ambiguous_indices)

    combo = combo[combo["analyte"].notna()].copy()
    combo = combo.drop(columns=["_group_key"], errors="ignore")

    result = pd.concat([non_combo, combo], ignore_index=True)
    stats = {
        "pairs_total": n_pairs,
        "heuristic": n_heuristic,
        "crossref": n_crossref,
        "ambiguous": n_ambiguous,
    }
    print(f"  Pairs total: {n_pairs:,}")
    print(f"  Heuristic-resolved: {n_heuristic:,}")
    print(f"  Cross-ref-resolved: {n_crossref:,}")
    print(f"  Ambiguous → review: {n_ambiguous:,}")
    print(f"  Rows after disambiguation: {len(result):,}")
    return result, review_df, stats


def _heuristic_disambiguate(res_a: str, res_b: str) -> str | None:
    """Detection-limit pattern matching. Returns 'a_is_tg', 'b_is_tg', or None."""
    tgab_sentinel = {"<0.9"}
    tg_sentinel = {"<0.1", "<0.2"}
    tgab_high_sentinel = {"<2", "<2.0", "<20"}

    if res_a in tgab_sentinel and res_b not in tgab_sentinel:
        return "b_is_tg"
    if res_b in tgab_sentinel and res_a not in tgab_sentinel:
        return "a_is_tg"

    if res_a in tg_sentinel and res_b not in tg_sentinel:
        return "a_is_tg"
    if res_b in tg_sentinel and res_a not in tg_sentinel:
        return "b_is_tg"

    if res_a in tgab_high_sentinel and res_b.startswith("<0."):
        return "b_is_tg"
    if res_b in tgab_high_sentinel and res_a.startswith("<0."):
        return "a_is_tg"

    return None


def _crossref_disambiguate(
    rid: int,
    res_a: str,
    res_b: str,
    tg_values: dict[int, set[str]],
    tgab_values: dict[int, set[str]],
) -> str | None:
    """Use same-patient labelled rows to infer combo-pair analyte by value pattern."""
    known_tg = tg_values.get(rid, set())
    known_tgab = tgab_values.get(rid, set())
    if not known_tg and not known_tgab:
        return None

    a_in_tg = res_a in known_tg
    a_in_tgab = res_a in known_tgab
    b_in_tg = res_b in known_tg
    b_in_tgab = res_b in known_tgab

    if a_in_tg and not a_in_tgab and b_in_tgab and not b_in_tg:
        return "a_is_tg"
    if b_in_tg and not b_in_tgab and a_in_tgab and not a_in_tg:
        return "b_is_tg"

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Phase F: Legacy result-string parsing (kept for QC visibility only).
# Phase H re-derives value_numeric / is_censored / value_correction_note via
# normalize_lab_value, so the canonical write does NOT depend on these.
# ─────────────────────────────────────────────────────────────────────────────
_TITER_RE = re.compile(r"^1:(\d+)$")
_NUMERIC_RE = re.compile(r"^[<>]?\s*(\d+\.?\d*)$")  # noqa: F841 — kept for QC parity


def phase_f_parse_results(df: pd.DataFrame) -> pd.DataFrame:
    section("Phase F — Result Parsing (QC visibility only)")
    df["result_raw"] = df["result"].astype(str).str.strip()
    df["result_qualifier"] = None
    df["result_flag"] = None

    for idx in df.index:
        raw = df.at[idx, "result_raw"]
        _classify_result_for_qc(df, idx, raw)

    flag_vc = df["result_flag"].value_counts(dropna=False)
    print("  Result flag distribution (QC):")
    for f, c in flag_vc.items():
        print(f"    {f}: {c:,}")
    return df


def _classify_result_for_qc(df: pd.DataFrame, idx: int, raw: str) -> None:
    upper = raw.upper()
    if upper == "FOOTNOTE":
        df.at[idx, "result_flag"] = "footnote"
        return
    if upper == "SEE SCANNED RESULT":
        df.at[idx, "result_flag"] = "see_scanned"
        return
    if upper == "NEGATIVE":
        df.at[idx, "result_flag"] = "negative"
        return

    titer = _TITER_RE.match(raw)
    if titer:
        df.at[idx, "result_qualifier"] = "="
        df.at[idx, "result_flag"] = "titer"
        return

    if raw.startswith("<"):
        df.at[idx, "result_qualifier"] = "<"
        df.at[idx, "result_flag"] = "below_detection"
        return

    if raw.startswith(">"):
        df.at[idx, "result_qualifier"] = ">"
        df.at[idx, "result_flag"] = "above_detection"
        return

    try:
        float(raw)
        df.at[idx, "result_qualifier"] = "="
        df.at[idx, "result_flag"] = "numeric"
        return
    except ValueError:
        pass

    df.at[idx, "result_flag"] = "non_numeric"


# ─────────────────────────────────────────────────────────────────────────────
# Phase G: Temporal Linkage (QC only — temporal_window is no longer in
# the per-analyte canonical schema).
# ─────────────────────────────────────────────────────────────────────────────
def phase_g_temporal_linkage(df: pd.DataFrame) -> pd.DataFrame:
    section("Phase G — Temporal Linkage (QC only)")
    df["surg_date_parsed"] = pd.to_datetime(df["surg_date"], errors="coerce")
    df["specimen_dt"] = df["specimen_collect_dt_parsed"]

    has_both = df["surg_date_parsed"].notna() & df["specimen_dt"].notna()
    df.loc[has_both, "days_from_surgery"] = (
        (df.loc[has_both, "specimen_dt"] - df.loc[has_both, "surg_date_parsed"])
        .dt.days
    )
    df["days_from_surgery"] = df["days_from_surgery"].astype("Int64")

    df["temporal_window"] = None
    for lo, hi, label in TEMPORAL_WINDOWS:
        mask = (
            has_both
            & (df["days_from_surgery"] >= lo)
            & (df["days_from_surgery"] <= hi)
        )
        df.loc[mask, "temporal_window"] = label

    print(f"  days_from_surgery computed: {has_both.sum():,}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Phase H: Build canonical frame matching canonical_labs_thyroglobulin_v1.
# Routes value normalization through scripts/_lab_value_normalizer.py.
# ─────────────────────────────────────────────────────────────────────────────
def phase_h_build_canonical(df: pd.DataFrame, cancer_cohort: set[int]) -> pd.DataFrame:
    section("Phase H — Build canonical frame (per-analyte schema)")

    n_normalized = 0
    n_unit_converted = 0
    discordances: list[dict] = []

    out_records: list[dict] = []
    now_utc = datetime.now(timezone.utc)
    for rec in df.itertuples(index=False):
        analyte = getattr(rec, "analyte")  # 'Tg' or 'TgAb'
        canon_key = _ANALYTE_TO_CANONICAL_KEY.get(analyte)
        if canon_key is None:
            continue  # Defensive — should never happen post-Phase E.

        value_raw = getattr(rec, "result_raw", None)
        if isinstance(value_raw, float) and value_raw != value_raw:  # NaN
            value_raw = None

        v_num, is_cens, note = normalize_lab_value(value_raw, canon_key)
        n_normalized += 1

        # Source CSV does NOT carry a unit column for Tg/TgAb; backfill the
        # canonical unit. Wired through convert_to_canonical_unit so any
        # future addition of a source unit column triggers the same
        # validation path Script 347 uses.
        try:
            v_num, unit_std, unit_note = convert_to_canonical_unit(
                v_num, None, canon_key
            )
        except ValueError as e:
            discordances.append({
                "research_id": int(getattr(rec, "research_id")),
                "analyte": analyte,
                "value_raw": value_raw,
                "error": str(e),
            })
            unit_std = CANONICAL_UNIT[canon_key]
            unit_note = "unit_unknown_aborted"
        if unit_note is not None:
            note = (note + "," + unit_note) if note else unit_note
            n_unit_converted += 1

        # lab_datetime: prefer specimen_collect_dt; fallback to midnight of
        # any available date. Skip rows with no usable timestamp (NOT NULL on
        # the canonical schema).
        sct = getattr(rec, "specimen_collect_dt_parsed", None)
        if isinstance(sct, pd.Timestamp) and not pd.isna(sct):
            lab_dt = sct.to_pydatetime()
        else:
            continue

        rid = int(getattr(rec, "research_id"))

        am = getattr(rec, "assay_method", None)
        if isinstance(am, float) and am != am:
            am = None

        out_records.append({
            "research_id": rid,
            "analyte": analyte,
            "assay_method": am,
            "lab_datetime": lab_dt,
            "value_raw": value_raw,
            "value_numeric": v_num,
            "is_censored": bool(is_cens),
            "value_correction_note": note,
            "unit_standardized": unit_std,
            "source": SOURCE_TAG,
            "is_in_canonical_cancer_cohort": rid in cancer_cohort,
            "ingestion_date": now_utc,
        })

    if discordances:
        _write_discordances(discordances)
        raise SystemExit(
            f"  ABORT: {len(discordances)} rows with unrecognised source units; "
            f"see studies/lab_ingestion_refactor_20260421/discordance_review.md"
        )

    out = pd.DataFrame.from_records(out_records)
    print(f"  Normalized {n_normalized:,} rows; {n_unit_converted} unit-noted")
    print(f"  Pre-dedup canonical rows: {len(out):,}")
    print(f"  Patients: {out['research_id'].nunique():,}")
    print(f"  Analytes: Tg={(out['analyte'] == 'Tg').sum():,} / "
          f"TgAb={(out['analyte'] == 'TgAb').sum():,}")
    return out


def _write_discordances(rows: list[dict]) -> None:
    out_dir = ROOT / "studies" / "lab_ingestion_refactor_20260421"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "discordance_review.md"
    with path.open("a") as f:
        f.write(f"\n## Script 113 unit discordances — {RUN_TS}\n\n")
        f.write("| research_id | analyte | value_raw | error |\n")
        f.write("|---|---|---|---|\n")
        for d in rows:
            f.write(
                f"| {d['research_id']} | {d['analyte']} | "
                f"{(d['value_raw'] or '')[:80]} | {d['error']} |\n"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Phase I: Write to main.canonical_labs_thyroglobulin_v1
# (FULL REBUILD with inline cross-wave dedup, single transaction).
# ─────────────────────────────────────────────────────────────────────────────

THY_INLINE_DEDUP_SQL = f"""
CREATE OR REPLACE TABLE {TARGET_TABLE} AS
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id,
                         analyte,
                         CAST(lab_datetime AS DATE),
                         COALESCE(CAST(value_numeric AS VARCHAR), value_raw)
            ORDER BY {DEDUP_RANK_CASE}, ingestion_date DESC
        ) AS rn
    FROM staging_113
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
    ingestion_date
FROM ranked
WHERE rn = 1
"""


def phase_i_write_canonical(
    canonical: pd.DataFrame,
    use_md: bool,
    dry_run: bool,
) -> dict:
    section("Phase I — Write main.canonical_labs_thyroglobulin_v1 (FULL REBUILD)")

    if dry_run and not use_md:
        print("  [DRY RUN, no DB] skipping")
        return {"pre_dedup": int(len(canonical)), "post_dedup": None}

    con = connect_md_locked() if use_md else connect_local()
    try:
        cpm_invariant(con, "pre")

        # Coerce types for safe register.
        df = canonical.copy()
        df["lab_datetime"] = pd.to_datetime(df["lab_datetime"])
        df["ingestion_date"] = pd.to_datetime(df["ingestion_date"], utc=True)\
            .dt.tz_localize(None)
        df["value_numeric"] = df["value_numeric"].astype("float64")
        df["research_id"] = df["research_id"].astype("int64")

        if dry_run:
            print(f"  [DRY RUN] would CREATE OR REPLACE TABLE {TARGET_TABLE} "
                  f"from {len(df):,} pre-dedup rows")
            con.register("staging_113", df)
            preview_n = con.execute(
                "SELECT COUNT(*) FROM (" + THY_INLINE_DEDUP_SQL.split(" AS\n", 1)[1]
                + ") t"
            ).fetchone()[0]
            con.unregister("staging_113")
            print(f"  [DRY RUN] post-dedup row count would be: {preview_n:,}")
            cpm_invariant(con, "post-dryrun")
            return {"pre_dedup": int(len(df)), "post_dedup": int(preview_n)}

        con.execute("BEGIN TRANSACTION")
        try:
            con.register("staging_113", df)
            con.execute(THY_INLINE_DEDUP_SQL)
            con.unregister("staging_113")
            n_post = con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]
            try:
                con.execute(
                    f"COMMENT ON TABLE {TARGET_TABLE} IS "
                    f"'Canonical per-analyte lab table for Tg + TgAb. "
                    f"Normalized via _lab_value_normalizer.py (uniform pipeline). "
                    f"Built by {SCRIPT_NAME} ({RUN_TS}). FULL REBUILD; cross-wave "
                    f"dedup applied inline.'"
                )
            except Exception as e:
                print(f"  (table comment failed, non-fatal: {e})")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        cpm_invariant(con, "post")
        print(f"  {TARGET_TABLE}: {n_post:,} rows after inline dedup")

        # Sanity: no row with source='other_structured'.
        n_other = con.execute(
            f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE source = 'other_structured'"
        ).fetchone()[0]
        if n_other:
            raise SystemExit(
                f"FAIL: {n_other} rows landed at source='other_structured' "
                "(expected 0)"
            )
        return {"pre_dedup": int(len(df)), "post_dedup": int(n_post)}
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────
# Phase K: Validation
# ─────────────────────────────────────────────────────────────────────────────
def phase_k_validate(
    raw_count: int,
    dedup_count: int,
    canonical: pd.DataFrame,
    review: pd.DataFrame,
    combo_stats: dict,
) -> dict:
    section("Phase K — Validation")
    v: dict = {
        "raw_rows": raw_count,
        "dedup_rows": dedup_count,
        "canonical_rows": int(len(canonical)),
        "review_rows": int(len(review)),
        "patients_total": int(canonical["research_id"].nunique()) if len(canonical) else 0,
    }
    print(f"  Row waterfall: {raw_count:,} → {dedup_count:,} → "
          f"{v['canonical_rows']:,} canonical + {v['review_rows']:,} review")

    if len(canonical):
        tg = canonical[canonical["analyte"] == "Tg"]
        tgab = canonical[canonical["analyte"] == "TgAb"]
        v["patients_tg"] = int(tg["research_id"].nunique())
        v["patients_tgab"] = int(tgab["research_id"].nunique())
        v["patients_both"] = int(
            len(set(tg["research_id"]) & set(tgab["research_id"]))
        )
        v["numeric_rate"] = float(canonical["value_numeric"].notna().mean() * 100)
        v["censored_rate"] = float(canonical["is_censored"].mean() * 100)
        v["unit_pct_ng_ml"] = float(
            (canonical.loc[canonical["analyte"] == "Tg", "unit_standardized"]
             == "ng/mL").mean() * 100
        ) if (canonical["analyte"] == "Tg").any() else 0.0
        v["unit_pct_iu_ml"] = float(
            (canonical.loc[canonical["analyte"] == "TgAb", "unit_standardized"]
             == "IU/mL").mean() * 100
        ) if (canonical["analyte"] == "TgAb").any() else 0.0
        print(f"  Tg: {v['patients_tg']:,} pts | TgAb: {v['patients_tgab']:,} pts "
              f"| both: {v['patients_both']:,}")
        print(f"  numeric_rate={v['numeric_rate']:.1f}%  "
              f"censored_rate={v['censored_rate']:.1f}%")
        print(f"  unit_standardized: Tg→ng/mL {v['unit_pct_ng_ml']:.1f}%  "
              f"TgAb→IU/mL {v['unit_pct_iu_ml']:.1f}%")

    v["combo_stats"] = combo_stats
    return v


# ─────────────────────────────────────────────────────────────────────────────
# Phase L: Documentation
# ─────────────────────────────────────────────────────────────────────────────
def phase_l_documentation(input_path: str, validation: dict) -> Path:
    section("Phase L — Documentation")
    DOCS.mkdir(exist_ok=True)
    report_path = DOCS / f"tg_lab_ingestion_report_{TIMESTAMP}.md"
    src = Path(input_path).name if input_path else "<rebuild_from_archive>"
    report = f"""# Thyroglobulin Lab Ingestion Report (per-analyte canonical)

**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M')}
**Script**: `{SCRIPT_NAME}`
**Source**: `{src}`
**Target**: `{TARGET_TABLE}`

## Row Waterfall

| Stage | Rows |
|-------|------|
| Raw input | {validation['raw_rows']:,} |
| After exact-match dedup | {validation['dedup_rows']:,} |
| Canonical (pre inline dedup) | {validation['canonical_rows']:,} |
| Review queue | {validation['review_rows']:,} |

## Patient Coverage

| Metric | Value |
|--------|-------|
| Total patients | {validation.get('patients_total', 0):,} |
| Patients with Tg | {validation.get('patients_tg', 0):,} |
| Patients with TgAb | {validation.get('patients_tgab', 0):,} |
| Patients with both | {validation.get('patients_both', 0):,} |

## Value Quality

| Metric | Value |
|--------|-------|
| Numeric parse rate | {validation.get('numeric_rate', 0):.1f}% |
| Censored rate | {validation.get('censored_rate', 0):.1f}% |
| Tg → ng/mL | {validation.get('unit_pct_ng_ml', 0):.1f}% |
| TgAb → IU/mL | {validation.get('unit_pct_iu_ml', 0):.1f}% |

## Combo Disambiguation

| Metric | Count |
|--------|-------|
| Pairs total | {validation['combo_stats'].get('pairs_total', 0):,} |
| Heuristic-resolved | {validation['combo_stats'].get('heuristic', 0):,} |
| Cross-ref-resolved | {validation['combo_stats'].get('crossref', 0):,} |
| Ambiguous → review | {validation['combo_stats'].get('ambiguous', 0):,} |

## Notes

All value normalisation was applied via `scripts/_lab_value_normalizer.py`
(uniform 2A–2F pipeline + canonical unit conversion). Cross-wave dedup
was applied INLINE at write time using PARTITION BY
`(research_id, analyte, lab_datetime::DATE, COALESCE(value_numeric, value_raw))`
with the Script 347 source priority ladder
`institutional_append > structured_ehr_tg > postop_structured > clinical_note`.

The legacy targets `thyroglobulin_lab_canonical_v1`,
`longitudinal_lab_canonical_v1`, and `lab_cross_wave_dedup_map_v1` were
removed by Script 347 and are NOT written by this script.
"""
    report_path.write_text(report, encoding="utf-8")
    print(f"  Wrote {report_path}")
    return report_path


# ─────────────────────────────────────────────────────────────────────────────
# Phase P: Machine-readable QC artifact
# ─────────────────────────────────────────────────────────────────────────────
def phase_p_qc_artifact(
    input_path: str,
    raw_count: int,
    dedup_count: int,
    canonical: pd.DataFrame,
    review: pd.DataFrame,
    combo_stats: dict,
    write_stats: dict,
    dry_run: bool,
) -> Path:
    section("Phase P — Machine-Readable QC Artifact")
    PROCESSED.mkdir(exist_ok=True)
    qc_path = PROCESSED / "tg_lab_ingestion_qc_v1.json"

    qc = {
        "schema_version": "2.0",
        "script": SCRIPT_NAME,
        "run_timestamp": RUN_TS,
        "source_file": Path(input_path).name if input_path else "<rebuild_from_archive>",
        "target_table": TARGET_TABLE,
        "row_waterfall": {
            "source_rows": raw_count,
            "after_dedup": dedup_count,
            "canonical_pre_dedup": int(len(canonical)),
            "review_queue_rows": int(len(review)),
            "post_inline_dedup": write_stats.get("post_dedup"),
        },
        "patients": {
            "unique_in_canonical":
                int(canonical["research_id"].nunique()) if len(canonical) else 0,
        },
        "combo_disambiguation": {
            "pairs_total": int(combo_stats.get("pairs_total", 0)),
            "heuristic_resolved": int(combo_stats.get("heuristic", 0)),
            "crossref_resolved": int(combo_stats.get("crossref", 0)),
            "ambiguous_to_review": int(combo_stats.get("ambiguous", 0)),
        },
        "promotion_gate": {
            "writes_to_dropped_legacy_tables": False,
            "value_normalizer_module": "scripts/_lab_value_normalizer.py",
            "inline_cross_wave_dedup": True,
            "source_priority": [
                "institutional_append", "structured_ehr_tg",
                "postop_structured", "clinical_note",
            ],
            "row_source_tag": SOURCE_TAG,
        },
    }
    if not dry_run:
        qc_path.write_text(json.dumps(qc, indent=2, default=str), encoding="utf-8")
        print(f"  Wrote {qc_path}")
    else:
        print(f"  [DRY RUN] Would write {qc_path}")
    return qc_path


# ─────────────────────────────────────────────────────────────────────────────
# --rebuild-from-archive: read the pre347 archive snapshot and re-derive
# ─────────────────────────────────────────────────────────────────────────────
def build_from_archive(use_md: bool, archive_table: str | None) -> tuple[
    pd.DataFrame, int, int, dict
]:
    section("Rebuild from pre347 archive")
    con = connect_md_locked() if use_md else connect_local()
    try:
        if archive_table is None:
            # Pick the most recent pre347 archive deterministically.
            row = con.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_catalog = ?
                  AND table_schema  = ?
                  AND table_name LIKE 'thyroglobulin_lab_canonical_v1_pre347_%'
                ORDER BY table_name DESC LIMIT 1
                """,
                [ARCHIVE_DB, ARCHIVE_SCHEMA],
            ).fetchone()
            if row is None:
                raise SystemExit(
                    "No pre347 thyroglobulin archive found in "
                    f"{ARCHIVE_QUALIFIED}; pass --archive-table explicitly."
                )
            archive_table = row[0]
        full = f'{ARCHIVE_QUALIFIED}."{archive_table}"'
        print(f"  source: {full}")
        df = con.execute(f"""
            SELECT
                research_id,
                analyte,
                assay_method,
                specimen_collect_dt   AS specimen_collect_dt_parsed,
                result_raw,
                is_in_canonical_cancer_cohort
            FROM {full}
        """).fetch_df()
    finally:
        con.close()

    raw_count = int(len(df))
    dedup_count = raw_count  # archive is already de-duplicated by ingest grain
    cancer_cohort = set(
        int(r) for r, flag in zip(df["research_id"], df["is_in_canonical_cancer_cohort"])
        if bool(flag)
    )

    # Phase H operates on the same column names; ensure they exist.
    canonical = phase_h_build_canonical(df, cancer_cohort)
    return canonical, raw_count, dedup_count, {"pairs_total": 0, "heuristic": 0,
                                              "crossref": 0, "ambiguous": 0}


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", help="Path to source CSV (CSV mode).")
    parser.add_argument(
        "--rebuild-from-archive",
        action="store_true",
        help=("Read from the pre347 archive snapshot of "
              "thyroglobulin_lab_canonical_v1 instead of CSV."),
    )
    parser.add_argument(
        "--archive-table",
        default=None,
        help=("Optional explicit archive_pub_v1_0 table name "
              "(default: most recent pre347 snapshot)."),
    )
    parser.add_argument("--duckdb", action="store_true", help="Use local DuckDB.")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck publication DB.")
    parser.add_argument("--dry-run", action="store_true", help="Validate; no writes.")
    args = parser.parse_args()

    if not (args.input or args.rebuild_from_archive):
        raise SystemExit("Pass --input <csv> OR --rebuild-from-archive")
    if args.input and args.rebuild_from_archive:
        raise SystemExit("Pass exactly one of --input / --rebuild-from-archive")
    if not (args.duckdb or args.md):
        # Default to MotherDuck for parity with Script 347/348.
        args.md = True

    print(f"{'=' * 76}")
    print("  Thyroglobulin Lab Ingestion (per-analyte canonical)")
    print(f"  Mode: {'CSV' if args.input else 'rebuild-from-archive'}  "
          f"DB: {'MD' if args.md else 'local'}  dry_run: {args.dry_run}")
    print(f"{'=' * 76}")

    review_df = pd.DataFrame()
    if args.rebuild_from_archive:
        canonical, raw_count, dedup_count, combo_stats = build_from_archive(
            args.md, args.archive_table
        )
    else:
        if not Path(args.input).exists():
            raise SystemExit(f"ERROR: --input not found: {args.input}")
        df = phase_a_load(args.input)
        raw_count = len(df)
        df = phase_b_strip_pii(df)
        df = phase_c_dedup(df)
        dedup_count = len(df)
        df, review_df = phase_d_normalize(df)
        df, review_df, combo_stats = phase_e_disambiguate_combos(df, review_df)
        df = phase_f_parse_results(df)
        df = phase_g_temporal_linkage(df)

        cancer_cohort = _load_canonical_cancer_cohort(args.md)
        canonical = phase_h_build_canonical(df, cancer_cohort)

    write_stats = phase_i_write_canonical(canonical, args.md, args.dry_run)
    validation = phase_k_validate(
        raw_count, dedup_count, canonical, review_df, combo_stats
    )
    if not args.dry_run:
        phase_l_documentation(args.input, validation)
        phase_p_qc_artifact(
            args.input, raw_count, dedup_count, canonical, review_df,
            combo_stats, write_stats, dry_run=False,
        )
    section("COMPLETE")
    print(f"  Pre-dedup rows: {len(canonical):,}")
    if write_stats.get("post_dedup") is not None:
        print(f"  Post-dedup rows in {TARGET_TABLE}: {write_stats['post_dedup']:,}")


def _load_canonical_cancer_cohort(use_md: bool) -> set[int]:
    """Return the set of research_ids whose lab rows carry the canonical cancer
    cohort flag.

    The flag was set upstream of the lab layer. We look it up per-rid from the
    most recent pre347 archive of ``thyroglobulin_lab_canonical_v1`` (which
    captured the flag at canonicalization time), falling back to the live
    ``main.canonical_labs_thyroglobulin_v1`` if the archive is unavailable.
    This keeps the refactored 113 self-sufficient and reproducible.
    """
    con = connect_md_locked() if use_md else connect_local()
    try:
        try:
            row = con.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_catalog = ? AND table_schema = ?
                  AND table_name LIKE 'thyroglobulin_lab_canonical_v1_pre347_%'
                ORDER BY table_name DESC LIMIT 1
                """,
                [ARCHIVE_DB, ARCHIVE_SCHEMA],
            ).fetchone()
        except Exception:
            row = None
        if row is not None:
            full = f'{ARCHIVE_QUALIFIED}."{row[0]}"'
            rs = con.execute(
                f"SELECT DISTINCT research_id FROM {full} "
                "WHERE is_in_canonical_cancer_cohort = TRUE"
            ).fetchall()
        else:
            rs = con.execute(
                f"SELECT DISTINCT research_id FROM {TARGET_TABLE} "
                "WHERE is_in_canonical_cancer_cohort = TRUE"
            ).fetchall()
    finally:
        con.close()
    return {int(r[0]) for r in rs}


if __name__ == "__main__":
    main()
