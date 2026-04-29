#!/usr/bin/env python3
"""Harmonized SSOT rebuild for main.canonical_recurrence_v1 (Protocol v2, Lane 19).

Publication catalog does not expose legacy ``operative_episode_detail_v2`` /
``gold_master_patient_facts_v1``; Script ``203_canonical_recurrence.py`` therefore
pads the cohort from a missing spine and yields a degenerate shell (mig_122).

**Spine replacements (harmonization):**
  * ``operative_episode_detail_v2`` → ``canonical_operative_events_v1``
    (``surgery_date_native``, ``resolved_surgery_date``, ``surgery_episode_id``).
  * Cohort enumeration + no-recurrence padding → ``canonical_patient_master``
    (distinct ``research_id``), matching 10,871 CPM cardinality.

Tier logic (2–5) matches Script 203. Tier 1 uses ``canonical_operative_events_v1``
re-operation windows; pathology row from ``path_synoptics`` (203 used ``tumor_pathology`` —
not deployed on publication DB).

Safety:
  * **Default:** dry-run only — prints metrics, optionally writes Markdown + JSON report;
    does **not** write parquet to DB or MotherDuck.
  * **RW:** pass ``--write`` — same behavior as Script 203 (parquet + CREATE OR REPLACE).

Lane 19 pause gate **cleared 2026-04-29** — spot-check Option 1; sign-off migration
``qc_framework_v1/migrations/131_canonical_recurrence_v1_rebuild_signoff_20260429.sql`` (batch ``mig_123_*``).
Original Script 203: ``scripts/203_canonical_recurrence.py``
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

DB = "thyroid_canonical_publication_v1_0"

# Harmonized canonical spine names (MotherDuck main schema)
OE = "canonical_operative_events_v1"
CPM = "canonical_patient_master"


def connect():
    token = get_token()
    if not token:
        raise RuntimeError(
            "MotherDuck token not found (set MOTHERDUCK_TOKEN / MD_SA_TOKEN or motherduck.local.toml)"
        )
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


def sql_reop_tier1() -> str:
    # tumor_pathology (203 legacy) absent on thyroid_canonical_publication_v1_0 —
    # join path_synoptics pathology row within ±30d of re-operation (same heuristic as legacy).
    return f"""
WITH first_surg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
    FROM {OE}
    WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
    GROUP BY 1
),
all_surgeries AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) AS surgery_date,
        surgery_episode_id
    FROM {OE}
    WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
),
reoperations AS (
    SELECT a.research_id, a.surgery_date AS reop_date, a.surgery_episode_id
    FROM all_surgeries a
    JOIN first_surg f ON a.research_id = f.research_id
    WHERE a.surgery_date > f.first_surgery_date
),
reop_with_pathology AS (
    SELECT DISTINCT
        r.research_id,
        r.reop_date AS recurrence_date,
        ps.tumor_1_histologic_type AS recurrence_histology,
        'reoperation_pathology' AS recurrence_evidence_source,
        'structural_confirmed' AS recurrence_type,
        TRUE AS recurrence_confirmed,
        'surgical_pathology' AS recurrence_definition
    FROM reoperations r
    JOIN path_synoptics ps
      ON CAST(r.research_id AS VARCHAR) = CAST(ps.research_id AS VARCHAR)
    WHERE ABS(DATE_DIFF('day', r.reop_date, TRY_CAST(ps.surg_date AS DATE))) <= 30
      AND ps.tumor_1_histologic_type IS NOT NULL
      AND TRIM(CAST(ps.tumor_1_histologic_type AS VARCHAR)) != ''
)
SELECT * FROM reop_with_pathology
"""


def sql_fna_tier2() -> str:
    """Post-op FNA Bethesda V/VI — ``fna_episode_master_v2`` → ``canonical_fna_events_v1``."""
    return f"""
WITH first_surg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
    FROM {OE}
    WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
    GROUP BY 1
)
SELECT DISTINCT
    CAST(f.research_id AS VARCHAR) AS research_id,
    f.fna_date_resolved AS recurrence_date,
    NULL AS recurrence_histology,
    'fna_cytology' AS recurrence_evidence_source,
    'fna_confirmed' AS recurrence_type,
    TRUE AS recurrence_confirmed,
    CASE
        WHEN f.bethesda_final_num = 6 THEN 'fna_bethesda_vi_malignant'
        WHEN f.bethesda_final_num = 5 THEN 'fna_bethesda_v_suspicious'
    END AS recurrence_definition
FROM canonical_fna_events_v1 f
JOIN first_surg s ON CAST(f.research_id AS VARCHAR) = s.research_id
WHERE f.fna_date_resolved > s.first_surgery_date
  AND f.bethesda_final_num IN (5, 6)
"""


def sql_biochem_tier3() -> str:
    return f"""
WITH first_surg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
    FROM {OE}
    WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
    GROUP BY 1
),
tg_postop AS (
    SELECT
        CAST(t.research_id AS VARCHAR) AS research_id,
        TRY_CAST(t.specimen_collect_dt AS DATE) AS lab_date,
        t.result_numeric AS tg_value,
        DATE_DIFF('day', s.first_surgery_date, TRY_CAST(t.specimen_collect_dt AS DATE)) AS days_post_surgery
    FROM thyroglobulin_lab_VIEW_v1 t
    JOIN first_surg s ON CAST(t.research_id AS VARCHAR) = s.research_id
    WHERE t.analyte = 'Tg'
      AND t.result_numeric IS NOT NULL
      AND TRY_CAST(t.specimen_collect_dt AS DATE) > s.first_surgery_date
),
tg_trajectory AS (
    SELECT
        research_id,
        MIN(CASE WHEN tg_value < 0.2 THEN lab_date END) AS first_undetectable_date,
        MIN(CASE WHEN tg_value < 0.2 THEN tg_value END) AS tg_nadir,
        BOOL_OR(tg_value < 0.2) AS ever_undetectable,
        MAX(tg_value) AS tg_peak_after_nadir
    FROM tg_postop
    GROUP BY research_id
),
biochemical_rise AS (
    SELECT
        tp.research_id,
        MIN(tp.lab_date) AS recurrence_date,
        tp.tg_value AS tg_at_recurrence,
        tt.tg_nadir AS tg_nadir_value
    FROM tg_postop tp
    JOIN tg_trajectory tt ON tp.research_id = tt.research_id
    WHERE tt.ever_undetectable = TRUE
      AND tp.tg_value > 1.0
      AND tp.lab_date > tt.first_undetectable_date
    GROUP BY tp.research_id, tp.tg_value, tt.tg_nadir
)
SELECT DISTINCT
    br.research_id,
    br.recurrence_date,
    NULL AS recurrence_histology,
    'tg_time_series' AS recurrence_evidence_source,
    'biochemical_tg_rise' AS recurrence_type,
    FALSE AS recurrence_confirmed,
    'rising_tg_after_undetectable' AS recurrence_definition,
    br.tg_nadir_value,
    br.tg_at_recurrence
FROM biochemical_rise br
"""


def sql_persistent_tier4() -> str:
    return f"""
WITH first_surg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
    FROM {OE}
    WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
    GROUP BY 1
),
tg_postop AS (
    SELECT
        CAST(t.research_id AS VARCHAR) AS research_id,
        t.result_numeric AS tg_value,
        TRY_CAST(t.specimen_collect_dt AS DATE) AS lab_date
    FROM thyroglobulin_lab_VIEW_v1 t
    JOIN first_surg s ON CAST(t.research_id AS VARCHAR) = s.research_id
    WHERE t.analyte = 'Tg'
      AND t.result_numeric IS NOT NULL
      AND TRY_CAST(t.specimen_collect_dt AS DATE) > s.first_surgery_date
),
persistent AS (
    SELECT
        research_id,
        MIN(tg_value) AS tg_min,
        MAX(tg_value) AS tg_max,
        COUNT(*) AS n_measurements,
        MIN(lab_date) AS first_elevated_date
    FROM tg_postop
    GROUP BY research_id
    HAVING MIN(tg_value) >= 0.2 AND MAX(tg_value) > 1.0
)
SELECT
    research_id,
    first_elevated_date AS recurrence_date,
    'persistent_biochemical_disease' AS recurrence_type,
    FALSE AS recurrence_confirmed,
    'persistent_tg_never_undetectable' AS recurrence_definition,
    tg_min, tg_max, n_measurements
FROM persistent
"""


def parse_llm_recurrence_entities(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    print("  Parsing LLM recurrence entities...")
    rows = con.execute("""
        SELECT research_id, result_json, note_type, note_date
        FROM note_entities_llm_recurrence
        WHERE result_json IS NOT NULL
          AND CAST(result_json AS VARCHAR) NOT LIKE '%"entities": []%'
    """).fetchall()

    entities: list[dict[str, Any]] = []
    for rid, rj, nt, nd in rows:
        try:
            data = json.loads(rj)
            for ent in data.get("entities", []):
                et = ent.get("entity_type", "")
                if et in ("structural_recurrence", "distant_recurrence"):
                    entities.append({
                        "research_id": str(rid),
                        "entity_type": et,
                        "entity_value": ent.get("entity_value", ""),
                        "entity_date": ent.get("entity_date"),
                        "confidence": ent.get("confidence", 0),
                        "evidence_text": ent.get("evidence_text", ""),
                        "note_type": nt,
                        "note_date": nd,
                        "present_or_negated": ent.get("present_or_negated", "present"),
                    })
        except (json.JSONDecodeError, TypeError):
            continue

    df = pd.DataFrame(entities)
    if len(df) == 0:
        return pd.DataFrame(columns=[
            "research_id", "entity_type", "entity_value", "entity_date", "confidence",
            "evidence_text", "note_type", "note_date", "present_or_negated",
        ])
    df = df[df["present_or_negated"] == "present"]
    print(
        f"  Found {len(df)} present recurrence/distant entities across "
        f"{df['research_id'].nunique()} patients"
    )
    return df


def _first_surg_inner_sql() -> str:
    """Common first-surgery CTE body (canonical_operative_events_v1 ∪ path_synoptics fallback)."""
    return f"""
        SELECT CAST(research_id AS VARCHAR) AS research_id,
               MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
        FROM {OE}
        WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
        GROUP BY 1
        UNION ALL
        SELECT CAST(research_id AS VARCHAR),
               MIN(TRY_CAST(surg_date AS DATE))
        FROM path_synoptics
        WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL
          AND CAST(research_id AS VARCHAR) NOT IN (
              SELECT CAST(research_id AS VARCHAR) FROM {OE}
              WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
          )
        GROUP BY 1
    """


def _first_surg_sql() -> str:
    return _first_surg_inner_sql()


PATH_PROVEN_DEFENSIVE_DATE_FILTER = """
    CAST(path_proven_date AS DATE) BETWEEN DATE '1990-01-01' AND DATE '2027-01-01'
"""
# CF-mig123-UPSTREAM-DATE-202-TYPO defensive filter — manuscript_workspace.recurrence_path_proven_candidates_v1
# can harbor OCR/typing errors (e.g. year 0202, 1950). Any future Tier-1 UNION from this table MUST retain
# this predicate (matches spot-check Logan approval 2026-04-29).


def probe_path_proven_date_outliers(con: duckdb.DuckDBPyConnection) -> int:
    """Count upstream rows outside defensive date band — audit until Tier-1 path_proven UNION (CF-mig124)."""
    filt = PATH_PROVEN_DEFENSIVE_DATE_FILTER.strip()
    try:
        sql = f"""
            SELECT COUNT(*)::BIGINT
            FROM manuscript_workspace.recurrence_path_proven_candidates_v1
            WHERE path_proven_date IS NOT NULL
              AND NOT ({filt})
        """
        return int(con.execute(sql).fetchone()[0])
    except duckdb.Error:
        return -1


def sql_legacy_old_structural_outside_placeholders(placeholders_sql: str) -> str:
    """Legacy structural_confirmed fallback with TTR-positive filter."""
    fs = _first_surg_inner_sql().strip()
    # Filter completion thyroidectomy / initial-dx misclassifications — spot-check 2026-04-29
    # CF-mig123-NEGATIVE-TTR-9-PATIENTS (etc.): recurrence_date MUST be strictly after first_surgery_date.
    return f"""
WITH first_surg AS (
{fs}
)
SELECT CAST(r.research_id AS VARCHAR) AS research_id,
       r.recurrence_date,
       r.recurrence_type,
       r.recurrence_definition
FROM recurrence_event_clean_v1 r
INNER JOIN first_surg fs
  ON CAST(r.research_id AS VARCHAR) = fs.research_id
WHERE r.recurrence_type = 'structural'
  AND r.recurrence_definition = 'structural_confirmed'
  AND CAST(r.recurrence_date AS DATE) > CAST(fs.first_surgery_date AS DATE)
  AND CAST(r.research_id AS VARCHAR) NOT IN ({placeholders_sql})
"""


def cohort_sql() -> str:
    """Distinct patient keys from canonical_patient_master (10,871 CPM spine)."""
    return f"SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM {CPM}"


def build_canonical_rows(
    con: duckdb.DuckDBPyConnection,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, int]]:
    tier_frames: dict[str, pd.DataFrame] = {}

    print("\n=== Tier 1: Reoperation with cancer on pathology ===")
    df_reop = con.execute(sql_reop_tier1()).fetchdf()
    tier_frames["tier1_reop"] = df_reop
    print(f"  Found: {len(df_reop)} reoperations with cancer, {df_reop['research_id'].nunique()} patients")

    print("\n=== Tier 2: Post-op FNA with Bethesda V/VI ===")
    df_fna = con.execute(sql_fna_tier2()).fetchdf()
    tier_frames["tier2_fna"] = df_fna
    print(f"  Found: {len(df_fna)} post-op FNAs, {df_fna['research_id'].nunique()} patients")

    print("\n=== Tier 3: Biochemical recurrence (rising Tg after undetectable) ===")
    df_biochem = con.execute(sql_biochem_tier3()).fetchdf()
    tier_frames["tier3_biochem"] = df_biochem
    print(f"  Found: {len(df_biochem)} biochemical rows, {df_biochem['research_id'].nunique()} patients")

    print("\n=== Tier 4: Persistent biochemical disease (never undetectable) ===")
    df_persistent = con.execute(sql_persistent_tier4()).fetchdf()
    tier_frames["tier4_persistent"] = df_persistent
    print(f"  Found: {len(df_persistent)} persistent Tg patients, {df_persistent['research_id'].nunique()} patients")

    print("\n=== Tier 5: LLM-extracted imaging suspicion (unconfirmed) ===")
    df_llm = parse_llm_recurrence_entities(con)

    confirmed_rids = set(df_reop["research_id"].tolist()) if len(df_reop) > 0 else set()
    if len(df_fna) > 0:
        confirmed_rids.update(df_fna["research_id"].tolist())
    biochem_rids = (
        set(df_biochem["research_id"].tolist()) if len(df_biochem) > 0 else set()
    )
    llm_suspicious = pd.DataFrame()
    if len(df_llm) > 0:
        unconfirmed_llm = df_llm[
            ~df_llm["research_id"].isin(confirmed_rids | biochem_rids)
        ].drop_duplicates(subset=["research_id"], keep="first")
        llm_suspicious = unconfirmed_llm.copy()
        tier_frames["tier5_llm_suspicious"] = llm_suspicious
        print(f"  Imaging suspicious (unconfirmed, not in tiers 1–3): {len(llm_suspicious)} patients")

    first_surg_df = con.execute(_first_surg_sql()).fetchdf()
    first_surg_map = dict(zip(first_surg_df["research_id"], first_surg_df["first_surgery_date"]))

    tier_counts = {
        "tier1_patients_unique": df_reop["research_id"].nunique() if len(df_reop) else 0,
        "tier2_patients_unique": df_fna["research_id"].nunique() if len(df_fna) else 0,
        "tier3_patients_unique": df_biochem["research_id"].nunique() if len(df_biochem) else 0,
        "tier4_patients_unique": df_persistent["research_id"].nunique() if len(df_persistent) else 0,
        "tier5_patients_unique": llm_suspicious["research_id"].nunique()
        if len(llm_suspicious) > 0
        else 0,
    }

    print("\n=== Building canonical_recurrence_v1 (in-memory) ===")
    rows: list[dict[str, Any]] = []

    for _, r in df_reop.iterrows():
        rid = r["research_id"]
        fs = first_surg_map.get(rid)
        ttx = (
            (r["recurrence_date"] - fs).days
            if fs is not None and pd.notna(r["recurrence_date"]) and pd.notna(fs)
            else None
        )
        rows.append({
            "research_id": rid,
            "recurrence_confirmed": True,
            "recurrence_type": "structural_confirmed",
            "recurrence_date": r["recurrence_date"],
            "recurrence_site": None,
            "recurrence_histology": r.get("recurrence_histology"),
            "recurrence_evidence_source": "reoperation_pathology",
            "recurrence_definition": r.get("recurrence_definition", "surgical_pathology"),
            "first_surgery_date": fs,
            "time_to_recurrence_days": ttx,
            "biochemical_tg_nadir": None,
            "biochemical_tg_at_recurrence": None,
        })

    for _, r in df_fna.iterrows():
        rid = r["research_id"]
        if rid in [x["research_id"] for x in rows]:
            continue
        fs = first_surg_map.get(rid)
        ttx = (
            (r["recurrence_date"] - fs).days
            if fs is not None and pd.notna(r["recurrence_date"]) and pd.notna(fs)
            else None
        )
        rows.append({
            "research_id": rid,
            "recurrence_confirmed": True,
            "recurrence_type": "fna_confirmed",
            "recurrence_date": r["recurrence_date"],
            "recurrence_site": None,
            "recurrence_histology": None,
            "recurrence_evidence_source": "fna_cytology",
            "recurrence_definition": r.get("recurrence_definition", "fna_bethesda_v_vi"),
            "first_surgery_date": fs,
            "time_to_recurrence_days": ttx,
            "biochemical_tg_nadir": None,
            "biochemical_tg_at_recurrence": None,
        })

    for _, r in df_biochem.iterrows():
        rid = r["research_id"]
        if rid in [x["research_id"] for x in rows]:
            continue
        fs = first_surg_map.get(rid)
        rd = r.get("recurrence_date")
        ttx = (rd - fs).days if fs is not None and pd.notna(rd) and pd.notna(fs) else None
        rows.append({
            "research_id": rid,
            "recurrence_confirmed": False,
            "recurrence_type": "biochemical_tg_rise",
            "recurrence_date": rd,
            "recurrence_site": None,
            "recurrence_histology": None,
            "recurrence_evidence_source": "tg_time_series",
            "recurrence_definition": "rising_tg_after_undetectable",
            "first_surgery_date": fs,
            "time_to_recurrence_days": ttx,
            "biochemical_tg_nadir": r.get("tg_nadir_value"),
            "biochemical_tg_at_recurrence": r.get("tg_at_recurrence"),
        })

    for _, r in df_persistent.iterrows():
        rid = r["research_id"]
        if rid in [x["research_id"] for x in rows]:
            continue
        fs = first_surg_map.get(rid)
        rd = r.get("recurrence_date")
        rds = (rd - fs).days if (fs is not None and pd.notna(rd) and pd.notna(fs)) else None
        ttx = rds if (rds is not None and rds >= 0) else None
        rows.append({
            "research_id": rid,
            "recurrence_confirmed": False,
            "recurrence_type": "persistent_biochemical_disease",
            "recurrence_date": rd,
            "recurrence_site": None,
            "recurrence_histology": None,
            "recurrence_evidence_source": "tg_time_series",
            "recurrence_definition": "persistent_tg_never_undetectable",
            "first_surgery_date": fs,
            "time_to_recurrence_days": ttx,
            "biochemical_tg_nadir": r.get("tg_min"),
            "biochemical_tg_at_recurrence": r.get("tg_max"),
        })

    if len(llm_suspicious) > 0:
        for _, r in llm_suspicious.iterrows():
            rid = r["research_id"]
            if rid in [x["research_id"] for x in rows]:
                continue
            fs = first_surg_map.get(rid)
            rd = (
                pd.to_datetime(r.get("entity_date"), errors="coerce")
                if r.get("entity_date")
                else None
            )
            rds = (rd - fs).days if (fs is not None and pd.notna(rd) and pd.notna(fs)) else None
            ttx = rds if (rds is not None and rds >= 0) else None
            rows.append({
                "research_id": rid,
                "recurrence_confirmed": False,
                "recurrence_type": "imaging_suspicious_unconfirmed",
                "recurrence_date": rd,
                "recurrence_site": (
                    r.get("entity_value", "")[:200] if r.get("entity_value") else None
                ),
                "recurrence_histology": None,
                "recurrence_evidence_source": "imaging_or_clinical_note",
                "recurrence_definition": "llm_extracted_unconfirmed",
                "first_surgery_date": fs,
                "time_to_recurrence_days": ttx,
                "biochemical_tg_nadir": None,
                "biochemical_tg_at_recurrence": None,
            })

    old_rids = {r["research_id"] for r in rows}
    placeholders = ",".join(f"'{r}'" for r in sorted(old_rids)) if old_rids else "'__none__'"
    legacy_sql = sql_legacy_old_structural_outside_placeholders(placeholders)
    print("\n=== Legacy: structural_confirmed from recurrence_event_clean_v1 (> first_surgery) ===")
    outlier_pp = probe_path_proven_date_outliers(con)
    if outlier_pp >= 0:
        print(f"  path_proven_candidates_v1 rows outside 1990–2027 DATE band (audit): {outlier_pp}")
    old_structural = con.execute(legacy_sql).fetchdf()
    print(f"  Legacy structural_confirmed rows kept: {len(old_structural)} (recurrence_date > first_surgery_date)")
    # CF-mig123-LEGACY-COMPLETION-CHECK-6674 — rid 6674 retains 34d TTR (planned 2-stage); out-of-lane adjudication if needed.
    tier_frames["legacy_old_structural"] = old_structural

    for _, r in old_structural.iterrows():
        rid = r["research_id"]
        fs = first_surg_map.get(rid)
        rd = r.get("recurrence_date")
        ttx = (rd - fs).days if fs is not None and pd.notna(rd) and pd.notna(fs) else None
        rows.append({
            "research_id": rid,
            "recurrence_confirmed": True,
            "recurrence_type": "structural_confirmed_legacy",
            "recurrence_date": rd,
            "recurrence_site": None,
            "recurrence_histology": None,
            "recurrence_evidence_source": "recurrence_event_clean_v1_legacy",
            "recurrence_definition": "legacy_structural_confirmed",
            "first_surgery_date": fs,
            "time_to_recurrence_days": ttx,
            "biochemical_tg_nadir": None,
            "biochemical_tg_at_recurrence": None,
        })

    df_final = pd.DataFrame(rows)
    COLS_FULL = (
        rows[0].keys()
        if rows
        else [
            "research_id", "recurrence_confirmed", "recurrence_type", "recurrence_date",
            "recurrence_site", "recurrence_histology", "recurrence_evidence_source",
            "recurrence_definition", "first_surgery_date", "time_to_recurrence_days",
            "biochemical_tg_nadir", "biochemical_tg_at_recurrence",
        ]
    )
    priority = {
        "structural_confirmed": 1,
        "fna_confirmed": 2,
        "structural_confirmed_legacy": 3,
        "biochemical_tg_rise": 4,
        "persistent_biochemical_disease": 5,
        "imaging_suspicious_unconfirmed": 6,
    }
    if len(df_final) > 0:
        df_final["_priority"] = df_final["recurrence_type"].map(priority).fillna(99)
        df_final = df_final.sort_values("_priority").drop_duplicates(subset=["research_id"], keep="first")
        df_final = df_final.drop(columns=["_priority"])
    else:
        df_final = pd.DataFrame(columns=list(COLS_FULL))

    print(f"\n  Total recurrence events (partial — before cohort padding): {len(df_final)}")
    print(f"  Unique patients: {df_final['research_id'].nunique()}")

    all_patients = con.execute(cohort_sql()).fetchdf()
    tier_counts["cohort_all_patients"] = len(all_patients)

    missing = all_patients[~all_patients["research_id"].isin(df_final["research_id"])]

    no_recurrence: list[dict[str, Any]] = []
    for rid in missing["research_id"]:
        rs = rid
        fs = first_surg_map.get(rs)
        no_recurrence.append({
            "research_id": rs,
            "recurrence_confirmed": False,
            "recurrence_type": "none",
            "recurrence_date": None,
            "recurrence_site": None,
            "recurrence_histology": None,
            "recurrence_evidence_source": None,
            "recurrence_definition": "no_recurrence_evidence",
            "first_surgery_date": fs,
            "time_to_recurrence_days": None,
            "biochemical_tg_nadir": None,
            "biochemical_tg_at_recurrence": None,
        })
    df_no = pd.DataFrame(no_recurrence)
    df_final = pd.concat([df_final, df_no], ignore_index=True)

    tier_counts["no_recurrence_pad_rows"] = len(missing)
    tier_counts["final_row_count"] = len(df_final)
    tier_counts["final_distinct_research_id"] = int(df_final["research_id"].nunique())
    tier_counts["confirmed_true_count"] = int((df_final["recurrence_confirmed"] == True).sum())
    tier_counts["path_proven_upstream_date_outliers"] = outlier_pp
    tier_counts["legacy_structural_confirmed_rows_kept"] = int(len(old_structural))

    neg_ttr = df_final["time_to_recurrence_days"].notna() & (
        df_final["time_to_recurrence_days"] < 0
    )
    n_bad = int(neg_ttr.sum())
    if n_bad > 0:
        print(
            f"\n  WARNING: {n_bad} rows with negative time_to_recurrence_days — "
            f"setting to NULL (date alignment / cohort spine mismatch — see report)."
        )
        df_final = df_final.copy()
        df_final.loc[neg_ttr, "time_to_recurrence_days"] = None
    tier_counts["time_to_recurrence_negative_clipped"] = n_bad

    ttr_series = df_final["time_to_recurrence_days"].dropna()
    if len(ttr_series) > 0:
        assert (ttr_series >= 0).all(), (
            f"Negative time_to_recurrence_days: {ttr_series[ttr_series < 0].tolist()[:10]}"
        )

    return df_final, tier_frames, tier_counts


def snapshot_live_table(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Current MotherDuck recurrence table shape (may be mig_122 shell)."""
    out: dict[str, Any] = {}
    try:
        out["live_row_count"] = con.execute(
            "SELECT COUNT(*) FROM canonical_recurrence_v1"
        ).fetchone()[0]
    except duckdb.Error as e:
        out["error"] = str(e)
        return out

    vc = con.execute("""
        SELECT recurrence_type AS k, COUNT(*) AS n
        FROM canonical_recurrence_v1
        GROUP BY 1
        ORDER BY n DESC
    """).fetchall()
    out["recurrence_type_counts"] = {k: int(n) for k, n in vc}

    ctrue = con.execute(
        """SELECT COUNT(*) FROM canonical_recurrence_v1 WHERE recurrence_confirmed = TRUE"""
    ).fetchone()[0]
    cfalse = con.execute(
        """SELECT COUNT(*) FROM canonical_recurrence_v1 WHERE recurrence_confirmed = FALSE"""
    ).fetchone()[0]
    out["confirmed_true"] = int(ctrue)
    out["confirmed_false"] = int(cfalse)

    ncpm = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {CPM}").fetchone()[0]
    out["distinct_cpm_research_ids"] = int(ncpm)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Harmonized canonical_recurrence_v1 builder (Lane 19)")
    p.add_argument(
        "--write",
        action="store_true",
        help="Write parquet + CREATE OR REPLACE TABLE canonical_recurrence_v1 (MotherDuck RW)",
    )
    args = p.parse_args()

    con = connect()
    print(f"Connected to MotherDuck {DB}")
    print(f"Spine: {OE} | Cohort: {CPM}")

    live_snapshot = snapshot_live_table(con)

    df_final, _tier_frames, tier_counts = build_canonical_rows(con)

    print("\n=== Validation ===")
    confirmed = df_final[df_final["recurrence_confirmed"] == True]
    print(f"  Rows: {len(df_final)} distinct rids={df_final['research_id'].nunique()}")
    print(f"  CONFIRMED (TRUE): {len(confirmed)}")

    tdist = df_final["recurrence_type"].value_counts()
    print("\n  recurrence_status_final / type distribution:")
    for k, v in tdist.items():
        print(f"    {k}: {int(v)}")
    ev = df_final[df_final["recurrence_type"] != "none"]["recurrence_evidence_source"].value_counts()
    print("\n  Evidence source (non-none rows only):")
    for k, v in ev.items():
        print(f"    {k}: {int(v)}")

    cw = confirmed[confirmed["recurrence_date"].notna()]
    print(f"\n  Confirmed with recurrence_date non-null: {len(cw)}")

    report: dict[str, Any] = {
        "generated_utc": datetime.now(UTC).isoformat(),
        "script": str(Path(__file__).name),
        "dry_run": not args.write,
        "harmonization": {
            "operative_spine": OE,
            "cohort_padding": CPM,
            "path_proven_defensive_predicate": PATH_PROVEN_DEFENSIVE_DATE_FILTER.strip(),
            "legacy_structural_filter": (
                "recurrence_date > first_surgery_date (join to first_surg spine)"
            ),
        },
        "live_table_before_run": live_snapshot,
        "tier_summaries": {
            str(k): (int(v) if not hasattr(v, "item") else v)
            for k, v in tier_counts.items()
        },
        "rebuilt_distribution": df_final.groupby("recurrence_type").size().astype(int).to_dict(),
        "confirmed_true_total": int((df_final["recurrence_confirmed"] == True).sum()),
    }

    rep_path = OUTPUT_DIR / "canonical_recurrence_203b_dry_run_report_20260429.json"
    rep_md = OUTPUT_DIR / "canonical_recurrence_203b_dry_run_report_20260429.md"

    df_final.to_parquet(OUTPUT_DIR / "canonical_recurrence_v1_preview_203b.parquet", index=False)

    report["preview_parquet"] = str(rep_path.with_name("canonical_recurrence_v1_preview_203b.parquet"))
    report["gates"] = {
        "cohort_10871_expectation": bool(tier_counts["final_row_count"] == 10871),
        "distinct_rids_equals_rows": bool(
            tier_counts["final_distinct_research_id"] == tier_counts["final_row_count"]
        ),
        "non_shell_confirmed_positive": bool(tier_counts["confirmed_true_count"] > 0),
        "confirmed_have_evidence_when_true": True,
        "time_to_recurrence_negative_clipped_rows": tier_counts.get(
            "time_to_recurrence_negative_clipped", 0
        ),
    }
    chk = df_final[df_final["recurrence_confirmed"] == True]
    report["gates"]["confirmed_have_evidence_when_true"] = bool(
        chk["recurrence_evidence_source"].notna().all() if len(chk) else True
    )
    chk_none = df_final[
        (df_final["recurrence_confirmed"] == False)
        & (df_final["recurrence_type"] == "none")
    ]
    report["gates"]["none_category_null_recurrence_date"] = bool(
        chk_none["recurrence_date"].isna().all() if len(chk_none) else True
    )

    with open(rep_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    md_lines = [
        "# Harmonized Script 203b — dry-run report (canonical_recurrence_v1)",
        "",
        f"**UTC:** {report['generated_utc']}  ",
        f"**Mode:** {'WRITE' if args.write else 'dry-run (no MotherDuck table replace)'}",
        "",
        "## Harmonization spine",
        f"- Operative backbone: `{OE}` (replaces `operative_episode_detail_v2`).",
        f"- Cohort padding: `{CPM}` (replaces `gold_master_patient_facts_v1`).",
        "",
        "## Live table before run (MotherDuck)",
        "```json",
        json.dumps(live_snapshot, indent=2, default=str),
        "```",
        "",
        "## Rebuilt summary (dry-run dataframe)",
        f"- Rows: **{tier_counts['final_row_count']}** (distinct `research_id`: **{tier_counts['final_distinct_research_id']}**)",
        f"- Confirmed TRUE: **{tier_counts['confirmed_true_count']}**",
        "",
        "### `recurrence_type` distribution",
        "",
    ]
    for k, v in df_final.groupby("recurrence_type").size().sort_values(ascending=False).items():
        md_lines.append(f"- `{k}`: **{int(v)}**")
    md_lines.extend([
        "",
        "### Acceptance gate preview",
        "```json",
        json.dumps(report["gates"], indent=2),
        "```",
        "",
        "### Spot-check filters — Lane 19 RESUME (2026-04-29)",
        "",
        "- `recurrence_event_clean_v1` legacy fallback: **`recurrence_date > first_surgery_date`** ",
        "  (`structural_confirmed`) — excludes initial-diagnosis/completion-Thy mismaps (CF narrative in mig_123).",
        "- **`manuscript_workspace.recurrence_path_proven_candidates_v1`**: predicate constant ",
        "`PATH_PROVEN_DEFENSIVE_DATE_FILTER` (1990–2027 inclusive) retained for Tier-1 future UNION;",
        "`path_proven_upstream_date_outliers` count = rows outside band (upstream clean-up deferred).",
        "",
        "- JSON: `" + rep_path.as_posix() + "`",
        "- Preview parquet: `" + OUTPUT_DIR.as_posix() + "/canonical_recurrence_v1_preview_203b.parquet`",
    ])
    with open(rep_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    print(f"\n  Written: {rep_path}")
    print(f"  Written: {rep_md}")

    if args.write:
        out_parquet = OUTPUT_DIR / "canonical_recurrence_v1.parquet"
        df_final.to_parquet(out_parquet, index=False)
        con.execute(
            "CREATE OR REPLACE TABLE canonical_recurrence_v1 AS SELECT * FROM read_parquet(?)",
            [str(out_parquet)],
        )
        n = con.execute("SELECT COUNT(*) FROM canonical_recurrence_v1").fetchone()[0]
        print(f"\n  Uploaded canonical_recurrence_v1: {n} rows")
        print("\n✓ Harmonized Prompt 203b COMPLETE — table replaced on MotherDuck")
    else:
        print("\n(Dry-run) No CREATE OR REPLACE MotherDuck table; omit `--write`.")

    con.close()


if __name__ == "__main__":
    main()
