#!/usr/bin/env python3
"""
THYROID_2026 — Recurrence Re-Adjudication
Prompt 2: SQL/Python with strict clinical definitions.

Publication-spine successor (operative + CPM cohort): see
scripts/203b_canonical_recurrence_harmonized_20260429.py (Lane 19 / mig_123 —
replaces operative_episode_detail_v2 + gold_master_patient_facts_v1 refs).

Creates: canonical_recurrence_v1

TRUE RECURRENCE requires ONE of:
  1. Reoperation with pathology showing recurrent/persistent cancer (structural, confirmed)
  2. FNA/biopsy with malignant cytology — Bethesda V/VI post-op (structural, confirmed)
  3. Rising Tg in a patient who previously had undetectable Tg (biochemical)

NOT true recurrence:
  - Imaging concerning without biopsy confirmation → imaging_suspicious_unconfirmed
  - Elevated Tg that was never undetectable → persistent_biochemical_disease
  - Clinical note mentions without confirmation → clinical_suspicion_unresolved
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Retargeted by Script 233 (2026-04-16) from the stale thyroid_ete_fix_20260413
# to the canonical publication DB so rebuilds always land in the clean master.
DB = "thyroid_canonical_publication_v1_0"


def connect():
    token = get_token()
    if not token:
        raise RuntimeError("MotherDuck token not found")
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


# ── TIER 1: Reoperation with cancer on pathology ─────────────────────────────
REOP_SQL = """
WITH first_surg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
    FROM operative_episode_detail_v2
    WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
    GROUP BY 1
),
all_surgeries AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) AS surgery_date,
        surgery_episode_id
    FROM operative_episode_detail_v2
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
        tp.tumor_1_histology_base AS recurrence_histology,
        'reoperation_pathology' AS recurrence_evidence_source,
        'structural_confirmed' AS recurrence_type,
        TRUE AS recurrence_confirmed,
        'surgical_pathology' AS recurrence_definition
    FROM reoperations r
    JOIN tumor_pathology tp ON CAST(r.research_id AS VARCHAR) = CAST(tp.research_id AS VARCHAR)
    WHERE ABS(DATE_DIFF('day', r.reop_date, TRY_CAST(tp.surgery_date AS DATE))) <= 30
      AND tp.tumor_1_histology_base IS NOT NULL
      AND TRIM(tp.tumor_1_histology_base) != ''
)
SELECT * FROM reop_with_pathology
"""

# ── TIER 2: Post-op FNA with malignant cytology (Bethesda V/VI) ──────────────
FNA_SQL = """
WITH first_surg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
    FROM operative_episode_detail_v2
    WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
    GROUP BY 1
)
SELECT DISTINCT
    CAST(f.research_id AS VARCHAR) AS research_id,
    TRY_CAST(f.resolved_fna_date AS DATE) AS recurrence_date,
    NULL AS recurrence_histology,
    'fna_cytology' AS recurrence_evidence_source,
    'fna_confirmed' AS recurrence_type,
    TRUE AS recurrence_confirmed,
    CASE
        WHEN f.bethesda_category = 6 THEN 'fna_bethesda_vi_malignant'
        WHEN f.bethesda_category = 5 THEN 'fna_bethesda_v_suspicious'
    END AS recurrence_definition
FROM fna_episode_master_v2 f
JOIN first_surg s ON CAST(f.research_id AS VARCHAR) = s.research_id
WHERE TRY_CAST(f.resolved_fna_date AS DATE) > s.first_surgery_date
  AND f.bethesda_category IN (5, 6)
"""

# ── TIER 3: Biochemical recurrence (rising Tg) ──────────────────────────────
BIOCHEM_SQL = """
WITH first_surg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
    FROM operative_episode_detail_v2
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
-- Find the first Tg rise >1.0 AFTER an undetectable nadir
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

# ── TIER 4: Persistent Tg (never undetectable) ──────────────────────────────
PERSISTENT_TG_SQL = """
WITH first_surg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
    FROM operative_episode_detail_v2
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
-- Patients with Tg always >= 0.2 (never undetectable) AND max > 1.0
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


def parse_llm_recurrence_entities(con):
    """Parse LLM recurrence extraction results to identify imaging/clinical suspicion."""
    print("  Parsing LLM recurrence entities...")
    rows = con.execute("""
        SELECT research_id, result_json, note_type, note_date
        FROM note_entities_llm_recurrence
        WHERE result_json IS NOT NULL
          AND CAST(result_json AS VARCHAR) NOT LIKE '%"entities": []%'
    """).fetchall()

    entities = []
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
        return pd.DataFrame(columns=["research_id", "entity_type", "entity_value",
                                     "entity_date", "confidence", "evidence_text",
                                     "note_type", "note_date", "present_or_negated"])
    # Only present (not negated)
    df = df[df["present_or_negated"] == "present"]
    print(f"  Found {len(df)} present recurrence/distant entities across {df['research_id'].nunique()} patients")
    return df


def main():
    con = connect()
    print(f"Connected to MotherDuck {DB}")

    # Get first surgery dates for time-to-recurrence calculations
    first_surg_df = con.execute("""
        SELECT CAST(research_id AS VARCHAR) AS research_id,
               MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
        FROM operative_episode_detail_v2
        WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
        GROUP BY 1
        UNION ALL
        SELECT CAST(research_id AS VARCHAR),
               MIN(TRY_CAST(surg_date AS DATE))
        FROM path_synoptics
        WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL
          AND CAST(research_id AS VARCHAR) NOT IN (
              SELECT CAST(research_id AS VARCHAR) FROM operative_episode_detail_v2
              WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
          )
        GROUP BY 1
    """).fetchdf()
    first_surg_map = dict(zip(first_surg_df["research_id"], first_surg_df["first_surgery_date"]))

    # ── TIER 1: Reoperation with pathology ─────────────────────────────────
    print("\n=== Tier 1: Reoperation with cancer on pathology ===")
    df_reop = con.execute(REOP_SQL).fetchdf()
    print(f"  Found: {len(df_reop)} reoperations with cancer, {df_reop['research_id'].nunique()} patients")

    # ── TIER 2: Post-op FNA Bethesda V/VI ──────────────────────────────────
    print("\n=== Tier 2: Post-op FNA with Bethesda V/VI ===")
    df_fna = con.execute(FNA_SQL).fetchdf()
    print(f"  Found: {len(df_fna)} post-op FNAs, {df_fna['research_id'].nunique()} patients")

    # ── TIER 3: Biochemical recurrence (rising Tg) ─────────────────────────
    print("\n=== Tier 3: Biochemical recurrence (rising Tg after undetectable) ===")
    df_biochem = con.execute(BIOCHEM_SQL).fetchdf()
    print(f"  Found: {len(df_biochem)} biochemical recurrences, {df_biochem['research_id'].nunique()} patients")

    # ── TIER 4: Persistent Tg (never undetectable) ─────────────────────────
    print("\n=== Tier 4: Persistent biochemical disease (never undetectable) ===")
    df_persistent = con.execute(PERSISTENT_TG_SQL).fetchdf()
    print(f"  Found: {len(df_persistent)} persistent Tg patients, {df_persistent['research_id'].nunique()} patients")

    # ── TIER 5: LLM-extracted imaging suspicion (unconfirmed) ──────────────
    print("\n=== Tier 5: LLM-extracted imaging suspicion (unconfirmed) ===")
    df_llm = parse_llm_recurrence_entities(con)

    # Combine confirmed patients from Tiers 1-2
    confirmed_rids = set()
    if len(df_reop) > 0:
        confirmed_rids.update(df_reop["research_id"].tolist())
    if len(df_fna) > 0:
        confirmed_rids.update(df_fna["research_id"].tolist())

    # Biochemical patients
    biochem_rids = set(df_biochem["research_id"].tolist()) if len(df_biochem) > 0 else set()

    # Persistent patients (set computed for parity with cohort enumeration;
    # not consumed downstream — derivation kept for audit traceability).
    _ = set(df_persistent["research_id"].tolist()) if len(df_persistent) > 0 else set()

    # LLM imaging suspicious (exclude those already in confirmed/biochem)
    llm_suspicious = pd.DataFrame()
    if len(df_llm) > 0:
        unconfirmed_llm = df_llm[
            ~df_llm["research_id"].isin(confirmed_rids | biochem_rids)
        ].drop_duplicates(subset=["research_id"], keep="first")
        llm_suspicious = unconfirmed_llm.copy()
        print(f"  Imaging suspicious (unconfirmed, not in tiers 1-3): {len(llm_suspicious)} patients")

    # ── BUILD CANONICAL TABLE ────────────────────────────────────────────────
    print("\n=== Building canonical_recurrence_v1 ===")
    rows = []

    # Add Tier 1 (reoperation confirmed)
    for _, r in df_reop.iterrows():
        fs = first_surg_map.get(r["research_id"])
        ttr = (r["recurrence_date"] - fs).days if fs is not None and pd.notna(r["recurrence_date"]) and pd.notna(fs) else None
        rows.append({
            "research_id": r["research_id"],
            "recurrence_confirmed": True,
            "recurrence_type": "structural_confirmed",
            "recurrence_date": r["recurrence_date"],
            "recurrence_site": None,
            "recurrence_histology": r.get("recurrence_histology"),
            "recurrence_evidence_source": "reoperation_pathology",
            "recurrence_definition": r.get("recurrence_definition", "surgical_pathology"),
            "first_surgery_date": fs,
            "time_to_recurrence_days": ttr,
            "biochemical_tg_nadir": None,
            "biochemical_tg_at_recurrence": None,
        })

    # Add Tier 2 (FNA confirmed)
    for _, r in df_fna.iterrows():
        if r["research_id"] in [row["research_id"] for row in rows]:
            continue
        fs = first_surg_map.get(r["research_id"])
        ttr = (r["recurrence_date"] - fs).days if fs is not None and pd.notna(r["recurrence_date"]) and pd.notna(fs) else None
        rows.append({
            "research_id": r["research_id"],
            "recurrence_confirmed": True,
            "recurrence_type": "fna_confirmed",
            "recurrence_date": r["recurrence_date"],
            "recurrence_site": None,
            "recurrence_histology": None,
            "recurrence_evidence_source": "fna_cytology",
            "recurrence_definition": r.get("recurrence_definition", "fna_bethesda_v_vi"),
            "first_surgery_date": fs,
            "time_to_recurrence_days": ttr,
            "biochemical_tg_nadir": None,
            "biochemical_tg_at_recurrence": None,
        })

    # Add Tier 3 (biochemical recurrence)
    for _, r in df_biochem.iterrows():
        rid = r["research_id"]
        if rid in [row["research_id"] for row in rows]:
            continue
        fs = first_surg_map.get(rid)
        rd = r.get("recurrence_date")
        ttr = (rd - fs).days if fs is not None and pd.notna(rd) and pd.notna(fs) else None
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
            "time_to_recurrence_days": ttr,
            "biochemical_tg_nadir": r.get("tg_nadir_value"),
            "biochemical_tg_at_recurrence": r.get("tg_at_recurrence"),
        })

    # Add Tier 4 (persistent Tg)
    for _, r in df_persistent.iterrows():
        rid = r["research_id"]
        if rid in [row["research_id"] for row in rows]:
            continue
        fs = first_surg_map.get(rid)
        rd = r.get("recurrence_date")
        # Script 233 (2026-04-16): derive time-to-recurrence from
        # recurrence_days_from_surg analog; guard against negatives so
        # the canonical never receives a nonsensical value for Tier 4/5.
        rds = (rd - fs).days if (fs is not None and pd.notna(rd) and pd.notna(fs)) else None
        ttr = rds if (rds is not None and rds >= 0) else None
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
            "time_to_recurrence_days": ttr,
            "biochemical_tg_nadir": r.get("tg_min"),
            "biochemical_tg_at_recurrence": r.get("tg_max"),
        })

    # Add Tier 5 (imaging suspicious unconfirmed)
    if len(llm_suspicious) > 0:
        for _, r in llm_suspicious.iterrows():
            rid = r["research_id"]
            if rid in [row["research_id"] for row in rows]:
                continue
            fs = first_surg_map.get(rid)
            rd = pd.to_datetime(r.get("entity_date"), errors="coerce") if r.get("entity_date") else None
            # Script 233 (2026-04-16): guard against negatives so Tier 5
            # never leaks a nonsensical day-count into the canonical.
            rds = (rd - fs).days if (fs is not None and pd.notna(rd) and pd.notna(fs)) else None
            ttr = rds if (rds is not None and rds >= 0) else None
            rows.append({
                "research_id": rid,
                "recurrence_confirmed": False,
                "recurrence_type": "imaging_suspicious_unconfirmed",
                "recurrence_date": rd,
                "recurrence_site": r.get("entity_value", "")[:200] if r.get("entity_value") else None,
                "recurrence_histology": None,
                "recurrence_evidence_source": "imaging_or_clinical_note",
                "recurrence_definition": "llm_extracted_unconfirmed",
                "first_surgery_date": fs,
                "time_to_recurrence_days": ttr,
                "biochemical_tg_nadir": None,
                "biochemical_tg_at_recurrence": None,
            })

    # Also add patients from old recurrence_event_clean_v1 not yet captured
    old_rids = set(r["research_id"] for r in rows)
    old_structural = con.execute("""
        SELECT CAST(research_id AS VARCHAR) AS research_id,
               recurrence_date, recurrence_type, recurrence_definition
        FROM recurrence_event_clean_v1
        WHERE recurrence_type = 'structural'
          AND recurrence_definition = 'structural_confirmed'
          AND CAST(research_id AS VARCHAR) NOT IN ({})
    """.format(",".join(f"'{r}'" for r in old_rids) if old_rids else "'__none__'")).fetchdf()
    for _, r in old_structural.iterrows():
        fs = first_surg_map.get(r["research_id"])
        rd = r.get("recurrence_date")
        ttr = (rd - fs).days if fs is not None and pd.notna(rd) and pd.notna(fs) else None
        rows.append({
            "research_id": r["research_id"],
            "recurrence_confirmed": True,
            "recurrence_type": "structural_confirmed_legacy",
            "recurrence_date": rd,
            "recurrence_site": None,
            "recurrence_histology": None,
            "recurrence_evidence_source": "recurrence_event_clean_v1_legacy",
            "recurrence_definition": "legacy_structural_confirmed",
            "first_surgery_date": fs,
            "time_to_recurrence_days": ttr,
            "biochemical_tg_nadir": None,
            "biochemical_tg_at_recurrence": None,
        })

    df_final = pd.DataFrame(rows)
    # Deduplicate by research_id — keep highest-priority tier
    priority = {
        "structural_confirmed": 1,
        "fna_confirmed": 2,
        "structural_confirmed_legacy": 3,
        "biochemical_tg_rise": 4,
        "persistent_biochemical_disease": 5,
        "imaging_suspicious_unconfirmed": 6,
    }
    df_final["_priority"] = df_final["recurrence_type"].map(priority).fillna(99)
    df_final = df_final.sort_values("_priority").drop_duplicates(subset=["research_id"], keep="first")
    df_final = df_final.drop(columns=["_priority"])

    print(f"\n  Total recurrence events: {len(df_final)}")
    print(f"  Unique patients: {df_final['research_id'].nunique()}")

    # ── ALL patients in gold get a row (no-recurrence = none) ────────────────
    all_patients = con.execute(
        "SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM gold_master_patient_facts_v1"
    ).fetchdf()
    missing = all_patients[~all_patients["research_id"].isin(df_final["research_id"])]
    no_recurrence = []
    for rid in missing["research_id"]:
        fs = first_surg_map.get(rid)
        no_recurrence.append({
            "research_id": rid,
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

    print(f"\n  After adding no-recurrence patients: {len(df_final)} rows")

    # ── Validation ────────────────────────────────────────────────────────────
    print("\n=== Validation ===")
    confirmed = df_final[df_final["recurrence_confirmed"] == True]
    print(f"  CONFIRMED recurrences: {len(confirmed)}")
    print("    vs old system: 1,946 (expected MUCH less now)")

    print("\n  Recurrence type distribution:")
    tdist = df_final["recurrence_type"].value_counts()
    for k, v in tdist.items():
        print(f"    {k}: {v}")

    confirmed_with_date = confirmed[confirmed["recurrence_date"].notna()]
    print(f"\n  Confirmed with date: {len(confirmed_with_date)}")
    if len(confirmed_with_date) > 0:
        ttr = confirmed_with_date["time_to_recurrence_days"].dropna()
        if len(ttr) > 0:
            print(f"  Median time-to-recurrence: {ttr.median():.0f} days ({ttr.median()/365.25:.1f} years)")

    print("\n  Evidence source breakdown (recurrence cases only):")
    recur = df_final[df_final["recurrence_type"] != "none"]
    edist = recur["recurrence_evidence_source"].value_counts()
    for k, v in edist.items():
        print(f"    {k}: {v}")

    # Script 233 (2026-04-16): validation gate — reject any negative
    # time_to_recurrence_days before we write the canonical.
    ttr_series = df_final["time_to_recurrence_days"].dropna()
    assert (ttr_series >= 0).all(), (
        f"Negative time_to_recurrence_days found: "
        f"{ttr_series[ttr_series < 0].tolist()[:10]}"
    )

    # Save and upload
    out_path = OUTPUT_DIR / "canonical_recurrence_v1.parquet"
    df_final.to_parquet(out_path, index=False)
    print(f"\n  Saved: {out_path}")

    con.execute("CREATE OR REPLACE TABLE canonical_recurrence_v1 AS SELECT * FROM read_parquet(?)", [str(out_path)])
    verify = con.execute("SELECT COUNT(*) FROM canonical_recurrence_v1").fetchone()[0]
    print(f"  Uploaded to MotherDuck: {verify} rows")

    print("\n✓ Prompt 2 COMPLETE — canonical_recurrence_v1 uploaded to MotherDuck")
    con.close()


if __name__ == "__main__":
    main()
