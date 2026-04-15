#!/usr/bin/env python3
"""
THYROID_2026 — Survival & Follow-Up Time Canonical Definition
Prompt 3: Pure SQL — NO LLM needed.

Creates: canonical_survival_followup_v1

START = first surgery date
END   = last documented clinical contact = MAX(last lab, last imaging, last note, last FNA)
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import numpy as np

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

DB = "thyroid_ete_fix_20260413"


def connect():
    token = get_token()
    if not token:
        raise RuntimeError("MotherDuck token not found")
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


SURVIVAL_SQL = """
WITH first_surgery AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE))) AS first_surgery_date
    FROM operative_episode_detail_v2
    WHERE COALESCE(surgery_date_native, TRY_CAST(resolved_surgery_date AS DATE)) IS NOT NULL
    GROUP BY 1
),
-- Fallback to path_synoptics for patients not in OED
ps_surgery AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MIN(TRY_CAST(surg_date AS DATE)) AS first_surgery_date
    FROM path_synoptics
    WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL
      AND CAST(research_id AS VARCHAR) NOT IN (SELECT research_id FROM first_surgery)
    GROUP BY 1
),
all_surgery AS (
    SELECT * FROM first_surgery
    UNION ALL
    SELECT * FROM ps_surgery
),
last_lab AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MAX(TRY_CAST(lab_date AS DATE)) AS last_lab_date,
        'lab' AS source
    FROM longitudinal_lab_canonical_v1
    WHERE lab_date IS NOT NULL
    GROUP BY 1
),
last_tg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MAX(TRY_CAST(specimen_collect_dt AS DATE)) AS last_tg_date
    FROM thyroglobulin_lab_canonical_v1
    WHERE specimen_collect_dt IS NOT NULL
    GROUP BY 1
),
last_us AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MAX(TRY_CAST(ultrasound_date AS DATE)) AS last_us_date
    FROM ultrasound_reports
    WHERE ultrasound_date IS NOT NULL
    GROUP BY 1
),
last_ct AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MAX(TRY_CAST(date_of_exam AS DATE)) AS last_ct_date
    FROM ct_imaging
    WHERE date_of_exam IS NOT NULL
    GROUP BY 1
),
last_nuc AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MAX(TRY_CAST(scandate AS DATE)) AS last_nuclear_date
    FROM nuclear_med
    WHERE scandate IS NOT NULL
    GROUP BY 1
),
-- clinical_notes_long has no note_date column on MotherDuck; skip note-based last-contact
last_fna AS (
    SELECT
        CAST(research_id AS VARCHAR) AS research_id,
        MAX(TRY_CAST(resolved_fna_date AS DATE)) AS last_fna_date
    FROM fna_episode_master_v2
    WHERE resolved_fna_date IS NOT NULL
    GROUP BY 1
),
combined AS (
    SELECT
        fs.research_id,
        fs.first_surgery_date,
        ll.last_lab_date,
        lt.last_tg_date,
        lus.last_us_date,
        lct.last_ct_date,
        lnuc.last_nuclear_date,
        lf.last_fna_date,
        GREATEST(
            COALESCE(ll.last_lab_date, fs.first_surgery_date),
            COALESCE(lt.last_tg_date, fs.first_surgery_date),
            COALESCE(lus.last_us_date, fs.first_surgery_date),
            COALESCE(lct.last_ct_date, fs.first_surgery_date),
            COALESCE(lnuc.last_nuclear_date, fs.first_surgery_date),
            COALESCE(lf.last_fna_date, fs.first_surgery_date)
        ) AS last_contact_date
    FROM all_surgery fs
    LEFT JOIN last_lab ll ON fs.research_id = ll.research_id
    LEFT JOIN last_tg lt ON fs.research_id = lt.research_id
    LEFT JOIN last_us lus ON fs.research_id = lus.research_id
    LEFT JOIN last_ct lct ON fs.research_id = lct.research_id
    LEFT JOIN last_nuc lnuc ON fs.research_id = lnuc.research_id
    LEFT JOIN last_fna lf ON fs.research_id = lf.research_id
)
SELECT
    research_id,
    first_surgery_date,
    last_lab_date,
    last_tg_date,
    last_us_date,
    last_ct_date,
    last_nuclear_date,
    last_fna_date,
    last_contact_date,
    CASE
        WHEN last_contact_date = last_lab_date THEN 'lab'
        WHEN last_contact_date = last_tg_date THEN 'thyroglobulin_lab'
        WHEN last_contact_date = last_us_date THEN 'ultrasound'
        WHEN last_contact_date = last_ct_date THEN 'ct_imaging'
        WHEN last_contact_date = last_nuclear_date THEN 'nuclear_med'
        WHEN last_contact_date = last_fna_date THEN 'fna'
        ELSE 'surgery_only'
    END AS last_contact_source,
    DATE_DIFF('day', first_surgery_date, last_contact_date) AS followup_days,
    ROUND(DATE_DIFF('day', first_surgery_date, last_contact_date) / 365.25, 2) AS followup_years,
    CASE
        WHEN DATE_DIFF('day', first_surgery_date, last_contact_date) < 365 THEN 'short_followup'
        WHEN DATE_DIFF('day', first_surgery_date, last_contact_date) < 1825 THEN 'medium_followup'
        ELSE 'long_followup'
    END AS followup_category
FROM combined
WHERE first_surgery_date IS NOT NULL
"""


def main():
    con = connect()
    print(f"Connected to MotherDuck {DB}")

    print("\n=== Building canonical_survival_followup_v1 ===")
    df = con.execute(SURVIVAL_SQL).fetchdf()
    print(f"  Rows: {len(df)}, Patients: {df['research_id'].nunique()}")

    # Validation
    print("\n=== Validation ===")
    neg_followup = df[df["followup_days"] < 0]
    print(f"  Negative followup_days: {len(neg_followup)}")
    if len(neg_followup) > 0:
        print(f"  ⚠ {len(neg_followup)} patients have last_contact before surgery — capping at 0")
        df.loc[df["followup_days"] < 0, "followup_days"] = 0
        df.loc[df["followup_years"] < 0, "followup_years"] = 0.0

    print(f"\n  Follow-up (years):")
    print(f"    Median: {df['followup_years'].median():.2f}")
    print(f"    Mean:   {df['followup_years'].mean():.2f}")
    q25, q75 = df["followup_years"].quantile([0.25, 0.75])
    print(f"    IQR:    {q25:.2f} – {q75:.2f}")
    print(f"    Range:  {df['followup_years'].min():.2f} – {df['followup_years'].max():.2f}")

    print(f"\n  Last contact source distribution:")
    src_dist = df["last_contact_source"].value_counts()
    for k, v in src_dist.items():
        print(f"    {k}: {v} ({100*v/len(df):.1f}%)")

    print(f"\n  Follow-up category:")
    cat_dist = df["followup_category"].value_counts()
    for k, v in cat_dist.items():
        print(f"    {k}: {v} ({100*v/len(df):.1f}%)")

    short = df[df["followup_years"] < 1]
    print(f"\n  Patients with <1 year follow-up: {len(short)} ({100*len(short)/len(df):.1f}%)")

    gold_count = con.execute("SELECT COUNT(DISTINCT research_id) FROM gold_master_patient_facts_v1").fetchone()[0]
    print(f"\n  Coverage: {len(df)}/{gold_count} gold_master patients ({100*len(df)/gold_count:.1f}%)")
    missing_n = gold_count - df["research_id"].nunique()
    if missing_n > 0:
        print(f"  ⚠ {missing_n} patients in gold_master have no surgery date → no survival row")

    # Save and upload
    out_path = OUTPUT_DIR / "canonical_survival_followup_v1.parquet"
    df.to_parquet(out_path, index=False)
    print(f"\n  Saved: {out_path}")

    con.execute("CREATE OR REPLACE TABLE canonical_survival_followup_v1 AS SELECT * FROM read_parquet(?)", [str(out_path)])
    verify = con.execute("SELECT COUNT(*) FROM canonical_survival_followup_v1").fetchone()[0]
    print(f"  Uploaded to MotherDuck: {verify} rows")

    print("\n✓ Prompt 3 COMPLETE — canonical_survival_followup_v1 uploaded to MotherDuck")
    con.close()


if __name__ == "__main__":
    main()
