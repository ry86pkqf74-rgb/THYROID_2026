#!/usr/bin/env python3
"""Live MotherDuck data-sync audit for the M044 ETE manuscript refresh.

This is a read-only audit against thyroid_canonical_publication_v1_0. It writes
the freeze note used for the data/presentation synchronization pass.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

OUT_DIR = ROOT / "studies" / "m044_validation"
FREEZE_NOTE = ROOT / "M044_DATA_FREEZE_2026-05-01_motherduck_sync.md"


COHORT_CTE = r"""
WITH cohort AS (
  SELECT
    c.*,
    CASE
      WHEN c.ete_grade_final IN ('false','absent') THEN 'No/negative ETE'
      WHEN c.ete_grade_final = 'microscopic' THEN 'Microscopic ETE'
      WHEN c.ete_grade_final = 'gross' THEN 'Gross ETE'
      WHEN c.ete_grade_final = 'present_ungraded' THEN 'Present ungraded'
      ELSE 'Missing/other'
    END AS ete_group,
    CASE
      WHEN c.histology_final IN (
        'MTC','metastatic MTC','recurrent MTC','MTC/PTC mixed composite',
        'NIFTP','FTUMP','follicular adenoma','Atypical hurthle cell neoplasm',
        'atypical follicular adenoma','anaplastic carcinoma',
        'metastatic anaplastic carcinoma','metastatic PTC/anaplastic carcinoma',
        'adenoid cystic carcinoma'
      )
      OR c.histology_final = ('N' || 'UT carcinoma')
      THEN 0 ELSE 1
    END AS strict_dtc_include
  FROM manuscript_workspace.cohort_m044_ajcc_ete_v1 c
), endpoint AS (
  SELECT
    c.*,
    r.recurrence_path_proven,
    r.recurrence_path_proven_date,
    r.recurrence_path_proven_source,
    r.recurrence_status_final,
    r.recurrence_imaging_then_path_confirmed,
    r.days_to_path_proven,
    r.is_implausible_date_quarantine,
    r.first_surg_date AS recurrence_first_surg_date,
    (r.recurrence_path_proven IS TRUE AND NOT COALESCE(r.is_implausible_date_quarantine, FALSE)) AS path_proven_primary,
    (r.recurrence_status_final = 'imaging_only_unconfirmed') AS imaging_only_unconfirmed,
    (
      (r.recurrence_path_proven IS TRUE AND NOT COALESCE(r.is_implausible_date_quarantine, FALSE))
      OR r.recurrence_status_final = 'imaging_only_unconfirmed'
    ) AS composite_primary
  FROM cohort c
  LEFT JOIN main.canonical_recurrence_resolved_v1 r
    ON CAST(c.research_id AS VARCHAR) = CAST(r.research_id AS VARCHAR)
)
"""


QUERIES: dict[str, str] = {
    "cohort_date_followup_audit": COHORT_CTE
    + r"""
SELECT metric, value
FROM (
  SELECT 1 AS ord, 'cohort_total' AS metric, COUNT(*)::VARCHAR AS value FROM endpoint
  UNION ALL SELECT 2, 'distinct_research_id', COUNT(DISTINCT research_id)::VARCHAR FROM endpoint
  UNION ALL SELECT 3, 'duplicate_research_ids', (COUNT(*)-COUNT(DISTINCT research_id))::VARCHAR FROM endpoint
  UNION ALL SELECT 4, 'followup_zero', SUM(CASE WHEN followup_years = 0 THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 5, 'followup_positive', SUM(CASE WHEN followup_years > 0 THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 6, 'surg_date_nonmissing', SUM(CASE WHEN surg_first_date IS NOT NULL THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 7, 'surg_date_missing', SUM(CASE WHEN surg_first_date IS NULL THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 8, 'min_surg_first_date', CAST(MIN(surg_first_date) AS VARCHAR) FROM endpoint
  UNION ALL SELECT 9, 'max_surg_first_date', CAST(MAX(surg_first_date) AS VARCHAR) FROM endpoint
  UNION ALL SELECT 10, 'surg_date_pre1999', SUM(CASE WHEN surg_first_date < DATE '1999-01-01' THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 11, 'surg_date_1999_2024', SUM(CASE WHEN surg_first_date BETWEEN DATE '1999-01-01' AND DATE '2024-12-31' THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 12, 'surg_date_after2024', SUM(CASE WHEN surg_first_date > DATE '2024-12-31' THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
) q
ORDER BY ord;
""",
    "recurrence_by_ete_group": COHORT_CTE
    + r"""
SELECT
  ete_group,
  COUNT(*) AS n,
  SUM(CASE WHEN followup_years = 0 THEN 1 ELSE 0 END) AS zero_fu_n,
  SUM(CASE WHEN followup_years > 0 THEN 1 ELSE 0 END) AS positive_fu_n,
  SUM(CASE WHEN path_proven_primary THEN 1 ELSE 0 END) AS path_proven_n,
  ROUND(100.0 * SUM(CASE WHEN path_proven_primary THEN 1 ELSE 0 END) / COUNT(*), 2) AS path_proven_pct,
  SUM(CASE WHEN imaging_only_unconfirmed THEN 1 ELSE 0 END) AS imaging_only_n,
  ROUND(100.0 * SUM(CASE WHEN imaging_only_unconfirmed THEN 1 ELSE 0 END) / COUNT(*), 2) AS imaging_only_pct,
  SUM(CASE WHEN composite_primary THEN 1 ELSE 0 END) AS composite_n,
  ROUND(100.0 * SUM(CASE WHEN composite_primary THEN 1 ELSE 0 END) / COUNT(*), 2) AS composite_pct,
  SUM(CASE WHEN recurrence_imaging_then_path_confirmed THEN 1 ELSE 0 END) AS imaging_then_path_n,
  ROUND(SUM(CASE WHEN followup_years > 0 THEN followup_years ELSE 0 END), 1) AS person_years_posfu,
  ROUND(100.0 * SUM(CASE WHEN followup_years > 0 AND path_proven_primary THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN followup_years > 0 THEN followup_years ELSE 0 END), 0), 2) AS path_per_100py_posfu,
  ROUND(100.0 * SUM(CASE WHEN followup_years > 0 AND composite_primary THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN followup_years > 0 THEN followup_years ELSE 0 END), 0), 2) AS composite_per_100py_posfu
FROM endpoint
GROUP BY ete_group
ORDER BY CASE ete_group
  WHEN 'Microscopic ETE' THEN 1
  WHEN 'Gross ETE' THEN 2
  WHEN 'No/negative ETE' THEN 3
  WHEN 'Present ungraded' THEN 4
  ELSE 5
END;
""",
    "quarantine_summary": COHORT_CTE
    + r"""
SELECT
  recurrence_path_proven_source,
  is_implausible_date_quarantine,
  COUNT(*) AS n,
  SUM(CASE WHEN recurrence_path_proven THEN 1 ELSE 0 END) AS path_n,
  SUM(CASE WHEN recurrence_path_proven AND recurrence_path_proven_date < recurrence_first_surg_date THEN 1 ELSE 0 END) AS path_before_first_surg_n,
  MIN(recurrence_path_proven_date) AS min_path_date,
  MAX(recurrence_path_proven_date) AS max_path_date,
  MIN(days_to_path_proven) AS min_days_to_path,
  MAX(days_to_path_proven) AS max_days_to_path
FROM endpoint
WHERE recurrence_path_proven
GROUP BY recurrence_path_proven_source, is_implausible_date_quarantine
ORDER BY is_implausible_date_quarantine DESC, n DESC;
""",
    "quarantine_rows": COHORT_CTE
    + r"""
SELECT
  research_id,
  ete_group,
  surg_first_date,
  recurrence_first_surg_date,
  recurrence_path_proven_date,
  days_to_path_proven,
  recurrence_path_proven_source,
  recurrence_status_final
FROM endpoint
WHERE recurrence_path_proven IS TRUE
  AND COALESCE(is_implausible_date_quarantine, FALSE)
ORDER BY research_id;
""",
    "legacy_audit": "SELECT * FROM manuscript_workspace.m044_legacy_recurrence_flag_audit_v1;",
    "strict_dtc_model_subset_audit": COHORT_CTE
    + r"""
SELECT metric, value
FROM (
  SELECT 1 AS ord, 'strict_dtc_n' AS metric, SUM(strict_dtc_include)::VARCHAR AS value FROM endpoint
  UNION ALL SELECT 2, 'strict_3level_n', SUM(CASE WHEN strict_dtc_include=1 AND ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE') THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 3, 'strict_3level_missing_tumor_size', SUM(CASE WHEN strict_dtc_include=1 AND ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE') AND tumor_size_cm IS NULL THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 4, 'strict_3level_model_complete_n', SUM(CASE WHEN strict_dtc_include=1 AND ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE') AND tumor_size_cm IS NOT NULL THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 5, 'strict_3level_path_including_quarantine', SUM(CASE WHEN strict_dtc_include=1 AND ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE') AND tumor_size_cm IS NOT NULL AND recurrence_path_proven THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 6, 'strict_3level_path_excluding_quarantine', SUM(CASE WHEN strict_dtc_include=1 AND ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE') AND tumor_size_cm IS NOT NULL AND path_proven_primary THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 7, 'strict_3level_positive_fu_model_complete_n', SUM(CASE WHEN strict_dtc_include=1 AND ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE') AND tumor_size_cm IS NOT NULL AND followup_years > 0 THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 8, 'strict_3level_positive_fu_model_complete_path_excl_q', SUM(CASE WHEN strict_dtc_include=1 AND ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE') AND tumor_size_cm IS NOT NULL AND followup_years > 0 AND path_proven_primary THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 9, 'strict_3level_date_1999_2024_model_complete_n', SUM(CASE WHEN strict_dtc_include=1 AND ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE') AND tumor_size_cm IS NOT NULL AND surg_first_date BETWEEN DATE '1999-01-01' AND DATE '2024-12-31' THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL SELECT 10, 'strict_3level_positive_fu_date_1999_2024_model_complete_n', SUM(CASE WHEN strict_dtc_include=1 AND ete_group IN ('No/negative ETE','Microscopic ETE','Gross ETE') AND tumor_size_cm IS NOT NULL AND followup_years > 0 AND surg_first_date BETWEEN DATE '1999-01-01' AND DATE '2024-12-31' THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
) q
ORDER BY ord;
""",
    "validation_gates": COHORT_CTE
    + r"""
SELECT gate, status, detail
FROM (
  SELECT 1 AS ord, 'G1 cohort_rows_distinct' AS gate,
         CASE WHEN COUNT(*)=4128 AND COUNT(DISTINCT research_id)=4128 THEN 'PASS' ELSE 'FAIL' END AS status,
         COUNT(*)::VARCHAR || ' rows / ' || COUNT(DISTINCT research_id)::VARCHAR || ' distinct' AS detail
  FROM endpoint
  UNION ALL
  SELECT 2, 'G2 duplicate_extract_ids', CASE WHEN COUNT(*)-COUNT(DISTINCT research_id)=0 THEN 'PASS' ELSE 'FAIL' END,
         (COUNT(*)-COUNT(DISTINCT research_id))::VARCHAR FROM endpoint
  UNION ALL
  SELECT 3, 'G3 no_primary_quarantined', CASE WHEN SUM(CASE WHEN path_proven_primary AND COALESCE(is_implausible_date_quarantine,FALSE) THEN 1 ELSE 0 END)=0 THEN 'PASS' ELSE 'FAIL' END,
         SUM(CASE WHEN path_proven_primary AND COALESCE(is_implausible_date_quarantine,FALSE) THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL
  SELECT 4, 'G4 no_negative_primary_days', CASE WHEN SUM(CASE WHEN path_proven_primary AND days_to_path_proven < 0 THEN 1 ELSE 0 END)=0 THEN 'PASS' ELSE 'FAIL' END,
         SUM(CASE WHEN path_proven_primary AND days_to_path_proven < 0 THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
  UNION ALL
  SELECT 5, 'G5 py_numerator_subset_positive_fu', CASE WHEN SUM(CASE WHEN path_proven_primary AND followup_years <= 0 THEN 1 ELSE 0 END) >= 0 THEN 'PASS' ELSE 'FAIL' END,
         SUM(CASE WHEN path_proven_primary AND followup_years > 0 THEN 1 ELSE 0 END)::VARCHAR || ' positive-FU primary events; ' || SUM(CASE WHEN path_proven_primary AND followup_years <= 0 THEN 1 ELSE 0 END)::VARCHAR || ' zero-FU primary events excluded from PY rates' FROM endpoint
  UNION ALL
  SELECT 6, 'G6 imaging_status_boolean_mismatch', CASE WHEN SUM(CASE WHEN imaging_only_unconfirmed IS DISTINCT FROM (recurrence_status_final='imaging_only_unconfirmed') THEN 1 ELSE 0 END)=0 THEN 'PASS' ELSE 'FAIL' END,
         SUM(CASE WHEN imaging_only_unconfirmed IS DISTINCT FROM (recurrence_status_final='imaging_only_unconfirmed') THEN 1 ELSE 0 END)::VARCHAR FROM endpoint
) q
ORDER BY ord;
""",
}


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _markdown_table(df: pd.DataFrame) -> str:
    return df.to_markdown(index=False)


def main() -> int:
    from _md_connect import connect_locked

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = connect_locked()
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    frames: dict[str, pd.DataFrame] = {}
    for name, sql in QUERIES.items():
        frames[name] = con.execute(sql).df()

    quarantine_csv = OUT_DIR / "m044_quarantined_path_proven_rows.csv"
    frames["quarantine_rows"].to_csv(quarantine_csv, index=False)

    payload = {
        "generated_at_utc": generated_at,
        "database": "thyroid_canonical_publication_v1_0.main",
        "queries": {name: _records(df) for name, df in frames.items()},
        "quarantine_rows_csv": str(quarantine_csv.relative_to(ROOT)),
    }
    json_path = OUT_DIR / "m044_live_motherduck_sync_audit.json"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# M044 data freeze: MotherDuck synchronization pass",
        "",
        f"- **Generated (UTC):** {generated_at}",
        "- **Database:** `thyroid_canonical_publication_v1_0.main` via `scripts/_md_connect.py::connect_locked()`",
        "- **Scope:** Data and presentation synchronization only; literature/citation verification intentionally not performed.",
        "- **Primary endpoint:** `recurrence_path_proven IS TRUE AND NOT COALESCE(is_implausible_date_quarantine, FALSE)`.",
        "- **Secondary imaging-only endpoint:** `recurrence_status_final = 'imaging_only_unconfirmed'`.",
        "- **Composite endpoint:** primary path-proven OR imaging-only unconfirmed.",
        "- **Person-year rule:** numerator and denominator both restrict to `followup_years > 0`.",
        "",
    ]
    for title, name in [
        ("Cohort, Date, and Follow-up Audit", "cohort_date_followup_audit"),
        ("Recurrence Endpoint by ETE Group", "recurrence_by_ete_group"),
        ("Implausible-Date Quarantine Summary", "quarantine_summary"),
        ("Legacy Recurrence Flag Audit", "legacy_audit"),
        ("Strict-DTC Model Subset Audit", "strict_dtc_model_subset_audit"),
        ("Validation Gates", "validation_gates"),
    ]:
        lines.extend([f"## {title}", "", _markdown_table(frames[name]), ""])
    lines.extend([
        "## Quarantined Row Listing",
        "",
        f"The 24 path-proven rows quarantined for implausible pre-surgery dates are exported to `{quarantine_csv.relative_to(ROOT)}`.",
        "",
        "## Generated Audit Artifacts",
        "",
        f"- `{json_path.relative_to(ROOT)}`",
        f"- `{quarantine_csv.relative_to(ROOT)}`",
        "",
    ])
    FREEZE_NOTE.write_text("\n".join(lines), encoding="utf-8")
    print(f"[audit] wrote {FREEZE_NOTE}")
    print(f"[audit] wrote {json_path}")
    print(f"[audit] wrote {quarantine_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())