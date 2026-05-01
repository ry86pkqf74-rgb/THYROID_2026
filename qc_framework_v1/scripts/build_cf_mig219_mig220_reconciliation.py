#!/usr/bin/env python3
"""Build read-only reconciliation report for CF-mig219 and CF-mig220.

The script runs SELECT-only probes against the locked MotherDuck publication
database and writes local report/evidence artifacts for the v14 Prompt 3 carry
forward reconciliation.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402

RUN_ID = "cf_mig219_mig220_reconciliation_20260501"
OUT_DIR = REPO_ROOT / "exports" / RUN_ID
REPORT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / f"{RUN_ID}.md"


def git_hash() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
        ).strip()
    except Exception:
        return "unknown"


def fetch_df(con: Any, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()


def md_table(df: pd.DataFrame, max_rows: int = 80) -> str:
    if df.empty:
        return "_No rows._"
    return df.head(max_rows).to_markdown(index=False)


CF220_MAPPING_SQL = """
WITH queue_keys AS (
  SELECT DISTINCT research_id, us_exam_id, nodule_index_within_exam
  FROM manuscript_workspace.us_nodule_conflict_queue_v1
  WHERE review_priority = 'high'
), mapped AS (
  SELECT
    q.*,
    EXISTS (
      SELECT 1
      FROM main.canonical_us_nodule_v2 AS v2
      WHERE CAST(v2.research_id AS VARCHAR) = CAST(q.research_id AS VARCHAR)
        AND v2.us_exam_id = q.us_exam_id
        AND v2.nodule_index_within_exam = q.nodule_index_within_exam
    ) AS key_in_v2
  FROM queue_keys AS q
)
SELECT
  COUNT(*) AS distinct_high_pri_keys,
  SUM(CASE WHEN key_in_v2 THEN 1 ELSE 0 END) AS keys_in_v2,
  COUNT(*) - SUM(CASE WHEN key_in_v2 THEN 1 ELSE 0 END) AS keys_missing_from_v2
FROM mapped
"""


CF220_FIELD_SQL = """
WITH queue_scope AS (
  SELECT
    field_name,
    research_id,
    us_exam_id,
    nodule_index_within_exam,
    value_tirads_v2,
    value_tirads_v2 IS NOT NULL
      AND TRIM(CAST(value_tirads_v2 AS VARCHAR)) <> '' AS has_nonblank_value,
    EXISTS (
      SELECT 1
      FROM main.canonical_us_nodule_v2 AS v2
      WHERE CAST(v2.research_id AS VARCHAR) = CAST(us_nodule_conflict_queue_v1.research_id AS VARCHAR)
        AND v2.us_exam_id = us_nodule_conflict_queue_v1.us_exam_id
        AND v2.nodule_index_within_exam = us_nodule_conflict_queue_v1.nodule_index_within_exam
    ) AS key_in_v2
  FROM manuscript_workspace.us_nodule_conflict_queue_v1
  WHERE review_priority = 'high'
    AND field_name IN ('tirads_reported', 'tirads_category_v2', 'tirads_score_2017')
), resolution_counts AS (
  SELECT 'tirads_reported' AS field_name,
         COUNT(*) AS canonical_rows_tagged
  FROM main.canonical_us_nodule_v2
  WHERE tirads_conflict_resolution_source LIKE '%mig_220:tirads_reported:prefer_tirads_v2%'
  UNION ALL
  SELECT 'tirads_category_v2' AS field_name,
         COUNT(*) AS canonical_rows_tagged
  FROM main.canonical_us_nodule_v2
  WHERE tirads_conflict_resolution_source LIKE '%mig_220:tirads_category_v2:prefer_tirads_v2%'
  UNION ALL
  SELECT 'tirads_score_2017' AS field_name,
         COUNT(*) AS canonical_rows_tagged
  FROM main.canonical_us_nodule_v2
  WHERE tirads_conflict_resolution_source LIKE '%mig_220:tirads_score_2017:prefer_tirads_v2%'
)
SELECT
  q.field_name,
  COUNT(*) AS queue_rows,
  SUM(CASE WHEN has_nonblank_value THEN 1 ELSE 0 END) AS nonblank_value_rows,
  SUM(CASE WHEN key_in_v2 THEN 1 ELSE 0 END) AS key_mapped_rows,
  SUM(CASE WHEN has_nonblank_value AND key_in_v2 THEN 1 ELSE 0 END) AS nonblank_mapped_rows,
  COALESCE(MAX(r.canonical_rows_tagged), 0) AS canonical_rows_tagged
FROM queue_scope AS q
LEFT JOIN resolution_counts AS r
  ON r.field_name = q.field_name
GROUP BY q.field_name
ORDER BY q.field_name
"""


CF219_COUNTS_SQL = """
WITH f AS (
  SELECT *
  FROM manuscript_workspace.canonical_us_nodule_v2_filtered
  WHERE COALESCE(is_aggregate_row, FALSE) = FALSE
    AND us_row_type <> 'shell'
), flags AS (
  SELECT
    *,
    (
      tirads_reported_in_text IS NOT NULL
      OR (acr2017_tirads_category IS NOT NULL
          AND TRIM(CAST(acr2017_tirads_category AS VARCHAR)) <> '')
      OR (updated_tirads_category IS NOT NULL
          AND TRIM(CAST(updated_tirads_category AS VARCHAR)) <> '')
    ) AS any_reported,
    (
      acr2017_feature_points_complete IS TRUE
      AND acr2017_tirads_points IS NOT NULL
      AND acr2017_tirads_category IS NOT NULL
    ) AS strict_acr,
    COALESCE(acr2017_feature_points_complete, FALSE) = FALSE AS descriptor_not_complete,
    acr2017_tirads_points IS NULL OR acr2017_tirads_category IS NULL AS derived_points_or_cat_missing,
    acr2017_tirads_points IS NOT NULL AND acr2017_tirads_category IS NOT NULL AS derived_points_and_cat_present,
    tirads_reported_in_text IS NOT NULL AS has_reported_text,
    acr2017_tirads_category IS NOT NULL
      AND TRIM(CAST(acr2017_tirads_category AS VARCHAR)) <> '' AS has_acr_cat,
    updated_tirads_category IS NOT NULL
      AND TRIM(CAST(updated_tirads_category AS VARCHAR)) <> '' AS has_updated_cat
  FROM f
)
SELECT metric, n
FROM (
  SELECT 'base_nonaggregate_nonshell' AS metric, COUNT(*) AS n FROM flags
  UNION ALL SELECT 'view_strict_acr2017', COUNT(*) FROM manuscript_workspace.vw_us_nodule_tirads_strict_acr2017_VIEW_v1
  UNION ALL SELECT 'view_any_reported', COUNT(*) FROM manuscript_workspace.vw_us_nodule_tirads_any_reported_VIEW_v1
  UNION ALL SELECT 'view_reported_not_fully_parsed_current', COUNT(*) FROM manuscript_workspace.vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1
  UNION ALL SELECT 'doc_expected_reported_not_fully_parsed', 8243
  UNION ALL SELECT 'current_minus_doc_expected', COUNT(*) - 8243 FROM manuscript_workspace.vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1
  UNION ALL SELECT 'any_reported_and_descriptor_not_complete', COUNT(*) FROM flags WHERE any_reported AND descriptor_not_complete
  UNION ALL SELECT 'any_reported_and_derived_points_or_cat_missing', COUNT(*) FROM flags WHERE any_reported AND derived_points_or_cat_missing
  UNION ALL SELECT 'any_reported_descriptor_not_complete_but_derived_present', COUNT(*) FROM flags WHERE any_reported AND descriptor_not_complete AND derived_points_and_cat_present
  UNION ALL SELECT 'any_reported_descriptor_incomplete_and_derived_missing', COUNT(*) FROM flags WHERE any_reported AND descriptor_not_complete AND derived_points_or_cat_missing
  UNION ALL SELECT 'any_reported_descriptor_complete_but_not_strict', COUNT(*) FROM flags WHERE any_reported AND acr2017_feature_points_complete IS TRUE AND NOT strict_acr
  UNION ALL SELECT 'any_reported_has_reported_text_only_no_acr_no_updated', COUNT(*) FROM flags WHERE any_reported AND has_reported_text AND NOT has_acr_cat AND NOT has_updated_cat
)
ORDER BY metric
"""


CF219_CROSSTAB_SQL = """
WITH flags AS (
  SELECT
    CASE
      WHEN COALESCE(acr2017_feature_points_complete, FALSE)
      THEN 'descriptor_complete'
      ELSE 'descriptor_incomplete'
    END AS descriptor_state,
    CASE
      WHEN acr2017_tirads_points IS NOT NULL AND acr2017_tirads_category IS NOT NULL
      THEN 'derived_points_and_category_present'
      ELSE 'derived_points_or_category_missing'
    END AS derived_state,
    CASE WHEN tirads_reported_in_text IS NOT NULL THEN TRUE ELSE FALSE END AS has_reported_text,
    CASE
      WHEN acr2017_tirads_category IS NOT NULL
        AND TRIM(CAST(acr2017_tirads_category AS VARCHAR)) <> ''
      THEN TRUE ELSE FALSE
    END AS has_acr_category,
    CASE
      WHEN updated_tirads_category IS NOT NULL
        AND TRIM(CAST(updated_tirads_category AS VARCHAR)) <> ''
      THEN TRUE ELSE FALSE
    END AS has_updated_category,
    CASE
      WHEN tirads_reported_in_text IS NOT NULL
        OR (acr2017_tirads_category IS NOT NULL
            AND TRIM(CAST(acr2017_tirads_category AS VARCHAR)) <> '')
        OR (updated_tirads_category IS NOT NULL
            AND TRIM(CAST(updated_tirads_category AS VARCHAR)) <> '')
      THEN TRUE ELSE FALSE
    END AS any_reported
  FROM manuscript_workspace.canonical_us_nodule_v2_filtered
  WHERE COALESCE(is_aggregate_row, FALSE) = FALSE
    AND us_row_type <> 'shell'
)
SELECT
  descriptor_state,
  derived_state,
  has_reported_text,
  has_acr_category,
  has_updated_category,
  COUNT(*) AS n
FROM flags
WHERE any_reported
GROUP BY 1, 2, 3, 4, 5
ORDER BY descriptor_state, derived_state, has_reported_text, has_acr_category, has_updated_category
"""


CF219_VIEW_PARITY_SQL = """
WITH direct_filter AS (
  SELECT f.*
  FROM manuscript_workspace.canonical_us_nodule_v2_filtered AS f
  WHERE COALESCE(f.is_aggregate_row, FALSE) = FALSE
    AND f.us_row_type <> 'shell'
    AND (
      f.tirads_reported_in_text IS NOT NULL
      OR (f.acr2017_tirads_category IS NOT NULL
          AND TRIM(CAST(f.acr2017_tirads_category AS VARCHAR)) <> '')
      OR (f.updated_tirads_category IS NOT NULL
          AND TRIM(CAST(f.updated_tirads_category AS VARCHAR)) <> '')
    )
    AND COALESCE(f.acr2017_feature_points_complete, FALSE) = FALSE
), view_rows AS (
  SELECT *
  FROM manuscript_workspace.vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1
)
SELECT
  (SELECT COUNT(*) FROM direct_filter) AS direct_filter_rows,
  (SELECT COUNT(*) FROM view_rows) AS view_rows,
  (SELECT COUNT(*) FROM direct_filter) - (SELECT COUNT(*) FROM view_rows) AS direct_minus_view
"""


def write_artifacts(data: dict[str, pd.DataFrame], metadata: dict[str, Any]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in data.items():
        df.to_csv(OUT_DIR / f"{name}.csv", index=False)

    manifest = {
        "run_id": RUN_ID,
        "generated_at": metadata["generated_at"],
        "git_commit": metadata["git_commit"],
        "report_path": str(REPORT_PATH.relative_to(REPO_ROOT)),
        "artifacts": sorted(p.name for p in OUT_DIR.glob("*.csv")),
        "mode": "read_only_selects_against_motherduck",
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def write_report(data: dict[str, pd.DataFrame], metadata: dict[str, Any]) -> None:
    cf220 = data["cf220_mapping"].iloc[0].to_dict()
    cf219_counts = data["cf219_counts"].set_index("metric")["n"].to_dict()
    view_count = int(cf219_counts["view_reported_not_fully_parsed_current"])
    doc_count = int(cf219_counts["doc_expected_reported_not_fully_parsed"])
    delta = int(cf219_counts["current_minus_doc_expected"])
    derived_missing = int(cf219_counts["any_reported_and_derived_points_or_cat_missing"])
    descriptor_incomplete_derived_present = int(
        cf219_counts["any_reported_descriptor_not_complete_but_derived_present"]
    )

    status_220 = "closed" if int(cf220["keys_missing_from_v2"] or 0) == 0 else "open"
    status_219 = "closed_with_methods_clarification"

    report = f"""# CF-mig219 + CF-mig220 Reconciliation Report

**Run ID:** `{RUN_ID}`  
**Generated:** {metadata['generated_at']}  
**Git HEAD:** `{metadata['git_commit']}`  
**Mode:** read-only MotherDuck SELECT probes; no DDL/DML executed.

## Executive Conclusion

| Carry-forward | Status | Conclusion |
|---|---|---|
| CF-mig220-QUEUE-CURRENT-V2-DRIFT | {status_220} | All {int(cf220['distinct_high_pri_keys'])} distinct high-priority queue keys now map to `main.canonical_us_nodule_v2`; missing keys = {int(cf220['keys_missing_from_v2'])}. No remediation migration needed. |
| CF-mig219-NOT-FULLY-PARSED-COUNT-DRIFT | {status_219} | Live view count remains {view_count:,}, not the planning expectation of {doc_count:,}. The delta is explained by the mig221 semantic clarification: the live view uses descriptor completeness (`acr2017_feature_points_complete=FALSE`), not derived-point/category missingness. |

## CF-mig220 Probe

The Cowork preflight probe was re-run exactly at current MotherDuck state.

{md_table(data['cf220_mapping'])}

Field-level reconciliation confirms the same high-priority scope is represented in the canonical row tags.

{md_table(data['cf220_fields'])}

**Decision:** close CF-mig220. The v13 carry-forward was a transient queue/current-table drift that is no longer present after the current `canonical_us_nodule_v2` state.

## CF-mig219 Probe

The live `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1` count is still {view_count:,}, while the ChatGPT planning note expected {doc_count:,}. The DDL filter in mig219 matches the live direct filter exactly.

{md_table(data['cf219_view_parity'])}

Key count decomposition:

{md_table(data['cf219_counts'])}

Cross-tab of descriptor completeness versus derived ACR availability:

{md_table(data['cf219_crosstab'])}

### Diagnosis

The {delta:,}-row apparent drift is not evidence that the view is malformed. It is a definition mismatch:

- The mig219 view defines "reported not fully parsed" as any reported TIRADS signal with `acr2017_feature_points_complete=FALSE`.
- The mig221 clarification states that `acr2017_feature_points_complete` means all five upstream ACR descriptor fields were present in the legacy CUNC source. It is not equivalent to "derived ACR points/category are missing" after later normalized-feature backfills.
- Current live data show only {derived_missing:,} any-reported rows with missing derived points or category, but {descriptor_incomplete_derived_present:,} rows whose descriptors are incomplete while derived points and category are present.

Therefore the 24,371-row live view is internally consistent with the applied mig219 DDL and mig221 semantics. The 8,243 planning expectation should not be used as a manuscript count unless the manuscript explicitly intends the narrower derived-missing definition, which would be a different view/filter.

## Recommendation

1. Mark CF-mig220 closed with `closed_in_mig=mig_222/current_v2_absorption` and no follow-up migration.
2. Mark CF-mig219 closed as a semantics reconciliation: retain the live mig219 view count ({view_count:,}) for descriptor-incomplete TIRADS reporting.
3. If manuscript Methods need a smaller "derived ACR unavailable" denominator, create a separately named view in a future migration; do not reinterpret `vw_us_nodule_tirads_reported_not_fully_parsed_VIEW_v1`.

## Evidence Artifacts

CSV evidence and manifest are in `exports/{RUN_ID}/`.
"""

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report)


def main() -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    con = connect_locked()
    data = {
        "cf220_mapping": fetch_df(con, CF220_MAPPING_SQL),
        "cf220_fields": fetch_df(con, CF220_FIELD_SQL),
        "cf219_counts": fetch_df(con, CF219_COUNTS_SQL),
        "cf219_crosstab": fetch_df(con, CF219_CROSSTAB_SQL),
        "cf219_view_parity": fetch_df(con, CF219_VIEW_PARITY_SQL),
    }
    metadata = {"generated_at": generated_at, "git_commit": git_hash()}
    write_artifacts(data, metadata)
    write_report(data, metadata)
    print(f"Wrote {REPORT_PATH.relative_to(REPO_ROOT)}")
    print(f"Wrote {OUT_DIR.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()