"""Surface has_op distribution + lab-row totals for the 403 orphan rids.

Read-only.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

con = connect_locked()

stats = con.execute(
    """
    WITH cls AS (
      SELECT CAST(research_id AS BIGINT) AS rid,
             classification, n_lab_rows
      FROM manuscript_workspace.lab_orphan_audit_v1
    ),
    rev AS (
      SELECT research_id AS rid, has_op, n_tg_rows, n_long_lab_rows,
             first_tg_dt, last_tg_dt
      FROM manuscript_workspace.lab_orphan_cohort_review_v1
    )
    SELECT
      COUNT(*) AS n_total,
      SUM(CASE WHEN rev.has_op THEN 1 ELSE 0 END) AS n_has_op,
      SUM(CASE WHEN NOT rev.has_op THEN 1 ELSE 0 END) AS n_no_op,
      SUM(CASE WHEN rev.has_op IS NULL THEN 1 ELSE 0 END) AS n_op_null,
      SUM(cls.n_lab_rows) AS sum_lab_rows_across_orphans,
      SUM(rev.n_tg_rows) AS sum_tg_rows,
      SUM(rev.n_long_lab_rows) AS sum_long_lab_rows
    FROM cls JOIN rev USING (rid)
    """
).fetchone()
keys = [d[0] for d in con.description]
result = dict(zip(keys, stats))

# Total lab-row counts in the source tables (for context: how many rows
# would be deleted vs total)
total_tg = con.execute(
    "SELECT COUNT(*) FROM main.thyroglobulin_lab_canonical_v1"
).fetchone()[0]
total_long = con.execute(
    "SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1"
).fetchone()[0]

# How many lab rows actually correspond to the 403 orphan rids?
n_tg_orphan = con.execute(
    """
    SELECT COUNT(*) FROM main.thyroglobulin_lab_canonical_v1
    WHERE research_id IN (
      SELECT CAST(research_id AS BIGINT) FROM manuscript_workspace.lab_orphan_audit_v1
    )
    """
).fetchone()[0]
n_long_orphan = con.execute(
    """
    SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1
    WHERE research_id IN (
      SELECT CAST(research_id AS BIGINT) FROM manuscript_workspace.lab_orphan_audit_v1
    )
    """
).fetchone()[0]

result["total_thyroglobulin_lab_canonical_v1_rows"] = total_tg
result["total_longitudinal_lab_canonical_v1_rows"] = total_long
result["thyroglobulin_lab_rows_belonging_to_403_orphans"] = n_tg_orphan
result["longitudinal_lab_rows_belonging_to_403_orphans"] = n_long_orphan
result["pct_tg_rows_orphan"] = round(100 * n_tg_orphan / total_tg, 2) if total_tg else None
result["pct_long_rows_orphan"] = round(100 * n_long_orphan / total_long, 2) if total_long else None

print(json.dumps(result, indent=2, default=str))
