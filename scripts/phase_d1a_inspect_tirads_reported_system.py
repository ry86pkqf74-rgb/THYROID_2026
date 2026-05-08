"""Phase D.1.a — Inspect existing tirads_reported_system column.

Run BEFORE the CTAS rebuild to capture baseline distribution numbers.
Results are printed and can be copy-pasted into the DFL / THY-30 comment.
"""
from __future__ import annotations
import json
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
client = bigquery.Client(project=PROJECT)


def q(sql: str, label: str) -> list[dict]:
    print(f"\n=== {label} ===")
    rows = [dict(r) for r in client.query(sql).result()]
    for r in rows:
        print(" ", r)
    return rows


# ── 1. Per-system distribution with counts ──────────────────────────────────
dist = q(
    """
    SELECT
      tirads_reported_system,
      COUNT(*) AS n,
      COUNTIF(tirads_reported_in_text IS NOT NULL) AS n_with_reported_value,
      COUNTIF(exam_date >= '2017-01-01') AS n_post_2017,
      ROUND(AVG(EXTRACT(YEAR FROM exam_date)), 1) AS mean_year
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2`
    GROUP BY 1
    ORDER BY 2 DESC
    """,
    "D.1.a — tirads_reported_system distribution",
)

# ── 2. Pre/post-2017 breakdown for nulls vs text-present ────────────────────
pre_post = q(
    """
    SELECT
      CASE WHEN tirads_reported_in_text IS NULL THEN 'no_tr_text' ELSE 'has_tr_text' END AS tr_text_flag,
      CASE WHEN exam_date >= '2017-01-01' THEN 'post_2017' ELSE 'pre_2017' END AS era,
      COUNT(*) AS n
    FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2`
    GROUP BY 1, 2
    ORDER BY 3 DESC
    """,
    "D.1.a — tirads_reported_in_text × era cross-tab",
)

# ── 3. Check if validated columns already exist ──────────────────────────────
col_check = q(
    """
    SELECT column_name
    FROM `thyroid-canonical-pub-2026.pub_canonical.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'canonical_us_nodule_v2'
      AND column_name IN ('tirads_reported_system_validated', 'tirads_reported_system_inference_method')
    """,
    "D.1.a — existing validated columns check",
)

print("\n\n=== SUMMARY FOR DFL / THY-30 COMMENT ===")
print(json.dumps({"dist": dist, "pre_post_breakdown": pre_post, "validated_cols_exist": col_check}, indent=2, default=str))
