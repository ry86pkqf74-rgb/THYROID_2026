#!/usr/bin/env python3
"""
365c — Backfill procedure_normalized_trusted, OP01/OP02 flags, and surgery dates
for 2023–2025 (and pathology pollution handling) in BigQuery pub_workspace.

Upstream context (read first):
  - scripts/362_operative_consolidation.py — builds main.canonical_operative_events_v1
    from operative_episode_detail_v2 (+ note_entities enrichment).
  - scripts/341_rebuild_operative_episode_multi_v2.py — multi-episode OED rebuild;
    authoritative dates from CPM first/second/third_surgery_date_v2.
  - qc_framework_v1/migrations/46_op01_op02_procedure_laterality_rules.sql — defines
    op01_tt_unilateral_flag, op02_hemi_bilateral_flag, procedure_normalized_trusted.
  - scripts/prompt6_355_fna_canonical_master.py — fna_date_resolved precedence pattern
    (COALESCE primary → secondary → tertiary sources + status/confidence).

notes_extraction_new/operative/ — not present in this repo; operative SSOT for this
script is ``pub_canonical.canonical_operative_events_v1`` in BigQuery.

PHI: Do not log raw procedure text at scale; samples in the markdown report are
truncated to 120 chars. research_id + normalized labels only in stdout.

Usage
-----
  export GOOGLE_APPLICATION_CREDENTIALS=...   # or gcloud auth application-default login
  .venv/bin/python scripts/ops/365c_procedure_normalize_2023_2025.py --dry-run
  .venv/bin/python scripts/ops/365c_procedure_normalize_2023_2025.py --apply

Optional:
  --refresh-manuscript-cohort-dates   NULL-fill surgery_date / first_surgery_date /
  --refresh-rollup                      rebuild canonical_operative_patient_rollup_v1
  --refresh-surgery-reconciled          rebuild manuscript_cohort_v1_surgery_reconciled
  --report-path docs/procedure_normalized_2023_2025_backfill_2026-05-08.md

Does **not** mutate ``pub_canonical.canonical_operative_events_v1`` (additive policy:
workspace copy is refreshed from it).
"""
from __future__ import annotations

import argparse
import re
from datetime import date, datetime, timezone
from typing import Any, Optional

import pandas as pd
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
DATASET_CANONICAL = "pub_canonical"
DATASET_WORKSPACE = "pub_workspace"
LOCATION = "us-central1"

EVENTS_TABLE = "canonical_operative_events_v1"
RULE_CLEAN_TABLE = "canonical_operative_events_v1_rule_clean"
ROLLUP_TABLE = "canonical_operative_patient_rollup_v1"
MCOHORT_TABLE = "manuscript_cohort_v1"
SURG_RECON_TABLE = "manuscript_cohort_v1_surgery_reconciled"

SCRIPT_TAG = "365c_procedure_normalize_2023_2025"

# Ingest-format v2: additional BigQuery column names that may hold procedure text
# if procedure_raw was mapped incorrectly in a past load.
PROCEDURE_RAW_ALIASES: tuple[str, ...] = (
    "procedure_raw",
    "thyroid_procedure",
    "thyroid_procedure_raw",
    "operative_procedure_raw",
    "procedure_text",
    "op_procedure_raw",
    "thyroid_procedure_name",
)

ANCHOR_EVENTS_FOR_PROC = (
    "https://github.com/THYROID_2026 — scripts/362_operative_consolidation.py "
    f"+ {SCRIPT_TAG}"
)


# ---------------------------------------------------------------------------
# Procedure + pathology logic
# ---------------------------------------------------------------------------

_PATH_TERMS = re.compile(
    r"(papillary|follicular|medullary|hurthle|hürthle|anaplastic|insular)\b",
    re.I,
)
_CARCINOMA_ADENOMA = re.compile(
    r"\b(carcinoma|adenoma|niftp|niftp|mtc|ptc|ftc|atc|pdtc)\b",
    re.I,
)
_PATH_GENERIC = re.compile(
    r"\b(bethesda|aus\b|flus\b|fnh|multinodular\s*goiter|"
    r"colloid\s*cyst|thyroid\s*cyst|metastatic|recurrent)\b",
    re.I,
)


LATERALITY_ALIASES: tuple[str, ...] = (
    "laterality",
    "operative_laterality",
    "specimen_laterality",
)


def coalesce_laterality(row: pd.Series) -> Any:
    for c in LATERALITY_ALIASES:
        if c in row.index and pd.notna(row[c]):
            return row[c]
    return None


def coalesce_procedure_raw(row: pd.Series) -> str:
    """Resolve procedure string from canonical row (handles aliased column names)."""
    for c in PROCEDURE_RAW_ALIASES:
        if c in row.index and pd.notna(row[c]):
            s = str(row[c]).strip()
            if s and s.lower() not in {"nan", "none", "null"}:
                return s
    return ""


def is_pathology_pollution(text: str) -> bool:
    if not text or len(text) < 3:
        return False
    tl = text.lower()
    if "fna" in tl and "biopsy" not in tl:
        # FNA mention alone doesn't make it pathology pollution; allow heuristics below
        pass
    if _PATH_TERMS.search(text) or _CARCINOMA_ADENOMA.search(text):
        return True
    if _PATH_GENERIC.search(text):
        return True
    if "thyroid carcinoma" in tl or "thyroid cancer" in tl:
        return True
    return False


def classify_procedure(procedure_text: str) -> Optional[str]:
    """
    Return canonical procedure_normalized_trusted label, or None if not classifiable.
    Ordering: completion > total > hemi > isthmus > biopsy > (other if plausible).
    """
    if not procedure_text:
        return None
    t = procedure_text.strip().lower()
    # strip trailing semicolons from synoptic dumps
    t = re.sub(r";+\s*$", "", t).strip()

    if re.search(r"\bfna\b", t) and "biopsy" not in t:
        return None  # fine-needle aspiration — not surgical biopsy bucket

    completion = "completion" in t and (
        "thyroidectomy" in t or "thyroid" in t or "lobectomy" in t
    )
    if completion:
        return "completion_thyroidectomy"

    if any(
        x in t
        for x in (
            "total thyroidectomy",
            "complete thyroidectomy",
            "bilateral thyroidectomy",
        )
    ) or re.search(r"\btt\b", t):
        return "total_thyroidectomy"

    if "hemithyroidectomy" in t or "thyroid lobectomy" in t or "lobectomy" in t:
        if "completion" in t:
            return "completion_thyroidectomy"
        if "left lobe" in t or "right lobe" in t or "left thyroid" in t or "right thyroid" in t:
            return "hemithyroidectomy"
        if "lobectomy" in t:
            return "hemithyroidectomy"
    if "unilateral thyroidectomy" in t:
        return "hemithyroidectomy"

    if "isthmusectomy" in t or "isthmus only" in t or "isthmus excision" in t:
        return "isthmusectomy"

    if "biopsy" in t:
        return "biopsy"

    if len(t) >= 3:
        return "other"
    return None


def norm_laterality(lat: Any) -> str:
    if lat is None or (isinstance(lat, float) and pd.isna(lat)):
        return ""
    s = str(lat).strip().lower()
    return s


def apply_op01_op02_trusted(
    proc: Optional[str], laterality: str
) -> tuple[bool, bool, Optional[str]]:
    """
    Mirrors migration 46:
      OP01: total_thyroidectomy + laterality in {left, right}
      OP02: hemithyroidectomy + laterality == bilateral
    procedure_normalized_trusted forced NULL when flagged.
    """
    lat = norm_laterality(laterality)
    if proc is None:
        return False, False, None
    op01 = proc == "total_thyroidectomy" and lat in {"left", "right"}
    op02 = proc == "hemithyroidectomy" and lat == "bilateral"
    if op01 or op02:
        return op01, op02, None
    return False, False, proc


def parse_date_flexible(val: Any) -> Optional[date]:
    """Parse DATE from STRING / datetime / date / pandas Timestamp."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if hasattr(val, "date") and callable(val.date):
        try:
            d = val.date()
            if isinstance(d, date):
                return d
        except Exception:
            pass
    s = str(val).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    try:
        return pd.to_datetime(s, errors="coerce").date()
    except Exception:
        return None


def resolve_surgery_date(row: pd.Series) -> tuple[Optional[str], str, Optional[int]]:
    """
    FNA-style chain: resolved_surgery_date > surgery_date_native > note_date_resolved.
    Returns (iso_string YYYY-MM-DD, status, confidence 0-100).
    """
    cand: list[tuple[str, date]] = []
    labels = [
        ("resolved_episode", "resolved_surgery_date"),
        ("native", "surgery_date_native"),
        ("note", "note_date_resolved"),
    ]
    for label, col in labels:
        if col not in row.index:
            continue
        d = parse_date_flexible(row[col])
        if d:
            cand.append((label, d))

    if not cand:
        return None, "unresolved_date", 0

    priority = {"resolved_episode": 0, "native": 1, "note": 2}
    cand.sort(key=lambda x: priority[x[0]])
    label, d = cand[0]
    conf_by = {"resolved_episode": 100, "native": 85, "note": 65}
    status_map = {
        "resolved_episode": "resolved_surgery_date_used",
        "native": "surgery_date_native_used",
        "note": "note_date_resolved_used",
    }
    return d.isoformat(), status_map[label], conf_by[label]


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def fq(project: str, dataset: str, table: str) -> str:
    return f"`{project}.{dataset}.{table}`"


def sql_op_resolved_date(col: str = "resolved_surgery_date") -> str:
    """
    BigQuery expression: normalize resolved_surgery_date to DATE whether the
    column is DATE, TIMESTAMP, or STRING (YYYY-MM-DD or MM/DD/YYYY).
    """
    return (
        f"COALESCE("
        f"SAFE_CAST({col} AS DATE), "
        f"SAFE.PARSE_DATE('%Y-%m-%d', NULLIF(TRIM(CAST({col} AS STRING)), '')), "
        f"SAFE.PARSE_DATE('%m/%d/%Y', NULLIF(TRIM(CAST({col} AS STRING)), ''))"
        f")"
    )


def run_sql(client: bigquery.Client, sql: str, label: str) -> None:
    job = client.query(sql, location=LOCATION)
    job.result()
    print(f"  ✓ {label}", flush=True)


def scalar(client: bigquery.Client, sql: str) -> Any:
    rows = list(client.query(sql, location=LOCATION).result())
    return rows[0][0] if rows else None


def table_columns(client: bigquery.Client, project: str, dataset: str, table: str) -> list[str]:
    t = client.get_table(f"{project}.{dataset}.{table}")
    return [f.name for f in t.schema]


def fetch_events_df(client: bigquery.Client, project: str) -> pd.DataFrame:
    sql = f"""
    SELECT *
    FROM {fq(project, DATASET_CANONICAL, EVENTS_TABLE)}
    """
    return client.query(sql, location=LOCATION).to_dataframe()


def enrich_events_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    out = df.copy()
    rows = []
    poll_samples: list[str] = []

    for idx in range(len(out)):
        row = out.iloc[idx]
        proc_raw = coalesce_procedure_raw(row)
        if not proc_raw and "procedure_normalized" in row.index and pd.notna(
            row["procedure_normalized"]
        ):
            proc_raw = str(row["procedure_normalized"]).strip()
        pollution = is_pathology_pollution(proc_raw)
        if pollution:
            parked = proc_raw[:2000]
            classified = None
            if parked not in poll_samples and len(poll_samples) < 40:
                poll_samples.append(parked)
        else:
            parked = None
            classified = classify_procedure(proc_raw)

        lat = coalesce_laterality(row)
        op01, op02, trusted = apply_op01_op02_trusted(classified, lat)

        iso, st, conf = resolve_surgery_date(row)
        # Prefer recomputed date when existing resolved_surgery_date is null or unparseable
        existing_iso = None
        if "resolved_surgery_date" in row.index:
            existing_iso = parse_date_flexible(row["resolved_surgery_date"])
        if iso is not None:
            out_date = iso
        elif existing_iso:
            out_date = existing_iso.isoformat()
            st = st or "prior_resolved_unchanged"
            conf = conf or 100
        else:
            out_date = None

        rows.append(
            {
                "procedure_raw_pathology_pollution": parked,
                "procedure_classified_pre_op_rules": classified,
                "procedure_normalized_trusted": trusted,
                "op01_tt_unilateral_flag": op01,
                "op02_hemi_bilateral_flag": op02,
                "resolved_surgery_date_365c": out_date,
                "surgery_date_status_365c": st,
                "surgery_date_confidence_365c": conf,
            }
        )

    meta = pd.DataFrame(rows, index=out.index)
    merged = pd.concat([out, meta], axis=1)
    # Backfill: prefer recomputed chain; retain legacy string if still unparseable
    merged["resolved_surgery_date"] = merged["resolved_surgery_date_365c"].where(
        merged["resolved_surgery_date_365c"].notna(), merged["resolved_surgery_date"]
    )
    return merged, poll_samples


def local_coverage_markdown(merged: pd.DataFrame) -> str:
    """Pre-BQ validation stats from the in-memory enriched frame."""
    m = merged.copy()
    dt_col = "resolved_surgery_date"
    m["_dt"] = m[dt_col].apply(parse_date_flexible)
    m["_yr"] = m["_dt"].apply(lambda d: d.year if d else None)
    sub = m[m["_yr"].isin([2023, 2024, 2025])]
    denom = len(sub)
    nnz = int(sub["procedure_normalized_trusted"].notna().sum())
    pct = round(100.0 * nnz / denom, 2) if denom else 0.0
    lines = [
        "### Local (pandas) coverage 2023–2025",
        "",
        f"- Rows with surgery year in 2023–2025: **{denom}**",
        f"- Non-NULL `procedure_normalized_trusted`: **{nnz}** ({pct}%)",
        "",
        "Procedure trusted distribution (all years):",
        "",
    ]
    vc = (
        m["procedure_normalized_trusted"]
        .fillna("_NULL_")
        .value_counts()
        .head(25)
    )
    lines.append("```")
    lines.append(vc.to_string())
    lines.append("```")
    return "\n".join(lines)


def build_rule_clean_replace_table_sql(project: str, job_id: str) -> str:
    """Load from temp table _365c_rule_clean_staging_{job_id} — created via load job."""
    staging = f"`{project}.{DATASET_WORKSPACE}._365c_rule_clean_staging_{job_id}`"
    dest = fq(project, DATASET_WORKSPACE, RULE_CLEAN_TABLE)
    return f"""
CREATE OR REPLACE TABLE {dest}
OPTIONS(description="Rule-clean operative events; rebuilt by {SCRIPT_TAG}")
AS
SELECT * FROM {staging}
"""


def alter_add_columns_if_missing(
    client: bigquery.Client, project: str, existing: list[str]
) -> None:
    """Best-effort ADD COLUMN for audit fields (ignored if already present)."""
    want = [
        ("procedure_raw_pathology_pollution", "STRING"),
        ("procedure_normalized_trusted", "STRING"),
        ("op01_tt_unilateral_flag", "BOOL"),
        ("op02_hemi_bilateral_flag", "BOOL"),
        ("surgery_date_status_365c", "STRING"),
        ("surgery_date_confidence_365c", "INT64"),
        ("procedure_classified_pre_op_rules", "STRING"),
        ("canonical_operative_ingest_note", "STRING"),
    ]
    for col, typ in want:
        if col not in existing:
            sql = (
                f"ALTER TABLE {fq(project, DATASET_WORKSPACE, RULE_CLEAN_TABLE)} "
                f"ADD COLUMN IF NOT EXISTS {col} {typ}"
            )
            try:
                run_sql(client, sql, f"ALTER ADD {col}")
            except Exception as exc:
                print(f"  (non-fatal) ALTER {col}: {exc}")


def qa_distribution_sql(project: str) -> dict[str, str]:
    rc = fq(project, DATASET_WORKSPACE, RULE_CLEAN_TABLE)
    d_expr = sql_op_resolved_date()
    pmh = fq(project, DATASET_CANONICAL, "canonical_pmh_patient_rollup_v1")
    return {
        "by_year_trusted": f"""
SELECT
  EXTRACT(YEAR FROM {d_expr}) AS yr,
  procedure_normalized_trusted,
  COUNT(*) AS n
FROM {rc}
GROUP BY 1, 2
ORDER BY 1, 3 DESC
""",
        "coverage_2023_2025": f"""
SELECT
  COUNTIF(
    EXTRACT(YEAR FROM {d_expr}) IN (2023, 2024, 2025)
    AND procedure_normalized_trusted IS NOT NULL
  ) AS nnz,
  COUNTIF(
    EXTRACT(YEAR FROM {d_expr}) IN (2023, 2024, 2025)
  ) AS denom,
  ROUND(100 * COUNTIF(
    EXTRACT(YEAR FROM {d_expr}) IN (2023, 2024, 2025)
    AND procedure_normalized_trusted IS NOT NULL
  ) / NULLIF(COUNTIF(
    EXTRACT(YEAR FROM {d_expr}) IN (2023, 2024, 2025)
  ), 0), 2) AS pct_nonnull
FROM {rc}
""",
        "proc_year_counts": f"""
SELECT
  EXTRACT(YEAR FROM {d_expr}) AS yr,
  COUNTIF(procedure_normalized_trusted = 'total_thyroidectomy') AS n_tt,
  COUNTIF(procedure_normalized_trusted = 'hemithyroidectomy') AS n_hemi,
  COUNTIF(procedure_normalized_trusted = 'completion_thyroidectomy') AS n_comp
FROM {rc}
WHERE EXTRACT(YEAR FROM {d_expr}) BETWEEN 1999 AND 2025
GROUP BY 1
ORDER BY 1
""",
        "anchor_source_pmh_rollup": f"""
SELECT
  anchor_source,
  COUNT(*) AS n,
  ROUND(100 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER(), 0), 2) AS pct
FROM {pmh}
GROUP BY 1
ORDER BY n DESC
""",
        "anchor_proxy": f"""
WITH strict_d AS (
  SELECT DISTINCT
    CAST(research_id AS STRING) AS research_id,
    procedure_normalized_trusted AS proc_t,
    {d_expr} AS d
  FROM {rc}
  WHERE procedure_normalized_trusted IS NOT NULL
    AND (
      LOWER(CAST(procedure_normalized_trusted AS STRING)) LIKE '%thyroidect%'
    )
    AND {d_expr} IS NOT NULL
),
per_pt AS (
  SELECT research_id, MIN(d) AS strict_dt
  FROM strict_d
  GROUP BY 1
),
cpm AS (
  SELECT
    CAST(research_id AS STRING) AS research_id,
    first_surgery_date AS cpm_fs
  FROM {fq(project, DATASET_CANONICAL, "canonical_patient_master")}
)
SELECT
  COUNTIF(c.cpm_fs IS NOT NULL) AS n_cpm,
  COUNTIF(p.strict_dt IS NOT NULL) AS n_strict,
  COUNTIF(p.strict_dt IS NOT NULL AND CAST(p.strict_dt AS DATE) = CAST(c.cpm_fs AS DATE))
    AS n_strict_matches_cpm_day,
  ROUND(100 * COUNTIF(p.strict_dt IS NOT NULL) / NULLIF(COUNTIF(c.cpm_fs IS NOT NULL), 0), 2)
    AS pct_strict_vs_cpm
FROM cpm c
LEFT JOIN per_pt p ON p.research_id = c.research_id
""",
        "surgery_reconciled_year": f"""
SELECT
  EXTRACT(YEAR FROM surgery_date_canonical) AS yr,
  COUNT(*) AS n_patients
FROM {fq(project, DATASET_WORKSPACE, SURG_RECON_TABLE)}
GROUP BY 1
ORDER BY 1
""",
    }


def surgery_reconciled_year_sql(project: str) -> str:
    return qa_distribution_sql(project)["surgery_reconciled_year"]


def surgery_year_snapshot(client: bigquery.Client, project: str) -> pd.DataFrame:
    try:
        return client.query(surgery_reconciled_year_sql(project), location=LOCATION).to_dataframe()
    except Exception:
        return pd.DataFrame()


def refresh_manuscript_cohort_null_dates(
    client: bigquery.Client, project: str
) -> None:
    """Fill NULL surgery columns from MIN operative resolved date (conservative)."""
    rc = fq(project, DATASET_WORKSPACE, RULE_CLEAN_TABLE)
    mc = fq(project, DATASET_CANONICAL, MCOHORT_TABLE)
    d_expr = sql_op_resolved_date()
    sql = f"""
    MERGE {mc} AS T
    USING (
      SELECT
        CAST(research_id AS INT64) AS research_id,
        MIN({d_expr}) AS op_min_dt
      FROM {rc}
      WHERE {d_expr} IS NOT NULL
      GROUP BY 1
    ) AS S
    ON SAFE_CAST(T.research_id AS INT64) = S.research_id
    WHEN MATCHED
      AND T.first_surgery_date IS NULL
      AND T.surgery_date IS NULL
      AND T.surg_first_date IS NULL
      AND S.op_min_dt IS NOT NULL
    THEN UPDATE SET
      first_surgery_date = S.op_min_dt,
      surgery_date = S.op_min_dt,
      surg_first_date = S.op_min_dt
    """
    run_sql(client, sql, "MERGE manuscript_cohort_v1 NULL surgery dates from rule_clean")


def build_surgery_reconciled_table(client: bigquery.Client, project: str) -> None:
    """Port of qc_framework_v1/migrations/23_surg01_02_surgery_date_reconcile.sql."""
    mc = fq(project, DATASET_CANONICAL, MCOHORT_TABLE)
    dest = fq(project, DATASET_WORKSPACE, SURG_RECON_TABLE)
    sql = f"""
CREATE OR REPLACE TABLE {dest} AS
WITH base AS (
  SELECT
    c.*,
    (IF(c.surgery_date IS NOT NULL, 1, 0)
      + IF(c.first_surgery_date IS NOT NULL, 1, 0)
      + IF(c.surg_first_date IS NOT NULL, 1, 0)) AS n_populated,
    (c.surgery_date = c.first_surgery_date) AS sd_eq_fsd,
    (c.surgery_date = c.surg_first_date) AS sd_eq_sfd,
    (c.first_surgery_date = c.surg_first_date) AS fsd_eq_sfd
  FROM {mc} c
)
SELECT
  b.* EXCEPT(n_populated, sd_eq_fsd, sd_eq_sfd, fsd_eq_sfd),
  CASE
    WHEN n_populated = 0 THEN NULL
    WHEN n_populated = 3 AND sd_eq_fsd AND fsd_eq_sfd THEN surgery_date
    WHEN n_populated = 3 AND sd_eq_fsd THEN surgery_date
    WHEN n_populated = 3 AND sd_eq_sfd THEN surgery_date
    WHEN n_populated = 3 AND fsd_eq_sfd THEN first_surgery_date
    WHEN n_populated = 3 THEN first_surgery_date
    WHEN n_populated = 2 AND sd_eq_fsd THEN surgery_date
    WHEN n_populated = 2 AND sd_eq_sfd THEN surgery_date
    WHEN n_populated = 2 AND fsd_eq_sfd THEN first_surgery_date
    WHEN n_populated = 2 AND first_surgery_date IS NOT NULL THEN first_surgery_date
    WHEN n_populated = 2 AND surgery_date IS NOT NULL THEN surgery_date
    WHEN n_populated = 2 THEN surg_first_date
    WHEN surgery_date IS NOT NULL THEN surgery_date
    WHEN first_surgery_date IS NOT NULL THEN first_surgery_date
    ELSE surg_first_date
  END AS surgery_date_canonical,
  CASE
    WHEN n_populated = 0 THEN 'all_null'
    WHEN n_populated = 3 AND sd_eq_fsd AND fsd_eq_sfd THEN 'all_three_agree'
    WHEN n_populated = 3 AND (sd_eq_fsd OR sd_eq_sfd OR fsd_eq_sfd) THEN 'consensus_2of3'
    WHEN n_populated = 3 THEN 'all_three_disagree_first_surgery_fallback'
    WHEN n_populated = 2 AND (sd_eq_fsd OR sd_eq_sfd OR fsd_eq_sfd) THEN 'two_agree'
    WHEN n_populated = 2 THEN 'two_disagree_first_surgery_fallback'
    WHEN n_populated = 1 THEN 'single_only'
    ELSE 'all_null'
  END AS surgery_date_source_rank
FROM base b
    """
    run_sql(client, sql, f"CREATE OR REPLACE {SURG_RECON_TABLE}")


def rebuild_rollup_sql(project: str) -> str:
    """
    Rebuild patient rollup using procedure_normalized_trusted from rule_clean when joined.
    Drain lookup omitted (may not exist in BQ) — mirrors Script 362 core surgery counts.
    """
    ev = fq(project, DATASET_CANONICAL, EVENTS_TABLE)
    rc = fq(project, DATASET_WORKSPACE, RULE_CLEAN_TABLE)
    dest = fq(project, DATASET_CANONICAL, ROLLUP_TABLE)
    return f"""
CREATE OR REPLACE TABLE {dest}
OPTIONS(description="Rebuilt by {SCRIPT_TAG}; procedure counts use procedure_normalized_trusted")
AS
WITH joined AS (
  SELECT
    e.*,
    COALESCE(rc.procedure_normalized_trusted,
             SAFE_CAST(e.procedure_normalized AS STRING)) AS proc_eff
  FROM {ev} e
  LEFT JOIN {rc} rc
    ON CAST(e.research_id AS INT64) = CAST(rc.research_id AS INT64)
   AND CAST(e.surgery_episode_id AS INT64) = CAST(rc.surgery_episode_id AS INT64)
)
SELECT
  research_id,
  COUNT(*) AS n_surgeries,
  SUM(CASE WHEN proc_eff = 'total_thyroidectomy' THEN 1 ELSE 0 END)
    AS n_total_thyroidectomies,
  SUM(CASE WHEN proc_eff = 'hemithyroidectomy' THEN 1 ELSE 0 END)
    AS n_hemithyroidectomies,
  SUM(CASE WHEN proc_eff = 'completion_thyroidectomy' THEN 1 ELSE 0 END)
    AS n_completion_thyroidectomies,
  SUM(CASE WHEN COALESCE(central_neck_dissection_flag, FALSE) THEN 1 ELSE 0 END)
    AS n_central_neck_dissections,
  SUM(CASE WHEN COALESCE(lateral_neck_dissection_flag, FALSE) THEN 1 ELSE 0 END)
    AS n_lateral_neck_dissections,
  LOGICAL_OR(COALESCE(reoperative_field_flag, FALSE)) AS any_reoperative_field,
  LOGICAL_OR(COALESCE(parathyroid_autograft_flag, FALSE)) AS any_parathyroid_autograft,
  COALESCE(SUM(parathyroid_autograft_count), 0) AS total_parathyroid_autograft_count,
  COALESCE(SUM(parathyroid_identified_count), 0) AS total_parathyroid_identified_count,
  COALESCE(SUM(CASE WHEN COALESCE(parathyroid_resection_flag, FALSE) THEN 1 ELSE 0 END), 0)
    AS total_parathyroid_resection,
  LOGICAL_OR(COALESCE(rln_monitoring_flag, FALSE)) AS any_rln_monitoring,
  LOGICAL_OR(COALESCE(frozen_section_flag, FALSE)) AS any_frozen_section,
  LOGICAL_OR(COALESCE(frozen_section_any_malignant_flag, FALSE)) AS any_frozen_section_malignant,
  MIN(CAST(surgery_date_native AS DATE)) AS earliest_surgery_date,
  MAX(CAST(surgery_date_native AS DATE)) AS latest_surgery_date,
  AVG(SAFE_CAST(ebl_ml AS FLOAT64)) AS mean_ebl_ml,
  MAX(SAFE_CAST(ebl_ml AS FLOAT64)) AS max_ebl_ml,
  FALSE AS any_drain_placed,
  '{SCRIPT_TAG}' AS build_script,
  CURRENT_TIMESTAMP() AS build_ts
FROM joined
GROUP BY research_id
"""


def dataframe_to_bq(
    client: bigquery.Client,
    df: pd.DataFrame,
    staging_table: str,
) -> None:
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(
        df, staging_table, job_config=job_config, location=LOCATION
    )
    job.result()


def write_report(
    path: str,
    project: str,
    poll_samples: list[str],
    client: Optional[bigquery.Client],
    merged: Optional[pd.DataFrame],
    run_bq_qa: bool,
    reconciled_before: Optional[pd.DataFrame] = None,
    reconciled_after: Optional[pd.DataFrame] = None,
) -> None:
    lines: list[str] = [
        f"# Procedure normalization + surgery date backfill ({SCRIPT_TAG})",
        "",
        f"- **Project:** `{project}`",
        f"- **Generated (UTC):** {datetime.now(timezone.utc).isoformat()}",
        f"- **Anchor:** {ANCHOR_EVENTS_FOR_PROC}",
        "",
        "## Tier-1 QA queries (from docs/tier1_cf_procedure_normalized_corruption_20260422.md)",
        "",
        "```sql",
        "SELECT procedure_normalized, COUNT(*) ",
        f"FROM `{project}.{DATASET_CANONICAL}.{EVENTS_TABLE}` ",
        "GROUP BY 1 ORDER BY 2 DESC;",
        "```",
        "",
    ]
    if merged is not None:
        lines.append(local_coverage_markdown(merged))
        lines.append("")

    if run_bq_qa and client is not None:
        lines.append("## BigQuery QA (post-schema)")
        lines.append("")
        for name, sql in qa_distribution_sql(project).items():
            try:
                df = client.query(sql, location=LOCATION).to_dataframe()
                lines.append(f"### {name}\n")
                try:
                    lines.append(df.to_markdown(index=False))
                except ImportError:
                    lines.append("```\n" + df.to_string(index=False) + "\n```")
                lines.append("")
            except Exception as exc:
                lines.append(f"### {name} — *query failed:* `{exc}`\n")

    if reconciled_before is not None and not reconciled_before.empty:
        lines.append("## manuscript_cohort_v1_surgery_reconciled — before\n")
        try:
            lines.append(reconciled_before.to_markdown(index=False))
        except ImportError:
            lines.append("```\n" + reconciled_before.to_string(index=False) + "\n```")
        lines.append("")
    if reconciled_after is not None and not reconciled_after.empty:
        lines.append("## manuscript_cohort_v1_surgery_reconciled — after\n")
        try:
            lines.append(reconciled_after.to_markdown(index=False))
        except ImportError:
            lines.append("```\n" + reconciled_after.to_string(index=False) + "\n```")
        lines.append("")

    lines.append("## Pathology pollution sample (truncated)\n")
    for i, s in enumerate(poll_samples[:20], 1):
        lines.append(f"{i}. `{s[:120]}{'…' if len(s) > 120 else ''}`")
    lines.append("")
    lines.append("## Acceptance\n")
    lines.append(
        "- >= 95% non-NULL `procedure_normalized_trusted` among rows with "
        "surgery year in {2023,2024,2025} (see coverage_2023_2025).\n"
        "- `manuscript_cohort_v1_surgery_reconciled` per-year counts for 2023+ "
        "(see surgery_reconciled_year).\n"
        "- No deletes in `pub_canonical.canonical_operative_events_v1` (script does not touch it).\n"
    )

    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"Wrote report: {path}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project", default=PROJECT_DEFAULT)
    ap.add_argument("--dry-run", action="store_true", help="Compute + print metrics only")
    ap.add_argument("--apply", action="store_true", help="Write staging + replace rule_clean")
    ap.add_argument("--refresh-manuscript-cohort-dates", action="store_true")
    ap.add_argument("--refresh-rollup", action="store_true")
    ap.add_argument("--refresh-surgery-reconciled", action="store_true")
    ap.add_argument(
        "--report-path",
        default=f"docs/procedure_normalized_2023_2025_backfill_{date.today().isoformat()}.md",
    )
    args = ap.parse_args()

    if args.apply:
        args.dry_run = False
    elif not args.dry_run:
        args.dry_run = True

    client = bigquery.Client(project=args.project)

    n_events = scalar(
        client,
        f"SELECT COUNT(*) FROM {fq(args.project, DATASET_CANONICAL, EVENTS_TABLE)}",
    )
    print(f"pub_canonical.{EVENTS_TABLE} rows: {n_events:,}", flush=True)

    df = fetch_events_df(client, args.project)
    if len(df) != n_events:
        print(
            f"WARN: dataframe row count {len(df)} != scalar count {n_events}", flush=True
        )

    merged, poll_samples = enrich_events_dataframe(df)
    merged["canonical_operative_ingest_note"] = SCRIPT_TAG

    # Drop merge helper only; keep surgery_date_*_365c audit columns on load
    merged_load = merged.drop(columns=["resolved_surgery_date_365c"], errors="ignore")

    # QA: 2023–2025 coverage pre-write
    merged_load["_yr"] = merged_load["resolved_surgery_date"].apply(
        lambda x: parse_date_flexible(x).year if parse_date_flexible(x) else None
    )
    m = merged_load["_yr"].isin([2023, 2024, 2025])
    denom = int(m.sum())
    nnz = int((m & merged_load["procedure_normalized_trusted"].notna()).sum())
    pct = round(100.0 * nnz / denom, 2) if denom else 0.0
    print(
        f"Pre-load sanity: {nnz}/{denom} ({pct}%) trusted non-NULL for yr in 2023–2025 "
        f"(by resolved_surgery_date)",
        flush=True,
    )
    merged_load = merged_load.drop(columns=["_yr"])

    if args.dry_run:
        print("Dry-run only — no BQ writes. Use --apply to persist.", flush=True)
        write_report(
            args.report_path,
            args.project,
            poll_samples,
            client,
            merged_load,
            run_bq_qa=False,
            reconciled_before=None,
            reconciled_after=None,
        )
        return

    reconciled_before = surgery_year_snapshot(client, args.project)
    job_suffix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    staging = f"{args.project}.{DATASET_WORKSPACE}._365c_rule_clean_staging_{job_suffix}"

    try:
        cols = table_columns(
            client, args.project, DATASET_WORKSPACE, RULE_CLEAN_TABLE
        )
    except Exception:
        cols = []
    alter_add_columns_if_missing(client, args.project, cols)

    dataframe_to_bq(client, merged_load, staging)
    run_sql(
        client,
        build_rule_clean_replace_table_sql(args.project, job_suffix),
        f"CREATE OR REPLACE {RULE_CLEAN_TABLE}",
    )

    n_rule = scalar(
        client,
        f"SELECT COUNT(*) FROM {fq(args.project, DATASET_WORKSPACE, RULE_CLEAN_TABLE)}",
    )
    if n_rule != n_events:
        raise SystemExit(
            f"Row count mismatch after replace: rule_clean={n_rule} events={n_events}"
        )

    if args.refresh_manuscript_cohort_dates:
        refresh_manuscript_cohort_null_dates(client, args.project)

    if args.refresh_rollup:
        run_sql(client, rebuild_rollup_sql(args.project), "rollup rebuild")

    if args.refresh_surgery_reconciled:
        build_surgery_reconciled_table(client, args.project)

    reconciled_after = surgery_year_snapshot(client, args.project)

    write_report(
        args.report_path,
        args.project,
        poll_samples,
        client,
        merged_load,
        run_bq_qa=True,
        reconciled_before=reconciled_before,
        reconciled_after=reconciled_after,
    )
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
