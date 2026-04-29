#!/usr/bin/env python3
"""Read-only mig_170 cross-canonical same-name data-type drift audit.

This script performs no database writes. It inventories verified canonical_* tables
in the publication database, identifies columns that are shared with
canonical_patient_master, and flags same-name columns whose physical data types
diverge across verified canonicals. For high-risk drifts involving CPM and a
table with research_id, it also runs read-only patient-level JOIN-trap probes.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402


PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
SCHEMA = "main"
PM_TABLE = "canonical_patient_master"
PM_FQ = f'"{PUBLICATION_DB}"."{SCHEMA}"."{PM_TABLE}"'
EXPORT_ROOT = REPO_ROOT / "exports"
REPORT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_170_cross_canonical_dtype_drift_20260429.md"
SQL_STUB_PATH = REPO_ROOT / "qc_framework_v1" / "migrations" / "170_cross_canonical_dtype_drift_probes_20260429.sql"

INTEGER_TYPES = {"BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT"}
FLOAT_TYPES = {"DOUBLE", "FLOAT", "REAL"}
DATE_TYPES = {"DATE"}
TIMESTAMP_TYPES = {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S"}
BOOLEAN_TYPES = {"BOOLEAN"}
TEXT_TYPES = {"VARCHAR", "TEXT", "CHAR", "CHARACTER VARYING"}

DATE_NAME_RE = re.compile(r"(^|_)(dates?|dt)(_|$)|(_date$|_dt$)", re.IGNORECASE)
COUNT_NAME_RE = re.compile(r"(^n_|^num_|^number_|^total_|_count$|_counts$|_n$|_examined$|_positive$|_negative$)", re.IGNORECASE)
ID_NAME_RE = re.compile(r"(^|_)(id|episode_id|exam_id|event_id|source_id)(_|$)", re.IGNORECASE)


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fq_table(table_name: str) -> str:
    return f'"{PUBLICATION_DB}"."{SCHEMA}".{qident(table_name)}'


def safe_pct(num: float, den: float) -> float:
    if not den:
        return 0.0
    return round((num / den) * 100.0, 3)


def canonical_data_type(data_type: str) -> str:
    dt = str(data_type).upper().strip()
    if dt.startswith("DECIMAL") or dt.startswith("NUMERIC"):
        return "DECIMAL"
    if dt.startswith("TIMESTAMP"):
        return "TIMESTAMP WITH TIME ZONE" if "WITH TIME ZONE" in dt else "TIMESTAMP"
    if "ENUM" in dt:
        return "ENUM"
    return dt


def type_family(data_type: str) -> str:
    dt = canonical_data_type(data_type)
    if dt in INTEGER_TYPES:
        return "integer"
    if dt in FLOAT_TYPES or dt == "DECIMAL":
        return "numeric"
    if dt in DATE_TYPES:
        return "date"
    if dt in TIMESTAMP_TYPES or dt == "TIMESTAMP":
        return "timestamp"
    if dt in BOOLEAN_TYPES:
        return "boolean"
    if dt in TEXT_TYPES:
        return "text"
    if dt == "ENUM":
        return "enum"
    return "other"


def markdown_table(df: pd.DataFrame, *, index: bool = False) -> str:
    if df.empty:
        return "No rows."
    safe = df.copy()
    for col in safe.columns:
        if pd.api.types.is_object_dtype(safe[col]) or pd.api.types.is_string_dtype(safe[col]):
            safe[col] = safe[col].astype("string").str.replace("|", r"\|", regex=False)
    return safe.to_markdown(index=index)


def fetch_verified_tables(con) -> pd.DataFrame:
    sql = """
    SELECT table_name, priority_tier, signoff_migration, notes
    FROM main.canonical_table_signoff_registry_v1
    WHERE table_status = 'verified'
      AND table_name LIKE 'canonical_%'
    ORDER BY table_name
    """
    tables = con.execute(sql).fetchdf().drop_duplicates(subset=["table_name"])
    if PM_TABLE not in set(tables["table_name"]):
        exists = con.execute(f"""
            SELECT COUNT(*) AS n
            FROM information_schema.tables
            WHERE table_catalog = '{PUBLICATION_DB}'
              AND table_schema = '{SCHEMA}'
              AND table_name = '{PM_TABLE}'
        """).fetchone()[0]
        if not exists:
            raise RuntimeError(f"{PM_TABLE} not found in information_schema.tables")
        tables = pd.concat([
            pd.DataFrame([{
                "table_name": PM_TABLE,
                "priority_tier": "primary_spine_not_in_signoff_registry",
                "signoff_migration": "n/a",
                "notes": "Included as CPM anchor for mig_170 PM-centered audit; Tier-2 comparators remain verified-only.",
            }]),
            tables,
        ], ignore_index=True).drop_duplicates(subset=["table_name"])
    return tables


def fetch_column_catalog(con, verified_tables: pd.DataFrame) -> pd.DataFrame:
    table_list = ", ".join("'" + str(t).replace("'", "''") + "'" for t in verified_tables["table_name"].tolist())
    sql = f"""
    SELECT DISTINCT
        c.table_name,
        c.ordinal_position,
        c.column_name,
        c.data_type
    FROM information_schema.columns c
    WHERE c.table_catalog = '{PUBLICATION_DB}'
      AND c.table_schema = '{SCHEMA}'
      AND c.table_name IN ({table_list})
    ORDER BY c.table_name, c.ordinal_position
    """
    cols = con.execute(sql).fetchdf()
    if cols.empty:
        raise RuntimeError("No information_schema columns found for verified canonical tables")
    cols["canonical_data_type"] = cols["data_type"].map(canonical_data_type)
    cols["type_family"] = cols["data_type"].map(type_family)
    return cols


def build_shared_columns(cols: pd.DataFrame) -> pd.DataFrame:
    pm_cols = set(cols.loc[cols["table_name"] == PM_TABLE, "column_name"])
    shared = cols.loc[cols["column_name"].isin(pm_cols)].copy()
    rows: list[dict[str, Any]] = []
    for column_name, grp in shared.groupby("column_name", sort=True):
        tables = sorted(grp["table_name"].unique().tolist())
        if len(tables) < 2 or PM_TABLE not in tables:
            continue
        dtypes = sorted(grp["canonical_data_type"].unique().tolist())
        pairs = " | ".join(
            f"{r.table_name}:{r.canonical_data_type}"
            for r in grp.sort_values(["table_name", "canonical_data_type"]).itertuples(index=False)
        )
        rows.append({
            "column_name": column_name,
            "n_tables_with_col": int(len(tables)),
            "n_distinct_dtypes": int(len(dtypes)),
            "dtypes": " | ".join(dtypes),
            "table_dtype_pairs": pairs,
            "tables_with_col": " | ".join(tables),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["column_name", "n_tables_with_col", "n_distinct_dtypes", "dtypes", "table_dtype_pairs", "tables_with_col"])
    return out.sort_values(["n_distinct_dtypes", "n_tables_with_col", "column_name"], ascending=[False, False, True]).reset_index(drop=True)


def classify_drift(column_name: str, data_types: set[str], type_families: set[str]) -> tuple[str, str, str, str]:
    fam = set(type_families)
    canonical_types = {canonical_data_type(t) for t in data_types}
    col_lower = column_name.lower()

    if ID_NAME_RE.search(col_lower) and "text" in fam and ("integer" in fam or "numeric" in fam):
        return "DATA-LOSS-RISK", "high", "identifier key dtype drift; explicit CAST is required for portable joins", "do not parse as measurement; standardize join code to CAST(other.research_id AS VARCHAR) or evaluate long-term key retype"
    if fam.issubset({"integer"}):
        return "BENIGN", "low", "integer widths auto-cast safely in DuckDB joins", "document only; no carry-forward"
    if fam.issubset({"integer", "numeric"}):
        return "CASTABLE", "low", "numeric-family drift; integer values may be widened to floating/decimal", "review semantics; standardize only if model code requires integer typing"
    if fam.issubset({"date", "timestamp"}):
        return "CASTABLE", "medium", "DATE/TIMESTAMP calendar drift; DATE is a subset but equality can fail without CAST", "open mig_170b date-retype/cast lane; prefer DATE for clinical calendar dates"
    if fam.issubset({"text", "enum"}):
        return "CASTABLE", "medium", "VARCHAR/ENUM drift; values are text-compatible but enum domain may differ", "review enum domain; standardize to VARCHAR or shared enum after ratification"
    if "boolean" in fam and fam.issubset({"boolean", "integer"}):
        return "CASTABLE", "medium", "BOOLEAN/integer flag drift; explicit cast required for safe comparisons", "standardize to BOOLEAN after confirming 0/1 semantics"
    if "text" in fam and ("date" in fam or "timestamp" in fam):
        return "DATA-LOSS-RISK", "high", "date/time values mixed with VARCHAR; parsing and equality are not guaranteed", "open high-priority mig_170b retype or explicit parse/cast follow-up"
    if "text" in fam and ("numeric" in fam or "integer" in fam):
        return "DATA-LOSS-RISK", "high", "numeric values mixed with VARCHAR; COALESCE/JOIN code may silently coerce or fail", "open high-priority mig_170b numeric retype/parse follow-up"
    if "text" in fam and "boolean" in fam:
        return "DATA-LOSS-RISK", "high", "BOOLEAN values mixed with VARCHAR tokens; truth-state logic may diverge", "open high-priority mig_170b boolean normalization follow-up"
    if len(fam) > 1:
        return "DATA-LOSS-RISK", "high", f"mixed incompatible type families: {', '.join(sorted(fam))}", "open high-priority mig_170b targeted compatibility review"
    if DATE_NAME_RE.search(col_lower) and ("TIMESTAMP" in canonical_types):
        return "CASTABLE", "medium", "date-name column stored as TIMESTAMP in at least one canonical", "review for DATE retype if calendar-only"
    if COUNT_NAME_RE.search(col_lower):
        return "BENIGN", "low", "count-like column has same broad family but differing widths", "document only unless downstream model requires exact type"
    return "BENIGN", "low", "dtype drift appears auto-cast compatible", "document only"


def build_findings(shared: pd.DataFrame, cols: pd.DataFrame) -> pd.DataFrame:
    finding_rows: list[dict[str, Any]] = []
    drift_cols = shared.loc[shared["n_distinct_dtypes"] >= 2, "column_name"].tolist() if not shared.empty else []
    for column_name in drift_cols:
        grp = cols.loc[(cols["column_name"] == column_name) & (cols["table_name"].isin(shared.loc[shared["column_name"] == column_name, "tables_with_col"].iloc[0].split(" | ")))].copy()
        classification, priority, evidence, action = classify_drift(
            column_name,
            set(grp["canonical_data_type"].tolist()),
            set(grp["type_family"].tolist()),
        )
        finding_rows.append({
            "column_name": column_name,
            "n_tables": int(grp["table_name"].nunique()),
            "n_distinct_dtypes": int(grp["canonical_data_type"].nunique()),
            "drift_classification": classification,
            "priority": priority,
            "dtypes_per_table": " | ".join(f"{r.table_name}:{r.canonical_data_type}" for r in grp.sort_values(["table_name", "canonical_data_type"]).itertuples(index=False)),
            "evidence": evidence,
            "recommended_action": action,
            "cf_tag": "" if classification == "BENIGN" else f"CF-mig170-DTYPE-DRIFT-{classification.replace('-', '_')}-{column_name}".replace(" ", "_"),
        })
    findings = pd.DataFrame(finding_rows)
    if findings.empty:
        return pd.DataFrame(columns=["column_name", "n_tables", "n_distinct_dtypes", "drift_classification", "priority", "dtypes_per_table", "evidence", "recommended_action", "cf_tag"])
    class_order = {"DATA-LOSS-RISK": 1, "CASTABLE": 2, "BENIGN": 3, "SEMANTIC-DRIFT": 4}
    priority_order = {"high": 1, "medium": 2, "low": 3}
    findings["class_order"] = findings["drift_classification"].map(class_order).fillna(9).astype(int)
    findings["priority_order"] = findings["priority"].map(priority_order).fillna(9).astype(int)
    return findings.sort_values(["class_order", "priority_order", "n_tables", "column_name"], ascending=[True, True, False, True]).reset_index(drop=True)


def has_research_id(cols: pd.DataFrame, table_name: str) -> bool:
    table_cols = set(cols.loc[cols["table_name"] == table_name, "column_name"])
    return "research_id" in table_cols


def run_join_trap_probe(con, column_name: str, table_name: str) -> dict[str, Any]:
    pm_col = qident(column_name)
    tbl = fq_table(table_name)
    sql = f"""
    WITH tier2 AS (
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            LIST(DISTINCT CAST({pm_col} AS VARCHAR)) FILTER (WHERE {pm_col} IS NOT NULL) AS tier2_values,
            COUNT(*) FILTER (WHERE {pm_col} IS NOT NULL) AS n_nonnull_rows,
            COUNT(DISTINCT CAST({pm_col} AS VARCHAR)) FILTER (WHERE {pm_col} IS NOT NULL) AS n_distinct_values
        FROM {tbl}
        GROUP BY 1
    ), pm AS (
        SELECT CAST(research_id AS VARCHAR) AS research_id, CAST({pm_col} AS VARCHAR) AS pm_value
        FROM {PM_FQ}
    )
    SELECT
        COUNT(*) FILTER (WHERE pm.pm_value IS NOT NULL OR tier2.tier2_values IS NOT NULL) AS n_either_nonnull,
        COUNT(*) FILTER (
            WHERE pm.pm_value IS NOT NULL
              AND tier2.tier2_values IS NOT NULL
              AND NOT list_contains(tier2.tier2_values, pm.pm_value)
        ) AS n_both_nonnull_no_exact_match,
        COUNT(*) FILTER (WHERE pm.pm_value IS NOT NULL AND tier2.tier2_values IS NULL) AS n_pm_only,
        COUNT(*) FILTER (WHERE pm.pm_value IS NULL AND tier2.tier2_values IS NOT NULL) AS n_tier2_only,
        COUNT(*) FILTER (WHERE COALESCE(tier2.n_distinct_values, 0) > 1) AS n_tier2_multi_value_patients,
        MAX(COALESCE(tier2.n_distinct_values, 0)) AS max_tier2_distinct_values_per_patient
    FROM pm
    LEFT JOIN tier2 USING (research_id)
    """
    rec = con.execute(sql).fetchdf().iloc[0].to_dict()
    rec.update({"column_name": column_name, "tier2_table": table_name})
    for key, value in list(rec.items()):
        if pd.isna(value):
            rec[key] = 0
        elif isinstance(value, float) and value.is_integer():
            rec[key] = int(value)
    return rec


def build_join_trap_probes(con, findings: pd.DataFrame, cols: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if findings.empty:
        return pd.DataFrame()
    dangerous = findings.loc[findings["drift_classification"] == "DATA-LOSS-RISK"]
    for rec in dangerous.itertuples(index=False):
        column_name = rec.column_name
        pm_dtype = cols.loc[(cols["table_name"] == PM_TABLE) & (cols["column_name"] == column_name), "canonical_data_type"].iloc[0]
        table_rows = cols.loc[(cols["column_name"] == column_name) & (cols["table_name"] != PM_TABLE)].copy()
        for table_rec in table_rows.itertuples(index=False):
            if table_rec.canonical_data_type == pm_dtype:
                continue
            if not has_research_id(cols, table_rec.table_name):
                rows.append({
                    "column_name": column_name,
                    "tier2_table": table_rec.table_name,
                    "pm_data_type": pm_dtype,
                    "tier2_data_type": table_rec.canonical_data_type,
                    "probe_status": "skipped_no_research_id",
                })
                continue
            try:
                probe = run_join_trap_probe(con, column_name, table_rec.table_name)
                probe.update({
                    "pm_data_type": pm_dtype,
                    "tier2_data_type": table_rec.canonical_data_type,
                    "probe_status": "ok",
                })
            except Exception as exc:  # read-only probe should never abort the whole audit
                probe = {
                    "column_name": column_name,
                    "tier2_table": table_rec.table_name,
                    "pm_data_type": pm_dtype,
                    "tier2_data_type": table_rec.canonical_data_type,
                    "probe_status": f"error: {type(exc).__name__}: {exc}",
                }
            rows.append(probe)
    return pd.DataFrame(rows)


def build_report(out_dir: Path, verified_tables: pd.DataFrame, shared: pd.DataFrame, findings: pd.DataFrame, join_probes: pd.DataFrame, manifest: dict[str, Any]) -> str:
    class_counts = findings["drift_classification"].value_counts().rename_axis("drift_classification").reset_index(name="n_findings") if not findings.empty else pd.DataFrame()
    priority_counts = findings["priority"].value_counts().rename_axis("priority").reset_index(name="n_findings") if not findings.empty else pd.DataFrame()
    top_findings = findings.drop(columns=[c for c in ["class_order", "priority_order"] if c in findings.columns]).head(80)
    dangerous_findings = findings.loc[findings["drift_classification"] == "DATA-LOSS-RISK"].drop(columns=[c for c in ["class_order", "priority_order"] if c in findings.columns])
    castable_findings = findings.loc[findings["drift_classification"] == "CASTABLE"].drop(columns=[c for c in ["class_order", "priority_order"] if c in findings.columns]).head(80)

    probe_cols = [
        "column_name", "tier2_table", "pm_data_type", "tier2_data_type", "probe_status",
        "n_either_nonnull", "n_both_nonnull_no_exact_match", "n_pm_only", "n_tier2_only", "n_tier2_multi_value_patients",
    ]
    probe_md = markdown_table(join_probes[[c for c in probe_cols if c in join_probes.columns]]) if not join_probes.empty else "No DATA-LOSS-RISK probes were required."

    return f"""# mig_170 — Cross-canonical data-type drift audit

**Date:** 2026-04-29  
**Posture:** read-only MotherDuck audit; no database writes; no retypes.  
**Scope:** `main.canonical_patient_master` same-name columns across verified `canonical_%` tables in `main`.  
**Export directory:** `{out_dir.relative_to(REPO_ROOT)}`

## Executive summary

| Metric | Value |
|---|---:|
| Canonical tables inspected (verified comparators + CPM anchor) | {len(verified_tables):,} |
| Verified comparator tables in signoff registry | {int(manifest.get('verified_comparator_tables_in_registry', 0)):,} |
| PM same-name columns shared with at least one verified canonical | {len(shared):,} |
| Shared PM columns with dtype drift | {len(findings):,} |
| DATA-LOSS-RISK findings | {int((findings['drift_classification'] == 'DATA-LOSS-RISK').sum()) if not findings.empty else 0:,} |
| CASTABLE findings | {int((findings['drift_classification'] == 'CASTABLE').sum()) if not findings.empty else 0:,} |
| BENIGN findings | {int((findings['drift_classification'] == 'BENIGN').sum()) if not findings.empty else 0:,} |
| JOIN-trap probes executed or skipped | {len(join_probes):,} |

## Findings by classification

{markdown_table(class_counts)}

## Findings by priority

{markdown_table(priority_counts)}

## DATA-LOSS-RISK findings

{markdown_table(dangerous_findings)}

## CASTABLE findings

{markdown_table(castable_findings)}

## Complete findings table

{markdown_table(top_findings)}

## Cross-canonical JOIN-trap probes

For each DATA-LOSS-RISK finding involving CPM and a verified canonical table with `research_id`, the probe aggregates the Tier-2/event table to one row per patient and compares string-cast values. These counts are a triage signal only; they do **not** imply a schema mutation.

{probe_md}

## Interpretation

This lane is a **read-only cross-canonical dtype drift audit**. It does not mutate the registry or any canonical table. Findings are conservative and intended to seed `mig_170b` follow-up lanes after ratification.

Classification rules:

- **BENIGN:** auto-cast-safe integer-width drift or cosmetic count-width differences.
- **CASTABLE:** likely lossless with explicit casts, such as DATE/TIMESTAMP or VARCHAR/ENUM drift.
- **DATA-LOSS-RISK:** type families differ in ways that can break or silently coerce JOIN/COALESCE logic, such as VARCHAR vs DATE/TIMESTAMP/NUMERIC/BOOLEAN.
- **SEMANTIC-DRIFT:** not automatically inferred here when dtypes match; same-name semantic disagreements require a separate definition-level audit.

## Recommended follow-up

1. Review all DATA-LOSS-RISK rows first and open targeted `mig_170b` lanes using the generated CF tags.
2. For date-like CASTABLE rows, prefer calendar `DATE` for clinical event dates and keep build/provenance timestamps as TIMESTAMP.
3. For VARCHAR/ENUM rows, compare value domains before any retype; enum compatibility can be semantic, not just physical.
4. For event-grain tables, treat JOIN-trap mismatch counts as triage only because a patient may legitimately have multiple event values.
5. Do not apply retypes from this lane; any schema mutation must archive snapshots and update CPM provenance per governance.

## Artifacts

| Artifact | Purpose |
|---|---|
| `{(out_dir / 'cross_canonical_shared_columns.csv').relative_to(REPO_ROOT)}` | All PM same-name columns shared with at least one verified canonical. |
| `{(out_dir / 'cross_canonical_dtype_findings.csv').relative_to(REPO_ROOT)}` | Dtype-drift findings with classification, evidence, action, and CF tag. |
| `{(out_dir / 'cross_canonical_join_trap_probes.csv').relative_to(REPO_ROOT)}` | Patient-level read-only probe results for DATA-LOSS-RISK pairs. |
| `{(out_dir / 'manifest.json').relative_to(REPO_ROOT)}` | Machine-readable run manifest. |
| `{SQL_STUB_PATH.relative_to(REPO_ROOT)}` | Commented read-only replay probes. |

## Run manifest

```json
{json.dumps(manifest, indent=2)}
```
"""


def build_sql_stub(manifest: dict[str, Any], findings: pd.DataFrame) -> str:
    dangerous = findings.loc[findings["drift_classification"] == "DATA-LOSS-RISK"] if not findings.empty else pd.DataFrame()
    probe_sections: list[str] = []
    for rec in dangerous.itertuples(index=False):
        column_name = rec.column_name
        probe_sections.append(f"""
-- -----------------------------------------------------------------------------
-- DATA-LOSS-RISK probe template: {column_name}
-- Replace <tier2_table> with a verified canonical table from the finding row.
-- -----------------------------------------------------------------------------
-- WITH tier2 AS (
--     SELECT
--         CAST(research_id AS VARCHAR) AS research_id,
--         LIST(DISTINCT CAST({qident(column_name)} AS VARCHAR)) FILTER (WHERE {qident(column_name)} IS NOT NULL) AS tier2_values,
--         COUNT(DISTINCT CAST({qident(column_name)} AS VARCHAR)) FILTER (WHERE {qident(column_name)} IS NOT NULL) AS n_distinct_values
--     FROM thyroid_canonical_publication_v1_0.main.<tier2_table>
--     GROUP BY 1
-- ), pm AS (
--     SELECT CAST(research_id AS VARCHAR) AS research_id,
--            CAST({qident(column_name)} AS VARCHAR) AS pm_value
--     FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master
-- )
-- SELECT
--     COUNT(*) FILTER (WHERE pm.pm_value IS NOT NULL OR tier2.tier2_values IS NOT NULL) AS n_either_nonnull,
--     COUNT(*) FILTER (WHERE pm.pm_value IS NOT NULL AND tier2.tier2_values IS NOT NULL AND NOT list_contains(tier2.tier2_values, pm.pm_value)) AS n_both_nonnull_no_exact_match,
--     COUNT(*) FILTER (WHERE pm.pm_value IS NOT NULL AND tier2.tier2_values IS NULL) AS n_pm_only,
--     COUNT(*) FILTER (WHERE pm.pm_value IS NULL AND tier2.tier2_values IS NOT NULL) AS n_tier2_only,
--     COUNT(*) FILTER (WHERE COALESCE(tier2.n_distinct_values, 0) > 1) AS n_tier2_multi_value_patients
-- FROM pm
-- LEFT JOIN tier2 USING (research_id);
""")

    return f"""-- =============================================================================
-- Migration 170 — CROSS-CANONICAL DATA-TYPE DRIFT AUDIT PROBES (read-only)
-- =============================================================================
-- Date: 2026-04-29
-- Batch: mig_170_cross_canonical_dtype_drift_20260429
-- Posture: read-only probe SQL only. Do NOT execute retypes in this lane.
-- Generated by qc_framework_v1/scripts/build_mig170_cross_canonical_dtype_drift_audit.py
-- Report: qc_framework_v1/reports/mig_170_cross_canonical_dtype_drift_20260429.md
-- Export dir: {manifest.get('export_dir', '<not-run>')}
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Scope query: PM same-name columns across verified canonical_* tables.
-- -----------------------------------------------------------------------------
-- WITH verified_tables AS (
--   SELECT table_name
--   FROM main.canonical_table_signoff_registry_v1
--   WHERE table_status='verified' AND table_name LIKE 'canonical_%'
--   UNION ALL
--   SELECT 'canonical_patient_master' AS table_name  -- CPM anchor may not be in signoff registry
-- ), cols_per_table AS (
--   SELECT DISTINCT c.table_name, c.column_name, c.data_type
--   FROM information_schema.columns c
--   JOIN verified_tables v ON c.table_name = v.table_name
--   WHERE c.table_catalog='thyroid_canonical_publication_v1_0'
--     AND c.table_schema='main'
-- ), pm_cols AS (
--   SELECT DISTINCT column_name
--   FROM cols_per_table
--   WHERE table_name='canonical_patient_master'
-- ), shared AS (
--   SELECT c.column_name,
--          COUNT(DISTINCT c.table_name) AS n_tables_with_col,
--          COUNT(DISTINCT c.data_type) AS n_distinct_dtypes,
--          STRING_AGG(DISTINCT (c.table_name || ':' || c.data_type), ' | ' ORDER BY c.table_name || ':' || c.data_type) AS table_dtype_pairs
--   FROM cols_per_table c
--   JOIN pm_cols p USING (column_name)
--   GROUP BY 1
--   HAVING COUNT(DISTINCT c.table_name) >= 2
-- )
-- SELECT *
-- FROM shared
-- WHERE n_distinct_dtypes >= 2
-- ORDER BY n_distinct_dtypes DESC, n_tables_with_col DESC, column_name;

{''.join(probe_sections) if probe_sections else '-- No DATA-LOSS-RISK probe templates were generated.\n'}
-- End mig_170 read-only probe stub.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=None, help="Optional output directory")
    args = parser.parse_args()

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or (EXPORT_ROOT / f"mig170_cross_canonical_dtype_drift_{run_ts}")
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    con = connect_locked()
    verified_tables = fetch_verified_tables(con)
    cols = fetch_column_catalog(con, verified_tables)
    shared = build_shared_columns(cols)
    findings = build_findings(shared, cols)
    join_probes = build_join_trap_probes(con, findings, cols)

    shared.to_csv(out_dir / "cross_canonical_shared_columns.csv", index=False)
    findings.drop(columns=[c for c in ["class_order", "priority_order"] if c in findings.columns]).to_csv(out_dir / "cross_canonical_dtype_findings.csv", index=False)
    join_probes.to_csv(out_dir / "cross_canonical_join_trap_probes.csv", index=False)

    manifest = {
        "migration": "mig_170",
        "run_timestamp_utc": run_ts,
        "posture": "read_only_motherduck_audit_no_db_writes",
        "scope": "PM same-name columns across verified canonical_% tables",
        "verified_comparator_tables_in_registry": int(len(verified_tables.loc[verified_tables["table_name"] != PM_TABLE])),
        "cpm_anchor_included_outside_signoff_registry": bool(verified_tables.loc[verified_tables["table_name"] == PM_TABLE, "priority_tier"].iloc[0] == "primary_spine_not_in_signoff_registry"),
        "verified_canonical_tables_inspected": int(len(verified_tables)),
        "pm_shared_columns": int(len(shared)),
        "dtype_drift_findings": int(len(findings)),
        "findings_by_classification": {str(k): int(v) for k, v in findings["drift_classification"].value_counts().to_dict().items()} if not findings.empty else {},
        "findings_by_priority": {str(k): int(v) for k, v in findings["priority"].value_counts().to_dict().items()} if not findings.empty else {},
        "join_trap_probe_rows": int(len(join_probes)),
        "export_dir": str(out_dir.relative_to(REPO_ROOT)),
        "artifacts": [
            "cross_canonical_shared_columns.csv",
            "cross_canonical_dtype_findings.csv",
            "cross_canonical_join_trap_probes.csv",
            "manifest.json",
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    REPORT_PATH.write_text(build_report(out_dir, verified_tables, shared, findings, join_probes, manifest))
    SQL_STUB_PATH.write_text(build_sql_stub(manifest, findings))

    print(json.dumps(manifest, indent=2))
    print(f"Report: {REPORT_PATH}")
    print(f"SQL stub: {SQL_STUB_PATH}")
    print(f"Exports: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())