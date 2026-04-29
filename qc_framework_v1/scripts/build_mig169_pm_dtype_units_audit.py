#!/usr/bin/env python3
"""Read-only mig_169 data-type and units sanity audit for PM verified columns.

This script performs no database writes. It scans verified analytic columns on
main.canonical_patient_master for common data-type smells: VARCHAR measurements
with units, VARCHAR numeric/boolean/date fields, TIMESTAMP clinical dates, and
DOUBLE columns that are integer-valued count fields.
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
from pandas.api import types as ptypes


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402


PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
TABLE_FQ = f'"{PUBLICATION_DB}".main.canonical_patient_master'
EXPORT_ROOT = REPO_ROOT / "exports"
REPORT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_169_pm_dtype_units_audit_20260429.md"
SQL_STUB_PATH = REPO_ROOT / "qc_framework_v1" / "migrations" / "169_pm_dtype_units_audit_probes_20260429.sql"

EXCLUDED_SUFFIXES = ("_source", "_raw", "_note_ref", "_evidence")
EXCLUDED_SUBSTRINGS = ("_keyword", "_text", "source", "evidence", "snippet", "notes")
BOOLEAN_VALUES = {"true", "false", "yes", "no", "y", "n", "t", "f", "1", "0"}
DATE_TYPE_SET = {"DATE"}
TIMESTAMP_TYPES = {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S"}
NUMERIC_TYPES = {"DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"}
INTEGER_TYPES = {"BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT"}

MEASUREMENT_NAME_RE = re.compile(
    r"(^|_)(size|diameter|diam|dose|volume|weight|height|width|length|depth|"
    r"largest|smallest|max|min|mean|median|nadir|peak|ratio|slope|value|"
    r"tg|tsh|pth|calcium|ca|vitamin_d|specimen|margin|distance|dimension|af|"
    r"mci|mm|cm|ml|kg|g|pg_ml|ng_ml|iu_ml|mg_dl|mmol_l)(_|$)",
    re.IGNORECASE,
)
UNIT_VALUE_RE = re.compile(
    r"(?:^|[^A-Za-z])[-+]?\d+(?:\.\d+)?\s*(?:cm|mm|mci|ml|kg|mg|g|pg/ml|ng/ml|iu/ml|mg/dl|mmol/l|%)\b",
    re.IGNORECASE,
)
DATE_NAME_RE = re.compile(r"(^|_)(dates?|dt)(_|$)|(_date$|_dt$)", re.IGNORECASE)
DATE_NAME_EXCLUDE_RE = re.compile(r"(_date_(confidence|status|source|granularity)|_date_confidence$|_date_status$|_date_source$|_date_granularity$)", re.IGNORECASE)
COUNT_NAME_RE = re.compile(
    r"(^n_|^num_|^number_|^total_|_count$|_counts$|_n$|_n_|_num$|_total$|_total_|_examined$|_positive$|_negative$)",
    re.IGNORECASE,
)
BOOL_NAME_RE = re.compile(
    r"(^is_|^has_|^had_|^was_|^were_|^ever_|_flag$|_any$|_present$|_positive$|_negative$|_confirmed$|_eligible$|_detected$|_available$)",
    re.IGNORECASE,
)


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def safe_pct(num: float, den: float) -> float:
    if not den:
        return 0.0
    return round((num / den) * 100.0, 3)


def canonical_data_type(data_type: str) -> str:
    dt = str(data_type).upper()
    if dt.startswith("DECIMAL") or dt.startswith("NUMERIC"):
        return "DECIMAL"
    if dt.startswith("TIMESTAMP"):
        return "TIMESTAMP" if "WITH TIME ZONE" not in dt else "TIMESTAMP WITH TIME ZONE"
    return dt


def in_scope_column(column_name: str) -> bool:
    c = column_name.lower()
    if c.startswith("nlp_") and c.endswith("_key_finding"):
        return False
    if any(c.endswith(sfx) for sfx in EXCLUDED_SUFFIXES):
        return False
    if any(tok in c for tok in EXCLUDED_SUBSTRINGS):
        return False
    return True


def is_date_name_like(column_name: str) -> bool:
    return bool(DATE_NAME_RE.search(column_name)) and not bool(DATE_NAME_EXCLUDE_RE.search(column_name))


def parse_datetimes(values: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(values, errors="coerce", format="mixed")
    except (TypeError, ValueError):
        return pd.to_datetime(values, errors="coerce")


def sample_values(series: pd.Series, max_items: int = 5, max_len: int = 80) -> str:
    vals: list[str] = []
    for value in series.dropna().astype("string").str.strip().unique().tolist():
        text = str(value)
        if not text:
            continue
        if len(text) > max_len:
            text = text[: max_len - 1] + "…"
        vals.append(text)
        if len(vals) >= max_items:
            break
    return " | ".join(vals)


def markdown_table(df: pd.DataFrame, *, index: bool = False) -> str:
    if df.empty:
        return "No rows."
    safe = df.copy()
    for col in safe.columns:
        if ptypes.is_object_dtype(safe[col]) or ptypes.is_string_dtype(safe[col]):
            safe[col] = safe[col].astype("string").str.replace("|", r"\|", regex=False)
    return safe.to_markdown(index=index)


def fetch_column_catalog(con) -> pd.DataFrame:
    sql = f"""
    SELECT
        c.ordinal_position,
        c.column_name,
        c.data_type,
        r.verification_status,
        r.verification_method,
        r.batch_id,
        r.notes AS registry_notes
    FROM information_schema.columns c
    JOIN main.canonical_column_verification_registry_v1 r
      ON r.schema_name = 'main'
     AND r.table_name = c.table_name
     AND r.column_name = c.column_name
    WHERE c.table_catalog = '{PUBLICATION_DB}'
      AND c.table_schema = 'main'
      AND c.table_name = 'canonical_patient_master'
      AND r.verification_status = 'verified'
    ORDER BY c.ordinal_position
    """
    catalog = con.execute(sql).fetchdf()
    catalog["canonical_data_type"] = catalog["data_type"].map(canonical_data_type)
    catalog["in_scope"] = catalog["column_name"].map(in_scope_column)
    return catalog.loc[catalog["in_scope"]].reset_index(drop=True)


def fetch_pm_dataframe(con, columns: list[str]) -> pd.DataFrame:
    select_list = ", ".join(qident(c) for c in columns)
    return con.execute(f"SELECT {select_list} FROM {TABLE_FQ} ORDER BY research_id").fetchdf()


def normalized_text(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip()
    text = text.mask(text == "")
    return text.str.replace(r"\s+", " ", regex=True).str.lower()


def summarize_column(series: pd.Series, column_name: str, data_type: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    dt = canonical_data_type(data_type)
    n_rows = int(len(series))
    non_null_mask = series.notna()
    if ptypes.is_string_dtype(series) or dt == "VARCHAR":
        text = series.astype("string").str.strip()
        non_null_mask = text.notna() & (text != "")
    n_nonnull = int(non_null_mask.sum())
    n_null = n_rows - n_nonnull
    rec: dict[str, Any] = {
        "column_name": column_name,
        "data_type": data_type,
        "canonical_data_type": dt,
        "n_rows": n_rows,
        "n_nonnull": n_nonnull,
        "null_pct": safe_pct(n_null, n_rows),
        "measurement_name_like": bool(MEASUREMENT_NAME_RE.search(column_name)),
        "date_name_like": is_date_name_like(column_name),
        "count_name_like": bool(COUNT_NAME_RE.search(column_name)),
        "boolean_name_like": bool(BOOL_NAME_RE.search(column_name)),
    }
    findings: list[dict[str, Any]] = []

    if dt == "VARCHAR":
        text = series.astype("string").str.strip()
        present = text[text.notna() & (text != "")]
        norm = normalized_text(series)
        norm_present = norm[norm.notna()]
        rec["distinct_norm"] = int(norm_present.nunique(dropna=True))
        rec["sample_values"] = sample_values(present)
        n_with_alpha = int(present.str.contains(r"[A-Za-z]", regex=True, na=False).sum())
        n_with_units = int(present.str.contains(UNIT_VALUE_RE, regex=True, na=False).sum())
        numeric_parse = pd.to_numeric(present.str.replace(",", "", regex=False), errors="coerce") if not present.empty else pd.Series(dtype="float64")
        n_numeric_parseable = int(numeric_parse.notna().sum()) if not present.empty else 0
        n_unparseable = int(n_nonnull - n_numeric_parseable)
        unique_norm = {str(v) for v in norm_present.dropna().unique().tolist()}
        bool_subset = bool(unique_norm) and unique_norm.issubset(BOOLEAN_VALUES)
        date_parse = parse_datetimes(present) if rec["date_name_like"] and not present.empty else pd.Series(dtype="datetime64[ns]")
        n_date_parseable = int(date_parse.notna().sum()) if rec["date_name_like"] and not present.empty else 0
        rec.update(
            {
                "n_with_alpha": n_with_alpha,
                "n_with_units": n_with_units,
                "n_numeric_parseable": n_numeric_parseable,
                "n_unparseable": n_unparseable,
                "n_boolean_like_values": int(len(unique_norm & BOOLEAN_VALUES)),
                "boolean_value_subset": bool_subset,
                "n_date_parseable": n_date_parseable,
            }
        )

        if rec["measurement_name_like"] and n_with_units > 0:
            findings.append(make_finding(
                column_name,
                data_type,
                "VARCHAR-with-units",
                n_nonnull,
                f"n_with_units={n_with_units:,}/{n_nonnull:,}; n_with_alpha={n_with_alpha:,}/{n_nonnull:,}",
                sample_values(present[present.str.contains(UNIT_VALUE_RE, regex=True, na=False)]),
                "retype DOUBLE + extract/normalize units in mig_169b after approval",
                "CF-mig169-DTYPE-VARCHAR-WITH-UNITS",
                priority="high",
            ))
        elif rec["measurement_name_like"] and n_nonnull > 0 and n_with_alpha == 0 and n_numeric_parseable == n_nonnull:
            findings.append(make_finding(
                column_name,
                data_type,
                "VARCHAR-where-numeric",
                n_nonnull,
                f"n_numeric_parseable={n_numeric_parseable:,}/{n_nonnull:,}; n_unparseable=0",
                sample_values(present),
                "retype DOUBLE in mig_169b after approval",
                "CF-mig169-DTYPE-VARCHAR-WHERE-NUMERIC",
                priority="medium",
            ))
        if bool_subset and (rec["boolean_name_like"] or int(len(unique_norm)) <= 4):
            findings.append(make_finding(
                column_name,
                data_type,
                "VARCHAR-where-BOOLEAN",
                n_nonnull,
                f"distinct normalized values subset of boolean tokens: {'|'.join(sorted(unique_norm))}",
                sample_values(present),
                "retype BOOLEAN or map to explicit enum in mig_169b after approval",
                "CF-mig169-DTYPE-VARCHAR-WHERE-BOOLEAN",
                priority="medium",
            ))
        if rec["date_name_like"]:
            evidence = f"date-name column stored as VARCHAR; n_date_parseable={n_date_parseable:,}/{n_nonnull:,}"
            findings.append(make_finding(
                column_name,
                data_type,
                "VARCHAR-date-column",
                n_nonnull,
                evidence,
                sample_values(present),
                "retype DATE or document as non-date label in mig_169b after approval",
                "CF-mig169-DTYPE-VARCHAR-DATE",
                priority="high",
            ))

    elif dt in TIMESTAMP_TYPES or dt.startswith("TIMESTAMP"):
        rec["sample_values"] = sample_values(series[non_null_mask])
        if rec["date_name_like"] or column_name.lower().endswith("date"):
            parsed = parse_datetimes(series[non_null_mask])
            has_subday = False
            if not parsed.empty:
                has_subday = bool(((parsed.dt.hour != 0) | (parsed.dt.minute != 0) | (parsed.dt.second != 0) | (parsed.dt.microsecond != 0)).any())
            rec["timestamp_has_subday"] = has_subday
            findings.append(make_finding(
                column_name,
                data_type,
                "TIMESTAMP-where-DATE-expected",
                n_nonnull,
                f"clinical date-name column stored as {data_type}; has_subday={has_subday}",
                sample_values(series[non_null_mask]),
                "consider DATE retype if calendar-only clinical date in mig_169b / date-retype lane",
                "CF-mig169-DTYPE-TIMESTAMP-WHERE-DATE",
                priority="medium" if has_subday else "high",
            ))

    elif dt in NUMERIC_TYPES or dt == "DECIMAL":
        rec["sample_values"] = sample_values(series[non_null_mask])
        if rec["count_name_like"] and n_nonnull > 0:
            numeric = pd.to_numeric(series[non_null_mask], errors="coerce")
            valid = numeric.dropna()
            n_integer = int((valid == valid.round()).sum())
            if len(valid) == n_nonnull and n_integer == n_nonnull:
                findings.append(make_finding(
                    column_name,
                    data_type,
                    "DOUBLE-where-INTEGER-expected",
                    n_nonnull,
                    f"count-like name; integer-valued rows={n_integer:,}/{n_nonnull:,}",
                    sample_values(series[non_null_mask]),
                    "consider BIGINT/INTEGER retype in mig_169b if semantically count-valued",
                    "CF-mig169-DTYPE-DOUBLE-WHERE-INTEGER",
                    priority="low",
                ))
    else:
        rec["sample_values"] = sample_values(series[non_null_mask])

    return rec, findings


def make_finding(
    column_name: str,
    data_type: str,
    bucket: str,
    n_nonnull: int,
    evidence: str,
    sample: str,
    proposed_action: str,
    cf_prefix: str,
    *,
    priority: str,
) -> dict[str, Any]:
    return {
        "column_name": column_name,
        "data_type": data_type,
        "bucket": bucket,
        "priority": priority,
        "n_nonnull": int(n_nonnull),
        "evidence": evidence,
        "sample": sample,
        "proposed_action": proposed_action,
        "cf_tag": f"{cf_prefix}-{column_name}",
    }


def bucket_sort_key(bucket: str) -> int:
    order = {
        "VARCHAR-with-units": 1,
        "VARCHAR-date-column": 2,
        "VARCHAR-where-numeric": 3,
        "VARCHAR-where-BOOLEAN": 4,
        "TIMESTAMP-where-DATE-expected": 5,
        "DOUBLE-where-INTEGER-expected": 6,
    }
    return order.get(bucket, 99)


def build_report(out_dir: Path, catalog: pd.DataFrame, inventory: pd.DataFrame, findings: pd.DataFrame, manifest: dict[str, Any]) -> str:
    if findings.empty:
        findings_md = "No findings."
        bucket_md = "No findings."
        high_md = "No high-priority findings."
    else:
        report_findings = findings.copy().sort_values(
            ["bucket_order", "priority_order", "n_nonnull", "column_name"],
            ascending=[True, True, False, True],
        )
        findings_md = markdown_table(report_findings[["column_name", "data_type", "bucket", "priority", "n_nonnull", "evidence", "sample", "proposed_action", "cf_tag"]])
        bucket_counts = (
            findings.groupby("bucket")
            .agg(n_findings=("column_name", "size"), n_nonnull_total=("n_nonnull", "sum"))
            .reset_index()
            .sort_values("bucket", key=lambda s: s.map(bucket_sort_key))
        )
        bucket_md = markdown_table(bucket_counts)
        high_md = markdown_table(report_findings.loc[report_findings["priority"].isin(["high", "medium"]), ["column_name", "bucket", "n_nonnull", "evidence", "cf_tag"]].head(40))

    dtype_counts = catalog["canonical_data_type"].value_counts().rename_axis("data_type").reset_index(name="n_columns")
    dtype_md = markdown_table(dtype_counts)

    measurement_varchar = inventory.loc[(inventory["canonical_data_type"] == "VARCHAR") & (inventory["measurement_name_like"])]
    date_named = inventory.loc[inventory["date_name_like"]]
    count_double = inventory.loc[inventory["canonical_data_type"].isin(NUMERIC_TYPES | {"DECIMAL"}) & inventory["count_name_like"]]
    bool_varchar = inventory.loc[(inventory["canonical_data_type"] == "VARCHAR") & (inventory.get("boolean_value_subset", False) == True)]  # noqa: E712

    return f"""# mig_169 — PM data-type & units sanity audit

**Date:** 2026-04-29  
**Posture:** read-only MotherDuck audit; no database writes; no retypes.  
**Target:** `main.canonical_patient_master` verified analytic columns only.  
**Export directory:** `{out_dir.relative_to(REPO_ROOT)}`

## Executive summary

| Metric | Value |
|---|---:|
| Verified PM columns in audit scope | {len(catalog):,} |
| VARCHAR measurement-name columns inspected | {len(measurement_varchar):,} |
| Date-name columns inspected | {len(date_named):,} |
| Numeric count-like columns inspected | {len(count_double):,} |
| Boolean-like VARCHAR columns inspected | {len(bool_varchar):,} |
| Total dtype/unit findings | {len(findings):,} |
| High-priority findings | {int((findings['priority'] == 'high').sum()) if not findings.empty else 0:,} |
| Medium-priority findings | {int((findings['priority'] == 'medium').sum()) if not findings.empty else 0:,} |
| Low-priority findings | {int((findings['priority'] == 'low').sum()) if not findings.empty else 0:,} |

## Data-type histogram for in-scope verified PM columns

{dtype_md}

## Findings by bucket

{bucket_md}

## Highest-priority findings

{high_md}

## Complete findings table

{findings_md}

## Interpretation

This lane is a **read-only dtype smell audit**, not a schema mutation. Findings are intentionally conservative and should be ratified before any `mig_169b` retype work. The audit excludes provenance/source/raw/evidence/text fields, then scans live CPM values to distinguish unit-bearing text, numeric text, boolean text, timestamp clinical dates, and integer-valued DOUBLE count fields.

Priority guidance:

- **High:** unit-bearing measurements or date-name columns stored as text/TIMESTAMP where downstream analysis could silently mis-handle values.
- **Medium:** parseable numeric or boolean values stored as VARCHAR; likely safe to normalize but still require semantics review.
- **Low:** integer-valued DOUBLE count fields; mostly cosmetic unless join/model code depends on integer typing.

## Recommended follow-up

1. Open `mig_169b` with one approved retype cluster per high/medium bucket. Do not batch all findings into one blind schema migration.
2. Preserve source columns or archive pre-snapshots before any mutation; all retypes must update `cpm_built_at` and `manuscript_workspace.cpm_reconciliation_provenance_v1` per CPM governance.
3. For unit-bearing VARCHAR measurements, parse numeric value and unit separately before choosing the canonical numeric unit.
4. For TIMESTAMP date columns, only retype to DATE when the value is calendar-only clinical truth; keep `build_ts`/provenance timestamps as TIMESTAMP.
5. Treat low-priority DOUBLE count findings as optional unless a downstream consumer requires integer semantics.

## Artifacts

| Artifact | Purpose |
|---|---|
| `{(out_dir / 'pm_dtype_column_catalog.csv').relative_to(REPO_ROOT)}` | One row per in-scope verified PM column with data type, name-pattern, and scan metrics. |
| `{(out_dir / 'pm_dtype_findings.csv').relative_to(REPO_ROOT)}` | Complete finding queue with bucket, evidence, sample, proposed action, and CF tag. |
| `{(out_dir / 'manifest.json').relative_to(REPO_ROOT)}` | Machine-readable run manifest. |
| `{SQL_STUB_PATH.relative_to(REPO_ROOT)}` | Commented read-only probe SQL template for reviewer replay. |

## Run manifest

```json
{json.dumps(manifest, indent=2)}
```
"""


def build_sql_stub(manifest: dict[str, Any]) -> str:
    return f"""-- =============================================================================
-- Migration 169 — PM DATA-TYPE & UNITS SANITY AUDIT PROBES (read-only)
-- =============================================================================
-- Date: 2026-04-29
-- Batch: mig_169_pm_dtype_units_audit_20260429
-- Posture: read-only probe SQL only. Do NOT execute retypes in this lane.
-- Generated by qc_framework_v1/scripts/build_mig169_pm_dtype_units_audit.py
-- Report: qc_framework_v1/reports/mig_169_pm_dtype_units_audit_20260429.md
-- Export dir: {manifest.get('export_dir', '<not-run>')}
-- =============================================================================

-- Scope: verified analytic columns on canonical_patient_master, excluding
-- source/raw/evidence/text/provenance fields.
--
-- SELECT c.column_name, c.data_type
-- FROM information_schema.columns c
-- JOIN main.canonical_column_verification_registry_v1 r
--   ON r.schema_name='main'
--  AND r.table_name=c.table_name
--  AND r.column_name=c.column_name
-- WHERE c.table_catalog='thyroid_canonical_publication_v1_0'
--   AND c.table_schema='main'
--   AND c.table_name='canonical_patient_master'
--   AND r.verification_status='verified'
--   AND c.column_name NOT LIKE '%_source'
--   AND c.column_name NOT LIKE '%_keyword%'
--   AND c.column_name NOT LIKE '%_raw'
--   AND c.column_name NOT LIKE '%_note_ref'
--   AND c.column_name NOT LIKE '%_evidence%'
--   AND c.column_name NOT LIKE 'nlp_%_key_finding'
--   AND c.column_name NOT LIKE '%_text%'
-- ORDER BY c.column_name;

-- -----------------------------------------------------------------------------
-- Bucket A: VARCHAR-with-units
-- Replace <col> with a measurement-name VARCHAR column.
-- -----------------------------------------------------------------------------
-- SELECT '<col>' AS col,
--        COUNT(*) FILTER (WHERE <col> IS NOT NULL AND TRIM(<col>) <> '') AS n_nonnull,
--        COUNT(*) FILTER (WHERE regexp_matches(<col>, '(?i)[0-9]\\s*(cm|mm|mci|ml|kg|mg|g|pg/ml|ng/ml|iu/ml|mg/dl|mmol/l|%)')) AS n_with_units,
--        STRING_AGG(DISTINCT <col>, ' | ' ORDER BY <col>)
--          FILTER (WHERE regexp_matches(<col>, '(?i)[0-9]\\s*(cm|mm|mci|ml|kg|mg|g|pg/ml|ng/ml|iu/ml|mg/dl|mmol/l|%)')) AS sample_unit_values
-- FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- -----------------------------------------------------------------------------
-- Bucket B: TIMESTAMP-where-DATE-expected
-- Replace <col> with a clinical event date-name TIMESTAMP column.
-- -----------------------------------------------------------------------------
-- SELECT '<col>' AS col,
--        COUNT(*) FILTER (WHERE <col> IS NOT NULL) AS n_nonnull,
--        BOOL_OR(EXTRACT(HOUR FROM <col>) <> 0
--             OR EXTRACT(MINUTE FROM <col>) <> 0
--             OR EXTRACT(SECOND FROM <col>) <> 0) AS has_subday,
--        MIN(<col>) AS min_value,
--        MAX(<col>) AS max_value
-- FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- -----------------------------------------------------------------------------
-- Bucket C: DOUBLE-where-INTEGER-expected
-- Replace <col> with a count-like DOUBLE/DECIMAL column.
-- -----------------------------------------------------------------------------
-- SELECT '<col>' AS col,
--        COUNT(*) FILTER (WHERE <col> IS NOT NULL) AS n_nonnull,
--        COUNT(*) FILTER (WHERE <col> IS NOT NULL AND <col> = ROUND(<col>)) AS n_integer_valued,
--        COUNT(DISTINCT <col>) AS n_distinct,
--        MIN(<col>) AS min_value,
--        MAX(<col>) AS max_value
-- FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- -----------------------------------------------------------------------------
-- Bucket D: VARCHAR-where-numeric
-- Replace <col> with a measurement-name VARCHAR column.
-- -----------------------------------------------------------------------------
-- SELECT '<col>' AS col,
--        COUNT(*) FILTER (WHERE <col> IS NOT NULL AND TRIM(<col>) <> '') AS n_nonnull,
--        COUNT(*) FILTER (WHERE TRY_CAST(REPLACE(TRIM(<col>), ',', '') AS DOUBLE) IS NOT NULL) AS n_numeric_parseable,
--        COUNT(*) FILTER (WHERE <col> IS NOT NULL AND TRIM(<col>) <> '' AND TRY_CAST(REPLACE(TRIM(<col>), ',', '') AS DOUBLE) IS NULL) AS n_unparseable
-- FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- -----------------------------------------------------------------------------
-- Bucket E: VARCHAR-where-BOOLEAN
-- Replace <col> with a low-cardinality VARCHAR flag column.
-- -----------------------------------------------------------------------------
-- SELECT '<col>' AS col,
--        COUNT(DISTINCT LOWER(TRIM(<col>))) AS n_distinct,
--        STRING_AGG(DISTINCT LOWER(TRIM(<col>)), '|' ORDER BY LOWER(TRIM(<col>))) AS distinct_values
-- FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master
-- WHERE <col> IS NOT NULL AND TRIM(<col>) <> '';

-- -----------------------------------------------------------------------------
-- Bucket F: Date-name VARCHAR columns
-- Replace <col> with a date-name VARCHAR column. Use a fuller mig_160 parse
-- ladder before any retype; TRY_CAST is only a quick screen.
-- -----------------------------------------------------------------------------
-- SELECT '<col>' AS col,
--        COUNT(*) FILTER (WHERE <col> IS NOT NULL AND TRIM(<col>) <> '') AS n_nonnull,
--        COUNT(*) FILTER (WHERE TRY_CAST(<col> AS DATE) IS NOT NULL) AS n_date_parseable,
--        COUNT(*) FILTER (WHERE <col> IS NOT NULL AND TRIM(<col>) <> '' AND TRY_CAST(<col> AS DATE) IS NULL) AS n_unparseable
-- FROM thyroid_canonical_publication_v1_0.main.canonical_patient_master;

-- End mig_169 read-only probe stub.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=None, help="Optional output directory")
    args = parser.parse_args()

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or (EXPORT_ROOT / f"mig169_pm_dtype_units_audit_{run_ts}")
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    con = connect_locked()
    catalog = fetch_column_catalog(con)
    if catalog.empty:
        raise SystemExit("No verified in-scope columns found for canonical_patient_master")

    audited_columns = catalog["column_name"].tolist()
    pm_df = fetch_pm_dataframe(con, audited_columns)

    inventory_rows: list[dict[str, Any]] = []
    finding_rows: list[dict[str, Any]] = []
    data_type_by_col = catalog.set_index("column_name")["data_type"].to_dict()
    for col in audited_columns:
        rec, findings = summarize_column(pm_df[col], col, str(data_type_by_col[col]))
        inventory_rows.append(rec)
        finding_rows.extend(findings)

    inventory = catalog.merge(pd.DataFrame(inventory_rows), on=["column_name", "data_type", "canonical_data_type"], how="left")
    findings = pd.DataFrame(finding_rows)
    if not findings.empty:
        findings["bucket_order"] = findings["bucket"].map(bucket_sort_key)
        priority_order = {"high": 1, "medium": 2, "low": 3}
        findings["priority_order"] = findings["priority"].map(priority_order).fillna(9).astype(int)
        findings = findings.sort_values(["bucket_order", "priority_order", "n_nonnull", "column_name"], ascending=[True, True, False, True]).reset_index(drop=True)

    inventory.to_csv(out_dir / "pm_dtype_column_catalog.csv", index=False)
    findings.to_csv(out_dir / "pm_dtype_findings.csv", index=False)

    bucket_counts = findings["bucket"].value_counts().to_dict() if not findings.empty else {}
    priority_counts = findings["priority"].value_counts().to_dict() if not findings.empty else {}
    manifest = {
        "migration": "mig_169",
        "run_timestamp_utc": run_ts,
        "posture": "read_only_motherduck_audit_no_db_writes",
        "target_table": "main.canonical_patient_master",
        "verified_pm_columns_audited": int(len(catalog)),
        "findings_total": int(len(findings)),
        "findings_by_bucket": {str(k): int(v) for k, v in bucket_counts.items()},
        "findings_by_priority": {str(k): int(v) for k, v in priority_counts.items()},
        "export_dir": str(out_dir.relative_to(REPO_ROOT)),
        "artifacts": [
            "pm_dtype_column_catalog.csv",
            "pm_dtype_findings.csv",
            "manifest.json",
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    report = build_report(out_dir, catalog, inventory, findings, manifest)
    REPORT_PATH.write_text(report)
    SQL_STUB_PATH.write_text(build_sql_stub(manifest))

    print(json.dumps(manifest, indent=2))
    print(f"Report: {REPORT_PATH}")
    print(f"SQL stub: {SQL_STUB_PATH}")
    print(f"Exports: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
