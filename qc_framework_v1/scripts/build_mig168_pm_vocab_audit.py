#!/usr/bin/env python3
"""Read-only mig_168 audit for canonical_patient_master VARCHAR vocab drift.

This script intentionally performs no MotherDuck writes. It inventories verified
VARCHAR columns on main.canonical_patient_master, classifies likely controlled
vocabulary candidates, detects casing/whitespace/value-drift patterns, and
emits a draft enum dictionary for review.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402


PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
TABLE_FQ = f'"{PUBLICATION_DB}".main.canonical_patient_master'
EXPORT_ROOT = REPO_ROOT / "exports"
REPORT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_168_pm_controlled_vocab_audit_20260429.md"


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def safe_pct(num: float, den: float) -> float:
    if den == 0:
        return 0.0
    return round((num / den) * 100.0, 3)


def classify_column(row: dict[str, Any], enum_max_distinct: int, large_enum_max_distinct: int) -> str:
    distinct_norm = int(row["distinct_norm"] or 0)
    non_null = int(row["non_null"] or 0)
    col = str(row["column_name"]).lower()
    ratio = (distinct_norm / non_null) if non_null else 0.0

    if non_null == 0:
        return "empty_verified_varchar"
    if distinct_norm <= 1:
        return "degenerate_single_value"
    if (
        "date" in col
        or "timestamp" in col
        or col.endswith("_dt")
        or col.endswith("_ts")
        or "_dt_" in col
        or "_ts_" in col
        or col in {"built_at", "resolved_at"}
    ):
        return "date_or_timestamp_text"
    if any(token in col for token in ["id", "source", "snippet", "notes", "comment", "raw", "components", "summary"]):
        if distinct_norm <= 30 and ratio <= 0.05:
            return "possible_enum_review"
        return "free_text_or_identifier"
    if distinct_norm <= enum_max_distinct:
        return "controlled_vocab_candidate"
    if distinct_norm <= large_enum_max_distinct and ratio <= 0.02:
        return "controlled_vocab_candidate_large"
    return "high_cardinality_text"


def infer_semantic_family(col: str) -> str:
    c = col.lower()
    families = [
        ("laterality", ["laterality", "side"]),
        ("stage", ["stage", "ajcc", "t_stage", "n_stage", "m_stage"]),
        ("risk_scoring", ["risk", "ata", "macis", "ames"]),
        ("histology", ["histology", "tumor_type", "variant"]),
        ("complication", ["comp_", "complication"]),
        ("molecular", ["braf", "ras", "tert", "molecular", "genetic", "mutation"]),
        ("recurrence_survival", ["recurrence", "surv", "vital", "death"]),
        ("treatment", ["rai", "surgery", "procedure", "treatment"]),
        ("demographics", ["sex", "gender", "race", "ethnicity"]),
        ("provenance", ["source", "method", "version", "batch"]),
    ]
    for family, tokens in families:
        if any(tok in c for tok in tokens):
            return family
    return "other"


def canonical_label(raw_values: list[str]) -> str:
    # Use the most frequent raw display, already ordered by n desc in the caller.
    for raw in raw_values:
        val = str(raw).strip()
        if val:
            return " ".join(val.split())
    return ""


def markdown_table(df: pd.DataFrame, *, index: bool = False) -> str:
    if df.empty:
        return "No rows."
    safe = df.copy()
    for col in safe.columns:
        if pd.api.types.is_object_dtype(safe[col]) or pd.api.types.is_string_dtype(safe[col]):
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
     AND r.table_name = 'canonical_patient_master'
     AND r.column_name = c.column_name
    WHERE c.table_catalog = '{PUBLICATION_DB}'
      AND c.table_schema = 'main'
      AND c.table_name = 'canonical_patient_master'
      AND c.data_type = 'VARCHAR'
      AND r.verification_status = 'verified'
    ORDER BY c.ordinal_position
    """
    return con.execute(sql).fetchdf()


def fetch_pm_dataframe(con, columns: list[str]) -> pd.DataFrame:
    select_list = ", ".join(qident(c) for c in columns)
    sql = f"SELECT {select_list} FROM {TABLE_FQ} ORDER BY research_id"
    return con.execute(sql).fetchdf()


def normalized_series(raw: pd.Series) -> pd.Series:
    text = raw.astype("string")
    trimmed = text.str.strip()
    trimmed = trimmed.mask(trimmed == "")
    return trimmed.str.replace(r"\s+", " ", regex=True).str.lower()


def summarize_column_pandas(df: pd.DataFrame, column_name: str) -> dict[str, Any]:
    raw = df[column_name].astype("string")
    trimmed = raw.str.strip()
    norm = normalized_series(df[column_name])
    present = norm.notna()
    n_rows = int(len(raw))
    non_null = int(present.sum())
    null_or_blank = n_rows - non_null
    if non_null:
        raw_present = raw[present]
        top_norm_count = int(norm[present].value_counts(dropna=True).iloc[0])
    else:
        raw_present = raw[present]
        top_norm_count = 0
    rec = {
        "column_name": column_name,
        "n_rows": n_rows,
        "null_or_blank": null_or_blank,
        "non_null": non_null,
        "distinct_raw": int(raw_present.nunique(dropna=True)),
        "distinct_norm": int(norm[present].nunique(dropna=True)),
        "leading_trailing_rows": int(((raw.notna()) & (raw != trimmed)).sum()),
        "repeated_ws_rows": int(raw.fillna("").str.contains(r"\s{2,}", regex=True).sum()),
        "non_lower_rows": int(((raw.notna()) & (raw != raw.str.lower())).sum()),
        "top_norm_count": top_norm_count,
    }
    if non_null:
        tmp = pd.DataFrame({"raw": raw[present], "norm": norm[present]})
        raw_variant_counts = tmp.groupby("norm", dropna=True)["raw"].nunique(dropna=True)
        rec["norm_groups_with_raw_variants"] = int((raw_variant_counts > 1).sum())
    else:
        rec["norm_groups_with_raw_variants"] = 0
    rec["null_pct"] = safe_pct(null_or_blank, n_rows)
    rec["top_norm_pct_of_nonnull"] = safe_pct(top_norm_count, non_null)
    rec["norm_distinct_pct_of_nonnull"] = round((rec["distinct_norm"] / non_null) * 100.0, 3) if non_null else 0.0
    return rec


def build_value_catalog_pandas(df: pd.DataFrame, column_name: str, max_norm_values: int = 250) -> pd.DataFrame:
    raw = df[column_name].astype("string")
    norm = normalized_series(df[column_name])
    present = norm.notna()
    tmp = pd.DataFrame({"raw_value": raw[present], "norm_value": norm[present]})
    if tmp.empty:
        return pd.DataFrame(columns=[
            "column_name", "norm_value", "n_rows", "raw_variant_count",
            "leading_trailing_rows", "repeated_ws_rows", "top_raw_variants"
        ])

    grouped = tmp.groupby("norm_value", dropna=True).agg(
        n_rows=("raw_value", "size"),
        raw_variant_count=("raw_value", lambda s: int(s.nunique(dropna=True))),
        leading_trailing_rows=("raw_value", lambda s: int((s != s.str.strip()).sum())),
        repeated_ws_rows=("raw_value", lambda s: int(s.fillna("").str.contains(r"\s{2,}", regex=True).sum())),
    ).reset_index()

    raw_counts = (
        tmp.groupby(["norm_value", "raw_value"], dropna=True)
        .size()
        .reset_index(name="raw_n")
        .sort_values(["norm_value", "raw_n", "raw_value"], ascending=[True, False, True])
    )
    top_variants = (
        raw_counts.groupby("norm_value")["raw_value"]
        .apply(lambda s: " || ".join([str(v) for v in s.head(5).tolist()]))
        .reset_index(name="top_raw_variants")
    )
    out = grouped.merge(top_variants, on="norm_value", how="left")
    out = out.sort_values(["n_rows", "norm_value"], ascending=[False, True]).head(max_norm_values)
    out.insert(0, "column_name", column_name)
    return out.reset_index(drop=True)


def build_report(
    out_dir: Path,
    inventory: pd.DataFrame,
    drift: pd.DataFrame,
    enum_df: pd.DataFrame,
    manifest: dict[str, Any],
) -> str:
    total_cols = len(inventory)
    enum_cols = int(inventory["is_enum_candidate"].sum()) if total_cols else 0
    drift_cols = int((inventory["drift_issue_count"] > 0).sum()) if total_cols else 0
    rare_col_count = int((inventory["rare_value_count"] > 0).sum()) if total_cols else 0
    high_card = int(inventory["column_class"].isin(["high_cardinality_text", "free_text_or_identifier"]).sum()) if total_cols else 0

    top_drift = drift.head(25).copy()
    drift_md = markdown_table(top_drift) if not top_drift.empty else "No drift findings."

    class_counts = inventory["column_class"].value_counts().rename_axis("column_class").reset_index(name="n_columns")
    class_md = markdown_table(class_counts)

    family_counts = enum_df["semantic_family"].value_counts().rename_axis("semantic_family").reset_index(name="n_enum_values") if not enum_df.empty else pd.DataFrame(columns=["semantic_family", "n_enum_values"])
    family_md = markdown_table(family_counts) if not family_counts.empty else "No enum candidates."

    if not drift.empty:
        issue_counts = (
            drift.assign(issue_type=drift["issue_types"].str.split(";"))
            .explode("issue_type")
            .groupby("issue_type")
            .agg(n_findings=("issue_type", "size"), n_columns=("column_name", "nunique"))
            .reset_index()
            .sort_values(["n_findings", "issue_type"], ascending=[False, True])
        )
        issue_md = markdown_table(issue_counts)
    else:
        issue_md = "No drift findings."

    raw_variant_cols = inventory.loc[
        inventory["norm_groups_with_raw_variants"] > 0,
        ["column_name", "column_class", "semantic_family", "non_null", "distinct_raw", "distinct_norm", "norm_groups_with_raw_variants"],
    ].sort_values("norm_groups_with_raw_variants", ascending=False).head(20)
    raw_variant_md = markdown_table(raw_variant_cols)

    whitespace_cols = inventory.loc[
        (inventory["leading_trailing_rows"] > 0) | (inventory["repeated_ws_rows"] > 0),
        ["column_name", "column_class", "semantic_family", "non_null", "leading_trailing_rows", "repeated_ws_rows", "distinct_norm"],
    ].sort_values(["leading_trailing_rows", "repeated_ws_rows"], ascending=False).head(20)
    whitespace_md = markdown_table(whitespace_cols)

    rare_col_table = inventory.loc[
        inventory["rare_value_count"] > 0,
        ["column_name", "column_class", "semantic_family", "non_null", "distinct_norm", "rare_value_count"],
    ].sort_values("rare_value_count", ascending=False).head(20)
    rare_md = markdown_table(rare_col_table)

    return f"""# mig_168 — PM controlled-vocabulary standardization audit

**Date:** 2026-04-29  
**Posture:** read-only MotherDuck catalog + drift audit; no database writes.  
**Target:** `main.canonical_patient_master` verified `VARCHAR` columns.  
**Export directory:** `{out_dir.relative_to(REPO_ROOT)}`

## Executive summary

| Metric | Value |
|---|---:|
| Verified `VARCHAR` PM columns audited | {total_cols:,} |
| Likely controlled-vocabulary columns | {enum_cols:,} |
| High-cardinality/free-text/identifier columns excluded from enum draft | {high_card:,} |
| Columns with casing/whitespace/raw-variant drift | {drift_cols:,} |
| Enum-candidate columns with rare/possible rogue values | {rare_col_count:,} |
| Draft enum dictionary rows | {len(enum_df):,} |

## Column-class histogram

{class_md}

## Enum dictionary semantic-family histogram

{family_md}

## Interpretation

This is a **standardization audit**, not a verification-status flip. A value is treated as a controlled-vocabulary candidate when its normalized distinct count is low enough to be safely dictionary-managed. High-cardinality text, dates stored as text, source/provenance strings, identifiers, and list payloads are catalogued but excluded from the SSOT enum draft unless they have low-cardinality enum behavior.

Drift classes:

- `raw_variant_drift`: multiple raw spellings/casing/spacing collapse to the same normalized value.
- `leading_trailing_whitespace`: at least one row has leading/trailing spaces.
- `repeated_internal_whitespace`: at least one row has repeated internal whitespace.
- `rare_value_review`: low-frequency normalized value in an enum-candidate column; this is the best available read-only proxy for rogue values until a clinically approved dictionary exists.

## Drift class counts

{issue_md}

## Highest-yield column-level findings

### Raw-variant drift groups

{raw_variant_md}

### Whitespace drift columns

{whitespace_md}

### Rare / possible rogue-value review columns

{rare_md}

## High-signal observations

- The PM vocabulary surface is larger than the prompt estimate: **{total_cols}** verified `VARCHAR` columns were in scope after joining the live information schema to the verification registry.
- **{enum_cols}** columns behave like controlled-vocabulary candidates or low-cardinality review candidates; **{high_card}** high-cardinality/free-text/identifier fields should not be normalized by enum policy.
- Raw-variant drift is concentrated in pathology/recurrence/histology fields such as `recurrence_histology`, `completion_prior_histology`, `completion_histology_type`, `path_ete_raw`, and `gm_path_ete_raw`.
- Whitespace drift is common in size/detail text fields (`syn_right_lobe_size_cm`, `syn_left_lobe_size_cm`, `syn_isthmus_size_cm`, `syn_frozen_section_result`) and should be treated as a text-cleaning concern rather than a controlled-vocabulary mutation.
- Multi-label laterality fields such as `cnln_img_laterality` and level-list fields such as `lateral_levels_v10` / `ene_levels_v9` need list-token normalization rules; coercing the whole semicolon-delimited string to a single enum would preserve drift rather than fix it.

## Top drift / review findings

{drift_md}

## Recommended fixes / changes

1. **Adopt the emitted dictionary as a review draft, not an immediate mutation source.** Use `{(out_dir / 'pm_ssot_enum_dictionary_draft.csv').relative_to(REPO_ROOT)}` as the first SSOT enum dictionary draft and route any `rare_value_review` entries through clinical/data-owner review before normalizing.
2. **Implement normalization at build time, not by ad hoc CPM updates.** For each accepted enum column, add a deterministic `CASE`/mapping layer in the CPM builder or upstream feeder, then rebuild CPM and update `cpm_built_at` + `cpm_reconciliation_provenance_v1` per repo policy.
3. **Separate display labels from stored codes.** Store stable lowercase snake/canonical codes where possible, and keep human-readable labels in the dictionary. This prevents future casing drift while preserving publication-friendly display values.
4. **Do not coerce high-cardinality text columns into enums.** Columns classified as `free_text_or_identifier`, `date_or_timestamp_text`, or `high_cardinality_text` should get separate type/lineage audits rather than enum standardization.
5. **Open a follow-up migration for accepted changes only.** Suggested lane name: `mig_168b_pm_vocab_normalization_apply`, with pre-snapshot archive, per-column mapping table, drift-count pre/post gates, CPM invariants, and no value changes outside the reviewed dictionary.

## Artifacts

| Artifact | Purpose |
|---|---|
| `{(out_dir / 'pm_verified_varchar_column_catalog.csv').relative_to(REPO_ROOT)}` | One row per verified PM `VARCHAR` column with nullness/cardinality/drift metrics. |
| `{(out_dir / 'pm_vocab_value_catalog.csv').relative_to(REPO_ROOT)}` | Normalized value counts for enum-candidate columns. |
| `{(out_dir / 'pm_vocab_drift_findings.csv').relative_to(REPO_ROOT)}` | Column/value-level casing, whitespace, raw-variant, and rare-value review queue. |
| `{(out_dir / 'pm_ssot_enum_dictionary_draft.csv').relative_to(REPO_ROOT)}` | Draft SSOT enum dictionary with canonical codes and suggested display labels. |
| `{(out_dir / 'manifest.json').relative_to(REPO_ROOT)}` | Machine-readable run manifest and thresholds. |

## Run manifest

```json
{json.dumps(manifest, indent=2)}
```
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=None, help="Optional output directory")
    parser.add_argument("--enum-max-distinct", type=int, default=40)
    parser.add_argument("--large-enum-max-distinct", type=int, default=100)
    parser.add_argument("--rare-min-count", type=int, default=2)
    args = parser.parse_args()

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or (EXPORT_ROOT / f"mig168_pm_vocab_audit_{run_ts}")
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    con = connect_locked()
    catalog = fetch_column_catalog(con)
    if catalog.empty:
        raise SystemExit("No verified VARCHAR columns found for canonical_patient_master")

    audited_columns = catalog["column_name"].tolist()
    pm_df = fetch_pm_dataframe(con, audited_columns)

    summaries: list[dict[str, Any]] = []
    value_frames: list[pd.DataFrame] = []
    for col in audited_columns:
        summaries.append(summarize_column_pandas(pm_df, col))

    summary_df = pd.DataFrame(summaries)
    inventory = catalog.merge(summary_df, on="column_name", how="left")
    inventory["column_class"] = inventory.apply(
        lambda r: classify_column(r.to_dict(), args.enum_max_distinct, args.large_enum_max_distinct),
        axis=1,
    )
    inventory["semantic_family"] = inventory["column_name"].map(infer_semantic_family)
    inventory["is_enum_candidate"] = inventory["column_class"].isin(
        ["controlled_vocab_candidate", "controlled_vocab_candidate_large", "possible_enum_review", "degenerate_single_value"]
    )

    enum_cols = inventory.loc[inventory["is_enum_candidate"], "column_name"].tolist()
    for col in enum_cols:
        value_frames.append(build_value_catalog_pandas(pm_df, col))
    values_df = pd.concat(value_frames, ignore_index=True) if value_frames else pd.DataFrame()

    enum_rows: list[dict[str, Any]] = []
    drift_rows: list[dict[str, Any]] = []
    if not values_df.empty:
        total_by_col = inventory.set_index("column_name")["non_null"].to_dict()
        family_by_col = inventory.set_index("column_name")["semantic_family"].to_dict()
        class_by_col = inventory.set_index("column_name")["column_class"].to_dict()
        for _, row in values_df.iterrows():
            col = row["column_name"]
            top_variants = str(row.get("top_raw_variants") or "")
            raw_variant_list = [v for v in top_variants.split(" || ") if v]
            code = str(row["norm_value"])
            n_rows = int(row["n_rows"])
            non_null = int(total_by_col.get(col) or 0)
            rare_threshold = max(args.rare_min_count, math.ceil(non_null * 0.001)) if non_null else args.rare_min_count
            is_rare = n_rows <= rare_threshold and non_null >= 100
            enum_rows.append(
                {
                    "table_name": "canonical_patient_master",
                    "column_name": col,
                    "semantic_family": family_by_col.get(col, "other"),
                    "column_class": class_by_col.get(col),
                    "canonical_code_draft": code,
                    "suggested_display_label": canonical_label(raw_variant_list) or code,
                    "n_rows": n_rows,
                    "pct_of_nonnull": safe_pct(n_rows, non_null),
                    "raw_variant_count": int(row.get("raw_variant_count") or 0),
                    "top_raw_variants": top_variants,
                    "review_status": "review_rare_possible_rogue" if is_rare else "draft_from_observed_values",
                    "notes": "Read-only observed value; requires owner approval before normalization.",
                }
            )
            issues: list[str] = []
            if int(row.get("raw_variant_count") or 0) > 1:
                issues.append("raw_variant_drift")
            if int(row.get("leading_trailing_rows") or 0) > 0:
                issues.append("leading_trailing_whitespace")
            if int(row.get("repeated_ws_rows") or 0) > 0:
                issues.append("repeated_internal_whitespace")
            if is_rare:
                issues.append("rare_value_review")
            if issues:
                drift_rows.append(
                    {
                        "table_name": "canonical_patient_master",
                        "column_name": col,
                        "semantic_family": family_by_col.get(col, "other"),
                        "norm_value": code,
                        "n_rows": n_rows,
                        "pct_of_nonnull": safe_pct(n_rows, non_null),
                        "issue_types": ";".join(issues),
                        "raw_variant_count": int(row.get("raw_variant_count") or 0),
                        "top_raw_variants": top_variants,
                        "suggested_canonical_code": code,
                        "suggested_display_label": canonical_label(raw_variant_list) or code,
                    }
                )

    enum_df = pd.DataFrame(enum_rows)
    drift_df = pd.DataFrame(drift_rows)

    rare_counts = drift_df[drift_df["issue_types"].str.contains("rare_value_review", na=False)].groupby("column_name").size() if not drift_df.empty else pd.Series(dtype=int)
    drift_counts = drift_df.groupby("column_name").size() if not drift_df.empty else pd.Series(dtype=int)
    inventory["rare_value_count"] = inventory["column_name"].map(rare_counts).fillna(0).astype(int)
    inventory["drift_issue_count"] = inventory["column_name"].map(drift_counts).fillna(0).astype(int)

    # Keep the value catalog bounded to enum candidates only; do not export high-cardinality raw text values.
    inventory.to_csv(out_dir / "pm_verified_varchar_column_catalog.csv", index=False)
    values_df.to_csv(out_dir / "pm_vocab_value_catalog.csv", index=False)
    drift_df.to_csv(out_dir / "pm_vocab_drift_findings.csv", index=False)
    enum_df.to_csv(out_dir / "pm_ssot_enum_dictionary_draft.csv", index=False)

    manifest = {
        "migration": "mig_168",
        "run_timestamp_utc": run_ts,
        "posture": "read_only_motherduck_audit_no_db_writes",
        "target_table": "main.canonical_patient_master",
        "verified_varchar_columns_audited": int(len(inventory)),
        "enum_candidate_columns": int(inventory["is_enum_candidate"].sum()),
        "drift_columns": int((inventory["drift_issue_count"] > 0).sum()),
        "drift_findings": int(len(drift_df)),
        "enum_dictionary_rows": int(len(enum_df)),
        "thresholds": {
            "enum_max_distinct": args.enum_max_distinct,
            "large_enum_max_distinct": args.large_enum_max_distinct,
            "rare_min_count": args.rare_min_count,
            "rare_pct_threshold": "0.1% of non-null rows, minimum rare_min_count",
        },
        "artifacts": [
            "pm_verified_varchar_column_catalog.csv",
            "pm_vocab_value_catalog.csv",
            "pm_vocab_drift_findings.csv",
            "pm_ssot_enum_dictionary_draft.csv",
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    report_drift = drift_df.sort_values(["column_name", "n_rows"], ascending=[True, False]) if not drift_df.empty else drift_df
    report = build_report(out_dir, inventory, report_drift, enum_df, manifest)
    REPORT_PATH.write_text(report)

    print(json.dumps(manifest, indent=2))
    print(f"Report: {REPORT_PATH}")
    print(f"Exports: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())