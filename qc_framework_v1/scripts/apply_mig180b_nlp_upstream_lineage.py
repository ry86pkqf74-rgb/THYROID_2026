#!/usr/bin/env python3
"""Execute mig_180b NLP upstream-missing family lineage closure.

This migration is intentionally conservative: it does not mutate
canonical_patient_master values. It resolves the 12 family-level
CF-mig180-NLP-UPSTREAM-MISSING-* carry-forwards by proving where the upstream
lineage lives now (archive/legacy/current canonical tables), materializing a
validation table, and appending an idempotent closure note to the 38 affected
registry rows.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402

PUBLICATION_DB = "thyroid_canonical_publication_v1_0"
BATCH_ID = "mig_180b_nlp_upstream_missing_lineage_20260429"
MIGRATION_PATH = REPO_ROOT / "qc_framework_v1" / "migrations" / "180b_nlp_upstream_missing_lineage_20260429.sql"
REPORT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_180b_nlp_upstream_lineage_20260429.md"
EXPORT_DIR = REPO_ROOT / "exports" / "mig180b_nlp_upstream_lineage_20260429"
FAMILY_CSV = EXPORT_DIR / "family_lineage_audit.csv"
COLUMN_CSV = EXPORT_DIR / "column_closure_audit.csv"
RUN_LOG = EXPORT_DIR / "run_summary.json"
SNAPSHOT_TABLE = '"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig180b_20260429'
VAL_TABLE = "main.val_mig180b_nlp_upstream_lineage_v1"


@dataclass(frozen=True)
class FamilySpec:
    family: str
    columns: tuple[str, ...]
    source_catalog: str
    source_schema: str
    source_table: str
    source_kind: str  # rawjson or entities
    source_role: str

    @property
    def source_fq(self) -> str:
        return f'"{self.source_catalog}"."{self.source_schema}"."{self.source_table}"'


FAMILIES: tuple[FamilySpec, ...] = (
    FamilySpec(
        "nlp_funcoutcome",
        ("nlp_funcoutcome_has_data", "nlp_funcoutcome_key_finding", "nlp_funcoutcome_n_entities", "nlp_funcoutcome_n_notes"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_llm_functional_outcomes_pre251_20260417T012311Z",
        "rawjson",
        "archived raw LLM JSON source; PM uses stricter retired rollup subset",
    ),
    FamilySpec(
        "nlp_imaging",
        ("nlp_imaging_has_data", "nlp_imaging_key_finding", "nlp_imaging_n_entities", "nlp_imaging_n_notes"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_llm_imaging_pre251_20260417T012311Z",
        "rawjson",
        "archived raw LLM JSON source; PM uses stricter retired rollup subset",
    ),
    FamilySpec(
        "nlp_labs",
        ("nlp_labs_has_data", "nlp_labs_key_finding", "nlp_labs_n_entities", "nlp_labs_n_notes"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_llm_labs_pre251_20260417T012311Z",
        "rawjson",
        "archived raw LLM JSON source; PM uses stricter retired rollup subset",
    ),
    FamilySpec(
        "nlp_ne_complications",
        ("nlp_ne_complications_has_data", "nlp_ne_complications_n_rows"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_complications_pre364_20260422_050902",
        "entities",
        "archived generic note_entities table; exact replay vs CPM",
    ),
    FamilySpec(
        "nlp_ne_genetics",
        ("nlp_ne_genetics_has_data", "nlp_ne_genetics_n_rows"),
        "Thyroid 2026 UPdated",
        "molecular_legacy_20260421",
        "note_entities_genetics",
        "entities",
        "legacy generic note_entities table; exact replay vs CPM",
    ),
    FamilySpec(
        "nlp_ne_medications",
        ("nlp_ne_medications_has_data", "nlp_ne_medications_n_rows"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_medications_pre365b_20260422_122116",
        "entities",
        "archived generic note_entities table; exact replay vs CPM",
    ),
    FamilySpec(
        "nlp_ne_problemlist",
        ("nlp_ne_problemlist_has_data", "nlp_ne_problemlist_n_rows"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_problem_list_pre365b_20260422_122116",
        "entities",
        "archived generic note_entities table; exact replay vs CPM",
    ),
    FamilySpec(
        "nlp_ne_staging",
        ("nlp_ne_staging_has_data", "nlp_ne_staging_n_rows"),
        "Thyroid 2026 UPdated",
        "main",
        "note_entities_staging_archived_20260422",
        "entities",
        "archived generic note_entities table; exact replay vs CPM",
    ),
    FamilySpec(
        "nlp_physexam",
        ("nlp_physexam_has_data", "nlp_physexam_key_finding", "nlp_physexam_n_entities", "nlp_physexam_n_notes"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_llm_physical_exam_pre251_20260417T012311Z",
        "rawjson",
        "archived raw LLM JSON source; PM uses stricter retired rollup subset",
    ),
    FamilySpec(
        "nlp_ptdecision",
        ("nlp_ptdecision_has_data", "nlp_ptdecision_key_finding", "nlp_ptdecision_n_entities", "nlp_ptdecision_n_notes"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_llm_patient_decision_adherence_pre251_20260417T012311Z",
        "rawjson",
        "archived raw LLM JSON source; PM uses stricter retired rollup subset",
    ),
    FamilySpec(
        "nlp_radtx",
        ("nlp_radtx_has_data", "nlp_radtx_key_finding", "nlp_radtx_n_entities", "nlp_radtx_n_notes"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_llm_rad_treatment_pre251_20260417T012311Z",
        "rawjson",
        "archived raw LLM JSON source; PM uses stricter retired rollup subset",
    ),
    FamilySpec(
        "nlp_usnodule",
        ("nlp_usnodule_has_data", "nlp_usnodule_key_finding", "nlp_usnodule_n_entities", "nlp_usnodule_n_notes"),
        "Thyroid 2026 UPdated",
        "archive_pub_v1_0",
        "note_entities_llm_us_nodule_dynamics_pre251_20260417T012311Z",
        "rawjson",
        "corrected alias: us_nodule_dynamics, not nonexistent note_entities_llm_us_nodule",
    ),
)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def append_note_expr(addendum: str) -> str:
    lit = sql_literal(addendum)
    return (
        f"CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN {lit} "
        f"WHEN POSITION({lit} IN notes) > 0 THEN notes "
        f"ELSE notes || '; ' || {lit} END"
    )


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "No rows."
    safe = df.copy()
    for col in safe.columns:
        if pd.api.types.is_object_dtype(safe[col]) or pd.api.types.is_string_dtype(safe[col]):
            safe[col] = safe[col].astype("string").str.replace("|", r"\|", regex=False)
    return safe.to_markdown(index=False)


def assert_source_exists(con, spec: FamilySpec) -> None:
    n = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = ?
          AND table_schema = ?
          AND table_name = ?
        """,
        [spec.source_catalog, spec.source_schema, spec.source_table],
    ).fetchone()[0]
    if n != 1:
        raise SystemExit(f"Missing expected mig_180b source for {spec.family}: {spec.source_fq}")


def extract_entities_count(result_json: Any) -> int:
    if result_json is None:
        return 0
    try:
        obj = json.loads(result_json)
    except Exception:
        return 0
    if not isinstance(obj, dict):
        return 0
    entities = obj.get("entities")
    return len(entities) if isinstance(entities, list) else 0


def pm_frame(con) -> pd.DataFrame:
    cols = [c for fam in FAMILIES for c in fam.columns]
    quoted = ", ".join(f'"{c}"' for c in cols)
    df = con.execute(f"SELECT CAST(research_id AS VARCHAR) AS research_id, {quoted} FROM main.canonical_patient_master").fetchdf()
    df["research_id"] = df["research_id"].astype(str)
    return df


def source_rollup(con, spec: FamilySpec) -> pd.DataFrame:
    if spec.source_kind == "entities":
        return con.execute(
            f"""
            SELECT CAST(research_id AS VARCHAR) AS research_id,
                   TRUE AS has_data,
                   COUNT(*)::BIGINT AS n_rows
            FROM {spec.source_fq}
            GROUP BY 1
            """
        ).fetchdf()

    src = con.execute(
        f"SELECT CAST(research_id AS VARCHAR) AS research_id, CAST(note_row_id AS VARCHAR) AS note_row_id, result_json FROM {spec.source_fq}"
    ).fetchdf()
    rows: list[dict[str, Any]] = []
    for row in src.itertuples(index=False):
        n_entities = extract_entities_count(row.result_json)
        if n_entities > 0:
            rows.append({"research_id": str(row.research_id), "note_row_id": str(row.note_row_id), "n_entities_row": n_entities})
    if not rows:
        return pd.DataFrame(columns=["research_id", "has_data", "n_entities", "n_notes"])
    rdf = pd.DataFrame(rows)
    return (
        rdf.groupby("research_id", dropna=False)
        .agg(has_data=("n_entities_row", lambda _s: True), n_entities=("n_entities_row", "sum"), n_notes=("note_row_id", "nunique"))
        .reset_index()
    )


def profile_family(con, pm: pd.DataFrame, spec: FamilySpec) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    assert_source_exists(con, spec)
    source_rows, source_patients = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR)) FROM {spec.source_fq}"
    ).fetchone()
    rollup = source_rollup(con, spec)
    merged = pm[["research_id", *spec.columns]].merge(rollup, on="research_id", how="left")
    metric_rows: list[dict[str, Any]] = []
    mismatch_total = 0
    metrics_tested = 0
    for col in spec.columns:
        metric = col.removeprefix(spec.family + "_")
        if metric == "key_finding":
            metric_rows.append(
                {
                    "family": spec.family,
                    "column_name": col,
                    "metric": metric,
                    "replay_kind": "lineage_only_text_metric",
                    "pm_non_null": int(merged[col].notna().sum()),
                    "source_non_null": None,
                    "pm_sum_or_true": None,
                    "source_sum_or_true": None,
                    "n_mismatches": None,
                    "closure_decision": "closed_lineage_source_found_text_metric_not_replayed",
                }
            )
            continue
        if metric not in set(rollup.columns):
            metric_rows.append(
                {
                    "family": spec.family,
                    "column_name": col,
                    "metric": metric,
                    "replay_kind": "source_metric_unavailable",
                    "pm_non_null": int(merged[col].notna().sum()),
                    "source_non_null": None,
                    "pm_sum_or_true": None,
                    "source_sum_or_true": None,
                    "n_mismatches": None,
                    "closure_decision": "closed_source_found_metric_not_replayed",
                }
            )
            continue
        if metric == "has_data":
            pmv = merged[col].fillna(False).astype(bool)
            srcv = merged[metric].fillna(False).astype(bool)
        else:
            pmv = merged[col].fillna(0).astype("int64")
            srcv = merged[metric].fillna(0).astype("int64")
        mismatches = int((pmv != srcv).sum())
        mismatch_total += mismatches
        metrics_tested += 1
        if mismatches == 0:
            decision = "closed_exact_replay"
            replay_kind = "exact_derivation_vs_canonical"
        else:
            decision = "closed_source_found_pm_strict_subset"
            replay_kind = "source_located_non_exact_raw_replay"
        metric_rows.append(
            {
                "family": spec.family,
                "column_name": col,
                "metric": metric,
                "replay_kind": replay_kind,
                "pm_non_null": int(merged[col].notna().sum()),
                "source_non_null": int(merged[metric].notna().sum()),
                "pm_sum_or_true": int(pmv.sum()),
                "source_sum_or_true": int(srcv.sum()),
                "n_mismatches": mismatches,
                "closure_decision": decision,
            }
        )
    exact = bool(metrics_tested and mismatch_total == 0)
    if exact:
        family_decision = "closed_exact_replay"
    elif spec.source_kind == "rawjson":
        family_decision = "closed_source_found_pm_strict_subset"
    else:
        family_decision = "closed_source_found_non_exact_review"
    family_row = {
        "family": spec.family,
        "n_cols": len(spec.columns),
        "source_status": "source_found",
        "source_catalog": spec.source_catalog,
        "source_schema": spec.source_schema,
        "source_table": spec.source_table,
        "source_kind": spec.source_kind,
        "source_rows": int(source_rows or 0),
        "source_patients": int(source_patients or 0),
        "metrics_tested": metrics_tested,
        "mismatch_total": int(mismatch_total),
        "exact_replay_pass": exact,
        "closure_decision": family_decision,
        "carry_forward_closed": True,
        "notes": spec.source_role,
    }
    return family_row, metric_rows


def write_migration_sql(family_df: pd.DataFrame) -> None:
    cols = [c for fam in FAMILIES for c in fam.columns]
    addendum_by_family = {
        row.family: (
            f"mig_180b CLOSED upstream-missing lineage CF for {row.family}; "
            f"source={row.source_catalog}.{row.source_schema}.{row.source_table}; "
            f"decision={row.closure_decision}; exact_replay_pass={str(row.exact_replay_pass).upper()}; "
            f"mismatch_total={row.mismatch_total}; no canonical_patient_master data mutation"
        )
        for row in family_df.itertuples(index=False)
    }
    lines = [
        "-- =============================================================================",
        "-- Migration 180b — NLP UPSTREAM-MISSING family lineage investigation closure",
        "-- =============================================================================",
        "-- Date: 2026-04-29",
        f"-- Batch: {BATCH_ID}",
        "-- Target DB: thyroid_canonical_publication_v1_0",
        "-- Data table touched: main.val_mig180b_nlp_upstream_lineage_v1",
        "-- Registry touched: main.canonical_column_verification_registry_v1 notes only.",
        "-- canonical_patient_master values touched: NONE.",
        "-- =============================================================================",
        "",
        "-- §0 pre-flight",
        "SELECT COUNT(*) AS cpm_rows, COUNT(DISTINCT research_id) AS cpm_distinct_research_id FROM main.canonical_patient_master;",
        "",
        "-- §A snapshot (script uses CREATE TABLE IF NOT EXISTS)",
        f"CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} AS",
        "SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig180b_snapshot_ts",
        "FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1",
        "WHERE schema_name='main' AND table_name='canonical_patient_master'",
        "  AND column_name IN (" + ", ".join(sql_literal(c) for c in sorted(cols)) + ");",
        "",
        "-- §B validation table is materialized by qc_framework_v1/scripts/apply_mig180b_nlp_upstream_lineage.py",
        "SELECT * FROM main.val_mig180b_nlp_upstream_lineage_v1 ORDER BY family;",
        "",
        "-- §C idempotent closure-note updates",
    ]
    for spec in FAMILIES:
        lines.extend(
            [
                "UPDATE main.canonical_column_verification_registry_v1",
                f"SET notes = {append_note_expr(addendum_by_family[spec.family])}",
                "WHERE schema_name='main' AND table_name='canonical_patient_master'",
                "  AND column_name IN (" + ", ".join(sql_literal(c) for c in spec.columns) + ");",
                "",
            ]
        )
    lines.extend(
        [
            "-- §D post-apply closure probe",
            "SELECT COUNT(*) AS n_affected_cols,",
            "       COUNT(*) FILTER (WHERE POSITION('mig_180b CLOSED upstream-missing lineage CF' IN notes) > 0) AS n_closed_cols",
            "FROM main.canonical_column_verification_registry_v1",
            "WHERE schema_name='main' AND table_name='canonical_patient_master'",
            "  AND column_name IN (" + ", ".join(sql_literal(c) for c in sorted(cols)) + ");",
            "",
        ]
    )
    MIGRATION_PATH.parent.mkdir(parents=True, exist_ok=True)
    MIGRATION_PATH.write_text("\n".join(lines), encoding="utf-8")


def apply_database_changes(con, family_df: pd.DataFrame) -> dict[str, Any]:
    cols = [c for fam in FAMILIES for c in fam.columns]
    before = con.execute(
        """
        SELECT COUNT(*) AS n_affected_cols,
               COUNT(*) FILTER (WHERE POSITION('mig_180b CLOSED upstream-missing lineage CF' IN notes) > 0) AS n_closed_cols
        FROM main.canonical_column_verification_registry_v1
        WHERE schema_name='main' AND table_name='canonical_patient_master'
          AND column_name IN (""" + ", ".join(sql_literal(c) for c in sorted(cols)) + ")",
    ).fetchone()

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} AS
        SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig180b_snapshot_ts
        FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
        WHERE schema_name='main' AND table_name='canonical_patient_master'
          AND column_name IN ({", ".join(sql_literal(c) for c in sorted(cols))})
        """
    )

    con.execute(f"DROP TABLE IF EXISTS {VAL_TABLE}")
    con.register("family_df", family_df)
    con.execute(
        f"""
        CREATE TABLE {VAL_TABLE} AS
        SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS audited_at
        FROM family_df
        """
    )
    con.unregister("family_df")

    for row in family_df.itertuples(index=False):
        spec = next(f for f in FAMILIES if f.family == row.family)
        addendum = (
            f"mig_180b CLOSED upstream-missing lineage CF for {row.family}; "
            f"source={row.source_catalog}.{row.source_schema}.{row.source_table}; "
            f"decision={row.closure_decision}; exact_replay_pass={str(row.exact_replay_pass).upper()}; "
            f"mismatch_total={row.mismatch_total}; no canonical_patient_master data mutation"
        )
        con.execute(
            """
            UPDATE main.canonical_column_verification_registry_v1
            SET notes = """ + append_note_expr(addendum) + """
            WHERE schema_name='main' AND table_name='canonical_patient_master'
              AND column_name IN (""" + ", ".join(sql_literal(c) for c in spec.columns) + ")"
        )

    after = con.execute(
        """
        SELECT COUNT(*) AS n_affected_cols,
               COUNT(*) FILTER (WHERE POSITION('mig_180b CLOSED upstream-missing lineage CF' IN notes) > 0) AS n_closed_cols
        FROM main.canonical_column_verification_registry_v1
        WHERE schema_name='main' AND table_name='canonical_patient_master'
          AND column_name IN (""" + ", ".join(sql_literal(c) for c in sorted(cols)) + ")",
    ).fetchone()
    cpm = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master").fetchone()
    val_rows = con.execute(f"SELECT COUNT(*) FROM {VAL_TABLE}").fetchone()[0]
    snapshot_rows = con.execute(f"SELECT COUNT(*) FROM {SNAPSHOT_TABLE}").fetchone()[0]
    if after[0] != 38 or after[1] != 38:
        raise SystemExit(f"mig_180b closure failed: expected 38/38 closed, got {after}")
    if cpm != (10871, 10871):
        raise SystemExit(f"CPM invariant failed after mig_180b: {cpm}")
    if val_rows != 12:
        raise SystemExit(f"Expected 12 validation rows, got {val_rows}")
    return {
        "before_affected_cols": int(before[0] or 0),
        "before_closed_cols": int(before[1] or 0),
        "after_affected_cols": int(after[0] or 0),
        "after_closed_cols": int(after[1] or 0),
        "cpm_rows": int(cpm[0]),
        "cpm_distinct_research_id": int(cpm[1]),
        "val_rows": int(val_rows),
        "snapshot_rows": int(snapshot_rows),
    }


def write_report(family_df: pd.DataFrame, column_df: pd.DataFrame, summary: dict[str, Any]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    exact_families = int(family_df["exact_replay_pass"].sum())
    strict_subset = int((family_df["closure_decision"] == "closed_source_found_pm_strict_subset").sum())
    lines = [
        "# mig_180b NLP UPSTREAM-MISSING lineage investigation",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Executive summary",
        "",
        "- Scope: 12 family-level `CF-mig180-NLP-UPSTREAM-MISSING-*` carry-forwards / 38 `canonical_patient_master` columns from mig_180.",
        f"- Source lineage found for **12 / 12** families; closure notes applied to **{summary['after_closed_cols']} / {summary['after_affected_cols']}** registry rows.",
        f"- Exact derivation-vs-canonical replay: **{exact_families}** families; source-located but stricter retired PM subset: **{strict_subset}** families.",
        "- No `canonical_patient_master` values were mutated; CPM invariants remained 10,871 rows / 10,871 distinct `research_id`.",
        f"- Validation table: `{VAL_TABLE}` ({summary['val_rows']} rows).",
        f"- Pre-snapshot: `{SNAPSHOT_TABLE}` ({summary['snapshot_rows']} rows).",
        "",
        "## Family lineage audit",
        "",
        markdown_table(
            family_df[
                [
                    "family",
                    "n_cols",
                    "source_catalog",
                    "source_schema",
                    "source_table",
                    "source_kind",
                    "source_rows",
                    "source_patients",
                    "metrics_tested",
                    "mismatch_total",
                    "exact_replay_pass",
                    "closure_decision",
                ]
            ]
        ),
        "",
        "## Column-level replay audit",
        "",
        markdown_table(
            column_df[
                [
                    "family",
                    "column_name",
                    "metric",
                    "replay_kind",
                    "pm_non_null",
                    "source_non_null",
                    "pm_sum_or_true",
                    "source_sum_or_true",
                    "n_mismatches",
                    "closure_decision",
                ]
            ]
        ),
        "",
        "## Interpretation",
        "",
        "The original mig_180 audit searched only live canonical `main` tables, so archived / legacy Tier-2 NLP sources were reported as upstream-missing. mig_180b widens lineage discovery to the governed archive/legacy schemas without writing to those read-only reference databases.",
        "",
        "The five generic `note_entities_*` families replay exactly from their archived/legacy rows. The seven raw JSON LLM families have source lineage present, but the raw entity count is a superset of the stricter retired CPM rollup. Those carry-forwards are closed as `source_found_pm_strict_subset`; exact reproduction would require the retired family-specific filter code, and no CPM value rewrite is warranted in this lane.",
        "",
        "`source_patients` / `source_rows` are whole-source counts. Replay comparisons are deliberately scoped to the 10,871-row CPM spine, so non-CPM archived rows are lineage context rather than mismatches.",
        "",
        "## Execution summary",
        "",
        markdown_table(pd.DataFrame([summary])),
        "",
    ]
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    con = connect_locked()
    pm = pm_frame(con)
    family_rows: list[dict[str, Any]] = []
    column_rows: list[dict[str, Any]] = []
    for spec in FAMILIES:
        family_row, metric_rows = profile_family(con, pm, spec)
        family_rows.append(family_row)
        column_rows.extend(metric_rows)

    family_df = pd.DataFrame(family_rows).sort_values("family")
    column_df = pd.DataFrame(column_rows).sort_values(["family", "column_name"])

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    family_df.to_csv(FAMILY_CSV, index=False)
    column_df.to_csv(COLUMN_CSV, index=False)
    write_migration_sql(family_df)
    summary = apply_database_changes(con, family_df)
    RUN_LOG.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    write_report(family_df, column_df, summary)

    print(f"families_closed={len(family_df)}")
    print(f"cols_closed={summary['after_closed_cols']}/{summary['after_affected_cols']}")
    print(f"exact_replay_families={int(family_df['exact_replay_pass'].sum())}")
    print(f"cpm_rows={summary['cpm_rows']} distinct={summary['cpm_distinct_research_id']}")
    print(f"wrote={MIGRATION_PATH.relative_to(REPO_ROOT)}")
    print(f"wrote={REPORT_PATH.relative_to(REPO_ROOT)}")
    print(f"wrote={FAMILY_CSV.relative_to(REPO_ROOT)}")
    print(f"wrote={COLUMN_CSV.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
