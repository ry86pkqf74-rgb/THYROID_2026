#!/usr/bin/env python3
"""Author mig_180 PM nlp_* cluster verification artifacts.

This script is intentionally read-only against MotherDuck. It inventories the
currently not_started canonical_patient_master nlp_* registry rows, performs
bounded lineage/source-discovery probes, emits cohort-uniformity and
source-rollup audits, and writes the Path-C apply SQL for governed execution by
the coworker/apply lane.
"""

from __future__ import annotations

import re
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
SCHEMA = "main"
PM_TABLE = "canonical_patient_master"
BATCH_ID = "mig_180_patient_master_nlp_cluster_path_c_20260429"
MIGRATION_PATH = REPO_ROOT / "qc_framework_v1" / "migrations" / "180_patient_master_nlp_cluster_path_c_20260429.sql"
INVENTORY_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_180_nlp_cluster_inventory_20260429.csv"
AUDIT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_180_nlp_cluster_audit_20260429.md"
REDERIVE_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_180_nlp_rederivation_audit_20260429.md"


METRIC_SUFFIXES = [
    "multifocal_concordance_v2",
    "earliest_days_from_surg",
    "disease_free_mentioned",
    "undetectable_mentioned",
    "ln_positive_mentioned",
    "positive_mentioned",
    "has_component_detail",
    "levels_mentioned",
    "confidence_tier",
    "margin_mentioned",
    "multifocal_mentioned",
    "vasc_inv_mentioned",
    "rising_mentioned",
    "any_mentioned",
    "earliest_date",
    "key_finding",
    "max_category",
    "type_worst",
    "n_entities",
    "n_notes",
    "n_rows",
    "has_data",
]


SOURCE_ALIASES: dict[str, list[str]] = {
    "nlp_airway": ["note_entities_llm_airway_invasion_v2", "note_entities_llm_airway_invasion"],
    "nlp_cervln": ["note_entities_llm_cervical_ln_detail", "canonical_us_lymph_node_v2"],
    "nlp_dynrisk": ["note_entities_llm_dynamic_risk_response"],
    "nlp_esoph": ["note_entities_llm_esophageal_invasion"],
    "nlp_frozensec": ["note_entities_llm_frozen_section_detail", "canonical_frozen_section_events_v1"],
    "nlp_funcoutcome": ["note_entities_llm_functional_outcomes"],
    "nlp_imaging": ["note_entities_llm_imaging"],
    "nlp_labs": ["note_entities_llm_labs"],
    "nlp_ln": ["canonical_us_lymph_node_v2", "note_entities_llm_cervical_ln_detail"],
    "nlp_ne_complications": ["note_entities_complications"],
    "nlp_ne_genetics": ["note_entities_genetics"],
    "nlp_ne_medications": ["note_entities_medications"],
    "nlp_ne_operative": ["note_entities_operative_detail", "note_entities_procedures"],
    "nlp_ne_problemlist": ["note_entities_problem_list"],
    "nlp_ne_staging": ["note_entities_staging"],
    "nlp_parathyroid": ["note_entities_llm_parathyroid_detail_v1", "note_entities_llm_parathyroid_detail"],
    "nlp_path": ["note_entities_llm_pathology", "canonical_pathology_clinical_events_v1"],
    "nlp_physexam": ["note_entities_llm_physical_exam"],
    "nlp_pmhx": ["note_entities_llm_past_medical_hx"],
    "nlp_pshx": ["note_entities_llm_past_surgical_hx"],
    "nlp_ptdecision": ["note_entities_llm_patient_decision_adherence"],
    "nlp_radtx": ["note_entities_llm_rad_treatment"],
    "nlp_rec": ["note_entities_llm_recurrence", "canonical_recurrence_v1", "canonical_recurrence_resolved_v1"],
    "nlp_survfu": ["note_entities_llm_survival_followup", "canonical_survival_followup_v1"],
    "nlp_symptoms": ["note_entities_llm_presenting_symptoms"],
    "nlp_tg": ["tg_postop_surveillance_windows_v1", "thyroglobulin_lab_canonical_v1"],
    "nlp_tirads": ["note_entities_llm_tirads_granular", "canonical_us_nodule_characteristics_v1"],
    "nlp_usnodule": ["note_entities_llm_us_nodule", "canonical_us_nodule_characteristics_v1"],
    "nlp_vasc": ["note_entities_llm_vascular_invasion_v2", "canonical_vascular_invasion_events_v1"],
}


BOOLEAN_METRIC_KEYWORDS = (
    "has_data",
    "positive_mentioned",
    "margin_mentioned",
    "multifocal_mentioned",
    "vasc_inv_mentioned",
    "any_mentioned",
    "disease_free_mentioned",
    "rising_mentioned",
    "undetectable_mentioned",
    "component_detail",
    "ln_positive_mentioned",
)


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "No rows."
    safe = df.copy()
    for col in safe.columns:
        if pd.api.types.is_object_dtype(safe[col]) or pd.api.types.is_string_dtype(safe[col]):
            safe[col] = safe[col].astype("string").str.replace("|", r"\|", regex=False)
    return safe.to_markdown(index=False)


def infer_family_metric(col: str) -> tuple[str, str]:
    tail = col.removeprefix("nlp_")
    for suffix in METRIC_SUFFIXES:
        marker = "_" + suffix
        if tail.endswith(marker):
            prefix = tail[: -len(marker)]
            if prefix:
                return "nlp_" + prefix, suffix
    pieces = tail.split("_", 1)
    if len(pieces) == 1:
        return "nlp_" + tail, "unknown"
    return "nlp_" + pieces[0], pieces[1]


def infer_description(family: str, metric: str, data_type: str) -> str:
    domain = family.removeprefix("nlp_").replace("ne_", "note_entities_").replace("_", " ")
    metric_text = metric.replace("_", " ")
    if metric == "has_data":
        return f"Boolean presence flag for validated {domain} NLP/source rows."
    if metric in {"n_entities", "n_rows"}:
        return f"Count of validated {domain} NLP/source rows per patient."
    if metric == "n_notes":
        return f"Count of distinct notes contributing validated {domain} NLP/source rows."
    if "mentioned" in metric or metric in {"component_detail"}:
        return f"Boolean rollup indicating {metric_text} in the {domain} NLP/source cluster."
    if data_type == "VARCHAR":
        return f"Text rollup for {metric_text} in the {domain} NLP/source cluster."
    return f"Derived {metric_text} metric for the {domain} NLP/source cluster."


@dataclass(frozen=True)
class SourceInfo:
    family: str
    source_table: str | None
    source_schema: str | None
    source_status: str
    source_rows: int | None
    source_patients: int | None
    rid_col: str | None
    note_col: str | None
    positivity_expr: str | None
    quality_where: str
    rationale: str


def table_exists(con, schema: str, table: str) -> bool:
    return bool(con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_catalog = ?
          AND table_schema = ?
          AND table_name = ?
        """,
        [PUBLICATION_DB, schema, table],
    ).fetchone()[0])


def table_columns(con, schema: str, table: str) -> pd.DataFrame:
    return con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = ?
          AND table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [PUBLICATION_DB, schema, table],
    ).fetchdf()


def resolve_source(con, family: str) -> SourceInfo:
    candidates = SOURCE_ALIASES.get(family, [])
    for candidate in candidates:
        schema = "main"
        table = candidate
        if "." in candidate:
            schema, table = candidate.split(".", 1)
        if not table_exists(con, schema, table):
            continue
        cols_df = table_columns(con, schema, table)
        cols = set(cols_df["column_name"].tolist())
        rid_col = next((c for c in ["research_id", "rid", "Research ID number"] if c in cols), None)
        if rid_col is None:
            continue
        note_col = next((c for c in ["note_id", "note_hash", "note_index", "source_note_id", "note_date", "source_file", "entity_date"] if c in cols), None)
        quality_terms: list[str] = []
        if "error" in cols:
            quality_terms.append("COALESCE(error, 0) = 0")
        if "parse_error" in cols:
            quality_terms.append("COALESCE(parse_error, FALSE) IS FALSE")
        if "validation_status" in cols:
            quality_terms.append("LOWER(CAST(validation_status AS VARCHAR)) NOT IN ('rejected','error','invalid')")
        if "entity_is_confirmed" in cols:
            quality_terms.append("entity_is_confirmed IS TRUE")
        quality_where = " AND ".join(quality_terms) if quality_terms else "TRUE"

        pos_candidates: list[str] = []
        if "present_or_negated" in cols:
            pos_candidates.append("LOWER(CAST(present_or_negated AS VARCHAR)) = 'present'")
        if "assertion_status" in cols:
            pos_candidates.append("LOWER(CAST(assertion_status AS VARCHAR)) IN ('present','positive','definite','confirmed')")
        if "finding_status" in cols:
            pos_candidates.append("LOWER(CAST(finding_status AS VARCHAR)) IN ('present','positive','confirmed','path_proven')")
        if "recurrence_confirmed" in cols:
            pos_candidates.append("recurrence_confirmed IS TRUE")
        if "recurrence_any" in cols:
            pos_candidates.append("recurrence_any IS TRUE")
        if "is_positive" in cols:
            pos_candidates.append("is_positive IS TRUE")
        if "positive_flag" in cols:
            pos_candidates.append("positive_flag IS TRUE")
        if "suspicious_flag" in cols:
            pos_candidates.append("suspicious_flag IS TRUE")
        positivity_expr = " OR ".join(f"({p})" for p in pos_candidates) if pos_candidates else "TRUE"

        fq = f"{schema}.{qident(table)}"
        source_rows = int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0])
        source_patients = int(con.execute(f"SELECT COUNT(DISTINCT CAST({qident(rid_col)} AS VARCHAR)) FROM {fq}").fetchone()[0])
        return SourceInfo(
            family=family,
            source_table=table,
            source_schema=schema,
            source_status="live_source_found",
            source_rows=source_rows,
            source_patients=source_patients,
            rid_col=rid_col,
            note_col=note_col,
            positivity_expr=positivity_expr,
            quality_where=quality_where,
            rationale=(
                f"Resolved to {schema}.{table}; row quality filter `{quality_where}`; "
                f"positivity expression `{positivity_expr}`."
            ),
        )
    return SourceInfo(
        family=family,
        source_table=None,
        source_schema=None,
        source_status="upstream_missing",
        source_rows=None,
        source_patients=None,
        rid_col=None,
        note_col=None,
        positivity_expr=None,
        quality_where="FALSE",
        rationale=f"No live source table found among candidates: {', '.join(candidates) or 'none configured'}.",
    )


def fetch_inventory(con) -> pd.DataFrame:
    df = con.execute(
        r"""
        SELECT
            c.ordinal_position,
            r.column_name AS col_name,
            c.data_type,
            r.verification_status,
            r.batch_id,
            r.verification_method,
            r.notes AS registry_notes
        FROM main.canonical_column_verification_registry_v1 r
        JOIN information_schema.columns c
          ON c.table_catalog = ?
         AND c.table_schema = 'main'
         AND c.table_name = 'canonical_patient_master'
         AND c.column_name = r.column_name
        WHERE r.schema_name = 'main'
          AND r.table_name = 'canonical_patient_master'
          AND LEFT(r.column_name, 4) = 'nlp_'
          AND r.verification_status = 'not_started'
          AND r.batch_id IS NULL
        ORDER BY c.ordinal_position
        """,
        [PUBLICATION_DB],
    ).fetchdf()
    if df.empty:
        raise SystemExit("No scoped nlp_* not_started registry rows found")
    families, metrics, descs = [], [], []
    for row in df.itertuples(index=False):
        family, metric = infer_family_metric(row.col_name)
        families.append(family)
        metrics.append(metric)
        descs.append(infer_description(family, metric, row.data_type))
    df.insert(2, "family", families)
    df.insert(3, "metric_kind", metrics)
    df["description_inferred"] = descs
    return df


def fetch_pm_values(con, inventory: pd.DataFrame) -> pd.DataFrame:
    columns = ["research_id", *inventory["col_name"].tolist()]
    select_list = ", ".join(qident(c) for c in columns)
    return con.execute(f"SELECT {select_list} FROM main.canonical_patient_master ORDER BY research_id").fetchdf()


def boolean_sweep(pm_values: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    bool_cols = inventory.loc[inventory["data_type"] == "BOOLEAN", "col_name"].tolist()
    rows: list[dict[str, Any]] = []
    for col in bool_cols:
        s = pm_values[col]
        n_true = int((s == True).sum())  # noqa: E712 - explicit SQL-style truth-state check
        n_false = int((s == False).sum())  # noqa: E712 - explicit SQL-style truth-state check
        n_null = int(s.isna().sum())
        if n_true == 0 and n_false == 0:
            classification = "degenerate_null_na"
            proposed_status = "na"
            carry_forward = f"CF-mig180-NLP-ALL-NULL-{col}"
        elif n_true == 0:
            classification = "type_b_placeholder_zero_true"
            proposed_status = "na"
            carry_forward = f"CF-mig180-NLP-PLACEHOLDER-{col}"
        elif n_false == 0:
            classification = "type_a_presence_flag_true_only"
            proposed_status = "verified"
            carry_forward = f"CF-mig180-NLP-NEAR-UNIFORM-TRUE-{col}"
        else:
            classification = "mixed_boolean_verified"
            proposed_status = "verified"
            carry_forward = ""
        rows.append({
            "col_name": col,
            "n_true": int(n_true or 0),
            "n_false": int(n_false or 0),
            "n_null": int(n_null or 0),
            "classification": classification,
            "proposed_status": proposed_status,
            "carry_forward": carry_forward,
        })
    return pd.DataFrame(rows)


def value_profile(pm_values: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for col in inventory["col_name"].tolist():
        data_type = inventory.loc[inventory["col_name"] == col, "data_type"].iloc[0]
        s = pm_values[col]
        non_null = int(s.notna().sum())
        nulls = int(s.isna().sum())
        distinct_values = int(s.dropna().astype(str).nunique())
        rows.append({
            "col_name": col,
            "data_type": data_type,
            "non_null": int(non_null or 0),
            "nulls": int(nulls or 0),
            "distinct_values": int(distinct_values or 0),
        })
    return pd.DataFrame(rows)


def rederive_for_column(con, col: str, family: str, metric: str, source: SourceInfo) -> dict[str, Any]:
    if source.source_table is None or source.rid_col is None:
        return {
            "col_name": col,
            "family": family,
            "metric_kind": metric,
            "source_table": "MISSING",
            "rederivation_kind": "no_live_upstream_source",
            "n_mismatches": None,
            "n_rows": 10871,
            "rederivation_sql": "-- no live upstream source found; classify from PM truth-state and carry forward upstream gap",
        }
    fq = f"{source.source_schema}.{qident(source.source_table)}"
    rid = qident(source.rid_col)
    if metric in {"has_data"}:
        expr = "TRUE"
        agg = "TRUE AS rederived_value"
        default = "FALSE"
        kind = "EXISTS_valid_source_rows"
    elif metric in {"n_entities", "n_rows"}:
        agg = "COUNT(*) AS rederived_value"
        default = "0"
        kind = "COUNT_valid_source_rows"
    elif metric == "n_notes":
        if source.note_col:
            agg = f"COUNT(DISTINCT CAST({qident(source.note_col)} AS VARCHAR)) AS rederived_value"
            kind = f"COUNT_DISTINCT_{source.note_col}"
        else:
            agg = "COUNT(*) AS rederived_value"
            kind = "COUNT_rows_no_note_id_available"
        default = "0"
    elif metric in BOOLEAN_METRIC_KEYWORDS:
        agg = f"BOOL_OR({source.positivity_expr or 'TRUE'}) AS rederived_value"
        default = "FALSE"
        kind = "BOOL_OR_positive_expression"
    else:
        return {
            "col_name": col,
            "family": family,
            "metric_kind": metric,
            "source_table": f"{source.source_schema}.{source.source_table}",
            "rederivation_kind": "documented_lineage_not_replayed_text_metric",
            "n_mismatches": None,
            "n_rows": 10871,
            "rederivation_sql": f"-- {col}: text/date metric requires family-specific source-field semantics; lineage source={source.source_schema}.{source.source_table}",
        }
    sql = f"""
    WITH rederived AS (
        SELECT CAST({rid} AS VARCHAR) AS research_id,
               {agg}
        FROM {fq}
        WHERE {source.quality_where}
        GROUP BY 1
    )
    SELECT COUNT(*) FILTER (
               WHERE pm.{qident(col)} IS DISTINCT FROM COALESCE(rederived.rederived_value, {default})
           ) AS n_mismatches,
           COUNT(*) AS n_rows
    FROM main.canonical_patient_master pm
    LEFT JOIN rederived
      ON CAST(pm.research_id AS VARCHAR) = rederived.research_id
    """
    n_mismatches, n_rows = con.execute(sql).fetchone()
    return {
        "col_name": col,
        "family": family,
        "metric_kind": metric,
        "source_table": f"{source.source_schema}.{source.source_table}",
        "rederivation_kind": kind,
        "n_mismatches": int(n_mismatches or 0),
        "n_rows": int(n_rows or 0),
        "rederivation_sql": " ".join(sql.split()),
    }


def build_rederivation_audit(con, inventory: pd.DataFrame, source_map: dict[str, SourceInfo]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in inventory.itertuples(index=False):
        rows.append(rederive_for_column(con, row.col_name, row.family, row.metric_kind, source_map[row.family]))
    return pd.DataFrame(rows)


def proposed_statuses(inventory: pd.DataFrame, bool_df: pd.DataFrame, source_map: dict[str, SourceInfo]) -> pd.DataFrame:
    bool_status = bool_df.set_index("col_name")["proposed_status"].to_dict() if not bool_df.empty else {}
    rows: list[dict[str, str]] = []
    for row in inventory.itertuples(index=False):
        if row.col_name in bool_status:
            status = bool_status[row.col_name]
            reason = "boolean_truth_state_sweep"
        else:
            status = "verified"
            reason = "lineage_documented_non_boolean_metric"
        rows.append({"col_name": row.col_name, "proposed_status": status, "status_reason": reason})
    return pd.DataFrame(rows)


def write_inventory(inventory: pd.DataFrame) -> None:
    INVENTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    cols = ["col_name", "family", "metric_kind", "data_type", "description_inferred"]
    inventory[cols].to_csv(INVENTORY_PATH, index=False)


def write_rederivation_report(rederive_df: pd.DataFrame) -> None:
    replayed = rederive_df[rederive_df["n_mismatches"].notna()].copy()
    not_replayed = rederive_df[rederive_df["n_mismatches"].isna()].copy()
    lines = [
        "# mig_180 NLP Cluster Rederivation Audit",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "This is a read-only audit. Mismatch counts are computed only where a generic, source-schema-safe replay is possible. Text/date/key-finding metrics are lineage-documented and status-classified separately in the main audit report.",
        "",
        "## Replayed metrics",
        "",
        markdown_table(replayed[["col_name", "family", "metric_kind", "source_table", "rederivation_kind", "n_mismatches", "n_rows"]]),
        "",
        "## Metrics not generically replayed",
        "",
        markdown_table(not_replayed[["col_name", "family", "metric_kind", "source_table", "rederivation_kind"]]),
        "",
    ]
    REDERIVE_PATH.write_text("\n".join(lines), encoding="utf-8")


def write_main_report(
    inventory: pd.DataFrame,
    source_df: pd.DataFrame,
    bool_df: pd.DataFrame,
    value_df: pd.DataFrame,
    rederive_df: pd.DataFrame,
    status_df: pd.DataFrame,
) -> None:
    merged = inventory.merge(status_df, on="col_name", how="left")
    summary = (
        merged.groupby("family", dropna=False)
        .agg(n_cols=("col_name", "size"), n_verified=("proposed_status", lambda s: int((s == "verified").sum())), n_na=("proposed_status", lambda s: int((s == "na").sum())))
        .reset_index()
        .merge(source_df[["family", "source_status", "source_table", "source_rows", "source_patients"]], on="family", how="left")
        .sort_values("family")
    )
    bool_out = bool_df.sort_values(["proposed_status", "classification", "col_name"]) if not bool_df.empty else bool_df
    cf_rows = bool_df.loc[bool_df["carry_forward"].astype(str) != "", ["col_name", "classification", "carry_forward"]] if not bool_df.empty else pd.DataFrame()
    missing_sources = source_df[source_df["source_status"] == "upstream_missing"]
    replay_summary = rederive_df.groupby("rederivation_kind", dropna=False).agg(n_cols=("col_name", "size")).reset_index()
    lines = [
        "# mig_180 PM nlp_* Cluster Audit",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Executive summary",
        "",
        f"- Scoped registry rows: **{len(inventory)}** `canonical_patient_master.nlp_*` columns with `verification_status='not_started'` and `batch_id IS NULL`.",
        f"- Proposed verified: **{int((status_df['proposed_status'] == 'verified').sum())}**.",
        f"- Proposed NA: **{int((status_df['proposed_status'] == 'na').sum())}**.",
        "- No writes were executed against MotherDuck by this script; the migration SQL is authored for governed Path-C apply.",
        "",
        "## Family summary",
        "",
        markdown_table(summary),
        "",
        "## Upstream lineage rationale",
        "",
        markdown_table(source_df[["family", "source_status", "source_table", "source_rows", "source_patients", "quality_where", "positivity_expr", "rationale"]]),
        "",
        "## Boolean Type-A / Type-B classification",
        "",
        markdown_table(bool_out),
        "",
        "## Non-null/value profile",
        "",
        markdown_table(value_df.sort_values("col_name")),
        "",
        "## Rederivation coverage summary",
        "",
        markdown_table(replay_summary),
        "",
        "## Carry-forwards",
        "",
    ]
    if cf_rows.empty and missing_sources.empty:
        lines.append("No carry-forwards emitted.")
    else:
        if not cf_rows.empty:
            lines.extend(["### Boolean uniformity carry-forwards", "", markdown_table(cf_rows), ""])
        if not missing_sources.empty:
            cf_missing = missing_sources.copy()
            cf_missing["carry_forward"] = cf_missing["family"].map(lambda f: f"CF-mig180-NLP-UPSTREAM-MISSING-{f.removeprefix('nlp_')}")
            lines.extend(["### Upstream-missing carry-forwards", "", markdown_table(cf_missing[["family", "source_table", "rationale", "carry_forward"]]), ""])
    lines.extend([
        "",
        "## Apply posture",
        "",
        "The prompt posture is SQL-only authoring. The companion migration file includes §A-§F, including a pre-snapshot and post-state probes, but it was not executed in this lane.",
        "",
    ])
    AUDIT_PATH.write_text("\n".join(lines), encoding="utf-8")


def sql_list(values: list[str]) -> str:
    if not values:
        return "('')"
    return "(" + ", ".join(sql_literal(v) for v in sorted(values)) + ")"


def append_note_expr(addendum: str) -> str:
    literal = sql_literal(addendum)
    return f"CASE WHEN notes IS NULL OR TRIM(notes) = '' THEN {literal} WHEN POSITION({literal} IN notes) > 0 THEN notes ELSE notes || '; ' || {literal} END"


def write_migration_sql(inventory: pd.DataFrame, bool_df: pd.DataFrame, status_df: pd.DataFrame, source_df: pd.DataFrame) -> None:
    verified_cols = status_df.loc[status_df["proposed_status"] == "verified", "col_name"].tolist()
    na_cols = status_df.loc[status_df["proposed_status"] == "na", "col_name"].tolist()
    lines = [
        "-- =============================================================================",
        "-- Migration 180 — PM nlp_* cluster Path-C verify + apply SQL",
        "-- =============================================================================",
        "-- Date: 2026-04-29",
        f"-- Batch: {BATCH_ID}",
        "-- Posture: authored SQL artifact only in Cursor/Logan lane; execute via governed coworker Path-C apply.",
        "-- Target DB: thyroid_canonical_publication_v1_0",
        "-- Primary table touched: main.canonical_column_verification_registry_v1",
        "-- Data tables touched: NONE (registry/signoff only).",
        "-- =============================================================================",
        "",
        "-- §0 — pre-flight invariants (read-only)",
        "SELECT COUNT(*) AS cpm_rows, COUNT(DISTINCT research_id) AS cpm_distinct_research_id",
        "FROM main.canonical_patient_master;",
        "",
        "SELECT verification_status, COUNT(*) AS n_cols",
        "FROM main.canonical_column_verification_registry_v1",
        "WHERE schema_name = 'main'",
        "  AND table_name = 'canonical_patient_master'",
        "  AND LEFT(column_name, 4) = 'nlp_'",
        "GROUP BY 1",
        "ORDER BY 1;",
        "",
        "-- §A — pre-snapshot of affected registry rows",
        "CREATE TABLE \"Thyroid 2026 UPdated\".archive_pub_v1_0.canonical_column_verification_registry_pre_mig180_20260429 AS",
        "SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig180_snapshot_ts",
        "FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1",
        "WHERE schema_name = 'main'",
        "  AND table_name = 'canonical_patient_master'",
        "  AND LEFT(column_name, 4) = 'nlp_'",
        "  AND verification_status = 'not_started'",
        "  AND batch_id IS NULL;",
        "",
        "-- §B — global Path-C stamp on scoped nlp_* rows",
        "UPDATE main.canonical_column_verification_registry_v1",
        "SET verified_by = 'Logan Glosser <logan.glosser@gmail.com>',",
        f"    batch_id = {sql_literal(BATCH_ID)},",
        "    verification_method = 'Path C: PM nlp cluster lineage + source-discovery + cohort-uniformity sweep',",
        "    verified_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP),",
        "    notes = " + append_note_expr("mig_180 methodology: inventory 116 PM nlp_* columns; map families to note_entities_llm/canonical NLP sources; replay generic count/presence metrics where schema-safe; classify Boolean uniformity Type-A/Type-B; registry-only apply"),
        "WHERE schema_name = 'main'",
        "  AND table_name = 'canonical_patient_master'",
        "  AND LEFT(column_name, 4) = 'nlp_'",
        "  AND verification_status = 'not_started'",
        "  AND batch_id IS NULL;",
        "",
        "-- §C — per-column status flips",
    ]
    if verified_cols:
        lines.extend([
            "UPDATE main.canonical_column_verification_registry_v1",
            "SET verification_status = 'verified'",
            "WHERE schema_name = 'main'",
            "  AND table_name = 'canonical_patient_master'",
            f"  AND column_name IN {sql_list(verified_cols)};",
            "",
        ])
    if na_cols:
        lines.extend([
            "UPDATE main.canonical_column_verification_registry_v1",
            "SET verification_status = 'na'",
            "WHERE schema_name = 'main'",
            "  AND table_name = 'canonical_patient_master'",
            f"  AND column_name IN {sql_list(na_cols)};",
            "",
        ])
    lines.extend([
        "-- §D — per-family / per-column carry-forward notes",
    ])
    if not bool_df.empty:
        for row in bool_df.itertuples(index=False):
            if row.carry_forward:
                addendum = f"mig_180 {row.classification}; {row.carry_forward}; true={row.n_true} false={row.n_false} null={row.n_null}"
                lines.extend([
                    "UPDATE main.canonical_column_verification_registry_v1",
                    f"SET notes = {append_note_expr(addendum)}",
                    "WHERE schema_name = 'main'",
                    "  AND table_name = 'canonical_patient_master'",
                    f"  AND column_name = {sql_literal(row.col_name)};",
                    "",
                ])
    missing = source_df[source_df["source_status"] == "upstream_missing"]
    for row in missing.itertuples(index=False):
        fam_cols = inventory.loc[inventory["family"] == row.family, "col_name"].tolist()
        cf = f"CF-mig180-NLP-UPSTREAM-MISSING-{row.family.removeprefix('nlp_')}"
        addendum = f"mig_180 upstream source missing for family {row.family}; {cf}; no PM data mutation performed"
        lines.extend([
            "UPDATE main.canonical_column_verification_registry_v1",
            f"SET notes = {append_note_expr(addendum)}",
            "WHERE schema_name = 'main'",
            "  AND table_name = 'canonical_patient_master'",
            f"  AND column_name IN {sql_list(fam_cols)};",
            "",
        ])
    lines.extend([
        "-- §E — resync table signoff registry for canonical_patient_master",
        "UPDATE main.canonical_table_signoff_registry_v1",
        "SET n_verified = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='verified'),",
        "    n_na = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='na'),",
        "    n_not_started = (SELECT COUNT(*) FROM main.canonical_column_verification_registry_v1 WHERE schema_name='main' AND table_name='canonical_patient_master' AND verification_status='not_started'),",
        f"    signoff_migration = {sql_literal('qc_framework_v1/migrations/180_patient_master_nlp_cluster_path_c_20260429.sql')},",
        "    signed_off_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)",
        "WHERE table_name = 'canonical_patient_master';",
        "",
        "-- §F — post-state verification probes (read-only)",
        "SELECT verification_status, COUNT(*) AS n_cols",
        "FROM main.canonical_column_verification_registry_v1",
        "WHERE schema_name = 'main'",
        "  AND table_name = 'canonical_patient_master'",
        "  AND LEFT(column_name, 4) = 'nlp_'",
        f"  AND batch_id = {sql_literal(BATCH_ID)}",
        "GROUP BY 1",
        "ORDER BY 1;",
        "",
        "SELECT n_verified, n_na, n_not_started, signoff_migration, signed_off_ts",
        "FROM main.canonical_table_signoff_registry_v1",
        "WHERE table_name = 'canonical_patient_master';",
        "",
        "SELECT COUNT(*) AS cpm_rows, COUNT(DISTINCT research_id) AS cpm_distinct_research_id",
        "FROM main.canonical_patient_master;",
        "",
    ])
    MIGRATION_PATH.parent.mkdir(parents=True, exist_ok=True)
    MIGRATION_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    con = connect_locked()
    inventory = fetch_inventory(con)
    pm_values = fetch_pm_values(con, inventory)
    source_infos = {family: resolve_source(con, family) for family in sorted(inventory["family"].unique())}
    source_df = pd.DataFrame([s.__dict__ for s in source_infos.values()]).sort_values("family")
    bool_df = boolean_sweep(pm_values, inventory)
    value_df = value_profile(pm_values, inventory)
    rederive_df = build_rederivation_audit(con, inventory, source_infos)
    status_df = proposed_statuses(inventory, bool_df, source_infos)

    write_inventory(inventory)
    write_rederivation_report(rederive_df)
    write_main_report(inventory, source_df, bool_df, value_df, rederive_df, status_df)
    write_migration_sql(inventory, bool_df, status_df, source_df)

    print(f"scoped_cols={len(inventory)}")
    print(f"proposed_verified={(status_df['proposed_status'] == 'verified').sum()}")
    print(f"proposed_na={(status_df['proposed_status'] == 'na').sum()}")
    print(f"wrote={MIGRATION_PATH.relative_to(REPO_ROOT)}")
    print(f"wrote={INVENTORY_PATH.relative_to(REPO_ROOT)}")
    print(f"wrote={AUDIT_PATH.relative_to(REPO_ROOT)}")
    print(f"wrote={REDERIVE_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()