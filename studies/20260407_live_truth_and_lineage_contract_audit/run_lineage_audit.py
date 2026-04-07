#!/usr/bin/env python3
"""One-shot lineage contract audit for MotherDuck catalog × Thyroid 2026.

Outputs: lineage_contract_matrix.csv, lineage_contract_matrix.md stubs,
         object_level_sql/*.sql — invoked from repo root via python studies/.../run_lineage_audit.py
"""
from __future__ import annotations

import csv
import json
import os
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
import sys

sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

CATALOG = "Thyroid 2026"
LATEST_RELEASE_SCHEMA = "release_20260409"

ORIGIN_COLS = (
    "source_domain",
    "source_table",
    "source_object_id",
    "note_row_id",
    "source_lineage_key",
    "source_row_fingerprint",
    "specimen_fingerprint_sha256",
    "lineage_id",
    "source_file_id",
    "ingestion_script",
    "source_workbook",
    "source_sheet",
    "excel_row_0based",
)

TIME_COLS = (
    "entity_date",
    "note_date",
    "lab_date",
    "test_date",
    "procedure_date_day",
    "performeddatetime",
    "collecteddatetime",
    "event_date",
    "surgery_date",
    "surg_date",
    "extracted_at",
    "ingestion_ts",
    "started_at",
    "specimen_collect_dt",
    "order_dt",
    "built_at",
    "ingested_at_utc",
)

LINKAGE_COLS = (
    "surgery_episode_id",
    "specimen_id",
    "specimen_focus_id",
    "molecular_result_id",
    "path_surgery_id",
    "tumor_episode_id",
    "fna_episode_id",
    "imaging_episode_id",
)

BUILD_COLS = (
    "extraction_run_id",
    "ingestion_wave",
    "identity_build_run_id",
    "materialized_at",
    "release_tag",
    "ingestion_run_id",
    "created_at",
    "ingestion_date",
    "git_commit",
    "ingest_script_version",
)


def qident(part: str) -> str:
    return '"' + part.replace('"', '""') + '"'


def fqtn(schema: str, name: str) -> str:
    return f'{qident(CATALOG)}.{qident(schema)}.{qident(name)}'


def classify(schema: str, name: str, table_type: str) -> tuple[str, bool]:
    """Return (category, needs_row_traceability_audit).

    Categories: row_level_fact_event | row_level_lab_specimen_molecular | aggregate_rollup |
               qa_control_plane
    """
    nl = name.lower()
    is_view = table_type.upper() == "VIEW"

    if schema == "qa":
        if nl.endswith("_summary_v") or nl.endswith("_summary") or (
            "summary" in nl and is_view
        ):
            return "aggregate_rollup", False
        if nl in (
            "release_manifest",
            "promotion_scorecard",
            "concordance_summary",
            "domain_validation",
            "tg_lab_ingestion_qc",
        ):
            return "qa_control_plane", False
        if nl.startswith("v_diag_"):
            return "aggregate_rollup", False

    if schema == "v2_stage" and nl == "load_inventory":
        return "qa_control_plane", False

    agg_hints = (
        "rollup",
        "_summary_v",
        "completeness_summary",
        "linkage_summary",
        "episode_completeness",
        "qc_summary",
        "patient_rollup",
        "rollup_verified",
        "deduped_v",
        "longitudinal_lab_deduped_v",
    )
    if any(h in nl for h in agg_hints):
        return "aggregate_rollup", False

    if nl.startswith("fhir_bundle") or nl == "fhir_patient_deid_map_v1":
        return "row_level_lab_specimen_molecular", True

    lab_names = ("longitudinal_lab", "thyroglobulin_lab", "lab_canonical")
    if any(x in nl for x in lab_names):
        return "row_level_lab_specimen_molecular", True

    if nl.startswith("specimen_") or nl.startswith("fhir_") or "genomic" in nl:
        return "row_level_lab_specimen_molecular", True

    if schema == "qa" and (nl.startswith("val_") or "review_queue" in nl or "review" in nl):
        return "qa_control_plane", True

    return "row_level_fact_event", True


def list_objects(con, schemas: Iterable[str]) -> list[tuple[str, str, str]]:
    sch = list(schemas)
    placeholders = ", ".join(["?"] * len(sch))
    rows = con.execute(
        f"""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = ?
          AND table_schema IN ({placeholders})
        ORDER BY table_schema, table_name
        """,
        [CATALOG, *sch],
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def table_columns(con, schema: str, table: str) -> dict[str, str]:
    rows = con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = ?
          AND table_schema = ?
          AND table_name = ?
        """,
        [CATALOG, schema, table],
    ).fetchall()
    return {r[0].lower(): r[0] for r in rows}


def first_present(cols: dict[str, str], candidates: tuple[str, ...]) -> list[str]:
    return [cols[c.lower()] for c in candidates if c.lower() in cols]


def safe_count(con, schema: str, name: str) -> int | None:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {fqtn(schema, name)}").fetchone()[0]
    except Exception:
        return None


def nulls_for(con, schema: str, name: str, col: str) -> tuple[int | None, int | None]:
    """Return (null_count, total) or (None, None) on failure."""
    try:
        total = con.execute(f"SELECT COUNT(*) FROM {fqtn(schema, name)}").fetchone()[0]
        nulls = con.execute(
            f"SELECT COUNT(*) FROM {fqtn(schema, name)} WHERE {qident(col)} IS NULL"
        ).fetchone()[0]
        return nulls, total
    except Exception:
        return None, None


def verdict_for_key(
    present: list[str],
    nulls: tuple[int | None, int | None],
    allow_partial_via_substitute: bool = False,
) -> tuple[str, str]:
    if not present:
        if allow_partial_via_substitute:
            return "PARTIAL", "no dedicated column; substitute via join"
        return "FAIL", "missing column(s)"
    n, t = nulls
    if t == 0:
        return "PASS", "empty object"
    if n is None:
        return "PARTIAL", "null scan failed"
    if n == 0:
        return "PASS", f"`{present[0]}` non-null all rows"
    return "PARTIAL", f"`{present[0]}` NULL rows={n}/{t}"


def main() -> None:
    study_dir = Path(__file__).resolve().parent
    sql_dir = study_dir / "object_level_sql"
    sql_dir.mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("MOTHERDUCK_CUSTOM_USER_AGENT", "THYROID_2026_lineage_truth_audit/1.0")
    utc = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    os.environ.setdefault("MOTHERDUCK_SESSION_HINT", f"lineage_truth_audit_{utc}")

    con = connect_md_fail_closed(
        ROOT / "thyroid_master.duckdb",
        prefer_service_account=True,
        custom_user_agent=os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT", ""),
        motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT", ""),
    )

    schemas = ["main", "qa", "v2_stage", LATEST_RELEASE_SCHEMA]
    objects = list_objects(con, schemas)

    # DB catalog evidence
    db_row = con.execute(
        """
        SELECT name, type, transient, CAST(historical_snapshot_retention AS VARCHAR)
        FROM md_information_schema.databases
        WHERE name = ?
        """,
        [CATALOG],
    ).fetchone()

    rm_rows = con.execute(
        """
        SELECT release_tag, created_at, created_by, tables_included, registry_version
        FROM qa.release_manifest
        ORDER BY TRY_CAST(release_tag AS BIGINT) DESC NULLS LAST, created_at DESC
        LIMIT 5
        """
    ).fetchdf()

    matrix_path = study_dir / "lineage_contract_matrix.csv"
    rows_out: list[dict[str, str]] = []

    for schema, name, ttype in objects:
        cat, audit = classify(schema, name, ttype)
        cols = table_columns(con, schema, name)
        colset = set(cols.keys())

        if not audit:
            rows_out.append(
                {
                    "catalog": CATALOG,
                    "schema": schema,
                    "object_name": name,
                    "table_type": ttype,
                    "classification": cat,
                    "row_count": str(safe_count(con, schema, name) or ""),
                    "patient_key": "N/A",
                    "origin_key": "N/A",
                    "time_key": "N/A",
                    "linkage_key": "N/A",
                    "build_release_key": "N/A",
                    "overall_verdict": "N/A (not row-grain audit)",
                    "missing_fields": "",
                    "substitutes": "",
                    "gap_kind": "",
                    "remediation": "",
                    "sql_file": "",
                }
            )
            continue

        rc = safe_count(con, schema, name)
        pcols = first_present(cols, ("research_id",))
        ocols = first_present(cols, ORIGIN_COLS)
        tcols = first_present(cols, TIME_COLS)
        lcols = first_present(cols, LINKAGE_COLS)
        bcols = first_present(cols, BUILD_COLS)

        # If no research_id, check synthetic QA keys
        if not pcols and schema == "qa" and "queue_id" in colset:
            pcols = first_present(cols, ("queue_id",))

        subst_note = ""
        if not ocols and "note_row_id" in colset:
            subst_note = "note_row_id present for note linkage"
            ocols = ["note_row_id"]

        p_verd, p_detail = verdict_for_key(
            pcols,
            nulls_for(con, schema, name, pcols[0]) if pcols else (None, None),
        )
        origin_contract_cols = first_present(cols, ORIGIN_COLS)
        if ocols:
            o_verd, o_detail = verdict_for_key(
                ocols,
                nulls_for(con, schema, name, ocols[0]),
            )
        else:
            o_verd, o_detail = "FAIL", "missing column(s)"
        if ocols and not origin_contract_cols and any(c.lower() == "note_row_id" for c in ocols):
            o_verd = "PARTIAL"
            o_detail = "note_row_id only (no source_domain/source_table on object)"

        t_verd, t_detail = verdict_for_key(
            tcols,
            nulls_for(con, schema, name, tcols[0]) if tcols else (None, None),
        )

        allow_link_partial = bool(first_present(cols, ("note_row_id",)) or first_present(cols, ORIGIN_COLS))
        l_verd, l_detail = verdict_for_key(
            lcols,
            nulls_for(con, schema, name, lcols[0]) if lcols else (None, None),
            allow_partial_via_substitute=allow_link_partial,
        )
        if not lcols and allow_link_partial:
            l_verd, l_detail = "PARTIAL", "no episode/specimen ids; join from note_row_id / canonical spine"

        b_verd, b_detail = verdict_for_key(
            bcols,
            nulls_for(con, schema, name, bcols[0]) if bcols else (None, None),
        )

        parts = [p_verd, o_verd, t_verd, l_verd, b_verd]
        if all(x == "PASS" for x in parts):
            overall = "PASS"
        elif any(x == "FAIL" for x in parts):
            overall = "FAIL"
        else:
            overall = "PARTIAL"

        missing: list[str] = []
        if p_verd != "PASS":
            missing.append("patient")
        if o_verd != "PASS":
            missing.append("origin")
        if t_verd != "PASS":
            missing.append("time")
        if l_verd != "PASS":
            missing.append("linkage")
        if b_verd != "PASS":
            missing.append("build/release")

        gap_kind = []
        if not pcols:
            gap_kind.append("schema")
        if not ocols:
            gap_kind.append("schema")
        remediation = ""
        if overall != "PASS":
            remediation = (
                "Add/maintain contract columns per docs/motherduck_database_contract_v1.md §3; "
                "backfill via 103/117/138; linkage gaps: join via surgery_episode_id or canonical spine."
            )

        # Documented design exceptions (not data defects)
        if name == "note_extraction_runs":
            overall = "PASS (exception: run-registry grain)"
            p_verd = "PASS (exception)"
            p_detail = "no research_id; run-grain registry keyed by run_id"
            o_verd = "PASS (exception)"
            o_detail = "git_commit + domains_requested surface build provenance"
            l_verd = "PASS (exception)"
            l_detail = "N/A at run grain"
            b_verd = "PASS (exception)"
            b_detail = "started_at + git_commit"
            missing = []
            gap_kind = []
            remediation = "N/A — run-telemetry registry (contract §2 note_extraction_runs)."
        elif name.startswith("fhir_") and name not in ("fhir_patient_deid_map_v1",):
            has_rid = bool(first_present(cols, ("research_id",)))
            if not has_rid and first_present(cols, ("patient_fhir_id",)):
                p_verd = "PARTIAL (exception)"
                p_detail = "patient via patient_fhir_id → fhir_patient_deid_map_v1 / specimen spine"
            if first_present(cols, ("specimen_id",)):
                o_verd = "PASS (exception)"
                o_detail = "specimen_id + resource_json prove FHIR resource identity"
            if first_present(cols, ("built_at",)):
                t_verd = "PASS"
                t_detail = "`built_at` present"
            if first_present(cols, ("specimen_id",)):
                l_verd = "PASS (exception)"
                l_detail = "specimen_id joins to specimen_master_v1"
            b_verd = "PARTIAL (exception)"
            b_detail = "built_at; release_tag only on release schema snapshot exports"
            missing = []
            for _v, _lbl in (
                (p_verd, "patient"),
                (o_verd, "origin"),
                (t_verd, "time"),
                (l_verd, "linkage"),
                (b_verd, "build/release"),
            ):
                if not (
                    _v.startswith("PASS")
                    or _v.startswith("PARTIAL (exception)")
                    or "PASS (exception)" in _v
                ):
                    missing.append(_lbl)
            if any(x.startswith("FAIL") for x in (p_verd, o_verd, t_verd, l_verd, b_verd)):
                overall = "FAIL"
            elif any("PARTIAL" in x for x in (p_verd, o_verd, t_verd, l_verd, b_verd)):
                overall = "PARTIAL (exception: FHIR export row)"
            else:
                overall = "PASS (exception: FHIR export row)"
            gap_kind = []
            remediation = (
                "N/A — analytic FHIR export rows; join patient_fhir_id to map; "
                "see docs/motherduck_database_contract_v1.md §Specimen identity + FHIR."
            )
        elif name in ("thyroglobulin_lab_canonical_v1", "longitudinal_lab_canonical_v1"):
            l_verd = "PASS (exception: lab grain)"
            l_detail = "research_id + surg_date/lab_date + ingestion_wave; no surgery_episode_id on row"
            parts2 = [p_verd, o_verd, t_verd, l_verd, b_verd]

            def _ok(x: str) -> bool:
                return bool(
                    x.startswith("PASS")
                    or x.startswith("PARTIAL (exception)")
                    or "PASS (exception)" in x
                )

            if all(_ok(x) for x in parts2):
                overall = "PASS"
            elif any(x.startswith("FAIL") for x in parts2):
                overall = "FAIL"
            else:
                overall = "PARTIAL"
            missing = [
                lbl
                for lbl, v in zip(
                    ("patient", "origin", "time", "linkage", "build/release"),
                    parts2,
                )
                if v.startswith("FAIL")
            ]
            gap_kind = []
            remediation = (
                "N/A — institution lab / Tg canonical long format per 113; linkage is temporal + patient, "
                "not episode-id denormalized."
            )
        elif name == "clinical_notes_long":
            l_verd = "PASS (exception: staging wide table)"
            l_detail = (
                "episode/specimen ids live on canonical/promoted tables; this legacy wide row is workbook-anchored"
            )
            parts2 = [p_verd, o_verd, t_verd, l_verd, b_verd]

            def _ok2(x: str) -> bool:
                return bool(
                    x.startswith("PASS")
                    or x.startswith("PARTIAL (exception)")
                    or "PASS (exception)" in x
                )

            if all(_ok2(x) for x in parts2):
                overall = "PASS"
            elif any(x.startswith("FAIL") for x in parts2):
                overall = "FAIL"
            else:
                overall = "PARTIAL"
            missing = [
                lbl
                for lbl, v in zip(
                    ("patient", "origin", "time", "linkage", "build/release"),
                    parts2,
                )
                if v.startswith("FAIL")
            ]
            gap_kind = []
            remediation = (
                "N/A — PHI-bearing wide extract in MotherDuck; publication claims must cite canonical fact / "
                "presentation views, not raw note bodies (contract §PHI)."
            )

        sql_name = f"{schema}__{re.sub(r'[^0-9a-zA-Z_]+', '_', name)}__count_nulls.sql"
        sql_path = sql_dir / sql_name
        prim_candidates = (
            first_present(cols, ("research_id",))
            or first_present(cols, ("run_id",))
            or first_present(cols, ("patient_fhir_id",))
            or first_present(cols, ("specimen_id",))
            or first_present(cols, ("queue_id",))
        )
        prim_p = prim_candidates[0] if prim_candidates else None
        sel_parts = ["COUNT(*) AS row_count"]
        if prim_p:
            sel_parts.append(
                f"SUM(CASE WHEN {qident(prim_p)} IS NULL THEN 1 ELSE 0 END) AS null_{prim_p}"
            )
        for c in ocols[:1] + tcols[:1] + lcols[:1] + bcols[:1]:
            if c != prim_p:
                sel_parts.append(
                    f"SUM(CASE WHEN {qident(c)} IS NULL THEN 1 ELSE 0 END) AS null_{c}"
                )
        sql_body = f"-- Live counts: {CATALOG}.{schema}.{name}\n"
        sql_body += f"SELECT\n  {',\n  '.join(sel_parts)}\nFROM {fqtn(schema, name)};\n"
        sql_path.write_text(sql_body, encoding="utf-8")

        rows_out.append(
            {
                "catalog": CATALOG,
                "schema": schema,
                "object_name": name,
                "table_type": ttype,
                "classification": cat,
                "row_count": str(rc if rc is not None else ""),
                "patient_key": f"{p_verd}: {p_detail}",
                "origin_key": f"{o_verd}: {o_detail}" + (f"; {subst_note}" if subst_note else ""),
                "time_key": f"{t_verd}: {t_detail}",
                "linkage_key": f"{l_verd}: {l_detail}",
                "build_release_key": f"{b_verd}: {b_detail}",
                "overall_verdict": overall,
                "missing_fields": ";".join(missing),
                "substitutes": subst_note or ("note_row_id" if "note_row_id" in colset else ""),
                "gap_kind": ",".join(gap_kind) if gap_kind else ("population" if overall == "PARTIAL" else ""),
                "remediation": remediation,
                "sql_file": str(sql_path.relative_to(study_dir)),
            }
        )

    con.close()

    fieldnames = list(rows_out[0].keys()) if rows_out else []
    with matrix_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_out)

    summary = {
        "catalog": CATALOG,
        "database_md_info": db_row,
        "latest_release_manifest_top": rm_rows.to_dict(orient="records"),
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "objects_scanned": len(rows_out),
    }
    (study_dir / "audit_run_meta.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
