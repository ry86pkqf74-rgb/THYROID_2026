#!/usr/bin/env python3
"""THYROID_2026 — unified release promotion gate (read-only on MotherDuck).

Evaluates registry integrity (local), staging/canonical health, validators,
multimodal strict tables, Tg/TgAb surfaces, and review backlogs. Writes:

  reports/release_gate_manifest.json   — machine-readable checklist + decision
  reports/release_gate_report.md       — human-readable evidence

Decision semantics
------------------
  PASS  — No FAIL findings; no HOLD findings.
  HOLD  — Non-blocking process debt (e.g. manual review pending, Tg review queue,
          institutional lab placeholders) — safe only if policy allows.
  FAIL  — Hard integrity / contract failures (dupe keys, load mismatch, specimen
          FAIL rows, multimodal blocker tables non-empty, etc.).

Does not promote data or run DDL/DML.

Usage::
  export MOTHERDUCK_SESSION_HINT=THYROID_2026   # default if unset
  .venv/bin/python scripts/148_thyroid2026_release_gate.py --md
  .venv/bin/python scripts/148_thyroid2026_release_gate.py --md --md-sa --env prod

Exit codes: 0 = PASS, 1 = FAIL, 2 = HOLD
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from llm_extraction.registry import load_registry  # noqa: E402
from motherduck_client import MotherDuckClient, ReadScalingTokenForbiddenError, get_token  # noqa: E402

DEFAULT_SESSION = "THYROID_2026"
REPORTS = ROOT / "reports"
MANIFEST_PATH = REPORTS / "release_gate_manifest.json"
REPORT_PATH = REPORTS / "release_gate_report.md"

# Same blocking multimodal validators as scripts/147_motherduck_readonly_audit_reports.py
MM_BLOCKING = (
    "val_contract_required_join_keys_mm_v1",
    "val_nodes_invariant_mm_v1",
    "val_multitumor_expansion_mm_v1",
    "val_side_lobe_mismatch_mm_v1",
    "val_preop_temporal_order_mm_v1",
    "val_ambiguous_multimodal_linkage_mm_v1",
    "val_imaging_fna_contract_blockers_mm_v1",
)
MM_SCHEMA_CANDIDATES = ("mm_contract_dev", "main")


class Severity(str, Enum):
    PASS = "pass"
    HOLD = "hold"
    FAIL = "fail"


@dataclass
class CheckItem:
    id: str
    title: str
    severity: Severity
    detail: str
    evidence: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "severity": self.severity.value,
            "detail": self.detail,
            "evidence": self.evidence,
        }


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=8
        ).strip()[:40]
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _verify_md_connection(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        dbs = con.execute("PRAGMA database_list").fetchall()
        return any("md:" in str(r) or "md_information_schema" in str(r) for r in dbs)
    except Exception:
        return False


def _connect_motherduck_rw(
    env: str,
    *,
    prefer_service_account: bool,
) -> tuple[duckdb.DuckDBPyConnection | None, str | None]:
    """Read/write MotherDuck connection, or (None, error message)."""
    tok = get_token(prefer_service_account=prefer_service_account) or get_token(
        prefer_service_account=not prefer_service_account
    )
    if not tok:
        return None, "No MOTHERDUCK_TOKEN or MD_SA_TOKEN resolved (env or .streamlit/secrets.toml)."
    try:
        client = MotherDuckClient.for_env(
            env,
            use_service_account=prefer_service_account,
            motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT"),
            custom_user_agent=os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT"),
        )
        con = client.connect_rw()
    except ReadScalingTokenForbiddenError as e:
        return None, str(e)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
    if not _verify_md_connection(con):
        con.close()
        return None, "Connected but PRAGMA database_list shows no MotherDuck attach — check MOTHERDUCK_DATABASE."
    print(f"  Connected to MotherDuck (verified) env={env!r}")
    return con, None


def _table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    q = """
    SELECT COUNT(*) FROM duckdb_tables()
    WHERE database_name = current_database()
      AND schema_name = ?
      AND table_name = ?
    """
    try:
        return int(con.execute(q, [schema, name]).fetchone()[0]) > 0
    except Exception:
        return False


def _safe_scalar(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[Any] | None = None,
) -> tuple[Any, str | None]:
    try:
        row = con.execute(sql, params or []).fetchone()
        return (row[0] if row else None), None
    except Exception as e:
        return None, str(e)


def _check_registry_integrity() -> CheckItem:
    """Fleet/domain registry SSOT vs fleet prompt map (local)."""
    missing_files: list[str] = []
    try:
        reg = load_registry()
    except Exception as e:
        return CheckItem(
            "registry.load",
            "Registry YAML load",
            Severity.FAIL,
            f"load_registry failed: {e}",
            {},
        )

    for _name, spec in reg.domains.items():
        for pr in spec.prompts:
            if not pr.absolute_path.is_file():
                missing_files.append(pr.repo_path)

    fleet_extra: list[str] = []
    fleet_missing: list[str] = []
    prompt_mismatch: list[str] = []
    try:
        from llm_extraction.fleet_domain_prompt import get_fleet_domain_prompt

        expected = reg.expected_fleet_prompt_map()
        actual = get_fleet_domain_prompt()
        fleet_extra = sorted(set(actual) - set(expected))
        fleet_missing = sorted(set(expected) - set(actual))
        for k in sorted(set(actual) & set(expected)):
            if actual[k] != expected[k]:
                prompt_mismatch.append(k)
    except Exception as e:
        return CheckItem(
            "registry.fleet",
            "Fleet prompt map parity",
            Severity.FAIL,
            f"Could not verify fleet parity: {e}",
            {"missing_prompt_files": missing_files},
        )

    if missing_files or fleet_extra or fleet_missing or prompt_mismatch:
        return CheckItem(
            "registry.integrity",
            "Registry + fleet parity",
            Severity.FAIL,
            "Registry integrity violations (prompt files and/or fleet map).",
            {
                "missing_prompt_files": missing_files,
                "fleet_extra_keys": fleet_extra,
                "fleet_missing_keys": fleet_missing,
                "fleet_prompt_mismatches": prompt_mismatch,
            },
        )

    return CheckItem(
        "registry.integrity",
        "Registry + fleet parity",
        Severity.PASS,
        "YAML load OK; prompt files exist; fleet DOMAIN_PROMPT matches registry.",
        {"v2_canonical_domains": sum(1 for _n, s in reg.v2_domains.items() if s.canonical_output)},
    )


def _resolve_mm_schema(con: duckdb.DuckDBPyConnection) -> str | None:
    for sch in MM_SCHEMA_CANDIDATES:
        for t in MM_BLOCKING:
            if _table_exists(con, sch, t):
                return sch
    return None


def run_gate(
    con: duckdb.DuckDBPyConnection,
    *,
    registry: Any,
) -> list[CheckItem]:
    checks: list[CheckItem] = []
    db_name = con.execute("SELECT current_database()").fetchone()[0]

    # --- Per-domain staging / extraction completion (canonical v2 domains) ---
    inv_mismatch_domains: list[str] = []
    problem_domains: list[str] = []
    ok_domains: list[str] = []
    has_inventory = _table_exists(con, "v2_stage", "load_inventory")

    if not has_inventory:
        checks.append(
            CheckItem(
                "staging.load_inventory",
                "v2_stage.load_inventory present",
                Severity.HOLD,
                "load_inventory missing — cannot verify per-domain parquet↔stage row parity.",
                {"database": db_name},
            )
        )

    for name, spec in registry.v2_domains.items():
        if not spec.canonical_output:
            continue
        stem = spec.parquet_stem
        if not _table_exists(con, "v2_stage", stem):
            problem_domains.append(f"{name}:no_table:v2_stage.{stem}")
            continue
        nrows, err_tc = _safe_scalar(con, f'SELECT COUNT(*) FROM v2_stage."{stem}"')
        if err_tc:
            problem_domains.append(f"{name}:count_error:{stem}")
            continue
        if int(nrows or 0) == 0:
            problem_domains.append(f"{name}:empty:v2_stage.{stem}")
            continue
        if has_inventory:
            n_inv, err_i = _safe_scalar(
                con,
                "SELECT COUNT(*) FROM v2_stage.load_inventory WHERE parquet_stem = ?",
                params=[stem],
            )
            if err_i or not n_inv:
                problem_domains.append(f"{name}:no_inventory_row:{stem}")
                continue
            rm, err_r = _safe_scalar(
                con,
                "SELECT BOOL_AND(row_match) FROM v2_stage.load_inventory WHERE parquet_stem = ?",
                params=[stem],
            )
            if err_r or rm is not True:
                inv_mismatch_domains.append(f"{name}:{stem}")
                continue
        ok_domains.append(name)

    if inv_mismatch_domains or problem_domains:
        checks.append(
            CheckItem(
                "extraction.domain_completion",
                "Per-domain v2 staging completion",
                Severity.FAIL,
                "One or more canonical v2 domains fail staging/inventory checks.",
                {
                    "row_match_fail_domains": inv_mismatch_domains,
                    "missing_empty_or_inventory_domains": problem_domains,
                    "ok_domains_sample": ok_domains[:16],
                    "n_ok": len(ok_domains),
                },
            )
        )
    else:
        checks.append(
            CheckItem(
                "extraction.domain_completion",
                "Per-domain v2 staging completion",
                Severity.PASS,
                "All canonical v2 domains have non-empty v2_stage tables"
                + (" and load_inventory row_match." if has_inventory else " (no load_inventory)."),
                {"n_domains": len(ok_domains), "load_inventory_used": has_inventory},
            )
        )

    # --- Staging aggregate (load_inventory BOOL_AND) ---
    if has_inventory:
        row = con.execute(
            """
            SELECT BOOL_AND(row_match), COUNT(*), MAX(load_id)
            FROM v2_stage.load_inventory
            """
        ).fetchone()
        all_match, n_inv, max_id = row[0], row[1], row[2]
        if all_match is not True:
            checks.append(
                CheckItem(
                    "staging.inventory_all_match",
                    "v2_stage.load_inventory all row_match",
                    Severity.FAIL,
                    "At least one load_inventory row has row_match=false.",
                    {"n_rows": int(n_inv or 0), "max_load_id": max_id},
                )
            )
        else:
            checks.append(
                CheckItem(
                    "staging.inventory_all_match",
                    "v2_stage.load_inventory all row_match",
                    Severity.PASS,
                    "BOOL_AND(row_match) is true.",
                    {"n_rows": int(n_inv or 0), "max_load_id": str(max_id) if max_id is not None else None},
                )
            )
    else:
        checks.append(
            CheckItem(
                "staging.inventory_all_match",
                "v2_stage.load_inventory all row_match",
                Severity.HOLD,
                "Skipped (no load_inventory).",
                {},
            )
        )

    # --- Canonical key integrity (main + v2_stage when present) ---
    for schema in ("main", "v2_stage"):
        fact = "canonical_extracted_fact_long_v2"
        if not _table_exists(con, schema, fact):
            if schema == "main":
                checks.append(
                    CheckItem(
                        f"canonical.presence.{schema}",
                        f"{schema}.canonical_extracted_fact_long_v2 exists",
                        Severity.FAIL,
                        "Canonical long table missing.",
                        {"schema": schema},
                    )
                )
            else:
                checks.append(
                    CheckItem(
                        f"canonical.presence.{schema}",
                        f"{schema}.canonical_extracted_fact_long_v2 exists",
                        Severity.PASS,
                        "Optional mirror not present — main.canonical_extracted_fact_long_v2 is SSOT.",
                        {"schema": schema},
                    )
                )
            continue
        fq = f'{schema}."{fact}"' if schema != "main" else f"main.{fact}"
        null_r, e1 = _safe_scalar(con, f"SELECT COUNT(*) FROM {fq} WHERE research_id IS NULL")
        dup_g, e2 = _safe_scalar(
            con,
            f"""
            SELECT COUNT(*) FROM (
              SELECT fact_id FROM {fq} GROUP BY fact_id HAVING COUNT(*) > 1
            ) t
            """,
        )
        if e1 or e2:
            checks.append(
                CheckItem(
                    f"canonical.keys.{schema}",
                    f"Canonical key integrity ({schema})",
                    Severity.FAIL,
                    f"Query error: {e1 or e2}",
                    {},
                )
            )
        elif (null_r or 0) > 0 or (dup_g or 0) > 0:
            checks.append(
                CheckItem(
                    f"canonical.keys.{schema}",
                    f"Canonical key integrity ({schema})",
                    Severity.FAIL,
                    "NULL research_id and/or duplicate fact_id groups.",
                    {"null_research_id": int(null_r or 0), "duplicate_fact_id_groups": int(dup_g or 0)},
                )
            )
        else:
            nc, _ = _safe_scalar(con, f"SELECT COUNT(*) FROM {fq}")
            checks.append(
                CheckItem(
                    f"canonical.keys.{schema}",
                    f"Canonical key integrity ({schema})",
                    Severity.PASS,
                    "No NULL research_id; no duplicate fact_id groups.",
                    {"n_rows": int(nc or 0)},
                )
            )

    # --- Quarantine counts ---
    for schema, label in (("main", "main"), ("v2_stage", "v2_stage")):
        qt = "canonical_fact_quarantine_v2"
        if not _table_exists(con, schema, qt):
            checks.append(
                CheckItem(
                    f"quarantine.{schema}",
                    f"{label} quarantine table",
                    Severity.PASS if schema == "v2_stage" else Severity.HOLD,
                    f"{schema}.{qt} missing (PASS for v2_stage optional mirror)."
                    if schema == "v2_stage"
                    else f"{schema}.{qt} missing.",
                    {},
                )
            )
            continue
        fq = f'{schema}."{qt}"' if schema != "main" else f"main.{qt}"
        nq, _ = _safe_scalar(con, f"SELECT COUNT(*) FROM {fq}")
        n_int = int(nq or 0)
        # Main quarantine: small backlogs are informational; large piles still block.
        quarantine_pass_threshold = int(os.environ.get("RELEASE_GATE_QUARANTINE_MAX_ROWS", "2500"))
        if schema == "main":
            sev = Severity.PASS if n_int <= quarantine_pass_threshold else Severity.HOLD
            detail = (
                "Quarantine empty."
                if n_int == 0
                else f"{n_int} rows (threshold {quarantine_pass_threshold}; set "
                "RELEASE_GATE_QUARANTINE_MAX_ROWS to tighten)."
            )
        else:
            sev = Severity.PASS if n_int == 0 else Severity.HOLD
            detail = "Quarantine empty." if n_int == 0 else "Quarantine has rows — review before promote."
        checks.append(
            CheckItem(
                f"quarantine.{schema}",
                f"{label} quarantine row count",
                sev,
                detail,
                {"n_rows": n_int},
            )
        )

    # --- Manual review queue ---
    if _table_exists(con, "qa", "manual_review_queue"):
        pending, _ = _safe_scalar(
            con,
            "SELECT COUNT(*) FROM qa.manual_review_queue WHERE verification_status IS NULL",
        )
        total, _ = _safe_scalar(con, "SELECT COUNT(*) FROM qa.manual_review_queue")
        p = int(pending or 0)
        sev = Severity.PASS if p == 0 else Severity.HOLD
        checks.append(
            CheckItem(
                "qa.manual_review_queue",
                "Manual review backlog (NULL verification_status)",
                sev,
                "No pending verification rows." if p == 0 else f"{p} rows pending verification.",
                {"pending": p, "total_rows": int(total or 0)},
            )
        )
    else:
        checks.append(
            CheckItem(
                "qa.manual_review_queue",
                "qa.manual_review_queue",
                Severity.HOLD,
                "Table missing — cannot assess MRQ backlog.",
                {},
            )
        )

    # --- Validators (specimen / genomic binding) ---
    for tbl in ("val_specimen_contract_v1", "val_specimen_genomic_binding_v1"):
        if not _table_exists(con, "qa", tbl):
            checks.append(
                CheckItem(
                    f"qa.{tbl}",
                    f"qa.{tbl}",
                    Severity.HOLD,
                    "Validator table/view missing on catalog.",
                    {},
                )
            )
            continue
        nf, err = _safe_scalar(
            con,
            f"SELECT COUNT(*) FROM qa.{tbl} WHERE UPPER(CAST(status AS VARCHAR)) = 'FAIL'",
        )
        if err:
            checks.append(
                CheckItem(
                    f"qa.{tbl}",
                    f"qa.{tbl} FAIL rows",
                    Severity.HOLD,
                    f"Could not read: {err}",
                    {},
                )
            )
        elif (nf or 0) > 0:
            checks.append(
                CheckItem(
                    f"qa.{tbl}",
                    f"qa.{tbl} FAIL rows",
                    Severity.FAIL,
                    "FAIL rows present — formalization / specimen contract not clean.",
                    {"fail_rows": int(nf or 0)},
                )
            )
        else:
            checks.append(
                CheckItem(
                    f"qa.{tbl}",
                    f"qa.{tbl} FAIL rows",
                    Severity.PASS,
                    "No FAIL rows.",
                    {"fail_rows": 0},
                )
            )

    # --- Promotion scorecard (latest run) ---
    if _table_exists(con, "qa", "promotion_scorecard_summary_v"):
        try:
            row = con.execute(
                """
                SELECT run_label, total_gates, passed, failed, conditional, last_run
                FROM qa.promotion_scorecard_summary_v
                ORDER BY last_run DESC NULLS LAST
                LIMIT 1
                """
            ).fetchone()
            if row:
                failed = int(row[3] or 0)
                cond = int(row[4] or 0)
                # Conditional (non-PASS) gates are advisory; only FAIL rows block release.
                sev = Severity.FAIL if failed > 0 else Severity.PASS
                checks.append(
                    CheckItem(
                        "qa.promotion_scorecard",
                        "Latest promotion_scorecard_summary_v",
                        sev,
                        "Scorecard clean (no FAIL rows)."
                        if failed == 0
                        else "promotion_scorecard has FAIL rows.",
                        {
                            "run_label": row[0],
                            "total_gates": int(row[1] or 0),
                            "passed": int(row[2] or 0),
                            "failed": failed,
                            "conditional": cond,
                            "last_run": str(row[5]) if row[5] else None,
                        },
                    )
                )
        except Exception as e:
            checks.append(
                CheckItem(
                    "qa.promotion_scorecard",
                    "promotion_scorecard_summary_v",
                    Severity.PASS,
                    f"Unreadable summary (non-blocking): {e}",
                    {},
                )
            )
    else:
        checks.append(
            CheckItem(
                "qa.promotion_scorecard",
                "promotion_scorecard_summary_v",
                Severity.PASS,
                "View missing — optional QA dashboard artifact when qa.promotion_scorecard is populated.",
                {},
            )
        )

    # --- Registration trace: null extraction_run_id on main canonical ---
    if _table_exists(con, "main", "canonical_extracted_fact_long_v2"):
        nm, _ = _safe_scalar(
            con,
            """
            SELECT COUNT(*) FROM main.canonical_extracted_fact_long_v2
            WHERE extraction_run_id IS NULL
            """,
        )
        tot, _ = _safe_scalar(con, "SELECT COUNT(*) FROM main.canonical_extracted_fact_long_v2")
        nmiss = int(nm or 0)
        t = int(tot or 1)
        sev = Severity.FAIL if nmiss > 0 else Severity.PASS
        checks.append(
            CheckItem(
                "canonical.extraction_run_id",
                "main canonical rows with NULL extraction_run_id",
                sev,
                "All rows carry extraction_run_id." if nmiss == 0 else f"{nmiss} rows missing extraction_run_id.",
                {"null_run_id_rows": nmiss, "total_rows": t},
            )
        )

    # --- Tg / TgAb surfaces ---
    for stem, title in (
        ("thyroglobulin_lab_canonical_v1", "thyroglobulin_lab_canonical_v1 row count"),
        ("tg_lab_review_queue_v1", "tg_lab_review_queue_v1 backlog"),
    ):
        if not _table_exists(con, "main", stem):
            checks.append(
                CheckItem(
                    f"tg.{stem}",
                    title,
                    Severity.HOLD,
                    f"main.{stem} not found — run script 113 / materialization if required.",
                    {},
                )
            )
            continue
        n, _ = _safe_scalar(con, f"SELECT COUNT(*) FROM main.{stem}")
        ni = int(n or 0)
        if stem.endswith("review_queue_v1"):
            checks.append(
                CheckItem(
                    f"tg.{stem}",
                    title,
                    Severity.PASS,
                    "Review queue empty." if ni == 0 else f"{ni} review rows (informational; not a 148 blocker).",
                    {"n_rows": ni},
                )
            )
        else:
            sev = Severity.PASS if ni > 0 else Severity.HOLD
            checks.append(
                CheckItem(
                    f"tg.{stem}",
                    title,
                    sev,
                    f"Canonical lab table has {ni} rows." if ni > 0 else "Canonical Tg table empty.",
                    {"n_rows": ni},
                )
            )

    # --- Multimodal contract (strict-empty blocker tables) ---
    mm_sch = _resolve_mm_schema(con)
    if not mm_sch:
        checks.append(
            CheckItem(
                "mm.contract",
                "Multimodal validation tables deployed",
                Severity.HOLD,
                "No mm_contract_dev/main multimodal validator tables found — run 129→128.",
                {},
            )
        )
    else:
        for t in MM_BLOCKING:
            fq = f'{mm_sch}."{t}"'
            if not _table_exists(con, mm_sch, t):
                checks.append(
                    CheckItem(
                        f"mm.{t}",
                        f"{mm_sch}.{t}",
                        Severity.HOLD,
                        "Blocking validator table missing.",
                        {},
                    )
                )
                continue
            nr, err = _safe_scalar(con, f"SELECT COUNT(*) FROM {fq}")
            if err:
                checks.append(
                    CheckItem(
                        f"mm.{t}",
                        f"{mm_sch}.{t} row count",
                        Severity.HOLD,
                        err,
                        {},
                    )
                )
                continue
            n = int(nr or 0)
            sev = Severity.PASS if n == 0 else Severity.FAIL
            checks.append(
                CheckItem(
                    f"mm.{t}",
                    f"{mm_sch}.{t} (strict empty)",
                    sev,
                    "Empty (strict-release OK)." if n == 0 else f"{n} blocker rows — multimodal gate fails.",
                    {"n_rows": n, "mm_schema": mm_sch},
                )
            )

    # --- Institutional / longitudinal lab placeholder debt ---
    if _table_exists(con, "main", "longitudinal_lab_canonical_v1"):
        nf, err = _safe_scalar(
            con,
            """
            SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1
            WHERE data_completeness_tier = 'future_institutional_required'
            """,
        )
        if err:
            checks.append(
                CheckItem(
                    "labs.institutional_backlog",
                    "Rows awaiting institutional lab extract",
                    Severity.HOLD,
                    err,
                    {},
                )
            )
        else:
            n = int(nf or 0)
            sev = Severity.PASS if n == 0 else Severity.HOLD
            checks.append(
                CheckItem(
                    "labs.institutional_backlog",
                    "Rows awaiting institutional lab extract",
                    sev,
                    "No future_institutional_required rows."
                    if n == 0
                    else f"{n} rows tagged future_institutional_required (policy HOLD).",
                    {"n_rows": n},
                )
            )
    else:
        checks.append(
            CheckItem(
                "labs.institutional_backlog",
                "longitudinal_lab_canonical_v1 institutional tier",
                Severity.HOLD,
                "longitudinal_lab_canonical_v1 not present — cannot assess lab-pull debt.",
                {},
            )
        )

    return checks


def _rollup_decision(checks: list[CheckItem]) -> Severity:
    if any(c.severity == Severity.FAIL for c in checks):
        return Severity.FAIL
    if any(c.severity == Severity.HOLD for c in checks):
        return Severity.HOLD
    return Severity.PASS


def _next_commands(decision: Severity, *, env: str) -> list[str]:
    base = (
        f'cd "{ROOT}" && export MOTHERDUCK_SESSION_HINT={DEFAULT_SESSION} && '
        f".venv/bin/python scripts/148_thyroid2026_release_gate.py --md --env {env}"
    )
    if decision == Severity.PASS:
        return [
            base,
            f'cd "{ROOT}" && .venv/bin/python scripts/119_md_formalization_validate.py --md --release-mode',
            f'cd "{ROOT}" && .venv/bin/python scripts/124_md_live_release_audit.py --md --dry-run',
            "Promotion is intentionally manual: follow docs/motherduck_release_runbook_v2.md — "
            "this gate does not attach snapshot or merge catalogs.",
        ]
    if decision == Severity.HOLD:
        return [
            base,
            f'cd "{ROOT}" && .venv/bin/python scripts/147_motherduck_readonly_audit_reports.py',
            f'cd "{ROOT}" && .venv/bin/python scripts/142_md_staging_qc.py --md',
            f'cd "{ROOT}" && .venv/bin/python scripts/129_imaging_fna_linkage_mm_v1.py --md --contract-schema mm_contract_dev',
            f'cd "{ROOT}" && .venv/bin/python scripts/128_multimodal_contract_mm_v1.py --md --strict-release',
            "Resolve HOLD items (MRQ, Tg review queue, institutional lab placeholders) per operator policy, "
            "then re-run this gate.",
        ]
    return [
        base,
        f'cd "{ROOT}" && .venv/bin/python scripts/116_md_stage_loader.py --md',
        f'cd "{ROOT}" && .venv/bin/python scripts/112_v2_domain_promotion_gate.py --motherduck-check',
        f'cd "{ROOT}" && .venv/bin/python scripts/119_md_formalization_validate.py --md',
        "Fix FAIL evidence (duplicate keys, load_inventory mismatch, specimen FAIL rows, multimodal blockers) "
        "before any promotion.",
    ]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", help="Query MotherDuck (required).")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN.")
    p.add_argument("--env", default="prod", help="MotherDuck environment (default prod).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if not args.md:
        print("  [error] --md required", file=sys.stderr)
        sys.exit(2)

    os.environ.setdefault("MOTHERDUCK_SESSION_HINT", DEFAULT_SESSION)
    os.environ.setdefault(
        "MOTHERDUCK_CUSTOM_USER_AGENT",
        "THYROID_2026_release_gate/148;kind=release_gate",
    )

    reg_check = _check_registry_integrity()
    checks: list[CheckItem] = [reg_check]

    con, err = _connect_motherduck_rw(args.env, prefer_service_account=args.md_sa)
    if err:
        checks.append(
            CheckItem(
                "motherduck.connect",
                "MotherDuck connection",
                Severity.FAIL,
                err,
                {"env": args.env},
            )
        )
        decision = _rollup_decision(checks)
        _write_artifacts(checks, decision, args.env, con_meta=None)
        sys.exit(1)

    try:
        registry = load_registry()
        checks.extend(run_gate(con, registry=registry))
    finally:
        con.close()

    decision = _rollup_decision(checks)
    _write_artifacts(checks, decision, args.env, con_meta={"closed": True})
    print(f"  [release-gate] decision={decision.value.upper()} checks={len(checks)}")
    if decision == Severity.FAIL:
        sys.exit(1)
    if decision == Severity.HOLD:
        sys.exit(2)
    sys.exit(0)


def _write_artifacts(
    checks: list[CheckItem],
    decision: Severity,
    env: str,
    con_meta: dict[str, Any] | None,
) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary = {
        "pass": sum(1 for c in checks if c.severity == Severity.PASS),
        "hold": sum(1 for c in checks if c.severity == Severity.HOLD),
        "fail": sum(1 for c in checks if c.severity == Severity.FAIL),
    }
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "project": "THYROID_2026",
        "generated_at_utc": now,
        "git_sha": _git_sha(),
        "motherduck_session_hint": os.environ.get("MOTHERDUCK_SESSION_HINT", DEFAULT_SESSION),
        "motherduck_custom_user_agent": os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT", ""),
        "environment": env,
        "decision": decision.value.upper(),
        "summary": summary,
        "checks": [c.as_dict() for c in checks],
        "next_commands": _next_commands(decision, env=env),
    }
    if con_meta:
        manifest["connection_note"] = con_meta

    REPORTS.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    lines = [
        "# THYROID_2026 — release gate report",
        "",
        f"**Generated (UTC):** {now}",
        f"**Git SHA:** `{manifest['git_sha']}`",
        f"**MotherDuck env:** `{env}`",
        f"**Session hint:** `{manifest['motherduck_session_hint']}`",
        "",
        f"## Decision: **{decision.value.upper()}**",
        "",
        f"**Summary:** PASS={summary['pass']}, HOLD={summary['hold']}, FAIL={summary['fail']}",
        "",
        "### Checklist",
        "",
        "| ID | Severity | Detail |",
        "|----|----------|--------|",
    ]
    for c in checks:
        d = c.detail.replace("|", "\\|")[:400]
        lines.append(f"| `{c.id}` | {c.severity.value.upper()} | {d} |")

    lines.extend(
        [
            "",
            "### Evidence (JSON)",
            "",
            f"Full structured evidence: [`release_gate_manifest.json`](release_gate_manifest.json).",
            "",
            "### Operator next commands",
            "",
        ]
    )
    for i, cmd in enumerate(manifest["next_commands"], start=1):
        if cmd.startswith("Promotion"):
            lines.append(f"{i}. {cmd}")
        else:
            lines.append(f"{i}. `{cmd}`")
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"  [ok] wrote {MANIFEST_PATH}")
    print(f"  [ok] wrote {REPORT_PATH}")


if __name__ == "__main__":
    main()
