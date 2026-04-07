#!/usr/bin/env python3
"""Generate read-only MotherDuck audit reports (SELECT/EXPLAIN only; no DDL/DML).

Writes:
  reports/motherduck_read_only_audit.md
  reports/motherduck_read_only_metrics.csv

Connection: prefers MD_READ_SCALING_TOKEN when set; otherwise read/write token.
Uses MOTHERDUCK_SESSION_HINT (default THYROID_2026) and MOTHERDUCK_CUSTOM_USER_AGENT for attribution.

Does not print tokens or PHI-bearing column values.
"""
from __future__ import annotations

import csv
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

from motherduck_client import (  # noqa: E402
    MotherDuckClient,
    get_read_scaling_token,
    get_token,
    read_scaling_token_mode,
    resolve_database_for_env,
    token_mode,
)

REPORTS = ROOT / "reports"
DEFAULT_SESSION = "THYROID_2026"

# Blocking multimodal validators (128 --strict-release); row count must be 0 for green.
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


def _safe_count(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[int | None, str | None]:
    try:
        n = con.execute(sql).fetchone()
        return (int(n[0]) if n and n[0] is not None else 0), None
    except Exception as e:
        return None, str(e)


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


def _connect_env(env: str) -> tuple[duckdb.DuckDBPyConnection, str]:
    """Return (connection, mode_label)."""
    hint = (os.environ.get("MOTHERDUCK_SESSION_HINT") or DEFAULT_SESSION).strip()
    ua = (os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT") or "").strip() or None
    client = MotherDuckClient.for_env(
        env,
        motherduck_session_hint=hint,
        custom_user_agent=ua,
    )
    if get_read_scaling_token():
        try:
            return client.connect_read_scaling(), "read_scaling"
        except Exception as exc:
            if get_token(prefer_service_account=False) or get_token(prefer_service_account=True):
                return client.connect_rw(), f"rw_after_rs_error:{type(exc).__name__}"
            raise
    tok = get_token(prefer_service_account=False) or get_token(prefer_service_account=True)
    if not tok:
        raise RuntimeError("No MotherDuck token (read-scaling or read/write) available.")
    return client.connect_rw(), "read_write"


def _audit_one_env(
    env: str,
    con: duckdb.DuckDBPyConnection,
    metrics_rows: list[dict[str, object]],
) -> dict[str, object]:
    db = con.execute("SELECT current_database()").fetchone()[0]
    out: dict[str, object] = {"env": env, "current_database": db}

    schemas_sql = """
    SELECT schema_name
    FROM information_schema.schemata
    WHERE catalog_name = current_database()
      AND schema_name NOT IN ('information_schema', 'pg_catalog')
    ORDER BY 1
    """
    try:
        schemas = [r[0] for r in con.execute(schemas_sql).fetchall()]
    except Exception:
        schemas = []
    out["schemas"] = schemas

    release_schemas = [s for s in schemas if s.startswith("release_")]
    out["release_schemas"] = release_schemas

    for sch in ("main", "qa", "v2_stage"):
        try:
            n = con.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_catalog = current_database()
                  AND table_schema = ?
                  AND table_type IN ('BASE TABLE', 'VIEW')
                """,
                [sch],
            ).fetchone()[0]
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "schema_object_count",
                    "object": sch,
                    "metric": "n_tables_views",
                    "value": int(n),
                    "status": "ok",
                    "detail": "",
                }
            )
        except Exception as e:
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "schema_object_count",
                    "object": sch,
                    "metric": "n_tables_views",
                    "value": "",
                    "status": "error",
                    "detail": str(e)[:500],
                }
            )

    counts_spec = [
        ("main", "canonical_extracted_fact_long_v2", "canonical_clean_rows"),
        ("main", "canonical_fact_quarantine_v2", "canonical_quarantine_rows"),
        ("v2_stage", "canonical_extracted_fact_long_v2", "staging_clean_rows"),
        ("v2_stage", "canonical_fact_quarantine_v2", "staging_quarantine_rows"),
    ]
    for schema, table, metric_key in counts_spec:
        if not _table_exists(con, schema, table):
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "row_count",
                    "object": f"{schema}.{table}",
                    "metric": metric_key,
                    "value": "",
                    "status": "missing",
                    "detail": "",
                }
            )
            continue
        fq = f'{schema}."{table}"' if schema != "main" else f"main.{table}"
        n, err = _safe_count(con, f"SELECT COUNT(*) FROM {fq}")
        metrics_rows.append(
            {
                "env": env,
                "database": db,
                "metric_group": "row_count",
                "object": f"{schema}.{table}",
                "metric": metric_key,
                "value": n if err is None else "",
                "status": "ok" if err is None else "error",
                "detail": (err or "")[:500],
            }
        )

    # Review queue (qa schema)
    if _table_exists(con, "qa", "manual_review_queue"):
        n, err = _safe_count(
            con,
            "SELECT COUNT(*) FROM qa.manual_review_queue WHERE verification_status IS NULL",
        )
        metrics_rows.append(
            {
                "env": env,
                "database": db,
                "metric_group": "review_queue",
                "object": "qa.manual_review_queue",
                "metric": "pending_verification_null",
                "value": n if err is None else "",
                "status": "ok" if err is None else "error",
                "detail": (err or "")[:500],
            }
        )
        n2, err2 = _safe_count(con, "SELECT COUNT(*) FROM qa.manual_review_queue")
        metrics_rows.append(
            {
                "env": env,
                "database": db,
                "metric_group": "review_queue",
                "object": "qa.manual_review_queue",
                "metric": "total_rows",
                "value": n2 if err2 is None else "",
                "status": "ok" if err2 is None else "error",
                "detail": (err2 or "")[:500],
            }
        )
    else:
        metrics_rows.append(
            {
                "env": env,
                "database": db,
                "metric_group": "review_queue",
                "object": "qa.manual_review_queue",
                "metric": "total_rows",
                "value": "",
                "status": "missing",
                "detail": "",
            }
        )

    # Latest release manifest rows (no PHI — tags/SHAs only)
    if _table_exists(con, "qa", "release_manifest"):
        try:
            rm = con.execute(
                """
                SELECT release_tag, git_sha, created_at
                FROM qa.release_manifest
                ORDER BY created_at DESC NULLS LAST
                LIMIT 5
                """
            ).fetchall()
            out["release_manifest_latest"] = [
                {"release_tag": r[0], "git_sha": r[1], "created_at": str(r[2]) if r[2] else None}
                for r in rm
            ]
        except Exception as e:
            out["release_manifest_latest"] = []
            out["release_manifest_error"] = str(e)
    else:
        out["release_manifest_latest"] = []

    # Release schema footprints
    for rs in release_schemas[:12]:
        try:
            n = con.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_catalog = current_database()
                  AND table_schema = ?
                """,
                [rs],
            ).fetchone()[0]
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "release_schema",
                    "object": rs,
                    "metric": "n_objects",
                    "value": int(n),
                    "status": "ok",
                    "detail": "",
                }
            )
        except Exception as e:
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "release_schema",
                    "object": rs,
                    "metric": "n_objects",
                    "value": "",
                    "status": "error",
                    "detail": str(e)[:500],
                }
            )

    # Staging load inventory
    if _table_exists(con, "v2_stage", "load_inventory"):
        try:
            row = con.execute(
                """
                SELECT BOOL_AND(row_match), COUNT(*), MAX(load_id)
                FROM v2_stage.load_inventory
                """
            ).fetchone()
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "staging_consistency",
                    "object": "v2_stage.load_inventory",
                    "metric": "all_row_match",
                    "value": str(row[0]) if row[0] is not None else "",
                    "status": "PASS" if row[0] else "FAIL",
                    "detail": "",
                }
            )
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "staging_consistency",
                    "object": "v2_stage.load_inventory",
                    "metric": "n_inventory_rows",
                    "value": int(row[1]) if row[1] is not None else "",
                    "status": "ok",
                    "detail": "",
                }
            )
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "staging_consistency",
                    "object": "v2_stage.load_inventory",
                    "metric": "max_load_id",
                    "value": str(row[2]) if row[2] is not None else "",
                    "status": "ok",
                    "detail": "",
                }
            )
        except Exception as e:
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "staging_consistency",
                    "object": "v2_stage.load_inventory",
                    "metric": "error",
                    "value": "",
                    "status": "error",
                    "detail": str(e)[:500],
                }
            )

    # Multimodal contract validation (blocking tables must be empty)
    mm_found_schema: str | None = None
    for sch in MM_SCHEMA_CANDIDATES:
        hit = False
        for t in MM_BLOCKING:
            if _table_exists(con, sch, t):
                hit = True
                break
        if hit:
            mm_found_schema = sch
            break
    out["mm_validation_schema"] = mm_found_schema
    if mm_found_schema:
        for t in MM_BLOCKING:
            fq = f'{mm_found_schema}."{t}"'
            if not _table_exists(con, mm_found_schema, t):
                metrics_rows.append(
                    {
                        "env": env,
                        "database": db,
                        "metric_group": "mm_validation",
                        "object": f"{mm_found_schema}.{t}",
                        "metric": "row_count",
                        "value": "",
                        "status": "missing",
                        "detail": "",
                    }
                )
                continue
            n, err = _safe_count(con, f"SELECT COUNT(*) FROM {fq}")
            status = "GREEN" if (n == 0 and err is None) else ("RED" if err is None else "error")
            metrics_rows.append(
                {
                    "env": env,
                    "database": db,
                    "metric_group": "mm_validation",
                    "object": f"{mm_found_schema}.{t}",
                    "metric": "row_count",
                    "value": n if err is None else "",
                    "status": status,
                    "detail": (err or "")[:500],
                }
            )

    return out


def _query_history_summary(con: duckdb.DuckDBPyConnection) -> dict[str, object]:
    """PHI-safe: aggregates only (no query text)."""
    out: dict[str, object] = {"ok": False}
    sql = """
    SELECT
      COALESCE(NULLIF(TRIM(user_agent), ''), '(empty)') AS user_agent,
      COUNT(*) AS n
    FROM md_information_schema.query_history
    WHERE start_time >= now() - INTERVAL '14 days'
      AND (
        user_agent ILIKE '%THYROID_2026%'
        OR session_name ILIKE '%THYROID%'
      )
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 25
    """
    try:
        rows = con.execute(sql).fetchall()
        out["ok"] = True
        out["by_user_agent_14d"] = [{"user_agent": r[0], "n_queries": int(r[1])} for r in rows]
    except Exception as e:
        out["error"] = str(e)
    return out


def main() -> None:
    os.environ.setdefault("MOTHERDUCK_SESSION_HINT", DEFAULT_SESSION)
    os.environ.setdefault(
        "MOTHERDUCK_CUSTOM_USER_AGENT",
        "THYROID_2026_molecular/147_readonly_audit;kind=audit",
    )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    metrics_rows: list[dict[str, object]] = []
    env_summaries: list[dict[str, object]] = []
    findings: list[tuple[str, str]] = []  # (severity, text)
    qh_summary: dict[str, object] | None = None

    token_rs = read_scaling_token_mode()
    token_rw = token_mode()

    for env in ("dev", "qa", "prod"):
        db_expect = resolve_database_for_env(env)
        try:
            con, mode = _connect_env(env)
        except Exception as e:
            env_summaries.append(
                {
                    "env": env,
                    "expected_database": db_expect,
                    "connect_error": str(e),
                    "connection_mode": "failed",
                }
            )
            findings.append(("high", f"{env}: cannot connect to MotherDuck — {type(e).__name__}"))
            continue
        try:
            summ = _audit_one_env(env, con, metrics_rows)
            summ["expected_database"] = db_expect
            summ["connection_mode"] = mode
            db_actual = summ.get("current_database")
            if db_actual != db_expect:
                findings.append(
                    (
                        "medium",
                        f"{env}: database name mismatch (expected `{db_expect}`, connected `{db_actual}`)",
                    )
                )
            env_summaries.append(summ)

            # Cross-check staging vs main row counts when both exist
            main_n = None
            stage_n = None
            for row in metrics_rows:
                if row.get("env") != env:
                    continue
                if row.get("object") == "main.canonical_extracted_fact_long_v2":
                    if row.get("metric") == "canonical_clean_rows" and row.get("status") == "ok":
                        main_n = row.get("value")
                if row.get("object") == "v2_stage.canonical_extracted_fact_long_v2":
                    if row.get("metric") == "staging_clean_rows" and row.get("status") == "ok":
                        stage_n = row.get("value")
            if (
                isinstance(main_n, int)
                and isinstance(stage_n, int)
                and stage_n > main_n * 1.05
            ):
                findings.append(
                    (
                        "low",
                        f"{env}: v2_stage canonical_extracted_fact_long_v2 row count ({stage_n:,}) "
                        f"materially exceeds main ({main_n:,}) — confirm promotion status.",
                    )
                )

            if env == "prod" and qh_summary is None:
                qh_summary = _query_history_summary(con)
        finally:
            con.close()

    if qh_summary and not qh_summary.get("ok"):
        findings.append(
            (
                "low",
                f"query_history telemetry not available: {qh_summary.get('error', 'unknown')}",
            )
        )

    # Post-process: multimodal + staging findings (dedupe across envs)
    mm_by_obj: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in metrics_rows:
        if row.get("metric_group") != "mm_validation":
            continue
        env_lab = str(row.get("env") or "")
        obj = str(row.get("object") or "")
        st = str(row.get("status") or "")
        if st == "RED":
            mm_by_obj[obj]["RED"].add(env_lab)
        elif st == "missing":
            mm_by_obj[obj]["missing"].add(env_lab)
        elif st == "GREEN":
            mm_by_obj[obj]["GREEN"].add(env_lab)

    for obj, states in sorted(mm_by_obj.items()):
        if states.get("RED"):
            envs = ", ".join(sorted(states["RED"]))
            val = next(
                (
                    r.get("value")
                    for r in metrics_rows
                    if r.get("object") == obj and r.get("status") == "RED"
                ),
                "?",
            )
            findings.append(
                (
                    "high",
                    f"Multimodal blocker `{obj}` has {val} rows on [{envs}] (strict-release expects 0).",
                )
            )
        if states.get("missing"):
            envs = ", ".join(sorted(states["missing"]))
            findings.append(
                (
                    "medium",
                    f"Multimodal validation object missing on [{envs}]: `{obj}` "
                    f"(present elsewhere or not deployed).",
                )
            )

    # Staging plane: v2_stage canonical fact table present on dev but absent on qa/prod
    dev_has_stage_facts = any(
        r.get("env") == "dev"
        and r.get("object") == "v2_stage.canonical_extracted_fact_long_v2"
        and r.get("metric") == "staging_clean_rows"
        and r.get("status") == "ok"
        for r in metrics_rows
    )
    for env in ("qa", "prod"):
        miss = next(
            (
                r
                for r in metrics_rows
                if r.get("env") == env
                and r.get("object") == "v2_stage.canonical_extracted_fact_long_v2"
                and r.get("metric") == "staging_clean_rows"
                and r.get("status") == "missing"
            ),
            None,
        )
        if dev_has_stage_facts and miss:
            findings.append(
                (
                    "medium",
                    f"{env}: `v2_stage.canonical_extracted_fact_long_v2` not materialized, "
                    f"while dev has staging facts — staging/replica plane may be incomplete for this catalog.",
                )
            )

    REPORTS.mkdir(parents=True, exist_ok=True)
    csv_path = REPORTS / "motherduck_read_only_metrics.csv"
    if metrics_rows:
        fields = sorted({k for r in metrics_rows for k in r.keys()})
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            for r in metrics_rows:
                w.writerow({k: r.get(k, "") for k in fields})

    # Dedupe findings by text
    seen: set[str] = set()
    ranked: list[tuple[str, str]] = []
    for sev, text in findings:
        if text in seen:
            continue
        seen.add(text)
        ranked.append((sev, text))
    severity_order = {"high": 0, "medium": 1, "low": 2}
    ranked.sort(key=lambda x: severity_order.get(x[0], 9))

    # Snapshot table for markdown (prod-focused; all envs in CSV)
    prod_snap: list[dict[str, object]] = [
        r for r in metrics_rows if str(r.get("env")) == "prod" and r.get("metric_group") == "row_count"
    ]
    prod_review = next(
        (r for r in metrics_rows if r.get("env") == "prod" and r.get("metric") == "total_rows"),
        None,
    )
    prod_pending = next(
        (
            r
            for r in metrics_rows
            if r.get("env") == "prod" and r.get("metric") == "pending_verification_null"
        ),
        None,
    )

    md_lines = [
        "# THYROID_2026 — MotherDuck read-only operational audit",
        "",
        f"**Generated (UTC):** {now}",
        "",
        "## Executive snapshot (production catalog)",
        "",
        "| Metric | Value |",
        "|--------|------:|",
    ]
    def _fmt_cell(v: object) -> str:
        if isinstance(v, int):
            return f"{v:,}"
        return str(v)

    for r in sorted(prod_snap, key=lambda x: str(x.get("object"))):
        if r.get("status") == "ok":
            md_lines.append(
                f"| `{r.get('object')}` — {r.get('metric')} | {_fmt_cell(r.get('value'))} |"
            )
        elif r.get("status") == "missing":
            md_lines.append(f"| `{r.get('object')}` — {r.get('metric')} | _(missing)_ |")
    if prod_pending and prod_pending.get("status") == "ok":
        md_lines.append(
            f"| `qa.manual_review_queue` — pending (NULL verification_status) | "
            f"{_fmt_cell(prod_pending.get('value'))} |"
        )
    if prod_review and prod_review.get("status") == "ok":
        md_lines.append(
            f"| `qa.manual_review_queue` — total rows | {_fmt_cell(prod_review.get('value'))} |"
        )
    md_lines.extend(
        [
            "",
            "_Full per-environment metrics:_ see CSV below.",
            "",
            "## Method & constraints",
            "",
            "- **Session hint:** `MOTHERDUCK_SESSION_HINT` = "
            f"`{(os.environ.get('MOTHERDUCK_SESSION_HINT') or DEFAULT_SESSION)}`",
            "- **Attribution:** `MOTHERDUCK_CUSTOM_USER_AGENT` set to project audit string (see runbook).",
            f"- **Read-scaling token source:** `{token_rs}`",
            f"- **Read/write token source (if used):** `{token_rw}`",
            "- **Queries:** `SELECT` / metadata only — no DDL/DML, no `ATTACH`/`DETACH` in this script.",
            "- **PHI:** report contains counts, schema names, release tags, and git SHAs only "
            "— not clinical narratives.",
            "",
            "## Environment summary",
            "",
        ]
    )
    for s in env_summaries:
        md_lines.append(f"### `{s.get('env')}`")
        md_lines.append("")
        if s.get("connect_error"):
            md_lines.append(f"- **Connect:** FAILED — `{s['connect_error'][:200]}`")
        else:
            md_lines.append(f"- **Database:** `{s.get('current_database')}`")
            md_lines.append(f"- **Expected (config):** `{s.get('expected_database')}`")
            md_lines.append(f"- **Connection mode:** `{s.get('connection_mode')}`")
            sch = s.get("schemas") or []
            md_lines.append(f"- **Schemas (information_schema):** {len(sch)} total")
            rs = s.get("release_schemas") or []
            if rs:
                md_lines.append(f"- **Release schemas:** {', '.join(rs[:20])}" + (" …" if len(rs) > 20 else ""))
            rm = s.get("release_manifest_latest") or []
            if rm:
                md_lines.append("- **qa.release_manifest (latest):**")
                for r in rm[:5]:
                    md_lines.append(
                        f"  - `{r.get('release_tag')}` | `{r.get('git_sha')}` | {r.get('created_at')}"
                    )
            elif s.get("release_manifest_error"):
                md_lines.append(f"- **qa.release_manifest:** error — `{s['release_manifest_error'][:200]}`")
            mm_s = s.get("mm_validation_schema")
            md_lines.append(
                f"- **Multimodal validation schema:** `{mm_s}`" if mm_s else "- **Multimodal validation schema:** _(not found)_"
            )
        md_lines.append("")

    md_lines.extend(
        [
            "## Query history (org-level, PHI-safe aggregate)",
            "",
        ]
    )
    if qh_summary and qh_summary.get("ok"):
        md_lines.append("_Last 14 days, filtered to THYROID session/user-agent tokens; no SQL text included._")
        md_lines.append("")
        md_lines.append("| user_agent | queries |")
        md_lines.append("|---|---:|")
        for r in qh_summary.get("by_user_agent_14d") or []:
            ua = str(r.get("user_agent", "")).replace("|", "\\|")[:120]
            md_lines.append(f"| `{ua}` | {r.get('n_queries')} |")
        md_lines.append("")
    else:
        err = (qh_summary or {}).get("error", "not queried")
        md_lines.append(f"_(Not available: {err})_")
        md_lines.append("")

    md_lines.extend(
        [
            "## Findings (severity-ranked)",
            "",
        ]
    )
    if ranked:
        for sev, text in ranked:
            md_lines.append(f"- **{sev.upper()}:** {text}")
        md_lines.append("")
    else:
        md_lines.append("_No automated findings recorded._")
        md_lines.append("")

    md_lines.extend(
        [
            "## Machine-readable metrics",
            "",
            "See [`motherduck_read_only_metrics.csv`](motherduck_read_only_metrics.csv).",
            "",
        ]
    )

    (REPORTS / "motherduck_read_only_audit.md").write_text("\n".join(md_lines), encoding="utf-8")
    print(f"  [ok] wrote {REPORTS / 'motherduck_read_only_audit.md'}")
    print(f"  [ok] wrote {csv_path}")


if __name__ == "__main__":
    main()
