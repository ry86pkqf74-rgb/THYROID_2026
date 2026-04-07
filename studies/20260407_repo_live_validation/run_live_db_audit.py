#!/usr/bin/env python3
"""Read-only live MotherDuck audit for publication validation (2026-04-07).

Uses only ``motherduck_client`` and ``utils/md_connect`` verification helpers.
Run from repository root:

  cd THYROID_2026
  export MOTHERDUCK_CUSTOM_USER_AGENT='THYROID_2026_live_audit'
  export MOTHERDUCK_SESSION_HINT='live_publication_validation_20260407'
  .venv/bin/python studies/20260407_repo_live_validation/run_live_db_audit.py

Writes:
  - live_db_audit.md
  - live_db_metrics.csv
  - live_vs_repo_consistency.md
"""
from __future__ import annotations

import csv
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

# Defaults requested by operator
os.environ.setdefault("MOTHERDUCK_CUSTOM_USER_AGENT", "THYROID_2026_live_audit")
os.environ.setdefault("MOTHERDUCK_SESSION_HINT", "live_publication_validation_20260407")

import duckdb  # noqa: E402

from motherduck_client import (  # noqa: E402
    MotherDuckClient,
    get_read_scaling_token,
    get_token,
    read_scaling_token_mode,
    resolve_database_for_env,
    token_mode,
)
from utils.md_connect import _verify_md_connection  # noqa: E402


OUT_DIR = Path(__file__).resolve().parent
DEFAULT_ENV = (os.getenv("MOTHERDUCK_ENV") or "prod").strip().lower()


def _len_status(val: str | None) -> tuple[str, int]:
    if val and str(val).strip():
        return "SET", len(str(val).strip())
    return "MISSING", 0


def preflight_token_report() -> tuple[str, str]:
    """Return (multiline preflight text, credential_mode: read_scaling | read_write)."""
    lines: list[str] = ["## Token preflight (names and lengths only)", ""]
    rs_keys = ("MD_READ_SCALING_TOKEN", "MOTHERDUCK_READ_SCALING_TOKEN")
    for k in rs_keys:
        st, ln = _len_status(os.getenv(k))
        lines.append(f"- `{k}`: **{st}** (length={ln})")
    rs_tok = get_read_scaling_token()
    if rs_tok:
        lines.append("")
        lines.append(
            f"**Effective read-scaling token:** **SET** (length={len(rs_tok)}), "
            f"source `{read_scaling_token_mode()}`."
        )
        lines.append("**Selected credential:** read-scaling (`MotherDuckClient.connect_read_scaling`).")
        return "\n".join(lines), "read_scaling"

    lines.append("")
    lines.append("_No read-scaling token in env; checking read/write env vars (then secrets file)._")
    for k in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        st, ln = _len_status(os.getenv(k))
        lines.append(f"- `{k}`: **{st}** (length={ln})")
    rw = get_token()
    if not rw:
        lines.append("")
        lines.append(
            "**FATAL:** No token from `get_read_scaling_token` / `get_token` "
            "(env + `.streamlit/secrets.toml` per `motherduck_client`)."
        )
        return "\n".join(lines), "none"
    tw = token_mode()
    lines.append("")
    lines.append(f"**Effective read/write token:** **SET** (length={len(rw)}), source `{tw}`.")
    lines.append("**Selected credential:** read/write (`MotherDuckClient.connect_rw`, SELECT-only here).")
    return "\n".join(lines), "read_write"


def connect_readonly_live() -> tuple[duckdb.DuckDBPyConnection, str]:
    """MotherDuck connection: read-scaling when configured, else RW (SELECT-only by caller)."""
    env = DEFAULT_ENV
    client = MotherDuckClient.for_env(
        env,
        custom_user_agent=os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT"),
        motherduck_session_hint=os.environ.get("MOTHERDUCK_SESSION_HINT"),
    )
    if get_read_scaling_token():
        con = client.connect_read_scaling()
        mode = "read_scaling"
    else:
        tok = get_token()
        if not tok:
            raise RuntimeError("No token available for MotherDuck connection.")
        con = client.connect_rw()
        mode = "read_write"
    if not _verify_md_connection(con):
        con.close()
        raise RuntimeError("PRAGMA database_list did not show MotherDuck (md:) — refusing to proceed.")
    return con, mode


def safe_scalar(con: duckdb.DuckDBPyConnection, sql: str) -> str:
    try:
        row = con.execute(sql).fetchone()
        if row is None:
            return ""
        return str(row[0]) if row[0] is not None else ""
    except Exception as exc:
        return f"ERROR: {exc}"


def safe_df(con: duckdb.DuckDBPyConnection, sql: str):
    try:
        return con.execute(sql).df()
    except Exception as exc:
        import pandas as pd

        return pd.DataFrame({"error": [str(exc)]})


def fetch_count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    """COUNT(*) helper — DuckDB stubs may type fetchone() as possibly None."""
    row = con.execute(sql).fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


@dataclass
class MetricRow:
    metric_id: str
    value: str
    detail: str = ""
    sql_or_note: str = ""


@dataclass
class AuditState:
    rows: list[MetricRow] = field(default_factory=list)
    md_features: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def add(self, mid: str, val: str, detail: str = "", note: str = "") -> None:
        self.rows.append(MetricRow(mid, val, detail, note))


def main() -> int:
    preflight_md, cred_mode = preflight_token_report()
    if cred_mode == "none":
        (OUT_DIR / "live_db_audit.md").write_text(
            "# Live DB audit — FAILED PREFLIGHT\n\n" + preflight_md + "\n", encoding="utf-8"
        )
        print(preflight_md)
        return 2

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    state = AuditState()
    db_name_plan = resolve_database_for_env(DEFAULT_ENV)

    con, conn_mode = connect_readonly_live()
    try:
        cur_db = safe_scalar(con, "SELECT current_database()")
        state.add("current_database", cur_db, f"MOTHERDUCK_ENV={DEFAULT_ENV}", "SELECT current_database()")
        state.add("planned_database_for_env", db_name_plan, DEFAULT_ENV, "resolve_database_for_env")

        # Schemas
        df_schemas = safe_df(
            con,
            """
            SELECT DISTINCT schema_name
            FROM information_schema.schemata
            ORDER BY 1
            """,
        )
        schema_list = []
        if "schema_name" in df_schemas.columns:
            schema_list = df_schemas["schema_name"].astype(str).tolist()
        release_schemas = sorted({s for s in schema_list if s.startswith("release_")})
        state.add("n_schemas_total", str(len(schema_list)))
        state.add("release_schemas_count", str(len(release_schemas)), ",".join(release_schemas[:15]))

        # MD information schema (case variants)
        for label, sql in (
            ("md_information_schema.databases", "SELECT * FROM md_information_schema.databases ORDER BY 1"),
            (
                "MD_INFORMATION_SCHEMA.DATABASES",
                "SELECT * FROM MD_INFORMATION_SCHEMA.DATABASES ORDER BY 1",
            ),
        ):
            try:
                n = fetch_count(con, f"SELECT COUNT(*) FROM ({sql}) t")
                state.add(f"accessible:{label}", "yes", f"row_count={n}")
                state.md_features.append(f"{label} ({n} rows)")
                break
            except Exception as exc:
                state.add(f"accessible:{label}", "no", str(exc)[:200])
        else:
            state.errors.append("No working MD_INFORMATION_SCHEMA.DATABASES query")

        for snap_sql, snap_label in (
            ("SELECT * FROM md_information_schema.database_snapshots ORDER BY 1", "database_snapshots"),
            ("SELECT * FROM MD_INFORMATION_SCHEMA.DATABASE_SNAPSHOTS ORDER BY 1", "DATABASE_SNAPSHOTS"),
        ):
            try:
                n = fetch_count(con, f"SELECT COUNT(*) FROM ({snap_sql}) t")
                state.add(f"accessible:{snap_label}", "yes", f"row_count={n}")
                state.md_features.append(f"MD snapshots metadata ({snap_label}, {n} rows)")
                break
            except Exception as exc:
                state.add(f"accessible:{snap_label}", "maybe_no", str(exc)[:200])

        for qh_sql, qh_lab in (
            (
                "SELECT COUNT(*) FROM md_information_schema.query_history",
                "query_history",
            ),
            (
                "SELECT COUNT(*) FROM MD_INFORMATION_SCHEMA.QUERY_HISTORY",
                "QUERY_HISTORY",
            ),
            ("SELECT COUNT(*) FROM md_information_schema.recent_queries", "recent_queries"),
            ("SELECT COUNT(*) FROM MD_INFORMATION_SCHEMA.RECENT_QUERIES", "RECENT_QUERIES"),
        ):
            try:
                n = fetch_count(con, qh_sql)
                if n >= 0:
                    state.add(f"accessible:{qh_lab}", "yes", f"row_count={n}")
                    state.md_features.append(f"Query history / {qh_lab} (count={n})")
                    break
            except Exception:
                continue
        else:
            state.add("accessible:query_history", "no", "Could not read query_history/recent_queries")

        # MRQ
        try:
            tot = fetch_count(con, "SELECT COUNT(*) FROM qa.manual_review_queue")
            state.add("mrq_total_rows", str(tot))
            df_v = con.execute(
                """
                SELECT COALESCE(verification_status::VARCHAR, '(NULL)') AS verification_status, COUNT(*) AS n
                FROM qa.manual_review_queue
                GROUP BY 1
                ORDER BY n DESC
                """
            ).df()
            for _, r in df_v.iterrows():
                state.add(f"mrq_status:{r['verification_status']}", str(int(r["n"])))
            syn_rows = fetch_count(
                con,
                """
                SELECT COUNT(*) FROM qa.manual_review_queue
                WHERE CAST(verification_status AS VARCHAR) ILIKE '%SYNTHETIC%'
                   OR CAST(verification_status AS VARCHAR) ILIKE '%AUTOMATION_ONLY%'
                """,
            )
            state.add("mrq_synthetic_or_automation_status_rows", str(syn_rows))
            auto_rows = fetch_count(
                con,
                """
                SELECT COUNT(*) FROM qa.manual_review_queue
                WHERE CAST(verification_status AS VARCHAR) ILIKE 'auto_accepted%'
                """,
            )
            state.add("mrq_auto_accepted_prefix_rows", str(auto_rows))
            human_ok = fetch_count(
                con,
                """
                SELECT COUNT(*) FROM qa.manual_review_queue
                WHERE CAST(verification_status AS VARCHAR) IN ('confirmed_correct')
                """,
            )
            state.add("mrq_confirmed_correct_rows", str(human_ok))
            null_mrq = fetch_count(
                con,
                "SELECT COUNT(*) FROM qa.manual_review_queue WHERE verification_status IS NULL",
            )
            state.add("mrq_verification_status_null_rows", str(null_mrq))
        except Exception as exc:
            state.add("mrq", "ERROR", str(exc))
            state.errors.append(str(exc))

        # Promotion decisions
        try:
            pr_tot = fetch_count(con, "SELECT COUNT(*) FROM qa.promotion_review_decisions")
            pr_batch_nonnull = fetch_count(
                con,
                "SELECT COUNT(*) FROM qa.promotion_review_decisions WHERE decision_batch_id IS NOT NULL",
            )
            pr_batch_null = fetch_count(
                con,
                "SELECT COUNT(*) FROM qa.promotion_review_decisions WHERE decision_batch_id IS NULL",
            )
            state.add("promotion_review_decisions_total", str(pr_tot))
            state.add("promotion_decision_batch_id_nonnull", str(pr_batch_nonnull))
            state.add("promotion_decision_batch_id_null", str(pr_batch_null))
            dist = con.execute(
                """
                SELECT COALESCE(decision_batch_id::VARCHAR, '(NULL)') AS batch, COUNT(*) AS n
                FROM qa.promotion_review_decisions
                GROUP BY 1
                ORDER BY n DESC
                """
            ).df()
            for _, r in dist.iterrows():
                state.add(f"promotion_batch:{r['batch']}", str(int(r["n"])))
        except Exception as exc:
            state.add("promotion_review_decisions", "ERROR", str(exc))

        # Longitudinal labs — institutional wave
        try:
            wave_df = con.execute(
                """
                SELECT COALESCE(ingestion_wave::VARCHAR, '(NULL)') AS wave, COUNT(*) AS n
                FROM main.longitudinal_lab_canonical_v1
                GROUP BY 1
                ORDER BY n DESC
                LIMIT 40
                """
            ).df()
            for _, r in wave_df.iterrows():
                state.add(f"lab_wave:{r['wave']}", str(int(r["n"])))
            fi = fetch_count(
                con,
                """
                SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1
                WHERE CAST(ingestion_wave AS VARCHAR) ILIKE '%final_institutional%'
                """,
            )
            state.add("longitudinal_lab_rows_final_institutional_wave", str(fi))
        except Exception as exc:
            state.add("longitudinal_lab_canonical_v1", "ERROR", str(exc))

        # Specimen / FHIR QA (119/142)
        for check in (
            (
                "val_specimen_contract_fail_rows",
                "SELECT COUNT(*) FROM qa.val_specimen_contract_v1 WHERE UPPER(CAST(status AS VARCHAR)) = 'FAIL'",
            ),
            (
                "val_specimen_genomic_binding_fail_rows",
                "SELECT COUNT(*) FROM qa.val_specimen_genomic_binding_v1 "
                "WHERE UPPER(CAST(status AS VARCHAR)) = 'FAIL'",
            ),
            ("v_diag_broken_fhir_refs", "SELECT COUNT(*) FROM qa.v_diag_specimen_fhir_broken_refs_v1"),
            ("v_diag_review_burden_rows", "SELECT COUNT(*) FROM qa.v_diag_specimen_review_burden_v1"),
        ):
            label, sql = check
            try:
                n = fetch_count(con, sql)
                state.add(label, str(n))
            except Exception as exc:
                state.add(label, "n/a", str(exc)[:300])

        # Release manifest freshness
        try:
            rm = con.execute(
                """
                SELECT release_tag, git_sha, created_at
                FROM qa.release_manifest
                ORDER BY created_at DESC NULLS LAST
                LIMIT 5
                """
            ).df()
            for i, r in rm.iterrows():
                state.add(
                    f"release_manifest_row_{i}",
                    str(r.get("release_tag", "")),
                    f"created_at={r.get('created_at')}, git_sha={r.get('git_sha')}",
                )
        except Exception as exc:
            state.add("qa.release_manifest", "ERROR", str(exc))

    finally:
        con.close()

    # Write CSV
    csv_path = OUT_DIR / "live_db_metrics.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["metric_id", "value", "detail", "sql_or_note"])
        for r in state.rows:
            w.writerow([r.metric_id, r.value, r.detail, r.sql_or_note])

    # Verdict
    blockers: list[str] = []
    tech_fail = False
    mrq_synth = next((r for r in state.rows if r.metric_id == "mrq_synthetic_or_automation_status_rows"), None)
    if mrq_synth and mrq_synth.value.isdigit() and int(mrq_synth.value) > 0:
        blockers.append(
            f"MRQ rows with SYNTHETIC/AUTOMATION status text: {mrq_synth.value} "
            "(README: not manuscript sign-off posture)."
        )
    mrq_auto = next((r for r in state.rows if r.metric_id == "mrq_auto_accepted_prefix_rows"), None)
    mrq_human = next((r for r in state.rows if r.metric_id == "mrq_confirmed_correct_rows"), None)
    mrq_tot = next((r for r in state.rows if r.metric_id == "mrq_total_rows"), None)
    if (
        mrq_auto
        and mrq_human
        and mrq_tot
        and mrq_auto.value.isdigit()
        and mrq_human.value.isdigit()
        and mrq_tot.value.isdigit()
    ):
        tot_n = int(mrq_tot.value)
        hum_n = int(mrq_human.value)
        if tot_n > 0 and hum_n < tot_n:
            blockers.append(
                f"Governance MRQ: {mrq_auto.value}/{mrq_tot.value} rows are auto_accepted*; "
                f"only {hum_n} confirmed_correct — README requires human-reviewed manuscript sign-off "
                f"beyond automation-only acceptance (remaining {tot_n - hum_n} rows)."
            )
    pr_null = next((r for r in state.rows if r.metric_id == "promotion_decision_batch_id_null"), None)
    if pr_null and pr_null.value.isdigit() and int(pr_null.value) > 0:
        blockers.append(f"promotion_review_decisions with NULL decision_batch_id: {pr_null.value}")

    brk = next((r for r in state.rows if r.metric_id == "v_diag_broken_fhir_refs"), None)
    if brk and brk.value.isdigit() and int(brk.value) > 0:
        blockers.append(f"qa.v_diag_specimen_fhir_broken_refs_v1 count: {brk.value} (investigate vs PASS reports).")
        tech_fail = True

    fail_contract = next((r for r in state.rows if r.metric_id == "val_specimen_contract_fail_rows"), None)
    if fail_contract and fail_contract.value.isdigit() and int(fail_contract.value) > 0:
        blockers.append(f"qa.val_specimen_contract_v1 FAIL rows: {fail_contract.value}")
        tech_fail = True

    fail_gen = next((r for r in state.rows if r.metric_id == "val_specimen_genomic_binding_fail_rows"), None)
    if fail_gen and fail_gen.value.isdigit() and int(fail_gen.value) > 0:
        blockers.append(
            f"qa.val_specimen_genomic_binding_v1 FAIL rows: {fail_gen.value} (specimen/FHIR publication gate)."
        )
        tech_fail = True

    fi_lab = next(
        (r for r in state.rows if r.metric_id == "longitudinal_lab_rows_final_institutional_wave"), None
    )
    if fi_lab and fi_lab.value == "0":
        blockers.append(
            "No rows in longitudinal_lab_canonical_v1 tagged with final_institutional ingestion_wave (README claims wave closed)."
        )

    mrq_pending = next((r for r in state.rows if r.metric_id == "mrq_verification_status_null_rows"), None)
    if mrq_pending and mrq_pending.value.isdigit() and int(mrq_pending.value) > 0:
        blockers.append(
            f"qa.manual_review_queue rows with NULL verification_status (pending): {mrq_pending.value}"
        )

    if tech_fail:
        verdict = "FAIL"
    elif blockers:
        verdict = "HOLD"
    else:
        verdict = "PASS"

    md_body = [
        "# Live MotherDuck audit (read-only)\n",
        f"- **UTC timestamp:** {ts}",
        f"- **Connection mode:** `{conn_mode}` (preflight credential class: `{cred_mode}`)",
        f"- **User agent:** `{os.environ.get('MOTHERDUCK_CUSTOM_USER_AGENT')}`",
        f"- **Session hint:** `{os.environ.get('MOTHERDUCK_SESSION_HINT')}`",
        "",
        preflight_md,
        "",
        "## Environment",
        f"- `MOTHERDUCK_ENV`: `{DEFAULT_ENV}`",
        f"- Resolved database name (config): `{db_name_plan}`",
        f"- `current_database()` at connect: `{cur_db}`",
        "",
        "## MotherDuck Business features exercised (this run)",
        "- Read-only **SELECT** only; no DDL/DML; no `ATTACH 'md:'` workspace mode.",
    ]
    for feat in state.md_features:
        md_body.append(f"- {feat}")
    if conn_mode == "read_scaling":
        md_body.append("- **Read scaling** token path via `MotherDuckClient.connect_read_scaling()`.")
    else:
        md_body.append("- **Read/write** token path via `MotherDuckClient.connect_rw()` (queries were SELECT-only).")

    md_body.extend(
        [
            "",
            "## Key metrics",
            "",
            "| metric_id | value | detail |",
            "|-----------|-------|--------|",
        ]
    )
    for r in state.rows[:80]:
        md_body.append(f"| {r.metric_id} | {r.value} | {r.detail} |")
    if len(state.rows) > 80:
        md_body.append(f"| … | … | _see live_db_metrics.csv for {len(state.rows)} total rows_ |")

    md_body.extend(
        [
            "",
            "## Publication readiness",
            f"- **Verdict:** **{verdict}**",
            "- **Live blockers (exact):**",
        ]
    )
    if blockers:
        for b in blockers:
            md_body.append(f"  - {b}")
    else:
        md_body.append("  - _(none flagged by this automated gate set)_")

    (OUT_DIR / "live_db_audit.md").write_text("\n".join(md_body), encoding="utf-8")

    # Consistency memo vs README + signoff folder
    readme_claims = [
        "README (2026-04-07): 119 release-mode PASS WITH WARN; governance blocked on synthetic MRQ until human-reviewed hydrate.",
        "README: final institutional non-Tg lab wave `final_institutional_20260407` closed via script 127.",
        "studies/20260407_publication_signoff_live/final_verdict_memo.md: historical MRQ 5620/5622 SYNTHETIC; later deltas said operator saw no synthetic slice — reconcile on live.",
    ]
    cmp_lines = [
        "# Live vs repository consistency\n",
        f"- Audit UTC: {ts}",
        f"- Credential: **{conn_mode}** (class `{cred_mode}`)",
        "",
        "## README / signoff claims checked",
    ]
    for c in readme_claims:
        cmp_lines.append(f"- {c}")
    cmp_lines.extend(["", "## Live evidence (this run)", ""])
    if mrq_synth:
        cmp_lines.append(
            f"- **MRQ synthetic/automation filter count:** {mrq_synth.value} rows "
            "(ILIKE '%SYNTHETIC%' OR '%AUTOMATION_ONLY%' on verification_status)."
        )
    if mrq_auto and mrq_human:
        cmp_lines.append(
            f"- **MRQ auto_accepted* vs confirmed_correct:** {mrq_auto.value} vs {mrq_human.value} "
            "(README: manuscript governance expects human-reviewed posture)."
        )
    if fi_lab:
        cmp_lines.append(f"- **final_institutional longitudinal rows:** {fi_lab.value}")
    if brk:
        cmp_lines.append(f"- **Broken FHIR refs (v_diag):** {brk.value}")
    if pr_null:
        cmp_lines.append(f"- **NULL decision_batch_id in promotion_review_decisions:** {pr_null.value}")

    cmp_lines.extend(
        [
            "",
            "## Stale checked-in PASS reports",
            "- Compare this run to `studies/20260407_formalization_validation_release_mode/validation_report.md` (early 20-check PASS — treat as history per README).",
            "- Prefer `studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md` for committed 27-check lineage audit; **re-run 119** if you need same checklist as that artifact.",
            "",
            "## Consistency summary",
        ]
    )
    if verdict == "PASS":
        cmp_lines.append("- Automated slice: **aligned** with PASS posture for the checks above.")
    elif verdict == "HOLD":
        cmp_lines.append("- **HOLD:** governance or data wave signal differs from publication-ready bar — see `live_db_audit.md` blockers.")
    else:
        cmp_lines.append("- **FAIL:** specimen/FHIR or contract FAIL signals require remediation before publication.")

    (OUT_DIR / "live_vs_repo_consistency.md").write_text("\n".join(cmp_lines), encoding="utf-8")

    print(preflight_md)
    print(f"\nWrote {csv_path.name}, live_db_audit.md, live_vs_repo_consistency.md")
    print(f"Verdict: {verdict}")
    return 0 if verdict != "FAIL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
