#!/usr/bin/env python3
"""Write studies/CURRENT_MOTHERDUCK_REPO_STATE.md — live DB + repo artifact reconciliation.

Read-only against MotherDuck unless views are deployed (no DDL). On ``--md``,
uses ``specimen_fhir_release_writer_attribution`` (default UA
``specimen_fhir_ref_integrity_v2``; override with ``MOTHERDUCK_CUSTOM_USER_AGENT``,
e.g. ``specimen_fhir_release_truth_v2`` for release-truth runs).

Usage:
  .venv/bin/python scripts/144_md_repo_current_state_summary.py
  .venv/bin/python scripts/144_md_repo_current_state_summary.py --md
  .venv/bin/python scripts/144_md_repo_current_state_summary.py --md --output studies/CURRENT_MOTHERDUCK_REPO_STATE.md
  .venv/bin/python scripts/144_md_repo_current_state_summary.py --introspect-local --db-path /tmp/ci.duckdb
"""
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
STUDIES = ROOT / "studies"
DEFAULT_OUT = STUDIES / "CURRENT_MOTHERDUCK_REPO_STATE.md"
STALE_DAYS = 14


def _git_head() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
            text=True,
        ).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def _rel(p: Path) -> str:
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate CURRENT_MOTHERDUCK_REPO_STATE.md")
    p.add_argument("--md", action="store_true", help="Attach MotherDuck (fail-closed).")
    p.add_argument(
        "--introspect-local",
        action="store_true",
        help="Offline/CI: run live-introspection queries against --db-path (file DuckDB only).",
    )
    p.add_argument("--db-path", default=str(ROOT / "thyroid_master.duckdb"))
    p.add_argument("--output", type=Path, default=DEFAULT_OUT)
    p.add_argument(
        "--stale-days",
        type=int,
        default=STALE_DAYS,
        help="Flag checked-in validation_report.md older than this many days",
    )
    p.add_argument(
        "--md-env",
        default=None,
        help="MotherDuck environment (dev|qa|prod) when using --md; sets MOTHERDUCK_DATABASE if unset.",
    )
    p.add_argument(
        "--also-write",
        type=Path,
        default=None,
        help="Write the same markdown to a second path (e.g. studies/CURRENT_MOTHERDUCK_REPO_STATE.md).",
    )
    return p.parse_args()


def _stale_validation_reports(max_age_days: int) -> list[tuple[str, int]]:
    """Paths of studies/*/validation_report.md older than max_age_days (approximate)."""
    out: list[tuple[str, int]] = []
    now = datetime.now(timezone.utc).timestamp()
    sec = max_age_days * 86400
    for p in sorted(STUDIES.glob("*/validation_report.md")):
        try:
            age = int((now - p.stat().st_mtime) // 86400)
            if now - p.stat().st_mtime > sec:
                out.append((_rel(p), age))
        except OSError:
            continue
    return out[:40]


def collect_catalog_probe(con: Any) -> list[str]:
    """Preflight: current catalog type from md_information_schema (read-only; no DDL)."""
    db = con.execute("SELECT current_database()").fetchone()[0]
    lines: list[str] = [
        "### Catalog probe (read-only)",
        f"- **current_database():** `{db}`",
    ]
    try:
        row = con.execute(
            "SELECT name, type FROM md_information_schema.databases WHERE name = ?",
            [db],
        ).fetchone()
        if row:
            typ = str(row[1]) if row[1] is not None else "NULL"
            ducklake = typ.upper() == "DUCKLAKE"
            lines.append(f"- **md_information_schema.databases.type:** `{typ}`")
            lines.append(
                "- **Named CREATE SNAPSHOT (policy):** "
                + (
                    "DUCKLAKE — do not assume native named snapshot semantics; "
                    "use dev/qa/prepromote-backup runbook patterns."
                    if ducklake
                    else "Non-DUCKLAKE — native named CREATE SNAPSHOT typically available "
                    "(not executed here)."
                )
            )
        else:
            lines.append("- **md_information_schema.databases:** _(no row for current catalog)_")
    except Exception as e:
        lines.append(f"- **catalog probe:** _(error: {e})_")
    return lines


def collect_live_introspection(con: Any) -> tuple[list[str], str, dict[str, Any]]:
    """Run MotherDuck-style bullets for ``## Live MotherDuck status``; works on file DB too.

    Returns ``live_meta`` for comparing live ``qa.release_manifest`` to checked-in JSON.
    """
    md_lines: list[str] = []
    telemetry_note = "(run with `--md` to populate)"
    live_meta: dict[str, Any] = {}
    md_lines.extend(collect_catalog_probe(con))
    md_lines.append("### Core analytic surfaces (row counts)")
    for label, sql in (
        ("canonical_extracted_fact_long_v2", "SELECT COUNT(*) FROM main.canonical_extracted_fact_long_v2"),
        ("canonical_fact_quarantine_v2", "SELECT COUNT(*) FROM main.canonical_fact_quarantine_v2"),
        ("master_fact_long_verified_v1", "SELECT COUNT(*) FROM main.master_fact_long_verified_v1"),
        ("master_patient_rollup_verified_v1", "SELECT COUNT(*) FROM main.master_patient_rollup_verified_v1"),
        ("master_source_lineage_v1", "SELECT COUNT(*) FROM main.master_source_lineage_v1"),
        ("longitudinal_lab_canonical_v1", "SELECT COUNT(*) FROM main.longitudinal_lab_canonical_v1"),
        ("longitudinal_lab_deduped_v", "SELECT COUNT(*) FROM main.longitudinal_lab_deduped_v"),
    ):
        try:
            n = con.execute(sql).fetchone()[0]
            md_lines.append(f"- **{label}:** {int(n):,} rows")
        except Exception as e:
            md_lines.append(f"- **{label}:** _(unavailable: {e})_")
    md_lines.append("### Specimen / FHIR layer row counts")
    for label, sql in (
        ("specimen_master_v1", "SELECT COUNT(*) FROM main.specimen_master_v1"),
        ("specimen_tumor_focus_v1", "SELECT COUNT(*) FROM main.specimen_tumor_focus_v1"),
        ("specimen_genomic_assay_v1", "SELECT COUNT(*) FROM main.specimen_genomic_assay_v1"),
        ("fhir_bundle_specimen_export_v1", "SELECT COUNT(*) FROM main.fhir_bundle_specimen_export_v1"),
    ):
        try:
            n = con.execute(sql).fetchone()[0]
            md_lines.append(f"- **{label}:** {int(n):,} rows")
        except Exception as e:
            md_lines.append(f"- **{label}:** _(unavailable: {e})_")
    try:
        rm_one = con.execute(
            """
            SELECT release_tag, git_sha, created_at FROM qa.release_manifest
            ORDER BY TRY_CAST(release_tag AS BIGINT) DESC NULLS LAST, created_at DESC
            LIMIT 1
            """
        ).fetchone()
        if rm_one:
            live_meta["latest_release_tag"] = rm_one[0]
            live_meta["latest_git_sha"] = rm_one[1]
            live_meta["latest_created_at"] = rm_one[2]
            md_lines.append(
                "- **qa.release_manifest (latest tag; ordering aligned with script 125):** "
                f"`{rm_one[0]}` | sha `{rm_one[1]}` | {rm_one[2]}"
            )
        rm = con.execute(
            "SELECT release_tag, git_sha, created_at FROM qa.release_manifest "
            "ORDER BY created_at DESC NULLS LAST LIMIT 3"
        ).fetchall()
        if rm:
            md_lines.append("- **qa.release_manifest (latest 3 by created_at):**")
            for row in rm:
                md_lines.append(f"  - tag `{row[0]}` | sha `{row[1]}` | {row[2]}")
        elif not rm_one:
            md_lines.append("- **qa.release_manifest:** _(empty)_")
    except Exception as e:
        md_lines.append(f"- **qa.release_manifest:** _(error: {e})_")
        live_meta["release_manifest_error"] = str(e)
    try:
        total_mrq = con.execute("SELECT COUNT(*) FROM qa.manual_review_queue").fetchone()[0]
        pending = con.execute(
            "SELECT COUNT(*) FROM qa.manual_review_queue "
            "WHERE verification_status IS NULL"
        ).fetchone()[0]
        md_lines.append(f"- **qa.manual_review_queue (total rows):** {int(total_mrq):,}")
        md_lines.append(
            f"- **qa.manual_review_queue (pending NULL verification_status):** {int(pending):,}"
        )
        live_meta["mrq_total"] = int(total_mrq)
        live_meta["mrq_pending_null_status"] = int(pending)
    except Exception as e:
        md_lines.append(f"- **manual_review_queue:** _(error: {e})_")
    try:
        hist = con.execute(
            """
            SELECT coalesce(user_agent, ''), COUNT(*) AS n
            FROM md_information_schema.query_history
            WHERE user_agent IN (
              'specimen_fhir_release_truth_v2',
              'specimen_fhir_release_truth_v1',
              'specimen_fhir_release_ops_v1',
              'specimen_fhir_export_restore_v1',
              'specimen_fhir_export_v1',
              'specimen_genomics_binding_v1',
              'specimen_identity_build_v1'
            )
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
            """
        ).fetchall()
        telemetry_note = "| user_agent | approx_queries |\n|---|---:|\n" + "\n".join(
            f"| `{r[0]}` | {r[1]} |" for r in hist
        )
    except Exception as e:
        telemetry_note = f"_(query_history not available: {e})_"
    if "not available" in telemetry_note or telemetry_note.startswith("_("):
        try:
            hist2 = con.execute(
                """
                SELECT coalesce(user_agent, ''), COUNT(*) AS n
                FROM md_information_schema.recent_queries
                WHERE user_agent IN (
                  'specimen_fhir_release_truth_v2',
                  'specimen_fhir_release_truth_v1',
                  'specimen_fhir_release_ops_v1',
                  'specimen_fhir_export_restore_v1',
                  'specimen_fhir_export_v1',
                  'specimen_genomics_binding_v1',
                  'specimen_identity_build_v1'
                )
                GROUP BY 1 ORDER BY 2 DESC LIMIT 15
                """
            ).fetchall()
            telemetry_note = (
                "| user_agent | approx_queries |\n|---|---:|\n"
                + "\n".join(f"| `{r[0]}` | {r[1]} |" for r in hist2)
                + "\n\n_(source: md_information_schema.recent_queries)_"
            )
        except Exception:
            pass
    return md_lines, telemetry_note, live_meta


def build_markdown(
    *,
    now_iso: str,
    sha: str,
    stale_days: int,
    md_lines: list[str] | None,
    telemetry_note: str,
    live_meta: dict[str, Any] | None = None,
) -> str:
    """Assemble full document (static + optional live bullets)."""
    lines: list[str] = [
        "# THYROID_2026 — current MotherDuck vs repo state",
        "",
        "> **Canonical contract:** [`docs/final_source_of_truth_contract.md`](../docs/final_source_of_truth_contract.md) "
        "defines live SSOT (`main` / `qa` on MotherDuck), analyst surfaces, and what is historical only.",
        "",
        "> **Naming:** This file is the default **output path** for this script. It is **not** guaranteed "
        "fresh unless **`Commit SHA`** matches `git rev-parse HEAD` **and** you trust the timestamp.",
        "",
        "> **Live catalog:** MotherDuck **`main`** (analytics) and **`qa`** (governance) — not local `thyroid_master.duckdb` "
        "unless you explicitly reconcile.",
        "",
        "> **Publication narratives:** Signoff context and superseding validation pointers: "
        "[`20260407_publication_signoff_live/README.md`](20260407_publication_signoff_live/README.md).",
        "",
        "> **Not automation SSOT.** Prefer a fresh `119_md_formalization_validate.py --md --release-mode` "
        "output (e.g. under `studies/`) for release verdicts. This file reconciles **checked-in** "
        "repo artifacts with optional live introspection.",
        "",
        "> **Stale guard:** If **`Commit SHA`** below ≠ `git rev-parse HEAD`, treat **Live MotherDuck** "
        "bullets as **point-in-time** until you re-run: `.venv/bin/python scripts/144_md_repo_current_state_summary.py --md` "
        "(RW token via `motherduck_client.get_token()` / `motherduck.local.toml` or env — do not log secrets).",
        "",
        "> **Repo posture (sync with README / `truth_sync_summary.md`):** Technical validation (`119 --release-mode`) "
        "can be green while **governance** (human-reviewed MRQ / promotion where policy requires) remains a separate "
        "concern — do not conflate them. Specimen/FHIR baselines: `studies/specimen_fhir_release_truth_*`. "
        "**Institutional non-Tg lab wave** (`final_institutional_20260407`) is **ingested**; residual lab gaps are "
        "**source-limited**. Evidence pack (may lag live row counts): `studies/20260411_final_master_release/EVIDENCE_PACK.md`.",
        "",
        f"**Machine-generated:** {now_iso}",
        f"**Commit SHA:** `{sha}`",
        "",
        "> Regenerate after promotion or specimen/FHIR deploy: "
        "`python scripts/144_md_repo_current_state_summary.py --md`",
        "",
        "## Read scaling (reviewers)",
        "",
        "For dashboards or ad hoc read-after-snapshot review, use **only** `MD_READ_SCALING_TOKEN` / "
        "`MOTHERDUCK_READ_SCALING_TOKEN` with `MotherDuckClient.connect_read_scaling()` — "
        "never for writes. Set `MD_READ_SCALING_SESSION_HINT` (or per-connection `session_hint`) "
        "for stable duckling affinity; after a writer creates a named snapshot, readers should run "
        "`REFRESH DATABASE` / helpers in `utils/md_read_scaling_refresh.py` (see "
        "[`docs/motherduck_read_scaling_dashboard.md`](../docs/motherduck_read_scaling_dashboard.md)).",
        "",
        "## Live MotherDuck status (`--md` runs only)",
        "",
    ]
    if md_lines:
        lines.extend(md_lines)
    else:
        lines.extend(
            [
                "- _(This run did **not** pass `--md` — no live MotherDuck session.)_",
                "- _(Any **previously committed** row counts in this file are not refreshed here; "
                "operators must not treat them as live.)_",
                "",
            ]
        )
    lines.extend(
        [
            "",
            "## Checked-in release manifest (exports/)",
            "",
        ]
    )
    latest_m = ROOT / "exports" / "release_manifests" / "LATEST_MANIFEST.json"
    if latest_m.exists():
        try:
            m = json.loads(latest_m.read_text(encoding="utf-8"))
            gen_at = m.get("generated_at")
            mtime_age = ""
            try:
                age_sec = max(0.0, datetime.now(timezone.utc).timestamp() - latest_m.stat().st_mtime)
                mtime_age = f" — file mtime ~{int(age_sec // 86400)}d old"
            except OSError:
                pass
            lines.extend(
                [
                    f"- **manifest_id:** `{m.get('manifest_id')}`",
                    f"- **overall_status:** {m.get('overall_status')}",
                    f"- **git_sha (at generation):** `{m.get('git_sha')}`",
                    f"- **generated_at (checked-in):** `{gen_at}`{mtime_age}",
                    f"- **role:** {m.get('role', '_(see exports/release_manifests/README.md)_')}",
                ]
            )
            if live_meta and "latest_release_tag" in live_meta:
                lt = live_meta.get("latest_release_tag")
                ls = live_meta.get("latest_git_sha")
                gt = m.get("git_sha")
                mid = m.get("manifest_id")
                warn_bits: list[str] = []
                if gt and ls and str(gt).strip() != str(ls).strip():
                    warn_bits.append(
                        f"checked-in `git_sha` (`{gt}`) ≠ live latest manifest sha (`{ls}`) — "
                        "**treat checked-in JSON as historical**; live SSOT is `qa.release_manifest`."
                    )
                if lt and mid and str(lt) not in str(mid):
                    warn_bits.append(
                        f"live tag `{lt}` may not match checked-in manifest_id era (`{mid}`) — "
                        "see `exports/release_manifests/README.md`."
                    )
                if warn_bits:
                    lines.append("- **WARNING (historical vs live):**")
                    for w in warn_bits:
                        lines.append(f"  - {w}")
        except Exception as e:
            lines.append(f"- _(could not parse LATEST_MANIFEST.json: {e})_")
    else:
        lines.append("- _(no `exports/release_manifests/LATEST_MANIFEST.json`)_")
    lines.extend(
        [
            "",
            "## Stale checked-in validation artifacts",
            "",
            f"_Validation reports under `studies/` older than **{stale_days}** days "
            "(by local mtime — regenerate with `119_md_formalization_validate.py --md`):_",
            "",
        ]
    )
    stale = _stale_validation_reports(stale_days)
    if stale:
        for path, days in stale:
            lines.append(f"- `{path}` (~{days}d old)")
    else:
        lines.append("- _(none matched staleness rule or no reports found)_")
    lines.extend(
        [
            "",
            "## Specimen / FHIR QA",
            "",
            "- Diagnostic views: `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`",
            "- Deploy-only: `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md`",
            "- Orchestrated build: `scripts/138_md_specimen_fhir_layer.py --md`",
            "",
            "## Query-history telemetry (MotherDuck)",
            "",
            telemetry_note,
            "",
            "## Reviewer RO share (manual)",
            "",
            "Restricted read-only shares are created in the MotherDuck UI/org console. "
            "Attach with org-issue token; document grant + manual refresh policy in your "
            "release ticket — do not commit tokens.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    if args.md and args.introspect_local:
        print("FATAL: pass only one of --md or --introspect-local.")
        raise SystemExit(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    sha = _git_head()

    md_lines: list[str] | None = None
    telemetry_note = "(run with `--md` to populate)"

    live_meta: dict[str, Any] | None = None
    if args.md:
        import os
        import sys

        sys.path.insert(0, str(ROOT))
        from utils.md_connect import connect_md_or_file
        from utils.md_pipeline_attribution import specimen_fhir_release_writer_attribution

        if args.md_env and not os.environ.get("MOTHERDUCK_DATABASE") and not os.environ.get(
            "MOTHERDUCK_DB"
        ):
            from motherduck_client import resolve_database_for_env

            os.environ["MOTHERDUCK_DATABASE"] = resolve_database_for_env(args.md_env)

        ua, hint = specimen_fhir_release_writer_attribution()
        con = connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            custom_user_agent=ua,
            motherduck_session_hint=hint,
            env=args.md_env,
        )
        try:
            md_lines, telemetry_note, live_meta = collect_live_introspection(con)
        finally:
            con.close()
    elif args.introspect_local:
        import duckdb

        con = duckdb.connect(str(args.db_path))
        try:
            md_lines, telemetry_note, live_meta = collect_live_introspection(con)
        finally:
            con.close()

    text = build_markdown(
        now_iso=now,
        sha=sha,
        stale_days=args.stale_days,
        md_lines=md_lines,
        telemetry_note=telemetry_note,
        live_meta=live_meta,
    )
    args.output.write_text(text, encoding="utf-8")
    print(f"Wrote {_rel(args.output)}")
    if args.also_write:
        args.also_write.parent.mkdir(parents=True, exist_ok=True)
        args.also_write.write_text(text, encoding="utf-8")
        print(f"Wrote {_rel(args.also_write)}")


if __name__ == "__main__":
    main()
