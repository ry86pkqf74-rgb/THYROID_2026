#!/usr/bin/env python3
"""Write studies/CURRENT_MOTHERDUCK_REPO_STATE.md — live DB + repo artifact reconciliation.

Read-only against MotherDuck unless views are deployed (no DDL). Uses custom_user_agent
``specimen_fhir_release_ops_v1`` on ``--md`` for query-history attribution (not a read-scaling token).

Usage:
  .venv/bin/python scripts/144_md_repo_current_state_summary.py
  .venv/bin/python scripts/144_md_repo_current_state_summary.py --md
  .venv/bin/python scripts/144_md_repo_current_state_summary.py --md --output studies/CURRENT_MOTHERDUCK_REPO_STATE.md
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STUDIES = ROOT / "studies"
DEFAULT_OUT = STUDIES / "CURRENT_MOTHERDUCK_REPO_STATE.md"
UA = "specimen_fhir_release_ops_v1"
STALE_DAYS = 14


def _git_head() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
            text=True,
        ).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def _git_short() -> str:
    h = _git_head()
    return h[:12] if len(h) >= 12 else h


def _rel(p: Path) -> str:
    try:
        return str(p.relative_to(ROOT))
    except ValueError:
        return str(p)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate CURRENT_MOTHERDUCK_REPO_STATE.md")
    p.add_argument("--md", action="store_true", help="Attach MotherDuck (fail-closed).")
    p.add_argument("--db-path", default=str(ROOT / "thyroid_master.duckdb"))
    p.add_argument("--output", type=Path, default=DEFAULT_OUT)
    p.add_argument(
        "--stale-days",
        type=int,
        default=STALE_DAYS,
        help="Flag checked-in validation_report.md older than this many days",
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


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    sha = _git_head()
    short = _git_short()

    lines: list[str] = [
        "# THYROID_2026 — current MotherDuck vs repo state",
        "",
        "> **Not automation SSOT.** Prefer a fresh `119_md_formalization_validate.py --md --release-mode` "
        "output (e.g. under `studies/`) for release verdicts. This file reconciles **checked-in** "
        "repo artifacts with optional live introspection.",
        "",
        "> **Stale guard:** If **`Commit SHA`** below ≠ `git rev-parse HEAD` on your machine, treat "
        "**Live MotherDuck** bullets as **historical** until you re-run this generator with `--md`.",
        "",
        f"**Machine-generated:** {now}",
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

    md_lines: list[str] = []
    telemetry_note = "(run with `--md` to populate)"
    if args.md:
        import sys

        sys.path.insert(0, str(ROOT))
        from utils.md_connect import connect_md_or_file

        hint = (os.environ.get("MOTHERDUCK_SESSION_HINT") or "").strip() or f"thyroid2026:current_state:{short}"
        ua = (os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT") or "").strip() or UA
        con = connect_md_or_file(
            Path(args.db_path),
            md=True,
            fail_closed=True,
            custom_user_agent=ua,
            motherduck_session_hint=hint,
        )
        try:
            db = con.execute("SELECT current_database()").fetchone()[0]
            md_lines.append(f"- **current_database():** `{db}`")
            for label, sql in (
                ("specimen_master_v1", "SELECT COUNT(*) FROM main.specimen_master_v1"),
                ("fhir_bundle_export", "SELECT COUNT(*) FROM main.fhir_bundle_specimen_export_v1"),
                ("specimen_genomic_assay_v1", "SELECT COUNT(*) FROM main.specimen_genomic_assay_v1"),
            ):
                try:
                    n = con.execute(sql).fetchone()[0]
                    md_lines.append(f"- **{label}:** {int(n):,} rows")
                except Exception as e:
                    md_lines.append(f"- **{label}:** _(unavailable: {e})_")
            try:
                rm = con.execute(
                    "SELECT release_tag, git_sha, created_at FROM qa.release_manifest "
                    "ORDER BY created_at DESC NULLS LAST LIMIT 3"
                ).fetchall()
                if rm:
                    md_lines.append("- **qa.release_manifest (latest 3):**")
                    for row in rm:
                        md_lines.append(f"  - tag `{row[0]}` | sha `{row[1]}` | {row[2]}")
                else:
                    md_lines.append("- **qa.release_manifest:** _(empty)_")
            except Exception as e:
                md_lines.append(f"- **qa.release_manifest:** _(error: {e})_")
            try:
                pending = con.execute(
                    "SELECT COUNT(*) FROM qa.manual_review_queue "
                    "WHERE verification_status IS NULL"
                ).fetchone()[0]
                md_lines.append(
                    f"- **qa.manual_review_queue (NULL verification_status):** {int(pending):,}"
                )
            except Exception as e:
                md_lines.append(f"- **manual_review_queue pending:** _(error: {e})_")
            try:
                hist = con.execute(
                    """
                    SELECT coalesce(user_agent, ''), COUNT(*) AS n
                    FROM md_information_schema.query_history
                    WHERE user_agent IN (
                      'specimen_fhir_release_ops_v1',
                      'specimen_fhir_export_v1',
                      'specimen_genomics_binding_v1'
                    )
                    GROUP BY 1 ORDER BY 2 DESC LIMIT 15
                    """
                ).fetchall()
                telemetry_note = "| user_agent | approx_queries |\n|---|---:|\n" + "\n".join(
                    f"| `{r[0]}` | {r[1]} |" for r in hist
                )
            except Exception as e:
                telemetry_note = f"_(query_history not available: {e})_"
        finally:
            con.close()
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
            lines.extend(
                [
                    f"- **manifest_id:** `{m.get('manifest_id')}`",
                    f"- **overall_status:** {m.get('overall_status')}",
                    f"- **git_sha (at generation):** `{m.get('git_sha')}`",
                ]
            )
        except Exception as e:
            lines.append(f"- _(could not parse LATEST_MANIFEST.json: {e})_")
    else:
        lines.append("- _(no `exports/release_manifests/LATEST_MANIFEST.json`)_")
    lines.extend(
        [
            "",
            "## Stale checked-in validation artifacts",
            "",
            f"_Validation reports under `studies/` older than **{args.stale_days}** days "
            "(by local mtime — regenerate with `119_md_formalization_validate.py --md`):_",
            "",
        ]
    )
    stale = _stale_validation_reports(args.stale_days)
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
    args.output.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {_rel(args.output)}")


if __name__ == "__main__":
    main()
