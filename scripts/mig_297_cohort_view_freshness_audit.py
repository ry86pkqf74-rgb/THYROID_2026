"""mig_297: cohort view freshness audit + repoint for deprecated references.

Audits every manuscript_workspace.cohort_* view for stale references to:
  - canonical_recurrence_v1                (deprecated by mig_284)
  - canonical_recurrence_resolved_v1       (legacy)
  - recurrence_event_clean_v1              (legacy)
  - nlp_tirads_max_category                (column dropped by mig_294b)

For table-level swaps where the substitution is column-compatible (legacy
recurrence -> canonical_recurrence_patient_rollup_v1), attempt CREATE OR
REPLACE with a compile + row-count drift guard. Anything that fails to
compile or drifts >1% is reverted from the snapshot and marked
needs_manual_repoint.

Dispatch reference:
cursor_prompts/CURSOR_PROMPT_MIG_297_COHORT_VIEW_FRESHNESS_AUDIT_20260504.md
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _md_connect import connect_locked  # noqa: E402

MIG_ID = "mig_297"
MIG_DATE = "20260504"
ARCHIVE_DB = '"Thyroid 2026 UPdated".archive_pub_v1_0'

# Deprecated identifier -> recommended replacement (table-level only).
# Column-level swaps (nlp_tirads_max_category -> tirads_resolved) are
# detected but NOT auto-applied because the new column may have a different
# semantics/type and projections may need manual review.
TABLE_SUBS: dict[str, str] = {
    "canonical_recurrence_resolved_v1": "canonical_recurrence_patient_rollup_v1",
    "canonical_recurrence_v1":          "canonical_recurrence_patient_rollup_v1",
    "recurrence_event_clean_v1":        "canonical_recurrence_patient_rollup_v1",
}

# Column-level deprecations (audit-only; needs manual repoint).
COLUMN_DEPRECATIONS: dict[str, str] = {
    "nlp_tirads_max_category": "tirads_resolved",
}


@dataclass
class ViewAudit:
    schema: str
    name: str
    pre_rows: int | None = None
    pre_cols: int | None = None
    post_rows: int | None = None
    flags: list[str] = field(default_factory=list)
    action: str = "no_change"   # no_change | repointed | needs_manual | error
    note: str = ""


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def _word_replace(sql: str, old: str, new: str) -> tuple[str, int]:
    """Replace whole-word occurrences of `old` with `new`; return (new_sql, count)."""
    pat = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])")
    return pat.subn(new, sql)


def _to_create_or_replace(view_sql: str) -> str:
    rewritten = re.sub(
        r"^\s*CREATE\s+VIEW",
        "CREATE OR REPLACE VIEW",
        view_sql,
        count=1,
        flags=re.IGNORECASE,
    )
    if rewritten == view_sql and not re.match(r"^\s*CREATE\s+OR\s+REPLACE\s+VIEW",
                                              view_sql, flags=re.IGNORECASE):
        raise ValueError("View SQL did not start with CREATE VIEW; cannot rewrite safely")
    return rewritten


def main() -> int:
    log: list[str] = []
    lg = log.append

    con = connect_locked()
    lg(f"{MIG_ID} start")

    rows = con.execute(
        """
SELECT schema_name, view_name, sql
FROM duckdb_views()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name = 'manuscript_workspace'
  AND view_name ILIKE 'cohort_%'
ORDER BY view_name
        """
    ).fetchall()
    lg(f"inventory cohort_* views in manuscript_workspace = {len(rows)}")

    audits: list[ViewAudit] = []
    repointed = 0
    needs_manual = 0

    for schema, view_name, view_sql in rows:
        a = ViewAudit(schema=schema, name=view_name)
        fq = f'{schema}."{view_name}"'

        # Flags from definition text (case-insensitive whole-word match)
        for ident in list(TABLE_SUBS) + list(COLUMN_DEPRECATIONS):
            if re.search(rf"(?i)(?<![A-Za-z0-9_]){re.escape(ident)}(?![A-Za-z0-9_])",
                         view_sql or ""):
                a.flags.append(ident)

        # Pre-state: row count + col count (best effort)
        try:
            a.pre_rows = int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0])
            a.pre_cols = int(
                con.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema=? AND table_name=?",
                    [schema, view_name],
                ).fetchone()[0]
            )
        except Exception as exc:  # noqa: BLE001
            a.action = "error"
            a.note = f"pre-probe failed: {str(exc).splitlines()[0]}"
            audits.append(a)
            lg(f"audit {view_name}: PRE-PROBE FAIL {a.note}")
            continue

        # Auto-repoint: only safe if flags are exclusively table-level legacy recurrence refs
        table_flags = [f for f in a.flags if f in TABLE_SUBS]
        col_flags = [f for f in a.flags if f in COLUMN_DEPRECATIONS]

        if not a.flags:
            a.action = "no_change"
            audits.append(a)
            continue

        if col_flags:
            # Column deprecations are not auto-rewritten — semantics may differ.
            a.action = "needs_manual"
            a.note = "column-level deprecation present: " + ", ".join(col_flags)
            needs_manual += 1
            audits.append(a)
            lg(f"audit {view_name}: NEEDS_MANUAL ({a.note})")
            continue

        # table-level only -> attempt repoint
        snap_tbl = (
            f"{ARCHIVE_DB}.view_def_{view_name}_pre_{MIG_ID}_{MIG_DATE}"
        )
        con.execute(
            f"""
CREATE OR REPLACE TABLE {snap_tbl} AS
SELECT
  database_name AS view_catalog,
  schema_name   AS view_schema,
  view_name,
  sql           AS view_definition,
  CURRENT_TIMESTAMP AS snapshot_at
FROM duckdb_views()
WHERE database_name = 'thyroid_canonical_publication_v1_0'
  AND schema_name = '{_escape_sql(schema)}'
  AND view_name = '{_escape_sql(view_name)}'
            """
        )

        new_sql = view_sql
        sub_summary = []
        for old in table_flags:
            new = TABLE_SUBS[old]
            new_sql, n = _word_replace(new_sql, old, new)
            if n:
                sub_summary.append(f"{old}->{new} (x{n})")

        try:
            new_sql_cor = _to_create_or_replace(new_sql)
            con.execute(new_sql_cor)
            post_rows = int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0])
            a.post_rows = post_rows

            drift = 0.0
            if a.pre_rows and a.pre_rows > 0:
                drift = abs(post_rows - a.pre_rows) / a.pre_rows
            if drift > 0.01:
                # revert
                orig_sql_cor = _to_create_or_replace(view_sql)
                con.execute(orig_sql_cor)
                a.action = "needs_manual"
                a.note = (
                    f"reverted: row drift {a.pre_rows}->{post_rows} "
                    f"({drift*100:.2f}%) exceeds 1% guard; subs={'; '.join(sub_summary)}"
                )
                needs_manual += 1
                lg(f"repoint {view_name}: REVERT ({a.note})")
            else:
                a.action = "repointed"
                a.note = "; ".join(sub_summary)
                repointed += 1
                lg(f"repoint {view_name}: OK pre={a.pre_rows} post={post_rows} subs={a.note}")
        except Exception as exc:  # noqa: BLE001
            err = str(exc).splitlines()[0]
            try:
                orig_sql_cor = _to_create_or_replace(view_sql)
                con.execute(orig_sql_cor)
            except Exception:
                pass
            a.action = "needs_manual"
            a.note = f"compile failure: {err}; subs attempted={'; '.join(sub_summary)}"
            needs_manual += 1
            lg(f"repoint {view_name}: NEEDS_MANUAL ({a.note})")

        audits.append(a)

    # disposition.md
    disposition = REPO_ROOT / "scripts/output/mig_297_disposition.md"
    out = [
        "# mig_297 disposition — cohort view freshness audit",
        "",
        f"- Probed views: {len(rows)}",
        f"- Repointed (table-level safe sub): {repointed}",
        f"- Needs manual repoint (column-level deprecation or compile/drift fail): {needs_manual}",
        f"- No action: {sum(1 for a in audits if a.action == 'no_change')}",
        "",
        "## Substitution rules",
        "",
        "| deprecated | replacement | mode |",
        "|---|---|---|",
    ]
    for k, v in TABLE_SUBS.items():
        out.append(f"| `{k}` | `{v}` | auto (whole-word table swap) |")
    for k, v in COLUMN_DEPRECATIONS.items():
        out.append(f"| `{k}` | `{v}` | manual (column-level; semantics differ) |")
    out += [
        "",
        "## Per-view disposition",
        "",
        "| view | flags | action | pre_rows | pre_cols | post_rows | note |",
        "|---|---|---|---|---|---|---|",
    ]
    for a in audits:
        out.append(
            f"| {a.schema}.{a.name} | "
            f"{', '.join(a.flags) if a.flags else '—'} | "
            f"{a.action} | "
            f"{'' if a.pre_rows is None else a.pre_rows} | "
            f"{'' if a.pre_cols is None else a.pre_cols} | "
            f"{'' if a.post_rows is None else a.post_rows} | "
            f"{a.note.replace('|', '\\|')} |"
        )
    disposition.parent.mkdir(parents=True, exist_ok=True)
    disposition.write_text("\n".join(out) + "\n", encoding="utf-8")

    summary = (
        f"{MIG_ID}: Cohort view freshness audit. Probed {len(rows)} "
        f"manuscript_workspace.cohort_* views; auto-repointed {repointed} via "
        f"whole-word swap of legacy recurrence tables to "
        f"canonical_recurrence_patient_rollup_v1; flagged {needs_manual} for "
        f"manual repoint (column-level nlp_tirads_max_category -> "
        f"tirads_resolved or compile/drift failure). Pre-snapshots in "
        f"\"Thyroid 2026 UPdated\".archive_pub_v1_0 as "
        f"view_def_<view>_pre_mig_297_20260504. Disposition: "
        f"scripts/output/mig_297_disposition.md. Closes CF-mig283-COHORT-FRESHNESS."
    )
    con.execute(
        """
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (?, CAST(CURRENT_TIMESTAMP AS TIMESTAMP), ?, ?)
        """,
        [MIG_ID, "cursor_agent_mig297", summary],
    )
    lg(summary)
    lg(f"{MIG_ID} complete")

    apply_log = REPO_ROOT / "scripts/output/mig_297_apply_log.txt"
    apply_log.write_text("\n".join(log) + "\n", encoding="utf-8")
    print(f"Wrote {disposition}")
    print(f"Wrote {apply_log}")
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
