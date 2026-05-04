"""mig_302: Refresh views_readable.Patient_Master_Canonical after mig_294b CPM drop.

Repoints the Logan-facing view to SELECT * FROM main.canonical_patient_master so
nlp_tirads_max_category cannot remain in the projection. Verifies tirads_resolved
still exposed and legacy column absent.

Dispatch: cursor_prompts/CURSOR_PROMPT_MIG_302_VIEWS_READABLE_LEGACY_TIRADS_20260504.md
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _md_connect import connect_locked  # noqa: E402

MIG_ID = "mig_302"
ARCHIVE_FQ = '"Thyroid 2026 UPdated".archive_pub_v1_0.view_def_patient_master_canonical_pre_mig302_20260504'
PUB_DB = "thyroid_canonical_publication_v1_0"
EXPECTED_CPM_ROWS = 10871
LOG_PATH = REPO_ROOT / "scripts" / "output" / "mig_302_apply_log.txt"


def lg(lines: list[str], msg: str) -> None:
    lines.append(msg)
    print(msg)


def main() -> int:
    ap = argparse.ArgumentParser(description=MIG_ID)
    ap.add_argument("--dry-run", action="store_true", help="connect + probes only")
    args = ap.parse_args()
    log_lines: list[str] = []
    lg(log_lines, f"{MIG_ID} started at {datetime.now(timezone.utc).isoformat()}")
    if args.dry_run:
        lg(log_lines, "DRY RUN — no DDL")

    con = connect_locked()

    lg(log_lines, "§0 pre: view definition excerpt from duckdb_views()")
    pre = con.execute(
        f"""
SELECT sql
FROM duckdb_views()
WHERE database_name = '{PUB_DB}'
  AND schema_name = 'views_readable'
  AND view_name = 'Patient_Master_Canonical'
        """
    ).fetchone()
    if not pre:
        lg(log_lines, "FAIL: views_readable.Patient_Master_Canonical not found")
        LOG_PATH.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        return 1
    sql_pre = pre[0] or ""
    lg(log_lines, f"  pre_def_len={len(sql_pre)} uses_nlp_tirads_max={('nlp_tirads_max_category' in sql_pre)}")

    # Patient_Master_Canonical may BinderException if stale vs CPM (types drift) — probe base table only.
    cpm_rows = con.execute(
        f"SELECT COUNT(*) FROM {PUB_DB}.main.canonical_patient_master"
    ).fetchone()[0]
    if cpm_rows != EXPECTED_CPM_ROWS:
        lg(log_lines, f"FAIL: CPM row count {cpm_rows} != {EXPECTED_CPM_ROWS}")
        LOG_PATH.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        return 1
    lg(log_lines, f"  canonical_patient_master rows OK ({cpm_rows})")

    view_readable = True
    try:
        vw_rows = con.execute(
            f"SELECT COUNT(*) FROM {PUB_DB}.views_readable.\"Patient_Master_Canonical\""
        ).fetchone()[0]
        lg(log_lines, f"  pre Patient_Master_Canonical COUNT OK ({vw_rows})")
    except Exception as e:
        view_readable = False
        lg(log_lines, f"  pre Patient_Master_Canonical COUNT SKIP (expected if view stale vs CPM): {type(e).__name__}")

    if args.dry_run:
        chk = [r[0] for r in con.execute(
            """
SELECT column_name
FROM duckdb_columns()
WHERE database_name = ?
  AND schema_name = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('nlp_tirads_max_category', 'tirads_resolved')
ORDER BY column_name
            """,
            [PUB_DB],
        ).fetchall()]
        lg(log_lines, f"  CPM tirads-related cols among pair: {chk}")
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOG_PATH.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        lg(log_lines, f"wrote {LOG_PATH.relative_to(REPO_ROOT)}")
        return 0

    lg(log_lines, f"§1 archive snapshot → {ARCHIVE_FQ}")
    con.execute(
        f"""
CREATE OR REPLACE TABLE {ARCHIVE_FQ} AS
SELECT
    CURRENT_TIMESTAMP AS snapshot_at,
    database_name,
    schema_name,
    view_name,
    sql AS view_definition
FROM duckdb_views()
WHERE database_name = '{PUB_DB}'
  AND schema_name = 'views_readable'
  AND view_name = 'Patient_Master_Canonical'
        """
    )

    lg(log_lines, "§2 CREATE OR REPLACE VIEW views_readable.Patient_Master_Canonical")
    con.execute(
        """
CREATE OR REPLACE VIEW views_readable.Patient_Master_Canonical AS
SELECT *
FROM main.canonical_patient_master
        """
    )

    post_rows = con.execute(
        f"SELECT COUNT(*) FROM {PUB_DB}.views_readable.\"Patient_Master_Canonical\""
    ).fetchone()[0]
    if post_rows != EXPECTED_CPM_ROWS:
        lg(log_lines, f"FAIL: post view row count {post_rows} != {EXPECTED_CPM_ROWS}")
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOG_PATH.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        return 1
    if not view_readable:
        lg(log_lines, "  (pre-migration view was unreadable; post replace OK)")

    tirads_chk = con.execute(
        """
SELECT
  MAX(CASE WHEN column_name = 'nlp_tirads_max_category' THEN 1 ELSE 0 END),
  MAX(CASE WHEN column_name = 'tirads_resolved' THEN 1 ELSE 0 END)
FROM duckdb_columns()
WHERE database_name = ?
  AND schema_name = 'views_readable'
  AND table_name = 'Patient_Master_Canonical'
  AND column_name IN ('nlp_tirads_max_category', 'tirads_resolved')
        """,
        [PUB_DB],
    ).fetchone()
    has_legacy, has_resolved = int(tirads_chk[0] or 0), int(tirads_chk[1] or 0)
    lg(log_lines, f"§3 verify Patient_Master_Canonical cols: legacy_nlp_col={has_legacy} tirads_resolved={has_resolved}")
    if has_legacy != 0:
        lg(log_lines, "FAIL: nlp_tirads_max_category still projected on Patient_Master_Canonical")
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOG_PATH.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
        return 1
    if has_resolved != 1:
        lg(log_lines, "WARN: tirads_resolved missing on Patient_Master_Canonical — check main.canonical_patient_master")

    dup = con.execute(
        """
SELECT COUNT(*) FROM main.signoff_migration WHERE mig_id = ?
        """,
        [MIG_ID],
    ).fetchone()[0]
    if dup:
        lg(log_lines, f"§4 skip signoff: {dup} existing signoff_migration row(s) for {MIG_ID}")
    else:
        lg(log_lines, "§4 INSERT main.signoff_migration")
        con.execute(
            """
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  ?,
  CURRENT_TIMESTAMP,
  'cursor_composer_mig302',
  'mig_302: Patient_Master_Canonical SELECT * FROM main.canonical_patient_master; archive '
  || 'view_def_patient_master_canonical_pre_mig302_20260504. Legacy nlp_tirads_max_category '
  || 'projection removed post-mig_294b.'
)
            """,
            [MIG_ID],
        )

    lg(log_lines, f"{MIG_ID} OK")
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    print(f"wrote {LOG_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
