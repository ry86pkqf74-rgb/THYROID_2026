#!/usr/bin/env python3
"""Script 271a — Fix pathology_vs_imaging_laterality_concordant to 3-valued
(TRUE / FALSE / NULL) and audit true discordance among the both-populated subset.

Background
----------
Script 271 Step 6 created the column as 2-valued (TRUE / FALSE), so 7,507
patients with NULL on either side were labeled FALSE. That mis-bins
"missing data" as "discordant" for any analyst running
``WHERE pathology_vs_imaging_laterality_concordant = FALSE``. This patch
makes the column 3-valued and audits the real discordance pattern among the
~3,364 patients with both ``cpm.laterality`` and ``imaging_laterality_rollup``
populated.

Run sequence (single execution):
  1. Snapshot canonical_patient_master to archive_pub_v1_0.
  2. UPDATE the column to NULL where either input is NULL.
  3. Update the column COMMENT.
  4. Print + persist the crosstab + summary to
     ``scripts/output/271a_laterality_audit.md``.
  5. Re-verify CPM invariants (10871 / 10871 / 0).

Idempotent: re-running just refreshes the audit; the UPDATE is naturally
idempotent (TRUE/FALSE rows where both sides remain non-NULL are untouched;
NULL-on-either-side rows are set to NULL again).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_PREFIX = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}'

ISO_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
SCRIPT_TAG = "script271a_2026-04-18"

LOG_PATH = OUT_DIR / "271a_run.log"
AUDIT_MD = OUT_DIR / "271a_laterality_audit.md"
AUDIT_JSON = OUT_DIR / "271a_audit.json"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as fh:
        fh.write(line + "\n")


def assert_invariants(con) -> None:
    n, d, nulls = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "COUNT(*) FILTER (WHERE research_id IS NULL) "
        f"FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()
    log(f"INVARIANTS canonical_patient_master: n={n} distinct={d} nulls={nulls}")
    if (n, d, nulls) != (10871, 10871, 0):
        raise SystemExit(f"INVARIANT VIOLATION: ({n},{d},{nulls}) != (10871,10871,0)")


def snapshot_cpm(con) -> str:
    snap_name = f"canonical_patient_master_pre271a_{ISO_TS}"
    snap_fq = f'{ARCHIVE_PREFIX}."{snap_name}"'
    log(f"Snapshotting canonical_patient_master -> {snap_fq}")
    exists = con.execute(
        f'SELECT COUNT(*) FROM "{ARCHIVE_DB}".information_schema.tables '
        f"WHERE table_schema='{ARCHIVE_SCHEMA}' AND table_name='{snap_name}'"
    ).fetchone()[0]
    if exists:
        log(f"  SKIP — already exists: {snap_fq}")
    else:
        con.execute(
            f"CREATE TABLE {snap_fq} AS "
            f"SELECT * FROM {PUBLICATION_DB}.main.canonical_patient_master"
        )
    src_n = con.execute(
        f"SELECT COUNT(*) FROM {PUBLICATION_DB}.main.canonical_patient_master"
    ).fetchone()[0]
    snap_n = con.execute(f"SELECT COUNT(*) FROM {snap_fq}").fetchone()[0]
    log(f"  src={src_n} snap={snap_n} match={src_n == snap_n}")
    if src_n != snap_n:
        raise SystemExit(f"Snapshot row mismatch: {src_n} vs {snap_n}")
    return snap_name


def make_three_valued(con) -> dict:
    log("Pre-state distribution (BEFORE UPDATE):")
    pre = con.execute(
        "SELECT pathology_vs_imaging_laterality_concordant, COUNT(*) "
        "FROM canonical_patient_master "
        "GROUP BY 1 ORDER BY 1 NULLS LAST"
    ).fetchall()
    pre_map = {str(r[0]): r[1] for r in pre}
    for r in pre:
        log(f"  {r[0]}: {r[1]}")

    log("Setting concordance = NULL where either input is NULL ...")
    con.execute(
        "UPDATE canonical_patient_master "
        "SET pathology_vs_imaging_laterality_concordant = NULL "
        "WHERE laterality IS NULL OR imaging_laterality_rollup IS NULL"
    )

    log("Post-state distribution (AFTER UPDATE):")
    post = con.execute(
        "SELECT pathology_vs_imaging_laterality_concordant, COUNT(*) "
        "FROM canonical_patient_master "
        "GROUP BY 1 ORDER BY 1 NULLS LAST"
    ).fetchall()
    post_map = {str(r[0]): r[1] for r in post}
    for r in post:
        log(f"  {r[0]}: {r[1]}")

    new_comment = (
        "TRUE iff both cpm.laterality and imaging_laterality_rollup are populated "
        "AND match. FALSE iff both populated AND differ. NULL iff either is missing. "
        "Script 271a, 2026-04-18."
    )
    safe = new_comment.replace("'", "''")
    con.execute(
        "COMMENT ON COLUMN canonical_patient_master."
        f"pathology_vs_imaging_laterality_concordant IS '{safe}'"
    )
    log("COMMENT updated.")

    return {"pre": pre_map, "post": post_map, "comment": new_comment}


def audit_crosstab(con) -> dict:
    log("Building crosstab cpm.laterality × imaging_laterality_rollup ...")

    both_populated_n = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master "
        "WHERE laterality IS NOT NULL AND imaging_laterality_rollup IS NOT NULL"
    ).fetchone()[0]
    log(f"  patients with both populated: {both_populated_n}")

    raw_lat_distinct = con.execute(
        "SELECT laterality, COUNT(*) "
        "FROM canonical_patient_master "
        "WHERE laterality IS NOT NULL "
        "GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    rollup_distinct = con.execute(
        "SELECT imaging_laterality_rollup, COUNT(*) "
        "FROM canonical_patient_master "
        "WHERE imaging_laterality_rollup IS NOT NULL "
        "GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()

    crosstab = con.execute(
        "SELECT laterality, imaging_laterality_rollup, COUNT(*) "
        "FROM canonical_patient_master "
        "WHERE laterality IS NOT NULL AND imaging_laterality_rollup IS NOT NULL "
        "GROUP BY 1, 2 ORDER BY 3 DESC"
    ).fetchall()

    normalized = con.execute(
        "SELECT LOWER(TRIM(CAST(laterality AS VARCHAR))) AS path_lat_norm, "
        "       imaging_laterality_rollup, COUNT(*) "
        "FROM canonical_patient_master "
        "WHERE laterality IS NOT NULL AND imaging_laterality_rollup IS NOT NULL "
        "GROUP BY 1, 2 ORDER BY 3 DESC"
    ).fetchall()

    concordance_post = con.execute(
        "SELECT pathology_vs_imaging_laterality_concordant, COUNT(*) "
        "FROM canonical_patient_master "
        "WHERE laterality IS NOT NULL AND imaging_laterality_rollup IS NOT NULL "
        "GROUP BY 1 ORDER BY 1 NULLS LAST"
    ).fetchall()

    return {
        "both_populated_n": both_populated_n,
        "raw_pathology_laterality_distinct": [
            {"value": r[0], "n": r[1]} for r in raw_lat_distinct
        ],
        "imaging_rollup_distinct": [
            {"value": r[0], "n": r[1]} for r in rollup_distinct
        ],
        "crosstab_raw": [
            {"path_lat": r[0], "img_rollup": r[1], "n": r[2]} for r in crosstab
        ],
        "crosstab_normalized": [
            {"path_lat_norm": r[0], "img_rollup": r[1], "n": r[2]} for r in normalized
        ],
        "concordance_among_both_populated": [
            {"value": r[0], "n": r[1]} for r in concordance_post
        ],
    }


def write_audit_md(audit: dict, transition: dict, snap_name: str) -> None:
    lines: list[str] = []
    lines.append("# Script 271a — Laterality Concordance Audit")
    lines.append("")
    lines.append(f"**Run:** {NOW}")
    lines.append(f"**Snapshot:** `{ARCHIVE_PREFIX}.\"{snap_name}\"`")
    lines.append("")
    lines.append("## Column transition (pathology_vs_imaging_laterality_concordant)")
    lines.append("")
    lines.append("| Value | Pre-271a | Post-271a |")
    lines.append("|---|---:|---:|")
    keys = sorted(set(transition["pre"].keys()) | set(transition["post"].keys()))
    for k in keys:
        lines.append(
            f"| {k} | {transition['pre'].get(k, 0)} | {transition['post'].get(k, 0)} |"
        )
    lines.append("")
    lines.append(
        "FALSE rows pre-271a included 7,507 patients whose FALSE meant "
        "\"missing data on at least one side\" rather than real disagreement. "
        "Post-271a, those rows are NULL."
    )
    lines.append("")

    bp = audit["both_populated_n"]
    cmap = {str(r["value"]): r["n"] for r in audit["concordance_among_both_populated"]}
    t_n = cmap.get("True", 0)
    f_n = cmap.get("False", 0)
    null_n = cmap.get("None", 0)
    pct_disc = (f_n / bp * 100.0) if bp else 0.0
    lines.append("## Real discordance among both-populated subset")
    lines.append("")
    lines.append(f"- patients with both `cpm.laterality` and `imaging_laterality_rollup` populated: **{bp}**")
    lines.append(f"  - concordant (TRUE): **{t_n}**")
    lines.append(f"  - discordant (FALSE): **{f_n}**  ({pct_disc:.1f}%)")
    if null_n:
        lines.append(f"  - NULL despite both populated (sanity violation): {null_n}")
    lines.append("")
    lines.append(
        "Coworker (PROMPT 19) estimated ~1,903 discordances. The realised count "
        f"is {f_n} (≈{pct_disc:.1f}% of the both-populated subset)."
    )
    lines.append("")

    lines.append("## Distinct values present")
    lines.append("")
    lines.append("### `cpm.laterality` (raw, restricted to non-NULL)")
    lines.append("")
    lines.append("| Value | n |")
    lines.append("|---|---:|")
    for r in audit["raw_pathology_laterality_distinct"]:
        lines.append(f"| `{r['value']}` | {r['n']} |")
    lines.append("")
    lines.append("### `imaging_laterality_rollup` (non-NULL)")
    lines.append("")
    lines.append("| Value | n |")
    lines.append("|---|---:|")
    for r in audit["imaging_rollup_distinct"]:
        lines.append(f"| `{r['value']}` | {r['n']} |")
    lines.append("")

    lines.append("## Crosstab (raw)")
    lines.append("")
    lines.append(
        "Each row is a (cpm.laterality, imaging_laterality_rollup) pair counted "
        "across patients where both inputs are non-NULL."
    )
    lines.append("")
    lines.append("| cpm.laterality (raw) | imaging_laterality_rollup | n | apparent match? |")
    lines.append("|---|---|---:|:---:|")
    for r in audit["crosstab_raw"]:
        path_norm = (r["path_lat"] or "").strip().lower()
        match = "✓" if path_norm == (r["img_rollup"] or "") else ""
        lines.append(
            f"| `{r['path_lat']}` | `{r['img_rollup']}` | {r['n']} | {match} |"
        )
    lines.append("")

    lines.append("## Crosstab (normalized: lower(trim(path_lat)))")
    lines.append("")
    lines.append("| path_lat_norm | imaging_laterality_rollup | n | match? |")
    lines.append("|---|---|---:|:---:|")
    for r in audit["crosstab_normalized"]:
        match = "✓" if (r["path_lat_norm"] or "") == (r["img_rollup"] or "") else ""
        lines.append(
            f"| `{r['path_lat_norm']}` | `{r['img_rollup']}` | {r['n']} | {match} |"
        )
    lines.append("")

    lines.append("## Notes for the next operator")
    lines.append("")
    lines.append(
        "- The Step 6 logic compares strict-string-equality after "
        "`LOWER(TRIM())` of `cpm.laterality` against the imaging rollup. If the "
        "pathology vocabulary uses `'isthmus_left'`, `'left_isthmus'`, `'lt'`, "
        "etc., those would be flagged as discordant against `'left'` even when "
        "they semantically match. Inspect the **Crosstab (normalized)** rows "
        "marked with no ✓ for cases that should arguably be concordant."
    )
    lines.append(
        "- The Step 6 derivation rule only emits `'mixed'` when both left+right "
        "AND isthmus appear in inm_v1; pure `'left+isthmus'` → `'left'`. "
        "Pathology may report this as a single-side or mixed value, "
        "so inspect any `path=mixed` × `img=left/right` cell."
    )
    lines.append(
        "- If the audit reveals systematic vocabulary collapsing, do NOT patch "
        "the concordance flag in place; instead introduce a "
        "`*_normalized` derivation in cunc_v1/inm_v1 and rebuild the rollup."
    )

    AUDIT_MD.write_text("\n".join(lines) + "\n")
    log(f"Wrote {AUDIT_MD}")


def main() -> int:
    log("########## Script 271a START ##########")
    con = connect_locked()
    try:
        snap_name = snapshot_cpm(con)
        transition = make_three_valued(con)
        audit = audit_crosstab(con)
        write_audit_md(audit, transition, snap_name)
        with AUDIT_JSON.open("w") as fh:
            json.dump(
                {"transition": transition, "audit": audit, "snapshot": snap_name},
                fh, indent=2, default=str,
            )
        log(f"Wrote {AUDIT_JSON}")
        assert_invariants(con)
    finally:
        con.close()
    log("########## Script 271a DONE ##########")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
