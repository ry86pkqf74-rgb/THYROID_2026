"""Phase 3 (audit-only stage) — inventory + classify every object in main.

For every BASE TABLE and VIEW in `main`, compute:
  - row_count
  - n_distinct_research_id (if research_id column exists)
  - is_empty
  - has_version_twin              (peer with same stem and different _vN suffix)
  - twin_is_higher_version
  - is_referenced_by_view         (parse view_definition across main + manuscript_workspace)
  - is_referenced_by_script       (grep repo/**/*.py for bare table name AND for `main.<table>`
                                   AND for `"<table>"` AND `'<table>'`)
  - is_identical_to_twin          (when applicable; EXCEPT both ways on COMMON cols)

Apply classifier rules in order, first match wins:
  DELETE    if is_empty AND NOT referenced (view OR script)
  DELETE    if has_version_twin AND is_identical_to_twin AND twin_higher
            AND NOT referenced
  DEPRECATE if has_version_twin AND twin_higher AND NOT identical
            AND NOT referenced
  ARCHIVE   if row_count > 0 AND NOT referenced AND name CONTAINS
            ('backup','snapshot','pre_','prev_')
  KEEP_REVIEW if classified would-be DELETE/DEPRECATE/ARCHIVE but the only
            references are from snapshot/deprecated objects (soft refs)
  LIVE      otherwise

This script DOES NOT execute any DDL/DML. It produces:
  - studies/canonical_cleanup_20260417_resume/phase3_object_signals.json
  - studies/canonical_cleanup_20260417_resume/phase3_proposed_actions.csv
  - studies/canonical_cleanup_20260417_resume/phase3_preview.md
  - studies/canonical_cleanup_20260417_resume/phase3_run.log

The execution stage (DEPRECATE → ARCHIVE → cross-DB move → DELETE)
is in a separate script that requires Logan's go-ahead AND obeys the
"stop if >10 DELETEs or >5 ARCHIVEs" gate.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG_PATH = HERE / "phase3_run.log"
SIGNALS_PATH = HERE / "phase3_object_signals.json"
ACTIONS_CSV = HERE / "phase3_proposed_actions.csv"
PREVIEW_MD = HERE / "phase3_preview.md"

ARCHIVE_NAME_TOKENS = ("backup", "snapshot", "pre_", "prev_")

# Skip ONLY non-source-control directories. Per Logan's spec: grep
# repo/**/*.py for the bare name + main.<name> + quoted forms — no
# directory exclusions beyond build/cache artifacts.
SKIP_DIR_NAMES = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
}


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def parse_version_suffix(name: str) -> tuple[str | None, int | None]:
    """Return (stem, version) for names ending in `_vN` or `_vNa/_vNb`.

    Examples:
      data_dictionary_v266a -> ('data_dictionary', 266)
      tumor_episode_master_v2 -> ('tumor_episode_master', 2)
      canonical_patient_master -> (None, None)
    """
    m = re.match(r"^(.+)_v(\d+)[a-z]?$", name)
    if not m:
        return None, None
    return m.group(1), int(m.group(2))


def collect_repo_py_text() -> dict[str, str]:
    """Return {relpath: contents} for all .py files under REPO, skipping
    big/irrelevant trees."""
    out: dict[str, str] = {}
    skip_set = SKIP_DIR_NAMES
    for path in REPO.rglob("*.py"):
        rel = path.relative_to(REPO)
        # Skip if any path component is in skip_set
        if any(part in skip_set for part in rel.parts):
            continue
        try:
            out[str(rel)] = path.read_text(errors="ignore")
        except Exception:
            continue
    return out


def main() -> int:
    LOG_PATH.write_text("")
    con = connect_locked()
    log("Phase 3 audit-only inventory starting...")

    # ---------- 1. Pull main objects ----------
    main_objs = con.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='main' ORDER BY 2, 1"
    ).fetchall()
    log(f"  main objects in scope: {len(main_objs)}")

    # ---------- 2. Pull all view definitions (main + manuscript_workspace) ----------
    view_defs = con.execute(
        "SELECT table_schema, table_name, view_definition "
        "FROM information_schema.views "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema IN ('main','manuscript_workspace')"
    ).fetchall()
    log(f"  view definitions loaded: {len(view_defs)}")
    # Also include manuscript_workspace BASE TABLES that hold snapshots of view defs
    snap_table_present = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='manuscript_workspace' "
        "AND table_name='view_definitions_snapshot_bigcleanup'"
    ).fetchone()[0]
    log(f"  view_definitions_snapshot_bigcleanup present: {bool(snap_table_present)}")

    # ---------- 3. Pull columns for each main object (for research_id detection
    # and identical-twin column overlap) ----------
    cols_by_obj: dict[str, list[str]] = defaultdict(list)
    for r in con.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='main' ORDER BY table_name, ordinal_position"
    ).fetchall():
        cols_by_obj[r[0]].append(r[1])

    # ---------- 4. Read all repo python files once ----------
    log("  reading repo python files for script-ref scan...")
    py_texts = collect_repo_py_text()
    log(f"    {len(py_texts)} .py files loaded for scanning")

    # ---------- 5. Twin map (stem -> [(name, version)]) ----------
    object_names = [t[0] for t in main_objs]
    name_set = set(object_names)
    stem_map: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for n in object_names:
        stem, ver = parse_version_suffix(n)
        if stem is not None:
            stem_map[stem].append((n, ver))

    # ---------- 6. For each object: signals ----------
    log("  computing per-object signals...")
    signals: dict[str, dict] = {}
    for name, ttype in main_objs:
        rec: dict = {
            "object_name": name,
            "object_type": ttype,
            "n_columns": len(cols_by_obj.get(name, [])),
        }
        # row_count
        try:
            rec["row_count"] = con.execute(
                f'SELECT COUNT(*) FROM main."{name}"'
            ).fetchone()[0]
        except Exception as e:
            rec["row_count"] = -1
            rec["row_count_error"] = str(e)[:200]

        # n_distinct_research_id
        if "research_id" in cols_by_obj.get(name, []):
            try:
                rec["n_distinct_research_id"] = con.execute(
                    f'SELECT COUNT(DISTINCT research_id) FROM main."{name}"'
                ).fetchone()[0]
            except Exception as e:
                rec["n_distinct_research_id"] = None
                rec["n_distinct_research_id_error"] = str(e)[:200]
        else:
            rec["n_distinct_research_id"] = None

        rec["is_empty"] = rec["row_count"] == 0

        # ---- Twin detection ----
        stem, ver = parse_version_suffix(name)
        rec["twin_stem"] = stem
        rec["my_version"] = ver
        rec["has_version_twin"] = False
        rec["twin_name"] = None
        rec["twin_version"] = None
        rec["twin_is_higher_version"] = False
        rec["is_identical_to_twin"] = None
        if stem is not None:
            peers = [p for p in stem_map[stem] if p[0] != name]
            if peers:
                rec["has_version_twin"] = True
                # Pick the highest-version peer
                top = max(peers, key=lambda p: p[1])
                rec["twin_name"] = top[0]
                rec["twin_version"] = top[1]
                rec["twin_is_higher_version"] = top[1] > (ver or -1)

        # ---- Reference scans ----
        # is_referenced_by_view (parse view_defs)
        view_refs: list[tuple[str, str]] = []
        # also track if all refs are from snapshot/deprecated objects
        soft_refs_only = True
        any_hard_ref = False
        # quoted name AND bare name with word boundary AND main.<name>
        bare = re.compile(rf"\b{re.escape(name)}\b")
        for vs, vn, vd in view_defs:
            if vd is None:
                continue
            if bare.search(vd):
                view_refs.append((vs, vn))
                # Hard ref unless view name is DEPRECATED__ or contains 'snapshot'
                if vn.startswith("DEPRECATED__"):
                    pass
                elif "snapshot" in vn.lower():
                    pass
                elif "ARCHIVE__" in vn:
                    pass
                else:
                    any_hard_ref = True
                    soft_refs_only = False
        rec["is_referenced_by_view"] = bool(view_refs)
        rec["view_refs"] = view_refs
        rec["n_view_refs"] = len(view_refs)

        # is_referenced_by_script
        # Patterns: bare \bname\b, "name", 'name', main.name
        pat_bare = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])")
        script_refs: list[str] = []
        for rel, txt in py_texts.items():
            if pat_bare.search(txt):
                script_refs.append(rel)
        rec["is_referenced_by_script"] = bool(script_refs)
        rec["n_script_refs"] = len(script_refs)
        rec["script_refs_sample"] = script_refs[:5]

        signals[name] = rec

    # ---------- 7. Identical-to-twin computation (only for relevant cases) ----------
    log("  evaluating identical-to-twin candidates...")
    # Only compute when:
    #   - has_version_twin
    #   - we are NOT the higher version (i.e., we're the candidate to retire)
    #   - both this and twin are BASE TABLE (cannot EXCEPT views safely)
    for name, rec in signals.items():
        if not rec["has_version_twin"]:
            continue
        if not rec["twin_is_higher_version"]:
            continue
        if rec["object_type"] != "BASE TABLE":
            continue
        twin = rec["twin_name"]
        if signals.get(twin, {}).get("object_type") != "BASE TABLE":
            continue
        my_cols = cols_by_obj.get(name, [])
        tw_cols = cols_by_obj.get(twin, [])
        common = [c for c in my_cols if c in tw_cols]
        if not common:
            rec["is_identical_to_twin"] = False
            rec["identical_check_method"] = "no_common_columns"
            continue
        col_csv = ", ".join(f'"{c}"' for c in common)
        try:
            n_a_minus_b = con.execute(
                f'SELECT COUNT(*) FROM '
                f'(SELECT {col_csv} FROM main."{name}" '
                f'EXCEPT SELECT {col_csv} FROM main."{twin}")'
            ).fetchone()[0]
            n_b_minus_a = con.execute(
                f'SELECT COUNT(*) FROM '
                f'(SELECT {col_csv} FROM main."{twin}" '
                f'EXCEPT SELECT {col_csv} FROM main."{name}")'
            ).fetchone()[0]
            rec["except_n_a_minus_b"] = n_a_minus_b
            rec["except_n_b_minus_a"] = n_b_minus_a
            rec["identical_check_method"] = (
                f"EXCEPT on {len(common)}/{len(my_cols)} common cols vs "
                f"{len(tw_cols)} twin cols"
            )
            same_cols = my_cols == tw_cols
            rec["is_identical_to_twin"] = bool(
                n_a_minus_b == 0 and n_b_minus_a == 0 and same_cols
            )
        except Exception as e:
            rec["is_identical_to_twin"] = False
            rec["except_error"] = str(e)[:200]

    # ---------- 8. Classifier ----------
    log("  applying classifier...")
    actions: list[dict] = []
    counts = defaultdict(int)
    for name, rec in signals.items():
        referenced = rec["is_referenced_by_view"] or rec["is_referenced_by_script"]
        action = "LIVE"
        reason_parts: list[str] = []
        destination = ""

        # Rule 1: empty + unreferenced -> DELETE
        if rec["is_empty"] and not referenced:
            action = "DELETE"
            reason_parts.append("empty (row_count=0) and unreferenced")

        # Rule 2: identical higher-version twin + unreferenced -> DELETE
        elif (
            rec["has_version_twin"]
            and rec["twin_is_higher_version"]
            and rec["is_identical_to_twin"]
            and not referenced
        ):
            action = "DELETE"
            reason_parts.append(
                f"identical to higher-version twin {rec['twin_name']} and "
                "unreferenced"
            )

        # Rule 3: non-identical lower-version twin + unreferenced -> DEPRECATE
        elif (
            rec["has_version_twin"]
            and rec["twin_is_higher_version"]
            and rec["is_identical_to_twin"] is False
            and not referenced
        ):
            action = "DEPRECATE"
            destination = f"DEPRECATED__{name}"
            reason_parts.append(
                f"older version of {rec['twin_name']} "
                "(content differs); unreferenced"
            )

        # Rule 4: ARCHIVE if row_count>0 AND name token AND unreferenced
        elif (
            rec["row_count"] > 0
            and not referenced
            and any(t in name.lower() for t in ARCHIVE_NAME_TOKENS)
        ):
            action = "ARCHIVE"
            destination = f"ARCHIVE__{name}__20260417"
            tokens_hit = [t for t in ARCHIVE_NAME_TOKENS if t in name.lower()]
            reason_parts.append(
                f"name suggests historical/snapshot ({','.join(tokens_hit)}) "
                "and unreferenced"
            )

        # KEEP_REVIEW: fallback safety net for ambiguous mid-cases
        # If would-be DELETE/DEPRECATE/ARCHIVE but referenced ONLY by
        # snapshot/deprecated objects → KEEP_REVIEW.
        elif rec["has_version_twin"] and rec["twin_is_higher_version"] and (
            rec["is_referenced_by_view"] and not any_hard_ref_in_views(rec, signals)
        ):
            action = "KEEP_REVIEW"
            reason_parts.append(
                "older version twin but only referenced by deprecated/snapshot "
                "views — needs manual call"
            )
        else:
            action = "LIVE"
            if referenced:
                ref_bits = []
                if rec["is_referenced_by_script"]:
                    ref_bits.append(f"{rec['n_script_refs']} python ref(s)")
                if rec["is_referenced_by_view"]:
                    ref_bits.append(f"{rec['n_view_refs']} view ref(s)")
                reason_parts.append("referenced by " + " and ".join(ref_bits))
            else:
                reason_parts.append(
                    f"non-empty ({rec['row_count']} rows), no twin or "
                    "unreferenced twin context, no archive name token"
                )

        rec["action"] = action
        rec["reason"] = "; ".join(reason_parts)
        rec["destination"] = destination
        counts[action] += 1
        actions.append(
            {
                "object_name": name,
                "object_type": rec["object_type"],
                "row_count": rec["row_count"],
                "n_columns": rec["n_columns"],
                "n_distinct_research_id": rec["n_distinct_research_id"],
                "is_empty": rec["is_empty"],
                "has_version_twin": rec["has_version_twin"],
                "twin_name": rec["twin_name"],
                "twin_is_higher_version": rec["twin_is_higher_version"],
                "is_identical_to_twin": rec["is_identical_to_twin"],
                "is_referenced_by_view": rec["is_referenced_by_view"],
                "n_view_refs": rec["n_view_refs"],
                "is_referenced_by_script": rec["is_referenced_by_script"],
                "n_script_refs": rec["n_script_refs"],
                "action": action,
                "destination": destination,
                "reason": rec["reason"],
            }
        )

    log(f"  action counts: {dict(counts)}")

    # Sort actions: non-LIVE first, by action then name
    order = {"DELETE": 0, "DEPRECATE": 1, "ARCHIVE": 2, "KEEP_REVIEW": 3, "LIVE": 4}
    actions.sort(key=lambda a: (order.get(a["action"], 99), a["object_name"]))

    # CSV
    fieldnames = list(actions[0].keys())
    with ACTIONS_CSV.open("w", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames)
        w.writeheader()
        for r in actions:
            w.writerow(r)
    log(f"  CSV -> {ACTIONS_CSV}")

    SIGNALS_PATH.write_text(json.dumps(signals, indent=2, default=str))
    log(f"  signals JSON -> {SIGNALS_PATH}")

    # Build preview markdown
    md = ["# Phase 3 — proposed actions (audit-only preview)", ""]
    md.append(f"_Generated {datetime.now(timezone.utc).isoformat()}_")
    md.append(
        "_Audit scope: every object in `main` (BASE TABLE + VIEW). "
        "No DDL/DML executed in this stage._"
    )
    md.append("")
    md.append("## Action counts")
    md.append("")
    md.append("| action | n |")
    md.append("|:---|---:|")
    for a in ("DELETE", "DEPRECATE", "ARCHIVE", "KEEP_REVIEW", "LIVE"):
        md.append(f"| {a} | {counts[a]} |")
    md.append(f"| **TOTAL** | {sum(counts.values())} |")
    md.append("")
    md.append("## Stop-gate evaluation (per Logan's spec)")
    md.append("")
    n_del = counts["DELETE"]
    n_arc = counts["ARCHIVE"]
    md.append(f"- DELETEs proposed: **{n_del}** (gate threshold: 10)")
    md.append(f"- ARCHIVEs proposed: **{n_arc}** (gate threshold: 5)")
    if n_del > 10 or n_arc > 5:
        md.append(
            "- **STOP GATE TRIPPED.** Execute DEPRECATE renames first, then "
            "show this preview to Logan before continuing to ARCHIVE/DELETE."
        )
    else:
        md.append(
            "- Stop gate NOT tripped. Logan can authorise full execution "
            "(DEPRECATE → ARCHIVE → DELETE) without an extra checkpoint."
        )
    md.append("")
    for a in ("DELETE", "DEPRECATE", "ARCHIVE", "KEEP_REVIEW"):
        rows = [r for r in actions if r["action"] == a]
        if not rows:
            continue
        md.append(f"## {a} ({len(rows)})")
        md.append("")
        md.append(
            "| object | type | row_count | n_distinct_rid | twin | "
            "identical | view_refs | py_refs | destination | reason |"
        )
        md.append("|:---|:---|---:|---:|:---|:---:|---:|---:|:---|:---|")
        for r in rows:
            md.append(
                f"| `{r['object_name']}` | {r['object_type']} | "
                f"{r['row_count']:,} | "
                f"{r['n_distinct_research_id'] if r['n_distinct_research_id'] is not None else '-'} | "
                f"{r['twin_name'] or '-'} | "
                f"{'T' if r['is_identical_to_twin'] else ('F' if r['is_identical_to_twin'] is False else '-')} | "
                f"{r['n_view_refs']} | {r['n_script_refs']} | "
                f"{r['destination'] or '-'} | {r['reason']} |"
            )
        md.append("")
    PREVIEW_MD.write_text("\n".join(md) + "\n")
    log(f"  preview markdown -> {PREVIEW_MD}")

    # CPM invariant re-assert
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit(
            f"CPM invariant regressed: {n_rows}/{n_distinct} != 10871/10871"
        )
    log(f"  CPM invariant re-asserted: {n_rows}/{n_distinct}")

    log("Phase 3 audit-only inventory complete; no DDL/DML executed.")
    return 0


def any_hard_ref_in_views(rec: dict, all_signals: dict) -> bool:
    for vs, vn in rec["view_refs"]:
        if vn.startswith("DEPRECATED__"):
            continue
        if "snapshot" in vn.lower():
            continue
        if "ARCHIVE__" in vn:
            continue
        return True
    return False


if __name__ == "__main__":
    sys.exit(main())
