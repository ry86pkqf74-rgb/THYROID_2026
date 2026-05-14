#!/usr/bin/env python3
"""
Prompt 9 — MotherDuck publisher footprint: TSV hits + disposition + BQ crosswalk hints.

Footprint = git-tracked *.py matching:
  connect_locked( | MotherDuckClient | connect_md(|_connect_md(|_round2_helpers |
  (duckdb.connect + md: in same file)
"""
from __future__ import annotations

import io
import json
import re
import subprocess
import tokenize
from pathlib import Path
from typing import FrozenSet, Iterable

INFRA_ALWAYS_ACTIVE = frozenset(
    {
        "scripts/_md_connect.py",
        "scripts/_round2_helpers.py",
        "motherduck_client.py",
        "utils/md_connect.py",
    }
)

REPO = Path(__file__).resolve().parents[1]
OUT_TSV = REPO / "studies" / "motherduck_duckdb_hits_20260514.tsv"

RE_CONNECT_LOCKED = re.compile(r"connect_locked\s*\(")
RE_MOTHERDUCK_CLIENT = re.compile(r"MotherDuckClient")
RE_ROUND2 = re.compile(r"_round2_helpers|connect_md\s*\(|_connect_md\s*\(")
RE_DUCKDB = re.compile(r"duckdb\.connect")
RE_MD = re.compile(r"md[:?]")
RE_DDL_CREATE = re.compile(
    r"CREATE\s+(OR\s+REPLACE\s+)?\b(TABLE|VIEW)\b", re.IGNORECASE
)
RE_TO_BQ = re.compile(r"to_gbq|load_job|load_table_from_file", re.IGNORECASE)

DEAD_PREFIXES = (
    "scripts/archive/",
    "scripts/frozen/",
    "M025_submission_package",
    "M032_submission_package",
    "M037_submission_package",
    "M038_submission_package",
    "M044_submission_package",
    "M025_FINAL_PACKAGE/",
    "snowflake_trial/",
)

UNCERTAIN_PREFIXES = (
    "scripts/output/",
    "studies/",
)

PUBLICATION_ANCHOR_TABLES: FrozenSet[str] = frozenset(
    {
        "canonical_patient_master",
        "manuscript_cohort_v1",
        "synoptic_tumor_long_v1",
        "canonical_path_malignant_events_v1",
        "canonical_tumor_characteristics_v1",
        "tumor_episode_master_v2",
        "thyroid_scoring_py_v1",
        "path_synoptics",
        "signoff_migration",
    }
)


def git_tracked_py() -> list[str]:
    raw = subprocess.check_output(
        ["git", "-C", str(REPO), "ls-files", "*.py"], text=True
    )
    return [ln for ln in raw.splitlines() if ln.endswith(".py")]


def in_footprint(text: str) -> bool:
    if RE_CONNECT_LOCKED.search(text):
        return True
    if RE_MOTHERDUCK_CLIENT.search(text):
        return True
    if RE_ROUND2.search(text):
        return True
    if RE_DUCKDB.search(text) and RE_MD.search(text):
        return True
    return False


def iter_non_comment_tokens(source: str) -> Iterable[tokenize.TokenInfo]:
    try:
        yield from tokenize.generate_tokens(io.StringIO(source).readline)
    except tokenize.TokenError:
        return


def non_comment_concat(source: str) -> str:
    parts: list[str] = []
    for tok in iter_non_comment_tokens(source):
        if tok.type == tokenize.COMMENT:
            continue
        parts.append(tok.string)
    return "".join(parts)


def coupling_pattern(nc: str, raw: str) -> str:
    """Primary coupling (strongest) using non-comment token stream only."""
    if re.search(r"connect_locked\s*\(", nc):
        return "connect_locked"
    if re.search(r"connect_md\s*\(|_connect_md\s*\(", nc) or "_round2_helpers" in nc:
        return "connect_md"
    if "MotherDuckClient" in nc:
        return "MotherDuckClient"
    if RE_DUCKDB.search(nc) and RE_MD.search(nc):
        return "duckdb.connect"
    if in_footprint(raw):
        return "comment-only"
    return "none"


def is_comment_only_md(source: str, nc: str) -> bool:
    """True if MD/DuckDB coupling appears only in # comments (not code/strings)."""
    if not in_footprint(source):
        return False
    if re.search(r"connect_locked\s*\(", nc):
        return False
    if RE_MOTHERDUCK_CLIENT.search(nc):
        return False
    if RE_ROUND2.search(nc):
        return False
    if RE_DUCKDB.search(nc) and RE_MD.search(nc):
        return False
    return True


def function_or_area(rel: str, source: str) -> str:
    if rel in ("scripts/_md_connect.py", "motherduck_client.py", "utils/md_connect.py"):
        return "connection_infra"
    if "_round2_helpers" in source and rel.endswith("_round2_helpers.py"):
        return "round2_helpers"
    if "/migrations/" in rel.replace("\\", "/"):
        return "qc_migration"
    if re.match(r"scripts/mig_\d+", Path(rel).name):
        return "numbered_mig"
    if rel.startswith("qc_framework_v1/scripts/"):
        return "qc_script"
    if rel.startswith("scripts/"):
        return "scripts"
    if rel.startswith("studies/"):
        return "studies"
    if rel.startswith("tests/"):
        return "tests"
    return "other"


def load_orchestra_haystack() -> str:
    parts: list[str] = []
    gh = REPO / ".github"
    if gh.is_dir():
        for p in sorted(gh.rglob("*")):
            if p.suffix in {".yml", ".yaml"} and p.is_file():
                try:
                    parts.append(p.read_text(encoding="utf-8", errors="replace"))
                except OSError:
                    pass
    for name in ("Makefile", "justfile"):
        p = REPO / name
        if p.is_file():
            try:
                parts.append(p.read_text(encoding="utf-8", errors="replace"))
            except OSError:
                pass
    return "\n".join(parts)


def referenced_by_orchestra(rel_path: str, haystack: str) -> bool:
    norm = rel_path.replace("\\", "/")
    base = Path(rel_path).name
    stem = base[:-3] if base.endswith(".py") else base
    if norm in haystack:
        return True
    if base in haystack:
        return True
    if f"scripts/{stem}" in haystack or f"qc_framework_v1/scripts/{stem}" in haystack:
        return True
    return False


def load_bq_identifiers() -> FrozenSet[str]:
    """Table and view ids from pub_canonical, pub_semantic, pub_views_readable."""
    ids: set[str] = set()
    for dataset in (
        "thyroid-canonical-pub-2026:pub_canonical",
        "thyroid-canonical-pub-2026:pub_semantic",
        "thyroid-canonical-pub-2026:pub_views_readable",
    ):
        try:
            out = subprocess.check_output(
                ["bq", "ls", "--max_results", "10000", dataset],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            print(f"WARNING: bq ls failed for {dataset}", file=sys.stderr)
            continue
        for line in out.splitlines()[2:]:
            parts = line.split()
            if parts and not parts[0].startswith("-"):
                tid = parts[0].strip()
                if tid and tid != "tableId":
                    ids.add(tid)
    return frozenset(ids)


def tables_touched_in_source(source: str, bq_ids: FrozenSet[str]) -> list[str]:
    hit = sorted(t for t in bq_ids if t in source)
    return hit


def is_dead_prefix(rel: str) -> bool:
    rel_f = rel.replace("\\", "/")
    for p in DEAD_PREFIXES:
        if rel_f == p or rel_f.startswith(p) or f"/{p}" in rel_f:
            return True
    return False


def is_uncertain_prefix(rel: str) -> bool:
    rel_f = rel.replace("\\", "/")
    return any(rel_f.startswith(p) for p in UNCERTAIN_PREFIXES)


def has_ddl_or_bq_write(source: str) -> bool:
    if RE_DDL_CREATE.search(source):
        return True
    if RE_TO_BQ.search(source):
        return True
    return False


def port_tier(
    disp: str,
    cp: str,
    source: str,
    bq_hits: list[str],
) -> str:
    if disp != "ACTIVE":
        return "-"
    ddl = has_ddl_or_bq_write(source)
    pub = bool(bq_hits)
    anchor = pub_critical(bq_hits)
    strong_coupling = cp in ("connect_locked", "connect_md")
    if strong_coupling and ddl and anchor:
        return "P0"
    if strong_coupling and ddl and pub:
        return "P1"
    if strong_coupling and pub:
        return "P2"
    if ddl and pub:
        return "P3"
    if pub or strong_coupling:
        return "P4"
    return "P5"


def disposition(
    rel: str,
    comment_only: bool,
    orch: bool,
    bq_hits: list[str],
) -> str:
    if comment_only:
        return "COMMENT"
    norm = rel.replace("\\", "/")
    if norm in INFRA_ALWAYS_ACTIVE:
        return "ACTIVE"
    if is_dead_prefix(rel):
        return "DEAD"
    if orch or bq_hits:
        return "ACTIVE"
    if is_uncertain_prefix(rel):
        return "UNCERTAIN"
    rel_f = rel.replace("\\", "/")
    if rel_f.startswith("scripts/") and "output" not in rel_f:
        if rel_f.endswith("_probe.py") or "/_" in rel_f:
            return "UNCERTAIN"
    return "DEAD"


def pub_critical(bq_hits: list[str]) -> bool:
    return bool(PUBLICATION_ANCHOR_TABLES.intersection(bq_hits))


def main() -> int:
    import sys

    bq_ids = load_bq_identifiers()
    haystack = load_orchestra_haystack()

    rows: list[tuple[str, str, str, str, str, str, str, str, str]] = []

    for rel in sorted(git_tracked_py()):
        path = REPO / rel
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if not in_footprint(source):
            continue

        nc = non_comment_concat(source)
        c_only = is_comment_only_md(source, nc)
        cp = "comment-only" if c_only else coupling_pattern(nc, source)
        area = function_or_area(rel, source)
        orch = referenced_by_orchestra(rel, haystack)
        bq_hits = tables_touched_in_source(source, bq_ids)
        disp = disposition(rel, c_only, orch, bq_hits)
        crit = "yes" if pub_critical(bq_hits) else "no"
        pub_any = "yes" if bq_hits else "no"
        ddl_f = "yes" if has_ddl_or_bq_write(source) else "no"
        pt = port_tier(disp, cp, source, bq_hits)
        bq_cell = ";".join(bq_hits[:80])
        if len(bq_hits) > 80:
            bq_cell += f";...(+{len(bq_hits) - 80})"
        rows.append((rel, cp, area, disp, crit, pub_any, ddl_f, pt, bq_cell))

    OUT_TSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_TSV.open("w", encoding="utf-8") as fh:
        fh.write(
            "file_path\tcoupling_pattern\tfunction_or_area\tdisposition\t"
            "publication_anchor_overlap\tmentions_any_pub_dataset_table\t"
            "ddl_or_bq_load_heuristic\tport_backlog_tier\tbq_tables_substring_mentions\n"
        )
        for r in rows:
            fh.write("\t".join(r) + "\n")

    from collections import Counter

    c_disp = Counter(r[3] for r in rows)
    n_footprint = len(rows)
    active = [r for r in rows if r[3] == "ACTIVE"]
    active_pc = [r for r in active if r[4] == "yes"]
    active_pub = [r for r in active if r[5] == "yes" and r[4] == "no"]
    active_orphan = [r for r in active if r[5] == "no"]
    tier_counts = Counter(r[7] for r in active)

    summary = {
        "N_footprint_gittracked_py_union": n_footprint,
        "N_ACTIVE": c_disp["ACTIVE"],
        "N_DEAD": c_disp["DEAD"],
        "N_COMMENT": c_disp["COMMENT"],
        "N_UNCERTAIN": c_disp["UNCERTAIN"],
        "disposition_sum_check": sum(c_disp.values()),
        "N_ACTIVE_publication_anchor_overlap": len(active_pc),
        "N_ACTIVE_pub_dataset_non_anchor": len(active_pub),
        "N_ACTIVE_orchestra_only_no_bq_string": len(active_orphan),
        "ACTIVE_port_tier_counts": dict(sorted(tier_counts.items())),
        "bq_object_count_loaded": len(bq_ids),
        "tsv": str(OUT_TSV.relative_to(REPO)),
    }
    print(json.dumps(summary, indent=2))

    p0 = [r for r in active if r[7] == "P0"]
    print("\n# P0 backlog (publication-critical port heuristic)", file=sys.stderr)
    for r in p0:
        print(f"  {r[0]}\t{r[8][:200]}", file=sys.stderr)
    print("\n# ACTIVE publication-anchor (any tier)", len(active_pc), file=sys.stderr)
    print("# ACTIVE other pub-dataset mentions", len(active_pub), file=sys.stderr)
    print("# ACTIVE orchestrator-only", len(active_orphan), file=sys.stderr)
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(main())
