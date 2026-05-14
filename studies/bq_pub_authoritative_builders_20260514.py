#!/usr/bin/env python3
"""
Prompt 10 — Authoritative BigQuery builders for pub_canonical / pub_semantic / pub_views_readable.

Evidence tiers (strong → auxiliary):
  - CREATE [OR REPLACE] TABLE|VIEW `…pub_*.<id>…` (inc. multiline / full project path)
  - Google client load_table_from_* with backtick destination
  - bq CLI load --replace with literal proj:dataset.table OR f-string dest resolved via module consts
  - mig_327_bulk_md_to_bq_missing_tables.TABLES catalogue (parquet → bq load --replace)
"""
from __future__ import annotations

import io
import json
import re
import subprocess
import tokenize
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, FrozenSet, Iterable

REPO = Path(__file__).resolve().parents[1]
SNAPSHOT = REPO / "studies" / "bq_pub_object_list_snapshot_20260514.json"
OUT_JSON = REPO / "studies" / "bq_pub_authoritative_builders_20260514.json"
OUT_TSV = REPO / "studies" / "bq_pub_authoritative_builders_table_20260514.tsv"

# Curated authoritative builders where automation cannot see pub_canonical CREATE (MD SSOT + release mirror).
# value: (builder_script, human_note)
CURATED_LINEAGE: dict[tuple[str, str], tuple[str, str]] = {
    (
        "pub_canonical",
        "canonical_path_malignant_events_v1",
    ): (
        "scripts/361_op_path_consolidation.py",
        "SSOT on MotherDuck main: CREATE OR REPLACE TABLE main.canonical_path_malignant_events_v1. "
        "pub_canonical copy is the publication mirror (MD→BQ release / bulk load), not a separate BQ-native DDL file.",
    ),
    (
        "pub_canonical",
        "canonical_path_malignant_patient_rollup_v1",
    ): (
        "scripts/361_op_path_consolidation.py",
        "Built in scripts/361_op_path_consolidation.py (malignant patient rollup from path events); BQ hydrate via release mirror.",
    ),
}

RE_CREATE_BT = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+`[^`]*\b(pub_canonical|pub_semantic|pub_views_readable)\.(\w+)`",
    re.IGNORECASE | re.DOTALL,
)
RE_CREATE_FULLPROJ = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+`thyroid-canonical-pub-2026\.(pub_canonical|pub_semantic|pub_views_readable)\.(\w+)`",
    re.IGNORECASE | re.DOTALL,
)
RE_LOAD_DEST = re.compile(
    r"load_table_from_(?:dataframe|json|uri|file)\s*\(\s*[^,]+,\s*`[^`]*\b(pub_canonical|pub_semantic|pub_views_readable)\.(\w+)`",
    re.IGNORECASE | re.DOTALL,
)
# mig_327 TABLES rows: ("table_name", "main"|...
RE_M327_ROW = re.compile(
    r'^\s*\(\s*"([^"]+)"\s*,\s*"(?:main|readonly_share|__ctc_rebuild__)"',
    re.MULTILINE,
)
RE_MOD_CONST = re.compile(
    r"^([A-Z][A-Z0-9_]*)\s*=\s*[\"']([^\"']+)[\"']",
    re.MULTILINE,
)
# Literal destination in bq helper
RE_QUAL_STR = re.compile(
    r"[\"']([a-z0-9.-]+):(pub_canonical|pub_semantic|pub_views_readable)\.(\w+)[\"']",
    re.IGNORECASE,
)


def expand_tuple_tables(inner: str, consts: dict[str, str]) -> list[str]:
    out: list[str] = []
    for part in inner.split(","):
        p = part.strip()
        if not p:
            continue
        if len(p) >= 2 and p[0] == p[-1] and p[0] in "\"'":
            out.append(p[1:-1])
            continue
        key = p.split("#", 1)[0].strip()
        if key in consts:
            out.append(consts[key])
    return out


def tuple_sync_load_hits(
    nc: str, rel: str, all_ids: dict[str, set[str]]
) -> list[tuple[str, str, str, str]]:
    """BQ_SYNC_TABLES-style tuples + load_table_from_file (e.g. 382_restore)."""
    if "load_table_from_file" not in nc:
        return []
    consts = module_consts(nc)
    out: list[tuple[str, str, str, str]] = []
    for m in re.finditer(
        r"([A-Z][A-Z0-9_]*)\s*:\s*tuple\[[^\]]+\]\s*=\s*\((.*?)\n\)",
        nc,
        re.DOTALL,
    ):
        name, inner = m.group(1), m.group(2)
        if "SYNC" not in name and "BQ_" not in name:
            continue
        for tid in expand_tuple_tables(inner, consts):
            for ds in ("pub_canonical", "pub_semantic", "pub_views_readable"):
                if ds in all_ids and tid in all_ids[ds]:
                    out.append((ds, tid, "LOAD_FILE_NAMED_TUPLE", name))
    return out


def iter_non_comment_tokens(source: str) -> Iterable[tokenize.TokenInfo]:
    try:
        yield from tokenize.generate_tokens(io.StringIO(source).readline)
    except tokenize.TokenError:
        return


def non_comment_text(source: str) -> str:
    parts: list[str] = []
    for tok in iter_non_comment_tokens(source):
        if tok.type == tokenize.COMMENT:
            continue
        parts.append(tok.string)
    return "".join(parts)


def git_ls_trackedsuffixes(suffixes: tuple[str, ...]) -> list[str]:
    paths: list[str] = []
    for suf in suffixes:
        raw = subprocess.check_output(
            ["git", "-C", str(REPO), "ls-files", f"*{suf}"], text=True
        )
        paths.extend(ln for ln in raw.splitlines() if ln.endswith(suf))
    return sorted(set(paths))


def load_bq_ids() -> dict[str, FrozenSet[str]]:
    data = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    return {ds: frozenset(ids) for ds, ids in data["datasets"].items()}


def load_runbook_haystack() -> str:
    parts: list[str] = []
    for glob in (REPO / ".github", REPO / "Makefile", REPO / "justfile"):
        if glob.is_file():
            parts.append(glob.read_text(encoding="utf-8", errors="replace"))
        elif glob.is_dir():
            for p in sorted(glob.rglob("*")):
                if p.suffix in {".yml", ".yaml", ".md"} and p.is_file():
                    try:
                        parts.append(p.read_text(encoding="utf-8", errors="replace"))
                    except OSError:
                        pass
    for doc in (
        "docs/release_runbook.md",
        "docs/motherduck_release_runbook_v2.md",
        "docs/motherduck_v2_staging_runbook.md",
        "docs/publication_governance_gate.md",
    ):
        p = REPO / doc
        if p.is_file():
            parts.append(p.read_text(encoding="utf-8", errors="replace"))
    return "\n".join(parts)


def referenced_in_runbook(rel_path: str, haystack: str) -> bool:
    norm = rel_path.replace("\\", "/")
    base = Path(rel_path).name
    stem_py = base[:-3] if base.endswith(".py") else base
    if norm in haystack or base in haystack:
        return True
    m = re.match(r"scripts/(\d{3})", norm.replace("\\", "/"))
    if m and m.group(1) in haystack:
        return True
    if f"scripts/{stem_py}" in haystack:
        return True
    if f"qc_framework_v1/migrations/{base}" in haystack:
        return True
    return False


def migration_score(rel_path: str) -> int:
    s = rel_path.replace("\\", "/")
    m = re.search(r"mig_(\d+)", s, re.I)
    if m:
        return int(m.group(1))
    m2 = re.search(r"/(\d{3})[a-z_]?", s)
    if m2:
        return int(m2.group(1))
    return 0


@dataclass
class Hit:
    rel_path: str
    evidence: str
    pattern: str


def parse_m327_tables(raw: str, rel: str) -> list[tuple[str, str, str, str]]:
    if "327_bulk_md_to_bq" not in raw and "mig_327" not in raw:
        return []
    if "TABLES:" not in raw or "_bq_load" not in raw:
        return []
    out: list[tuple[str, str, str, str]] = []
    for m in RE_M327_ROW.finditer(raw):
        tid = m.group(1)
        out.append(("pub_canonical", tid, "M327_TABLES_CATALOG", f"TABLES row {tid}"))
    return out


def module_consts(nc: str) -> dict[str, str]:
    return {m.group(1): m.group(2) for m in RE_MOD_CONST.finditer(nc)}


def fstring_bq_dest_hits(nc: str, consts: dict[str, str], all_ids: dict[str, set[str]]):
    """Resolve dest = f\"{...}:{...}.{VAR}\" patterns against string constants."""
    out: list[tuple[str, str, str, str]] = []
    if "subprocess" not in nc and "bq" not in nc:
        return out
    if "load" not in nc or "--replace" not in nc:
        return out
    ds_keys = ("CANONICAL_DATASET", "_DATASET", "DATASET", "CANON_DS", "DATASET_PUB")
    ds_val = next((consts[k] for k in ds_keys if k in consts and consts[k] in all_ids), None)
    if not ds_val:
        return out
    for m in re.finditer(
        r'dest\s*=\s*f(?P<q>["\'])\{[^}]+\}:\{[^}]+\}\.\{(?P<var>[A-Z][A-Z0-9_]*)\}(?P=q)',
        nc,
    ):
        var = m.group("var")
        if var not in consts:
            continue
        tid = consts[var]
        if tid in all_ids[ds_val]:
            out.append((ds_val, tid, "BQ_CLI_FSTRING_DEST", f"dest uses {{{var}}}={tid}"))
    return out


def literal_bq_load_hits(nc: str, all_ids: dict[str, set[str]]):
    if "subprocess" not in nc or "bq" not in nc:
        return []
    if "load" not in nc or "--replace" not in nc:
        return []
    if "PARQUET" not in nc:
        return []
    out: list[tuple[str, str, str, str]] = []
    for m in RE_QUAL_STR.finditer(nc):
        ds, tid = m.group(2), m.group(3)
        if ds in all_ids and tid in all_ids[ds]:
            out.append((ds, tid, "BQ_CLI_LITERAL_DEST", m.group(0)[:80]))
    return out


def find_strong_hits(nc: str, rel: str) -> list[tuple[str, str, str, str]]:
    out: list[tuple[str, str, str, str]] = []
    for rx, pname in (
        (RE_CREATE_BT, "CREATE_BACKTICK"),
        (RE_CREATE_FULLPROJ, "CREATE_FULLPROJ"),
        (RE_LOAD_DEST, "LOAD_TABLE_API"),
    ):
        for m in rx.finditer(nc):
            ds, tid = m.group(1), m.group(2)
            snippet = m.group(0)[:160].replace("\n", " ")
            out.append((ds, tid, pname, snippet))
    return out


def all_hits_for_file(raw: str, rel: str, all_ids: dict[str, set[str]]) -> list[tuple[str, str, str, str]]:
    if rel.endswith(".py"):
        nc = non_comment_text(raw)
        mh = parse_m327_tables(raw, rel)
    else:
        nc = raw
        mh = []
    hits = find_strong_hits(nc, rel) + mh
    consts = module_consts(nc if rel.endswith(".py") else raw)
    hits.extend(fstring_bq_dest_hits(nc, consts, all_ids))
    hits.extend(literal_bq_load_hits(nc, all_ids))
    hits.extend(tuple_sync_load_hits(nc, rel, all_ids))
    valid = []
    for ds, tid, pname, ev in hits:
        if ds in all_ids and tid in all_ids[ds]:
            valid.append((ds, tid, pname, ev))
    return valid


PUB_CRITICAL = frozenset(
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


def main() -> int:
    datasets_objs = load_bq_ids()
    all_ids: dict[str, set[str]] = {k: set(v) for k, v in datasets_objs.items()}
    haystack = load_runbook_haystack()

    builders: DefaultDict[tuple[str, str], list[Hit]] = defaultdict(list)

    for rel in git_ls_trackedsuffixes((".py", ".sql")):
        path = REPO / rel
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for ds, tid, pname, ev in all_hits_for_file(raw, rel, all_ids):
            builders[(ds, tid)].append(Hit(rel, ev, pname))

    auth: dict[str, dict] = {}
    conflicts: list[dict] = []

    for ds in sorted(all_ids.keys()):
        for tid in sorted(all_ids[ds]):
            hits = builders.get((ds, tid), [])
            if not hits:
                auth[f"{ds}.{tid}"] = {
                    "dataset": ds,
                    "table": tid,
                    "authoritative_builder": "ORPHAN_BUILDER",
                    "port_tier": "P0"
                    if tid in PUB_CRITICAL
                    or "signoff" in tid
                    or tid.startswith("canonical_patient")
                    else "P1",
                    "alternate_builders": [],
                    "evidence": [],
                }
                continue

            candidates = sorted({h.rel_path for h in hits})
            scored = [
                (migration_score(c), referenced_in_runbook(c, haystack), c) for c in candidates
            ]
            scored.sort(key=lambda x: (-int(x[1]), -x[0], x[2]))
            winner = scored[0][2]
            alts = [c for c in candidates if c != winner]

            if len(candidates) > 1:
                conflicts.append(
                    {
                        "object": f"{ds}.{tid}",
                        "winner": winner,
                        "alternates": alts,
                        "resolution": "runbook_then_mig_score",
                    }
                )

            auth[f"{ds}.{tid}"] = {
                "dataset": ds,
                "table": tid,
                "authoritative_builder": winner,
                "port_tier": "P0"
                if tid in PUB_CRITICAL or "signoff" in tid
                else "P1",
                "alternate_builders": alts,
                "evidence": [
                    {"file": h.rel_path, "pattern": h.pattern, "snippet": h.evidence}
                    for h in hits
                    if h.rel_path == winner
                ][:4],
            }

    # Apply curated MD→BQ lineage for objects with no direct pub_* CREATE in-repo.
    for (ds, tid), (bldr, note) in CURATED_LINEAGE.items():
        k = f"{ds}.{tid}"
        rec = auth.get(k)
        if not rec or rec.get("authoritative_builder") != "ORPHAN_BUILDER":
            continue
        rec["authoritative_builder"] = bldr
        rec["lineage_note"] = note
        rec["evidence"] = [
            {
                "file": bldr,
                "pattern": "CURATED_MD_LINEAGE",
                "snippet": note[:280],
            }
        ]

    ORPHAN = sum(1 for v in auth.values() if v["authoritative_builder"] == "ORPHAN_BUILDER")

    # TSV map for spreadsheets / join to other audits
    lines = ["dataset\ttable\tauthoritative_builder\tport_tier\talternate_builders_csv"]
    for key in sorted(auth.keys()):
        v = auth[key]
        alts = ";".join(v.get("alternate_builders") or [])
        lines.append(
            f"{v['dataset']}\t{v['table']}\t{v['authoritative_builder']}\t{v['port_tier']}\t{alts}"
        )
    OUT_TSV.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # P0 publication spine: unique non-orphan builders for PUB_CRITICAL ∩ snapshot
    p0_scripts = sorted(
        {
            auth[f"{ds}.{t}"]["authoritative_builder"]
            for t in PUB_CRITICAL
            for ds in ("pub_canonical", "pub_semantic", "pub_views_readable")
            if f"{ds}.{t}" in auth
            and auth[f"{ds}.{t}"]["authoritative_builder"] != "ORPHAN_BUILDER"
        }
    )
    p0_table_map = []
    for t in sorted(PUB_CRITICAL):
        row = {"table": t, "builders_by_dataset": {}}
        for ds in ("pub_canonical", "pub_semantic", "pub_views_readable"):
            k = f"{ds}.{t}"
            if k not in auth:
                continue
            b = auth[k]["authoritative_builder"]
            row["builders_by_dataset"][ds] = {
                "authoritative_builder": b,
                "port_tier": auth[k].get("port_tier"),
                "lineage_note": auth[k].get("lineage_note"),
            }
        if row["builders_by_dataset"]:
            p0_table_map.append(row)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps(
            {
                "generated_note": "Pub snapshot + CREATE/load/M327 + tuple sync + CURATED_MD_LINEAGE (path malignant SSOT=361).",
                "orphan_count": ORPHAN,
                "multi_builder_conflicts": conflicts,
                "p0_publication_spine_builders": p0_scripts,
                "p0_publication_critical_table_map": p0_table_map,
                "objects": auth,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "out": str(OUT_JSON),
                "tsv": str(OUT_TSV),
                "orphans": ORPHAN,
                "conflicts": len(conflicts),
                "p0_builders_n": len(p0_scripts),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
