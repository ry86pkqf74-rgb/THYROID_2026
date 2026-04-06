#!/usr/bin/env python3
"""Generate machine-readable domain inventory artifacts.

Reads the registry YAML (SSOT), scans on-disk parquet locations, and
compares against the VastAI fleet DOMAIN_PROMPT map to produce:

  - domain_inventory.csv   — full domain × parquet × classification matrix
  - fleet_parity_report.csv — fleet key vs registry alignment
  - sub_prompt_map.csv      — sub-prompt key → parent domain mapping
  - inventory_summary.md    — human-readable overview

Usage:
    .venv/bin/python studies/20260406_domain_inventory_current/generate_inventory.py
"""
from __future__ import annotations

import csv
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from llm_extraction.registry import load_registry

PROCESSED = ROOT / "processed"
V2_FLEET_DIR = PROCESSED / "output" / "v2_parquets"
PROMPTS_DIR = ROOT / "llm_extraction" / "prompts"
OUT_DIR = Path(__file__).resolve().parent

FLEET_SCRIPTS = [
    ROOT / "scripts" / "vastai" / "run_extraction_concurrent.py",
    ROOT / "scripts" / "run_extraction_split.py",
]


def _extract_domain_prompt_dict(script_path: Path) -> dict[str, str]:
    src = script_path.read_text(encoding="utf-8")
    match = re.search(r"DOMAIN_PROMPT\s*=\s*\{", src)
    if not match:
        return {}
    start = match.start()
    depth = 0
    brace_start = src.index("{", start)
    for i, ch in enumerate(src[brace_start:], brace_start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                block = src[brace_start : i + 1]
                break
    pairs = re.findall(r'"([^"]+)"\s*:\s*"([^"]+)"', block)
    return dict(pairs)


def _scan_parquets() -> dict[str, Path]:
    result: dict[str, Path] = {}
    for d in [PROCESSED, V2_FLEET_DIR]:
        if d.is_dir():
            for pq in d.glob("*.parquet"):
                if pq.stem.startswith("note_entities"):
                    result[pq.stem] = pq
    return result


def main() -> None:
    load_registry.cache_clear()
    reg = load_registry()
    on_disk = _scan_parquets()

    # ── domain_inventory.csv ────────────────────────────────────────────────
    rows = []
    for name, spec in reg.domains.items():
        stem = spec.parquet_stem
        pq_path = on_disk.get(stem)
        md_stage = f"v2_stage.{stem}" if spec.is_v2 else f"main.{stem}"
        rows.append({
            "registry_domain": name,
            "parquet_stem": stem,
            "canonical_target": spec.canonical_target,
            "note_scope": spec.note_scope,
            "linkage_anchor_family": spec.linkage_anchor_family,
            "qa_tier": spec.qa_tier,
            "tier": spec.tier,
            "parquet_on_disk": pq_path is not None,
            "parquet_path": str(pq_path) if pq_path else "",
            "md_stage_table": md_stage,
            "classification": reg.classify_stem(stem),
        })

    for sp_name, sp in reg.sub_prompt_domains.items():
        stem = sp.parquet_stem
        pq_path = on_disk.get(stem)
        parent_spec = reg.domains.get(sp.parent_domain)
        rows.append({
            "registry_domain": f"{sp.parent_domain}__sub:{sp_name}",
            "parquet_stem": stem,
            "canonical_target": parent_spec.canonical_target if parent_spec else "",
            "note_scope": parent_spec.note_scope if parent_spec else "all",
            "linkage_anchor_family": parent_spec.linkage_anchor_family if parent_spec else "",
            "qa_tier": parent_spec.qa_tier if parent_spec else "",
            "tier": parent_spec.tier if parent_spec else "v2",
            "parquet_on_disk": pq_path is not None,
            "parquet_path": str(pq_path) if pq_path else "",
            "md_stage_table": "",
            "classification": "child-enrichment",
        })

    unclaimed = set(on_disk.keys()) - reg.all_known_stems()
    for stem in sorted(unclaimed):
        if stem == "note_entities_llm_combined":
            continue
        rows.append({
            "registry_domain": "UNCLAIMED",
            "parquet_stem": stem,
            "canonical_target": "",
            "note_scope": "",
            "linkage_anchor_family": "",
            "qa_tier": "",
            "tier": "",
            "parquet_on_disk": True,
            "parquet_path": str(on_disk[stem]),
            "md_stage_table": "",
            "classification": "unclaimed",
        })

    inv_path = OUT_DIR / "domain_inventory.csv"
    with open(inv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {inv_path.name}: {len(rows)} rows")

    # ── fleet_parity_report.csv ─────────────────────────────────────────────
    expected = reg.expected_fleet_prompt_map()
    fleet_rows = []
    for script in FLEET_SCRIPTS:
        fleet_map = _extract_domain_prompt_dict(script)
        for key in sorted(set(fleet_map) | set(expected)):
            fleet_val = fleet_map.get(key, "")
            expected_val = expected.get(key, "")
            if key in fleet_map and key in expected:
                status = "match" if fleet_val == expected_val else "prompt-mismatch"
            elif key in fleet_map:
                status = "fleet-extra"
            else:
                status = "fleet-missing"
            fleet_rows.append({
                "script": script.name,
                "fleet_key": key,
                "fleet_prompt_file": fleet_val,
                "registry_prompt_file": expected_val,
                "registry_domain": key if key in reg.domains else (
                    reg.sub_prompt_domains[key].parent_domain
                    if key in reg.sub_prompt_domains else ""
                ),
                "status": status,
            })

    fleet_path = OUT_DIR / "fleet_parity_report.csv"
    with open(fleet_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(fleet_rows[0].keys()))
        w.writeheader()
        w.writerows(fleet_rows)
    print(f"  Wrote {fleet_path.name}: {len(fleet_rows)} rows")

    # ── sub_prompt_map.csv ──────────────────────────────────────────────────
    sp_rows = []
    for sp_name, sp in reg.sub_prompt_domains.items():
        sp_rows.append({
            "sub_prompt_key": sp_name,
            "parent_domain": sp.parent_domain,
            "prompt_file": sp.prompt_file,
            "parquet_stem": sp.parquet_stem,
            "prompt_exists": sp.prompt_absolute_path.exists(),
        })
    sp_path = OUT_DIR / "sub_prompt_map.csv"
    with open(sp_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(sp_rows[0].keys()))
        w.writeheader()
        w.writerows(sp_rows)
    print(f"  Wrote {sp_path.name}: {len(sp_rows)} rows")

    # ── inventory_summary.md ────────────────────────────────────────────────
    n_total = len(reg.domains)
    n_v1 = len(reg.v1_domains)
    n_v2 = len(reg.v2_domains)
    n_canonical = len(reg.canonical_domains)
    n_sub = len(reg.sub_prompt_domains)
    n_on_disk = sum(1 for r in rows if r["parquet_on_disk"])
    n_missing = sum(
        1 for r in rows
        if not r["parquet_on_disk"] and r["classification"] == "standalone"
    )
    n_unclaimed = sum(1 for r in rows if r["classification"] == "unclaimed")

    fleet_ok = all(r["status"] == "match" for r in fleet_rows)

    lines = [
        "# Domain Inventory Summary",
        "",
        f"**Generated:** {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Registry",
        "",
        f"- Total domains: {n_total} ({n_v1} v1, {n_v2} v2)",
        f"- Canonical domains: {n_canonical}",
        f"- Sub-prompt domains: {n_sub}",
        f"- Total known stems: {len(reg.all_known_stems())}",
        "",
        "## On-Disk Parquets",
        "",
        f"- Parquets on disk: {n_on_disk}",
        f"- Standalone missing: {n_missing}",
        f"- Unclaimed on disk: {n_unclaimed}",
        "",
        "## Fleet Parity",
        "",
        f"- Overall: {'PASS' if fleet_ok else 'DRIFT DETECTED'}",
        f"- Scripts checked: {', '.join(s.name for s in FLEET_SCRIPTS)}",
        "",
        "## Classification Breakdown",
        "",
        "| Classification | Count |",
        "|---------------|-------|",
    ]
    from collections import Counter
    class_counts = Counter(r["classification"] for r in rows)
    for cls in ["standalone", "child-enrichment", "audit-only", "missing", "unclaimed"]:
        lines.append(f"| {cls} | {class_counts.get(cls, 0)} |")
    lines.append("")

    summary_path = OUT_DIR / "inventory_summary.md"
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Wrote {summary_path.name}")

    print(f"\n  Inventory artifacts written to: {OUT_DIR}")


if __name__ == "__main__":
    main()
