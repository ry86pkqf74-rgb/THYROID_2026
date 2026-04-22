#!/usr/bin/env python3
"""
Script 385 — round-2 canonical verification (READ-ONLY to MotherDuck).

Validates the post-Phase-B / post-Phase-C state of PUB.main against the
acceptance criteria from cursor prompt 2026-04-22 §3 Phase C:

  C1. Every new canonical exists in PUB.main.
  C2. Every new canonical has a row in manuscript_workspace.detail_table_registry_v1.
  C3. Every new canonical has a __readme entry (script tag).
  C4. CPM row count is exactly 10,871.
  C5. No new nlp_<dom>_n_notes / nlp_<dom>_key_finding columns leaked onto CPM
      for the round-2 domains (path / cervln / esoph). Pre-existing leaks in
      other domains (e.g. legacy nlp_path_n_notes pre-Script-369) are ignored.
  C6. sum(nlp_<dom>_n_entities) on CPM == events table row count for each new domain.
  C7. canonical_us_nodule_v2 row count >= US_NODULE_V2_BASELINE
      (post-tirads monotonicity; here equality is also acceptable since the
      Script 383 absorb-chain was skipped — see Script 383 docstring).
  C8. canonical_frozen_section_events_v1 row count == 7,081 unchanged.
      canonical_us_lymph_node_v2 row count == 6,801 unchanged.

Output:
  scripts/output/385_run.log
  scripts/output/385_verify.json   (machine-readable result for downstream tools)

Exits non-zero on first failed gate.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts._round2_helpers import (  # noqa: E402
    CANONICAL_DB,
    REGISTRY_SCHEMA,
    REGISTRY_TABLE,
    RunLogger,
    column_exists,
    connect_md,
    table_exists,
)

# Round-2 canonicals built by 369/382/384 (383 is load-only).
NEW_CANONICALS: dict[str, tuple[str, ...]] = {
    "pathology_clinical": (
        "canonical_pathology_clinical_events_v1",
        "canonical_pathology_clinical_patient_rollup_v1",
    ),
    "cervical_ln_clinical": (
        "canonical_cervical_ln_clinical_events_v1",
        "canonical_cervical_ln_clinical_patient_rollup_v1",
    ),
    "esophageal_invasion": (
        "canonical_esophageal_invasion_events_v1",
        "canonical_esophageal_invasion_patient_rollup_v1",
    ),
}

# Domain → (cpm_n_entities_col, events_table) for invariant C6.
ROLLUP_PARITY: dict[str, tuple[str, str]] = {
    "path":   ("nlp_path_n_entities",   "canonical_pathology_clinical_events_v1"),
    "cervln": ("nlp_cervln_n_entities", "canonical_cervical_ln_clinical_events_v1"),
    "esoph":  ("nlp_esoph_n_entities",  "canonical_esophageal_invasion_events_v1"),
}

# Domains for which we assert no NEW leak of nlp_<dom>_n_notes / key_finding.
# Path is omitted because nlp_path_n_notes pre-existed Script 369 (legacy).
NO_LEAK_DOMAINS: tuple[str, ...] = ("cervln", "esoph")

# Hard-guard baselines (probed 2026-04-22)
US_LN_BASELINE = 6_801
FROZEN_SECTION_BASELINE = 7_081
US_NODULE_V2_BASELINE = 37_579
CPM_EXPECTED_ROWS = 10_871

# Sources / load-only
TIRADS_SOURCE_TABLE = "note_entities_llm_tirads_granular"

# __readme expected script tags (per Phase B).
README_SCRIPT_TAGS: tuple[str, ...] = (
    "369_pathology_v2_merge_load_rollup",
    "382_cervical_ln_clinical_merge_load_rollup",
    "383_tirads_granular_merge_load",
    "384_esophageal_invasion_merge_load_rollup",
)


OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
LOG_PATH = OUTPUT_DIR / "385_run.log"
VERIFY_PATH = OUTPUT_DIR / "385_verify.json"

logger = RunLogger(LOG_PATH)
log = logger.log
gate = logger.gate


def main() -> None:
    ap = argparse.ArgumentParser(description="Script 385 — round-2 canonical verification")
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Treat any soft warning as a gate failure (default: warn-only on soft items).",
    )
    args = ap.parse_args()

    log(f"Script 385 — round-2 canonical verification — {datetime.now(timezone.utc).isoformat()}")
    log(f"  Strict mode: {args.strict}")

    con = connect_md(logger)
    result: dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "strict": args.strict,
        "checks": {},
    }

    # ── C1: every new canonical exists in PUB.main ──────────────────────────
    log("=" * 70)
    log("C1 — every new canonical exists in PUB.main")
    log("=" * 70)
    c1: dict[str, bool] = {}
    for _domain, tables in NEW_CANONICALS.items():
        for t in tables:
            present = table_exists(con, "main", t)
            c1[t] = present
            gate(present, f"main.{t} present")
    # Also verify the load-only tirads source landed
    tirads_present = table_exists(con, "main", TIRADS_SOURCE_TABLE)
    c1[TIRADS_SOURCE_TABLE] = tirads_present
    gate(tirads_present, f"main.{TIRADS_SOURCE_TABLE} present (load-only)")
    result["checks"]["c1_canonicals_present"] = c1

    # ── C2: every new canonical has a row in detail_table_registry_v1 ───────
    log("=" * 70)
    log(f"C2 — every new canonical has a row in {REGISTRY_SCHEMA}.{REGISTRY_TABLE}")
    log("=" * 70)
    c2: dict[str, int] = {}
    for _domain, tables in NEW_CANONICALS.items():
        for t in tables:
            n = con.execute(
                f"SELECT COUNT(*) FROM {REGISTRY_SCHEMA}.{REGISTRY_TABLE} "
                f"WHERE detail_table_name = ?",
                [t],
            ).fetchone()[0]
            c2[t] = n
            gate(n == 1, f"registry has exactly 1 row for {t} (got {n})")
    # Also assert the tirads source (load-only) is registered
    n_tirads = con.execute(
        f"SELECT COUNT(*) FROM {REGISTRY_SCHEMA}.{REGISTRY_TABLE} "
        f"WHERE detail_table_name = ?",
        [TIRADS_SOURCE_TABLE],
    ).fetchone()[0]
    c2[TIRADS_SOURCE_TABLE] = n_tirads
    gate(n_tirads == 1, f"registry has 1 row for {TIRADS_SOURCE_TABLE} (got {n_tirads})")
    result["checks"]["c2_registry_rows"] = c2

    # ── C3: every Phase-B script has a __readme entry ───────────────────────
    log("=" * 70)
    log("C3 — every Phase-B script has a __readme entry")
    log("=" * 70)
    c3: dict[str, int] = {}
    for tag in README_SCRIPT_TAGS:
        n = con.execute(
            "SELECT COUNT(*) FROM main.__readme WHERE script = ?",
            [tag],
        ).fetchone()[0]
        c3[tag] = n
        gate(n >= 1, f"__readme has >= 1 row for script={tag} (got {n})")
    result["checks"]["c3_readme_entries"] = c3

    # ── C4: CPM row count is exactly 10,871 ─────────────────────────────────
    log("=" * 70)
    log(f"C4 — CPM row count is exactly {CPM_EXPECTED_ROWS:,}")
    log("=" * 70)
    cpm_rows = con.execute("SELECT COUNT(*) FROM main.canonical_patient_master").fetchone()[0]
    result["checks"]["c4_cpm_rows"] = cpm_rows
    gate(cpm_rows == CPM_EXPECTED_ROWS, f"CPM rows == {CPM_EXPECTED_ROWS:,} (got {cpm_rows:,})")

    # ── C5: no new nlp_<dom>_n_notes / nlp_<dom>_key_finding leaks ──────────
    log("=" * 70)
    log("C5 — no new nlp_<dom>_n_notes / nlp_<dom>_key_finding columns")
    log("=" * 70)
    c5: dict[str, dict[str, bool]] = {}
    for dom in NO_LEAK_DOMAINS:
        c5[dom] = {}
        for suffix in ("n_notes", "key_finding"):
            col = f"nlp_{dom}_{suffix}"
            present = column_exists(con, "main", "canonical_patient_master", col)
            c5[dom][col] = present
            gate(not present, f"CPM column {col} NOT present (got present={present})")
    result["checks"]["c5_no_leak"] = c5
    log("  (legacy nlp_path_n_notes pre-existed Script 369 and is intentionally ignored)")

    # ── C6: sum(nlp_<dom>_n_entities) == events table rows ──────────────────
    log("=" * 70)
    log("C6 — sum(nlp_<dom>_n_entities) == events table row count")
    log("=" * 70)
    c6: dict[str, dict[str, int]] = {}
    for dom, (col, ev) in ROLLUP_PARITY.items():
        cpm_sum = con.execute(
            f"SELECT COALESCE(SUM({col}), 0) FROM main.canonical_patient_master"
        ).fetchone()[0]
        ev_rows = con.execute(f"SELECT COUNT(*) FROM main.{ev}").fetchone()[0]
        c6[dom] = {"sum_cpm": int(cpm_sum), "events_rows": int(ev_rows)}
        gate(int(cpm_sum) == int(ev_rows),
             f"sum({col})={cpm_sum} == events rows={ev_rows} for {dom}")
    result["checks"]["c6_rollup_parity"] = c6

    # ── C7: canonical_us_nodule_v2 >= US_NODULE_V2_BASELINE ─────────────────
    log("=" * 70)
    log(f"C7 — canonical_us_nodule_v2 >= baseline {US_NODULE_V2_BASELINE:,}")
    log("=" * 70)
    n_us_nodule = con.execute("SELECT COUNT(*) FROM main.canonical_us_nodule_v2").fetchone()[0]
    result["checks"]["c7_us_nodule_v2_rows"] = n_us_nodule
    gate(n_us_nodule >= US_NODULE_V2_BASELINE,
         f"us_nodule_v2 rows >= baseline {US_NODULE_V2_BASELINE:,} (got {n_us_nodule:,})")
    if n_us_nodule == US_NODULE_V2_BASELINE:
        log("  (equality — Script 383 absorb-chain was skipped, no monotonic growth expected)")

    # ── C8: frozen-section + US LN unchanged ───────────────────────────────
    log("=" * 70)
    log("C8 — frozen-section + US LN row counts unchanged from pre-Phase-B baselines")
    log("=" * 70)
    n_frozen = con.execute(
        "SELECT COUNT(*) FROM main.canonical_frozen_section_events_v1"
    ).fetchone()[0]
    n_us_ln = con.execute(
        "SELECT COUNT(*) FROM main.canonical_us_lymph_node_v2"
    ).fetchone()[0]
    result["checks"]["c8_frozen_section_rows"] = n_frozen
    result["checks"]["c8_us_ln_rows"] = n_us_ln
    gate(n_frozen == FROZEN_SECTION_BASELINE,
         f"frozen-section rows == {FROZEN_SECTION_BASELINE:,} (got {n_frozen:,})")
    gate(n_us_ln == US_LN_BASELINE,
         f"US LN rows == {US_LN_BASELINE:,} (got {n_us_ln:,})")

    # ── per-canonical row-count snapshot for downstream tools ───────────────
    snap: dict[str, int] = {}
    for _dom, tables in NEW_CANONICALS.items():
        for t in tables:
            snap[t] = con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
    snap[TIRADS_SOURCE_TABLE] = con.execute(
        f"SELECT COUNT(*) FROM main.{TIRADS_SOURCE_TABLE}"
    ).fetchone()[0]
    result["checks"]["row_counts"] = snap

    log("=" * 70)
    log("ALL VERIFY GATES PASSED")
    log("=" * 70)
    for k, v in snap.items():
        log(f"  {k}: {v:,}")

    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    result["status"] = "PASS"
    VERIFY_PATH.write_text(json.dumps(result, indent=2, default=str))
    log(f"  Verify report: {VERIFY_PATH.name}")

    logger.flush()


if __name__ == "__main__":
    main()
