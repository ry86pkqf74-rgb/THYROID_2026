"""Preflight + drift report for canonical cleanup 20260417.

Connects via _md_connect.connect_locked() (which itself asserts CPM=10871),
then runs assertions (a)-(i) from the prompt, plus a drift report,
plus the two pre-state snapshots.

Read-only except for:
  - studies/canonical_cleanup_20260417/cpm_cols_pre.txt (file)
  - manuscript_workspace.view_definitions_snapshot_bigcleanup (snapshot table
    in the canonical publication DB only)

Fail-closed: any assert failure raises SystemExit.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

OUT_DIR = HERE
LOG_PATH = OUT_DIR / "preflight.log"
DRIFT_PATH = OUT_DIR / "drift_report.md"
PRE_COLS_PATH = OUT_DIR / "cpm_cols_pre.txt"
PRE_JSON_PATH = OUT_DIR / "preflight.json"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("")  # truncate

    log("Opening locked MotherDuck connection...")
    con = connect_locked()
    log("Connection ok; CPM invariant (10,871 / distinct 10,871) confirmed by connect_locked().")

    results: dict = {"checks": {}, "drift": {}, "snapshots": {}}
    failures: list[str] = []

    def chk(key: str, ok: bool, observed, expected, note: str = "") -> None:
        results["checks"][key] = {
            "ok": ok,
            "observed": observed,
            "expected": expected,
            "note": note,
        }
        status = "PASS" if ok else "FAIL"
        log(f"  [{status}] {key}: observed={observed!r} expected={expected!r} {note}")
        if not ok:
            failures.append(key)

    # (a) and (b) - already enforced by connect_locked, but record them
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    chk("a_cpm_rowcount", n_rows == 10871, n_rows, 10871)
    chk("b_cpm_distinct_research_id", n_distinct == 10871, n_distinct, 10871)

    # (c) Required CPM columns
    cpm_cols = [
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master' "
            "ORDER BY ordinal_position"
        ).fetchall()
    ]
    PRE_COLS_PATH.write_text("\n".join(cpm_cols) + "\n")
    log(f"  cpm_cols_pre.txt written ({len(cpm_cols)} columns)")
    chk(
        "c_col_r_class_true",
        "r_class_true" in cpm_cols,
        "r_class_true" in cpm_cols,
        True,
    )
    chk(
        "c_col_ete_grade_final_v2",
        "ete_grade_final_v2" in cpm_cols,
        "ete_grade_final_v2" in cpm_cols,
        True,
    )

    # (d) operative_episode_detail_v2 ~ 9371 +/- 5
    n_oed = con.execute(
        "SELECT COUNT(*) FROM main.operative_episode_detail_v2"
    ).fetchone()[0]
    chk(
        "d_operative_episode_detail_v2_rows",
        9366 <= n_oed <= 9376,
        n_oed,
        "9371 +/- 5",
    )

    # (e) complication_phenotype_v1 ~ 5978 +/- 50
    n_cp = con.execute(
        "SELECT COUNT(*) FROM main.complication_phenotype_v1"
    ).fetchone()[0]
    chk(
        "e_complication_phenotype_v1_rows",
        5928 <= n_cp <= 6028,
        n_cp,
        "5978 +/- 50",
    )

    # (f) canonical_us_nodule_characteristics_v1 has tirads_reported AND tirads_acr_recalculated
    us_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_us_nodule_characteristics_v1'"
        ).fetchall()
    }
    chk(
        "f_us_tirads_columns",
        {"tirads_reported", "tirads_acr_recalculated"}.issubset(us_cols),
        sorted(us_cols & {"tirads_reported", "tirads_acr_recalculated"}),
        ["tirads_reported", "tirads_acr_recalculated"],
    )

    # (g) thyroglobulin_lab_canonical_v1 row count
    n_tg = con.execute(
        "SELECT COUNT(*) FROM main.thyroglobulin_lab_canonical_v1"
    ).fetchone()[0]
    chk(
        "g_thyroglobulin_lab_canonical_v1_rows",
        73758 <= n_tg <= 74758,
        n_tg,
        "74258 +/- 500",
    )

    # (h) manuscript_workspace.vc_paralysis_recalibration_v236 exists
    n_v236 = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='manuscript_workspace' "
        "AND table_name='vc_paralysis_recalibration_v236'"
    ).fetchone()[0]
    chk(
        "h_vc_paralysis_recalibration_v236_exists",
        n_v236 == 1,
        n_v236,
        1,
    )

    # (i) "Thyroid 2026 UPdated".archive_pub_v1_0 schema exists with >100 tables
    n_arch = con.execute(
        'SELECT COUNT(*) FROM information_schema.tables '
        "WHERE table_catalog='Thyroid 2026 UPdated' "
        "AND table_schema='archive_pub_v1_0'"
    ).fetchone()[0]
    chk(
        "i_archive_pub_v1_0_table_count",
        n_arch > 100,
        n_arch,
        "> 100",
    )

    # ---------- DRIFT REPORT ----------
    log("Computing drift vs PROMPT_18 / PART2 cited counts...")
    drift_rows = []

    cited = {
        "canonical_patient_master": (10871, n_rows),
        "operative_episode_detail_v2": (9371, n_oed),
        "complication_phenotype_v1": (5978, n_cp),
        "thyroglobulin_lab_canonical_v1": (76971, n_tg),  # original prompt cited 76971
    }
    for tbl, (cited_n, observed_n) in cited.items():
        delta = observed_n - cited_n
        flag = "OK" if abs(delta) <= 50 else ("DRIFT" if abs(delta) <= 5000 else "BIG_DRIFT")
        drift_rows.append((tbl, cited_n, observed_n, delta, flag))
        results["drift"][tbl] = {
            "cited": cited_n,
            "observed": observed_n,
            "delta": delta,
            "flag": flag,
        }

    # Additional important counts referenced in fix SQL
    extra_counts = {
        "fna_episode_master_v2": "SELECT COUNT(*) FROM main.fna_episode_master_v2",
        "rai_treatment_episode_v2": "SELECT COUNT(*) FROM main.rai_treatment_episode_v2",
        "canonical_us_nodule_characteristics_v1": "SELECT COUNT(*) FROM main.canonical_us_nodule_characteristics_v1",
        "synoptic_tumor_long_v1": "SELECT COUNT(*) FROM main.synoptic_tumor_long_v1",
    }
    extras: dict = {}
    for tbl, q in extra_counts.items():
        try:
            extras[tbl] = con.execute(q).fetchone()[0]
        except Exception as e:  # noqa: BLE001
            extras[tbl] = f"ERR: {e}"
    results["extras"] = extras

    # Probe ground-truth for FNA episode-count distribution (Phase 1.7 expects
    # 11 = 2 patients, 12 = 3 patients).
    try:
        fna_dist = con.execute(
            """
            WITH per_patient AS (
              SELECT research_id, COUNT(*) AS n
              FROM main.fna_episode_master_v2
              GROUP BY 1
            )
            SELECT n, COUNT(*) AS n_patients
            FROM per_patient
            WHERE n >= 10
            GROUP BY 1 ORDER BY 1
            """
        ).fetchall()
        results["fna_episode_distribution_n_ge_10"] = [
            {"n_episodes": r[0], "n_patients": r[1]} for r in fna_dist
        ]
    except Exception as e:  # noqa: BLE001
        results["fna_episode_distribution_n_ge_10"] = f"ERR: {e}"

    # Probe count of CPM patients with n_fna_episodes IN (11,12) (the broadcast cluster)
    if "n_fna_episodes" in cpm_cols:
        try:
            n_cluster = con.execute(
                "SELECT COUNT(*) FROM main.canonical_patient_master "
                "WHERE n_fna_episodes IN (11, 12)"
            ).fetchone()[0]
        except Exception as e:  # noqa: BLE001
            n_cluster = f"ERR: {e}"
    else:
        n_cluster = "n_fna_episodes column absent"
    results["cpm_n_fna_in_11_12"] = n_cluster

    # Probe ajcc8_t_stage column presence (Phase 4.6)
    results["ajcc8_columns_present"] = {
        c: (c in cpm_cols)
        for c in ("ajcc8_t_stage", "ajcc8_t_stage_v2", "ajcc8_t_stage_corrected")
    }

    # Probe path_multifocal_flag rename status (Phase 4.1)
    results["multifocal_columns_present"] = {
        c: (c in cpm_cols)
        for c in (
            "multifocal_flag_path",
            "DEPRECATED__path_multifocal_flag",
            "path_multifocal_flag",
            "nlp_path_multifocal_mentioned",
        )
    }

    # Probe Phase 1.8 columns (per-entity comp_*_confirmed)
    results["complication_columns_present"] = {
        c: (c in cpm_cols)
        for c in (
            "comp_vc_paralysis_confirmed",
            "comp_vc_paresis_confirmed",
            "comp_hematoma_confirmed",
            "comp_seroma_confirmed",
            "comp_chyle_leak_confirmed",
            "comp_wound_infection_confirmed",
            "any_confirmed_complication_flag",
            "comp_hypoparathyroidism_permanent",
            "comp_hypopara_permanent_source",
            "lateral_neck_dissected",
            "lateral_neck_dissected_structured_or_nlp",
            "cpm_built_at",
            "worst_bethesda_source",
            "rai_max_dose_mci",
            "rai_dose_v9",
            "n_fna_episodes",
            "max_tirads_ever",
        )
    }

    # manuscript_workspace view count
    n_views = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
        "AND table_schema='manuscript_workspace' AND table_type='VIEW'"
    ).fetchone()[0]
    results["manuscript_workspace_view_count"] = n_views
    log(f"  manuscript_workspace VIEW count: {n_views} (expected 65)")

    # ---------- SNAPSHOTS (only if all asserts passed) ----------
    if not failures:
        log("All preflight assertions PASSED. Taking pre-state snapshots...")
        # Snapshot view definitions for manuscript_workspace
        con.execute("DROP TABLE IF EXISTS manuscript_workspace.view_definitions_snapshot_bigcleanup")
        con.execute(
            """
            CREATE TABLE manuscript_workspace.view_definitions_snapshot_bigcleanup AS
            SELECT
              table_catalog,
              table_schema,
              table_name,
              view_definition,
              CURRENT_TIMESTAMP AS snapshot_ts
            FROM information_schema.views
            WHERE table_catalog='thyroid_canonical_publication_v1_0'
              AND table_schema='manuscript_workspace'
            """
        )
        n_snap = con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.view_definitions_snapshot_bigcleanup"
        ).fetchone()[0]
        results["snapshots"]["view_definitions_snapshot_bigcleanup_rows"] = n_snap
        log(f"  view_definitions_snapshot_bigcleanup: {n_snap} rows captured")
    else:
        log(f"FAILURES: {failures} -- skipping snapshots; do NOT proceed to Phase 1.")
        results["snapshots"]["skipped_due_to_failures"] = failures

    # ---------- DRIFT REPORT FILE ----------
    lines = ["# Drift report (preflight) — canonical cleanup 20260417", ""]
    lines.append(
        f"_Generated {datetime.now(timezone.utc).isoformat()}; database="
        "`thyroid_canonical_publication_v1_0`._\n"
    )
    lines.append("## Cited vs observed counts\n")
    lines.append("| Object | Cited (markdowns) | Observed (live) | Delta | Flag |")
    lines.append("|---|---:|---:|---:|---|")
    for tbl, cited_n, observed_n, delta, flag in drift_rows:
        lines.append(f"| `{tbl}` | {cited_n:,} | {observed_n:,} | {delta:+,} | {flag} |")
    lines.append("")
    lines.append("## Additional pre-state row counts\n")
    lines.append("| Object | Rows |")
    lines.append("|---|---:|")
    for tbl, v in extras.items():
        if isinstance(v, int):
            lines.append(f"| `{tbl}` | {v:,} |")
        else:
            lines.append(f"| `{tbl}` | {v} |")
    lines.append("")
    lines.append("## Phase-relevant column presence on canonical_patient_master\n")
    for k in ("ajcc8_columns_present", "multifocal_columns_present", "complication_columns_present"):
        lines.append(f"### {k}")
        for c, present in results[k].items():
            lines.append(f"- `{c}`: {'present' if present else 'absent'}")
        lines.append("")
    lines.append("## FNA episode-count distribution (n >= 10)\n")
    if isinstance(results["fna_episode_distribution_n_ge_10"], list):
        lines.append("| n_episodes | n_patients |")
        lines.append("|---:|---:|")
        for r in results["fna_episode_distribution_n_ge_10"]:
            lines.append(f"| {r['n_episodes']} | {r['n_patients']} |")
    else:
        lines.append(str(results["fna_episode_distribution_n_ge_10"]))
    lines.append("")
    lines.append(f"CPM patients currently with n_fna_episodes IN (11,12): **{n_cluster}**")
    lines.append(f"\nmanuscript_workspace VIEW count: **{n_views}** (expected 65)\n")
    lines.append("## Preflight assertions\n")
    lines.append("| Key | Status | Observed | Expected |")
    lines.append("|---|---|---|---|")
    for key, c in results["checks"].items():
        lines.append(
            f"| `{key}` | {'PASS' if c['ok'] else 'FAIL'} | {c['observed']!r} | {c['expected']!r} |"
        )
    lines.append("")
    lines.append("## Result\n")
    if failures:
        lines.append(f"**STOP** — {len(failures)} preflight failure(s): {failures}")
    else:
        lines.append("**All preflight assertions PASSED.**")
    DRIFT_PATH.write_text("\n".join(lines) + "\n")

    PRE_JSON_PATH.write_text(json.dumps(results, indent=2, default=str))
    log(f"Drift report → {DRIFT_PATH}")
    log(f"JSON results → {PRE_JSON_PATH}")
    log(f"CPM columns → {PRE_COLS_PATH}")

    if failures:
        log("PREFLIGHT FAILED — do NOT begin Phase 1.")
        return 2
    log("PREFLIGHT PASSED — awaiting go/no-go decision before Phase 1.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
