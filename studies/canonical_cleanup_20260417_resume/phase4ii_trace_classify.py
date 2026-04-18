"""Phase 4 (ii) — trace + classify the 80 path_tumor_size_cm > tumor_size_cm_max
violations. Strictly READ-ONLY on canonical_patient_master.

For each rid:
  1. Pull CPM dominant + max + all *_size_cm sibling values.
  2. Pull every tumor focus size from:
       - synoptic_tumor_long_v1 (size_greatest_dimension_cm per row)
       - tumor_episode_master_v2 (tumor_size_cm per row)
       - canonical_tumor_characteristics_v1 (size_greatest_dimension_cm per row)
       - path_synoptics (tumor_1_..._5_size_greatest_dimension_cm + lobe/goiter cols)
       - tumor_pathology (tumor_1_size_cm..tumor_5_size_cm + _source)
  3. Compute observed_max_tumor_focus = MAX of all per-tumor-focus values found
     across the 5 feeders.
  4. Compute observed_non_tumor_size_pool = the lobe/goiter/whole-gland values
     for that patient (these are anatomic measurements, not tumor focus sizes).
  5. Apply classifier:
       A — Unit/decimal error on path_tumor_size_cm: path ≈ 10× or 0.1×
           observed_max_tumor_focus.
       B — Wrong source for path_tumor_size_cm: path matches a non-tumor
           anatomic value (total_thyroid_size, substernal_goiter_size_cm,
            lobe sizes, isthmus, pyramidal) within ±0.1 cm, exceeds
           observed_max_tumor_focus.
       C — NLP contamination of path_tumor_size_cm: path > observed_max_tumor_focus
           AND path matches NO feeder value (tumor or anatomic).
       D — Multi-focus enumeration drift: path ≤ observed_max_tumor_focus
           AND path matches some tumor focus, max comes from a different
           focus pick; small delta.
       E — Unresolvable from structured data.
       F — tumor_size_cm_max under-reports: path matches the highest tumor
           focus across feeders (correct), but tumor_size_cm_max is
           aggregating from an incomplete feeder set (typically misses a
           second surgery's tumor). The broken column is
           tumor_size_cm_max, NOT path_tumor_size_cm.
  6. Write studies/canonical_cleanup_20260417_resume/phase4ii_classification.csv
     and phase4ii_classification.md with full per-rid trace + classification.

NO writes to canonical_patient_master. NO writes to any main.* table.
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore  # noqa: E402

LOG = HERE / "phase4ii_run.log"
CSV_PATH = HERE / "phase4ii_classification.csv"
JSON_PATH = HERE / "phase4ii_classification.json"
MD_PATH = HERE / "phase4ii_classification.md"

EPS = 0.10            # cm tolerance for value-equality matches
RATIO_TOL = 0.05      # 5% tolerance around 10x / 0.1x for unit detection
SMALL_DELTA = 1.0     # cm threshold for "small" (D-eligible) deltas


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG.open("a") as f:
        f.write(line + "\n")


def fetch_dicts(con, sql: str, params=None) -> list[dict]:
    cur = con.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def to_float(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (ValueError, TypeError):
        return None
    if f != f:  # NaN
        return None
    return f


def near(a: float, b: float, tol: float = EPS) -> bool:
    return abs(a - b) <= tol


def main() -> int:
    LOG.write_text("")
    log("Phase 4 (ii) trace + classify starting (read-only on CPM)...")
    con = connect_locked()

    # 1) Load the 80 violation rids (BIGINT and as VARCHAR; CPM rid is VARCHAR)
    viols = fetch_dicts(
        con,
        """
        SELECT
          research_id,
          path_tumor_size_cm,
          tumor_size_cm_max,
          ROUND(path_tumor_size_cm - tumor_size_cm_max, 2) AS delta_cm
        FROM manuscript_workspace.path_tumor_size_invariant_v1
        ORDER BY delta_cm DESC
        """,
    )
    log(f"  {len(viols)} violation rids loaded")
    if len(viols) == 0:
        log("  nothing to classify; exiting.")
        return 0

    rid_int_csv = ",".join(str(int(v["research_id"])) for v in viols)
    rid_str_csv = ",".join(f"'{v['research_id']}'" for v in viols)

    # 2) CPM sibling cols (DOUBLE only, plus reasonable VARCHAR ones cast)
    log("  loading CPM size siblings for the 80 rids...")
    cpm = fetch_dicts(
        con,
        f"""
        SELECT
          research_id,
          path_tumor_size_cm,
          tumor_size_cm_max,
          tumor_size_cm_dominant,
          tumor_size_cm_min,
          tumor_size_cm_mean,
          tumor_size_cm_sum,
          dominant_nodule_size_cm,
          preop_imaging_size_cm,
          TRY_CAST(syn_isthmus_size_cm AS DOUBLE) AS syn_isthmus_size_cm,
          TRY_CAST(syn_left_lobe_size_cm AS DOUBLE) AS syn_left_lobe_size_cm,
          TRY_CAST(syn_right_lobe_size_cm AS DOUBLE) AS syn_right_lobe_size_cm,
          TRY_CAST(syn_tumor2_size_cm AS DOUBLE) AS syn_tumor2_size_cm,
          n_tumors_path,
          multifocal_flag_path IS NOT NULL AS multifocal_flag_path_present
        FROM main.canonical_patient_master
        WHERE research_id IN ({rid_str_csv})
        """,
    )
    cpm_by_rid = {str(c["research_id"]): c for c in cpm}

    # 3) synoptic_tumor_long_v1 — per-tumor focus rows (rid is BIGINT)
    log("  pulling synoptic_tumor_long_v1...")
    syn_rows = fetch_dicts(
        con,
        f"""
        SELECT research_id, surg_date, tumor_index, source_table,
               source_path_file, size_greatest_dimension_cm,
               histologic_type
        FROM main.synoptic_tumor_long_v1
        WHERE research_id IN ({rid_int_csv})
        """,
    )
    syn_by_rid: dict[str, list[dict]] = {}
    for r in syn_rows:
        syn_by_rid.setdefault(str(r["research_id"]), []).append(r)

    # 4) tumor_episode_master_v2 — per-tumor focus rows
    log("  pulling tumor_episode_master_v2...")
    tem_rows = fetch_dicts(
        con,
        f"""
        SELECT research_id, surgery_episode_id, tumor_ordinal, surgery_date,
               tumor_size_cm, primary_histology, source_tables
        FROM main.tumor_episode_master_v2
        WHERE research_id IN ({rid_int_csv})
        """,
    )
    tem_by_rid: dict[str, list[dict]] = {}
    for r in tem_rows:
        tem_by_rid.setdefault(str(r["research_id"]), []).append(r)

    # 5) canonical_tumor_characteristics_v1 — per-tumor rows
    log("  pulling canonical_tumor_characteristics_v1...")
    ctc_rows = fetch_dicts(
        con,
        f"""
        SELECT research_id, size_greatest_dimension_cm, tumor_size_cm_per_surgery
        FROM main.canonical_tumor_characteristics_v1
        WHERE research_id IN ({rid_int_csv})
        """,
    )
    ctc_by_rid: dict[str, list[dict]] = {}
    for r in ctc_rows:
        ctc_by_rid.setdefault(str(r["research_id"]), []).append(r)

    # 6) path_synoptics — wide per-patient (or per-specimen?) rows with tumor_1..5
    log("  pulling path_synoptics...")
    ps_rows = fetch_dicts(
        con,
        f"""
        SELECT research_id,
               tumor_1_size_greatest_dimension_cm,
               tumor_2_size_greatest_dimension_cm,
               tumor_3_size_greatest_dimension_cm,
               tumor_4_size_greatest_dimension_cm,
               tumor_5_size_greatest_dimension_cm,
               tumor_1_size_of_largest_metastatic_deposit_cm,
               total_thyroid_size,
               substernal_goiter_size_cm,
               isthmus_size_cm,
               ll_size_cm,
               rl_size_cm,
               pyramidal_lobe_cm
        FROM main.path_synoptics
        WHERE research_id IN ({rid_int_csv})
        """,
    )
    ps_by_rid: dict[str, list[dict]] = {}
    for r in ps_rows:
        ps_by_rid.setdefault(str(r["research_id"]), []).append(r)

    # 7) tumor_pathology — tumor_1..5_size_cm + _source (rid type?)
    log("  pulling tumor_pathology...")
    tp_rows = fetch_dicts(
        con,
        f"""
        SELECT research_id,
               tumor_1_size_cm, tumor_1_size_source,
               tumor_2_size_cm, tumor_2_size_source,
               tumor_3_size_cm, tumor_3_size_source,
               tumor_4_size_cm, tumor_4_size_source,
               tumor_5_size_cm, tumor_5_size_source
        FROM main.tumor_pathology
        WHERE research_id IN ({rid_int_csv})
        """,
    )
    tp_by_rid: dict[str, list[dict]] = {}
    for r in tp_rows:
        tp_by_rid.setdefault(str(r["research_id"]), []).append(r)

    # ---- Classify per rid ----
    log("Classifying...")
    out_rows: list[dict] = []
    bucket_counts = {"A": 0, "B": 0, "C": 0, "D": 0, "E": 0, "F": 0}

    for v in viols:
        rid = str(v["research_id"])
        path_v = to_float(v["path_tumor_size_cm"])
        max_v = to_float(v["tumor_size_cm_max"])
        delta = (path_v or 0) - (max_v or 0)

        # Collect all tumor-focus sizes from all feeders
        tumor_focus_sizes: list[tuple[float, str, str]] = []  # (value, feeder, label)
        for r in syn_by_rid.get(rid, []):
            x = to_float(r.get("size_greatest_dimension_cm"))
            if x is not None:
                tumor_focus_sizes.append(
                    (
                        x,
                        "synoptic_tumor_long_v1",
                        f"tumor_index={r.get('tumor_index')}",
                    )
                )
        for r in tem_by_rid.get(rid, []):
            x = to_float(r.get("tumor_size_cm"))
            if x is not None:
                tumor_focus_sizes.append(
                    (
                        x,
                        "tumor_episode_master_v2",
                        f"surg={r.get('surgery_episode_id')},ord={r.get('tumor_ordinal')}",
                    )
                )
        for r in ctc_by_rid.get(rid, []):
            x = to_float(r.get("size_greatest_dimension_cm"))
            if x is not None:
                tumor_focus_sizes.append(
                    (x, "canonical_tumor_characteristics_v1", "")
                )
            x = to_float(r.get("tumor_size_cm_per_surgery"))
            if x is not None:
                tumor_focus_sizes.append(
                    (x, "canonical_tumor_characteristics_v1.per_surgery", "")
                )
        for r in ps_by_rid.get(rid, []):
            for col in (
                "tumor_1_size_greatest_dimension_cm",
                "tumor_2_size_greatest_dimension_cm",
                "tumor_3_size_greatest_dimension_cm",
                "tumor_4_size_greatest_dimension_cm",
                "tumor_5_size_greatest_dimension_cm",
            ):
                x = to_float(r.get(col))
                if x is not None:
                    tumor_focus_sizes.append((x, "path_synoptics", col))
        for r in tp_by_rid.get(rid, []):
            for i in range(1, 6):
                x = to_float(r.get(f"tumor_{i}_size_cm"))
                if x is not None:
                    src = r.get(f"tumor_{i}_size_source")
                    tumor_focus_sizes.append(
                        (
                            x,
                            "tumor_pathology",
                            f"tumor_{i}_size_cm src={src}",
                        )
                    )

        observed_max_tumor = (
            max(s[0] for s in tumor_focus_sizes) if tumor_focus_sizes else None
        )

        # Anatomic-only values (lobes, isthmus, goiter, total) — these would
        # signal a B (wrong-source) match if path_v == one of them.
        anatomic_pool: list[tuple[float, str]] = []
        for r in ps_by_rid.get(rid, []):
            for col in (
                "total_thyroid_size",
                "substernal_goiter_size_cm",
                "isthmus_size_cm",
                "ll_size_cm",
                "rl_size_cm",
                "pyramidal_lobe_cm",
                "tumor_1_size_of_largest_metastatic_deposit_cm",
            ):
                x = to_float(r.get(col))
                if x is not None:
                    anatomic_pool.append((x, f"path_synoptics.{col}"))
        c = cpm_by_rid.get(rid) or {}
        for col in (
            "syn_isthmus_size_cm",
            "syn_left_lobe_size_cm",
            "syn_right_lobe_size_cm",
        ):
            x = to_float(c.get(col))
            if x is not None:
                anatomic_pool.append((x, f"cpm.{col}"))

        # Classifier
        bucket = "E"
        evidence = ""
        proposed_corrected_value = None
        proposed_corrected_source = None

        # Cross-feeder summary stats useful for F-bucket detection
        feeder_max_per_table: dict[str, float] = {}
        for v_, feeder, _label in tumor_focus_sizes:
            if (
                feeder not in feeder_max_per_table
                or v_ > feeder_max_per_table[feeder]
            ):
                feeder_max_per_table[feeder] = v_
        # If TEM has multiple distinct surgery_episode_ids → multi-surgery
        tem_surg_ids = {
            tem.get("surgery_episode_id")
            for tem in tem_by_rid.get(rid, [])
            if tem.get("surgery_episode_id") is not None
        }

        if path_v is None or max_v is None:
            bucket = "E"
            evidence = "path_tumor_size_cm or tumor_size_cm_max is NULL"
        else:
            # F — tumor_size_cm_max under-reports (the BROKEN column is max,
            # path is correct). Test FIRST because it's the dominant pattern.
            if (
                observed_max_tumor is not None
                and near(path_v, observed_max_tumor)
                and max_v < observed_max_tumor - EPS
            ):
                bucket = "F"
                # Identify which feeder(s) saw the larger value (= path) and
                # which feeder(s) tumor_size_cm_max appears to have come from.
                feeders_with_path = sorted({
                    s[1] for s in tumor_focus_sizes if near(path_v, s[0])
                })
                feeders_with_max = sorted({
                    s[1] for s in tumor_focus_sizes if near(max_v, s[0])
                })
                evidence = (
                    f"path={path_v} matches the HIGHEST tumor focus across "
                    f"feeders ({observed_max_tumor}); tumor_size_cm_max="
                    f"{max_v} corresponds to a smaller focus seen only in "
                    f"{feeders_with_max or 'unknown subset'}; "
                    f"larger focus appears in {feeders_with_path}; "
                    f"n_distinct_TEM_surgeries={len(tem_surg_ids)}. "
                    "tumor_size_cm_max is aggregating from an incomplete "
                    "feeder set (likely missing a second-surgery focus)."
                )
                proposed_corrected_value = observed_max_tumor
                proposed_corrected_source = (
                    "tumor_size_cm_max should be re-aggregated from the "
                    "TEM rollup (or whatever feeder produced "
                    f"observed_max={observed_max_tumor})"
                )

            # A — unit/decimal error on path_tumor_size_cm
            if bucket == "E" and observed_max_tumor is not None and observed_max_tumor > 0:
                ratio = path_v / observed_max_tumor
                if abs(ratio - 10.0) / 10.0 <= RATIO_TOL:
                    bucket = "A"
                    evidence = (
                        f"path={path_v} ≈ 10× observed_max_tumor_focus="
                        f"{observed_max_tumor:.2f} → likely decimal-point "
                        f"shift (units/typo)"
                    )
                    proposed_corrected_value = round(path_v / 10, 2)
                    proposed_corrected_source = "decimal_shift_correction"
                elif abs(ratio - 0.1) / 0.1 <= RATIO_TOL:
                    bucket = "A"
                    evidence = (
                        f"path={path_v} ≈ 0.1× observed_max_tumor_focus="
                        f"{observed_max_tumor:.2f} → likely decimal-point "
                        f"shift (units/typo)"
                    )
                    proposed_corrected_value = round(path_v * 10, 2)
                    proposed_corrected_source = "decimal_shift_correction"

            # B — wrong source: path matches an anatomic (non-tumor) value
            if bucket == "E":
                for ax, label in anatomic_pool:
                    if near(path_v, ax) and (
                        observed_max_tumor is None or path_v > observed_max_tumor
                    ):
                        bucket = "B"
                        evidence = (
                            f"path={path_v} matches non-tumor anatomic "
                            f"value {label}={ax} (within ±{EPS}); "
                            f"observed_max_tumor_focus="
                            f"{observed_max_tumor}"
                        )
                        proposed_corrected_value = (
                            observed_max_tumor
                            if observed_max_tumor is not None
                            else None
                        )
                        proposed_corrected_source = (
                            "re-rollup from tumor-focus only"
                        )
                        break

            # D — small-delta multi-focus enumeration drift: path matches
            # SOME tumor focus in feeders, but max comes from a different one
            if bucket == "E":
                path_matches_focus = any(
                    near(path_v, s[0]) for s in tumor_focus_sizes
                )
                if (
                    abs(delta) <= SMALL_DELTA
                    and path_matches_focus
                ):
                    bucket = "D"
                    matched = [
                        f"{s[1]}({s[2]})={s[0]}"
                        for s in tumor_focus_sizes
                        if near(path_v, s[0])
                    ]
                    evidence = (
                        f"small Δ={delta:.2f}cm; path={path_v} matches "
                        f"focus(es): {matched[:3]}; max picker chose a "
                        f"different focus → multi-focus enumeration drift"
                    )

            # C — NLP contamination: path doesn't match any feeder value
            # (tumor or anatomic) AND path > observed_max_tumor_focus AND
            # delta is non-trivial (otherwise D)
            if bucket == "E":
                in_tumor = any(near(path_v, s[0]) for s in tumor_focus_sizes)
                in_anat = any(near(path_v, ax) for ax, _ in anatomic_pool)
                if (
                    not in_tumor
                    and not in_anat
                    and observed_max_tumor is not None
                    and path_v > observed_max_tumor + EPS
                    and abs(delta) > SMALL_DELTA
                ):
                    bucket = "C"
                    evidence = (
                        f"path={path_v} matches NO synoptic/path/TEM/CTC "
                        f"tumor focus value AND no anatomic value; "
                        f"observed_max_tumor_focus={observed_max_tumor}; "
                        f"likely NLP / free-text contamination"
                    )
                    proposed_corrected_value = (
                        observed_max_tumor
                        if observed_max_tumor is not None
                        else None
                    )
                    proposed_corrected_source = "synoptic-sourced replacement"

            # E — fallback
            if bucket == "E":
                evidence = (
                    f"no clean classifier match: "
                    f"path={path_v}, max={max_v}, delta={delta:.2f}, "
                    f"observed_max_tumor_focus={observed_max_tumor}, "
                    f"n_tumor_focus_values={len(tumor_focus_sizes)}, "
                    f"n_anatomic_values={len(anatomic_pool)}"
                )

        bucket_counts[bucket] += 1

        out_rows.append(
            {
                "research_id": rid,
                "path_tumor_size_cm": path_v,
                "tumor_size_cm_max": max_v,
                "delta_cm": round(delta, 2),
                "delta_band": (
                    "extreme(>5)"
                    if abs(delta) > 5
                    else ("moderate(1<Δ≤5)" if abs(delta) > 1 else "small(≤1)")
                ),
                "n_tumor_focus_values": len(tumor_focus_sizes),
                "observed_max_tumor_focus": observed_max_tumor,
                "n_anatomic_values": len(anatomic_pool),
                "anatomic_pool": json.dumps(
                    [{"v": ax, "label": lbl} for ax, lbl in anatomic_pool],
                    default=str,
                ),
                "tumor_focus_dump": json.dumps(
                    [
                        {"v": s[0], "feeder": s[1], "label": s[2]}
                        for s in tumor_focus_sizes
                    ],
                    default=str,
                ),
                "bucket": bucket,
                "evidence": evidence,
                "proposed_corrected_value": proposed_corrected_value,
                "proposed_corrected_source": proposed_corrected_source,
            }
        )

    log(f"Bucket counts: {bucket_counts}")

    # CSV
    fieldnames = list(out_rows[0].keys())
    with CSV_PATH.open("w", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)
    log(f"CSV -> {CSV_PATH}")

    JSON_PATH.write_text(json.dumps(out_rows, indent=2, default=str))
    log(f"JSON -> {JSON_PATH}")

    # Markdown summary
    md = ["# Phase 4 (ii) — 80-rid invariant violation classification", ""]
    md.append(f"_Generated {datetime.now(timezone.utc).isoformat()}_  ")
    md.append(
        "_Strictly read-only on `canonical_patient_master`. No size values "
        "modified._  "
    )
    md.append("")
    md.append("## Bucket counts")
    md.append("")
    md.append("| bucket | description | broken column | n |")
    md.append("|:---|:---|:---|---:|")
    md.append(f"| **A** | Unit / decimal error (10× or 0.1× ratio) | path_tumor_size_cm | {bucket_counts['A']} |")
    md.append(f"| **B** | Wrong source (matches anatomic / non-tumor value) | path_tumor_size_cm | {bucket_counts['B']} |")
    md.append(f"| **C** | NLP / free-text contamination (no feeder match) | path_tumor_size_cm | {bucket_counts['C']} |")
    md.append(f"| **D** | Multi-focus enumeration drift (small Δ, path matches a focus) | neither (semantics) | {bucket_counts['D']} |")
    md.append(f"| **E** | Unresolvable from structured data | unknown | {bucket_counts['E']} |")
    md.append(f"| **F** | tumor_size_cm_max under-reports (incomplete feeder set, multi-surgery) | tumor_size_cm_max | {bucket_counts['F']} |")
    md.append(f"| **TOTAL** | | | {sum(bucket_counts.values())} |")
    md.append("")
    md.append("## Cross-tab: bucket × delta band")
    md.append("")
    crosstab = {b: {"extreme(>5)": 0, "moderate(1<Δ≤5)": 0, "small(≤1)": 0} for b in "ABCDEF"}
    for r in out_rows:
        crosstab[r["bucket"]][r["delta_band"]] += 1
    md.append("| bucket | extreme(>5) | moderate(1<Δ≤5) | small(≤1) |")
    md.append("|:---|---:|---:|---:|")
    for b in "ABCDEF":
        md.append(
            f"| {b} | {crosstab[b]['extreme(>5)']} | "
            f"{crosstab[b]['moderate(1<Δ≤5)']} | {crosstab[b]['small(≤1)']} |"
        )
    md.append("")
    md.append("## Per-bucket samples (up to 10 each)")
    md.append("")
    queue_map = {
        "A": "CORRECTION queue (path_tumor_size_cm)",
        "B": "CORRECTION queue (path_tumor_size_cm)",
        "C": "CORRECTION queue (path_tumor_size_cm)",
        "D": "multifocal_notes",
        "E": "chart_review_queue",
        "F": "CORRECTION queue (tumor_size_cm_max — re-aggregate)",
    }
    for b in "ABCDEF":
        rows = [r for r in out_rows if r["bucket"] == b]
        if not rows:
            continue
        md.append(
            f"### Bucket {b} (n={len(rows)}, recommend → {queue_map[b]})"
        )
        md.append("")
        md.append(
            "| rid | path | max | Δ | observed_max_tumor_focus | "
            "evidence (truncated) | proposed_corrected_value |"
        )
        md.append("|---:|---:|---:|---:|---:|:---|---:|")
        for r in rows[:10]:
            ev = (r["evidence"] or "")[:120].replace("|", "\\|")
            md.append(
                f"| {r['research_id']} | {r['path_tumor_size_cm']} | "
                f"{r['tumor_size_cm_max']} | {r['delta_cm']} | "
                f"{r['observed_max_tumor_focus']} | {ev} | "
                f"{r['proposed_corrected_value'] or ''} |"
            )
        md.append("")

    md.append(
        "_Full per-rid trace_: "
        "[`phase4ii_classification.csv`](./phase4ii_classification.csv) "
        "(JSON variant alongside)"
    )
    MD_PATH.write_text("\n".join(md) + "\n")
    log(f"MD -> {MD_PATH}")

    # CPM invariant
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        raise SystemExit("CPM invariant regressed!")
    log(f"  CPM invariant re-asserted: {n_rows}/{n_distinct}")
    log("Phase 4 (ii) trace + classify complete (read-only).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
