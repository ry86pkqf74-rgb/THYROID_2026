#!/usr/bin/env python3
"""Probe phase for US/TIRADS cleanup follow-up. Read-only inventory."""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB

# Tables we want to verify/drop in this pass (full list across phases)
TARGETS = {
    # Phase 1 (already archived; drop after verify)
    "main.canonical_us_nodule_master_v1": "phase1",
    "main.canonical_us_nodule_characteristics_v1": "phase1",
    "main.imaging_nodule_master_v1": "phase1",
    "main.canonical_us_exam_master_v1": "phase1",
    "main.canonical_us_patient_master_v1": "phase1",
    "main.tirads_llm_extracted_v2": "phase1",
    "main.serial_imaging_us": "phase1",
    "manuscript_workspace.tirads_granular_parsed_v1": "phase1",
    "manuscript_workspace.us_nodule_dynamics_parsed_v1": "phase1",
    # Phase 2
    "main.tirads_v2_nodules_raw": "phase2",
    "main.extracted_tirads_validated_v1": "phase2",
    "main.tirads_reextraction_queue_v1": "phase2",
    # Phase 3
    "main.note_entities_llm_tirads_granular": "phase3",
    "main.note_entities_llm_us_nodule_dynamics": "phase3",
    "main.note_entities_llm_imaging": "phase3",
    # Phase 4 (workspace + views)
    "manuscript_workspace.imaging_nodule_master_clean_v1": "phase4",
    "manuscript_workspace.us_nodules_tirads_vs_inm_v1_discordance_v1": "phase4",
    "manuscript_workspace.tirads_v1_v2_discordance_v1": "phase4",
}


def table_kind(con, schema: str, name: str) -> str | None:
    rows = con.execute(
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [PUB, schema, name],
    ).fetchall()
    return rows[0][0] if rows else None


def main() -> int:
    con = connect_locked()
    out: dict = {"targets": {}}

    # Per-target: existence, kind, row count, dependent views
    for fq, phase in TARGETS.items():
        sch, name = fq.split(".", 1)
        kind = table_kind(con, sch, name)
        info: dict = {"phase": phase, "kind": kind}
        if kind:
            try:
                info["rows"] = con.execute(
                    f'SELECT COUNT(*) FROM {PUB}.{sch}."{name}"'
                ).fetchone()[0]
            except Exception as e:
                info["row_error"] = str(e)
        out["targets"][fq] = info

    # All views referencing any target table — DuckDB stores definitions in
    # information_schema.views.view_definition.
    views = con.execute(
        "SELECT table_schema, table_name, view_definition "
        "FROM information_schema.views "
        "WHERE table_catalog = ?",
        [PUB],
    ).fetchall()
    out["views"] = []
    for sch, name, defn in views:
        defn_l = (defn or "").lower()
        refs = []
        for fq in TARGETS:
            t_short = fq.split(".", 1)[1].lower()
            if t_short in defn_l:
                refs.append(fq)
        out["views"].append({
            "schema": sch, "name": name, "refs_targets": refs,
            "defn_len": len(defn or ""),
        })

    # views_readable inventory
    out["views_readable"] = [
        r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = ? AND table_schema = 'views_readable' "
            "ORDER BY 1",
            [PUB],
        ).fetchall()
    ]

    # us_legacy_20260421 contents
    con.execute('USE "Thyroid 2026 UPdated"')
    arch = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'Thyroid 2026 UPdated' "
        "AND table_schema = 'us_legacy_20260421' ORDER BY 1"
    ).fetchall()
    out["legacy_archive"] = [r[0] for r in arch]
    con.execute(f'USE "{PUB}"')

    # Note-entity entity_type breakdown for Phase 3
    out["entity_types"] = {}
    for tbl in (
        "note_entities_llm_tirads_granular",
        "note_entities_llm_us_nodule_dynamics",
        "note_entities_llm_imaging",
    ):
        if table_kind(con, "main", tbl) is None:
            continue
        cols = [
            r[0] for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_catalog=? AND table_schema='main' AND table_name=?",
                [PUB, tbl],
            ).fetchall()
        ]
        et_col = "entity_type" if "entity_type" in cols else (
            "entity_domain" if "entity_domain" in cols else (
                "domain" if "domain" in cols else None
            )
        )
        if not et_col:
            out["entity_types"][tbl] = {"_no_entity_col": True, "cols": cols}
            continue
        rows = con.execute(
            f'SELECT {et_col}, COUNT(*) FROM {PUB}.main.{tbl} '
            f"GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        out["entity_types"][tbl] = {et_col: dict(rows)}

    # CPM column existence check
    cpm_cols = [
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog=? AND table_schema='main' "
            "AND table_name='canonical_patient_master' "
            "AND (column_name LIKE 'imaging_tirads%' OR column_name LIKE 'imaging_updated%')",
            [PUB],
        ).fetchall()
    ]
    out["cpm_imaging_tirads_cols"] = cpm_cols

    # canonical_us_nodule_v2 TIRADS column inventory
    nv2_cols = [
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog=? AND table_schema='main' "
            "AND table_name='canonical_us_nodule_v2' "
            "AND column_name LIKE '%tirads%'",
            [PUB],
        ).fetchall()
    ]
    out["nv2_tirads_cols"] = nv2_cols

    # tirads_reported vs tirads_score_2017 equality
    eq = con.execute(
        f"""SELECT
            COUNT(*) AS both_present,
            SUM(CASE WHEN tirads_reported = tirads_score_2017 THEN 1 ELSE 0 END) AS equal_rows,
            SUM(CASE WHEN tirads_reported <> tirads_score_2017 THEN 1 ELSE 0 END) AS unequal_rows
           FROM {PUB}.main.canonical_us_nodule_v2
           WHERE tirads_reported IS NOT NULL
             AND tirads_score_2017 IS NOT NULL"""
    ).fetchone()
    out["tirads_reported_vs_score_2017"] = {
        "both_present": eq[0], "equal": eq[1], "unequal": eq[2]
    }

    # ACR feature availability
    feat = con.execute(
        f"""SELECT
            COUNT(*) total,
            SUM(CASE WHEN composition IS NOT NULL AND echogenicity IS NOT NULL
                          AND shape IS NOT NULL AND margins IS NOT NULL
                          AND echogenic_foci IS NOT NULL THEN 1 ELSE 0 END) all5_present,
            SUM(CASE WHEN composition_pts IS NOT NULL AND echogenicity_pts IS NOT NULL
                          AND shape_pts IS NOT NULL AND margin_pts IS NOT NULL
                          AND foci_pts IS NOT NULL THEN 1 ELSE 0 END) all5_pts_present,
            COUNT(tirads_reported) has_tirads_reported,
            COUNT(tirads_category_v2) has_tirads_category_v2,
            SUM(CASE WHEN tirads_category_v2 IS NOT NULL AND tirads_level_2017 IS NOT NULL THEN 1 ELSE 0 END) both_cat_populated
           FROM {PUB}.main.canonical_us_nodule_v2"""
    ).fetchone()
    out["nv2_feature_availability"] = {
        "total": feat[0],
        "all5_features_present": feat[1],
        "all5_pts_present": feat[2],
        "has_tirads_reported": feat[3],
        "has_tirads_category_v2": feat[4],
        "both_categories_populated": feat[5],
    }

    out_path = HERE / "_us_cleanup_probe.json"
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"wrote {out_path}")
    print(f"\ntargets ({len(TARGETS)}):")
    for fq, info in out["targets"].items():
        print(f"  {fq:60s} kind={info.get('kind')!r} rows={info.get('rows')}")
    print(f"\nlegacy archive ({len(out['legacy_archive'])} tables): "
          f"{out['legacy_archive']}")
    print(f"\nviews_readable ({len(out['views_readable'])}): {out['views_readable']}")
    print("\nviews referencing target tables (any non-empty refs_targets):")
    for v in out["views"]:
        if v["refs_targets"]:
            print(f"  {v['schema']}.{v['name']:50s} refs={v['refs_targets']}")
    print(f"\nentity_types: {json.dumps(out['entity_types'], indent=2)}")
    print(f"\ncpm_imaging_tirads_cols: {out['cpm_imaging_tirads_cols']}")
    print(f"\nnv2_tirads_cols: {out['nv2_tirads_cols']}")
    print(f"\nnv2_feature_availability: {out['nv2_feature_availability']}")
    print(f"\ntirads_reported vs tirads_score_2017: {out['tirads_reported_vs_score_2017']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
