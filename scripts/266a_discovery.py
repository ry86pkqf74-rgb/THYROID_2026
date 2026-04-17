#!/usr/bin/env python3
"""Discovery for Script 266a — read-only inspection of dictionary, registry,
CPM columns we'll touch."""
from __future__ import annotations
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

OUT = HERE / "output" / "266a_discovery.json"


CPM_COLS_TOUCHED = [
    "n_tumors", "n_tumors_v10", "n_tumors_path", "n_tumors_pni_present",
    "n_tumors_vi_present", "n_tumors_ete_present", "n_tumors_lvi_present",
    "n_tumors_margin_involved", "n_tumors_margin_uninvolved", "n_tumors_with_size",
    "ajcc7_m_stage", "ajcc8_m_stage", "gm_path_stage_raw", "gm_path_m_stage_raw",
    "has_left_tumor", "has_right_tumor", "has_isthmus_tumor",
    "path_stage_raw", "path_t_stage_raw", "path_n_stage_raw", "path_m_stage_raw",
]


def main() -> int:
    con = connect_locked()
    out: dict = {}

    out["dictionary_v240_columns"] = [
        c[0] for c in con.execute(f"""
            SELECT column_name FROM information_schema.columns
             WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
               AND table_name='data_dictionary_v240'
             ORDER BY ordinal_position
        """).fetchall()
    ]

    rows = con.execute(f"""
        SELECT *
          FROM {PUBLICATION_DB}.main.data_dictionary_v240
         WHERE column_name IN ({",".join(repr(c) for c in CPM_COLS_TOUCHED)})
         ORDER BY column_name
    """).fetchall()
    out["dictionary_v240_target_rows"] = [
        dict(zip(out["dictionary_v240_columns"], r)) for r in rows
    ]

    out["cpm_columns_present"] = {
        col: bool(con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
             WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
               AND table_name='canonical_patient_master' AND column_name=?
        """, [col]).fetchone()[0])
        for col in CPM_COLS_TOUCHED
    }

    out["registry_columns"] = [
        c[0] for c in con.execute(f"""
            SELECT column_name FROM information_schema.columns
             WHERE table_catalog='{PUBLICATION_DB}'
               AND table_schema='manuscript_workspace'
               AND table_name='detail_table_registry_v1'
             ORDER BY ordinal_position
        """).fetchall()
    ]

    rrows = con.execute(f"""
        SELECT *
          FROM {PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1
         WHERE detail_table_name IN ('tumor_pathology', 'patient_tumor_rollup_v1',
              'canonical_tumor_characteristics_v1')
         ORDER BY detail_table_name
    """).fetchall()
    out["registry_target_rows"] = [
        dict(zip(out["registry_columns"], r)) for r in rrows
    ]

    out["pointer_object_type"] = con.execute(f"""
        SELECT table_type FROM information_schema.tables
         WHERE table_catalog='{PUBLICATION_DB}'
           AND table_schema='manuscript_workspace'
           AND table_name='canonical_detail_pointer_v1'
    """).fetchone()

    out["pointer_current_distinct_master_cols"] = con.execute(f"""
        SELECT COUNT(DISTINCT master_column)
          FROM {PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1
    """).fetchone()[0]

    out["unmapped_columns_currently_in_C"] = [
        r[0] for r in con.execute(f"""
            SELECT column_name FROM
              {PUBLICATION_DB}.manuscript_workspace.cpm_unmapped_triage_v265
             WHERE triage_bucket='C_missing_feeder'
               AND column_name IN ({",".join(repr(c) for c in CPM_COLS_TOUCHED)})
             ORDER BY column_name
        """).fetchall()
    ]

    out["v266_existence"] = {
        "data_dictionary_v266a": bool(con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
             WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
               AND table_name='data_dictionary_v266a'
        """).fetchone()[0]),
        "data_dictionary_v266": bool(con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
             WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
               AND table_name='data_dictionary_v266'
        """).fetchone()[0]),
        "cpm_unmapped_triage_v266a": bool(con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
             WHERE table_catalog='{PUBLICATION_DB}'
               AND table_schema='manuscript_workspace'
               AND table_name='cpm_unmapped_triage_v266a'
        """).fetchone()[0]),
    }

    OUT.write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
