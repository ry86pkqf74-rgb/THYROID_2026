"""Probe size-related columns across all feeder tables."""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore

con = connect_locked()
out = {}
CAT = "thyroid_canonical_publication_v1_0"


def cols_matching(schema, table, like):
    return [
        list(r) for r in con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog=? AND table_schema=? AND table_name=? "
            "AND column_name ILIKE ? ORDER BY ordinal_position",
            [CAT, schema, table, like],
        ).fetchall()
    ]


# CPM: every column that has 'size' or 'cm' or 'goiter' or 'thyroid'
out["cpm.size_cm_cols"] = sorted(
    cols_matching("main", "canonical_patient_master", "%size%cm%")
    + cols_matching("main", "canonical_patient_master", "%size_cm%")
    + cols_matching("main", "canonical_patient_master", "%diameter%")
    + cols_matching("main", "canonical_patient_master", "%goiter%size%")
    + cols_matching("main", "canonical_patient_master", "%substernal%")
    + cols_matching("main", "canonical_patient_master", "%greatest%")
    + cols_matching("main", "canonical_patient_master", "%path_tumor_size%")
    + cols_matching("main", "canonical_patient_master", "%dominant%size%"),
    key=lambda x: x[0],
)
# all 'cm' suffix cols
out["cpm.all_cm_cols"] = sorted(
    cols_matching("main", "canonical_patient_master", "%_cm")
    + cols_matching("main", "canonical_patient_master", "%cm_max%")
    + cols_matching("main", "canonical_patient_master", "%cm_dom%"),
    key=lambda x: x[0],
)

for s, t in [
    ("main", "synoptic_tumor_long_v1"),
    ("main", "tumor_episode_master_v2"),
    ("main", "canonical_tumor_characteristics_v1"),
    ("main", "path_synoptics"),
    ("main", "tumor_pathology"),
]:
    out[f"{s}.{t}.cols"] = [
        list(r) for r in con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog=? AND table_schema=? AND table_name=? "
            "ORDER BY ordinal_position",
            [CAT, s, t],
        ).fetchall()
    ]

print(json.dumps(out, indent=2, default=str))
