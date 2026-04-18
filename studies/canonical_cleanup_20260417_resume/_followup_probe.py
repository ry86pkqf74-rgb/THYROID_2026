"""Probe schemas for the upstream feeder audit + cancer-text investigation."""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore

con = connect_locked()
out: dict = {}
CAT = "thyroid_canonical_publication_v1_0"


def cols_like(schema, table, like):
    return [
        list(r) for r in con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog=? AND table_schema=? AND table_name=? "
            "AND column_name ILIKE ? ORDER BY ordinal_position",
            [CAT, schema, table, like],
        ).fetchall()
    ]


# CPM columns of interest for the upstream feeder audit
patterns = [
    "%ete%",
    "%lvi%",
    "%vasc%",
    "%lymphatic%",
    "%margin%",
    "%multifocal%",
    "%n_tumors%",
    "%number_of_tumors%",
    "%histology_final%",
    "%histology_v%",
    "%nodes%",
    "%nodal%",
]
all_cpm = []
for p in patterns:
    all_cpm.extend(cols_like("main", "canonical_patient_master", p))
# Dedup
seen = set()
out["cpm.candidate_worstcase_cols"] = []
for c in all_cpm:
    k = (c[0], c[1])
    if k in seen:
        continue
    seen.add(k)
    out["cpm.candidate_worstcase_cols"].append(c)
out["cpm.candidate_worstcase_cols"].sort()

# TEM columns of interest
out["tem.cols"] = cols_like("main", "tumor_episode_master_v2", "%")

# operative_episode_detail_v2 procedure_raw / procedure_normalized
out["oed.procedure_cols"] = (
    cols_like("main", "operative_episode_detail_v2", "%procedure%")
    + cols_like("main", "operative_episode_detail_v2", "%histology%")
    + cols_like("main", "operative_episode_detail_v2", "%diagnosis%")
)

# Sample 5 procedure_raw values for orphan rids
rids = con.execute(
    "SELECT CAST(research_id AS BIGINT) AS rid "
    "FROM manuscript_workspace.lab_orphan_audit_v1 LIMIT 10"
).fetchall()
rids_csv = ",".join(str(r[0]) for r in rids)
sample = con.execute(
    f"SELECT research_id, procedure_raw, procedure_normalized "
    f"FROM main.operative_episode_detail_v2 "
    f"WHERE research_id IN ({rids_csv}) "
    f"LIMIT 15"
).fetchall()
out["oed.sample_procedures_for_orphan_rids"] = [
    {"rid": r[0], "procedure_raw": r[1], "procedure_normalized": r[2]}
    for r in sample
]

# Total orphan rids in OED
n_oed_orphan = con.execute(
    """
    SELECT COUNT(DISTINCT oed.research_id) FROM main.operative_episode_detail_v2 oed
    WHERE oed.research_id IN (
        SELECT CAST(research_id AS BIGINT) FROM manuscript_workspace.lab_orphan_audit_v1
    )
    """
).fetchone()[0]
out["n_orphan_rids_in_oed"] = n_oed_orphan

print(json.dumps(out, indent=2, default=str))
