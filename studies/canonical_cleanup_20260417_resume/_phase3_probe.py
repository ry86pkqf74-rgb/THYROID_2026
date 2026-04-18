"""Probe canonical_cleanup_audit_v1 schema + scope of audit objects."""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore

con = connect_locked()
out: dict = {}
CAT = "thyroid_canonical_publication_v1_0"

audit_cols = con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_catalog=? AND table_schema='manuscript_workspace' "
    "AND table_name='canonical_cleanup_audit_v1' ORDER BY ordinal_position",
    [CAT],
).fetchall()
out["canonical_cleanup_audit_v1.columns"] = [list(r) for r in audit_cols]

# Sample first 5 rows
rows = con.execute(
    "SELECT * FROM manuscript_workspace.canonical_cleanup_audit_v1 LIMIT 5"
).fetchall()
keys = [d[0] for d in con.description]
out["canonical_cleanup_audit_v1.sample"] = [dict(zip(keys, r)) for r in rows]

# Status distribution
dist = con.execute(
    "SELECT status, COUNT(*) FROM manuscript_workspace.canonical_cleanup_audit_v1 "
    "GROUP BY 1 ORDER BY 2 DESC"
).fetchall()
out["status_distribution"] = dist

# Schema-level inventory
sl = con.execute(
    "SELECT table_schema, table_type, COUNT(*) "
    "FROM information_schema.tables "
    "WHERE table_catalog = ? "
    "GROUP BY 1, 2 ORDER BY 1, 2",
    [CAT],
).fetchall()
out["schema_inventory"] = sl

# Total objects in main + manuscript_workspace
n_main_table = con.execute(
    "SELECT COUNT(*) FROM information_schema.tables "
    "WHERE table_catalog=? AND table_schema='main' AND table_type='BASE TABLE'",
    [CAT],
).fetchone()[0]
n_main_view = con.execute(
    "SELECT COUNT(*) FROM information_schema.tables "
    "WHERE table_catalog=? AND table_schema='main' AND table_type='VIEW'",
    [CAT],
).fetchone()[0]
n_mw_table = con.execute(
    "SELECT COUNT(*) FROM information_schema.tables "
    "WHERE table_catalog=? AND table_schema='manuscript_workspace' "
    "AND table_type='BASE TABLE'",
    [CAT],
).fetchone()[0]
n_mw_view = con.execute(
    "SELECT COUNT(*) FROM information_schema.tables "
    "WHERE table_catalog=? AND table_schema='manuscript_workspace' "
    "AND table_type='VIEW'",
    [CAT],
).fetchone()[0]
out["main.base_tables"] = n_main_table
out["main.views"] = n_main_view
out["manuscript_workspace.base_tables"] = n_mw_table
out["manuscript_workspace.views"] = n_mw_view
out["main.total"] = n_main_table + n_main_view

# Audit object_name distribution (with prefix breakdown)
audit_obj_type = con.execute(
    "SELECT object_type, COUNT(*) FROM manuscript_workspace.canonical_cleanup_audit_v1 "
    "GROUP BY 1 ORDER BY 2 DESC"
).fetchall()
out["audit_object_type_dist"] = audit_obj_type

audit_obj_pref = con.execute(
    """
    SELECT
      CASE
        WHEN object_name LIKE 'main.%' THEN 'main.'
        WHEN object_name LIKE 'manuscript_workspace.%' THEN 'manuscript_workspace.'
        ELSE 'other'
      END AS prefix,
      COUNT(*)
    FROM manuscript_workspace.canonical_cleanup_audit_v1
    GROUP BY 1 ORDER BY 2 DESC
    """
).fetchall()
out["audit_object_name_prefix_dist"] = audit_obj_pref

# A few example rows
ex = con.execute(
    "SELECT object_name, object_type, status, n_columns, created_at "
    "FROM manuscript_workspace.canonical_cleanup_audit_v1 LIMIT 8"
).fetchall()
out["audit_sample_short"] = ex

print(json.dumps(out, indent=2, default=str))
