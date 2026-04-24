#!/usr/bin/env python3
"""Verify note_entities_llm_* tables landed in MotherDuck."""
import duckdb
import pathlib
import re

root = pathlib.Path(__file__).resolve().parents[2]
tok = re.search(
    r'MD_SA_TOKEN\s*=\s*"([^"]+)"',
    (root / "motherduck.local.toml").read_text(),
).group(1)
con = duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token={tok}")

tables = [
    "note_entities_llm_ete_subgrade_v1",
    "note_entities_llm_t4b_invasion_v1",
    "note_entities_llm_vascular_invasion_v2",
    "note_entities_llm_airway_invasion_v2",
    "note_entities_llm_parathyroid_detail_v1",
]
for t in tables:
    try:
        r = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id), "
            f"SUM(error) AS err, MAX(build_ts) "
            f"FROM main.{t}"
        ).fetchone()
        print(f"{t}: {r[0]:,} rows / {r[1]:,} patients / err={r[2]} / build={r[3]}")
    except Exception as e:
        print(f"{t}: MISSING -- {e.__class__.__name__}: {str(e)[:120]}")
