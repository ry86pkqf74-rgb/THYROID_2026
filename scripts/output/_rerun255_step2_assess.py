#!/usr/bin/env python3
"""Assess 255 mid-run state: did CPM updates commit? Are the replay queries clean?"""
import json
import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

con = connect_locked()
CPM = f"{PUBLICATION_DB}.main.canonical_patient_master"
RAI_EP = f"{PUBLICATION_DB}.main.rai_treatment_episode_v2"
TG = f"{PUBLICATION_DB}.main.thyroglobulin_lab_VIEW_v1"

# 1. CPM invariant
inv = con.execute(f"""
  SELECT COUNT(*), COUNT(DISTINCT research_id),
         SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
  FROM {CPM}
""").fetchone()
print(f"CPM invariant: {inv}")
assert inv == (10871, 10871, 0)

# 2. Replay queries (these are what 255 checks at VERIFY step — all should be 0 if rebuilds landed)
REPLAY_RAI = f"""
WITH e AS (SELECT TRY_CAST(research_id AS INTEGER) AS rid, MAX(dose_mci) AS max_dose
           FROM {RAI_EP} GROUP BY 1)
SELECT COUNT(*) FROM {CPM} cpm
JOIN e ON TRY_CAST(cpm.research_id AS INTEGER) = e.rid
WHERE (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL) AND e.max_dose > 0
"""
rai_a = con.execute(REPLAY_RAI).fetchone()[0]
print(f"Replay RAI mismatches (should be 0): {rai_a}")

REPLAY_TG = f"""
WITH t AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid,
         COUNT(*) FILTER (WHERE analyte='Tg') AS n_tg,
         COUNT(*) FILTER (WHERE analyte='TgAb') AS n_tgab
  FROM {TG} GROUP BY 1
)
SELECT
  COUNT(*) FILTER (WHERE COALESCE(cpm.n_tg_measurements_structured,-1) <> COALESCE(t.n_tg,-1)),
  COUNT(*) FILTER (WHERE COALESCE(cpm.n_tgab_measurements,-1)         <> COALESCE(t.n_tgab,-1))
FROM {CPM} cpm
JOIN t ON TRY_CAST(cpm.research_id AS INTEGER) = t.rid
"""
tg_c = con.execute(REPLAY_TG).fetchone()
print(f"Replay Tg count mismatches (should be 0,0): tg={tg_c[0]} tgab={tg_c[1]}")

REPLAY_TGPN = f"""
WITH t AS (
  SELECT TRY_CAST(research_id AS INTEGER) AS rid,
         MAX(result_numeric) FILTER (WHERE analyte='Tg') AS tg_peak_c,
         MIN(result_numeric) FILTER (WHERE analyte='Tg') AS tg_nadir_c
  FROM {TG} GROUP BY 1
)
SELECT
  COUNT(*) FILTER (WHERE COALESCE(cpm.tg_peak,-1e300)  <> COALESCE(t.tg_peak_c,-1e300)),
  COUNT(*) FILTER (WHERE COALESCE(cpm.tg_nadir,-1e300) <> COALESCE(t.tg_nadir_c,-1e300))
FROM {CPM} cpm
JOIN t ON TRY_CAST(cpm.research_id AS INTEGER) = t.rid
"""
tg_pn = con.execute(REPLAY_TGPN).fetchone()
print(f"Replay Tg peak/nadir mismatches (should be 0,0): peak={tg_pn[0]} nadir={tg_pn[1]}")

# 3. Per-analyte canonicals + VIEWs — compare to pre-state
state_path = sorted(Path(__file__).resolve().parent.glob("_rerun255_pre_state_*.json"))[-1]
pre = json.loads(state_path.read_text())
print(f"\nLoaded pre-state from {state_path.name}")

def row_count(tbl):
    return con.execute(f'SELECT COUNT(*) FROM main."{tbl}"').fetchone()[0]

def sha256_table(tbl):
    cols = con.execute(f"""
      SELECT column_name FROM information_schema.columns
      WHERE table_schema='main' AND table_name='{tbl}' ORDER BY ordinal_position
    """).fetchall()
    col_list = ", ".join([f'COALESCE(CAST("{c[0]}" AS VARCHAR), \'\')' for c in cols])
    q = f"""
      SELECT md5(string_agg(row_hash, ',' ORDER BY row_hash))
      FROM (SELECT md5(concat_ws('|', {col_list})) AS row_hash FROM main."{tbl}")
    """
    return con.execute(q).fetchone()[0]

print("\n== PER-ANALYTE CANONICALS (must be bit-exact unchanged) ==")
for t, info in pre["per_analyte"].items():
    rc = row_count(t)
    h = sha256_table(t)
    ok = (rc == info["rows"]) and (h == info["hash"])
    print(f"  {t}: rows={rc} (pre={info['rows']}), hash_match={h == info['hash']} [{'PASS' if ok else 'FAIL'}]")

print("\n== VIEWS ==")
for t, info in pre["views"].items():
    rc = row_count(t)
    ok = (rc == info["rows"])
    print(f"  {t}: rows={rc} (pre={info['rows']}) [{'PASS' if ok else 'FAIL'}]")

print("\n== ROLLUP TABLES (expected: unchanged — 255 does NOT touch these) ==")
for t, info in pre["rollups"].items():
    rc = row_count(t)
    h = sha256_table(t)
    print(f"  {t}: rows={rc} (pre={info['rows']}), hash_match={h == info['hash']}")
