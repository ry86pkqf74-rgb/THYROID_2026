#!/usr/bin/env python3
"""Migrate 'Thyroid 2026' from old MD account -> local file -> new MD as 'Thyroid 2026 UPdated'.
Table-by-table with progress, so stalls are visible."""
import os, re, subprocess, time, pathlib, duckdb

toml_text = pathlib.Path('/Users/ros/THyroid 2026/motherduck.local.toml').read_text()
OLD_TOKEN = re.search(r'^MOTHERDUCK_TOKEN\s*=\s*"([^"]+)"', toml_text, re.M).group(1)
rtf_path = pathlib.Path.home() / 'Desktop' / 'Motherduck_apikey_acccount2.rtf'
txt = subprocess.check_output(['textutil', '-convert', 'txt', '-stdout', str(rtf_path)]).decode()
NEW_TOKEN = [ln.strip() for ln in txt.splitlines() if ln.strip().startswith('eyJ')][0]
print(f"tokens: OLD len={len(OLD_TOKEN)} NEW len={len(NEW_TOKEN)}", flush=True)

SRC_DB = 'Thyroid 2026'
DST_DB = 'Thyroid 2026 UPdated'
LOCAL = '/Users/ros/THyroid 2026/exports/md_migration_20260415.duckdb'
pathlib.Path(LOCAL).parent.mkdir(parents=True, exist_ok=True)
if os.path.exists(LOCAL):
    os.remove(LOCAL)

# ---------- PHASE 1: pull table-by-table ----------
print("\n=== PHASE 1: PULL old MD -> local ===", flush=True)
os.environ['motherduck_token'] = OLD_TOKEN
con = duckdb.connect(LOCAL)
con.execute("ATTACH 'md:' (READ_ONLY)")

tables = con.execute(
    """SELECT table_schema, table_name, table_type
       FROM information_schema.tables
       WHERE table_catalog = ? AND table_type IN ('BASE TABLE','VIEW')
       ORDER BY table_type DESC, table_schema, table_name""",
    [SRC_DB]
).fetchall()
base = [t for t in tables if t[2] == 'BASE TABLE']
views = [t for t in tables if t[2] == 'VIEW']
print(f"Source has {len(base)} tables + {len(views)} views across schemas", flush=True)

# track schemas seen
schemas_made = set()
def ensure_schema(sc):
    if sc not in schemas_made:
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{sc}"')
        schemas_made.add(sc)

t_start = time.time()
total_rows = 0
for i, (sc, tn, _) in enumerate(base, 1):
    ensure_schema(sc)
    fq_src = f'"{SRC_DB}"."{sc}"."{tn}"'
    fq_dst = f'"{sc}"."{tn}"'
    t0 = time.time()
    con.execute(f'CREATE OR REPLACE TABLE {fq_dst} AS SELECT * FROM {fq_src}')
    rows = con.execute(f'SELECT COUNT(*) FROM {fq_dst}').fetchone()[0]
    total_rows += rows
    elapsed = time.time() - t0
    print(f"[{i}/{len(base)}] {sc}.{tn}  rows={rows:,}  {elapsed:.1f}s", flush=True)

# views
view_defs = []
for sc, vn, _ in views:
    try:
        row = con.execute(
            "SELECT view_definition FROM information_schema.views WHERE table_catalog=? AND table_schema=? AND table_name=?",
            [SRC_DB, sc, vn]
        ).fetchone()
        if row and row[0]:
            view_defs.append((sc, vn, row[0]))
    except Exception as e:
        print(f"  [view-def skip] {sc}.{vn}: {e}", flush=True)

print(f"PHASE 1 done: {len(base)} tables, {total_rows:,} rows, {time.time()-t_start:.0f}s", flush=True)
print(f"Captured {len(view_defs)} view definitions (will recreate in target)", flush=True)
con.close()
size_gb = os.path.getsize(LOCAL)/1024**3
print(f"Local dump: {size_gb:.2f} GiB at {LOCAL}", flush=True)

# save view defs to a sidecar
vfile = LOCAL + '.views.sql'
with open(vfile, 'w') as f:
    for sc, vn, ddl in view_defs:
        f.write(f'-- {sc}.{vn}\nCREATE SCHEMA IF NOT EXISTS "{sc}";\nCREATE OR REPLACE VIEW "{sc}"."{vn}" AS {ddl};\n\n')
print(f"View DDL saved: {vfile}", flush=True)

# ---------- PHASE 2: push local -> new MD ----------
print("\n=== PHASE 2: PUSH local -> new MD ===", flush=True)
os.environ['motherduck_token'] = NEW_TOKEN
con2 = duckdb.connect(':memory:')
con2.execute("ATTACH 'md:'")
con2.execute(f'CREATE DATABASE IF NOT EXISTS "{DST_DB}"')
con2.execute(f'USE "{DST_DB}"')
con2.execute(f"ATTACH '{LOCAL}' AS local_db (READ_ONLY)")

# list tables from local
locals_ = con2.execute(
    "SELECT table_schema, table_name FROM local_db.information_schema.tables WHERE table_type='BASE TABLE' ORDER BY 1,2"
).fetchall()
print(f"Uploading {len(locals_)} tables to new MD DB '{DST_DB}'", flush=True)

schemas_made2 = set()
t_start = time.time()
for i, (sc, tn) in enumerate(locals_, 1):
    if sc not in schemas_made2:
        con2.execute(f'CREATE SCHEMA IF NOT EXISTS "{DST_DB}"."{sc}"')
        schemas_made2.add(sc)
    fq_src = f'local_db."{sc}"."{tn}"'
    fq_dst = f'"{DST_DB}"."{sc}"."{tn}"'
    t0 = time.time()
    con2.execute(f'CREATE OR REPLACE TABLE {fq_dst} AS SELECT * FROM {fq_src}')
    rows = con2.execute(f'SELECT COUNT(*) FROM {fq_dst}').fetchone()[0]
    print(f"[{i}/{len(locals_)}] {sc}.{tn}  rows={rows:,}  {time.time()-t0:.1f}s", flush=True)

# recreate views
n_views = 0
with open(vfile) as f:
    sql_text = f.read()
for stmt in [s.strip() for s in sql_text.split(';') if s.strip()]:
    try:
        con2.execute(stmt)
        if 'CREATE OR REPLACE VIEW' in stmt.upper():
            n_views += 1
    except Exception as e:
        print(f"  [view skip] {stmt[:80]}...: {e}", flush=True)
print(f"Recreated {n_views} views", flush=True)

print(f"\nPHASE 2 done in {time.time()-t_start:.0f}s", flush=True)
print(f"\nMIGRATION COMPLETE: '{SRC_DB}' -> '{DST_DB}' in new MD account", flush=True)
