"""Cowork PM BOOLEAN cohort-uniformity back-sweep — single-shot read-only audit.

Sweeps every verified BOOLEAN col on canonical_patient_master, classifies each as
Type-A (T-only on cohort), Type-A_with_NULL (presence flag), Type-B (0 TRUE),
Near-uniform-TRUE / FALSE, or normal. Cross-references existing CF notes in the
column registry to surface only NEW findings (i.e., cohort-uniformity sneakers
that prior rounds missed).

Usage: python3 scripts/_cowork_pm_bool_sweep.py
"""
import sys
sys.path.insert(0, '/Users/ros/THyroid 2026')
from scripts._md_connect import connect_locked

con = connect_locked()

boolean_cols = [r[0] for r in con.execute("""
    SELECT c.column_name
    FROM information_schema.columns c
    JOIN main.canonical_column_verification_registry_v1 r
      ON r.schema_name='main' AND r.table_name=c.table_name AND r.column_name=c.column_name
    WHERE c.table_catalog='thyroid_canonical_publication_v1_0' AND c.table_schema='main'
      AND c.table_name='canonical_patient_master'
      AND c.data_type='BOOLEAN'
      AND r.verification_status='verified'
    ORDER BY c.column_name
""").fetchall()]

print(f"Sweeping {len(boolean_cols)} verified BOOLEAN cols on canonical_patient_master")
print()

sums = []
for c in boolean_cols:
    sums.append(f'SUM(CASE WHEN "{c}" THEN 1 ELSE 0 END) AS "{c}__t"')
    sums.append(f'SUM(CASE WHEN NOT "{c}" THEN 1 ELSE 0 END) AS "{c}__f"')
    sums.append(f'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "{c}__n"')
sql = f"SELECT {', '.join(sums)} FROM main.canonical_patient_master"

row = con.execute(sql).fetchone()
cols_out = [d[0] for d in con.description]
stats = dict(zip(cols_out, row))

total = 10871
type_a, type_a_w_null, type_b, near_t, near_f = [], [], [], [], []

for c in boolean_cols:
    t = stats[f'{c}__t']
    f_ = stats[f'{c}__f']
    n = stats[f'{c}__n']
    populated = t + f_
    if t > 0 and f_ == 0 and n == 0:
        type_a.append((c, t, f_, n))
    elif t > 0 and f_ == 0 and n > 0:
        type_a_w_null.append((c, t, f_, n))
    elif t == 0 and f_ > 0:
        type_b.append((c, t, f_, n))
    elif populated > 100 and t / populated > 0.99 and f_ > 0:
        near_t.append((c, t, f_, n))
    elif populated > 100 and t / populated < 0.01 and t > 0:
        near_f.append((c, t, f_, n))

existing_cf = {r[0]: r[1] or '' for r in con.execute("""
    SELECT column_name, notes FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status='verified'
""").fetchall()}


def flagged(col, kind):
    notes = existing_cf.get(col, '').upper()
    if kind == 'A':
        return any(s in notes for s in ['NEAR-UNIFORM-TRUE', 'TYPE-A', '-ALLTRUE', 'UNIFORM_TRUE'])
    return any(s in notes for s in [
        'UNIFORM-FALSE', 'TYPE-B', 'PLACEHOLDER', 'ALL-FALSE',
        'ROLLUP-SEMANTICS', 'COHORT-NEAR-UNIFORM', 'RAI-AVIDITY',
    ])


def report(label, items, kind):
    new = [x for x in items if not flagged(x[0], kind)]
    docd = [x for x in items if flagged(x[0], kind)]
    print(f"=== {label}: {len(items)} (NEW={len(new)}, doc'd={len(docd)}) ===")
    for c, t, f_, n in new:
        print(f"  [NEW] {c:60s} t={t:6d} f={f_:6d} n={n:6d}")
    print()


report('TYPE-A: T-only / 0 FALSE / 0 NULL (all-TRUE on cohort)', type_a, 'A')
report('TYPE-A_with_NULL: T>0 / 0 FALSE / N>0 NULL (presence flag)', type_a_w_null, 'A')
report('TYPE-B: 0 TRUE / FALSE>0 (degenerate placeholder)', type_b, 'B')
report('Near-uniform TRUE (>99% T on populated, F>0)', near_t, 'A')
report('Near-uniform FALSE (<1% T on populated, T>0)', near_f, 'B')

con.close()
