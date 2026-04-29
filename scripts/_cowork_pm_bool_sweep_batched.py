"""PM BOOLEAN cohort-uniformity sweep — batched. Runs 5 batches of ~80 cols each."""
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

print(f"{len(boolean_cols)} verified BOOLEAN cols on canonical_patient_master")

existing_cf = {r[0]: (r[1] or '').upper() for r in con.execute("""
    SELECT column_name, notes FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status='verified'
""").fetchall()}

def already_doc(col, kind):
    notes = existing_cf.get(col, '')
    if kind == 'A':
        keys = ['NEAR-UNIFORM-TRUE', 'TYPE-A', '-ALLTRUE', 'NEAR_UNIFORM_TRUE']
    else:
        keys = ['UNIFORM-FALSE', 'TYPE-B', 'PLACEHOLDER', 'ALL-FALSE',
                'ROLLUP-SEMANTICS', 'COHORT-NEAR-UNIFORM-FALSE',
                'RAI-AVIDITY', 'AVIDITY']
    return any(k in notes for k in keys)


B = 80
type_a, type_a_w_null, type_b, near_t, near_f = [], [], [], [], []

for batch_idx in range(0, len(boolean_cols), B):
    chunk = boolean_cols[batch_idx:batch_idx + B]
    sums = []
    for c in chunk:
        sums.append(f'SUM(CASE WHEN "{c}" THEN 1 ELSE 0 END) AS "{c}__t"')
        sums.append(f'SUM(CASE WHEN NOT "{c}" THEN 1 ELSE 0 END) AS "{c}__f"')
        sums.append(f'SUM(CASE WHEN "{c}" IS NULL THEN 1 ELSE 0 END) AS "{c}__n"')
    sql = f"SELECT {', '.join(sums)} FROM main.canonical_patient_master"
    print(f"  batch {batch_idx//B + 1}/{(len(boolean_cols)+B-1)//B}: {len(chunk)} cols", flush=True)
    row = con.execute(sql).fetchone()
    cols_out = [d[0] for d in con.description]
    stats = dict(zip(cols_out, row))

    for c in chunk:
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


def report(label, items, kind):
    new = [x for x in items if not already_doc(x[0], kind)]
    docd = [x for x in items if already_doc(x[0], kind)]
    print(f"\n=== {label}: {len(items)} (NEW={len(new)}, doc'd={len(docd)}) ===")
    for c, t, f_, n in sorted(new):
        print(f"  [NEW] {c:60s} t={t:6d} f={f_:6d} n={n:6d}")


report('TYPE-A pure (T-only, no FALSE, no NULL)', type_a, 'A')
report('TYPE-A presence-flag (T>0, F=0, N>0)', type_a_w_null, 'A')
report('TYPE-B (0 TRUE, F>0, n>=0)', type_b, 'B')
report('Near-uniform TRUE (>99% T, F>0)', near_t, 'A')
report('Near-uniform FALSE (<1% T)', near_f, 'B')

con.close()
