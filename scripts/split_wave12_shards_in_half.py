"""Split each wave-1+2 shard in half for 2x parallelism.

Reads processed/remaining/shards_wave12/clinical_notes_shard_{00..05}of06.parquet
Writes processed/remaining/shards_wave12_half/
    shard_{00..05}_halfA.parquet   (first half)
    shard_{00..05}_halfB.parquet   (second half)

Split is deterministic: sort by note_row_id, first half -> A, second half -> B.
"""
from pathlib import Path
import pandas as pd

SRC = Path("processed/remaining/shards")
DST = Path("processed/remaining/shards_half")
DST.mkdir(parents=True, exist_ok=True)

for i in range(6):
    p = SRC / f"clinical_notes_shard_{i:02d}of06.parquet"
    if not p.exists():
        print(f"[skip] {p} missing")
        continue
    df = pd.read_parquet(p).sort_values("note_row_id").reset_index(drop=True)
    mid = len(df) // 2
    a = df.iloc[:mid].copy()
    b = df.iloc[mid:].copy()
    a.to_parquet(DST / f"shard_{i:02d}_halfA.parquet", index=False)
    b.to_parquet(DST / f"shard_{i:02d}_halfB.parquet", index=False)
    print(f"shard_{i:02d}: {len(df):,}  ->  A={len(a):,}  B={len(b):,}")
