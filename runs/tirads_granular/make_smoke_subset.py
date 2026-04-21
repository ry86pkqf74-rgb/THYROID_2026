"""Pick 3 diverse rows for the driver smoke run."""
import pandas as pd
from pathlib import Path

BASE = Path("/Users/loganglosser/THYROID_2026")
df = pd.read_parquet(BASE / "processed/remaining/tirads_us_reports.parquet")

# one short, one medium, one longest — gives us coverage
df = df.assign(_len=df["note_text"].str.len())
short = df[df["_len"].between(300, 600)].head(1)
med = df[df["_len"].between(1500, 2200)].head(1)
long_ = df.nlargest(1, "_len")
subset = pd.concat([short, med, long_]).drop(columns=["_len"]).reset_index(drop=True)

print("Smoke subset note_row_ids + char lengths:")
for _, r in subset.iterrows():
    print(f"  {r['note_row_id']:>20s}  note_date={r['note_date']}  chars={len(r['note_text']):>5d}")

out = BASE / "processed/remaining/tirads_us_reports_smoke3.parquet"
subset.to_parquet(out, index=False)
print(f"Wrote {out}")
