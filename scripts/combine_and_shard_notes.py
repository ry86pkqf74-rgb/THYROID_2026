"""
Combine wave-1 (Notes 12_1_25.xlsx → clinical_notes_long_rerun.parquet) with
wave-2 (Imaging+FNA → clinical_notes_long_wave2.parquet), then split into N
shards by deterministic hash(note_row_id) % N for a multi-host extraction
fleet. Also writes a manifest so we can prove every note is assigned to
exactly one shard.

Usage:
    python scripts/combine_and_shard_notes.py --shards 6
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

WAVE1 = Path("processed/remaining/clinical_notes_long_rerun.parquet")
WAVE2 = Path("processed/remaining/clinical_notes_long_wave2.parquet")
COMBINED = Path("processed/remaining/clinical_notes_long_combined.parquet")
SHARD_DIR = Path("processed/remaining/shards")
MANIFEST = SHARD_DIR / "manifest.json"


def _shard_id(row_id: str, n: int) -> int:
    h = hashlib.md5(row_id.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, required=True)
    args = ap.parse_args()
    n = args.shards

    w1 = pd.read_parquet(WAVE1) if WAVE1.exists() else pd.DataFrame()
    w2 = pd.read_parquet(WAVE2) if WAVE2.exists() else pd.DataFrame()
    print(f"wave-1: {len(w1):,} notes  ({w1.research_id.nunique() if len(w1) else 0:,} rids)")
    print(f"wave-2: {len(w2):,} notes  ({w2.research_id.nunique() if len(w2) else 0:,} rids)")

    cols = sorted(set(list(w1.columns)) | set(list(w2.columns)))
    for col in cols:
        if col not in w1.columns:
            w1[col] = ""
        if col not in w2.columns:
            w2[col] = ""
    combined = pd.concat([w1[cols], w2[cols]], ignore_index=True)
    # de-dup if any note_row_id collides
    combined = combined.drop_duplicates(subset=["note_row_id"], keep="first").reset_index(drop=True)
    print(f"combined unique notes: {len(combined):,}  rids: {combined.research_id.nunique():,}")

    COMBINED.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(COMBINED, index=False)
    print(f"wrote combined -> {COMBINED}")

    SHARD_DIR.mkdir(parents=True, exist_ok=True)
    combined["shard_id"] = combined["note_row_id"].apply(lambda r: _shard_id(r, n))

    manifest = {"shards": n, "combined_total": len(combined), "shards_detail": {}}
    for sid, grp in combined.groupby("shard_id"):
        out = SHARD_DIR / f"clinical_notes_shard_{sid:02d}of{n:02d}.parquet"
        grp.drop(columns=["shard_id"]).to_parquet(out, index=False)
        manifest["shards_detail"][str(sid)] = {
            "path": str(out),
            "notes": int(len(grp)),
            "rids": int(grp.research_id.nunique()),
        }
        print(f"  shard {sid:02d}: {len(grp):>6,} notes  ({grp.research_id.nunique():>5,} rids)  -> {out.name}")

    MANIFEST.write_text(json.dumps(manifest, indent=2))
    print(f"\nmanifest -> {MANIFEST}")
    print("\ndeploy command for each host h with shard S:")
    print("  scp <shard.parquet>  vast-host:/root/THYROID_2026/processed/remaining/clinical_notes_shard.parquet")
    print("  ssh vast-host 'bash /root/THYROID_2026/scripts/vastai/run_shard.sh'")


if __name__ == "__main__":
    main()
