#!/usr/bin/env python3
"""
cleanup_bad_model_rows.py — Strip rows extracted by known-bad models from checkpoint files.

Background:
  Early extraction runs used qwen3:14b (returns empty {}) and thyroid-moe/qwen3:30b-a3b
  (26% error rate). Those rows are baked into checkpoint JSONL files and block re-extraction
  because the resume logic treats them as "done". This script removes them so the supervisor
  can re-process those notes with qwen3:32b.

Usage:
  python3 cleanup_bad_model_rows.py [--dry-run] [--output-dir /opt/thyroid_extraction/output]

Safety:
  - Backs up each checkpoint to *.bak before modifying
  - Skips the currently-running domain (labs) by default, or pass --skip-domain
  - Never touches parquet files (those are handled separately)
  - Prints summary of rows removed per domain
"""

import argparse
import json
import glob
import os
import shutil
import sys

GOOD_MODEL = "qwen3:32b"
BAD_MODELS = {"qwen3:14b", "thyroid-moe", "qwen3:30b-a3b", "teacher:30b-a3b"}


def clean_checkpoint(filepath, dry_run=False):
    """Remove bad-model rows from a single checkpoint file. Returns (kept, removed) counts."""
    kept_lines = []
    removed = 0
    total = 0

    with open(filepath, "r") as f:
        for raw in f:
            total += 1
            try:
                row = json.loads(raw)
                model = row.get("llm_model", "")
                if model in BAD_MODELS or model != GOOD_MODEL:
                    removed += 1
                    continue
            except json.JSONDecodeError:
                removed += 1
                continue
            kept_lines.append(raw)

    kept = len(kept_lines)
    domain = os.path.basename(filepath).replace("note_entities_llm_", "").replace(".ckpt.jsonl", "")

    if removed == 0:
        print(f"  {domain}: {total} rows — all clean, skipping")
        return kept, 0

    if dry_run:
        print(f"  {domain}: {total} total → would keep {kept}, remove {removed}")
        return kept, removed

    # Backup original
    backup_path = filepath + ".bak"
    shutil.copy2(filepath, backup_path)

    # Write cleaned file
    with open(filepath, "w") as f:
        f.writelines(kept_lines)

    print(f"  {domain}: {total} total → kept {kept}, removed {removed} (backup: {backup_path})")
    return kept, removed


def main():
    parser = argparse.ArgumentParser(description="Strip bad-model rows from extraction checkpoints")
    parser.add_argument("--output-dir", default="/opt/thyroid_extraction/output",
                        help="Directory containing checkpoint JSONL files")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be removed without modifying files")
    parser.add_argument("--skip-domain", default="labs",
                        help="Skip this domain (it may be actively running). Use 'none' to clean all.")
    args = parser.parse_args()

    pattern = os.path.join(args.output_dir, "note_entities_llm_*.ckpt.jsonl")
    checkpoints = sorted(glob.glob(pattern))

    if not checkpoints:
        print(f"No checkpoint files found matching {pattern}")
        sys.exit(1)

    skip = args.skip_domain if args.skip_domain != "none" else None

    print(f"{'[DRY RUN] ' if args.dry_run else ''}Cleaning checkpoints in {args.output_dir}")
    print(f"  Good model: {GOOD_MODEL}")
    print(f"  Bad models: {BAD_MODELS}")
    if skip:
        print(f"  Skipping domain: {skip}")
    print()

    total_kept = 0
    total_removed = 0

    for fpath in checkpoints:
        domain = os.path.basename(fpath).replace("note_entities_llm_", "").replace(".ckpt.jsonl", "")
        if skip and domain == skip:
            print(f"  {domain}: SKIPPED (actively running)")
            continue
        kept, removed = clean_checkpoint(fpath, dry_run=args.dry_run)
        total_kept += kept
        total_removed += removed

    print()
    print(f"TOTAL: kept {total_kept}, removed {total_removed}")

    if not args.dry_run and total_removed > 0:
        print()
        print("Done. Restart the supervisor to re-extract the cleaned rows:")
        print("  nohup bash /opt/thyroid_extraction/supervisor_qwen32b.sh > /var/log/supervisor_qwen32b.log 2>&1 &")


if __name__ == "__main__":
    main()
