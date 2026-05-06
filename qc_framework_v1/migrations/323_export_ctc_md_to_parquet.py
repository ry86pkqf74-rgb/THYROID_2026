#!/usr/bin/env python3
"""mig_323_export_ctc_md_to_parquet — THY-18: export CTC from MotherDuck to PHI-safe parquet.

MD source (locked search path):
  thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1

Output:
  Parquet + manifest JSON under --out-dir (default: exports/bq_ctc_mig323/).

PHI: drop any column whose name matches direct identifiers (MRN, name, full DOB, etc.).
Clinical tumor rows are keyed by research_id only in BQ per project guardrails.

Usage:
  cd /path/to/THYROID_2026
  .venv/bin/python qc_framework_v1/migrations/323_export_ctc_md_to_parquet.py
  .venv/bin/python qc_framework_v1/migrations/323_export_ctc_md_to_parquet.py --out-dir /tmp/ctc_export
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts._md_connect import (  # noqa: E402
    PUBLICATION_DB,
    connect_locked,
)

CTC_TABLE = "canonical_tumor_characteristics_v1"

# Names to exclude from export (case-insensitive substring on column name).
_PHI_SUBSTRINGS = (
    "mrn",
    "medical_record",
    "patient_name",
    "first_name",
    "last_name",
    "full_name",
    "ssn",
    "social_security",
    "phone",
    "address",
    "street",
    "email",
    "zip_code",
    "postal",
)

# Exact name drops (lowercase).
_PHI_EXACT = frozenset(
    {
        "dob",
        "date_of_birth",
        "birth_date",
        "name",
        "patient_first_name",
        "patient_last_name",
    }
)


def _is_phi_column(name: str) -> bool:
    n = name.strip().lower()
    if n in _PHI_EXACT:
        return True
    if any(s in n for s in _PHI_SUBSTRINGS):
        return True
    # Full DOB-like; keep dob_year if it ever appears on CTC
    if n in ("dob_timestamp", "patient_dob"):
        return True
    return False


def _pick_columns(con) -> tuple[list[str], list[str]]:
    rows = con.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_catalog = '{PUBLICATION_DB}'
          AND table_schema = 'main'
          AND table_name = '{CTC_TABLE}'
        ORDER BY ordinal_position
        """
    ).fetchall()
    if not rows:
        raise SystemExit(
            f"No columns for {PUBLICATION_DB}.main.{CTC_TABLE}. "
            "Confirm the table exists in MotherDuck (script 245 / 266b lineage)."
        )
    cols = [r[0] for r in rows]
    kept, dropped = [], []
    for c in cols:
        if _is_phi_column(c):
            dropped.append(c)
        else:
            kept.append(c)
    return kept, dropped


def main() -> None:
    ap = argparse.ArgumentParser(description="Export CTC from MotherDuck to parquet (THY-18).")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=_REPO / "exports" / "bq_ctc_mig323",
        help="Directory for parquet + manifest",
    )
    args = ap.parse_args()
    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / f"{CTC_TABLE}.parquet"
    manifest_path = out_dir / f"{CTC_TABLE}_export_manifest.json"

    con = connect_locked()
    kept, dropped = _pick_columns(con)
    if dropped:
        print(f"[mig_323] Dropped {len(dropped)} PHI-risk columns: {dropped}")

    select_list = ", ".join(f'"{c}"' for c in kept)
    n_rows = con.execute(f'SELECT COUNT(*) FROM "{CTC_TABLE}"').fetchone()[0]
    n_pk = con.execute(
        f"""
        SELECT COUNT(*) FROM (
          SELECT COUNT(*) AS c
          FROM "{CTC_TABLE}"
          GROUP BY research_id, surgery_episode_id, tumor_ordinal
        ) s
        WHERE s.c > 1
        """
    ).fetchone()[0]
    if n_pk:
        print(
            f"[mig_323] WARNING: {n_pk} composite PK groups have duplicate rows "
            "(research_id, surgery_episode_id, tumor_ordinal). Export continues; "
            "investigate MD duplicate grain if unintended.",
            file=sys.stderr,
        )

    fq_from = f'"{CTC_TABLE}"'
    con.execute(
        f"""
        COPY (SELECT {select_list} FROM {fq_from})
        TO '{parquet_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    md5_manifest = {
        "migration_id": "mig_323_export_ctc_md_to_parquet",
        "linear_issue": "THY-18",
        "dfl_note": "DFL-20260506-CTCBQ (Data Feedback Log, base appJYOnUb7KrHKwpV)",
        "exported_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_table": f"{PUBLICATION_DB}.main.{CTC_TABLE}",
        "row_count": n_rows,
        "columns_exported": kept,
        "phi_columns_dropped": dropped,
        "parquet_path": str(parquet_path.resolve()),
        "pk_grain": ["research_id", "surgery_episode_id", "tumor_ordinal"],
    }
    manifest_path.write_text(json.dumps(md5_manifest, indent=2), encoding="utf-8")
    print(f"[mig_323] Wrote {parquet_path} ({n_rows} rows)")
    print(f"[mig_323] Manifest {manifest_path}")


if __name__ == "__main__":
    main()
