#!/usr/bin/env python3
"""
BigQuery replication driver for ``pub_canonical.canonical_patient_master``.

This is the **publication-mirror builder**: it does not re-derive 1,500+ clinical
columns in BigQuery SQL. The **source of truth** remains MotherDuck
``thyroid_canonical_publication_v1_0.main.canonical_patient_master``, which is
maintained by the tracked MD pipeline (finalization / reconciliation / column
registry migrations — see ``docs/CANONICAL_STATE_20260417_SCRIPT271.md`` and
``qc_framework_v1/manuscript/canonical_methods_footnotes/canonical_patient_master.md``).

**Cohort / spine rule (10,871 patients):**
The live MD table enforced by ``scripts/_md_connect.connect_locked()`` must
contain exactly **10,871 rows** with **10,871 distinct** ``research_id`` values.
That spine is the same population referenced by downstream tables such as
``canonical_survival_followup_v1`` (joined on ``research_id``).

**Operational flow:**
  1. Export MD ``main.canonical_patient_master`` to Parquet (PHI column names
     dropped using the same substring rules as ``327_bulk_md_to_bq_missing_tables.py``).
  2. ``bq load --replace`` into ``thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master``.

**After a full replace-load**, any BigQuery-specific column additions applied
via historical ``ALTER TABLE`` migrations (for example ``bq_migrations/mig_080_*``,
``mig_082_*``, ``mig_088_*``, ``qc_framework_v1/migrations/320_*``,
``qc_framework_v1/migrations/334_*``) may need replay in migration-id order if
those columns are absent from MD. Prefer **schema parity review** against
``INFORMATION_SCHEMA.COLUMNS`` before production promotion.

Inputs such as demographic refresh workbooks (“Thyroid Patient Demographic
Refresh_*.xlsx”, Epic lists, wrong-DOB QA CSVs) feed the **MD build** via
operators’ ingest scripts — they are **not** read directly by this mirror driver.

Auth: MotherDuck token via ``motherduck_client`` / ``.toml``; BigQuery via
Application Default Credentials and ``bq`` CLI (same pattern as mig_327).

Usage:
    .venv/bin/python scripts/bq_replicate_canonical_patient_master.py [--dry-run] [--skip-load]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts._md_connect import PUBLICATION_DB, connect_locked  # noqa: E402

PROJECT = "thyroid-canonical-pub-2026"
DATASET = "pub_canonical"
TABLE = "canonical_patient_master"
LOCATION = "us-central1"
OUT_DIR_DEFAULT = _REPO / "exports" / "bq_cpm_replica"

_PHI_SUBS = ("mrn", "medical_record", "patient_name", "first_name", "last_name",
             "full_name", "ssn", "social_security", "phone", "address",
             "street", "email", "zip_code", "postal")
_PHI_EXACT = frozenset({"dob", "date_of_birth", "birth_date", "name",
                        "patient_first_name", "patient_last_name",
                        "dob_timestamp", "patient_dob"})


def _phi(name: str) -> bool:
    n = name.strip().lower()
    return n in _PHI_EXACT or any(s in n for s in _PHI_SUBS)


def _cols(con, tbl: str) -> tuple[list[str], list[str]]:
    """Return (kept_cols, phi_cols); dedupe preserves first ordinal_position."""
    rows = con.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{tbl}'
        ORDER BY ordinal_position
        """
    ).fetchall()
    if not rows:
        raise ValueError(f"No columns for {tbl}")
    seen: set[str] = set()
    cols_ordered: list[str] = []
    for (c,) in rows:
        if c not in seen:
            seen.add(c)
            cols_ordered.append(c)
    kept = [c for c in cols_ordered if not _phi(c)]
    dropped = [c for c in cols_ordered if _phi(c)]
    return kept, dropped


def export_parquet(con, out_dir: Path, dry_run: bool) -> tuple[int, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pq_path = out_dir / f"{TABLE}.parquet"
    kept, dropped = _cols(con, TABLE)
    if dropped:
        print(f"[PHI-drop] {len(dropped)} columns omitted from export: {dropped[:8]}{'...' if len(dropped) > 8 else ''}")
    sel = ", ".join(f'"{c}"' for c in kept)
    q = f"SELECT {sel} FROM \"{TABLE}\""
    n = con.execute(f"SELECT COUNT(*) FROM ({q}) _").fetchone()[0]
    if not dry_run:
        con.execute(
            f"COPY ({q}) TO '{pq_path.as_posix()}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    return n, pq_path


def bq_load(pq_path: Path) -> int:
    dest = f"{PROJECT}:{DATASET}.{TABLE}"
    cmd = [
        "bq",
        f"--location={LOCATION}",
        "load",
        "--replace",
        "--source_format=PARQUET",
        "--clustering_fields=research_id",
        dest,
        str(pq_path.resolve()),
    ]
    print("Running:", " ".join(cmd))
    return subprocess.call(cmd)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="counts only, no files")
    ap.add_argument("--skip-load", action="store_true", help="Parquet export only")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR_DEFAULT)
    args = ap.parse_args()

    con = connect_locked()
    n, pq_path = export_parquet(con, args.out_dir, args.dry_run)
    print(f"{TABLE}: {n} rows export")
    if args.dry_run:
        sys.exit(0)

    manifest = {
        "builder_script": "scripts/bq_replicate_canonical_patient_master.py",
        "source_ssot": f"{PUBLICATION_DB}.main.{TABLE}",
        "destination": f"{PROJECT}.{DATASET}.{TABLE}",
        "row_count_motherduck": n,
        "parquet_uri": str(pq_path.resolve()),
        "ran_at": datetime.now(timezone.utc).isoformat(),
    }
    if not args.skip_load:
        rc = bq_load(pq_path)
        manifest["bq_load_exit_code"] = rc
        if rc != 0:
            mf = args.out_dir / "manifest_partial.json"
            mf.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            print(f"bq load failed rc={rc}; manifest {mf}")
            sys.exit(rc)
    mf = args.out_dir / "manifest.json"
    mf.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Manifest: {mf}")
    print("Done.")


if __name__ == "__main__":
    main()
