#!/usr/bin/env python3
"""Materialize ``main.molecular_testing`` (long format) on MotherDuck + rebuild ``molecular_test_episode_v2``.

Reads ``raw/THYROSEQ_AFIRMA_12_5.xlsx`` (wide per-patient slots) and unpivots to the long
schema expected by ``scripts/22_canonical_episodes_v2.py`` (``MOLECULAR_TEST_EPISODE_V2_SQL``).

Does **not** run ``register_parquets`` — avoids CREATE OR REPLACE of unrelated ``main.*``
tables from local ``processed/*.parquet`` when targeting prod.

Usage:
  .venv/bin/python scripts/145_md_materialize_molecular_testing.py --md --md-sa --md-env prod
  .venv/bin/python scripts/145_md_materialize_molecular_testing.py --md --md-sa \\
      --excel raw/THYROSEQ_AFIRMA_12_5.xlsx --skip-episode-rebuild
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_EXCEL = ROOT / "raw" / "THYROSEQ_AFIRMA_12_5.xlsx"
DB_PATH = ROOT / "thyroid_master.duckdb"


def _load_episode_sql() -> str:
    spec = importlib.util.spec_from_file_location(
        "canonical_episodes_v22",
        ROOT / "scripts" / "22_canonical_episodes_v2.py",
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load 22_canonical_episodes_v2.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return str(mod.MOLECULAR_TEST_EPISODE_V2_SQL)


def _nonempty(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return False
    s = str(v).strip()
    return s not in ("", "None", "nan")


def _s(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return str(v).strip() or None


def _date_cell(v) -> str | None:
    """Normalize workbook date cells; placeholders like 'x' → NULL."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if hasattr(v, "isoformat"):
        try:
            return v.date().isoformat()
        except Exception:
            pass
    s = str(v).strip()
    if not s or s.lower() in ("x", "xx", "-", "n/a", "na", "unk", "unknown", "none", "nan"):
        return None
    return s


def wide_to_long_molecular_testing(
    df: pd.DataFrame, *, max_slot: int, grain: str
) -> pd.DataFrame:
    """Unpivot assay slots (1..max_slot) to long rows (legacy ``molecular_testing`` shape).

    ``grain``:
      - ``patient`` (default): for each workbook row, emit slot 1 once (manuscript-scale spine).
      - ``strict``: emit a slot row only when there is a real date or result/mutation/detailed text.
    """
    if grain not in ("patient", "strict"):
        raise SystemExit("--grain must be patient or strict")
    rid_col = "Research ID number"
    if rid_col not in df.columns:
        raise SystemExit(f"Missing {rid_col!r} in workbook")

    slot_defs: list[dict[str, str]] = [
        {
            "thyroseq": "Thyroseq/Afirma_1",
            "nodule": "Nodule_info_1",
            "fna_bethesda": "FNA_bethesda_1",
            "date": "DATE_1",
            "genetic_performed": "Genetic Test Performed_1",
            "result": "RESULT_1",
            "detailed": "Detailed findings_1",
            "mutation": "MUTATION_1",
        },
        {
            "thyroseq": "Thyroseq/Afirma_2",
            "nodule": "Nodule_info_2",
            "fna_bethesda": "FNA Bethesda_2",
            "date": "DATE_2",
            "genetic_performed": "Genetic_test_2",
            "result": "RESULT_2",
            "detailed": "Detailed findings_2",
            "mutation": "MUTATION-2",
        },
        {
            "thyroseq": "Thyroseq/Afirma_3",
            "nodule": "Nodule_info_3",
            "fna_bethesda": "FNA_Bethesda_3",
            "date": "DATE_3",
            "genetic_performed": "Genetic_Test_3",
            "result": "RESULT_3",
            "detailed": "Detailed findings_3",
            "mutation": "MUTATION_3",
        },
    ]
    if max_slot < 1 or max_slot > 3:
        raise SystemExit("--max-slot must be 1, 2, or 3")
    slots = slot_defs[:max_slot]

    out_rows: list[dict] = []
    for _, row in df.iterrows():
        rid = row.get(rid_col)
        if rid is None or (isinstance(rid, float) and pd.isna(rid)):
            continue
        try:
            research_id = int(float(rid))
        except (TypeError, ValueError):
            continue

        for test_index, m in enumerate(slots, start=1):
            vals = {}
            for logical, col in m.items():
                vals[logical] = row[col] if col in df.columns else None
            if grain == "strict":
                has_date = _date_cell(vals["date"]) is not None
                has_text = any(_nonempty(vals[k]) for k in ("result", "mutation", "detailed"))
                if not (has_date or has_text):
                    continue

            gp = _s(vals["genetic_performed"])
            thy = _s(vals["thyroseq"])
            out_rows.append(
                {
                    "research_id": research_id,
                    "thyroseq_afirma": thy,
                    "genetic_test_performed": gp,
                    "genetic_test": gp,
                    "result": _s(vals["result"]),
                    "mutation": _s(vals["mutation"]),
                    "detailed_findings": _s(vals["detailed"]),
                    "nodule_info": _s(vals["nodule"]),
                    "fna_bethesda": _s(vals["fna_bethesda"]),
                    "test_index": test_index,
                    "_raw_date": _date_cell(vals["date"]),
                }
            )

    long_df = pd.DataFrame(out_rows)
    if long_df.empty:
        return long_df
    long_df = long_df.sort_values(["research_id", "test_index"]).reset_index(drop=True)
    return long_df


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--md", action="store_true", help="Target MotherDuck (fail-closed).")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN.")
    p.add_argument(
        "--md-env",
        default=None,
        help="dev|qa|prod when MOTHERDUCK_DATABASE unset.",
    )
    p.add_argument("--database", default=None, help="Override MotherDuck database name.")
    p.add_argument(
        "--excel",
        default=str(DEFAULT_EXCEL),
        help="Path to THYROSEQ_AFIRMA workbook.",
    )
    p.add_argument(
        "--skip-episode-rebuild",
        action="store_true",
        help="Only replace molecular_testing; do not run molecular_test_episode_v2 SQL.",
    )
    p.add_argument(
        "--max-slot",
        type=int,
        default=1,
        help="Include workbook assay slots 1..N only (default 1 = primary slot; use 3 for all).",
    )
    p.add_argument(
        "--grain",
        choices=("patient", "strict"),
        default="patient",
        help="patient = one slot-1 row per workbook patient; strict = evidence-filtered rows only.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    xlsx = Path(args.excel).expanduser().resolve()
    if not xlsx.is_file():
        raise SystemExit(f"Input not found: {xlsx}")

    print(f"Reading {xlsx.name} …")
    wide = pd.read_excel(xlsx, sheet_name=0)
    long_df = wide_to_long_molecular_testing(
        wide, max_slot=int(args.max_slot), grain=str(args.grain)
    )
    print(f"  Long molecular_testing rows: {len(long_df):,}")

    from utils.md_connect import connect_md_or_file

    if (args.database or "").strip():
        os.environ["MOTHERDUCK_DATABASE"] = str(args.database).strip()
    elif args.md and not os.environ.get("MOTHERDUCK_DATABASE") and not os.environ.get(
        "MOTHERDUCK_DB"
    ):
        from motherduck_client import resolve_database_for_env

        os.environ["MOTHERDUCK_DATABASE"] = resolve_database_for_env(
            (args.md_env or os.getenv("MOTHERDUCK_ENV") or "prod").strip()
        )

    con = connect_md_or_file(
        DB_PATH,
        md=args.md,
        fail_closed=args.md,
        prefer_service_account=bool(args.md_sa),
        env=args.md_env,
        custom_user_agent=os.getenv(
            "MOTHERDUCK_CUSTOM_USER_AGENT",
            "THYROID_2026_scripts/145_md_materialize_molecular_testing",
        ),
        motherduck_session_hint=os.getenv("MOTHERDUCK_SESSION_HINT"),
    )

    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.register("_mt_long_raw", long_df)
    con.execute(
        """
        CREATE OR REPLACE TABLE main.molecular_testing AS
        SELECT
            CAST(research_id AS INTEGER) AS research_id,
            thyroseq_afirma,
            genetic_test_performed,
            genetic_test,
            result,
            mutation,
            detailed_findings,
            nodule_info,
            fna_bethesda,
            CAST(test_index AS INTEGER) AS test_index,
            _raw_date AS date
        FROM _mt_long_raw
        """
    )
    con.unregister("_mt_long_raw")
    n_mt = con.execute("SELECT COUNT(*) FROM main.molecular_testing").fetchone()[0]
    print(f"  main.molecular_testing: {n_mt:,} rows")

    if args.skip_episode_rebuild:
        con.close()
        return

    episode_sql = _load_episode_sql()
    print("Rebuilding molecular_test_episode_v2 …")
    con.execute(episode_sql)
    n_ep = con.execute(
        "SELECT COUNT(*) FROM main.molecular_test_episode_v2"
    ).fetchone()[0]
    n_dated = con.execute(
        "SELECT COUNT(*) FROM main.molecular_test_episode_v2 WHERE test_date_native IS NOT NULL"
    ).fetchone()[0]
    print(f"  main.molecular_test_episode_v2: {n_ep:,} rows; with test_date_native: {n_dated:,}")

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
