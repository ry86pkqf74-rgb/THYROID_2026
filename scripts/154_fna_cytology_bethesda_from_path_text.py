#!/usr/bin/env python3
"""
Deterministic Bethesda class extraction from fna_cytology.path_text into category_num,
then propagate to fna_episode_master_v2 (same join as script 152).

Patterns (case-insensitive path_text), first match wins:
  1) bethesda\\s*category\\s*(VI|IV|III|II|V|I|[1-6])\\b — Roman order in group
  2) cytopathology\\s*category\\s*(VI|IV|III|II|V|I|[1-6])\\b  (e.g. Bethesda system …)

Phase A — Python: UPDATE fna_cytology rows (deduped per research_id, fna_index like script 152).
Phase B — SQL: UPDATE fna_episode_master_v2 from cytology (same as script 152).
Phase C — export residual NULL episode worklist CSV + manifest to exports/.

Run:
  .venv/bin/python scripts/154_fna_cytology_bethesda_from_path_text.py --md
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_fail_closed  # noqa: E402

# Primary: "Bethesda Category VI" / Arabic 1–6
_REG_PRIMARY = (
    r"bethesda\s*category\s*(vi|iv|iii|ii|v|i|[1-6])\b"
)
# Secondary: "... thyroid cytopathology category III ..."
_REG_SECONDARY = r"cytopathology\s*category\s*(vi|iv|iii|ii|v|i|[1-6])\b"

BACKFILL_EPISODE_SQL = """
WITH cy AS (
    SELECT
        research_id,
        fna_index,
        category_num,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, fna_index
            ORDER BY COALESCE(confidence, 0) DESC NULLS LAST,
                     ingested_at_utc DESC NULLS LAST
        ) AS rn
    FROM fna_cytology
    WHERE category_num IS NOT NULL
)
UPDATE fna_episode_master_v2 AS e
SET bethesda_category = cy.category_num
FROM cy
WHERE cy.rn = 1
  AND CAST(e.research_id AS BIGINT) = CAST(cy.research_id AS BIGINT)
  AND CAST(e.fna_episode_id AS BIGINT) = CAST(cy.fna_index AS BIGINT)
  AND e.bethesda_category IS NULL
"""

WORKLIST_SQL = """
SELECT
    e.research_id,
    e.fna_episode_id,
    e.bethesda_raw,
    e.pathology_diagnosis,
    LEFT(CAST(e.pathology_extended AS VARCHAR), 200) AS pathology_extended_snip,
    e.resolved_fna_date,
    v.bethesda_unscorable_reason,
    v.bethesda_cytology_num
FROM fna_episode_master_v2 e
LEFT JOIN v_fna_episode_bethesda_resolved_v1 v
    ON v.research_id = e.research_id AND v.fna_episode_id = e.fna_episode_id
WHERE e.bethesda_category IS NULL
ORDER BY v.bethesda_unscorable_reason, e.research_id, e.fna_episode_id
"""


def _map_token(tok: str | None) -> int | None:
    if tok is None or tok == "":
        return None
    t = tok.strip().lower()
    rom = {"vi": 6, "iv": 4, "iii": 3, "ii": 2, "v": 5, "i": 1}
    if t in rom:
        return rom[t]
    if len(t) == 1 and t in "123456":
        return int(t)
    return None


def extract_bethesda_from_path_text(path_text: str | object | None) -> int | None:
    """Deterministic parse; used by Phase A and unit tests."""
    if path_text is None or (isinstance(path_text, float) and np.isnan(path_text)):
        return None
    if pd.isna(path_text):
        return None
    s = str(path_text)
    m = re.search(_REG_PRIMARY, s, re.I)
    if m:
        return _map_token(m.group(1))
    m = re.search(_REG_SECONDARY, s, re.I)
    if m:
        return _map_token(m.group(1))
    return None


def _dedupe_cytology_window(df: pd.DataFrame) -> pd.DataFrame:
    """Keep winning row per (research_id, fna_index); aligns with view / 152."""
    if df.empty:
        return df
    df = df.copy()
    df["confidence_num"] = pd.to_numeric(df.get("confidence"), errors="coerce").fillna(0)
    ing = "ingested_at_utc"
    if ing not in df.columns:
        df[ing] = pd.NaT
    df["ingested_at_utc"] = pd.to_datetime(df[ing], errors="coerce", utc=True)
    df = df.sort_values(
        by=["research_id", "fna_index", "confidence_num", "ingested_at_utc"],
        ascending=[True, True, False, False],
        na_position="last",
    )
    return df.drop_duplicates(subset=["research_id", "fna_index"], keep="first")


def phase_a_update_cytology(con: object, dry_run: bool) -> int:
    """Returns count of fna_cytology cells set."""
    df = con.execute(
        """
        SELECT research_id, fna_index, path_text, category_num,
               confidence, ingested_at_utc
        FROM fna_cytology
        WHERE category_num IS NULL
          AND path_text IS NOT NULL
          AND TRIM(CAST(path_text AS VARCHAR)) <> ''
        """
    ).fetchdf()
    if df.empty:
        return 0
    win = _dedupe_cytology_window(df)
    updates: list[tuple[int, int, int]] = []
    for _, r in win.iterrows():
        cat = extract_bethesda_from_path_text(r.get("path_text"))
        if cat is not None:
            updates.append((int(cat), int(r["research_id"]), int(r["fna_index"])))
    if dry_run or not updates:
        return len(updates)
    for cat, rid, fidx in updates:
        con.execute(
            """
            UPDATE fna_cytology
            SET category_num = ?
            WHERE CAST(research_id AS BIGINT) = ?
              AND CAST(fna_index AS BIGINT) = ?
              AND category_num IS NULL
            """,
            [cat, rid, fidx],
        )
    return len(updates)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--md", action="store_true", help="MotherDuck (fail-closed)")
    p.add_argument("--dry-run", action="store_true", help="Counts only, no UPDATE")
    p.add_argument(
        "--skip-episode",
        action="store_true",
        help="After cytology UPDATE, skip fna_episode_master_v2 propagation",
    )
    p.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip worklist CSV export",
    )
    args = p.parse_args()
    if not args.md:
        args.md = True

    con = connect_md_fail_closed(ROOT / "thyroid_master.duckdb")

    prev_null_cy = int(
        con.execute("SELECT COUNT(*) FROM fna_cytology WHERE category_num IS NULL").fetchone()[0]
    )
    prev_null_ep = int(
        con.execute(
            "SELECT COUNT(*) FROM fna_episode_master_v2 WHERE bethesda_category IS NULL"
        ).fetchone()[0]
    )

    print(f"[154] fna_cytology NULL category_num before: {prev_null_cy}")
    print(f"[154] fna_episode_master_v2 NULL bethesda before: {prev_null_ep}")

    n_derivable = phase_a_update_cytology(con, dry_run=args.dry_run)
    tag = "dry-run count" if args.dry_run else "updated"
    print(f"[154] Phase A {tag} (deduped per research_id,fna_index): {n_derivable}")

    if not args.dry_run:
        after_null_cy = int(
            con.execute("SELECT COUNT(*) FROM fna_cytology WHERE category_num IS NULL").fetchone()[0]
        )
        print(f"[154] fna_cytology NULL category_num after Phase A: {after_null_cy}")
    else:
        after_null_cy = prev_null_cy

    if not args.dry_run and not args.skip_episode:
        con.execute(BACKFILL_EPISODE_SQL)
        after_null_ep = int(
            con.execute(
                "SELECT COUNT(*) FROM fna_episode_master_v2 WHERE bethesda_category IS NULL"
            ).fetchone()[0]
        )
        print(f"[154] fna_episode_master_v2 NULL bethesda after Phase B: {after_null_ep}")
    else:
        after_null_ep = prev_null_ep

    if not args.dry_run and not args.skip_export:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_dir = ROOT / "exports" / f"fna_bethesda_residual_worklist_{ts}"
        out_dir.mkdir(parents=True, exist_ok=True)
        wl = con.execute(WORKLIST_SQL).fetchdf()
        csv_path = out_dir / "fna_bethesda_residual_worklist.csv"
        wl.to_csv(csv_path, index=False)
        manifest = {
            "script": "154_fna_cytology_bethesda_from_path_text.py",
            "utc": datetime.now(timezone.utc).isoformat(),
            "cytology_null_category_before": prev_null_cy,
            "cytology_null_category_after": after_null_cy,
            "episode_null_bethesda_before": prev_null_ep,
            "episode_null_bethesda_after": after_null_ep,
            "residual_worklist_rows": len(wl),
            "worklist_csv": str(csv_path.relative_to(ROOT)),
        }
        (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"[154] worklist rows: {len(wl)} -> {csv_path.relative_to(ROOT)}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
