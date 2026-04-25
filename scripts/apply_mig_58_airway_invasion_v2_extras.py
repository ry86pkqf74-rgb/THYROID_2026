#!/usr/bin/env python3
"""Idempotent follow-up to qc_framework_v1/migrations/58_airway_invasion_v2_*.sql

- Renames main.note_entities_llm_airway_invasion →
  main._deprecated_note_entities_llm_airway_invasion when the former still exists
- Registers the deprecated table in detail_table_registry_v1 (replaces any row for
  note_entities_llm_airway_invasion or the _deprecated_ name)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

os.environ.setdefault("MOTHERDUCK_DATABASE", "thyroid_canonical_publication_v1_0")

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402


def _table_exists(con, schema: str, name: str) -> bool:
    n = con.execute(
        """
        SELECT COUNT(*)::BIGINT
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, name],
    ).fetchone()[0]
    return int(n) > 0


def main() -> int:
    client = MotherDuckClient(MotherDuckConfig())
    con = client.connect_rw()

    legacy = "note_entities_llm_airway_invasion"
    depr = "_deprecated_note_entities_llm_airway_invasion"
    if _table_exists(con, "main", legacy) and not _table_exists(con, "main", depr):
        con.execute(f'ALTER TABLE main."{legacy}" RENAME TO "{depr}"')
        print(f"[OK] Renamed main.{legacy} → main.{depr}")
    elif _table_exists(con, "main", depr):
        print(f"[skip] main.{depr} already present")
    else:
        print(f"[skip] main.{legacy} not found (nothing to rename)")

    if not _table_exists(con, "main", depr):
        print("[done] no deprecated table to register")
        con.close()
        return 0

    con.execute(
        """
        COMMENT ON TABLE main._deprecated_note_entities_llm_airway_invasion IS
        '[deprecated 2026-04-24] — superseded by main.note_entities_llm_airway_invasion_v2
        and main.canonical_airway_invasion_events_v1. Renamed from note_entities_llm_airway_invasion.'
        """
    )
    con.execute(
        f"""
        DELETE FROM manuscript_workspace.detail_table_registry_v1
        WHERE detail_table_name IN ('{legacy}', '{depr}')
        """
    )
    con.execute(
        f"""
        INSERT INTO manuscript_workspace.detail_table_registry_v1
          (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
           domain, feeds_master_columns, description, canonical_version)
        SELECT
          '{depr}',
          'main',
          'research_id',
          'one row per note entity (legacy v1; DEPRECATED — do not use for new work)',
          COUNT(*),
          COUNT(DISTINCT research_id),
          'invasion / pathology NLP (deprecated)',
          'legacy airway columns; use note_entities_llm_airway_invasion_v2 + canonical_airway_invasion_*',
          '2026-04-24: Renamed from note_entities_llm_airway_invasion; superseded by v2 + canonical_airway_invasion.',
          'deprecated'
        FROM main."{depr}"
        """
    )
    print(
        "[OK] detail_table_registry_v1: registered _deprecated_note_entities_llm_airway_invasion"
    )
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
