"""Canonical cleanup 20260417 — Phase 5.1 + 5.2 (inventory + classification).

Builds manuscript_workspace.canonical_cleanup_audit_v1 listing every object in
thyroid_canonical_publication_v1_0.main with:
  object_name, object_type, row_count, created_at (NULL — DuckDB info_schema
  does not expose creation timestamps), status, reason, destination

Classification heuristics (read-only against MD; STOPS before any 5.3 action):

  DEPRECATE  - object_name starts with 'DEPRECATED__'
  ARCHIVE    - object_name starts with 'ARCHIVE__'
  DELETE     - object_name starts with 'md_' AND a non-md_ counterpart
                exists in main with identical row_count and column set; row_count
                comparison only (full content hash deferred to Phase 5.3 with
                motherduck-dedup-hygiene skill); OR row_count = 0
                (empty placeholder).
  LIVE       - referenced (substring match) in any view definition in either
                main or manuscript_workspace.
  KEEP_REVIEW - everything else (default safe bucket).

Output also goes to studies/canonical_cleanup_20260417/phase5_inventory.json.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HERE = REPO / "studies" / "canonical_cleanup_20260417"
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

LOG_PATH = HERE / "phase5_inventory.log"
JSON_PATH = HERE / "phase5_inventory.json"


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def collect_script_references() -> list:
    """Return Python source file paths to scan for table-name substrings.

    Object names <= 3 chars are skipped at use-site to avoid false positives.
    """
    roots = [REPO / "scripts", REPO / "llm_extraction", REPO / "app",
             REPO / "utils"]
    files: list[Path] = []
    for root in roots:
        if root.exists():
            files.extend(root.rglob("*.py"))
    log(f"scanning {len(files)} python files for table references")
    return files  # placeholder; resolved later in main with per-object pass


def main() -> int:
    HERE.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("")
    con = connect_locked()
    log("=== Phase 5 inventory start ===")
    py_files = collect_script_references()
    file_blobs: list[tuple[str, str]] = []
    for p in py_files:
        try:
            file_blobs.append((str(p), p.read_text(errors="ignore")))
        except Exception as e:  # noqa: BLE001
            log(f"  WARN read {p}: {e}")
    log(f"loaded {len(file_blobs)} python source blobs")

    # 1. List all base tables (and views) in main.
    objects = con.execute(
        """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
          AND table_schema = 'main'
        ORDER BY table_name
        """
    ).fetchall()
    log(f"main schema objects: {len(objects)}")

    # 2. Reference index: collect view text from BOTH schemas.
    view_texts = con.execute(
        """
        SELECT table_schema, table_name, LOWER(view_definition) AS d
        FROM information_schema.views
        WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
          AND view_definition IS NOT NULL
        """
    ).fetchall()
    log(f"view definitions captured: {len(view_texts)}")

    # 3. Build column-set index per object so we can compare md_/bare pairs
    col_idx: dict[tuple[str, str], list[str]] = {}
    rows = con.execute(
        """
        SELECT table_schema, table_name, column_name, ordinal_position
        FROM information_schema.columns
        WHERE table_catalog='thyroid_canonical_publication_v1_0'
          AND table_schema='main'
        ORDER BY table_schema, table_name, ordinal_position
        """
    ).fetchall()
    for sch, tbl, col, _ in rows:
        col_idx.setdefault((sch, tbl), []).append(col)

    # 4. Per-object row counts (chunked to avoid one giant query).
    def safe_count(table: str) -> int | None:
        try:
            return con.execute(f'SELECT COUNT(*) FROM main."{table}"').fetchone()[0]
        except Exception as e:  # noqa: BLE001
            log(f"  WARN: count failed for {table}: {e}")
            return None

    inventory = []
    bare_names = {n for n, _ in objects if not n.startswith("md_")}
    for name, otype in objects:
        n_rows = safe_count(name)
        # Reference detection: substring match in any view definition.
        ref_hits = []
        for sch, vname, d in view_texts:
            if name.lower() in d:
                ref_hits.append(f"{sch}.{vname}")
        # Reference detection #2: substring match in Python source files.
        py_hits: list[str] = []
        if len(name) > 3:
            for path, blob in file_blobs:
                if name in blob:
                    py_hits.append(path.replace(str(REPO) + "/", ""))
                    if len(py_hits) >= 5:
                        break
        # Classify
        status = "KEEP_REVIEW"
        reason_parts: list[str] = []
        destination = ""
        if name.startswith("DEPRECATED__"):
            status = "DEPRECATE"
            reason_parts.append("already labeled DEPRECATED__")
        elif name.startswith("ARCHIVE__"):
            status = "ARCHIVE"
            reason_parts.append("already labeled ARCHIVE__")
            destination = (
                f'"Thyroid 2026 UPdated".archive_pub_v1_0.{name}_pre_bigcleanup_<UTC>'
            )
        else:
            # md_ duplicate check
            if name.startswith("md_"):
                bare = name[3:]
                if bare in bare_names:
                    bare_count = safe_count(bare)
                    bare_cols = col_idx.get(("main", bare), [])
                    md_cols = col_idx.get(("main", name), [])
                    if (n_rows is not None and bare_count is not None
                            and n_rows == bare_count and set(md_cols) == set(bare_cols)):
                        status = "DELETE"
                        reason_parts.append(
                            f"md_ duplicate of {bare} (same rowcount={n_rows}, same columns)"
                        )
                    else:
                        status = "KEEP_REVIEW"
                        reason_parts.append(
                            f"md_ prefix but content/cols differ from {bare} "
                            f"(md_rows={n_rows} bare_rows={bare_count})"
                        )
            elif n_rows == 0:
                status = "DELETE"
                reason_parts.append("empty placeholder (row_count=0)")
            elif ref_hits or py_hits:
                status = "LIVE"
                bits = []
                if ref_hits:
                    bits.append(f"{len(ref_hits)} view(s)")
                if py_hits:
                    bits.append(f"{len(py_hits)}+ python ref(s)")
                reason_parts.append("referenced by " + ", ".join(bits))
            else:
                status = "KEEP_REVIEW"
                reason_parts.append(
                    "non-empty and not referenced by any view or python script"
                )

        inventory.append({
            "object_name": name,
            "object_type": otype,
            "row_count": n_rows,
            "n_view_references": len(ref_hits),
            "view_references": ref_hits[:5],  # cap for readability
            "n_python_references": len(py_hits),
            "python_references": py_hits[:5],
            "n_columns": len(col_idx.get(("main", name), [])),
            "status": status,
            "reason": "; ".join(reason_parts),
            "destination": destination,
        })

    # 5. Write to manuscript_workspace.canonical_cleanup_audit_v1
    con.execute("DROP TABLE IF EXISTS manuscript_workspace.canonical_cleanup_audit_v1")
    con.execute(
        """
        CREATE TABLE manuscript_workspace.canonical_cleanup_audit_v1 (
          object_name        VARCHAR,
          object_type        VARCHAR,
          row_count          BIGINT,
          n_view_references  INTEGER,
          n_columns          INTEGER,
          status             VARCHAR,
          reason             VARCHAR,
          destination        VARCHAR,
          created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    for inv in inventory:
        con.execute(
            """
            INSERT INTO manuscript_workspace.canonical_cleanup_audit_v1
              (object_name, object_type, row_count, n_view_references,
               n_columns, status, reason, destination)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                inv["object_name"], inv["object_type"], inv["row_count"],
                inv["n_view_references"], inv["n_columns"], inv["status"],
                inv["reason"], inv["destination"],
            ],
        )
    log(f"manuscript_workspace.canonical_cleanup_audit_v1 written ({len(inventory)} rows)")

    # 6. Summary
    summary: dict[str, int] = {}
    for inv in inventory:
        summary[inv["status"]] = summary.get(inv["status"], 0) + 1
    log(f"classification summary: {summary}")

    JSON_PATH.write_text(json.dumps(
        {"summary": summary, "objects": inventory}, indent=2, default=str
    ))
    log(f"json written -> {JSON_PATH}")
    log("=== Phase 5 inventory end (HALT before 5.3 actions) ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
