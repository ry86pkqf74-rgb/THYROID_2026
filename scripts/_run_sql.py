#!/usr/bin/env python3
"""
Generic SQL runner for the canonical_v1_0 finalization scripts (237-247).

Reads a .sql file containing multiple statements separated by `;` on its own
line (or end of line), executes each against MotherDuck via
`_md_connect.connect_locked()`, and prints timestamped progress. Lines
starting with `-- LOG:` are echoed verbatim before the *next* statement
executes. Lines starting with `-- ASSERT:` mark the immediately-following
statement as an assertion: it must return a single row whose first column is
TRUE / 1 / 'PASS'; otherwise the runner exits non-zero with a diagnostic.

Usage:
    .venv/bin/python scripts/_run_sql.py scripts/237_document_fna_size_gap.sql
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked  # noqa: E402


def ts() -> str:
    return datetime.utcnow().strftime("%H:%M:%S.") + f"{datetime.utcnow().microsecond//1000:03d}Z"


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


# Split on ';' that ends a logical statement. We use a simple state machine
# that tolerates single-quoted strings (with '' escaping) and -- line
# comments. This is sufficient for the handwritten scripts in this series.
def split_statements(sql: str) -> list[tuple[list[str], str]]:
    """Return a list of (preceding_directive_lines, statement_sql) tuples."""
    statements: list[tuple[list[str], str]] = []
    pending_directives: list[str] = []
    buf: list[str] = []
    in_string = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    n = len(sql)
    line_start_buf: list[str] = []

    def flush(stmt: str) -> None:
        s = stmt.strip()
        if not s:
            return
        # Strip trailing semicolon if present.
        if s.endswith(";"):
            s = s[:-1].rstrip()
        if not s:
            return
        statements.append((list(pending_directives), s))
        pending_directives.clear()

    # Walk lines (because directives are line-prefix based).
    def buf_is_empty() -> bool:
        return not buf or all(ch in " \t\r\n" for ch in buf)

    for raw_line in sql.splitlines(keepends=True):
        stripped = raw_line.strip()
        # If the buffer is empty (we're between statements) and the line is a
        # directive comment, capture it for the next statement.
        if buf_is_empty() and (stripped.startswith("-- LOG:") or stripped.startswith("-- ASSERT:")):
            buf.clear()
            pending_directives.append(stripped)
            continue
        # If the buffer is empty and the line is a plain comment / blank,
        # ignore it (don't carry into the statement).
        if buf_is_empty() and (stripped.startswith("--") or not stripped):
            buf.clear()
            continue
        # Otherwise, walk the line char-by-char looking for an unquoted ';'.
        j = 0
        line_len = len(raw_line)
        while j < line_len:
            c = raw_line[j]
            buf.append(c)
            if in_block_comment:
                if c == "*" and j + 1 < line_len and raw_line[j + 1] == "/":
                    buf.append(raw_line[j + 1])
                    j += 2
                    in_block_comment = False
                    continue
                j += 1
                continue
            if in_line_comment:
                if c == "\n":
                    in_line_comment = False
                j += 1
                continue
            if in_string:
                if c == "'":
                    if j + 1 < line_len and raw_line[j + 1] == "'":
                        buf.append(raw_line[j + 1])
                        j += 2
                        continue
                    in_string = False
                j += 1
                continue
            # Not in any special context.
            if c == "'":
                in_string = True
                j += 1
                continue
            if c == "-" and j + 1 < line_len and raw_line[j + 1] == "-":
                # Inline comment until newline.
                in_line_comment = True
                buf.append(raw_line[j + 1])
                j += 2
                continue
            if c == "/" and j + 1 < line_len and raw_line[j + 1] == "*":
                in_block_comment = True
                buf.append(raw_line[j + 1])
                j += 2
                continue
            if c == ";":
                flush("".join(buf))
                buf = []
                j += 1
                continue
            j += 1
    if buf:
        flush("".join(buf))
    return statements


def run_file(path: Path, dry_run: bool = False) -> int:
    log(f"=== START {path.name}")
    if not path.is_file():
        log(f"ERROR: file not found: {path}")
        return 2
    sql = path.read_text()
    statements = split_statements(sql)
    log(f"parsed {len(statements)} statements from {path.name}")

    if dry_run:
        for i, (directives, stmt) in enumerate(statements, 1):
            for d in directives:
                log(f"  (dry) {d}")
            preview = stmt.strip().splitlines()[0][:90]
            log(f"  (dry) [{i}] {preview} ...")
        return 0

    con = connect_locked()
    log("connected to MotherDuck (publication DB locked)")

    failures = 0
    t0 = time.time()
    for i, (directives, stmt) in enumerate(statements, 1):
        is_assert = False
        for d in directives:
            log(d.replace("-- ", "").strip())
            if d.strip().startswith("-- ASSERT:"):
                is_assert = True
        s_t0 = time.time()
        try:
            if is_assert:
                row = con.execute(stmt).fetchone()
                ok = bool(row) and (
                    row[0] is True
                    or row[0] == 1
                    or (isinstance(row[0], str) and row[0].upper() == "PASS")
                )
                dur = (time.time() - s_t0) * 1000
                if ok:
                    log(f"  [{i}] ASSERT PASS ({dur:.0f} ms)  row={row}")
                else:
                    log(f"  [{i}] ASSERT FAIL ({dur:.0f} ms)  row={row}")
                    log(f"        SQL: {stmt[:200]}")
                    failures += 1
            else:
                con.execute(stmt)
                dur = (time.time() - s_t0) * 1000
                first = stmt.strip().splitlines()[0][:90]
                log(f"  [{i}] OK ({dur:.0f} ms)  {first}")
        except Exception as e:
            dur = (time.time() - s_t0) * 1000
            log(f"  [{i}] ERROR ({dur:.0f} ms): {e}")
            log(f"        SQL: {stmt[:300]}")
            failures += 1
            # Stop on first error — let the user inspect.
            break
    elapsed = time.time() - t0
    log(f"=== END {path.name}  elapsed={elapsed:.1f}s  failures={failures}")
    return 1 if failures else 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sql_file")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    rc = run_file(Path(args.sql_file).resolve(), dry_run=args.dry_run)
    sys.exit(rc)


if __name__ == "__main__":
    main()
