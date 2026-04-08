#!/usr/bin/env python3
"""Merge MOTHERDUCK_TOKEN from stdin into gitignored .streamlit/secrets.toml.

Reads one line (the JWT / PAT). Does not echo the secret. Safe to commit.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SECRETS = ROOT / ".streamlit" / "secrets.toml"


def main() -> int:
    token = (sys.stdin.read() or "").strip()
    if not token:
        print("ERROR: provide token on stdin (one line).", file=sys.stderr)
        return 1
    SECRETS.parent.mkdir(parents=True, exist_ok=True)
    try:
        import toml  # type: ignore
    except ImportError:
        print("ERROR: install toml (bundled with streamlit env).", file=sys.stderr)
        return 1
    data: dict = {}
    if SECRETS.exists():
        data = toml.load(str(SECRETS)) or {}
    data["MOTHERDUCK_TOKEN"] = token
    with SECRETS.open("w", encoding="utf-8") as fh:
        toml.dump(data, fh)
    print(f"OK: wrote merged secrets to {SECRETS.relative_to(ROOT)} (keys: {len(data)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
