#!/usr/bin/env python3
"""Merge MD_READ_SCALING_TOKEN from stdin into gitignored .streamlit/secrets.toml.

Reads one line (MotherDuck Business read-scaling PAT). Does not echo the secret.
Use a **reader** token only — never use the read/write token here.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SECRETS = ROOT / ".streamlit" / "secrets.toml"


def main() -> int:
    token = (sys.stdin.read() or "").strip()
    if not token:
        print("ERROR: provide read-scaling token on stdin (one line).", file=sys.stderr)
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
    data["MD_READ_SCALING_TOKEN"] = token
    with SECRETS.open("w", encoding="utf-8") as fh:
        toml.dump(data, fh)
    print(
        f"OK: wrote MD_READ_SCALING_TOKEN to {SECRETS.relative_to(ROOT)} "
        f"(total keys: {len(data)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
