#!/usr/bin/env python3
"""
08_integrate_missing_sources.py — Phase 6 ETL entry point

Wrapper that calls the master integration script.
Fits into the existing 01-07 pipeline sequence.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def etl_missing_sources() -> None:
    """Run the Phase 6 integration of 8 missing high-value Excel sources."""
    script = ROOT / "integrate_missing_sources.py"
    if not script.exists():
        print(f"ERROR: {script} not found")
        sys.exit(1)
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(ROOT),
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    etl_missing_sources()
