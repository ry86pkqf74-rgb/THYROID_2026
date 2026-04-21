#!/usr/bin/env python3
"""Phase 6 (views portion): repoint views_readable.US_Reports_Raw and
views_readable.US_Nodules_TIRADS from main.* to raw.*."""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent
sys.path.insert(0, str(SCRIPTS))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB


def main() -> int:
    con = connect_locked()

    print("CREATE OR REPLACE VIEW views_readable.US_Reports_Raw "
          "AS SELECT * FROM raw.ultrasound_reports")
    con.execute(
        f"CREATE OR REPLACE VIEW {PUB}.views_readable.US_Reports_Raw "
        f"AS SELECT * FROM {PUB}.raw.ultrasound_reports"
    )

    print("CREATE OR REPLACE VIEW views_readable.US_Nodules_TIRADS "
          "AS SELECT * FROM raw.us_nodules_tirads")
    con.execute(
        f"CREATE OR REPLACE VIEW {PUB}.views_readable.US_Nodules_TIRADS "
        f"AS SELECT * FROM {PUB}.raw.us_nodules_tirads"
    )

    n_a = con.execute(
        f"SELECT COUNT(*) FROM {PUB}.views_readable.US_Reports_Raw"
    ).fetchone()[0]
    n_b = con.execute(
        f"SELECT COUNT(*) FROM {PUB}.views_readable.US_Nodules_TIRADS"
    ).fetchone()[0]
    print(f"  US_Reports_Raw rows: {n_a}")
    print(f"  US_Nodules_TIRADS rows: {n_b}")
    if n_a != 6793 or n_b != 10859:
        raise SystemExit(
            f"Unexpected row counts: US_Reports_Raw={n_a} (expected 6793), "
            f"US_Nodules_TIRADS={n_b} (expected 10859)"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
