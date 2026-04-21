#!/usr/bin/env python3
"""Connection helper for the adjudication agent run."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["MOTHERDUCK_DATABASE"] = "thyroid_canonical_publication_v1_0"

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402


def connect_rw():
    cfg = MotherDuckConfig(database="thyroid_canonical_publication_v1_0")
    return MotherDuckClient(cfg).connect_rw()


if __name__ == "__main__":
    con = connect_rw()
    print("db=", con.execute("select current_database()").fetchone()[0])
    con.close()
