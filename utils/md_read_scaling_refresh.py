"""MotherDuck read-scaling freshness helpers.

Writer (read/write duckling): ``CREATE SNAPSHOT OF <database>`` makes the latest
state visible for read-scaling replicas.

Reader (read-scaling duckling): ``REFRESH DATABASE`` / ``REFRESH DATABASES``
pulls snapshots. Automatic sync is typically ~1 minute; this pair guarantees
freshness when you need it.

See: https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/read-scaling
"""

from __future__ import annotations

import re
from typing import Literal

# Letters, digits, space, underscore — conservative guard (no quotes / SQL injection)
_SAFE_DB_NAME = re.compile(r"^[A-Za-z0-9_ ]+$")
_SAFE_SNAPSHOT_NAME = re.compile(r"^[A-Za-z0-9_]+$")


def _quote_db(database: str) -> str:
    db = database.strip()
    if not db or not _SAFE_DB_NAME.match(db):
        raise ValueError(f"unsafe or empty database name: {database!r}")
    esc = db.replace('"', "")
    if esc != db:
        raise ValueError("database name must not contain double quotes")
    return f'"{esc}"'


def sql_create_snapshot(database: str, snapshot_name: str | None = None) -> str:
    """Return ``CREATE SNAPSHOT ... OF ...`` SQL for a writer connection."""
    quoted = _quote_db(database)
    if snapshot_name:
        sn = snapshot_name.strip()
        if not sn or not _SAFE_SNAPSHOT_NAME.match(sn):
            raise ValueError(f"unsafe snapshot name: {snapshot_name!r}")
        return f"CREATE SNAPSHOT {sn} OF {quoted}"
    return f"CREATE SNAPSHOT OF {quoted}"


def sql_refresh_database(
    database: str | None,
    *,
    mode: Literal["single", "all"] = "single",
) -> str:
    """Return ``REFRESH DATABASE`` SQL for a read-scaling reader.

    *database* — concrete DB name, or None with *mode* ``all`` for ``REFRESH DATABASES``.
    """
    if mode == "all":
        return "REFRESH DATABASES"
    if database is None:
        raise ValueError("database is required when mode='single'")
    quoted = _quote_db(database)
    return f"REFRESH DATABASE {quoted}"


def run_writer_snapshot(
    con,
    database: str,
    snapshot_name: str | None = None,
) -> str:
    """Execute snapshot DDL on a writer; returns the SQL executed."""
    sql = sql_create_snapshot(database, snapshot_name=snapshot_name)
    con.execute(sql)
    return sql


def run_reader_refresh(
    con,
    database: str | None,
    *,
    refresh_all: bool = False,
) -> str:
    """Execute refresh on a read-scaling reader; returns the SQL executed."""
    if refresh_all:
        sql = sql_refresh_database(None, mode="all")
    else:
        sql = sql_refresh_database(database, mode="single")
    con.execute(sql)
    return sql
