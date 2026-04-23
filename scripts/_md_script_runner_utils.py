"""Shared helpers for MotherDuck one-off scripts (393+, probe → apply runners).

* SHA256 of probe report body (UTF-8) for human-in-the-loop --i-approve gate.
* Normalize CLI tokens: optional ``sha256:`` prefix, lowercase hex.
"""
from __future__ import annotations

import hashlib
import re


def probe_report_sha256(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def normalize_i_approve(token: str) -> str | None:
    """Return 64-char lowercase hex or None if invalid."""
    s = token.strip().lower()
    if s.startswith("sha256:"):
        s = s[7:].strip()
    if not re.fullmatch(r"[0-9a-f]{64}", s):
        return None
    return s
