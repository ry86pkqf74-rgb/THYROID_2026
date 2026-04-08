"""MotherDuck query-history attribution for THYROID_2026 molecular pipelines.

Surfaces in MotherDuck ``MD_INFORMATION_SCHEMA.QUERY_HISTORY`` / ``RECENT_QUERIES``
as ``USER_AGENT`` and ``SESSION_NAME`` (session hint).

Convention
----------
* **User-Agent:** ``THYROID_2026_molecular/<component>;kind=<run_kind>``
* **Session hint:** ``thyroid2026:<run_kind>:<git_short_sha>``

``run_kind`` is one of: ``ingest``, ``lineage``, ``contract``, ``materialize``,
``validate``, ``release``.

Environment overrides (highest precedence — unchanged from ``motherduck_client``):

* ``MOTHERDUCK_CUSTOM_USER_AGENT`` — if set, :func:`molecular_custom_user_agent` returns it as-is.
* ``MOTHERDUCK_SESSION_HINT`` — if set, :func:`molecular_session_hint` returns it as-is.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent


def git_sha_short(cwd: Path | None = None) -> str:
    """Short git SHA for the repo; never reads secrets."""
    root = cwd or _REPO_ROOT
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        return out or "unknown"
    except Exception:
        gh = (os.environ.get("GITHUB_SHA") or "").strip()
        return gh[:7] if len(gh) >= 7 else (gh or "unknown")


def molecular_custom_user_agent(component: str, run_kind: str) -> str:
    """Return ``custom_user_agent`` for MotherDuck connection strings."""
    override = (os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT") or "").strip()
    if override:
        return override
    comp = (component or "unknown").strip().replace(" ", "_")
    kind = (run_kind or "unknown").strip().lower()
    return f"THYROID_2026_molecular/{comp};kind={kind}"


def molecular_session_hint(run_kind: str) -> str | None:
    """Return ``motherduck_session_hint`` or None to let the client skip SET."""
    override = (os.environ.get("MOTHERDUCK_SESSION_HINT") or "").strip()
    if override:
        return override
    kind = (run_kind or "unknown").strip().lower()
    sha = git_sha_short()
    return f"thyroid2026:{kind}:{sha}"


def connect_attribution(
    *,
    component: str,
    run_kind: str,
) -> tuple[str, str | None]:
    """Tuple of (custom_user_agent, session_hint) for ``connect_md_or_file``."""
    return (molecular_custom_user_agent(component, run_kind), molecular_session_hint(run_kind))


# Specimen / FHIR release writers (138/139/140/143) — single UA for query-history filtering.
SPECIMEN_FHIR_RELEASE_TRUTH_UA = "specimen_fhir_release_truth_v2"
SPECIMEN_FHIR_RELEASE_TRUTH_SESSION_HINT_DEFAULT = "specimen_fhir_release_truth_v2"


def specimen_fhir_release_writer_attribution() -> tuple[str, str]:
    """Return (custom_user_agent, motherduck_session_hint) for specimen/FHIR MD writes.

    ``MOTHERDUCK_CUSTOM_USER_AGENT`` / ``MOTHERDUCK_SESSION_HINT`` override defaults when set.
    """
    ua = (os.environ.get("MOTHERDUCK_CUSTOM_USER_AGENT") or "").strip() or SPECIMEN_FHIR_RELEASE_TRUTH_UA
    hint = (os.environ.get("MOTHERDUCK_SESSION_HINT") or "").strip() or SPECIMEN_FHIR_RELEASE_TRUTH_SESSION_HINT_DEFAULT
    return ua, hint
