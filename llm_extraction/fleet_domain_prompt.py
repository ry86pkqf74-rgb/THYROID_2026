"""Per-domain prompt filenames for fleet / split LLM workers.

Single source of truth: ``config/extraction_domain_registry.yaml``, via
``Registry.expected_fleet_prompt_map()``. Import this instead of duplicating
``DOMAIN_PROMPT`` literals in worker scripts.
"""

from __future__ import annotations

import functools


@functools.lru_cache(maxsize=1)
def get_fleet_domain_prompt() -> dict[str, str]:
    from llm_extraction.registry import load_registry

    return dict(load_registry().expected_fleet_prompt_map())


def clear_fleet_domain_prompt_cache() -> None:
    """Invalidate cached map (tests, registry hot-reload)."""
    get_fleet_domain_prompt.cache_clear()
