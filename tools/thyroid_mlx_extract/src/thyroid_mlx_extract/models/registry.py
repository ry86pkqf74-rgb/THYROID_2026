"""LRU-cached MLX model loader.

mlx-lm caches model weights in ~/.cache/huggingface on first download. This module
wraps the load and keeps loaded model+tokenizer pairs in memory across multiple
extraction calls within a single Python process.
"""
from __future__ import annotations

import functools
from typing import Any

from ..config import MODELS, ModelSpec


class ModelHandle:
    """A loaded MLX model + tokenizer pair, ready to generate."""

    def __init__(self, spec: ModelSpec, model: Any, tokenizer: Any):
        self.spec = spec
        self.model = model
        self.tokenizer = tokenizer


@functools.lru_cache(maxsize=4)
def load(model_key: str) -> ModelHandle:
    """Load a model by registry key. Cached so repeat calls within a process are free."""
    if model_key not in MODELS:
        raise KeyError(f"Unknown model '{model_key}'. Available: {sorted(MODELS)}")
    spec = MODELS[model_key]
    try:
        from mlx_lm import load as mlx_load
    except ImportError as e:
        raise ImportError(
            "mlx-lm not installed. `pip install mlx-lm` (Apple Silicon only)."
        ) from e
    model, tokenizer = mlx_load(spec.hf_repo)
    return ModelHandle(spec=spec, model=model, tokenizer=tokenizer)


def evict(model_key: str) -> None:
    """Drop a specific model from the LRU cache (frees unified memory)."""
    # functools.lru_cache doesn't expose per-key eviction; clear all and reload as needed.
    load.cache_clear()
