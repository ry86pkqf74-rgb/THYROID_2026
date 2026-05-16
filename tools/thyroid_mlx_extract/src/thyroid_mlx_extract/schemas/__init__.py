"""Pydantic schemas — one module per extraction task.

Convention: each module exports an `Extraction` class which is the root
schema passed to Outlines for constrained decoding. Every Extraction has:
  - `entities: list[<task-specific Entity>]`
  - field-by-field evidence spans (substring from the source text)
  - a per-field confidence (0.0–1.0)
"""
from __future__ import annotations

from importlib import import_module

from pydantic import BaseModel


def load_schema(module_path: str) -> type[BaseModel]:
    """Dynamically load the Extraction class from a schema module."""
    mod = import_module(module_path)
    return getattr(mod, "Extraction")
