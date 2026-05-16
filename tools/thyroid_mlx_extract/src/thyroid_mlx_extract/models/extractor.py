"""Core extractor: MLX inference + Outlines-constrained JSON decoding.

Given (model_key, prompt_template, pydantic_schema, source_text) returns a
validated Pydantic instance (or a parse error to flag).
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ValidationError

from . import registry


@dataclass
class ExtractionResult:
    """Result of a single extraction call."""

    success: bool
    payload: BaseModel | None        # validated schema instance
    raw_text: str                    # exact model output (for SHA + audit)
    error: str | None                # validation/decoding error if any
    model_key: str
    prompt_version: str
    elapsed_seconds: float
    input_tokens: int | None
    output_tokens: int | None


def extract(
    model_key: str,
    schema_cls: type[BaseModel],
    prompt_template: str,
    source_text: str,
    *,
    prompt_version: str = "v1",
    temperature: float = 0.0,
    max_tokens: int = 4096,
    constrained: bool = True,
) -> ExtractionResult:
    """Run a single extraction. Returns ExtractionResult either way."""
    handle = registry.load(model_key)
    prompt = prompt_template.replace("{source_text}", source_text)

    t0 = time.time()

    if constrained:
        text = _generate_constrained(handle, schema_cls, prompt, temperature, max_tokens)
    else:
        text = _generate_unconstrained(handle, prompt, temperature, max_tokens)

    elapsed = time.time() - t0

    # Try to parse JSON → schema
    try:
        # If model emitted prose around the JSON, find the first {...} block
        json_text = _extract_json_block(text)
        data = json.loads(json_text)
        payload = schema_cls(**data)
        return ExtractionResult(
            success=True,
            payload=payload,
            raw_text=text,
            error=None,
            model_key=model_key,
            prompt_version=prompt_version,
            elapsed_seconds=elapsed,
            input_tokens=None,
            output_tokens=None,
        )
    except (json.JSONDecodeError, ValidationError, ValueError) as e:
        return ExtractionResult(
            success=False,
            payload=None,
            raw_text=text,
            error=f"{type(e).__name__}: {e}",
            model_key=model_key,
            prompt_version=prompt_version,
            elapsed_seconds=elapsed,
            input_tokens=None,
            output_tokens=None,
        )


def _generate_constrained(
    handle: registry.ModelHandle,
    schema_cls: type[BaseModel],
    prompt: str,
    temperature: float,
    max_tokens: int,
) -> str:
    """Outlines-constrained generation that guarantees valid JSON matching schema_cls."""
    try:
        from outlines import generate, models
    except ImportError as e:
        raise ImportError("outlines not installed. `pip install outlines`") from e

    # Outlines mlx integration: wrap the mlx model
    olm = models.mlxlm(handle.model, handle.tokenizer)
    json_generator = generate.json(olm, schema_cls)
    return json_generator(prompt, max_tokens=max_tokens, sampler_temperature=temperature)


def _generate_unconstrained(
    handle: registry.ModelHandle,
    prompt: str,
    temperature: float,
    max_tokens: int,
) -> str:
    """Plain mlx-lm generation. Used as a fallback / debug path."""
    from mlx_lm import generate as mlx_generate

    return mlx_generate(
        handle.model,
        handle.tokenizer,
        prompt=prompt,
        max_tokens=max_tokens,
        temp=temperature,
    )


def _extract_json_block(text: str) -> str:
    """If model emits prose around JSON, isolate the first {...} block."""
    start = text.find("{")
    if start == -1:
        return text
    # Find matching close brace via depth counting
    depth = 0
    in_string = False
    escape = False
    for i, ch in enumerate(text[start:], start):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return text[start:]
