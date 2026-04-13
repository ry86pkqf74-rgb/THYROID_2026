# Specimen / FHIR script hygiene (lint + mypy)

**UTC:** 2026-04-13T17:09:35Z  
**Scope:** `scripts/119_md_formalization_validate.py`, `scripts/138_md_specimen_fhir_layer.py`, `scripts/139_md_specimen_identity_layer.py`  
**Intent:** No behavior change — typing/import-order cleanup only.

## Summary

- **119:** `from typing import Any`; `_require_row()` helper for DuckDB `fetchone()` narrowing; `# noqa: E402` on post-`sys.path` import; wrapped `con.execute(...).fetchone()` chains used for indexing.
- **138:** isort-aligned stdlib imports (`importlib.util` with other stdlib); `assert spec139/spec140 is not None and .loader is not None` before `module_from_spec` / `exec_module`.
- **139:** `_require_row()` + wrapped row counts in validation report block.

## Before — `ruff check` (three files)

```
E402 Module level import not at top of file
  --> scripts/119_md_formalization_validate.py:76:1
   |
74 | sys.path.insert(0, str(ROOT))
75 |
76 | from llm_extraction.registry import load_registry
   | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Found 1 error.
```

## After — `ruff check`

```
All checks passed!
```

## Before — `mypy` (three files)

```
scripts/139_md_specimen_identity_layer.py:477: error: Value of type "tuple[Any, ...] | None" is not indexable  [index]
scripts/139_md_specimen_identity_layer.py:478: error: Value of type "tuple[Any, ...] | None" is not indexable  [index]
scripts/139_md_specimen_identity_layer.py:479: error: Value of type "tuple[Any, ...] | None" is not indexable  [index]
scripts/139_md_specimen_identity_layer.py:480: error: Value of type "tuple[Any, ...] | None" is not indexable  [index]
scripts/138_md_specimen_fhir_layer.py:294: error: Argument 1 to "module_from_spec" has incompatible type "ModuleSpec | None"; expected "ModuleSpec"  [arg-type]
scripts/138_md_specimen_fhir_layer.py:295: error: Item "None" of "ModuleSpec | None" has no attribute "loader"  [union-attr]
scripts/138_md_specimen_fhir_layer.py:296: error: Item "None" of "ModuleSpec | None" has no attribute "loader"  [union-attr]
scripts/138_md_specimen_fhir_layer.py:301: error: Argument 1 to "module_from_spec" has incompatible type "ModuleSpec | None"; expected "ModuleSpec"  [arg-type]
scripts/138_md_specimen_fhir_layer.py:302: error: Item "None" of "ModuleSpec | None" has no attribute "loader"  [union-attr]
scripts/138_md_specimen_fhir_layer.py:303: error: Item "None" of "ModuleSpec | None" has no attribute "loader"  [union-attr]
scripts/119_md_formalization_validate.py:297: error: Value of type "tuple[Any, ...] | None" is not indexable  [index]
... (61 errors total in 3 files)
```

## After — `mypy`

```
Success: no issues found in 3 source files
```

## Verification commands (post-change)

- `python -m py_compile scripts/119_md_formalization_validate.py scripts/138_md_specimen_fhir_layer.py scripts/139_md_specimen_identity_layer.py` — OK  
- `python -m ruff check` (same three files) — OK  
- `python -m mypy` (same three files) — OK  
- `pytest tests/test_specimen_identity_layer.py tests/test_specimen_fhir_layer.py tests/test_specimen_fhir_qa_diagnostics.py tests/test_specimen_genomics_binding.py tests/test_specimen_fhir_scripts_offline.py -v` — **43 passed**

## Notes

- MotherDuck token path unchanged; connection behavior in these scripts is unchanged.
- `_require_row` only asserts non-`None` rows where the prior code already indexed `[0]` (would have raised at runtime if `None`).
