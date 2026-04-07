# LLM regression fixtures (synthetic, no PHI)

Short, de-identified note snippets and frozen mock LLM JSON used by
`tests/test_llm_extraction_regression.py`. No real clinical text or
identifiers.

## Contents

- `cases.json` — inputs (`note_row_id`, `research_id`, `note_type`, `note_date`, `note_text`, `domain`).
- `mock_llm/<case_id>.json` — fake `{"entities":[...]}` payloads (offline CI).
- `expected/<case_id>.json` — normalized golden rows after `_parse_llm_response` + `_stamp_row` (no timestamps in file; tests freeze time).

## Refreshing goldens

When **`_parse_llm_response`**, **`_stamp_row`**, or **entity schema** behavior changes intentionally:

1. Update `mock_llm/*.json` if the LLM JSON contract changes.
2. Regenerate `expected/*.json` with the same transport metadata and provider tag
   the tests use (`github_models`, fixed `llm_sdk_version` placeholder), e.g.:

   ```bash
   env -u GITHUB_TOKEN -u OPENAI_API_KEY python - <<'PY'
   # see tests/test_llm_extraction_regression.py helper REGRESSION_TRANSPORT
   PY
   ```

3. Re-run `python -m pytest tests/test_llm_extraction_regression.py -v`.

Prompt file edits change `prompt_version` digests automatically; tests assert the
live digest, not a hard-coded prompt hash.

## MotherDuck

This suite does **not** connect to MotherDuck. For optional end-to-end checks
with a local token (e.g. from project `.toml`), use `utils/md_connect` separately;
CI for this repo uses a dedicated offline job for these tests.
