# Silver layer

Cleaned, typed, joinable analytic tables aligned to `research_id` and study contracts.

- **Primary home in-repo:** [`processed/`](../processed/) — DVC-tracked parquets (e.g. `processed/*.parquet` sidecars), [`processed/remaining/`](../processed/remaining/) for staged extracts, and [`processed/output/`](../processed/output/) for LLM checkpoint / staging artifacts (e.g. `v2_parquets/`, `*_checkpoints/`).
- **Extraction code:** [`llm_extraction/`](../llm_extraction/) (regex + LLM entity pipeline, audit engines, prompts under `llm_extraction/prompts/`).
- **Policy:** Silver tables are the integration layer for DuckDB views, Streamlit, and downstream study scripts; preserve provenance columns (`source_table`, `date_source`, etc.) per project standards.
