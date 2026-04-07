# Molecular fact lineage (note extraction + structured assays)

## Boundary

| Layer | Storage | `fact_provenance_category` | Role |
|--------|---------|---------------------------|------|
| Note LLM / regex genetics | `canonical_extracted_fact_long_v1` / `_v2` | `note_derived` (column on parquet from `103_fact_lineage_materialize.py`) | Mentions of ThyroSeq, Afirma, FNA, etc. tied to `note_row_id` and evidence spans. |
| ThyroSeq workbook ingest | `main.molecular_results`, `main.molecular_variant_long` | `assay_structured_import` in unified view | Authoritative assay date, classifier summary, and variant calls from `41_thyroseq_excel_workbook` rows. |
| Afirma structured file ingest | Same tables (`42_afirma_structured_file`) | `assay_structured_import` | Same as ThyroSeq path; vendor-normalized envelope + XA variants. |
| Manual review | `qa.manual_review_queue` (domain `genetics`) | N/A (overlay) | `human_review_overlay` on `molecular_fact_long_v`: `manual_adjudicated_effective` when verification status indicates approval/verification. |

Downstream consumers should use **`main.molecular_fact_long_v`** (or synonym **`main.molecular_results_unified_v`**) when they need a single long table that aligns note-derived and assay-derived molecular facts with explicit precedence.

## Precedence (no double-counting)

- Matching uses **`research_id` + `molecular_family` (`thyroseq` / `afirma`) + event dates within ±21 days** between genetics note facts and `molecular_results` envelope rows.
- **Structured assay wins** for the primary analytic row: `record_role = primary_assay_record`, full date and result fields from vendor tables.
- **Matched note rows are retained** as context: `record_role = supporting_note_evidence`, `included_in_primary_analytics = false`, `precedence_rationale = structured_assay_supersedes_note_row`.
- Variant rows inherit assay–note pairing via parent `molecular_result_id` (`assay_has_note_support`).

Filter **`WHERE included_in_primary_analytics`** for cohort summaries that must not duplicate assay events mentioned in notes.

## QA

- **`main.molecular_fact_lineage_qa_duplicate_candidates_v`**: note vs structured envelope pairs within the date window (inspect gaps and excerpts).
- Validation run:

```bash
.venv/bin/python scripts/132_molecular_fact_lineage_views.py --execute
.venv/bin/python scripts/132_molecular_fact_lineage_views.py --validate-only
```

MotherDuck (token via `motherduck_client` / environment configured for this repo):

```bash
.venv/bin/python scripts/132_molecular_fact_lineage_views.py --execute --md --md-env dev
.venv/bin/python scripts/132_molecular_fact_lineage_views.py --validate-only --md --md-env dev
```

## Related

- Registry: `config/extraction_domain_registry.yaml` (`genetics`, sub-prompt `molecular_thyroseq_afirma`).
- Prompt: `llm_extraction/prompts/molecular_thyroseq_afirma_extraction_v1.txt`.
- Governed DDL: `scripts/sql/131_molecular_results_layer_ddl.sql`.
- Afirma ingest doc: `docs/AFIRMA_INGEST.md`.
