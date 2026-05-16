# thyroid-mlx-extract

Runnable harness for on-device MLX-based clinical text extraction over the THYROID_2026 BigQuery database.

Pulls source text from `thyroid-canonical-pub-2026.pub_canonical`, runs Pydantic-schema-constrained extraction via mlx-lm + Outlines, scores against gold subsets, and writes results back as `note_entities_llm_<domain>_v<n>` tables following the existing provenance pattern (`extraction_run_id`, `model_name`, `prompt_version`, `raw_response_sha256`).

## Why this exists

The companion docs in `docs/mlx/`:
- `thyroid_mlx_extraction_gaps.md` — empirical analysis of which BQ columns are under-parsed
- `thyroid_model_selection_guide.md` — model-task matrix, evaluation framework, hardware budget

10 Tier-1 extraction targets identified; this package implements the extraction + eval pipeline so you can iterate per task.

## Install

```bash
# Apple Silicon Mac, Python 3.11+
git clone https://github.com/ry86pkqf74-rgb/THYROID_2026
cd THYROID_2026/tools/thyroid_mlx_extract
pip install -e .

# auth: gcloud application-default + token in env
gcloud auth application-default login
export BQ_PROJECT=thyroid-canonical-pub-2026
```

## Tasks

| Task ID | Source columns | Output schema | Recommended model |
|---|---|---|---|
| `molecular` | `molecular_results.raw_payload_json` | variants, fusions, cnvs, risk_call | MedGemma 1.5 27B-IT |
| `synoptic` | `path_synoptics` + `clinical_notes_long` (path) | ki67, mitoses, capsule, ETE, ENE, PNI | Llama 3.3 70B + R1 distill adj. |
| `ultrasound` | `ultrasound_reports.nodule_*_source_description` | halo, vasc, microcalc, ratio | MedGemma 1.5 4B |
| `imaging_ct` | `ct_imaging.original_report` | T4a/T4b features, distant mets | Llama 3.3 70B |
| `imaging_mri` | `mri_imaging.original_report` | T4a/T4b features | Llama 3.3 70B |
| `imaging_nm` | `nuclear_med.findings_text` | uptake patterns, mediastinal | MedGemma 1.5 27B |
| `fna` | `fna_cytology.path_text` | subtype, nuclear features, adequacy | MedGemma 1.5 27B |
| `complications` | `clinical_notes_long` OPNOTE/HP | typed complication events | MedGemma 1.5 27B |
| `death` | `clinical_notes_long` DEATH | cancer_specific, proximate_cause | Llama 3.3 70B + R1 distill |
| `risk_factors` | `clinical_notes_long` HP | radiation, family hx, smoking, BMI | MedGemma 1.5 4B |

## Workflow

```bash
# 1. Pull source data for a task
thyroid-mlx pull molecular --limit 100 --out gold/molecular_sample.jsonl

# 2. Build a gold subset (manual: annotate gold/molecular_gold.csv)

# 3. Evaluate candidate models against gold
thyroid-mlx eval molecular --gold gold/molecular_gold.csv --models medgemma27b,llama33-70b,qwen3-32b

# 4. Pick winner from eval report; run over full corpus
thyroid-mlx run molecular --model medgemma27b --batch-size 8 --resume

# 5. Push results back to BQ
thyroid-mlx push molecular --run-id <run_id>
```

## Architecture

```
src/thyroid_mlx_extract/
├── cli.py                   # click CLI entry point
├── config.py                # task definitions, model registry, BQ config
├── schemas/                 # Pydantic schemas per task (one file per task)
├── prompts/                 # System+user prompt templates per task
├── models/
│   ├── extractor.py         # Outlines-constrained MLX extraction
│   ├── registry.py          # MLX model lookup + LRU cache
│   └── adjudicator.py       # Two-model agreement / disagreement routing
├── bq/
│   ├── pull.py              # Source data queries
│   └── push.py              # Write to note_entities_llm_* with provenance
├── eval/
│   ├── gold.py              # Gold CSV loader, alignment
│   ├── scoring.py           # F1, hallucination rate, span coverage
│   └── runner.py            # Run candidate models, build comparison report
├── deid/
│   └── philter_wrapper.py   # Philter pre-pass for any PHI-sensitive flow
└── utils/
    ├── provenance.py        # run_id, sha256, lineage cols
    └── chunk.py             # Long-text chunking with overlap
```

## Provenance pattern

Every extracted row gets the standard columns used elsewhere in `pub_canonical`:

```
research_id           extraction_run_id     raw_response_sha256
note_row_id           extractor_name        verification_status
note_type             extractor_version     verification_step
note_date             model_name            evidence_span
entity_domain         model_version         evidence_global_start
entity_type           prompt_version        evidence_global_end
entity_value_raw      llm_provider          confidence_score
entity_value_norm     llm_sdk               extracted_at_utc
present_or_negated    llm_sdk_version
```

This keeps your `canonical_table_signoff_registry_v1` machinery working unchanged.

## Hardware targets

- M5 Max 128 GB: comfortably runs MedGemma 27B, Llama 3.3 70B, or DeepSeek-R1-Distill-70B
- M5 Ultra 192 GB: adds Qwen3-235B-A22B-Thinking for the hardest adjudication
- Throughput estimates per model documented in `docs/mlx/thyroid_model_selection_guide.md`

## License

Internal research use. See repo root LICENSE.
