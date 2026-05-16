# Quickstart — first extraction in under an hour

Assumes M5 Mac, Python 3.11+, gcloud SDK already configured with access to `thyroid-canonical-pub-2026`.

## 1. Install

```bash
cd tools/thyroid_mlx_extract
make install   # or: pip install -e .
```

## 2. Verify

```bash
thyroid-mlx list-tasks
thyroid-mlx list-models
```

## 3. Pull a sample for the molecular task

```bash
thyroid-mlx pull molecular --limit 100
# writes runs/molecular/source.jsonl
```

## 4. Build a tiny gold subset

Open `runs/molecular/source.jsonl`, hand-annotate 20–30 cases by filling in `gold/molecular_gold.csv` (use `gold/molecular_gold_template.csv` as a starting point).

## 5. Evaluate candidate models

```bash
thyroid-mlx eval molecular --gold gold/molecular_gold.csv --models medgemma27b,llama33-70b
# downloads weights on first run (~14 GB + ~38 GB)
# writes results/molecular/eval/comparison.md
```

Read `comparison.md` — the highest Macro F1 model wins. If both are below 0.90, consider:
- Tightening the prompt
- LoRA fine-tuning Llama-3-8B (separate workflow)

## 6. Run over the full corpus

```bash
thyroid-mlx pull molecular   # no limit → all 10,862 rows
thyroid-mlx run molecular --model medgemma27b --resume
# writes runs/molecular/<run_id>.jsonl incrementally
# safe to Ctrl-C and rerun with --resume
```

## 7. Push to BigQuery

```bash
thyroid-mlx push molecular --results runs/molecular/<run_id>.jsonl --workspace
# writes to pub_workspace.note_entities_llm_molecular_v1
# review there before promoting to pub_canonical
```

## Troubleshooting

**`ImportError: mlx-lm not installed`** — `pip install mlx-lm`. Apple Silicon only; will not work on Linux/Windows.

**Out of memory** — switch to a smaller model (`medgemma4b` or `medgemma27b` instead of `llama33-70b`). Or close other applications.

**Slow first call** — model weights are downloaded from HuggingFace on first use (~14 GB for MedGemma-27B). Subsequent calls use the cache.

**Constrained decoding fails** — set `--no-constrained` (CLI flag TBD) to fall back to plain generation. Some models behave better unconstrained on complex schemas; manually validate JSON afterward.

**Resume not picking up** — check the run_id file in `runs/<task>/` — it tracks completed `source_pk`s. Delete only if you want to fully restart.
