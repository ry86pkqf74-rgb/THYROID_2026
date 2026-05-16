# LoRA fine-tuning track

When zero-shot doesn't clear Macro F1 0.90 on a task, LoRA-fine-tune Llama-3-8B with the gold set. Published 2026 work achieved Macro F1 0.976 on TNM/grade/biomarker extraction with this exact recipe (10,677 reports, Llama-3-8B-Instruct base).

## Recipe

### 1. Prepare training data

Format: JSONL with `prompt` and `completion` fields.

```bash
python -m thyroid_mlx_extract.lora.prepare \
  --task synoptic \
  --gold gold/synoptic_gold.csv \
  --source runs/synoptic/source.jsonl \
  --out lora/synoptic_train.jsonl
```

(Stub: see `src/thyroid_mlx_extract/lora/prepare.py` — to be implemented when first task hits the LoRA branch.)

Sweet spot per published work: **2,000–5,000 high-quality examples**.

### 2. Train

```bash
mlx_lm.lora \
  --train \
  --model mlx-community/Meta-Llama-3-8B-Instruct-4bit \
  --data lora/synoptic_train.jsonl \
  --iters 2000 \
  --lora-layers 8 \
  --batch-size 4 \
  --adapter-path adapters/synoptic_v1
```

Loop takes ~2 hours on M5 Max for an 8B base. Adapter weights are ~30 MB.

### 3. Evaluate

```bash
thyroid-mlx eval synoptic \
  --gold gold/synoptic_gold.csv \
  --models llama-3-8b-lora-synoptic
```

Add the LoRA model to `MODELS` in `config.py` with the adapter path:

```python
"llama-3-8b-lora-synoptic": ModelSpec(
    key="llama-3-8b-lora-synoptic",
    hf_repo="mlx-community/Meta-Llama-3-8B-Instruct-4bit",
    quant="4bit",
    memory_gb=5.0,  # 8B + adapter
    strengths=("synoptic_pathology_specialist",),
    # adapter loading handled in registry.py
),
```

### 4. Deploy

Once the LoRA model clears your F1 threshold on held-out gold, swap it in as the task's `primary_model` and re-run the corpus.

## Why this is the long-game

A LoRA-tuned 8B model runs 5–10× faster than a zero-shot 70B model, costs ~30 MB of disk per task to store, and can outperform much larger frontier models on the narrow task it was trained for. The gold set you build for eval is the same gold set you use for fine-tuning — no extra annotation work.

Maintain one adapter per (task, prompt_version, model_base) combination, version them in `adapters/`, and treat them like any other code artifact.
