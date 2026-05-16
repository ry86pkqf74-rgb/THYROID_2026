# reference: MLX local model selection for thyroid extraction tasks

**Created** 2026-05-16 (commit `30edf986`). **Skill**: `thyroid-mlx-extract`. **Harness**: `tools/thyroid_mlx_extract/`. **Docs**: `docs/mlx/`.

## What this is

Distilled rules for picking models when extracting structured data from clinical free text on Logan's M5. Empirical basis: Medmarks April 2026 + Nature Med / Nature Comm 2026 clinical extraction studies. Full justification in `docs/mlx/thyroid_model_selection_guide.md`.

## Task → model assignments (canonical)

| Task | Primary | Fallback | Adjudicator |
|---|---|---|---|
| Molecular variants (ThyroSeq/Afirma/Castle) | `medgemma27b` | `llama33-70b` | `r1-distill-70b` |
| Synoptic pathology (Ki67/capsule/ETE/ENE/PNI) | `llama33-70b` | `qwen3-72b` | `r1-distill-70b` |
| US nodule features (halo/vasc/microcalc) | `medgemma4b` | `medgemma27b` | — |
| CT imaging (T4a/T4b staging) | `llama33-70b` | `medgemma27b` | `r1-distill-70b` |
| MRI imaging | `llama33-70b` | `medgemma27b` | — |
| NM imaging | `medgemma27b` | `medgemma4b` | — |
| FNA cytology subtype | `medgemma27b` | `medgemma4b` | — |
| Complications subtyping | `medgemma27b` | `llama33-70b` | `r1-distill-70b` |
| Cause-of-death attribution | `llama33-70b` | `r1-distill-70b` | `r1-distill-70b` (REQUIRED — two-model agreement) |
| Pre-existing risk factors | `medgemma4b` | `medgemma27b` | — |

## Non-task model picks (for reference)

- **De-identification (PHI)** → **Philter (UCSF) — NOT an LLM.** 99.46% recall vs LLMs missing >50%. Hard rule.
- **NL → BigQuery SQL** → Gemini in BigQuery (managed) or Claude Sonnet 4.6. Not PHI.
- **Manuscript drafting** → Claude Opus 4.7 (cloud). After de-id.
- **PubMed RAG** → BM25 + MedCPT rerank → Claude/Llama 3.3 70B for synthesis.
- **Short clinical embeddings (patient similarity)** → `jina-embeddings-v2-base-en` (generalist beats ClinicalBERT 84% vs 64%).
- **Medical dictation** → Google MedASR. Beats Whisper on medical audio.
- **WSI / whole-slide pathology** (if/when in scope) → TITAN (Mahmood lab).
- **Reasoning adjudication** → `r1-distill-70b` (38 GB) or `qwen3-235b-thinking` (120 GB, M5 Ultra only).

## Workflow rules — non-negotiable

1. **Always gold-set eval before corpus run.** Published 2026 evidence: 11–32 point reality gap between synthetic and real-world performance. Use `thyroid-mlx eval <task>` and require Macro F1 ≥ 0.90.
2. **Workspace before canonical.** `--workspace` flag → `pub_workspace.note_entities_llm_<task>_v<n>`. Analyst review precedes promotion.
3. **Full provenance columns required.** Match existing `note_entities_llm_*` schema (`extraction_run_id`, `model_name`, `prompt_version`, `raw_response_sha256`, `verification_status`, etc.).
4. **Bump `prompt_version` on prompt edits.**
5. **Two-model agreement for high-stakes fields**: cause of death, ETE grade, complication type, molecular risk call.
6. **LoRA fine-tune Llama-3-8B** when zero-shot doesn't clear 0.90 F1. Published 2026: Macro F1 0.976 on TNM/grade/biomarker with 10,677 reports (2-hour MLX run).
7. **Never let an LLM near raw PHI.** Run Philter first.

## Memory budget (M5 Max 128 GB / Ultra 192 GB)

- MedGemma-4B: 2.5 GB
- Phi-4 14B: 8 GB
- MedGemma-27B / Gemma-3-27B: 14 GB
- Qwen3-32B: 18 GB
- Llama-3.3-70B / R1-Distill-70B / Qwen3-72B: 38–40 GB
- Qwen3-235B-A22B-Thinking: 120 GB (Ultra only)

## Anti-patterns

- One-model-for-everything ("just use Qwen") — pick by task type.
- Skipping gold-set eval.
- Direct `pub_canonical` write from a new run (always workspace-first).
- LLM for de-identification.
- Editing the gold CSV to make a model look better.
- Trusting a blog benchmark without running on your own gold set.

## Empirical anchors (one-liner + source)

- MedGemma 1.5 27B-IT — strongest open-weight medical, 78% F1 lab extraction (Medmarks April 2026).
- Llama 3.3 70B — beat OpenBioLLM-70B on 59/73 extraction tasks (2026 clinical extraction study).
- DeepSeek-R1 — 93% MedQA diagnostic accuracy (Nature Medicine 2025).
- LoRA Llama-3-8B — 0.976 Macro F1 pathology extraction (2026 published, 10,677 reports).
- Philter — 99.46% PHI recall vs LLMs missing >50% (Nature Digital Medicine 2020 + 2026 review).
- jina-embeddings-v2 — 84% vs ClinicalBERT 64% short-clinical retrieval (arxiv 2024).
- MedCPT — trained on 255M PubMed query-article pairs; SOTA biomedical IR.
- Google MedASR — trained on 5,000 hours physician dictation; lower error rate than Whisper on medical audio.

## Related files in repo

- `docs/mlx/thyroid_mlx_extraction_gaps.md` — empirical gap analysis from BQ (which columns are under-parsed, with row counts).
- `docs/mlx/thyroid_model_selection_guide.md` — comprehensive model-task matrix and eval framework.
- `docs/mlx/LORA_FINETUNING.md` — fine-tuning recipe.
- `tools/thyroid_mlx_extract/` — runnable Python package, CLI: `thyroid-mlx pull|eval|run|push <task>`.
- `.cowork/skills/thyroid-mlx-extract/` — Cowork skill (auto-triggers on relevant requests).
