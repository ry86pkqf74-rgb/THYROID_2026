# MLX model selection — memory for the THYROID_2026 workflow

**Updated:** 2026-05-16. Sources in `docs/mlx/thyroid_model_selection_guide.md`.

## When working on extraction from clinical free text

Use the `thyroid-mlx-extract` skill (lives at `.cowork/skills/thyroid-mlx-extract/`). The harness is at `tools/thyroid_mlx_extract/` and exposes a CLI: `thyroid-mlx pull|eval|run|push <task>`.

## Model selection rules — internalize these

1. **Templated medical text (lab reports, FNA cytology, IHC panels, complications)** → `medgemma27b` (MedGemma 1.5 27B-IT). It's the strongest open-weight medical model per Medmarks April 2026, 78% F1 on lab extraction.

2. **Hard semantic extraction (Ki-67, capsule, ETE distinctions, imaging T-staging)** → `llama33-70b` (Llama 3.3 70B Instruct). 2026 study: beats OpenBioLLM-70B on 59/73 clinical extraction tasks. General models often beat medical fine-tunes on extraction.

3. **Short narrative + constrained vocab (US descriptions, PROMs, risk factors)** → `medgemma4b` or `phi4`. Smaller is faster; eval head-to-head.

4. **Adjudication / high-stakes fields** → `r1-distill-70b` (DeepSeek-R1-Distill-Llama-70B). 93% MedQA. Use as a different-family second opinion on ETE grade, cause of death, complication type, molecular risk.

5. **Top reasoner on M5 Ultra** → `qwen3-235b-thinking`. Best open-weight reasoner; 120 GB at 4-bit. Reserve for hardest adjudication.

6. **De-identification** → **Philter (UCSF), NOT an LLM.** 99.46% recall. LLMs alone miss >50% of PHI. This is non-negotiable.

7. **PubMed/biomedical retrieval** → MedCPT retriever + reranker (BM25 first stage → MedCPT rerank hits 0.90 accuracy).

8. **Short clinical semantic search (patient similarity)** → `jina-embeddings-v2-base-en`. Generalist beats ClinicalBERT on short clinical text (84% vs 64% exact match).

9. **Medical dictation** → Google MedASR. Trained on 5,000 hours of physician dictation; beats Whisper on medical audio.

10. **WSI / whole-slide pathology (if/when in scope)** → TITAN (Mahmood lab). Best open-weight WSI foundation model.

## Workflow rules — non-negotiable

1. **Always gold-set eval before corpus run.** Published 2026 evidence: 11–32 point reality gap between synthetic and real-world performance. Use `thyroid-mlx eval <task>` and require Macro F1 ≥ 0.90 before deploying.

2. **Workspace before canonical.** `--workspace` flag writes to `pub_workspace.note_entities_llm_<task>_v<n>`. Analyst review precedes promotion to `pub_canonical`.

3. **Full provenance columns required.** Match the existing `note_entities_llm_*` schema: `extraction_run_id`, `model_name`, `model_version`, `prompt_version`, `raw_response_sha256`, `verification_status`, `confidence_score`, `extraction_timestamp_utc`. `bq/push.py` enforces this.

4. **Bump `prompt_version` on every prompt edit.** Same model + different prompt = different extraction.

5. **Two-model agreement for high-stakes fields.** Cause of death, ETE grade, complication type, molecular risk call. Use `adjudicator.py`.

6. **LoRA fine-tune Llama-3-8B** when zero-shot doesn't clear 0.90 F1. 2026 published recipe: Macro F1 0.976 on TNM/grade/biomarker with 10,677 reports. 2-hour MLX run.

7. **Never let an LLM near raw PHI.** Run Philter first.

## Memory budget (M5 Max 128 GB / M5 Ultra 192 GB)

- MedGemma-4B 4-bit: 2.5 GB
- Phi-4 14B 4-bit: 8 GB
- MedGemma-27B / Gemma-3-27B 4-bit: 14 GB
- Qwen3-32B 4-bit: 18 GB
- Llama-3.3-70B / R1-Distill-70B / Qwen3-72B 4-bit: 38–40 GB
- Qwen3-235B-A22B-Thinking 4-bit: 120 GB (M5 Ultra only)

## Anti-patterns — never do these

- Pick a model without checking the task type
- Use a single model for everything
- Skip the gold-set + eval step
- Write directly to `pub_canonical` from a new run
- Send PHI to a cloud LLM without de-identification
- Use an LLM for de-identification
- Edit the gold CSV to make a model look better
- Trust a frontier-blog benchmark without running on your own gold set

## Empirical anchors (one-liners with sources)

- MedGemma 1.5 27B-IT — strongest open-weight medical, 78% F1 lab extraction (Medmarks April 2026)
- Llama 3.3 70B — beat OpenBioLLM on 59/73 extraction tasks (2026 clinical extraction study)
- DeepSeek-R1 — 93% MedQA diagnostic accuracy (Nature Medicine 2025)
- LoRA Llama-3-8B — 0.976 Macro F1 pathology extraction (2026 published, 10,677 reports)
- Philter — 99.46% PHI recall vs LLMs missing >50% (Nature Digital Medicine 2020 + 2026 review)
- jina-embeddings-v2 — 84% vs ClinicalBERT 64% short-clinical retrieval (arxiv 2024)
- MedCPT — trained on 255M PubMed query-article pairs; SOTA for biomedical IR

## Files in this repo

- `docs/mlx/thyroid_mlx_extraction_gaps.md` — empirical gap analysis
- `docs/mlx/thyroid_model_selection_guide.md` — comprehensive model-task matrix and eval framework
- `docs/mlx/LORA_FINETUNING.md` — fine-tuning recipe
- `tools/thyroid_mlx_extract/` — runnable Python package
- `.cowork/skills/thyroid-mlx-extract/` — Cowork skill (auto-triggers on relevant requests)
