---
name: thyroid-mlx-extract
description: On-device MLX clinical text extraction for THYROID_2026 BigQuery (tools/thyroid_mlx_extract). Use whenever extracting structured data from clinical free text in the thyroid cohort: pathology synoptic enrichment, molecular variant parsing, ultrasound nodule features, CT/MRI/NM staging, FNA cytology subtype, complications subtyping, cause-of-death adjudication, pre-existing risk factors. Triggers: MLX, on-device, local LLM, mlx-lm, Outlines, Llama 3.3, MedGemma, Qwen3, DeepSeek R1, Phi-4, extraction harness, Ki-67, mitotic count, capsular invasion, ETE, ENE, PNI, angioinvasion, ThyroSeq, Afirma, raw_payload_json, synoptic_diagnosis, Bethesda subcategory, halo, microcalcification, ACR TI-RADS, hypoparathyroidism subtyping, RLN injury, cause of death, childhood radiation, family history. Enforces: model selection by task tier, gold-set F1 eval before corpus runs, Philter (NOT an LLM) for PHI, provenance matching note_entities_llm_*.
---

# thyroid-mlx-extract — operational playbook

This skill is the operational front-door for the `tools/thyroid_mlx_extract/` package in the THYROID_2026 repo. Use it any time the user wants to extract structured data from clinical free text in the thyroid cohort.

## Before doing anything: read these references

Two companion docs in the repo capture the empirical and decision rationale. **Read both before recommending a model or starting an extraction:**

- `docs/mlx/thyroid_mlx_extraction_gaps.md` — empirical analysis: which BQ columns are under-parsed, with row counts and coverage %
- `docs/mlx/thyroid_model_selection_guide.md` — model-task matrix, benchmark evidence, hardware budget, eval framework

Also reference:
- `references/model_task_matrix.md` (in this skill) — quick lookup table
- `references/provenance_pattern.md` (in this skill) — required BQ columns

## The 10 registered tasks

`molecular`, `synoptic`, `ultrasound`, `imaging_ct`, `imaging_mri`, `imaging_nm`, `fna`, `complications`, `death`, `risk_factors`.

Each has a fixed source-table mapping, Pydantic schema, and recommended model. See `tools/thyroid_mlx_extract/src/thyroid_mlx_extract/config.py` for the registry.

## Standard workflow — ALWAYS in this order

1. **Pull** source rows: `thyroid-mlx pull <task> --limit N`
2. **Build a gold subset** (50–200 cases, manual): fill `gold/<task>_gold.csv`
3. **Eval** candidate models: `thyroid-mlx eval <task> --gold gold/<task>_gold.csv --models m1,m2,m3`
4. **Pick winner** from `results/<task>/eval/comparison.md` (need Macro F1 ≥ 0.90)
5. **Run** over corpus: `thyroid-mlx run <task> --model <winner> --resume`
6. **Push** to BQ workspace first: `thyroid-mlx push <task> --results <run>.jsonl --workspace`
7. **Promote** to canonical only after analyst signoff: `thyroid-mlx push <task> --canonical`

NEVER skip the gold-set + eval step. The 2026 pathology-extraction literature documents an 11–32 point "reality gap" between synthetic and real-world performance — gold subsets are the only thing that detects this.

## Model selection — recall key rules

When the user asks "which model should I use for X":

| Task type | Primary model | Why |
|---|---|---|
| Templated medical lab reports (molecular, FNA, IHC) | `medgemma27b` | MedGemma 1.5 27B-IT (Jan 2026) is strongest open-weight medical, 78% F1 on lab extraction |
| Hard semantic pathology (Ki-67, capsule, ETE) | `llama33-70b` + `r1-distill-70b` adjudication | Llama 3.3 70B beat medical fine-tunes on 59/73 clinical extraction tasks |
| Short narrative (US descriptions, PROMs) | `medgemma4b` or `phi4` | Constrained vocab; small + fast wins |
| Imaging reports (CT/MRI) | `llama33-70b` | Long narrative + complex anatomy |
| Cause-of-death | `llama33-70b` + `r1-distill-70b` both required to agree | High stakes, small corpus, two-model affordable |
| De-identification | **Philter, NOT an LLM** | LLMs alone miss >50% of PHI; Philter 99.46% recall |
| Adjudication | `r1-distill-70b` or `qwen3-235b-thinking` (M5 Ultra only) | Different family from extractor |

If zero-shot fails to clear F1 0.90 on gold, the next step is **LoRA fine-tune Llama-3-8B** with the gold set (see `docs/mlx/LORA_FINETUNING.md`). Published 2026 result: Macro F1 0.976 on TNM/grade/biomarker with 10,677 reports. 2-hour MLX run.

## Hard constraints — never violate

1. **PHI handling**: any flow that touches raw clinical notes must run Philter first. Do NOT send clinical text to a cloud LLM unless the user explicitly authorizes after seeing the de-identification result.

2. **Provenance columns are required** for any BQ push. The schema in `bq/push.py` includes: `extraction_run_id`, `model_name`, `model_version`, `prompt_version`, `raw_response_sha256`, `verification_status`, `confidence_score`, `extraction_timestamp_utc`. Matches the existing `note_entities_llm_*` pattern in `pub_canonical`.

3. **Workspace-first writes**: new extractions land in `pub_workspace` first. Promotion to `pub_canonical` requires analyst review via the `canonical_table_signoff_registry_v1` workflow.

4. **No model swaps without re-eval**: changing the primary model for a task invalidates prior gold scores. Re-run eval first.

5. **Two-model agreement for high-stakes fields**: cause-of-death attribution, ETE grade, complication type, molecular risk call. The `adjudicator.py` module enforces this.

## Hardware reality check

- M5 Max 128 GB: comfortably runs everything up to 70B at 4-bit
- M5 Ultra 192 GB: adds Qwen3-235B-A22B-Thinking (~120 GB)
- Llama 4 Maverick / DeepSeek V4 Pro: cloud-only

If user is on M4 or earlier: stick to ≤27B models (MedGemma-27B, Gemma 3 27B, Qwen3 32B). Llama 3.3 70B at 4-bit needs 38 GB which is uncomfortable on 64 GB machines.

## Common interactions

**User: "extract Ki-67 from path reports"**
→ This is the synoptic task. Check current gold-set status, recommend running an eval over `medgemma27b` + `llama33-70b` first, point them at the QUICKSTART.

**User: "I want to parse all our ThyroSeq reports"**
→ Molecular task. `thyroid-mlx pull molecular` then eval `medgemma27b` first (templated lab is its sweet spot).

**User: "what model should I use for X"**
→ Look up the task in `config.py`'s TASKS dict. Quote the empirical justification from `thyroid_model_selection_guide.md`. Insist on a gold-set eval before committing.

**User: "can we just use Qwen for everything"**
→ NO. See `thyroid_model_selection_guide.md` for why model choice is task-dependent. The 2026 evidence is clear: MedGemma wins templated medical, Llama 3.3 wins general extraction, reasoning models win adjudication, and Philter (not an LLM) wins de-identification.

**User: "de-identify these notes before I share"**
→ Philter, never an LLM. `from thyroid_mlx_extract.deid.philter_wrapper import scrub`. Sanity-check with `is_safe()`.

**User: "this extraction is wrong"**
→ Don't edit the gold set. Either (a) tighten the prompt, (b) swap to the fallback model, (c) flag the case to the analyst, or (d) start the LoRA track.

## Skill maintenance

When new models drop (Qwen 4, Llama 5, MedGemma 2, etc.):
1. Add to `MODELS` in `config.py`
2. Run a head-to-head eval against current incumbent on the gold sets
3. Update `references/model_task_matrix.md` if the recommendation changes
4. Bump `prompt_version` if you also change the prompt
