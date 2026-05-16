# Model × Task Matrix — Quick Reference

Generated from empirical 2026 benchmarks. Sources documented in `docs/mlx/thyroid_model_selection_guide.md`.

## Tier 1 — Structured extraction from clinical free text

| Task | Primary | Fallback | Adjudicator | Memory (primary) |
|---|---|---|---|---|
| Molecular variants (ThyroSeq, Afirma, etc.) | `medgemma27b` | `llama33-70b` | `r1-distill-70b` | 14 GB |
| Synoptic pathology (Ki67, ETE, capsule, ENE) | `llama33-70b` | `qwen3-72b` | `r1-distill-70b` | 38 GB |
| US nodule features (halo, vasc, microcalc) | `medgemma4b` | `medgemma27b` | — | 2.5 GB |
| CT imaging (T4a/T4b features) | `llama33-70b` | `medgemma27b` | `r1-distill-70b` | 38 GB |
| MRI imaging | `llama33-70b` | `medgemma27b` | — | 38 GB |
| NM imaging | `medgemma27b` | `medgemma4b` | — | 14 GB |
| FNA cytology subtype | `medgemma27b` | `medgemma4b` | — | 14 GB |
| Complications subtyping | `medgemma27b` | `llama33-70b` | `r1-distill-70b` | 14 GB |
| Cause-of-death attribution | `llama33-70b` | `r1-distill-70b` | `r1-distill-70b` (required) | 38 GB |
| Risk factors (radiation, family hx) | `medgemma4b` | `medgemma27b` | — | 2.5 GB |

## Other tasks

| Task | Primary | Notes |
|---|---|---|
| De-identification (PHI) | **Philter** | Not an LLM. 99.46% recall vs LLMs missing >50% |
| NL → BigQuery SQL | Gemini in BigQuery (managed) or Claude Sonnet 4.6 | Not PHI; cloud is fine |
| Manuscript drafting | Claude Opus 4.7 | Long-form reasoning; frontier cloud models lead |
| PubMed RAG retrieval | BM25 + MedCPT rerank | Hybrid beats either alone (0.90 accuracy) |
| Short clinical embedding | jina-embeddings-v2-base-en | Generalist beats specialist on short clinical |
| Patient/report similarity | jina-embeddings-v2-base-en | Same as above |
| Medical dictation → text | Google MedASR | Medical-trained; beats Whisper on medical audio |
| General speech → text | Qwen3-ASR | New SOTA on FLEURS, beats Whisper Large V3 |
| Whole-slide pathology | TITAN (Mahmood lab) | Open-weight WSI foundation model |
| Multimodal medical (text + image) | MedGemma 1.5 27B Multimodal | Same family as our text MedGemma |

## Reasoning model usage policy

Use `r1-distill-70b` (38 GB) or `qwen3-235b-thinking` (120 GB, M5 Ultra only) **only for adjudication** on disagreement-flagged cases. Don't use as primary extractor — they're slower and the gain over a strong extractor like Llama 3.3 70B doesn't justify it for routine extraction.

## Empirical justification (one-liners)

- **MedGemma 1.5 27B-IT** is best open-weight medical model on Medmarks (April 2026). 78% F1 on lab extraction. Strongest pick for templated medical text.
- **Llama 3.3 70B** beat OpenBioLLM-70B on 59 of 73 clinical extraction tasks (2026 study). Best general open-weight extractor.
- **DeepSeek-R1** hit 93% diagnostic accuracy on MedQA. Best reasoning at this scale.
- **Qwen3-235B-A22B-Thinking** is best open-weight reasoner on Medmarks. Worth the 120 GB if you have M5 Ultra.
- **LoRA-fine-tuned Llama-3-8B** hit Macro F1 0.976 on TNM/grade/biomarker extraction (2026 published). Long-game move when zero-shot doesn't clear 0.90.
- **Philter (UCSF)** hit 99.46% recall on PHI vs LLMs missing >50%. Specialist tool wins.
- **jina-embeddings-v2** beat ClinicalBERT on short clinical retrieval (84% vs 64% exact match). Generalist embeddings win short-context.
- **MedCPT** trained on 255M PubMed search log pairs. Best for biomedical document retrieval.
- **Google MedASR** trained on 5,000 hours of physician dictation. Lower error rate than Whisper on medical audio.
