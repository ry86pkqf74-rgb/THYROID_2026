# Local Model Selection Guide — Thyroid Research Workflow

**Scope:** Comprehensive task-by-task model recommendations for the thyroid BQ database and research workflow, running on Apple Silicon (M5 family) via MLX. Replaces the over-anchored "Qwen for everything" recommendation in the earlier gap-analysis doc.

**Hardware assumption:** M5 Max (128 GB unified memory) or M5 Ultra (192 GB). The M5 GPU's neural accelerators give 3.5–4× faster prompt processing than M4 and push TTFT under 10 s for a dense 14B and under 3 s for a 30B MoE.

**Software stack:** `mlx-lm` 0.31+ for LLM inference and LoRA fine-tuning; `mlx-vlm` for vision-language models; `outlines` for constrained JSON decoding (XGrammar isn't on MLX yet); the `mlx-community` HuggingFace org hosts 4,300+ pre-converted models.

**Key empirical findings driving the recommendations:**
- General-purpose models beat medical fine-tunes on extraction. Llama 3.3 70B exceeded 90% accuracy on 59/73 clinical extraction tasks, outperforming OpenBioLLM-70B.
- MedGemma 1.5 27B-IT (Jan 2026) is the strongest *open-weight medical* model on Medmarks; 78% F1 on lab report extraction, 18-point jump from prior version.
- LoRA-fine-tuned Llama-3-8B achieved Macro F1 0.976 on pathology extraction with 10,677 reports — small fine-tuned models can match much bigger ones on narrow tasks.
- Gemini-2.5-pro tops real-world pathology synoptic extraction at 87.7% recall — a "reality gap" of 11–32 points exists between synthetic-eval and real-world performance, so gold-standard local evals are non-negotiable.
- Generalist embedding models (jina-v2, e5) outperform specialized medical embeddings on short clinical semantic search; MedCPT wins on PubMed-style biomedical IR.
- Philter (regex + statistical) at 99.46% recall outperforms general-purpose LLMs (which miss >50% of PHI) for de-identification — use the specialist tool, not an LLM.

---

## Quick reference — task → model matrix

| Task category | Primary model | Fallback | Adjudicator | Notes |
|---|---|---|---|---|
| Templated lab-report extraction (molecular, labs) | MedGemma 1.5 27B-IT | Llama 3.3 70B 4-bit | Qwen3-235B-A22B Thinking | Small medical wins on templated medical |
| Hard semantic pathology (Ki67, capsule, ETE distinction) | Llama 3.3 70B 4-bit | Qwen3-72B 4-bit | DeepSeek R1 distill 70B | Reasoning matters for subtle distinctions |
| Short narrative parsing (US descriptions, FNA cyto) | MedGemma 1.5 4B / 27B | Phi-4 14B | n/a | Short text, constrained vocab |
| Imaging report parsing (CT/MRI/NM) | Llama 3.3 70B 4-bit | MedGemma 1.5 27B | Qwen3-235B Thinking | Long narrative, complex anatomy |
| Complications subtyping (op note free text) | MedGemma 1.5 27B | Llama 3.3 70B | DeepSeek R1 distill 70B | Medical vocab heavy |
| Cause-of-death adjudication | Llama 3.3 70B + R1 distill | Qwen3 32B | n/a | Two-model agreement |
| De-identification (PHI scrubbing) | **Philter** (NOT an LLM) | John Snow Labs Healthcare NLP | n/a | LLMs alone miss >50% of PHI |
| NL → BigQuery SQL | DeepSeek V4 Pro (cloud) or Mistral Medium 3.5 local | Qwen 3.5 32B | n/a | SQL is structured; cloud is fine here (no PHI in queries) |
| Code generation (Python, dbt, pipelines) | DeepSeek V4 Pro | Qwen3 Coder 32B | n/a | Highest open-weight coding scores |
| Long-context patient bundle synthesis | Llama 3.3 70B (128K) | Qwen3 32B (128K) | n/a | 128K is local ceiling on consumer hw |
| Biomedical literature retrieval (PubMed RAG) | **MedCPT** retriever + reranker | BM25 + MedCPT rerank | n/a | Embedding task; LLM is generator on top |
| Short clinical sentence retrieval (cohort similarity) | jina-embeddings-v2-base-en | e5-large-v2 | n/a | Generalist beats specialist on short clinical |
| WSI / digital pathology (if ever in scope) | TITAN | mSTAR / PolyPath | n/a | Whole-slide foundation models |
| Medical dictation → text | Google MedASR | Whisper Large V3 Turbo | n/a | Medical-specific beats general STT |
| General dictation / meetings | Qwen3-ASR | Whisper Large V3 | n/a | New SOTA on FLEURS |
| Manuscript drafting (from extracted data) | Claude Opus 4.7 (cloud, your existing workflow) | Llama 3.3 70B local | n/a | Drafting is long-form reasoning; not PHI sensitive once data is de-identified |
| Adjudication / verification (any task) | Qwen3-235B-A22B Thinking | DeepSeek R1 distill 70B | n/a | Different model family from extractor |

---

## Task category 1: Structured data extraction from clinical free text

### 1A. Molecular lab report parsing
**Task:** `molecular_results.raw_payload_json` → structured variants, fusions, CNVs, VAFs, risk call.

**Recommendation: MedGemma 1.5 27B-IT (4-bit MLX, ~14 GB).**

Why: lab reports are templated, medical vocabulary is dense, MedGemma was specifically trained on lab report extraction with 78% F1 demonstrated. It's small enough to run alongside other models in memory.

Pipeline: feed the raw report text + a Pydantic schema (variants[], fusions[], cnvs[], risk_call) via Outlines constrained decoding. Run twice with temperature 0 and 0.2 to estimate self-consistency. Anything where the two runs disagree goes to **Qwen3-235B-A22B Thinking** for adjudication.

Expected throughput on M5 Max: ~50–100 tok/s for MedGemma-27B 4-bit. Your 10,862 reports × ~2K input + ~500 output ≈ 27M tokens total → 80–160 hours single-pass. Run as a nightly batch over 1–2 weeks.

### 1B. Synoptic pathology enrichment (Ki67, mitoses, capsule, ETE, ENE, PNI)
**Task:** Recover the structural fields in `path_synoptics` that are <15% filled, drawing from `clinical_notes_long` (HP + OPNOTE), `synoptic_diagnosis`, and `path_diagnosis_comment`.

**Recommendation: Llama 3.3 70B-Instruct (4-bit MLX, ~38 GB) as the primary extractor; DeepSeek-R1-Distill-Llama-70B (4-bit) as adjudicator.**

Why: this is the hardest semantic task in the portfolio — distinguishing "abuts capsule" from "capsular invasion" from "extracapsular extension" from "gross ETE beyond strap muscle" requires reasoning, not just lexical matching. The 2026 clinical extraction study found Llama 3.3 70B beating medical fine-tunes on exactly this kind of task.

If you can spare the memory budget (M5 Ultra 192 GB or external offload), **Qwen3-235B-A22B-Thinking at 4-bit (~120 GB)** is the open-weight reasoning leader and worth benchmarking head-to-head — it's the model class that closes the gap to Gemini-2.5-pro (87.7% real-world recall on synoptic extraction).

Workflow: chain-of-thought prompt with explicit decision tree (the WHO 2022 / AJCC 8 criteria pasted into the system prompt), JSON schema enforcement via Outlines, two-pass with disagreement-routing to the R1 distill.

**Alternative worth piloting: LoRA fine-tune Llama-3-8B-Instruct on 2,000–5,000 hand-adjudicated synoptic cases.** Published 2026 work hit Macro F1 0.976 on TNM/grade/biomarker extraction with this exact recipe on Llama-3-8B with 10,677 reports. On MLX the loop is ~2 hours for adapter weights. If you can build the gold set, fine-tuning will probably outperform a 70B zero-shot model and run 5–10× faster.

### 1C. Ultrasound nodule feature parsing
**Task:** Extract halo / vascularity / microcalc subtype / shape ratio / spongiform from `ultrasound_reports.nodule_*_source_description`.

**Recommendation: MedGemma 1.5 4B-IT (4-bit MLX, ~2.5 GB) for first-pass; MedGemma 1.5 27B-IT for the ~10% of descriptions with complex syntax.**

Why: descriptions are short (median <500 chars), vocabulary is constrained, throughput matters. Phi-4 14B is a viable alternative (best small-model reasoning per 2026 benchmarks, runs on 8 GB) — worth benchmarking head-to-head against MedGemma 4B on a gold subset.

### 1D. Cross-sectional imaging (CT/MRI/NM) — T-staging features
**Task:** Parse `ct_imaging.original_report` (7,435), `mri_imaging.original_report` (715), `nuclear_med.findings_text` (1,472) for tracheal cartilage erosion, esophageal layers, RLN groove abutment, carotid encasement, mediastinal LN, distant mets.

**Recommendation: Llama 3.3 70B 4-bit for CT/MRI; MedGemma 1.5 4B or 27B for nuclear medicine.**

Why: radiology reports are longer and require anatomic reasoning. NM reports are short and constrained. Different tools.

### 1E. FNA cytology subtype + nuclear features
**Task:** Parse `fna_cytology.path_text` for subtype (the 70% currently null), nuclear features (grooves, pseudoinclusions, powdery chromatin), architectural features, adequacy qualifiers.

**Recommendation: MedGemma 1.5 27B-IT (4-bit MLX).**

Constrained medical vocabulary, short documents — exactly MedGemma's strength.

### 1F. Complication subtyping from op notes
**Task:** Convert `note_entities_complications` from flat "complication" tag to typed (hypoparathyroidism transient/permanent, RLN injury, hematoma, seroma, chyle leak, tracheostomy, voice change, dysphagia, SSI, death).

**Recommendation: MedGemma 1.5 27B-IT as primary, Llama 3.3 70B for adjudication when MedGemma confidence is low.**

The downstream phenotype tables (`complication_phenotype_v1`, `complications_strict_v1`, `extracted_complications_refined_v5`) already do regex-based subtyping, so the LLM's job is to add the cases regex misses and to attribute timing. Tier the extraction: regex first, LLM on negatives only — this drops the corpus to extract from by 90%+.

### 1G. Cause-of-death adjudication
**Task:** From 153 DEATH notes, classify cancer-specific vs non-cancer vs uncertain; extract proximate cause.

**Recommendation: Llama 3.3 70B + DeepSeek-R1-Distill-Llama-70B in tandem, both required to agree, otherwise flag for manual review.**

Small enough corpus that the cost of running two models on every case is negligible (~10 minutes total), and the stakes are high — disease-specific survival is a core endpoint.

### 1H. Functional outcomes (PROMs, severity scales)
**Task:** Extract VHI-10, VHI-30, EAT-10 scores, calcium supplementation regimens, return-to-work timing from clinical notes.

**Recommendation: MedGemma 1.5 4B (4-bit) — fast, accurate on templated medical mentions.**

These are sparse; the small model + a tight regex pre-filter is the right shape.

### 1I. Pre-existing risk factors (radiation, family history)
**Task:** From HP notes (4,280 notes), extract childhood neck radiation, family history of thyroid cancer / MEN2 / FAP / Cowden, smoking pack-years, BMI.

**Recommendation: MedGemma 1.5 4B-IT — single-pass per HP note.**

### 1J. IHC panel parsing
**Task:** `path_synoptics.ancillary_studies` and `path_special_studies` (~1,000 filled total) likely contain IHC results (TTF-1, thyroglobulin, calcitonin, PAX8, Ki-67 IHC, BRAF V600E IHC, p53).

**Recommendation: MedGemma 1.5 27B-IT — small corpus, medical-vocabulary-heavy.**

---

## Task category 2: De-identification (special case — NOT an LLM task)

**Recommendation: Philter (UCSF, open source) as primary; John Snow Labs Healthcare NLP if you can budget it.**

This deserves a callout: published 2026 evidence shows general-purpose LLMs miss >50% of PHI in clinical notes. Philter (regex + statistical model + blacklist/whitelist) hit 99.46% recall on a 2,000-note UCSF corpus and has processed >70M notes in production. John Snow Labs' purpose-built clinical de-identification pipeline beat GPT-4o on PHI detection (96% F1 vs 79%).

Do not use a general LLM for de-identification, even MedGemma. If you want LLM-in-the-loop for de-id, the right pattern is the LPPA framework (2025) — use synthetic notes during training to avoid exposing real PHI.

For your workflow, the practical recipe is:
1. Philter scrubs the note as a regex + statistical first pass (>99% recall on PHI).
2. Optionally, run MedGemma 4B over the scrubbed text as a second-pass detector for residual PHI (catches edge cases Philter misses but is itself imperfect).
3. Manual spot-check on a 5% sample before any external sharing.

---

## Task category 3: Adjudication and quality control

### 3A. Two-model adjudication
Pattern: run extractor (MedGemma or Llama 3.3 70B) → check disagreement → route disagreements to a reasoning model (Qwen3-235B-A22B-Thinking, DeepSeek-R1-Distill-Llama-70B).

This mirrors your existing `ete_adjudication_v1` workflow.

### 3B. Anomaly detection on extracted values
Numerical fields (Ki-67%, mitotic count, tumor size cm, LN ratio) — flag outliers with simple statistical bounds (>3 SD from cohort distribution) and re-extract those cases with a different model.

Don't need an LLM for the bound check itself; just SQL or a Python notebook. LLM only for the re-extraction step.

### 3C. Self-consistency
Run the same model 3× at temperature 0.2–0.4, take majority vote. Useful for low-confidence cases when you don't want to involve a second model.

---

## Task category 4: Database and cohort work

### 4A. NL → BigQuery SQL
**Recommendation: For your workflow, Gemini in BigQuery (managed) or Claude Sonnet 4.6 — both perform well; this isn't a PHI-sensitive task because schemas/queries don't contain PHI.**

If you want a local fallback: Mistral Medium 3.5 (4-bit MLX) or Qwen 3.5 32B. DeepSeek V4 Pro is best open-weight at SQL but too large for local at 1.6T params even with quantization.

The "Gemini for BigQuery" managed offering scores 76.13% on BIRD benchmark — strong, native to your stack, and Google fine-tunes it on BigQuery-specific syntax.

### 4B. Cohort definition refinement
Same as 4A — SQL generation with iteration. Add a verification pass where the model writes its query, then a different agent reads the query plus your cohort manifest and checks for inclusion/exclusion errors.

### 4C. Data dictionary updates / schema documentation
**Recommendation: Llama 3.3 70B local for batch generation of column descriptions; Claude Opus 4.7 for high-stakes documentation that goes to collaborators.**

Schema documentation isn't PHI; Llama-3.3-70B writes clean technical prose. For the Atlas / Notion / Airtable update workflow you already have, the existing Claude Opus call is the right tool.

---

## Task category 5: Manuscript and research workflow

### 5A. Manuscript scope check against the Atlas
You already have this wired through the `thyroid-manuscript-workflow` skill — Claude reads the Atlas before drafting. No change needed; this is exactly the right use of a frontier cloud model.

### 5B. Statistical method recommendation
**Recommendation: Claude Opus 4.7 or DeepSeek-R1 (local 70B distill).**

Reasoning models genuinely outperform here. Qwen3-235B-A22B-Thinking is the open-weight option if you need this offline; DeepSeek-R1 hits 93% diagnostic accuracy on MedQA and shows medical reasoning patterns that match GPT-4-class models.

### 5C. Manuscript drafting from extracted data
Your current workflow (Claude in Cowork) is the right tool. Once your data is structured and de-identified, drafting becomes long-form reasoning + writing — cloud frontier models still lead here.

### 5D. Reviewer response drafting
Same as 5C — Claude Opus 4.7 / Sonnet 4.6 with the manuscript + reviewer comments in context.

### 5E. Literature integration / PubMed RAG
**Recommendation: BM25 + MedCPT retriever → MedCPT cross-encoder reranker → Claude or local Llama 3.3 70B for synthesis.**

The hybrid retrieval (BM25 first stage, MedCPT rerank) hits 0.90 accuracy with 1.91s response time on biomedical IR — better than MedCPT alone. ModernBERT + ColBERT is a newer alternative reaching 0.4448 average on MIRAGE (vs MedCPT's 0.4436) — marginal, not worth the migration unless you're starting fresh.

For embeddings stored in BQ: 768-dim MedCPT vectors fit comfortably in a `VECTOR` column; use `VECTOR_SEARCH` for cosine similarity. Or store in a local vector DB (Qdrant, LanceDB) if you want it off-cloud.

---

## Task category 6: Multimodal — image, audio

### 6A. Whole-slide pathology images (if you ever get H&E digitization)
**Recommendation: TITAN (Mahmood lab, Harvard/MGB) — open-weight whole-slide foundation model.**

Trained on 335,645 WSIs with vision-language alignment. Outperforms ROI and slide foundation models on linear probing, few/zero-shot classification, rare cancer retrieval, cross-modal retrieval, and pathology report generation.

Alternatives: **mSTAR** (slides + reports + gene expression, 26K paired cases across 32 cancer types) for true tri-modal use cases — could be powerful if you ever pair your molecular_results with WSIs. **PolyPath** is MedGemma-based and handles multi-slide cases (up to 40K patches at 10×) and is the right choice for generating reports across multiple slides per case.

If WSIs aren't in scope, skip this section — it's noted because it's a natural extension of the thyroid data.

### 6B. Medical dictation → text
**Recommendation: Google MedASR for dictation; Qwen3-ASR for general meetings/voice memos.**

Google MedASR is medical-specific (trained on 5,000 hours of de-identified physician dictation across specialties) and "significantly lower" error rate than Whisper on medical audio. Open weights, runs on Apple Silicon.

Qwen3-ASR became the new SOTA general open-source ASR in early 2026, beating both commercial and open ASR on most metrics including elderly/child speech and very low SNR. Whisper Large V3 Turbo is still strong (6× faster than V3, 1–2% accuracy loss) if you already have that pipeline.

### 6C. US or radiology image classification (validation tool, not primary)
Generally not a high priority for your structured-data pipeline, but if you ever want a sanity check that a structured TI-RADS extraction matches the actual image — MedGemma 1.5 27B is multimodal and can take an image input. Useful for QC sampling, not for primary extraction.

---

## Task category 7: Embedding-based retrieval and clustering

### 7A. Patient similarity (cohort retrieval, analog cases)
**Recommendation: jina-embeddings-v2-base-en for short clinical text (problem lists, brief summaries); MedCPT for longer biomedical text.**

The 2026 finding that generalist embeddings beat specialist clinical embeddings on short text is counterintuitive but well-replicated. jina-v2 hit 84% exact-match retrieval vs 64% for ClinicalBERT on short clinical queries.

### 7B. Report similarity (QC outliers — "find reports unlike any other")
Same recommendation. Compute embeddings, cluster (UMAP + HDBSCAN), inspect outliers.

### 7C. Find similar molecular profiles
Don't embed text for this — use the structured variant data directly. Tanimoto / Jaccard over the variant set, or feature-engineered vectors for VAF bins, panel coverage, etc.

### 7D. Literature retrieval (RAG over PubMed for manuscript writing)
Covered in 5E — MedCPT.

---

## Task category 8: Specialized tooling

### 8A. Medical NER (entity recognition only, no extraction-to-schema)
**Recommendation: scispaCy (`en_core_sci_lg` or `en_ner_bc5cdr_md`) for entity recognition; MedSpaCy for clinical-context features (negation, history, hypothetical).**

These are sub-second-per-note traditional NLP tools. Good for the regex-replacement layer in your existing entity_value_norm logic. Not LLMs but should be in the stack.

### 8B. Negation/uncertainty detection
**MedSpaCy** with the negation pipeline (NegEx implementation). Or just check the LLM extractor outputs for `present_or_negated`.

### 8C. ICD-10 / CPT / SNOMED code mapping
**Recommendation: SapBERT or BioGPT for surface-form linking; or call NLM UMLS API directly.**

Not an LLM-heavy task — embedding-based dictionary lookup against the UMLS metathesaurus is faster and more deterministic than asking an LLM.

---

## Evaluation framework — how to actually pick

For every Tier 1 extraction (1A–1F above), do this before running over the full corpus:

1. **Build a gold-standard subset.** 50–200 cases per task, manually adjudicated by you (or by you + a second annotator for high-stakes fields). Stratify by complexity: easy templated cases, ambiguous cases, edge cases.

2. **Run the candidate shortlist** with identical prompts and JSON schemas (Outlines):
   - MedGemma 1.5 4B-IT (where appropriate)
   - MedGemma 1.5 27B-IT
   - Llama 3.3 70B-Instruct
   - Qwen3 32B-Instruct (or Qwen3 72B if MLX-converted)
   - DeepSeek-R1-Distill-Llama-70B (for reasoning-heavy tasks)
   - Phi-4 14B (for short-narrative tasks where small + smart wins)

3. **Score on:**
   - F1 per field (micro and macro)
   - Hallucination rate (extracted value with no supporting evidence span in source text)
   - Date attribution accuracy
   - Latency (tokens/sec on M5)
   - Memory footprint
   - Cost-per-corpus (notional: time × elec)

4. **Pick the smallest model that hits F1 ≥ 0.90 on macro.** Bigger isn't better if it doesn't move the needle — small fast models let you re-run extractions cheaply when prompts change.

5. **For tasks where no zero-shot model clears 0.90:** LoRA fine-tune. Llama-3-8B-Instruct + 2,000–5,000 gold-adjudicated examples → 2-hour MLX run → expect macro F1 in the 0.95+ range based on published 2026 pathology results. Cheap, reproducible, and the adapter weights become a permanent asset.

6. **Lock the gold subsets as regression tests.** Anytime a prompt, model, or schema changes, re-score against the gold and watch F1 move.

---

## Memory budget reference (4-bit quantization, MLX)

| Model | Memory | Notes |
|---|---|---|
| MedGemma 1.5 4B | ~2.5 GB | Run many in parallel; fastest for high-volume |
| Phi-4 14B | ~8 GB | Best small-model reasoning |
| MedGemma 1.5 27B | ~14 GB | Best open-weight medical |
| Gemma 3 27B | ~14 GB | Best non-medical Gemma |
| Qwen3 32B | ~18 GB | Strong instruct |
| Llama 3.3 70B | ~38 GB | Best general extraction |
| DeepSeek-R1-Distill-Llama-70B | ~38 GB | Best local reasoning at this size |
| Qwen3-72B-Instruct | ~40 GB | Comparable to Llama 3.3 70B |
| Qwen3-235B-A22B-Thinking | ~120 GB | Top open-weight reasoner; M5 Ultra only |
| Llama 4 Maverick 400B (MoE) | ~210 GB | Out of reach for local; cloud-only |
| DeepSeek V4 Pro 1.6T | n/a | Cloud-only |

For M5 Max 128 GB: fits everything up to Llama 3.3 70B comfortably, with room for context + a small parallel model.
For M5 Ultra 192 GB: can run Qwen3-235B-Thinking, with tight context budget.

---

## Stack recommendation

```
Local (Apple Silicon, MLX):
  Extractors:
    mlx-community/MedGemma-1.5-27B-IT-4bit       # Tier 1: templated medical (1A, 1C, 1E, 1F, 1J)
    mlx-community/Llama-3.3-70B-Instruct-4bit    # Tier 1: hard semantics + imaging (1B, 1D, 1G)
    mlx-community/MedGemma-1.5-4B-IT-4bit        # Tier 1: high-volume short (1H, 1I)
    mlx-community/Phi-4-4bit                     # Optional small-model alternative

  Reasoning / adjudicators:
    mlx-community/DeepSeek-R1-Distill-Llama-70B-4bit
    mlx-community/Qwen3-235B-A22B-Thinking-4bit  # M5 Ultra only

  Embeddings:
    NeuML/pubmedbert-base-embeddings              # biomedical text
    jinaai/jina-embeddings-v2-base-en             # short clinical
    ncbi/MedCPT-Query-Encoder                     # PubMed retrieval
    ncbi/MedCPT-Article-Encoder
    ncbi/MedCPT-Cross-Encoder

  Speech:
    google/medasr                                 # medical dictation
    Qwen/Qwen3-ASR                                # general speech

  Vision (optional, for WSI work):
    mahmoodlab/TITAN                              # whole-slide pathology
    mlx-community/MedGemma-1.5-27B-IT-Multimodal  # general medical VL

  PHI scrubbing (specialist, not LLM):
    BCHSI/philter-ucsf

  Constrained decoding:
    outlines (with mlx backend)
    pydantic schemas per extraction

Cloud (for tasks not constrained by PHI):
  Claude Opus 4.7 / Sonnet 4.6                    # manuscript drafting, scope checking
  Gemini in BigQuery                              # NL → SQL
  DeepSeek V4 Pro                                 # complex code generation if open-weight needed

LoRA fine-tuning track (for tasks where zero-shot doesn't clear 0.90 F1):
  mlx-lm lora --train --model Llama-3-8B-Instruct
  ~2,000-5,000 gold examples per task
  ~2 hours per adapter on M5 Max
  Store adapters in a versioned registry
```

---

## What this changes in the gap analysis

Going back to the original gap doc's recommendations and applying the corrected model choices:

| Original target | Was recommended | Now recommended | Why |
|---|---|---|---|
| Tier 1.1 Molecular variant parsing | Qwen2.5 32B | MedGemma 1.5 27B-IT | Templated medical = MedGemma's sweet spot |
| Tier 1.2 Synoptic enrichment | Qwen2.5 72B | Llama 3.3 70B + R1 distill adjudication, or LoRA Llama-3-8B | Best general extractor + reasoning adjudicator |
| Tier 1.3 US nodule features | Qwen2.5 7B | MedGemma 1.5 4B (volume) or Phi-4 14B | Smaller medical + reasoning option |
| Tier 1.4 CT/MRI/NM | Qwen2.5 32B / 7B | Llama 3.3 70B (CT/MRI) + MedGemma 4B (NM) | Different needs by modality |
| Tier 1.5 FNA cytology | Qwen2.5 7B / 14B | MedGemma 1.5 27B-IT | Medical vocab, short docs |
| Tier 2.6 Complications | Qwen2.5 32B | MedGemma 1.5 27B + Llama 3.3 70B adjudication | Medical-first, fallback adjudicator |
| Tier 2.7 Cause of death | Qwen2.5 7B | Llama 3.3 70B + R1 distill (both must agree) | High stakes, small corpus, two-model affordable |
| All de-identification | (LLM) | **Philter, NOT an LLM** | Specialist tool dominates LLMs at 99.46% recall |

The framework matters more than any specific model name — **benchmark on a gold subset before committing**. Model release velocity through 2026 means today's leader may be replaced in months.

---

## Sources

- [Medmarks: A Comprehensive Open-Source LLM Benchmark Suite for Medical Tasks](https://arxiv.org/abs/2605.01417)
- [MedGemma 1.5 Technical Report](https://arxiv.org/html/2604.05081v2)
- [MedGemma research blog](https://research.google/blog/next-generation-medical-image-interpretation-with-medgemma-15-and-medical-speech-to-text-with-medasr/)
- [Biomedical LLMs not superior to generalist models on unseen medical data](https://arxiv.org/html/2408.13833v1)
- [Comprehensive testing of LLMs for structured pathology extraction (Communications Medicine)](https://www.nature.com/articles/s43856-025-00808-8)
- [Multi-Task LLM with LoRA Fine-Tuning for Cancer Staging and Biomarker Extraction (2026)](https://arxiv.org/abs/2604.13328)
- [Agent-based LLM system for breast cancer synoptic reports (JAMIA Open)](https://academic.oup.com/jamiaopen/article/9/1/ooag016/8496817)
- [Apple ML Research — LLMs with MLX on M5](https://machinelearning.apple.com/research/exploring-llms-mlx-m5)
- [mlx-lm GitHub](https://github.com/ml-explore/mlx-lm)
- [Philter (UCSF) GitHub](https://github.com/BCHSI/philter-ucsf)
- [Open Source PHI De-Identification: Technical Review (IntuitionLabs)](https://intuitionlabs.ai/articles/open-source-phi-de-identification-tools)
- [MedCPT GitHub](https://github.com/ncbi/MedCPT)
- [Generalist embeddings beat specialized on short clinical search](https://arxiv.org/html/2401.01943v2)
- [DeepSeek R1 medical reasoning evaluation (Nature Medicine)](https://www.nature.com/articles/s41591-025-03727-2)
- [Qwen3 announcement](https://qwenlm.github.io/blog/qwen3/)
- [Open-Source LLM Landscape 2026 (DeepSeek V4 / Llama 4 / Qwen 3.5 / Gemma 4)](https://codersera.com/blog/open-source-llms-landscape-2026/)
- [TITAN: Multimodal whole-slide foundation model (Nature Medicine)](https://www.nature.com/articles/s41591-025-03982-3)
- [PolyPath: Multi-slide pathology report generation (Modern Pathology)](https://www.modernpathology.org/article/S0893-3952(25)00184-X/fulltext)
- [mSTAR: Multimodal pathology foundation model](https://www.nature.com/articles/s41467-025-66220-x)
- [Google MedASR launch coverage](https://slator.com/google-launches-medasr-an-open-medical-speech-to-text-model/)
- [Qwen3-ASR (Northflank STT benchmark)](https://northflank.com/blog/best-open-source-speech-to-text-stt-model-in-2026-benchmarks)
- [Long-context LLMs 2026 (TokenMix)](https://tokenmix.ai/blog/llm-context-window-explained)
- [Phi-4 review (TokenMix)](https://tokenmix.ai/blog/phi-4-review-microsoft-small-model-2026)
- [BigQuery NL2SQL with Gemini](https://cloud.google.com/blog/products/data-analytics/nl2sql-with-bigquery-and-gemini)
- [Outlines (structured generation)](https://github.com/dottxt-ai/outlines)
