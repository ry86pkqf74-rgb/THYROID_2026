# Thyroid BQ — MLX On-Device Extraction Gap Analysis

**Scope:** `thyroid-canonical-pub-2026.pub_canonical` (185+ tables), profiled against raw text columns and existing `note_entities_llm_*` outputs to identify under-parsed fields where on-device MLX inference would meaningfully improve the database.

**Why MLX specifically (and not cloud):** every gap below operates on PHI-bearing free text (op notes, path reports, molecular lab reports). Local inference keeps data on the machine, eliminates per-token API costs at corpus scale (~14M tokens of clinical notes alone), and your M5 with neural accelerators + unified memory can sustain useful throughput on a quantized Qwen2.5-72B or Llama-3.3-70B without leaving the desktop.

---

## Tier 1 — High-yield gaps you should run first

### 1. Molecular results — canonical structured calls are functionally empty

`molecular_results` (10,862 rows / 6,028 patients):

| Field | Filled |
|---|---|
| `raw_payload_json` | 10,862 (100%) — full lab report text present |
| `canonical_hgvs` | **0** |
| `risk_call` | **2** |
| `interpretation_summary` | 1,154 (11%) |

Every raw report mentions BRAF, RAS family, TERT, and fusion partners (PAX8/PPARG/RET-PTC/NTRK/ALK/ETV6) — these aren't sparse, they're a checklist format across all 10,862 reports. But the canonical structured columns designed to capture variants, HGVS notation, and risk classification are essentially unpopulated.

**MLX target schema (per `molecular_result_id`):**
- `variants[]`: gene, protein_change, hgvs_c, hgvs_p, VAF, classification (pathogenic / likely / VUS), zygosity
- `fusions[]`: gene_5prime, gene_3prime, breakpoint, supporting_reads
- `cnvs[]`: gene, type (gain/loss/amp/del), copy_number
- `expression_alterations[]`: gene, direction, magnitude
- `risk_call`: ThyroSeq GC v3 risk band (positive/negative/low/high) plus calculated risk %
- `interpretation_summary`: 1–2 sentence narrative

**Model fit:** Qwen2.5-32B-Instruct 4-bit on MLX handles structured genomic extraction well; lab reports are templated and short (median raw_payload is a few KB). Run with `mlx_lm` + JSON-mode constrained decoding.

Downstream effect: unblocks every molecular-stratified survival, recurrence, and ETE analysis. `survival_cohort_enriched` already has `braf_status`/`tert_status`/`ras_status`/`ret_status` columns waiting on this.

---

### 2. Synoptic pathology enrichment — 81.5% of the table is empty

`note_entities_llm_synoptic_pathology_enrichment`:
- 11,037 rows
- **Only 2,041 (18.5%) have meaningful `result_json`**
- The remaining 9,000 rows are empty stubs

Combined with the structural sparseness in `path_synoptics` itself, this is the single biggest extraction debt. Fields downstream analysts need but rarely have:

| `path_synoptics` field | Filled (of 11,688) | Coverage |
|---|---|---|
| `tumor_1_ki_67_labeling_index` | 18 | 0.15% |
| `tumor_1_angioinvasion_quantify` | 310 | 3% |
| `tumor_1_mitotic_rate_per_2mm2` | 713 | 6% |
| `tumor_1_capsular_invasion` | 1,243 | 11% |
| `tumor_1_extranodal_extension` | 1,374 | 12% |
| `tumor_1_perineural_invasion` | 1,508 | 13% |
| `tumor_1_capsule` | 1,726 | 15% |
| `tumor_1_extrathyroidal_extension` | 3,999 | 34% |
| `tumor_1_margin_status` | 4,086 | 35% |

Note: `path_synoptics.microscopic_description` is itself often a stub ("Microscopic examination performed.") — p50 length 305 chars. The real microscopic narrative lives in **`clinical_notes_long`** (path reports embedded in HP/OPNOTE) and in `synoptic_diagnosis` (p90 = 2,232 chars) plus `path_diagnosis_comment` (p90 = 1,304 chars).

**MLX target schema (per `research_id` × tumor index):**
- mitotic_count_per_2mm2 (integer)
- ki67_labeling_index_pct (float)
- capsule_status (encapsulated / partially / unencapsulated / not_described)
- capsular_invasion (present / focal / extensive / absent / not_described)
- angioinvasion_present + vessels_involved_count (Turin/WHO quantification)
- perineural_invasion (yes/no/not_described)
- ETE: none / microscopic-only / gross / beyond_strap (T4a) / aerodigestive (T4b)
- ENE present + size of largest deposit
- evidence_span for each (exact substring quote — needed for QC against the `verification_status` pattern your `note_entities_*` tables already use)

**Model fit:** Qwen2.5-72B-Instruct 4-bit. This is the most semantically demanding extraction in the portfolio (subtle distinctions like "abuts capsule" vs "capsular invasion" vs "extracapsular extension") so do not skimp on model size. Run twice and adjudicate disagreements — your `ete_adjudication_v1` and `extracted_ete_subgraded_v1` patterns already template this workflow.

Why this matters operationally: this single extraction unlocks ATA risk re-stratification, T-stage refinement, and any ATA-2025 / WHO-2022 classification work across the entire malignant cohort.

---

### 3. Ultrasound nodule descriptions — feature parsing under structured fields

`ultrasound_reports`: 6,793 rows. Structured TI-RADS components are well filled (composition/echogenicity/calcifications/margins/shape all ~6,028, ~89%). But `nodule_1_source_description` (6,023 filled) contains features **not** captured in those structured columns:

| Feature in free text | Mentions (of 6,026 with description) |
|---|---|
| Composition language (cyst/solid/mixed) | 4,073 |
| Microcalcifications detail | 424 |
| Spongiform | 397 |
| Halo | 556 |
| Peripheral/rim macrocalcification | 240 |
| Vascularity / doppler / flow | 122 |
| Taller-than-wide explicit language | 122 |
| Capsule contour / abuts | 12 |
| US-ETE language ("extends beyond", "extracapsular") | 18 |
| Tracheal involvement | 2 |
| Comet-tail / colloid | 150 |

The base rates are low for many features because radiologists only mention them when present — that's the signal. Halo (often benign sign), microcalcs (often malignant sign), spongiform (Bethesda-1-leaning), comet-tail (colloid cyst) all currently invisible to structured queries.

**MLX target schema (per `us_report_number` × nodule):**
- vascularity: peripheral / internal / mixed / absent / not_described
- halo: present / absent / not_described
- peripheral_macrocalcs / rim_calcifications
- microcalc_subtype: punctate_echogenic_foci / comet_tail_artifacts / coarse / dystrophic
- shape_ratio: taller_than_wide / wider_than_tall / round
- spongiform_pct (>50% spongiform → ACR TR1)
- us_ete_suspected, trachea_involvement_suspected
- nuclear_features_mentioned, washout_doppler_pattern

**Model fit:** Qwen2.5-7B-Instruct 4-bit is enough — descriptions are short, vocabulary is constrained. Cheap and fast on M5.

This is a feeder for any TI-RADS validation, ACR vs EU-TIRADS concordance, or AI-radiology comparison paper.

---

### 4. Cross-sectional imaging (CT/MRI/NM) — invasion + distant disease

| Source | Full report present |
|---|---|
| `ct_imaging.original_report` | 7,435 |
| `mri_imaging.original_report` | 715 |
| `nuclear_med.findings_text` | 1,472 |

CT structured columns capture some things (substernal extension, tracheal deviation, LN locations) but miss the granular T4 features that drive aerodigestive invasion manuscripts:

- Tracheal cartilage erosion vs lumen invasion (the T4a vs T4b distinction)
- Esophageal muscularis vs adventitia vs lumen
- RLN groove abutment
- Carotid encasement degree (<180° / 180–270° / ≥270°)
- Prevertebral fascia involvement
- Mediastinal LN level (VII)
- Pulmonary mets (count, miliary vs macronodular, size range)
- Bone mets (lytic vs blastic, site)
- Brain mets

There's `note_entities_llm_airway_invasion`, `note_entities_llm_esophageal_invasion`, `note_entities_llm_vascular_invasion`, `note_entities_llm_t4b_invasion_v1` — but these were extracted from **clinical notes**, not from imaging reports directly. Re-running them against `ct_imaging.original_report` and `mri_imaging.original_report` would give you radiologist-direct evidence vs surgeon-paraphrased.

Nuclear medicine `findings_text` is similar — current structured columns capture global uptake but miss focal mediastinal uptake, retrosternal extension on iodine scan, and pulmonary RAI-avid disease patterns.

**Model fit:** Qwen2.5-32B for CT/MRI (longer narrative, complex anatomy), Qwen2.5-7B for NM (shorter reports).

---

### 5. FNA cytology subtype + adequacy

`fna_cytology`: 6,995 rows with `path_text > 100` chars, 7,935 with Bethesda 2023 numeric grade, but **only 2,357 (30%) have `subtype` populated**.

The path_text contains rich cytology language — nuclear features (grooves, pseudoinclusions, powdery chromatin), architecture (papillary fronds, microfollicles, three-dimensional clusters), colloid quality (thin/dense/watery/bubble-gum), atypia descriptors (focal/diffuse/architectural/nuclear), and adequacy notes (Hurthle-only specimen, scant cellularity, hemorrhagic, obscuring blood).

**MLX target schema:**
- bethesda_subcategorization (e.g., Bethesda III split into AUS-nuclear vs AUS-other — drives the 2023 reporting)
- nuclear_features_papillary[] (grooves, pseudoinclusions, irregular contours, powdery chromatin, overlap)
- architectural_features[] (microfollicular, papillary, hurthle, oncocytic, solid)
- atypia_descriptors[]
- adequacy_qualifier (satisfactory / less-than-optimal / scant / obscuring blood / cyst-fluid-only)
- molecular_recommended (yes/no)

**Model fit:** Qwen2.5-7B or 14B. Cytology vocabulary is constrained.

This is a small but high-leverage extraction because every FNA-to-surgery validation cohort depends on it.

---

## Tier 2 — Worthwhile, lower urgency

### 6. Complication entity sub-typing

`note_entities_complications` has 9,359 rows but `entity_type` is **the literal word "complication" for every single row** — no subtyping. Downstream `complication_phenotype_v1`, `extracted_complications_refined_v5`, `complications_strict_v1`, `extracted_rln_injury_refined_v2` rebuild structure post-hoc, but the upstream extraction is flat.

A re-extraction with a typed schema would cleanly categorize:
- hypoparathyroidism (transient / persistent / permanent), with PTH/Ca evidence
- RLN injury (unilateral/bilateral, transient/permanent, with stroboscopy evidence)
- hematoma (postop/late, reoperation required)
- seroma
- chyle leak (output volume, conservative vs surgical management)
- wound infection (SSI grade, organism)
- tracheostomy (timing, indication, decannulated)
- voice change (subjective vs objective, VHI score if present)
- dysphagia (severity, EAT-10 if present)
- death attribution

**MLX target:** Qwen2.5-32B with a strict JSON schema. Re-run over `clinical_notes_long` (OPNOTE + HP + ENDOCRINE_FM = 9,529 notes).

### 7. Cause-of-death adjudication

153 DEATH notes in `clinical_notes_long`, and `survival_cohort_enriched.event_type` exists as a column. Cause of death (cancer-specific vs non-cancer vs uncertain) is essential for **disease-specific survival** vs overall survival — a real methodological win in any survival paper.

**MLX target:** small model (Qwen2.5-7B), tight schema: `cod_attribution {cancer_specific, non_cancer, uncertain}`, `cod_proximate_cause`, `evidence_text`.

### 8. Functional outcome PROMs and severity scales

`note_entities_llm_functional_outcomes` exists but is sparse. The clinical-note corpus likely contains buried mentions of:
- VHI-10 / VHI-30 voice scores
- EAT-10 dysphagia scores
- Quality of life statements (good/limited/poor)
- Return-to-work timing
- Calcium supplementation regimen + duration (proxy for permanence)

Worth a dedicated extraction pass — feeds any quality-of-life or functional-outcome manuscript.

### 9. Pre-existing risk factors

Not currently extracted with dedicated tables:
- Childhood/adolescent neck radiation exposure (yes/no, dose, age, indication)
- Family history of thyroid cancer / MEN2 / FAP / Cowden / Carney
- Iodine deficiency vs sufficient region of origin
- Pack-year smoking
- Alcohol
- BMI / obesity at diagnosis

These live in HP notes' social history and family history sections. **MLX target:** Qwen2.5-7B, single-pass per HP note (4,280 notes).

### 10. Synoptic IHC panel parsing

`path_synoptics.ancillary_studies` and `path_special_studies` (817 + 228 filled) likely contain IHC results (TTF-1, thyroglobulin, calcitonin, PAX8, Ki-67 quantitative, BRAF V600E IHC, NRAS Q61R IHC, p53 IHC, β-catenin). None are parsed to columns. Extract once into a `canonical_ihc_panel_v1` table.

---

## Tier 3 — Nice-to-have / opportunistic

- **Specimen handling adequacy**: cold ischemia time, fixation duration (relevant for molecular reliability) — buried in pathology text occasionally.
- **Preop preparation**: Lugol's iodine, beta-blockers, plasmapheresis (Graves cases) — sometimes in OP notes, sometimes in ED/endo notes.
- **Intraop nerve monitoring details**: `op_nlp_nerve_monitoring_v2` exists; check whether it captures loss-of-signal type/timing (immediate/progressive) — feeds an RLN paper.
- **Genetic counseling referrals / germline testing mentions**: rare events but high-impact if linked to surgical decisions in MEN/FMTC cases.

---

## Recommended execution stack

For a clinical-research M5 / MLX setup:

```
mlx-lm (0.20+)
  ├─ Qwen2.5-72B-Instruct-MLX-4bit          # Tier 1.2 (synoptic enrichment)
  ├─ Qwen2.5-32B-Instruct-MLX-4bit          # Tier 1.1, 1.4, Tier 2.6 (molecular, imaging, complications)
  └─ Qwen2.5-7B-Instruct-MLX-4bit           # Tier 1.3, 1.5, Tier 2.7-2.9 (US, FNA, COD, PROMs)

outlines or jsonformer for constrained JSON decoding
pydantic models per extraction schema for typed output
hash-based caching keyed on (note_row_id, prompt_version) — your existing
  raw_response_sha256 / prompt_version columns already template this
```

Materialize each extraction to a sibling table following your existing pattern:
`note_entities_llm_<domain>_v<n>` → `extracted_<domain>_v<n>` → `canonical_<domain>_events_v<n>` → `canonical_<domain>_patient_rollup_v<n>`. Keep `verification_status`, `extraction_run_id`, `model_name`, `model_version`, `prompt_version` columns identical to what `note_entities_complications` already uses — preserves your QC and signoff machinery (`canonical_table_signoff_registry_v1`).

## A skill worth building

If you want to formalize this into reusable Cowork tooling, a single skill — call it something like **`thyroid-mlx-extract`** — that wraps:

1. Pulling a target note set from BQ
2. Running it through a chosen Qwen-MLX model with a schema
3. Writing back to a `note_entities_llm_*` table with full provenance columns
4. Optionally invoking a second model for adjudication on disagreements

…would let you spin up new extractions without rebuilding the pipeline each time. Drop the schemas above into the skill's `assets/schemas/` folder and you have a reproducible MLX extraction harness. This is the natural follow-up to the "local-mlx-deidentify" skill I mentioned earlier — same infra, different prompts.
