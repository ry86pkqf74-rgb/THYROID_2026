# READ-ONLY SCOPING; LOGAN RATIFICATION REQUIRED BEFORE ANY APPLY

# mig_194 — thyroid US NLP source unblock scoping (mig_189 prerequisite)

**Date:** 2026-04-30  
**Lane:** `mig_194 / thyroid_us_nlp_source_unblock_scoping`  
**Batch:** `mig_194_thyroid_us_nlp_source_unblock_20260430`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Posture:** READ-ONLY diagnostic. No MotherDuck DDL/DML was executed.  
**Prompt source note:** `git fetch origin` was executed, but the mig_194 prompt file is not present on `origin/main`; the executable prompt was the workspace file `cursor_prompts/CURSOR_PROMPT_mig194_thyroid_us_nlp_source_unblock_20260430.md`.

## Executive decision card

**Blocker confirmed:** `main.clinical_note_thyroid_us_extracted_v1` does **not** exist on live MotherDuck. Therefore the ratified mig_189 skeleton cannot safely run as written because its §0d NLP-only-pair gate and §B–§F DDL reference a missing upstream table.

**Recommendation:** **Option B — shell-only build**. Rewrite the follow-up apply lane to build `canonical_us_thyroid_gland_events_v2` and the patient rollup from the existing `canonical_us_thyroid_gland_v2` shell only, with `exam_id_source` limited to `{structured, fallback}`. Do **not** manufacture an NLP table and do **not** run a new remote LLM extraction in this prerequisite lane.

**Why:** Script 364 explicitly says "NO LLM in this pass" and sets all parenchyma/NLP fields to NULL with `nlp_backfill_pending=TRUE`; live MD has LN and TIRADS LLM infrastructure but no thyroid-US-gland extraction table or domain-specific registry entry. Building a high-quality gland-parenchyma NLP source would be a separate prompt/runtime/QC project, not a quick unblock for mig_189.

---

## §1 NLP infrastructure inventory (live MD + repo findings)

### 1.1 Live MD source table inventory

Read-only probes wrote inventory artifacts under `exports/mig194_nlp_source_inventory_20260430/`.

| Probe target | Exists on MD? | Rows | Distinct patients | Interpretation |
|---|---:|---:|---:|---|
| `main.clinical_note_thyroid_us_extracted_v1` | **No** | — | — | **Hard blocker for mig_189 as written.** |
| `main.clinical_note_ln_extracted_v1` | Yes | 7,751 | 3,588 | Existing LN analog; suitable schema pattern, not a thyroid gland source. |
| `main.canonical_us_thyroid_gland_v2` | Yes | 13,578 | 10,859 | Current shell/source table; built by Script 364/mig_117 lineage. |
| `main.note_entities_llm_cervical_ln_detail` | Yes | 10,084 | 5,106 | LLM note-level source for cervical LN detail. |
| `main.note_entities_llm_tirads_granular` | Yes | 10,084 | 5,106 | LLM note-level source for TIRADS/nodule descriptors. |
| `main.clinical_notes_long` | Yes | 11,050 | 5,593 | Clinical-note corpus, but not itself extracted gland phenotype facts. |
| `raw.ultrasound_reports` | Yes | 6,793 | 4,074 | Structured/raw ultrasound report table used by Script 364. |
| `raw.us_nodules_tirads` | Yes | 10,859 | 10,859 | Fallback US shell source used by Script 364. |

Inventory class counts from `note_tables_inventory.csv`:

| Class | Count |
|---|---:|
| `US_RELATED_TABLE` | 46 |
| `LLM_OTHER_DOMAIN` | 14 |
| `RAW_OR_CANONICAL_US_SOURCE` | 5 |
| `LN_ANALOG_TABLE` | 1 |
| `LLM_LN_SOURCE` | 1 |
| `LLM_TIRADS_SOURCE` | 1 |

No inventory row matched `clinical_note_thyroid_us_extracted_v1` because the table does not exist.

### 1.2 Existing `canonical_us_thyroid_gland_v2`

`canonical_us_thyroid_gland_v2` exists with **13,578 rows / 10,859 patients / 32 columns**. Its design is a shell/structured table:

- structured dimensions: right/left lobe dimensions and volumes, isthmus thickness, total volume/size text;
- text carry-through: `clinical_impression_text`, `source_us_impression_text`, `recommendation_text`, `radiologist`, `study_indication`;
- source flags: `source_ultrasound_reports`, `source_us_nodules_tirads`;
- diagnostic flag: `nlp_backfill_pending`.

The key parenchyma fields are currently NULL by construction in Script 364: `background_echogenicity`, `heterogeneity`, `hashimoto_pattern`, `vascularity_overall`, `calcifications_parenchymal`, plus related boolean shell fields.

### 1.3 Script 364 lineage conclusion

`script_364_lineage_trace.csv` captures the relevant source lines. The decisive facts are:

- Line 4: Script 364 declares: **"NO LLM in this pass. Pure regex"**.
- Lines 91 and 185: only source tables are `raw.ultrasound_reports` and `raw.us_nodules_tirads`.
- Lines 121-126 and 169-174: parenchyma/gland phenotype fields are hard-coded `NULL`.
- Lines 134 and 182: `nlp_backfill_pending=TRUE` is assigned for both structured and fallback rows.
- Lines 196-204: the table comment reiterates that parenchyma fields have no current parsed source and remain NULL with `nlp_backfill_pending=TRUE`.

**Judgment:** Script 364 did not ingest any NLP-derived thyroid gland data. It intentionally left the NLP surface as a future backfill.

### 1.4 NLP extractor pattern from the LN analog

The most relevant analog is Script 382: `scripts/382_cervical_ln_clinical_merge_load_rollup.py`.

Observed pattern:

1. A remote/concurrent LLM run produces checkpoint JSONL at `runs/round2_20260421/cervical_ln_detail/output/note_entities_llm_cervical_ln_detail.ckpt.jsonl`.
2. Script 382 merges/deduplicates checkpoint rows to a note-level parquet with one row per `note_row_id`.
3. It loads that parquet to `main.note_entities_llm_cervical_ln_detail`.
4. It UNNESTs `result_json.entities` into `canonical_cervical_ln_clinical_events_v1`.
5. It creates a per-patient rollup and populates `nlp_cervln_*` columns.
6. It registers lineage and preserves the US-imaging-side canonical separately.

Runtime/model evidence from `runs/round2_20260421/chain.log`:

- domain: `cervical_ln_detail`;
- input: `processed/remaining/round2_20260421/input_clinical_notes_long.parquet`;
- input size: **10,084 notes**;
- model: **`qwen2.5-32b`**;
- remote vLLM URL: RunPod endpoint;
- concurrency: **256**;
- completed **10,084 rows** in **48.7 minutes** at about **3.5 notes/sec**.

This is mature enough to serve as a pattern, but it is not a substitute source for thyroid gland parenchyma; it extracts LN-specific entities (`ln_level`, `ln_size`, `fna_of_ln`, etc.).

### 1.5 `note_entities_llm_*` tables

Live MD has **16** `note_entities_llm*` tables. Domain tables present include cervical LN detail, TIRADS granular, pathology, RAI, recurrence, frozen section, symptoms, PMH/PSH, dynamic risk, and several invasion/detail domains.

There is **no** table named like any of:

- `note_entities_llm_thyroid_us_gland`
- `note_entities_llm_us_gland`
- `note_entities_llm_thyroid_us`
- `clinical_note_thyroid_us_extracted_v1`

The closest imaging-domain source is `note_entities_llm_tirads_granular`; it is aimed at nodules/TIRADS, not gland parenchyma fields required by mig_189.

### 1.6 Registry and runs inventory

`config/extraction_domain_registry.yaml` has v2 imaging-related domains:

- `imaging` → `note_entities_llm_imaging`
- `tirads_granular` → `note_entities_llm_tirads_granular`
- `us_nodule_dynamics` → `note_entities_llm_us_nodule_dynamics`

It has **no thyroid-US-gland/parenchyma domain** and no canonical target for `clinical_note_thyroid_us_extracted_v1`.

`runs/` evidence shows round-2 runs for `cervical_ln_detail` and `tirads_granular`, plus TIRADS v2 sanitizer/ship artifacts. No thyroid-US-gland extraction run was found.

### 1.7 `clinical_note_ln_extracted_v1` schema

The LN analog has **30 columns**, including generic evidence fields and LN-specific normalized fields:

- generic/provenance: `source_note_type`, `note_row_id`, `research_id`, `note_date`, `source_workbook`, `source_sheet`, `original_llm_model`, `entity_index`, `entity_type`, `entity_value`, `entity_date`, `date_confidence`, `date_source_keyword`, `present_or_negated`, `confidence`, `evidence_text`, `source_line`, `evidence_source_modality`, `modality_classification_method`, `extraction_status`, `extraction_error`;
- LN-specific: `ln_level`, `laterality`, `size_cm`, `count_positive`, `count_total_examined`, `ln_status`, `extranodal_extension`.

A thyroid-US-gland analog would need a new domain-specific schema replacing LN-specific fields with echogenicity, heterogeneity, Hashimoto/thyroiditis pattern, vascularity, parenchymal calcification, goiter, substernal extension, pyramidal lobe, and measurement-snippet provenance.

---

## §2 Decision matrix — mig_189 unblock surface

| Option | Description | Effort | Manuscript impact | Operational risk | Assessment |
|---|---|---:|---|---|---|
| **A — Build NLP source from scratch** | Design/run a new thyroid-US-gland LLM extraction, load `clinical_note_thyroid_us_extracted_v1`, then re-run mig_189 with NLP supplemental rows. | **2-5 days** including prompt/schema design, remote runtime, upload, QC, and false-positive review. | Potentially improves parenchyma fields, but unlikely to change current primary denominators unless gland parenchyma becomes a manuscript exposure. | Medium/high: new remote LLM run, new source table, new parser, new validation burden, PHI-safe evidence handling. | Correct long-term solution if gland parenchyma becomes analytically important; too heavy for a prerequisite unblock. |
| **B — Shell-only build** | Supersede mig_189 with a shell-only migration: events/rollup from `canonical_us_thyroid_gland_v2`, no `clinical_note_thyroid_us_extracted_v1` dependency. `exam_id_source ∈ {structured, fallback}` only. | **0.5-1 day** for SQL rewrite + validation. | Does not add NLP-only gland findings; closes CF-117 trace for shell/events lineage without changing clinical definitions. | Low: reads verified shell table; does not mutate Script 364 or invent NLP rows. | **Recommended.** Best balance of unblock value, governance safety, and manuscript pragmatism. |
| **C — Cancel/defer mig_189** | Leave CF-117 as tagged/source-limited; document as data-source limitation. | **0-2 hours** to update status docs only. | No data or denominator changes. | Lowest DB risk, but leaves v2 event/rollup gap and stale CF open. | Acceptable only if Logan decides CF-117 is nonessential for current manuscript freeze. |

---

## §3 Recommendation

**Recommend Option B — shell-only build.**

Rationale:

1. The expected NLP source table is absent on MD.
2. Script 364 deliberately made the shell table structured-only and set `nlp_backfill_pending=TRUE` for all rows.
3. Existing LLM infrastructure can support a future thyroid-US-gland extraction, but there is no current domain prompt, registry entry, run artifact, or loaded table for it.
4. Option B unblocks the mig_189 family by making the apply lane match the actual source state rather than assuming a non-existent NLP table.
5. The manuscript risk of omitting NLP-only gland parenchyma is low unless Logan has a specific analysis depending on Hashimoto/heterogeneity/vascularity/calcs from free text.

Recommended ratification text:

> Ratify Option B. Cowork should author a follow-up `mig_194_apply_B` / revised mig_189 migration that builds `canonical_us_thyroid_gland_events_v2` and its patient rollup from `canonical_us_thyroid_gland_v2` only; removes all `clinical_note_thyroid_us_extracted_v1` references; constrains source values to `structured` and `fallback`; and changes NLP-specific gates to documented WARN/SKIP source-limitation checks.

---

## §4 What Cowork applies if Logan picks each option

### If Logan picks Option A

Cowork should **not** start by editing mig_189. First author a dedicated extraction lane:

1. Add a registry domain, likely `thyroid_us_gland` or `thyroid_us_parenchyma`, in `config/extraction_domain_registry.yaml`.
2. Write prompt/schema for gland parenchyma and global thyroid findings.
3. Build input parquet from thyroid-US-relevant notes/reports only, with PHI-safe evidence snippets.
4. Run LLM extraction on an approved local/remote endpoint with cost/runtime approval.
5. Validate extracted entities and create/load `clinical_note_thyroid_us_extracted_v1` with LN-analog generic fields plus gland-specific normalized fields.
6. Only then revisit mig_189's NLP supplemental DDL.

### If Logan picks Option B

Cowork should author an apply migration that:

1. Reads only `main.canonical_us_thyroid_gland_v2` plus existing exam-master helpers if needed.
2. Builds `main.canonical_us_thyroid_gland_events_v2` from shell rows.
3. Builds a patient rollup on the 10,871-row CPM spine.
4. Sets source classification to `structured` when `source_ultrasound_reports=TRUE`, otherwise `fallback` when `source_us_nodules_tirads=TRUE` or date/source metadata is missing.
5. Removes `nlp_supplemental` as an allowed source value.
6. Changes G7 to assert only `structured` and `fallback` values.
7. Changes G8 to a WARN/SKIP record: "no NLP supplemental source exists as of mig_194; source limitation accepted by Logan."
8. Preserves `canonical_us_thyroid_gland_v2` unchanged.

### If Logan picks Option C

Cowork should:

1. Leave mig_189 untouched as a historical skeleton.
2. Add a brief report/registry note that CF-117 remains source-limited pending future thyroid-US-gland NLP extraction.
3. Optionally add a manuscript caveat that gland parenchyma fields are shell-present but NLP-unpopulated.

---

## Deliverables

- `qc_framework_v1/reports/mig_194_thyroid_us_nlp_source_unblock_20260430.md`
- `exports/mig194_nlp_source_inventory_20260430/note_tables_inventory.csv`
- `exports/mig194_nlp_source_inventory_20260430/script_364_lineage_trace.csv`
- `exports/mig194_nlp_source_inventory_20260430/manifest.json`

Supplementary artifacts also created:

- `exports/mig194_nlp_source_inventory_20260430/focused_table_probe.csv`
- `exports/mig194_nlp_source_inventory_20260430/clinical_note_ln_extracted_v1_schema.csv`
- `exports/mig194_nlp_source_inventory_20260430/note_entities_llm_tables.csv`
