# Targeted TI-RADS / Cervical LN / Bethesda Rerun — 2026-04-14

Responds to: ChatGPT critique of the "source-limited" verdict on criteria B, D, E.

## Summary

The prior "all remaining gaps are source-limited" conclusion is not sustained.
The TI-RADS, cervical-LN, and Bethesda extraction queues have substantial
recoverable source text already present in the repo under `raw/`.

Queue builder (`scripts/build_extraction_queues.py`) produced:

| Queue | Research IDs | Source evidence |
|---|---|---|
| `ids_tirads.txt` | **3,438** | Nodules described but TR blank or `Not_Scored` in `US Nodules TIRADS 12_1_25.xlsx` (14 US waves) + 191 from `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` |
| `ids_ln.txt` | **1,760** | 61 from `Imaging_12_1_25.xlsx` LN US sheet + 1,716 with cervical/lymph/level mentions in Thyroid US free text |
| `ids_fna.txt` | **5,721** | 98 with null `category_num` or non-empty `error` in `FNAs_Rescored_Long_Format.xlsx` + 5,623 RIDs present in `FNAs 12_5_2025.xlsx` with populated FNA but missing from long-format rescored output |
| **Union** | **8,446** | |

Cross-checked against `processed/remaining/clinical_notes_long.parquet` (5,641 RIDs):
- **4,934 RIDs** (9,703 notes) are both in a queue AND have preprocessed notes → ready for rerun
- **3,512 RIDs** are in a queue but NOT in `clinical_notes_long.parquet` → need preprocessing pass (separate fix; see "Gap" below)

Filtered notes parquet: `processed/remaining/clinical_notes_long_rerun.parquet`

## Vast.ai deployment runbook

### 1. Provision instance

Logan prefers "most capable, want headroom." Choose H100 80GB SXM5 or H200 141GB on Vast.ai.

Image: `ollama/ollama:latest` or base CUDA 12.4 + install ollama.

Minimum disk: 200 GB (qwen3:32b is ~20 GB, notes + outputs ~5 GB, logs + checkpoints ~5 GB, overhead).

### 2. Deploy repo + data

```bash
# On the Vast.ai host
mkdir -p /opt/thyroid_extraction && cd /opt/thyroid_extraction
git clone https://github.com/ry86pkqf74-rgb/THYROID_2026.git .

# Pull the filtered notes parquet + queues (if not via git-lfs/dvc, scp from Logan's mac)
mkdir -p processed/remaining queues
# scp clinical_notes_long_rerun.parquet host:/opt/thyroid_extraction/processed/remaining/
# scp queues/*.txt host:/opt/thyroid_extraction/queues/

pip install -r requirements.txt
```

### 3. Start Ollama + pull model

```bash
ollama serve &            # listens on :11434
ollama pull qwen3:32b
```

### 4. Kick off targeted extraction

```bash
cd /opt/thyroid_extraction

# tirads_granular only (highest value, smallest blast radius)
python scripts/vastai/run_extraction_concurrent.py \
    --input-parquet processed/remaining/clinical_notes_long_rerun.parquet \
    --domains tirads_granular \
    --concurrency 8 \
    --model qwen3:32b \
    --url http://localhost:11434/v1

# then cervical_ln_detail
python scripts/vastai/run_extraction_concurrent.py \
    --input-parquet processed/remaining/clinical_notes_long_rerun.parquet \
    --domains cervical_ln_detail \
    --concurrency 8

# then pathology
python scripts/vastai/run_extraction_concurrent.py \
    --input-parquet processed/remaining/clinical_notes_long_rerun.parquet \
    --domains pathology \
    --concurrency 8
```

Output parquets land in `processed/output/note_entities_llm_<domain>.parquet`
with JSONL per-note checkpoints in the same directory.

### 5. Pull results back

```bash
# On Logan's mac
rsync -av vast-host:/opt/thyroid_extraction/processed/output/note_entities_llm_tirads_granular.parquet \
    "/THYROID_2026/processed/output/"
# repeat for cervical_ln_detail, pathology
```

## Post-rerun: adjudication + promotion

After Vast.ai rerun completes:

1. **OpenAI structured-output adjudication** for residuals (rows where local
   pass returned empty entities or failed validation). Use `gpt-5.4-mini` with
   batch API (50% discount). Use the three prompts in ChatGPT's plan
   (tirads/ln/bethesda adjudicators) with `response_format=json_schema`.

2. **Promotion + formalization gate**:
   ```bash
   python scripts/112_v2_domain_promotion_gate.py --motherduck-check
   python scripts/119_md_formalization_validate.py --md --release-mode
   ```

3. **Before/after audit**: write `studies/20260414_tirads_ln_fna_rerun/` with
   counts pre/post per domain, parser failure counts, and residual source-sparse
   counts.

## Gap: 3,512 RIDs missing from notes parquet

These research_ids have structured TI-RADS, LN, or FNA data in `raw/` but
their note text was never preprocessed into `clinical_notes_long.parquet`.
See `queues/filter_audit.json` for the full missing list sample.

Root-cause hypotheses:
- Wide workbook includes patients without full note text (headers/dates only)
- Preprocessing script filtered these out as "sparse"
- Newer patients added to raw/ after the last preprocess batch
  (`preprocess_batch_id` uniform across current parquet suggests single-batch)

**Follow-up**: re-run the notes preprocessing step against the full `raw/`
set before the next Vast.ai pass, then rebuild the filter parquet.
