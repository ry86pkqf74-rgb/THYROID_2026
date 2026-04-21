# THYROID_2026 — Cowork Continuation Prompt

**Copy everything below into a new Cowork chat to resume.**

---

## Context

I'm Logan Glosser (Emory University) working on **THYROID_2026**, a thyroid cancer research database. The canonical publication database (`thyroid_canonical_publication_v1_0`) is hosted in MotherDuck with 10,871 patients and 1,492 columns. My workspace folder is the THYROID_2026 repo.

## What We're Doing

We're **re-extracting 5 NLP domains** that had near-zero entity extraction rates (0.1–1.1%) due to a chunk truncation bug — the original pipeline truncated clinical notes to 6,000 characters, but these domains require much longer context. We fixed the code to support configurable chunk sizes via environment variables and are running extraction on **Emory HPC L40S GPUs** using a self-hosted **Qwen2.5-72B-Instruct-AWQ** model served via **vLLM**.

### The 5 Failing Domains

| Domain | note_scope | Status |
|--------|-----------|--------|
| `tg_kinetics` | all (11,050 notes) | **RUNNING NOW on HPC** — 8 workers, ~120 notes/min, est. ~1.5-2 hrs |
| `dynamic_risk_response` | all (11,050 notes) | Pending — run after tg_kinetics |
| `us_nodule_dynamics` | all (11,050 notes) | Pending — run after dynamic_risk_response |
| `presenting_symptoms` | all (11,050 notes) | Pending — run after us_nodule_dynamics |
| `synoptic_pathology_enrichment` | path_report (0 notes matched!) | **BLOCKED** — needs scope fix (see below) |

### Root Cause of Original Failure

`llm_extraction/extract_llm.py` had `MAX_CHUNK_CHARS = 6000` hardcoded. For domains needing full-note context (Tg values buried deep in lab sections, synoptic reports at end of path notes, etc.), critical content was silently truncated. Fix: environment variable override `EXTRACT_MAX_CHUNK_CHARS=32000` and `EXTRACT_OP_CHUNK_CHARS=48000`.

## Current HPC Session Details

- **Cluster**: Emory HyperC3, node `hyper-01-prod-comp-di-0241-132`
- **GPUs**: 8× NVIDIA L40S (46GB each), all allocated to vLLM
- **SLURM job**: ~6-hour allocation on partition `l40s-8-gm384-c192-m1536`
- **vLLM server**: Running on all 8 GPUs, port 8000
  - Model: `Qwen/Qwen2.5-72B-Instruct-AWQ`
  - Quantization: `awq_marlin`
  - Max model len: 16384 tokens
  - Enforce eager mode (no CUDA graphs — needed to fit in memory)
- **Repo on HPC**: `/tmp/thyroid_repo` (shallow clone)
- **Clinical notes**: `/tmp/thyroid_repo/processed/clinical_notes_long.parquet` (11,050 notes)
- **Python packages**: Installed to `/tmp/pylibs` (home dir has 50GB quota, already full)
- **Environment variables set**:
  ```
  PYTHONPATH=/tmp/pylibs:$PYTHONPATH
  HF_HOME=/tmp/hf_cache
  VLLM_BASE_URL=http://localhost:8000/v1
  VLLM_MODEL_ID=Qwen/Qwen2.5-72B-Instruct-AWQ
  VLLM_API_KEY=dummy
  EXTRACT_MAX_CHUNK_CHARS=32000
  EXTRACT_OP_CHUNK_CHARS=48000
  ```

### Commands to Run Remaining Domains (on HPC GPU node)

After tg_kinetics finishes:
```bash
cd /tmp/thyroid_repo
python3 llm_extraction/run_extraction.py --target dynamic_risk_response --workers 8
python3 llm_extraction/run_extraction.py --target us_nodule_dynamics --workers 8
python3 llm_extraction/run_extraction.py --target presenting_symptoms --workers 8
```

### If the SLURM Job Expires

You'll need a new allocation and to redo the setup (everything in /tmp is lost):
```bash
srun --partition=l40s-8-gm384-c192-m1536 --gpus=4 --mem=192G --time=06:00:00 --pty bash

# Then on the GPU node:
export LD_LIBRARY_PATH=/opt/anaconda/lib:$LD_LIBRARY_PATH
PIP_CACHE_DIR=/tmp/pc pip install --no-cache-dir --target=/tmp/pylibs vllm openai pandas pyarrow pyyaml
export PYTHONPATH=/tmp/pylibs:$PYTHONPATH
export HF_HOME=/tmp/hf_cache
export VLLM_BASE_URL=http://localhost:8000/v1
export VLLM_MODEL_ID=Qwen/Qwen2.5-72B-Instruct-AWQ
export VLLM_API_KEY=dummy
export EXTRACT_MAX_CHUNK_CHARS=32000
export EXTRACT_OP_CHUNK_CHARS=48000

git clone --depth 1 https://github.com/ry86pkqf74-rgb/THYROID_2026.git /tmp/thyroid_repo
# Upload clinical_notes_long.parquet to /tmp/thyroid_repo/processed/ via OnDemand Files

CUDA_VISIBLE_DEVICES=0,1,2,3,4,5,6,7 python3 -m vllm.entrypoints.openai.api_server \
  --model Qwen/Qwen2.5-72B-Instruct-AWQ --quantization awq_marlin \
  --tensor-parallel-size 8 --max-model-len 16384 --gpu-memory-utilization 0.85 \
  --port 8000 --dtype float16 --trust-remote-code --enforce-eager &

# Wait for "Application startup complete", then run extraction
```

## Known Issues & Fixes Applied

| Issue | Fix |
|-------|-----|
| Chunk truncation (6K chars) | Env vars: EXTRACT_MAX_CHUNK_CHARS=32000, EXTRACT_OP_CHUNK_CHARS=48000 |
| Home dir quota (50GB full) | Install everything to /tmp (746GB tmpfs) |
| Python 3.9 on HPC (vLLM needs 3.10+) | Base conda already has Python 3.11 |
| CUDA OOM on 4 GPUs | Use all 8 GPUs with awq_marlin quantization + enforce_eager |
| conda activate fails in scripts | Use base conda directly, pip install to /tmp |
| synoptic_pathology_enrichment: 0 notes | note_scope="path_report" but no notes have note_type="path_report" — actual types are OPNOTE, HP, OTHER_HISTORY, ENDOCRINE_FM, ED_NOTE, DC_SUM, OTHER_NOTES, DEATH |
| OnDemand terminal paste issues | Type commands manually or use very short single-line commands |

## synoptic_pathology_enrichment Scope Fix (TODO)

The registry (`config/extraction_domain_registry.yaml`) sets `note_scope: path_report`, but no notes in clinical_notes_long.parquet have `note_type="path_report"`. The pathology findings are likely embedded in OPNOTE notes.

**Options:**
1. Change the registry to `note_scope: all` for this domain (extracts from all 11,050 notes)
2. Change the registry to `note_scope: op_note` (extracts from 4,727 OPNOTE notes)
3. Add a new scope like `path_and_op` to `_NOTE_SCOPE_TYPES` in `run_extraction.py`
4. Temporarily override in code for this run only

The OPNOTE scope makes the most clinical sense — synoptic pathology summaries are typically appended to or referenced in operative/surgical notes.

## Post-Extraction Plan

### Step 1: Validate Outputs
After each domain finishes, spot-check the output parquet:
```python
import pandas as pd
df = pd.read_parquet('processed/note_entities_llm_tg_kinetics.parquet')
print(f"Total entities: {len(df)}")
print(f"Unique patients: {df['research_id'].nunique()}")
print(f"Entity types: {df['entity_type'].value_counts()}")
print(f"Verification: {df['verification_status'].value_counts()}")
print(f"Sample evidence: {df['evidence_span'].head(3).tolist()}")
```

### Step 2: Download Results from HPC
Copy parquets from HPC to local machine. From the OnDemand file manager, download:
- `/tmp/thyroid_repo/processed/note_entities_llm_tg_kinetics.parquet`
- `/tmp/thyroid_repo/processed/note_entities_llm_dynamic_risk_response.parquet`
- `/tmp/thyroid_repo/processed/note_entities_llm_us_nodule_dynamics.parquet`
- `/tmp/thyroid_repo/processed/note_entities_llm_presenting_symptoms.parquet`
- (Eventually) `note_entities_llm_synoptic_pathology_enrichment.parquet`

Place them in the local repo at `processed/`.

### Step 3: Ingest to MotherDuck
Run locally (the script is already written):
```bash
python hpc/02_ingest_to_motherduck.py --dry-run   # validate first
python hpc/02_ingest_to_motherduck.py              # upload to MotherDuck
```
This replaces the near-empty tables in `thyroid_canonical_publication_v1_0` with the new extractions. It backs up existing tables first.

### Step 4: Phase 1 SQL Quick Fixes to canonical_patient_master
These were identified before the re-extraction work:
- Backfill `multifocal_flag` from tumor characteristics
- Align `ln_positive_flag` with pathology N-stage
- Normalize TIRADS scoring and frozen section results
- Roll up the new NE (entity) tables into canonical views
- Surface central neck lymph node (CNLN) positives

### Step 5: v1.1 Promotion
Run `scripts/225_promote_canonical_version.py` to promote the database from v1.0 to v1.1 with the re-extracted domains included.

## Key Files Reference

| File | Purpose |
|------|---------|
| `llm_extraction/extract_llm.py` | LLMExtractor class — vLLM provider at lines 78-90, chunk sizes at lines 45-47 |
| `llm_extraction/run_extraction.py` | Orchestrator — note_scope filtering at lines 104-136, domain dispatch |
| `config/extraction_domain_registry.yaml` | Domain definitions (31 domains, note_scope, tier, extractors) |
| `llm_extraction/prompts/*_extraction_v1.txt` | Per-domain extraction prompts (well-crafted, no changes needed) |
| `hpc/02_ingest_to_motherduck.py` | Upload parquets to MotherDuck (supports --dry-run) |
| `processed/clinical_notes_long.parquet` | 11,050 clinical notes, 11 columns |

## Code Review Findings (from pipeline audit)

**Working correctly:**
- vLLM provider with JSON mode enforcement (`response_format={"type": "json_object"}`)
- Evidence text verified as exact substring in original note
- 44-column output schema with full provenance tracking
- Rejected/hallucinated entities flagged with `verification_status="rejected"`

**Watch for in results:**
- Filter to `verification_status="verified_substring"` for clean analysis data
- Confidence values not bounds-checked (could be >1.0 or <0.0 theoretically)
- Evidence span uses first occurrence via `.find()` — correct position but could mismatch if text appears multiple times

## MotherDuck Connection

Database: `thyroid_canonical_publication_v1_0`
Token: via MOTHERDUCK_TOKEN env var or `.env` file or `motherduck_client.py` helper
The MotherDuck MCP connector is available in Cowork for direct SQL queries.
