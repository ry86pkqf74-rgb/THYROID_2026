# TIRADS v1_1 extraction — L4-1 + Qwen2.5-7B

**Goal**: run the `tirads_granular` domain extraction on ~2-3k TIRADS-relevant
notes, today, on the only partition that isn't 7-15 days out (`l4-1-gm24-c16-m64`).

**Pipeline**:

1. `filter_tirads_input.py` — keyword-filters `clinical_notes_long.parquet`
   (11,050 rows) down to notes that plausibly contain TIRADS / ultrasound
   thyroid nodule content (~2,000-3,000 rows).
2. `tirads_v1_1.sbatch` — launches vLLM with Qwen2.5-7B-Instruct in bf16 on 1x L4,
   then runs the existing `scripts/vastai/run_extraction_concurrent.py` engine
   against the filtered parquet with `--domains tirads_granular`.
3. Output lands at
   `/users/lglosse/THYROID_2026/extractions/tirads_v1_1/note_entities_llm_tirads_granular.parquet`.

No MotherDuck config is needed on the cluster. You upload to MD from your Mac
afterward (see §4 below).

---

## 1. One-time prep (from your Mac, before sbatch)

Adjust the `CLUSTER_HOST` / `CLUSTER_USER` variables to match your SSH config.

```bash
# From your Mac terminal
CLUSTER_USER=lglosse
CLUSTER_HOST=hypercluster   # or the full host per your ~/.ssh/config

# 1a. Ship the runbook folder
scp -r ~/THYROID_2026/runbook/v1_1 \
    ${CLUSTER_USER}@${CLUSTER_HOST}:/users/${CLUSTER_USER}/THYROID_2026/runbook/

# 1b. Ship the full clinical_notes_long.parquet (9.9 MB)
ssh ${CLUSTER_USER}@${CLUSTER_HOST} "mkdir -p /scratch/${CLUSTER_USER}/thyroid_repo/processed/remaining"
scp ~/THYROID_2026/scripts/output/parquet/main/clinical_notes_long.parquet \
    ${CLUSTER_USER}@${CLUSTER_HOST}:/scratch/${CLUSTER_USER}/thyroid_repo/processed/remaining/

# 1c. Ship the (possibly newer) run_extraction_concurrent.py and the TIRADS prompt
#     The cluster repo was cloned earlier for tg_kinetics; these files might be stale.
scp ~/THYROID_2026/scripts/vastai/run_extraction_concurrent.py \
    ${CLUSTER_USER}@${CLUSTER_HOST}:/scratch/${CLUSTER_USER}/thyroid_repo/scripts/vastai/
scp ~/THYROID_2026/llm_extraction/prompts/tirads_granular_extraction_v1.txt \
    ${CLUSTER_USER}@${CLUSTER_HOST}:/scratch/${CLUSTER_USER}/thyroid_repo/llm_extraction/prompts/
# also ship the supporting module (base.py etc.) if any imports fail on the cluster run
scp ~/THYROID_2026/llm_extraction/base.py \
    ${CLUSTER_USER}@${CLUSTER_HOST}:/scratch/${CLUSTER_USER}/thyroid_repo/llm_extraction/ 2>/dev/null || true
```

## 2. Submit the job (on the cluster login node)

```bash
# On the cluster
sbatch /users/lglosse/THYROID_2026/runbook/v1_1/tirads_v1_1.sbatch
```

That prints `Submitted batch job <JOBID>`. Track it with:

```bash
squeue -u $USER --start   # predicted start time
squeue -j <JOBID>          # current state
sacct -j <JOBID> --format=JobID,State,Elapsed,ExitCode,MaxRSS
```

While the job runs, tail the progress:

```bash
tail -f /users/lglosse/THYROID_2026/runbook/logs/tirads_v11_<JOBID>.out
```

## 3. Expected timeline on L4-1

| Phase                         | Wall clock    |
|-------------------------------|---------------|
| Slurm cold node spin-up       | 3–5 min       |
| Conda env activation + probe  | <10 s         |
| Input filter                  | <5 s          |
| Qwen2.5-7B download (first run) | 5–10 min (~15 GB via Xet) |
| vLLM server warm-up           | 2–4 min       |
| Extraction (~2,500 notes @ 6 concurrent) | 30–90 min |
| Teardown + summary            | <10 s         |
| **Total**                     | **~45–120 min** |

Subsequent runs skip the model download (cached in `/scratch/lglosse/.cache/huggingface`).

## 4. After it finishes

The output parquet lives on the cluster at:

```
/users/lglosse/THYROID_2026/extractions/tirads_v1_1/note_entities_llm_tirads_granular.parquet
```

Pull it back to your Mac:

```bash
mkdir -p ~/THYROID_2026/extractions/tirads_v1_1/
scp ${CLUSTER_USER}@${CLUSTER_HOST}:/users/${CLUSTER_USER}/THYROID_2026/extractions/tirads_v1_1/note_entities_llm_tirads_granular.parquet \
    ~/THYROID_2026/extractions/tirads_v1_1/
```

Upload to MotherDuck from your Mac (which already has `motherduck.local.toml`
with your RW token):

```bash
# Option A — direct SQL via duckdb CLI
duckdb -c "
ATTACH 'md:Thyroid 2026' AS md;
CREATE OR REPLACE TABLE md.note_entities_llm_tirads_granular AS
  SELECT * FROM read_parquet('$HOME/THYROID_2026/extractions/tirads_v1_1/note_entities_llm_tirads_granular.parquet');
SELECT COUNT(*) AS rows FROM md.note_entities_llm_tirads_granular;
"
```

```python
# Option B — Python using the repo's motherduck_client
# (from ~/THYROID_2026/)
import pandas as pd
from motherduck_client import connect_rw
df = pd.read_parquet("extractions/tirads_v1_1/note_entities_llm_tirads_granular.parquet")
con = connect_rw()  # reads token from motherduck.local.toml
con.execute("CREATE OR REPLACE TABLE note_entities_llm_tirads_granular AS SELECT * FROM df")
print(con.execute("SELECT COUNT(*) FROM note_entities_llm_tirads_granular").fetchone())
```

## 5. Schema of the output parquet

Each row = one note × domain run. The LLM's structured extraction is stored
as a JSON string in `result_json`; unpack with:

```python
df["entities"] = df["result_json"].map(lambda s: json.loads(s).get("entities", []))
```

Columns written by `run_extraction_concurrent.py`:

| Column                    | Meaning                                   |
|---------------------------|-------------------------------------------|
| `note_row_id`             | stable per-note id                        |
| `domain`                  | `"tirads_granular"`                       |
| `llm_model`               | `"Qwen/Qwen2.5-7B-Instruct"`              |
| `llm_base_url`            | `"http://127.0.0.1:8000/v1"`              |
| `extracted_at`            | ISO timestamp                             |
| `result_json`             | full `{"entities": [...]}` JSON           |
| `research_id`             | patient id                                |
| `note_type`               | HP / OPNOTE / etc.                        |
| `note_date`               | note date (if present in input)           |
| `linkage_date`            | `note_date` or `preprocessed_at_utc[:10]` |
| `source_workbook`         | provenance                                |
| `source_sheet`            | provenance                                |
| `source_column`           | provenance                                |
| `note_index`              | provenance                                |
| `preprocess_batch_id`     | provenance                                |
| `preprocessed_at_utc`     | provenance                                |
| `preprocess_script_version` | provenance                              |

Inside each `entities[i]` dict: `entity_type`, `entity_value`, `entity_date`,
`date_confidence`, `date_source_keyword`, `present_or_negated`, `confidence`,
`evidence_text`, `source_line` — per the TIRADS prompt.

## 6. Resumability

The engine writes a per-note JSONL checkpoint at
`${OUT_DIR}/note_entities_llm_tirads_granular.ckpt.jsonl`. If the job is killed
(cluster preemption, Slurm timeout, etc.), re-submitting the same sbatch skips
notes already in the checkpoint.

## 7. If something goes wrong

Common issues:

- **`torch._C` ImportError at phase 1 probe** → `LD_LIBRARY_PATH` isn't being
  set correctly. Check `NV_LIB_PATHS` in the job log. The bundled nvidia libs
  must precede system `/usr/local/cuda/...`.
- **vLLM never becomes ready** → tail
  `/users/lglosse/THYROID_2026/extractions/tirads_v1_1/vllm_server_<JOBID>.log`.
  If it's an OOM at warmup, lower `--max-model-len 4096` and `--gpu-memory-utilization 0.85`.
- **HuggingFace 403 / timeout** → the `cas-bridge.xethub.hf.co` Xet endpoint has
  been reachable from login + compute nodes in prior probes. If you hit
  persistent 403, `export HF_HUB_ENABLE_HF_TRANSFER=0` and retry.
- **`run_extraction_concurrent.py` crashes on `from llm_extraction.base`** →
  the repo layout on cluster is missing `base.py`. Re-scp it per §1c.

## 8. Next steps after TIRADS lands

Once TIRADS is validated:

1. Launch the Qwen2.5-72B-FP8-Dynamic pre-download on a CPU partition or in
   the background on the login node so the A100 job is ready the moment it
   starts (A100-×2 has the shortest 72B-capable queue at ~6 days).
2. Repeat this sbatch pattern for `us_nodule_dynamics`, `cervical_ln_detail`,
   `tg_kinetics`, `pathology`, `imaging` domains using the 72B model on A100.
3. `scripts/vastai/run_extraction_concurrent.py` already supports all domains
   via `--domains <list>`; just swap the model + partition + vLLM flags.
