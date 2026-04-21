# HYPERCLUSTER EXTRACTION RUNBOOK v3

**Target:** `tg_kinetics` domain extraction, 11,050 clinical notes / 5,593 unique
research_ids, via vLLM-served `Qwen/Qwen2.5-72B-Instruct-AWQ` on Emory
HyperCluster (8× L40S, TP=8).

**Supersedes:** `HYPERCLUSTER_EXTRACTION_RUNBOOK_v2.md` (2026-04-17 morning).
v2 assumed an interactive `salloc` session with work staged to node-local
`/tmp`. That approach lost state when the SSH session dropped. v3 uses
`sbatch` so the job is independent of the spawning shell, plus NFS-persistent
runbook artifacts.

**Deliverables, all NFS-resident at `~/THYROID_2026/runbook/`:**

  - `RUNBOOK_v3.md`   — this document
  - `extraction.sbatch` — the batch job script
  - `apply_patches.py` — idempotent patcher for Bug 9 + Bug 10
  - `logs/`           — per-job output (created on first run)

---

## §0  Hard-won gotchas (2026-04-17 session)

Five failure modes observed today. v3 eliminates or documents each.

**G1. `salloc` releases the allocation when your SSH session ends.** Even
`nohup`'d, `disown`'d, and backgrounded processes inside an interactive
`salloc` shell get SIGTERM'd when the parent shell exits. Slurm treats the
spawning shell as the allocation's lifeline. *Fix:* use `sbatch` — the batch
job is a separate entity that survives disconnect. This is the single largest
change in v3.

**G2. `/tmp` is node-local and wiped on allocation release.** Anything staged
to `/tmp/thyroid_repo` or `/tmp/launch_vllm.sh` disappears the moment the job
ends. *Fix:* NFS is the only durable storage. Runbook artifacts live at
`~/THYROID_2026/runbook/`. Logs are written via sbatch `--output` directives
pointing to NFS. The job still stages the repo to `/tmp` for faster IO — but
it's a cache, not a source of truth.

**G3. Prior-model output contaminates the run directory.** On 2026-04-03 a
`qwen3:32b` run wrote `note_entities_llm_tg_kinetics.parquet` (11,037 rows,
61 with entities). The new `qwen2.5-72b` run will either overwrite or append
depending on extractor behaviour — either way, silent contamination is the
risk. *Fix:* before any relaunch, check `extracted_at` dates and the
declared model in the backup parquet. If a prior run is present, rename it
with a provenance suffix, e.g. `*_qwen3_32b_baseline.parquet`.

**G4. `rsync` fails with permission errors on this cluster; `cp` works.**
Don't reach for `rsync -av` on NFS — use `cp -r` or `cp -v`. (Root cause not
fully diagnosed, likely `setgid` bit interaction on project directories.)

**G5. `/dev/shm` leaks IPC handles after a crashed vLLM.** `psm_*`,
`sem.loky-*`, and `__KMP_REGISTERED_LIB_*` files accumulate. A subsequent
vLLM launch then fails or behaves erratically until the node reboots. *Fix:*
the sbatch script runs `rm -f /dev/shm/psm_* /dev/shm/sem.loky-*
/dev/shm/__KMP_REGISTERED_LIB_*` before every vLLM launch.

**G6. The l40s partition's job_submit plugin requires `--gpus=N` UNTYPED.**
On `l40s-8-gm384-c192-m1536` (and presumably other l40s partitions), the
following all get rejected even though they should be equivalent:

  - `--gres=gpu:l40s:8`                       → "no GPU requested"
  - `--gres=gpu:l40s:8 --gpus-per-node=l40s:8` → "no GPU requested"
  - `--gpus=l40s:8`                           → "more GPUs than partition provides"
  - `--gpus-per-node=l40s:8`                  → "no GPU requested"

Only `--gpus=8` (untyped, scalar) is accepted. This contrasts with the
a100 partition where `--gres=gpu:a100:1` works fine. Tested 2026-04-17 by
submitting seven 1-minute probe jobs. Plugin error messages are custom
Lua-style strings from a cluster-local `job_submit.lua` we don't have
read access to.

Related plugin quirk: you must also specify `--mem=<SIZE>` explicitly —
without it, the plugin emits "You missed both memory and GPU requirments"
[sic].

**Bonus: HF cache placement.** The HuggingFace cache defaults to
`~/.cache/huggingface/`, which is NFS on this cluster. That's exactly what
you want — the ~140 GB of model safetensors persists across allocations,
saving ~40 min on re-runs. Do NOT set `HF_HOME` to anything under `/tmp`.

---

## §1  Paste-and-go launch

Assumes the three runbook files are already present at
`~/THYROID_2026/runbook/` on the HPC (see §2 for how to get them there).

```bash
# One-time per machine: confirm partition and gres syntax
sinfo -o "%P %c %G %D %N" | grep -i l40s

# If the output differs from the defaults in extraction.sbatch, edit the
# #SBATCH --partition and --gres lines to match. Then:

mkdir -p ~/THYROID_2026/runbook/logs
sbatch ~/THYROID_2026/runbook/extraction.sbatch
```

Slurm prints `Submitted batch job 12345`. That's it — you can log out.

---

## §2  Getting the runbook files onto NFS

The three files were authored in the Cowork workspace on the laptop. To push
them to NFS:

```bash
# From laptop terminal (not HPC)
scp ~/THYROID_2026/runbook/RUNBOOK_v3.md      hpc:~/THYROID_2026/runbook/
scp ~/THYROID_2026/runbook/extraction.sbatch  hpc:~/THYROID_2026/runbook/
scp ~/THYROID_2026/runbook/apply_patches.py   hpc:~/THYROID_2026/runbook/

# On HPC, make the scripts executable and create the logs dir
ssh hpc 'chmod +x ~/THYROID_2026/runbook/extraction.sbatch \
                  ~/THYROID_2026/runbook/apply_patches.py; \
         mkdir -p ~/THYROID_2026/runbook/logs'
```

The `hpc:` alias in the examples is whatever you use — substitute
`lglosse@hyperlogin.emory.edu` or similar.

---

## §3  Monitoring

All logs land on NFS, so you can `tail -F` them from any HPC login node:

```bash
# sbatch stdout / stderr (job-level)
tail -F ~/THYROID_2026/runbook/logs/sbatch_<JOBID>.out
tail -F ~/THYROID_2026/runbook/logs/sbatch_<JOBID>.err

# vLLM server log (one per run)
tail -F ~/THYROID_2026/runbook/logs/<JOBID>/vllm_server.log

# Per-chunk extraction logs (23 total, chunk_01.log … chunk_23.log)
tail -F ~/THYROID_2026/runbook/logs/<JOBID>/chunk_NN.log

# Completed-chunk markers (NFS, idempotent)
ls ~/THYROID_2026/extracted/.completed_chunks/
```

Job state:

```bash
squeue -u $USER
sacct -j <JOBID> --format=JobID,State,Elapsed,ExitCode,MaxRSS
```

---

## §4  Outputs

The sbatch cleanup trap copies outputs to NFS on every exit (successful,
timeout, or crashed), so you always get something back:

```
~/THYROID_2026/extracted/outputs/note_entities_llm_tg_kinetics_<JOBID>.parquet
~/THYROID_2026/extracted/outputs/note_entities_llm_tg_kinetics_backup_<JOBID>.parquet
```

The `_backup_` variant is the extractor's own periodic snapshot (written
every N notes by `LLMExtractor`). Both are note-level parquets: one row per
LLM call, `result_json` column holds the full JSON response as a string.

---

## §5  Resuming an incomplete run

If the job hits the 16-hour wall and only 17/23 chunks completed, the NFS
`.completed_chunks/chunk_NN.done` markers let you resume:

```bash
sbatch ~/THYROID_2026/runbook/extraction.sbatch
```

Submit it again. The chunk loop in `extraction.sbatch` skips any chunk with
a `.done` marker on NFS. New extraction output is appended to the same
backup parquet (the extractor handles this). Re-runs are safe.

If you want to force a full re-run, delete the marker directory first:

```bash
rm -rf ~/THYROID_2026/extracted/.completed_chunks/
# and optionally move the prior output aside
mv ~/THYROID_2026/extracted/outputs/note_entities_llm_tg_kinetics_*.parquet \
   ~/THYROID_2026/extracted/outputs/archive/
```

---

## §6  Patches (Bug 9 + Bug 10)

The sbatch script runs `apply_patches.py` against the staged `/tmp` repo
every launch, so patches apply automatically. You can also apply them
directly to the NFS working tree if you want to commit them:

```bash
python ~/THYROID_2026/runbook/apply_patches.py --repo ~/scratch_repo
# Reports: Bug9 remaining=0, Bug10 present=1 → STATUS: OK
```

Dry-run first if unsure:

```bash
python ~/THYROID_2026/runbook/apply_patches.py --repo ~/scratch_repo --check
```

To commit to a feature branch:

```bash
cd ~/scratch_repo
git checkout -b patches/vllm-extraction-fixes-20260417
git add llm_extraction/extract_llm.py
git commit -m "Bug 9: remove response_format=json_object (outlines_core crash)

Bug 10: strip Qwen's \`\`\`json markdown fences before json.loads.

See ~/THYROID_2026/runbook/apply_patches.py for the idempotent patcher."
```

**Bug 9** — `response_format={"type": "json_object"}` is passed to
`openai.chat.completions.create()` on line ~351 of `extract_llm.py`. This
crashes outlines_core 0.1.26 under vLLM. The patch deletes the line.

**Bug 10** — Qwen2.5-72B-Instruct wraps JSON output in ```` ```json … ``` ````
markdown fences, which `json.loads()` rejects. The patch prepends a
fence-strip before the parse call at line ~420 in `extract_llm.py`.

---

## §7  What the sbatch job actually does

Seven phases (`extraction.sbatch`):

1. **Conda env.** Loads or creates `thyroid_vllm` Python 3.11 conda env.
   Installs vllm==0.6.6 and friends idempotently.
2. **Stage repo to `/tmp`.** `cp -r ~/scratch_repo /tmp/thyroid_repo` for
   faster IO. Strips `.git/`, `.cursor/`, `logs/`.
3. **Apply patches.** `python ~/THYROID_2026/runbook/apply_patches.py
   --repo /tmp/thyroid_repo`. Idempotent — safe on every run.
4. **Stage parquet.** Copies `processed/clinical_notes_long.parquet` from
   NFS into the staged repo. Sanity-checks `rows >= 11000`.
5. **Launch vLLM.** Cleans `/dev/shm`, starts
   `vllm.entrypoints.openai.api_server` in background, polls `/v1/models`
   for up to 60 min (supports cold HF download).
6. **Chunked extraction.** 23 chunks of 250 rids (last = 93). Per chunk:
   check `.completed_chunks/*.done` marker, check remaining time against
   Slurm EndTime minus 15 min buffer, check vLLM health, then run
   `python llm_extraction/run_extraction.py --target tg_kinetics
   --research-ids chunks/chunk_NN.txt --workers 2`.
7. **Summary + cleanup.** Print output row count. The trap on EXIT copies
   parquet outputs from `/tmp` back to NFS `~/THYROID_2026/extracted/outputs/`.

---

## §8  Tunables

Edit these in `extraction.sbatch` if needed:

  - `#SBATCH --partition`  — cluster partition; confirm via `sinfo`
  - `#SBATCH --gres`       — gpu request syntax; confirm via `sinfo -o "%P %G"`
  - `#SBATCH --time`       — default 16h (~2h rebuild + ~13h extraction)
  - `EXTRACT_MAX_CHUNK_CHARS` / `EXTRACT_OP_CHUNK_CHARS` — char budget for
    long notes (default 32k / 48k — matches `run_hpc.sh`)
  - `--workers 2` in the `run_extraction.py` call — concurrent in-flight
    requests to vLLM. 2 is conservative. 4 may work if vLLM isn't saturated.
  - `--max-model-len 8192` in vLLM launch — raise to 16384 or 32768 if you
    see truncation warnings in chunk logs. Costs KV cache memory.

---

## §9  File inventory (what lives where)

**On HPC NFS (persistent):**

  - `~/scratch_repo/` — the working repo. Canonical source.
  - `~/scratch_repo/llm_extraction/extract_llm.py` — the file Bug 9/10
    patches target.
  - `~/scratch_repo/llm_extraction/run_extraction.py` — canonical
    entrypoint. `--target`, `--research-ids`, `--workers`.
  - `~/scratch_repo/config/extraction_domain_registry.yaml` — domain
    definitions. `tg_kinetics` at lines 194-204, `note_scope: all`.
  - `~/scratch_repo/processed/clinical_notes_long.parquet` — 11,050 notes,
    5,593 rids.
  - `~/.cache/huggingface/` — HF model cache, ~140 GB, persistent across
    allocations.
  - `~/THYROID_2026/runbook/` — this runbook, the sbatch, the patcher.
  - `~/THYROID_2026/runbook/logs/` — sbatch stdout/stderr + per-job
    subdirs with `vllm_server.log` and `chunk_NN.log`.
  - `~/THYROID_2026/extracted/outputs/` — final parquets, one pair per
    job ID.
  - `~/THYROID_2026/extracted/.completed_chunks/` — resumability markers.
  - `~/THYROID_2026/extracted/note_entities_llm_tg_kinetics.qwen3_32b_baseline.parquet`
    — preserved prior-model output from 2026-04-03 run.

**On compute node `/tmp` (ephemeral, per-allocation):**

  - `/tmp/thyroid_repo/` — staged working copy of `~/scratch_repo`.
  - `/tmp/thyroid_repo/processed/clinical_notes_long.parquet` — staged.
  - `/tmp/thyroid_repo/chunks/chunk_NN.txt` — regenerated each run.
  - `/tmp/thyroid_repo/processed/note_entities_llm_tg_kinetics.parquet` —
    live output. Copied to NFS by the trap on EXIT.

---

## §10  Quick sanity checks after submission

```bash
# Job is in the queue
squeue -u $USER

# stdout starts appearing within ~30s
tail -F ~/THYROID_2026/runbook/logs/sbatch_<JOBID>.out

# After ~5 min: env set up, repo staged, patches applied, data copied,
# vLLM loading. Watch for "vLLM ready" line.

# First chunk log appears shortly after "vLLM ready"
ls -la ~/THYROID_2026/runbook/logs/<JOBID>/

# Chunk markers accumulate as work completes
watch -n 30 'ls ~/THYROID_2026/extracted/.completed_chunks/ | wc -l'
```

Expected wall time: ~45 min model load on cold cache (or ~3 min on warm
cache), then ~30-40 min per chunk, 23 chunks total → 12-14h extraction.
Total 13-15h including setup. The 16h Slurm limit gives a 1-3h cushion.

---

## §11  When things go wrong

**vLLM won't start.** Check `logs/<JOBID>/vllm_server.log` — typical causes:
HF auth (shouldn't happen for AWQ since it's public), shm limit (G5),
CUDA version mismatch. The sbatch script will exit with a non-zero code and
the trap will NOT copy outputs (there are none).

**Chunk failures mid-run.** The loop logs `FAIL chunk_NN rc=<code>` and
continues. Investigate `chunk_NN.log`. Common causes: vLLM OOM on a single
long note (bump `EXTRACT_MAX_CHUNK_CHARS` down), network flake between
Python and localhost:8000, outlines_core still triggered by some edge
response shape.

**Job times out.** The 15-min HALT buffer should give the trap time to copy
outputs. Re-submit with the same command; resumable markers pick up from the
last completed chunk.

**Slurm EndTime moves** (admin extends the partition, or your job's end time
changes). The loop reads `scontrol show job <JOBID>` each iteration so the
halt-at epoch is re-computed — no action needed.

**NFS outputs missing after success.** Check the trap ran: last line of
`sbatch_<JOBID>.out` should be `cleanup complete`. If not, `scontrol` may
have force-killed the job past the grace period. The output still lives on
the compute node's `/tmp` until the node's next reboot — SSH to the node and
copy manually:

```bash
ssh <compute-hostname>
cp /tmp/thyroid_repo/processed/note_entities_llm_tg_kinetics.parquet \
   ~/THYROID_2026/extracted/outputs/recovered_<timestamp>.parquet
```

---

## §12  Changelog

  - **v3** (2026-04-17 afternoon) — sbatch pivot, NFS-resident runbook,
    `apply_patches.py` idempotent patcher, five gotchas documented.
  - **v2** (2026-04-17 morning) — interactive salloc, `/tmp` staging,
    chunk loop as standalone shell script. Superseded by v3.
  - **v1** (2026-04-16) — initial attempt with `run_hpc.sh`, TP=4, vanilla
    awq quantization, max-model-len 32768. Hit Bug 9. Superseded.
