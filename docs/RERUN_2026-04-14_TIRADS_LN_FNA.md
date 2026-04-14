# Targeted TI-RADS / Cervical LN / Bethesda Rerun — 2026-04-14

Responds to ChatGPT's critique of the "source-limited" verdict on criteria B, D, E.

## Summary

Prior "all remaining gaps are source-limited" conclusion is overturned. Two
waves of filtered notes are LLM-extracted on a 6-host Vast.ai H100/H200 fleet.

### Wave-1 (Notes 12_1_25.xlsx — already in clinical_notes_long.parquet)

| Queue | Research IDs | Source evidence |
|---|---|---|
| `ids_tirads.txt` | 3,438 | Nodules described but TR blank/`Not_Scored` in `US Nodules TIRADS 12_1_25.xlsx` (14 US waves) + 191 from `COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx` |
| `ids_ln.txt` | 1,760 | 61 from `Imaging_12_1_25.xlsx :: LN US` sheet + 1,716 with cervical/lymph/level mentions in Thyroid US free text |
| `ids_fna.txt` | 5,721 | 98 with null `category_num` or non-empty `error` in `FNAs_Rescored_Long_Format.xlsx` + 5,623 RIDs with populated FNA in `FNAs 12_5_2025.xlsx` missing from long-format |
| **Union** | **8,446** | |

Wave-1 coverage against `processed/remaining/clinical_notes_long.parquet`
(5,641 RIDs, preprocess_batch b29a01dc):
- **4,934 RIDs / 9,703 notes** present → ready for rerun
- **3,512 RIDs** missing → need wave-2 preprocessing (now done)

### Wave-2 (Imaging + FNA free-text — ingested 2026-04-14)

The original notes parquet only contained `Notes 12_1_25.xlsx` (op notes,
H&P, discharge, ED, endocrine, history summaries). It has **zero coverage**
of the primary radiology / cytology text — where TI-RADS scoring, detailed
LN US findings, and Bethesda categories actually live.

Wave-2 preprocessor `scripts/preprocess_imaging_fna_wave2.py` fixes this:

| Source | Column group | Rows scanned | Non-empty notes emitted |
|---|---|---|---|
| `Imaging_12_1_25.xlsx :: Thyroid US` | US-1..US-14: `Clinical hx` + `findings` + `nodule_details` + `Impression` concatenated per US episode | 152,054 episode slots (14 × 10,861 RIDs) | **8,804 thyroid_us_reports** |
| `Imaging_12_1_25.xlsx :: Thyroid US` | supplementary (`Head/neck US findings`, `Thyroid scintigraphy`, `Preop Laryngoscopy`) | — | included |
| `Imaging_12_1_25.xlsx :: LN US` | LN_US1..LN_US4 | 77 populated slots | **77 cervical_ln_us_reports** |
| `FNAs 12_5_2025.xlsx` | FNA#1..FNA#12: `specimen` + `history` + `path_extended` + `Bethesda` concatenated per episode | 137,592 episode slots (12 × 11,467 RIDs) | **8,059 fna_episodes** |
| **Total wave-2** | | | **16,940 notes / 6,545 RIDs** |

### Combined — fleet input

`processed/remaining/clinical_notes_long_combined.parquet`
- **26,643 unique notes / 8,157 unique research_ids**
- 6-way shard by `md5(note_row_id) % 6` in `processed/remaining/shards/`
  (manifest at `shards/manifest.json`, ~4,400 notes per shard)

## Output schema (per note, per domain)

Every extracted row carries full provenance — verified on the live checkpoint:

| Field | Source |
|---|---|
| `note_row_id` | sha1(research_id + source_sheet + source_column + note_index + text) |
| `research_id` | normalized (int/float/str-safe) |
| `note_type`, `note_index`, `note_date` | from preprocess |
| `linkage_date` | preprocessed_at fallback if note_date missing |
| `source_workbook`, `source_sheet`, `source_column` | preprocess provenance |
| `preprocess_batch_id`, `preprocessed_at_utc`, `preprocess_script_version` | preprocess identity |
| `llm_model`, `llm_base_url`, `extracted_at` | LLM identity |
| `domain` | tirads_granular / cervical_ln_detail / pathology |
| `result_json` | domain-specific entities with per-entity `entity_date`, `date_confidence`, `date_source_keyword`, `present_or_negated`, `confidence`, `evidence_text`, `source_line` |

## Fleet topology (provisioned 2026-04-14)

6 single-GPU Vast.ai hosts, each processing one shard:

| Shard | Instance | GPU | SSH | $/hr |
|---|---|---|---|---|
| 00 | 34897258 | H100 SXM 80 GB | root@192.222.53.66:27154 | 1.67 |
| 01 | 34898808 | H100 SXM 80 GB | root@ssh2.vast.ai:18808 | 1.60 |
| 02 | 34898810 | H100 SXM 80 GB | root@ssh3.vast.ai:18810 | 2.13 |
| 03 | 34898811 | H100 NVL 94 GB | root@ssh9.vast.ai:18810 | 2.27 |
| 04 | 34898814 | H200 141 GB | root@ssh7.vast.ai:18814 | 2.32 |
| 05 | 34898815 | H200 141 GB | root@ssh1.vast.ai:18814 | 2.58 |

Approx **$12.57/hr total**, ~2-3 hr wall time → **~$30 for the full rerun**.

### Per-host ollama config (critical)

qwen3:32b default context = 262144. Setting `OLLAMA_NUM_PARALLEL=8`
without capping requests 80 GB KV cache, OOMs, silently falls back to
partial CPU offload (30% GPU util, 2-min request timeouts).

Correct config:
```
OLLAMA_NUM_PARALLEL=8
OLLAMA_CONTEXT_LENGTH=8192
OLLAMA_KV_CACHE_TYPE=q8_0
OLLAMA_FLASH_ATTENTION=1
OLLAMA_KEEP_ALIVE=24h
OLLAMA_MAX_LOADED_MODELS=1
```

Result: 18 GB model + 8.5 GB KV = 27 GB on GPU, 50+ GB headroom, GPU
pinned at ~90%, no CPU offload. Per-call latency ~25 s (8-way parallel →
~19 calls/min/host → ~115/min across 6 hosts → **~12 min per domain,
~35 min for all three**; conservatively 1-2 hours end-to-end incl. warmup
and retries).

`/no_think` is prepended to every system prompt — verified honored (no
`<think>` tags in responses).

## Operate

```bash
# From Mac in repo root

# 1. Build queues + filter + wave-2 + shard (once per rerun)
python scripts/build_extraction_queues.py
python scripts/filter_notes_for_rerun.py \
    --queues queues/ids_tirads.txt queues/ids_ln.txt queues/ids_fna.txt
python scripts/preprocess_imaging_fna_wave2.py
python scripts/combine_and_shard_notes.py --shards 6

# 2. Provision fleet (once; see topology table)
for offer_id in ...; do
    vastai create instance "$offer_id" \
        --image pytorch/pytorch:2.4.1-cuda12.4-cudnn9-runtime \
        --disk 200 --ssh --direct --label "thy-shardN"
done

# 3. Deploy to fleet in parallel
bash scripts/vastai/deploy_fleet.sh

# 4. Monitor throughput per shard
for spec in "root@192.222.53.66 27154" "root@ssh2.vast.ai 18808" \
            "root@ssh3.vast.ai 18810" "root@ssh9.vast.ai 18810" \
            "root@ssh7.vast.ai 18814" "root@ssh1.vast.ai 18814"; do
    set -- $spec
    ssh -p "$2" "$1" \
        'wc -l /root/THYROID_2026/processed/output/note_entities_llm_*.ckpt.jsonl 2>/dev/null'
done

# 5. Aggregate (after all shards complete)
mkdir -p processed/output/fleet_raw
for spec in ... ; do
    set -- $spec
    rsync -av -e "ssh -p $2" \
        "$1:/root/THYROID_2026/processed/output/note_entities_llm_*.parquet" \
        "processed/output/fleet_raw/$(echo $1 | tr @ _)/"
done
python scripts/aggregate_fleet_outputs.py --input-dir processed/output/fleet_raw

# 6. Teardown
vastai destroy instance 34898808 34898810 34898811 34898814 34898815 34897258
```

## Post-rerun: adjudication + promotion

1. **OpenAI structured-output adjudication** for residuals (empty
   entities or validator failures). `gpt-5.4-mini` batch API. **PHI
   constraint**: do NOT send raw `note_text`; only send the pre-extracted
   `evidence_text` strings from the local pass (they are short, local
   sentences that contain only the entity phrase).

2. **Promotion + formalization gate**:
   ```
   python scripts/112_v2_domain_promotion_gate.py --motherduck-check
   python scripts/119_md_formalization_validate.py --md --release-mode
   ```

3. **Before/after audit**: `studies/20260414_tirads_ln_fna_rerun/` —
   counts pre/post per domain, parser failure counts, residual
   source-sparse counts, recovered-RID list.

## Source-limited residual (post-wave-2)

Combined coverage: 8,157 RIDs / 8,446 queue union = **97%**. The
remaining **289 RIDs** have structured TI-RADS/LN/FNA data in raw/ but
no prose free text anywhere in Notes, Imaging, or FNA workbooks. These
are genuinely source-limited and will remain as such.
