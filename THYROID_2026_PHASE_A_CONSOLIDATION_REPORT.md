# THYROID 2026 — Phase A Consolidation Report

- Run timestamp (UTC): `2026-04-19T06:10:56.743365+00:00`
- Git HEAD: `refs/heads/phase-a-consolidate-reruns -> 717d6ee07755`
- Script: `scripts/phase_a_consolidate_reruns.py`
- Script SHA-256: `b5d27d492d7c018a0d2efad65b9b249696cc8cc1a0130ea80e6a8a847b2fdbdf`
- Domains consolidated this run: **3** (pathology, tirads_granular, cervical_ln_detail)
- Provenance label: `entity_domain=<domain>_targeted_rerun_qwen3_32b_ollama`, `llm_provider=ollama_local`, `llm_sdk=ollama` (honest labeling — these shards are NOT qwen2.5-32b vLLM)

## Per-domain consolidation

| Domain | Shards | Raw rows | Output rows | Output RIDs | RIDs w/ entity | Verdict |
|---|---:|---:|---:|---:|---:|---|
| synoptic_pathology_enrichment (reference; pre-consolidated) | n/a | n/a | 26584 | 10862 | 4992 | upgrade |
| pathology | 7 | 95458 | 19810 | 5884 | 2822 | modest_upgrade |
| tirads_granular | 8 | 50430 | 10871 | 5305 | 1948 | upgrade |
| cervical_ln_detail | 4 | 25927 | 10417 | 2937 | 1105 | upgrade |
| imaging | 11037 (canon) | — | — | — | — | marginal (no_upgrade_vs_canonical) |
| past_surgical_hx | 11037 (canon) | — | — | — | — | skipped (empty_shard) |

## Cross-domain audit vs canonical snapshot

| Domain | New rows | Canon rows | Δ rows | New RIDs | Canon RIDs | Δ RIDs | New RIDs w/ ent | Canon RIDs w/ ent | Δ | x-mult | Verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| synoptic_pathology_enrichment | 26584 | 11037 | +15547 | 10862 | 5641 | +5221 | 4992 | 33 | +4959 | 151.27x | upgrade |
| pathology | 19810 | 11037 | +8773 | 5884 | 5641 | +243 | 2822 | 2290 | +532 | 1.23x | modest_upgrade |
| tirads_granular | 10871 | 11037 | -166 | 5305 | 5641 | -336 | 1948 | 86 | +1862 | 22.65x | upgrade |
| cervical_ln_detail | 10417 | 11037 | -620 | 2937 | 5641 | -2704 | 1105 | 167 | +938 | 6.62x | upgrade |
| imaging | — | 11037 | — | — | 5641 | — | — | 2218 | — | — | marginal |
| past_surgical_hx | — | 11037 | — | — | 5641 | — | — | 1942 | — | — | skipped |

## Dropped rows per domain

| Domain | Dup groups | Rows dedupped | Conflict rows | Log file |
|---|---:|---:|---:|---|
| pathology | 19810 | 75648 | 417 | `runs/domain_reruns_qwen3_32b_targeted/pathology/output/pathology_consolidation_dropped_rows.jsonl` |
| tirads_granular | 10871 | 39559 | 176 | `runs/domain_reruns_qwen3_32b_targeted/tirads_granular/output/tirads_granular_consolidation_dropped_rows.jsonl` |
| cervical_ln_detail | 10417 | 15510 | 101 | `runs/domain_reruns_qwen3_32b_targeted/cervical_ln_detail/output/cervical_ln_detail_consolidation_dropped_rows.jsonl` |
| imaging | — | — | — | (skipped) |
| past_surgical_hx | — | — | — | (skipped) |

### SHA-256 duplicate shards (collapsed losslessly by note_row_id dedup)

- `5e67b7101bca…` (2 files): note_entities_llm_tirads_granular.ckpt.thy-tirads-4.jsonl, note_entities_llm_tirads_granular.ckpt.thy-tirads-4c.jsonl

## Model / provenance audit

| Domain | Source models (rows) | extracted_at min | extracted_at max |
|---|---|---|---|
| pathology | qwen3:32b=11058, qwen3:14b=8752 | 2026-03-30T05:47:33.179658+00:00 | 2026-04-14T18:25:32.786310+00:00 |
| tirads_granular | qwen3:14b=6132, qwen3:32b=4739 | 2026-03-31T12:54:46.071411+00:00 | 2026-04-14T17:56:39.544416+00:00 |
| cervical_ln_detail | qwen3:14b=7975, qwen3:32b=2442 | 2026-04-14T06:41:58.535945+00:00 | 2026-04-14T16:58:27.244179+00:00 |
| imaging | (skipped) | — | — |
| past_surgical_hx | (skipped) | — | — |

### Provenance warnings

- pathology: observed models ['qwen3:32b'] are NOT qwen2.5 — shards are the targeted qwen3:32b Ollama rerun (see provenance labeling in Phase 1 outputs).
- tirads_granular: observed models ['qwen3:32b'] are NOT qwen2.5 — shards are the targeted qwen3:32b Ollama rerun (see provenance labeling in Phase 1 outputs).
- cervical_ln_detail: observed models ['qwen3:32b'] are NOT qwen2.5 — shards are the targeted qwen3:32b Ollama rerun (see provenance labeling in Phase 1 outputs).
- imaging: observed models ['qwen3:32b'] are NOT qwen2.5 — shards are the targeted qwen3:32b Ollama rerun (see provenance labeling in Phase 1 outputs).

## Phase B planning notes (synoptic schema gap)

- Synoptic gold-standard parquet (`processed/remaining/9domain_v4/output/note_entities_llm_synoptic_pathology_enrichment.parquet`) has **17 columns** on disk, while the new consolidated parquets emit the canonical **23-column** layout (the extra 6 are the synthesized `entity_domain`, `llm_provider`, `llm_sdk`, `llm_sdk_version`, `provider_returned_model`, `provider_system_fingerprint` provenance cols).
- Phase B must reconcile this: either `read_parquet([...], union_by_name=true)` with explicit NULL fills, or backfill the 6 provenance cols on the synoptic parquet at read time. **Do NOT modify the synoptic parquet in place — Phase B's load script should add the cols on the fly.**
- Suggested provenance for the synoptic backfill: `entity_domain='synoptic_pathology_enrichment_rerun_qwen25_32b'`, `llm_provider='vastai_vllm'`, `llm_sdk='openai-compatible'`, `provider_returned_model=llm_model` (echo).

## Next-step pointers

- Synoptic rerun parquet (Phase B input):  
  `processed/remaining/9domain_v4/output/note_entities_llm_synoptic_pathology_enrichment.parquet`
- `pathology` consolidated parquet:  
  `runs/domain_reruns_qwen3_32b_targeted/pathology/output/note_entities_llm_pathology.parquet`
- `tirads_granular` consolidated parquet:  
  `runs/domain_reruns_qwen3_32b_targeted/tirads_granular/output/note_entities_llm_tirads_granular.parquet`
- `cervical_ln_detail` consolidated parquet:  
  `runs/domain_reruns_qwen3_32b_targeted/cervical_ln_detail/output/note_entities_llm_cervical_ln_detail.parquet`
- `imaging` (no_upgrade_vs_canonical): `runs/domain_reruns_qwen3_32b_targeted/imaging/output/consolidation_summary.json` (stub; no parquet)
- `past_surgical_hx` (empty_shard): no output emitted; investigate why the Vast.ai job produced a 0-byte shard before re-running this domain.

## Final report checklist

- [x] Inventory discovered expected shards for 5 domains
- [x] In-scope domains produced a parquet with canonical 23-column schema
- [x] Empty domain (past_surgical_hx) flagged with status='empty_shard' and no parquet emitted
- [x] Imaging flagged status='no_upgrade_vs_canonical' (stub summary, no parquet)
- [x] All emitted parquets have research_id as VARCHAR
- [x] Dedup audit: see Dropped-rows table above (per-domain counts + log files)
- [x] Rerun upgrade verdict per domain (Cross-domain audit table)
- [ ] Git commit present (atomic per-domain commits + final audit commit; user verifies via `git log` on branch `phase-a-consolidate-reruns`)
- [x] NO MotherDuck writes attempted (grep `md:`/`motherduck` in this script returns only docstrings/comments)
