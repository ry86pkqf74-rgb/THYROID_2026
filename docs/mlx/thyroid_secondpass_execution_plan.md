# Second-pass gaps — execution plan

How to actually run the fixes in `docs/mlx/thyroid_secondpass_gaps.md`. Phased so each phase produces something measurable, and so we can pause/resume between phases.

## Phase 0 — environment prerequisites (one-time, ~2 hours)

Done once. Blocks everything that follows.

```bash
# 0.1 Python env on the M5
cd ~/code/THYROID_2026/tools/thyroid_mlx_extract
make dev   # installs mlx-lm, outlines, pydantic, google-cloud-bigquery, philter-lite, dev tooling

# 0.2 BQ auth (writes to pub_workspace, reads everywhere)
gcloud auth application-default login
export BQ_PROJECT=thyroid-canonical-pub-2026
bq query --use_legacy_sql=false "SELECT 1"   # smoke test

# 0.3 Pull model weights you'll need (first run downloads ~150 GB total)
python3 -c "from mlx_lm import load; load('mlx-community/MedGemma-1.5-27B-IT-4bit')"
python3 -c "from mlx_lm import load; load('mlx-community/Llama-3.3-70B-Instruct-4bit')"
python3 -c "from mlx_lm import load; load('mlx-community/DeepSeek-R1-Distill-Llama-70B-4bit')"
python3 -c "from mlx_lm import load; load('mlx-community/MedGemma-1.5-4B-IT-4bit')"

# 0.4 Verify harness runs
thyroid-mlx list-tasks
thyroid-mlx list-models
pytest tools/thyroid_mlx_extract/tests
```

Disk: ~150 GB for model weights. They go to `~/.cache/huggingface` and are reused across all subsequent runs.

## Phase 1 — harness extensions (code only, ~6 hours of dev)

Several things the current harness doesn't do yet. Build these once, then everything below becomes a single CLI call.

### 1.1 `verify` CLI command — adjudicate existing rows in place

The current `adjudicator.py` only runs *during* extraction. We need a separate mode that:
- pulls existing `note_entities_*` rows from BQ
- looks up the source note text from `clinical_notes_long`
- runs a *second* model over the same source + the extracted entity
- writes back `verification_status`, `verifier_name`, `verifier_version`, `verification_step`, `date_confidence` columns

Add to `tools/thyroid_mlx_extract/src/thyroid_mlx_extract/cli.py`:

```python
@cli.command()
@click.argument("table")            # e.g. note_entities_complications
@click.option("--verifier", default="r1-distill-70b")
@click.option("--limit", type=int, default=None)
@click.option("--entity-types", default=None, help="Comma-separated filter")
def verify(table, verifier, limit, entity_types):
    """Run a second-model verification pass over existing extraction rows."""
    ...
```

Wire up `models/verifier.py` (new file, ~80 lines): pulls rows + source text, calls model with `(entity, evidence_span, source_text)` as input, parses `{ok: bool, corrected_value: ..., date_confidence: float, reasoning: str}` output.

### 1.2 Error-class column on `note_entities_llm_*`

Add to the schema in `bq/push.py`:
```
extraction_error_class STRING  -- one of: ok, json_parse, validation, context_overflow, auth, empty_input, other
```

Update `models/extractor.py` to classify errors into this enum instead of just storing the error string. Backfill the column on existing tables by re-classifying their `error` fields with a regex.

### 1.3 Chunking integration in `models/extractor.py`

`utils/chunk.py` exists but isn't called. Wire it in: if `len(source_text) > max_chars_for_model`, chunk with overlap, extract per chunk, merge results with deduplication. Per-model `max_chars` lookup in `config.MODELS`.

### 1.4 New tasks in `config.py` (add to TASKS dict)

```python
"adjudicate_existing": TaskSpec(
    task_id="adjudicate_existing",
    source_tables=("note_entities_complications", "note_entities_operative_detail", ...),
    ...
    primary_model="r1-distill-70b",
    ...
    notes="Verification pass over existing extractions. No new entities extracted.",
),
"recurrence_site_date": TaskSpec(...),    # for the 1,946 events missing site+date
"tumor_size_normalize": TaskSpec(...),    # 49 unparseable + similar columns
"biochem_recurrence_rules": TaskSpec(...), # not an LLM task — rule-based over Tg labs
"capsular_invasion_event": TaskSpec(...),  # ETE-pattern replicas
"perineural_invasion_event": TaskSpec(...),
"angioinvasion_event": TaskSpec(...),
"extranodal_extension_event": TaskSpec(...),
```

### 1.5 Event-resolved table builder template

Generic SQL template that takes (feature_name, candidate_source_tables, evidence_columns) and produces a `canonical_<feature>_event_resolved_v1` table mirroring `canonical_ete_event_resolved_v1`'s column shape. Saves writing four near-identical migrations.

### 1.6 Biochemical recurrence rule engine

Pure SQL (no LLM). Walks each malignant patient's Tg trajectory:
```
Stimulated Tg >1 ng/mL post-total-thyroidectomy → biochem incomplete (ATA 2015)
Unstimulated Tg >0.2 ng/mL trending up over 2 consecutive measurements → biochem incomplete
Rising anti-Tg → biochem incomplete (surrogate)
```
Output: `canonical_biochemical_recurrence_v1` with `(research_id, recurrence_date, definition_applied, evidence_lab_ids)`.

## Phase 2 — gold sets (human time, 8–20 hours per task)

Required before any corpus run. Same workflow as the existing `gold/README.md`.

Priority order — build these in this sequence so the highest-leverage extractions can start first:

| Task | Gold cases | Why prioritize |
|---|---|---|
| `synoptic_pathology` (existing) | 200 | Unlocks Ki-67/capsule/ETE/ENE/PNI extraction over all 11,688 path rows |
| `molecular` (existing) | 100 | Unlocks 10,862 ThyroSeq/Afirma reports |
| `recurrence_site_date` | 100 | Critical fix — `recurrence_site` is 0% filled |
| `complications` (existing) | 200 | Re-extract + adjudicate 9,359 rows |
| `adjudicate_existing` | 100 | Sample across 7 `note_entities_*` tables for verifier eval |
| `imaging_ct` | 100 | T4a/T4b features for staging-stratified papers |
| `fna` (existing) | 100 | 4,500 path_text records to subtype |

Annotation budget: ~6 minutes per case for templated (molecular, FNA), ~15 minutes per case for synoptic, ~12 minutes for recurrence, ~10 minutes for imaging. Total ~100 hours of analyst time for the priority queue. Front-load this — it's the rate-limiting step for everything else.

If you can hire/borrow another annotator for high-stakes fields (synoptic ETE, cause of death, recurrence site), double-annotate and adjudicate disagreements. Otherwise single-annotator is acceptable for Phase 1 with a 10% spot-check by a second person.

## Phase 3 — the big adjudication pass (M5 time, ~43 hours runtime)

Single highest-leverage execution. Run after Phase 1.1 (`verify` CLI) is built.

```bash
# Run sequentially per table — easier to monitor
for tbl in \
  note_entities_complications \
  note_entities_operative_detail \
  note_entities_staging \
  note_entities_genetics \
  note_entities_procedures \
  note_entities_problem_list \
  note_entities_medications; do
    thyroid-mlx verify $tbl \
      --verifier r1-distill-70b \
      --resume \
      2>&1 | tee runs/verify_${tbl}.log
done
```

Memory: ~38 GB for R1-Distill-70B at 4-bit. Throughput on M5 Max: ~3–5 entities/sec. 76,641 entities → ~28–42 hours. Use `--resume` so a crash mid-run doesn't lose progress.

Output: `note_entities_*` rows updated with `verification_status` ∈ {agreed, disagreed, primary_only, both_failed}, `date_confidence` ∈ [0,1], and a `verifier_*` provenance trail. Disagreement rate expected 5–15% per published 2026 clinical-extraction work — those go to a human queue.

Validation: after each table, run:
```sql
SELECT verification_status, COUNT(*) FROM pub_canonical.note_entities_<table>
GROUP BY verification_status;
```
Expect ≥95% non-NULL. If <90%, something broke; investigate before continuing.

## Phase 4 — re-extract the worst empty LLM tables (M5 time, ~80 hours runtime)

After Phase 1.3 (chunking) and 1.2 (error-class column).

Priority order by empty rate:

```bash
# 4.1 — dynamic_risk_response (97.7% empty)
thyroid-mlx run dynamic_risk_response --model medgemma27b --resume

# 4.2 — recurrence (92.3% empty)
thyroid-mlx run llm_recurrence --model medgemma27b --resume

# 4.3 — us_nodule_dynamics (87% empty)
thyroid-mlx run llm_us_nodule_dynamics --model medgemma4b --resume

# 4.4 — synoptic_pathology_enrichment (81.5% empty)
thyroid-mlx run llm_synoptic_enrich --model llama33-70b --resume
```

Each is ~11,037 source rows × ~15s/row at MedGemma-27B = ~46 hours per task. Llama-3.3-70B on synoptic enrichment is closer to ~30s/row = ~92 hours; run that overnight for a week or use M5 Ultra if available.

Before running corpus: run on the gold subset, confirm Macro F1 ≥0.85 vs the gold annotations. If below, adjust prompt and re-eval. Don't waste 46 hours of compute on a model that's failing on gold.

## Phase 5 — build the event-resolved tables (M5 + BQ, ~6 hours each)

After Phase 1.5 (template builder) and gold sets for synoptic.

```bash
# For each of: capsular_invasion, perineural_invasion, angioinvasion, extranodal_extension
thyroid-mlx run synoptic --model llama33-70b --adjudicate --resume
# Then run the event-resolution SQL template:
python -m thyroid_mlx_extract.sql.build_event_resolved \
  --feature capsular_invasion \
  --sources path_synoptics,note_entities_llm_synoptic_pathology_enrichment,note_entities_llm_pathology \
  --output pub_workspace.canonical_capsular_invasion_event_resolved_v1
```

Output schema mirrors `canonical_ete_event_resolved_v1`: same `pm_disagreement_flag`, `is_unresolved`, `inline_evidence` columns. Lands in `pub_workspace` first; promote to `pub_canonical` after manual signoff via the existing `canonical_table_signoff_registry_v1` workflow.

## Phase 6 — deterministic QC assertions (BQ time, minutes)

No LLM, no M5. Add to `pub_eval.qc_assertions_v1`. Each assertion is a CTE returning offending rows.

```sql
-- Example: t_stage_discordance
INSERT INTO `thyroid-canonical-pub-2026.pub_eval.qc_assertions_v1` (assertion_id, ...)
SELECT
  'qc_t_stage_discordance' AS assertion_id,
  research_id, surg_date AS event_date,
  CONCAT('reported=', reported_t_stage_ajcc8, ' derived=', derived_t_stage_ajcc8) AS detail,
  CURRENT_TIMESTAMP() AS detected_at
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_ete_event_resolved_v1`
WHERE t_stage_discordance_flag = TRUE;
```

Eight assertions to add (listed in `thyroid_secondpass_gaps.md` Tier 3). Total time to write + commit: ~2 hours. Schedule them to run via the existing assertion harness daily.

## Phase 7 — LoRA fine-tune track (only if Phase 4 doesn't clear F1 0.90)

Likely needed for **synoptic_pathology** specifically — zero-shot Llama-3.3-70B may not clear 0.90 on the hardest semantic distinctions (capsule vs ETE vs gross ETE beyond strap).

```bash
# Generate training data from your gold set
python -m thyroid_mlx_extract.lora.prepare \
  --task synoptic \
  --gold gold/synoptic_gold.csv \
  --source runs/synoptic/source.jsonl \
  --out lora/synoptic_train.jsonl

# Train (2 hours on M5 Max)
mlx_lm.lora \
  --train \
  --model mlx-community/Meta-Llama-3-8B-Instruct-4bit \
  --data lora/synoptic_train.jsonl \
  --iters 2000 \
  --lora-layers 8 \
  --batch-size 4 \
  --adapter-path adapters/synoptic_v1

# Add to MODELS registry as "llama-3-8b-lora-synoptic", re-eval
thyroid-mlx eval synoptic --models llama33-70b,llama-3-8b-lora-synoptic
```

Published expectation: 0.976 Macro F1 with 10,677 reports. Your gold set will be smaller (200 cases) but should still hit ~0.92+ which is sufficient.

## Sequencing — what blocks what

```
Phase 0 ──┬─► Phase 1.1 ──► Phase 3 (adjudication corpus)
          │
          ├─► Phase 1.2 + 1.3 ──► Phase 4 (re-extraction)
          │
          ├─► Phase 1.4 + 1.5 ──► Phase 5 (event-resolved tables)
          │                       ▲
          │                       │
          ├─► Phase 2 (gold sets) ┴──► Phase 4 + 5 (need gold for eval)
          │
          ├─► Phase 1.6 ──► biochem recurrence rules
          │
          └─► Phase 6 (deterministic QC) — independent, runs anytime
          
Phase 7 (LoRA) — only if Phase 5 doesn't clear F1 0.90 on synoptic gold
```

Phase 6 has no dependencies — could be done first as a quick win.
Phase 0 + 1 is dev work, ~8 hours total.
Phase 2 is the human bottleneck — 100+ hours of annotation.
Phases 3, 4, 5 are compute — runs unattended.

## Realistic timeline (single-person, evenings/weekends)

| Week | Work | Deliverable |
|---|---|---|
| 1 | Phase 0 + 6 + start Phase 1 | Environment ready, QC assertions live, harness extensions ~50% built |
| 2 | Finish Phase 1, start Phase 2 (synoptic + molecular gold) | All harness features built, 2 gold sets begun |
| 3 | Continue Phase 2, kick off Phase 3 (runs overnight) | Synoptic + molecular gold done, adjudication pass running |
| 4 | Phase 3 finishes + start Phase 4 | All `note_entities_*` adjudicated; re-extraction running |
| 5–6 | Phase 4 finishes + Phase 5 begins | Re-extracted tables, event-resolved tables building |
| 7 | Finish Phase 5, evaluate against gold | All four event-resolved tables in pub_workspace |
| 8 | Promote to pub_canonical via signoff, evaluate need for Phase 7 | Production-ready data; LoRA training if needed |

## What I can scaffold now (so you can start)

Three things would unblock execution immediately:

1. **Build the `verify` CLI command + `verifier.py`** module (Phase 1.1). ~3 hours of code, all in the existing harness pattern. Commit + push.

2. **Add the 8 deterministic QC assertions as SQL files** in `qc_framework_v1/migrations/` ready to run against `pub_eval.qc_assertions_v1`. ~1 hour.

3. **Add the new TaskSpec entries** (Phase 1.4) — `adjudicate_existing`, `recurrence_site_date`, `tumor_size_normalize`, the four event-resolved tasks. ~1 hour. Each is a config-only change; the actual extraction logic reuses the existing extractor module.

Say "yes scaffold those" and I'll do all three in one commit so you can pull and start running. Phase 2 (gold sets) is the only part I can't do for you — that's analyst eyeballs.
