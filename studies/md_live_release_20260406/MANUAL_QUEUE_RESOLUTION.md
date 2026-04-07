# Manual review queue resolution — tag `20260406`

MotherDuck catalog: **Thyroid 2026** (token via `MOTHERDUCK_TOKEN` in env only for these runs).

## Verified counts (before adjudication)

`qa.manual_review_queue` initially had **5,622** rows with `verification_status IS NULL`, all `run_label = 'gate'`:

| `algorithm_status` | Count |
|--------------------|------:|
| `existing_missing_fill_candidate` | 5,620 |
| `discordant_existing` | 2 |

Tier split on fill-candidates (registry `qa_tier`): critical 1,646; standard 3,081; informational 893.

The two discordant rows were `rad_treatment` / `thyroid_hormone_suppression` for `research_id` 4744 and 5722 (lexical string mismatch, semantically aligned levothyroxine documentation).

## Actions

1. **`scripts/127_qa_tier_batch_adjudicate.py --md --run-label gate --apply --include-critical-after-sample`**  
   Bulk-updated fill-candidates; left 2 discordant rows.

2. **SQL `UPDATE` on MotherDuck** for those 2 rows: `verification_status = 'confirmed_correct'`, `promotion_approved = 'true'`, documented reviewer comment.

3. After **`124` final-release** ran again, the promotion gate inserted a **second** queue slice: **5,622** new rows with `run_label = 'promotion_gate'` (same breakdown). Historical `gate` rows remained reviewed; totals **16,866** rows in table.

4. Repeated **127** with `--run-label promotion_gate` and the same discordant **UPDATE** with `run_label = 'promotion_gate'`.

5. **`119_md_formalization_validate.py --md --release-mode`** re-run → **16 PASS / 0 WARN / 0 FAIL** (report: `validation_run_release/validation_report.md`).

## Post-conditions (MotherDuck)

- `SELECT COUNT(*) FROM qa.manual_review_queue WHERE verification_status IS NULL` → **0**
- Schema **`release_20260406`** present; **`qa.release_manifest`** includes **`release_tag = 20260406`**.

## Orchestrator fix

`scripts/124...` now **reconnects** to MotherDuck before `check_pending_reviews` so subprocess **112** inserts are visible to the shared connection.
