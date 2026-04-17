# Script 263 - Decision Pending: bethesda_final semantic ambiguity

**Status:** dry-run only. No mutations applied. No snapshot written.
**Run date (UTC):** 20260417T025408Z
**Branch:** `cleanup/v1_1_finalization-20260416`

## What was verified live (post Scripts 261 + 262)

CPM rows=10871; bethesda_final populated=5249; worst_bethesda_num populated=5249.
- bethesda_final = worst_bethesda_num: **5138** patients
- bethesda_final IS DISTINCT FROM worst_bethesda_num: **111** patients

Compared against `MAX(bethesda_category)` from `fna_episode_master_v2` (the
canonical FNA-cytology source, joined on `research_id` after the dtype align):

| Column                | Equal to MAX(FEM) | Not equal | Pct equal (of joined) |
|-----------------------|------------------:|----------:|----------------------:|
| `bethesda_final`      |             4,454 |       796 |                 84.84% |
| `worst_bethesda_num`  |             4,565 |       685 |                 86.95% |

`worst_bethesda_num` matches FNA-only MAX more often than `bethesda_final` does.
This is consistent with the dry-run finding that `bethesda_final` is currently
populated as "worst observed across cytology + path" rather than "final cytology
only".

Sample of 10 discordant rows (bethesda_final vs MAX(FEM.bethesda_category)):

| research_id | bethesda_final | worst_bethesda_num | MAX(FEM.bethesda_category) |
|-------------|---------------:|-------------------:|---------------------------:|
| 1001 | 4 | 4 | 3 |
| 1002 | 6 | 6 | 3 |
| 10020 | 4 | 4 | 3 |
| 10031 | 6 | 6 | 2 |
| 10049 | 6 | 6 | 5 |
| 10051 | 2 | 2 | 3 |
| 10052 | 4 | 4 | 3 |
| 10059 | 4 | 4 | 3 |
| 10066 | 1 | 2 | 2 |
| 10080 | 6 | 6 | 3 |

## Decision required

Reply in chat with one of:

- **Path A (final cytology semantics):**
  - Rename current `bethesda_final` to `bethesda_worst_across_sources_legacy`
    (status='legacy' in `data_dictionary_v240`).
  - Create a new `bethesda_final` column = `MAX(fna_episode_master_v2.bethesda_category)`
    grouped by `CAST(research_id AS VARCHAR)`.
  - Snapshot CPM and any `manuscript_workspace` view referencing `bethesda_final`
    before rewrite. Re-verify view counts post-rewrite.

- **Path B (worst-across-sources semantics, the de facto current behavior):**
  - Keep column name. Update `data_dictionary_v240.description` to make explicit:
    "worst Bethesda category observed across cytology + path".
  - Snapshot then drop `worst_bethesda_num` (now duplicative).
  - Snapshot CPM and any `manuscript_workspace` view referencing
    `worst_bethesda_num` before drop. Re-verify view counts post-rewrite.

Either path:
  - Append a row to `manuscript_workspace.__conventions` with
    `convention_id='bethesda_semantics'` documenting the chosen interpretation.
  - Re-run Script 263 with `--apply` (and `--path A|B`) once the decision is set.

## What this script DID do (no mutation)

- Verified live ground truth above.
- Wrote `scripts/output/263_run.log` with the same numbers.
- Wrote `scripts/output/263_decision_log.json` flagging the decision as pending.
- Wrote this `scripts/output/263_semantic_decision_pending.md`.
- Did **not** snapshot, alter, or rewrite any table/view.
