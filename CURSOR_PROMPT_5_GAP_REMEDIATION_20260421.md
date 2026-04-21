# Cursor Prompt 5 — Gap Remediation: close the four Prompt 3 gaps that didn't fully land

**Date:** 2026-04-21
**Author:** handoff from Cowork (Logan + Claude) to Opus 4.7 in Cursor
**Runs after:** Prompts 1–4 (Scripts 288–340) all complete.
**Runs in parallel with:** RunPod extraction chat (H200 jobs for 3 stale domains + TIRADS requeue + dedicated esophageal — see `RUNPOD_EXTRACTION_PROMPT_V2_20260421.md`).
**Purpose:** Four Prompt-3 scripts (327, 330, 331, 334) reported success but the canonical CPM/detail columns show the actual change was near-zero. This prompt is a tight re-run/repair with explicit pre/post deltas so we can prove what moved.

## Verified state of Prompt 3 outputs on MotherDuck (2026-04-21)

| Target | Prompt 3 claim | Actual CPM state |
|---|---|---|
| Re-op detail rebuild (Script 327) | 735 patients get multi-episode rows | **Only 3 patients have >1 row** in `operative_episode_detail_v2` despite 738 having `n_surgeries_v2 > 1` |
| VC tier (Script 330) | Populate `comp_vc_*_evidence_tier` | 34 paralysis + 22 paresis tiered. Denominator: 19 paralysis confirmed/suspected + 13 paresis confirmed/suspected = 32 total. **Tier coverage is fine, but the column is undefined for the 10,837 patients with no VC mention at all.** |
| Calcium LLM recovery (Script 331) | +300 RIDs from `note_entities_llm_labs` | `lab_calcium_first_date` = 165 nonnull; `note_entities_llm_labs` has 279 RIDs with `'calcium'` substring in JSON. **~114 RIDs unrecovered.** |
| `op_esophageal_inv_any` (Script 334) | 381 RIDs from airway_invasion JSON | CPM column = **0 nonnull**. A sibling column `op_nlp_esophageal_involvement` = 4,028 nonnull but only 2 TRUE (4,026 FALSE, 6,843 NULL). `operative_episode_detail_v2.esophageal_involvement_flag` = all NULL. Signal is near-zero — the 381 claim was overstated. |

Canonical root causes:
- **Script 327 hybrid relied on `entity_type='operative_date'` that doesn't exist** (confirmed: `note_entities_operative_detail` has no rows of that entity_type; the entity types present are nerve_monitoring, ebl, rln_finding, parathyroid_management, etc.). The fallback to note_date clustering either didn't execute or didn't write multi-episode rows.
- **Script 334 wrote to the wrong column.** The CPM column it targeted (`op_esophageal_inv_any`) was not UPDATEd; only `op_nlp_esophageal_involvement` moved. And the source signal is essentially two patients, not 381.
- **Script 331 parser missed LLM calcium rows.** 279 JSON-mentioned RIDs vs. 165 dated rows on CPM — a straight recovery script should close ~40–60% of that gap.
- **Script 330 is actually fine** — the low tier counts reflect the small VC population, not a script failure. We just need to confirm by asserting the tier count >= confirmed+suspected count.

## Operating constraints

1. **PHI safety**: `research_id` only in stdout; never print evidence_text or source_text.
2. **Pre/post snapshots mandatory**: every script must SELECT-and-log the pre-state of its target column(s) to `manuscript_workspace.prompt5_remediation_log_v1` BEFORE any UPDATE/INSERT, and re-select post-state afterward. Log the delta. If delta is zero, the script FAILS loud instead of silently "succeeding."
3. **Archive before write**: if the script overwrites data (not just fills NULLs), archive to `archive_pub_v1_0.<name>_preSCRIPT<N>_<UTCZ>` per the established pattern.
4. **Env**: `scripts/_md_connect.py::connect_locked()`.
5. **CPM invariants**: rows=10,871, distinct_rid=10,871 preserved throughout.

---

## Script 341 — Rebuild `operative_episode_detail_v2` with real multi-episode rows

**Why:** The existing table has one row per patient for 9,365 of 9,368 RIDs. 738 of those patients have `n_surgeries_v2 > 1` in CPM. We need one row per surgery episode, not one per patient.

**Source data:**
- `main.clinical_notes_long WHERE note_type='OPNOTE'` → 4,727 notes across the cohort (verified 2026-04-21).
- `main.canonical_patient_master.n_surgeries_v2` → expected episode count per patient (authoritative per Script 287).
- `main.note_entities_operative_detail` → no `operative_date` entity exists, but `nerve_monitoring` (5,556), `ebl` (1,683), `rln_finding` (1,307), etc. carry `note_row_id` + `note_date` that link back to op notes.
- Existing `operative_episode_detail_v2` has `surgery_date_native` (TIMESTAMP) + `surgery_episode_id` (BIGINT) + `note_date_resolved` (TIMESTAMP) already — the schema supports multi-episode; it just isn't populated.

**Procedure:**

1. **Pre-state snapshot** — log rowcount, distinct research_id count, and histogram of rows-per-RID to `prompt5_remediation_log_v1` with `script_n=341, phase='pre'`.

2. **Archive** current `operative_episode_detail_v2` to `archive_pub_v1_0.operative_episode_detail_v2_preSCRIPT341_<UTCZ>`. Log to `archive_move_log_v1`.

3. **Cluster op-note dates per patient:**
   ```sql
   WITH op_note_dates AS (
     SELECT DISTINCT cnl.research_id, cnl.note_row_id, cnl.note_date
       FROM main.clinical_notes_long cnl
      WHERE cnl.note_type = 'OPNOTE'
        AND cnl.note_date IS NOT NULL
   ),
   clustered AS (
     -- Cluster within patient: two notes within 7 days are the same episode.
     SELECT research_id,
            note_row_id,
            note_date,
            -- episode id = running count of notes where gap >= 8d from prior note
            SUM(CASE WHEN note_date - LAG(note_date) OVER (PARTITION BY research_id ORDER BY note_date) > INTERVAL 7 DAY
                     OR LAG(note_date) OVER (PARTITION BY research_id ORDER BY note_date) IS NULL
                     THEN 1 ELSE 0 END)
              OVER (PARTITION BY research_id ORDER BY note_date) AS episode_rank
       FROM op_note_dates
   ),
   episodes AS (
     SELECT research_id, episode_rank,
            MIN(note_date) AS surgery_date_native,
            MIN(note_row_id) AS anchor_note_row_id,
            LIST(DISTINCT note_row_id) AS episode_note_row_ids
       FROM clustered
      GROUP BY 1, 2
   )
   SELECT * FROM episodes;
   ```
   This gives you one row per (research_id, episode_rank) with a surgery date and the list of op-note row_ids that belong to it.

4. **Attach detail flags** by joining each episode to the op-note `note_row_id`s and pulling from `note_entities_operative_detail` (match on `note_row_id IN episode_note_row_ids`). Aggregate within an episode: `BOOL_OR` for flags, `MAX` for counts, take the earliest `note_date` as the surgery date.

5. **Assertion** — must be `COUNT(*) >= 700` patients with episode_rank > 1. If fewer than 700, the script raises and backs out. Log the count to `prompt5_remediation_log_v1`.

6. **Drop + rebuild** `main.operative_episode_detail_v2` from the new episodes CTE. Preserve all original columns; fill flags as best available, NULL for domains with no evidence in that episode. Keep `surgery_episode_id` = globally unique BIGINT.

7. **Post-state snapshot** — log new rowcount, distinct RID count, multi-episode RID count. Delta against pre-state goes into `prompt5_remediation_log_v1`.

8. **CPM sync** — re-backfill CPM columns that aggregate from this table: `n_surgeries_from_opdetail_v2`, `op_first_surgery_date`, `op_last_surgery_date`, `op_episode_count`. Use Script 292's UPDATE pattern.

**Invariants after 341:**
- `SELECT COUNT(DISTINCT research_id) FROM operative_episode_detail_v2 WHERE episode_rank > 1` ≥ 700 OR the script failed loudly.
- `SELECT COUNT(*) FROM operative_episode_detail_v2` ≥ 10,500 (was 9,368; new multi-episode rows bump it).
- CPM rows unchanged (10,871).

**Script:** `scripts/341_rebuild_operative_episode_multi_v2.py`.

---

## Script 342 — Populate `op_esophageal_inv_any` correctly + sync detail flag

**Why:** CPM's canonical flag `op_esophageal_inv_any` is the column downstream manuscript queries will read. It's 0 nonnull. The sibling `op_nlp_esophageal_involvement` has 4,028 nonnull (2 TRUE, 4,026 FALSE) — that's the actual signal we harvested. `operative_episode_detail_v2.esophageal_involvement_flag` is also all NULL.

**Procedure:**

1. **Pre-state** — log nonnull counts + TRUE counts for `op_esophageal_inv_any`, `op_nlp_esophageal_involvement`, `esophageal_involvement_flag` (in operative_episode_detail_v2).

2. **Harvest from two sources:**
   - `main.note_entities_operative_detail WHERE entity_type='esophageal_involvement'` — 2 rows (verified).
   - `main.note_entities_llm_airway_invasion` — scan `result_json` for `'esophag'` substring with `present_or_negated='present'`. This is what Script 334 was *supposed* to do.
   ```sql
   WITH airway_esoph AS (
     SELECT DISTINCT research_id, TRUE AS from_airway
       FROM main.note_entities_llm_airway_invasion,
            UNNEST(CAST(json_extract(result_json, '$.entities') AS VARCHAR[])) AS e(entity_json)
      WHERE LOWER(json_extract_string(entity_json, 'evidence_text')) LIKE '%esophag%'
        AND COALESCE(json_extract_string(entity_json, 'present_or_negated'), '') <> 'negated'
   ),
   op_esoph AS (
     SELECT DISTINCT research_id, TRUE AS from_op
       FROM main.note_entities_operative_detail
      WHERE entity_type='esophageal_involvement'
   )
   SELECT COALESCE(a.research_id, o.research_id) AS research_id,
          COALESCE(a.from_airway, FALSE) AS from_airway,
          COALESCE(o.from_op, FALSE) AS from_op
     FROM airway_esoph a FULL OUTER JOIN op_esoph o USING (research_id);
   ```
   Log the RID count from each source and the union.

3. **UPDATE** CPM:
   ```sql
   UPDATE main.canonical_patient_master cpm
      SET op_esophageal_inv_any = TRUE
    WHERE research_id IN (<union set>);
   UPDATE main.canonical_patient_master
      SET op_esophageal_inv_any = FALSE
    WHERE op_esophageal_inv_any IS NULL
      AND research_id IN (SELECT research_id FROM main.operative_episode_detail_v2);
   -- NULL remains for patients who never had an op note at all.
   ```

4. **Sync `operative_episode_detail_v2.esophageal_involvement_flag`** — same logic applied per-episode where the source note_row_id is available.

5. **Add Constraint-7 companions** on CPM if missing: `op_esophageal_inv_first_date`, `op_esophageal_inv_first_source_note_ref`, `op_esophageal_inv_first_evidence_text`, `op_esophageal_inv_n_notes_documenting`. Populate from the airway_invasion JSON source note metadata.

6. **Post-state** — log nonnull + TRUE counts. Delta row added to `prompt5_remediation_log_v1`.

7. **Assert** `COUNT(op_esophageal_inv_any) WHERE op_esophageal_inv_any IS NOT NULL` > 4,000 (matching the existing `op_nlp_esophageal_involvement` population, since they should align). TRUE count expected to be small (2–20 patients). Note: real coverage requires **RunPod Job 3** dedicated extraction; this script just makes the existing signal readable on the canonical column.

**Script:** `scripts/342_backfill_op_esophageal_inv_any.py`.

---

## Script 343 — VC tier diagnostic + completeness confirmation (lightweight)

**Why:** VC tier counts (34 + 22 = 56) look sparse but the denominators are tiny (19 confirmed/suspected paralysis + 13 confirmed/suspected paresis = 32 actual VC patients). Need to verify the tier column reflects all evidenced patients, not leave this as a lurking concern.

**Procedure:**

1. **Pre-state** — log distributions of tier column + confirmed/suspected booleans.

2. **Cross-tabulate** tier vs. confirmed/suspected in `prompt5_remediation_log_v1`:
   ```sql
   SELECT
     CASE WHEN comp_vc_paralysis_confirmed THEN 'confirmed'
          WHEN comp_vc_paralysis_suspected THEN 'suspected'
          ELSE 'neither' END AS status,
     comp_vc_paralysis_evidence_tier AS tier,
     COUNT(*)
     FROM main.canonical_patient_master
    GROUP BY 1, 2 ORDER BY 1, 2;
   ```

3. **Repair any mismatch**: any RID where `comp_vc_paralysis_confirmed = TRUE` but `comp_vc_paralysis_evidence_tier IS NULL` gets tier=1 (or whatever Script 295 used for confirmed). Same for paresis. Report count fixed.

4. **Assert final state**: for every RID with confirmed=TRUE, tier IS NOT NULL. For every RID with suspected=TRUE but confirmed=FALSE, tier >= 2. Log any violations.

5. Decision gate: if no mismatches were found and tier population matches confirmed/suspected population, note in audit that "Script 330 VC tiering confirmed accurate — sparse counts reflect small VC cohort, not a defect" and move on.

**Script:** `scripts/343_vc_tier_diagnostic.py`.

---

## Script 344 — Calcium LLM labs recovery (close the 279→165 gap)

**Why:** `note_entities_llm_labs` has 279 RIDs with `'calcium'` substring in `result_json`. CPM `lab_calcium_first_date` is populated for only 165. ~114 RIDs unrecovered.

**Procedure:**

1. **Pre-state** — log nonnull count + date range for `lab_calcium_first_date`, `lab_calcium_last_date`, `lab_calcium_most_recent`.

2. **Parse calcium entities from LLM labs JSON:**
   ```sql
   WITH parsed AS (
     SELECT research_id, note_index, extracted_at,
            json_extract_string(entity_json, 'entity_type') AS etype,
            json_extract_string(entity_json, 'entity_value') AS evalue,
            json_extract_string(entity_json, 'entity_date') AS edate,
            json_extract_string(entity_json, 'evidence_text') AS etext,
            json_extract_string(entity_json, 'present_or_negated') AS neg
       FROM main.note_entities_llm_labs,
            UNNEST(CAST(json_extract(result_json, '$.entities') AS VARCHAR[])) AS e(entity_json)
      WHERE LOWER(CAST(entity_json AS VARCHAR)) LIKE '%calcium%'
   ),
   calcium_dated AS (
     SELECT research_id,
            MIN(TRY_CAST(edate AS DATE)) AS calcium_first_date_llm,
            MAX(TRY_CAST(edate AS DATE)) AS calcium_last_date_llm,
            COUNT(*) AS n_llm_calcium_mentions
       FROM parsed
      WHERE COALESCE(neg, '') <> 'negated'
        AND etype IS NOT NULL
      GROUP BY 1
   )
   SELECT * FROM calcium_dated;
   ```

3. **UPDATE CPM** for RIDs where `lab_calcium_first_date IS NULL` and LLM has a dated calcium row:
   ```sql
   UPDATE main.canonical_patient_master cpm
      SET lab_calcium_first_date = COALESCE(cpm.lab_calcium_first_date, c.calcium_first_date_llm),
          lab_calcium_last_date  = COALESCE(cpm.lab_calcium_last_date,  c.calcium_last_date_llm),
          lab_calcium_source = COALESCE(cpm.lab_calcium_source, 'llm_notes')
     FROM calcium_dated c
    WHERE cpm.research_id = c.research_id;
   ```
   Do NOT overwrite existing Excel-sourced calcium values; only fill NULLs.

4. **Add Constraint-7 companions** if missing: `lab_calcium_first_source = 'llm_notes'` vs `'excel_labs'`, `lab_calcium_llm_n_mentions`. Populate.

5. **Post-state** — log nonnull count delta. Expected ≥ +80, ≤ +120 (the 279 JSON RIDs minus the 165 already populated, minus overlap). Assert delta > 50 or fail loud.

**Script:** `scripts/344_calcium_llm_recovery.py`.

---

## Script 345 — Final prompt-5 audit + `__readme` refresh

1. **Summary table** `manuscript_workspace.prompt5_remediation_summary_v1` with one row per script (341–344) showing pre-state, post-state, delta, and pass/fail status.

2. **Re-run CPM invariants** and assert:
   - rows = 10,871, distinct_rid = 10,871
   - `n_surgeries_v2 > 1` AND episode_rank > 1 in operative_episode_detail_v2: at least 700 patients reconciled
   - `op_esophageal_inv_any` IS NOT NULL for ≥ 4,000 patients (matches op_nlp_esophageal_involvement nonnull)
   - VC tier matches confirmed/suspected (zero violations)
   - `lab_calcium_first_date` nonnull count ≥ 230 (was 165; expect +80 minimum)

3. **Refresh `main.__readme`** with "Post-Prompt-5 remediation" note:
   ```
   Prompt 5 (2026-04-21): closed four Prompt-3 gaps.
   - operative_episode_detail_v2 now holds <N> multi-episode rows (was 3 of 738).
   - op_esophageal_inv_any populated from airway LLM JSON + operative entities (canonical read-column, not op_nlp_* sibling).
   - VC tiering confirmed accurate against small VC cohort.
   - Calcium LLM recovery added <N> new dated patients (now <N> total).
   Dedicated esophageal extraction + 3 stale-domain re-runs + TIRADS requeue still pending RunPod handoff.
   ```

4. Write `scripts/output/345_prompt5_audit.md` with pre/post tables, deltas, any violations, any leftover gaps.

**Script:** `scripts/345_prompt5_audit.py`.

---

## Git discipline

Per script:
```bash
cd "/Users/ros/THyroid 2026"
git add scripts/<N>_*.py
python -m pyflakes scripts/<N>_*.py
git commit -m "Script <N>: <summary>"
git push origin main
```

## Definition of done

1. `operative_episode_detail_v2` has ≥ 700 patients with multi-episode rows (currently 3).
2. `op_esophageal_inv_any` populated on ≥ 4,000 CPM rows with TRUE count logged (likely 2–20).
3. VC tiering has zero `confirmed=TRUE AND tier IS NULL` violations.
4. `lab_calcium_first_date` nonnull count > 230 (+65 minimum from baseline 165).
5. `prompt5_remediation_log_v1` has pre + post rows for every script with nonzero deltas.
6. `prompt5_remediation_summary_v1` exists and every row shows `status='pass'`.
7. `main.__readme` refreshed.
8. `scripts/output/345_prompt5_audit.md` committed.
9. CPM invariants unchanged (10,871 rows / 10,871 distinct research_id).

## What this prompt does NOT fix (deferred to RunPod)

- Real esophageal invasion coverage beyond the ~2 existing TRUEs requires a dedicated extraction on 4,727 op-notes (RunPod Job 3).
- 3 stale LLM domains (pathology, cervical_ln_detail, tirads_granular) remain at qwen3:32b with 5,641-RID coverage; full 10,871-RID re-extraction at qwen2.5-32b is RunPod Job 1.
- TIRADS nodule `calcifications` field for 4,363 queued nodules is RunPod Job 2.

Those three jobs are covered in `RUNPOD_EXTRACTION_PROMPT_V2_20260421.md` — a separate Cowork chat, not a Cursor job.
