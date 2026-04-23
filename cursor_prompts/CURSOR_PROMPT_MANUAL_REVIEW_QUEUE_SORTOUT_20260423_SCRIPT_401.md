# CURSOR COMPOSER 2.0 PROMPT — Script 401 — Manual-review queue sort-out (narrowed: 1 apply + 4 reason refreshes + 1 delete)

**REVISED 2026-04-23:** Scope narrowed per user decision (Option B). Only rid 4015 is applied in this script. Rid 6275 (PDTC via histology) stays queued pending Script 402 (comprehensive PDTC/DHGTC/Hurthle/variant/grade classification audit) — applying rid 6275 → Stage I now would set a `diagnosis_primary='other_malignant'` precedent before we understand the 47-row PDTC cohort.

**Target DB:** `thyroid_canonical_publication_v1_0` (MotherDuck cloud DuckDB)
**Auth:** `.env.motherduck` → `MOTHERDUCK_TOKEN`
**Mode:** Phase-gated runner (Phase 0 probe → plan-approval → `--apply` → Phase 3 verify → Phase 4 commit+tag+push)
**Verbal-gate formalization:** `--i-approve=<probe_report_sha256>` required for `--apply`
**CPM invariant:** `main.canonical_patient_master` row count = **10,871**
**Repository:** `/Users/loganglosser/THYROID_2026`
**Script path:** `scripts/apply_manual_review_queue_sortout.py` (new)
**Tag-prefix:** `v1_0-manual-review-queue-sortout-`
**Close-out path:** `cursor_prompts/CLOSE_OUT_401.md`
**Run-log path:** `scripts/output/apply_manual_review_queue_sortout_run.log`
**Probe-report path:** `scripts/output/apply_manual_review_queue_sortout_probe.md`
**CPM snapshot:** `archive_pub_v1_0.cpm_pre_manual_review_queue_sortout_<ts>` (1 UPDATE target row)
**Queue snapshot:** `archive_pub_v1_0.queue_pre_manual_review_queue_sortout_<ts>` (8 pre-change queue rows)

---

## Context — AJCC7 + histology probe reframes the queue

A post-399 probe surfaced two findings that change the queue resolution picture:

1. **CPM has AJCC7 staging** — `ajcc7_t_stage`, `ajcc7_n_stage`, `ajcc7_m_stage`, `ajcc7_stage_group`, `ajcc7_stage_calculable_flag`, `ajcc7_missing_components`. Populated for DTC rows where calculable. Shows what each row staged under AJCC 7th edition rules.

2. **Histology columns** disambiguate the 2 `other_malignant` queue rows:
   - rid 6275 = poorly differentiated thyroid carcinoma (PDTC) — **47-row PDTC cohort** scattered across diagnosis_primary={PDTC(1), other_malignant(37), PTC(6), FTC(3)}; applying rid 6275 in isolation before Script 402's classification audit would set a premature precedent. **Deferred.**
   - rid 6768 = angiosarcoma of the thyroid — **not AJCC8-thyroid-stageable**; soft tissue sarcoma framework applies.

Of the 8 queued rows, **only rid 4015 (MTC T2 N1a M0 → Stage III per AJCC 8th Ch 73 MTC rule) is applied in this script.** It has no PTC variant or PDTC classification dependency. AJCC8 MTC rule (T1-T3 N1a M0 → III) is unambiguous.

The 4 other reason refreshes enrich the remaining queue rows with AJCC7 + histology context but make no CPM writes.

---

## Planned writes

### Write A — CPM UPDATE (single row, stage_group only)

```sql
UPDATE main.canonical_patient_master
SET ajcc8_stage_group = 'III'
WHERE research_id='4015' AND diagnosis_primary='MTC'
  AND ajcc8_t_stage='T2' AND ajcc8_n_stage='N1a' AND ajcc8_m_stage='M0'
  AND ajcc8_stage_group IS NULL
RETURNING research_id;
```

### Write B — Queue DELETE (remove applied row)

```sql
DELETE FROM manuscript_workspace.cpm_stage_group_manual_review_v1
WHERE research_id='4015' AND source_script='399';
```

### Writes C-1 through C-4 — Queue reason UPDATEs

```sql
-- C-1: rid 1404 — AJCC7=III confirmed, AJCC8 migration needs T
UPDATE manuscript_workspace.cpm_stage_group_manual_review_v1
SET reason = 'ptc_age_64_ajcc7_stage_iii_calculable_missing_T_ajcc8_migration_requires_T_chart_review'
WHERE research_id='1404' AND source_script='395';

-- C-2: rid 12198 — AJCC7=III confirmed, AJCC8 migration needs T
UPDATE manuscript_workspace.cpm_stage_group_manual_review_v1
SET reason = 'ptc_age_61_ajcc7_stage_iii_calculable_missing_T_ajcc8_migration_requires_T_chart_review'
WHERE research_id='12198' AND source_script='395';

-- C-3: rid 924 — 3/4 sources agree T1a N1b (primary is outlier)
UPDATE manuscript_workspace.cpm_stage_group_manual_review_v1
SET reason = 'mtc_age_33_primary_t3b_n1a_outlier_vs_v2_ajcc7_dominant_all_t1a_n1b_majority_signal_yields_iva_under_ajcc8_mtc_t1_t3_n1b_m0_rule_source_review_needed'
WHERE research_id='924' AND source_script='399';

-- C-4: rid 6768 — angiosarcoma, not thyroid-stageable
UPDATE manuscript_workspace.cpm_stage_group_manual_review_v1
SET reason = 'angiosarcoma_of_thyroid_per_histology_final_not_ajcc8_thyroid_stageable_soft_tissue_sarcoma_framework_applies_path_stage_ii_source_unknown'
WHERE research_id='6768' AND source_script='399';
```

Rows NOT modified: rid 423 (T=NULL blocks derivation, reason is already accurate), rid 9600 (M disagreement flagged correctly already), **rid 6275 (PDTC — deferred to Script 402 classification audit)**.

### Write D — `__readme` provenance row

`script='script_401'`, `script_name='apply_manual_review_queue_sortout.py'`, content including:
- 1 CPM UPDATE (rid 4015 MTC → III via AJCC 8th Ch 73 MTC rule)
- 1 queue DELETE (rid 4015)
- 4 queue reason refreshes (rids 1404, 12198, 924, 6768)
- Explicit note: rid 6275 PDTC intentionally NOT applied — deferred to Script 402
- Probe SHA256 consumed
- CPM snapshot FQN + queue snapshot FQN
- Post-state: queue 8 → 7 rows (1 deleted); malignant NULL stage_group in CPM 8 → 7

**Explicitly NOT doing:**
- No writes to T/N/M columns (stage_group-only CPM writes)
- No new queue INSERTs
- No changes to NIFTP/FTUMP rows (CF-401-1 carries forward)
- No changes to rids 423, 9600, 6275 queue rows
- No variant / grade / histology column modifications (Script 402 scope)

---

## Script requirements (`scripts/apply_manual_review_queue_sortout.py`)

Copy Script 399's skeleton (mixed CPM UPDATE + queue DELETE/UPDATE). Inherit every runner behavior:

- Stable probe hash (no timestamps in hashed region; `---HASH-BOUNDARY---` footer)
- `--i-approve=<sha256>` required (exit 3 mismatch, exit 5 missing)
- `--phase4` NO-OP-safe
- `FORCE_ADD_PATTERNS = [r"scripts/output/.*_run\.log$"]`
- `git push origin HEAD` before tag push
- Close-out write AFTER idempotency check
- **Two snapshots** (CPM + queue) since both tables are modified
- Steady-state post-apply probe pattern (inherit from 399's improvements — default run on applied DB emits post-apply steady state, not pre-apply gates)

### Halt gates (Phase 0; all must PASS)

- **H1 — Queue scope lock:** `SELECT COUNT(*) FROM cpm_stage_group_manual_review_v1` = 8 pre-script. 2 from source='395', 6 from source='399'.
- **H2 — Apply row predicate:** Write A WHERE clause returns exactly 1 row from CPM.
- **H3 — Queue delete target present:** rid 4015 currently in queue with source_script='399'.
- **H4 — Queue reason refresh targets present:** rids 1404, 12198 (source='395'), rids 924, 6768 (source='399') all currently in queue.
- **H5 — CPM invariant:** 10,871.
- **H6 — AJCC8 MTC rule derivation for rid 4015:** T2 N1a M0 → III per AJCC 8th Ch 73 MTC stage grouping (T1-T3 + N1a + M0 → III). Static rule check, FAIL if drift.
- **H7 — Deferred rows preserved:** verify rids 423, 9600, 6275 currently in queue and will NOT be touched (read-only verification; any touch in Write section fails gate).
- **H8 — Archive targets unused:** neither `cpm_pre_manual_review_queue_sortout_` nor `queue_pre_manual_review_queue_sortout_` prefix has existing occurrences.
- **H9 — No T/N/M CPM writes:** static SQL audit: CPM `SET` clauses contain ONLY `ajcc8_stage_group`. FAIL otherwise.
- **H10 — No rid 6275 modifications:** static SQL audit: no SQL statement references research_id='6275' in this script (belt-and-suspenders check on the Script 402 deferral).

### Idempotency

Treat as applied iff all of:
1. CPM snapshot `cpm_pre_manual_review_queue_sortout_*` exists
2. Queue snapshot `queue_pre_manual_review_queue_sortout_*` exists
3. `__readme script='script_401'` row exists
4. rid 4015 has `ajcc8_stage_group='III'` in CPM
5. rid 4015 absent from queue
6. Reasons for rids 1404, 12198, 924, 6768 match the updated strings
7. Queue row count = 7 (was 8)
8. rid 6275 STILL in queue with source_script='399' (sanity check that deferral held)

If applied → NO-OP, Phase 3 verify only, exit 0, **DO NOT** overwrite close-out.

### Phase 3 post-state gates

- P1 — CPM total = 10,871
- P2 — rid 4015 has `ajcc8_stage_group='III'`
- P3 — Malignant allowlist NULL stage_group count = 7 (down from 8)
- P4 — Queue row count = 7 (down from 8)
- P5 — Queue row integrity: rids 1404, 12198, 423, 924, 9600, 6275, 6768 all present with expected reasons (6275 reason UNCHANGED — preserves deferral)
- P6 — `__readme script='script_401'` count = 1
- P7 — CPM snapshot row count = 1
- P8 — Queue snapshot row count = 8 (full pre-change queue)
- P9 — No unintended CPM column mutations: diff CPM snapshot vs current shows only `ajcc8_stage_group` changed for rid 4015; all other columns identical
- P10 — No queue column mutations besides `reason` for 4 refresh rows and DELETE for 4015

---

## Execution plan for Composer

1. Create `scripts/apply_manual_review_queue_sortout.py` from 399's skeleton, adapted for DELETE+UPDATE on queue + single CPM UPDATE.
2. Run Phase 0:
   ```
   python3 scripts/apply_manual_review_queue_sortout.py --phase 0
   ```
3. Pause at plan-approval gate. Post H1–H10 verdicts + writes summary.
4. On approval:
   ```
   python3 scripts/apply_manual_review_queue_sortout.py --apply \
     --i-approve=<sha256> --phase4
   ```
5. Phase 4 surgical git-add (5 paths):
   - `scripts/apply_manual_review_queue_sortout.py`
   - `scripts/output/apply_manual_review_queue_sortout_probe.md`
   - `scripts/output/apply_manual_review_queue_sortout_run.log` (`-f` via `FORCE_ADD_PATTERNS`)
   - `cursor_prompts/CURSOR_PROMPT_MANUAL_REVIEW_QUEUE_SORTOUT_20260423_SCRIPT_401.md`
   - `cursor_prompts/CLOSE_OUT_401.md`
6. Commit message: `Script 401: manual-review queue sort-out (narrowed; 4015→III apply, 1 delete, 4 reason refreshes; 6275 deferred to Script 402)`
7. Tag: `v1_0-manual-review-queue-sortout-<YYYYMMDD_HHMMSS>`
8. Push HEAD + tag.
9. Close-out write AFTER idempotency check clears.

---

## Close-out contents (`cursor_prompts/CLOSE_OUT_401.md`)

- Commit SHA, tag, UTC timestamp
- Probe SHA256 (consumed)
- Halt-gate verdict table (H1–H10)
- Applied: rid 4015 MTC T2 N1a M0 → III (AJCC 8th Ch 73 MTC rule)
- Queue DELETE: rid 4015
- Queue reason refreshes: 4 (1404, 12198, 924, 6768) with AJCC7/histology context
- Queue unchanged: 3 (423, 9600, 6275) — note 6275 intentional deferral
- Post-state: queue 8 → 7; malignant NULL stage_group in CPM 8 → 7
- Per-diagnosis breakdown of the 7 remaining queue rows
- CF-401 followups:
  - **CF-401-1** (carried from Script 400 draft): NIFTP/FTUMP builder bug — 3 non-cancer rows with erroneous `_corrected='I'`. Data correction candidate.
  - **CF-401-2:** rid 924 primary column (T3b N1a) appears to be outlier vs v2/AJCC7/dominant (T1a N1b). Potential builder T/N source-precedence bug. Probe whether systematic.
  - **CF-401-3:** rid 9600 M disagreement (AJCC7=M0, AJCC8=M1, path='IVB') — source-level adjudication needed.
  - **CF-401-4:** rid 6768 angiosarcoma — separate tracking for non-thyroid thyroid-bed malignancies. CPM may have other such rows.
  - **CF-401-5:** rid 6275 PDTC deferred — resolution depends on Script 402 classification audit outcome.
  - **CF-401-6:** PDTC cohort consolidation (47 rows across 4 dx_primary codes) — Script 402 scope.
  - **CF-401-7:** PTC variant audit — `diagnosis_variant` clean single-value column (follicular 778, tall_cell 171, oncocytic_warthin 144, etc.) + messy `histologic_variants_all` (case inconsistency, pipe-order inconsistency, embedded newlines). Script 402 scope.

---

## Verbal gate — confirm before Composer begins

Reply with:
- **"Approved. Run Phase 0, return SHA256 for `--i-approve`."** — standard path
- **"Hold — narrow further:"** (e.g., "skip reason refreshes too; apply 4015 only")
- **"Hold — expand:"** (e.g., "also apply 6275 despite Script 402 deferral")
- **"Hold — change wording:"** if you want any reason string edited before apply

The narrowed scope makes Script 401 a clean, conservative write (1 CPM row modified, 5 queue rows modified, 3 queue rows untouched). Rid 6275 stays where it is until Script 402 establishes PDTC handling strategy.
