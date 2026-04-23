# CURSOR COMPOSER 2.0 PROMPT — Script 396 — DTC NULL-N stage_group fill (4-row cohort)

**Target DB:** `thyroid_canonical_publication_v1_0` (MotherDuck cloud DuckDB)
**Auth:** `.env.motherduck` → `MOTHERDUCK_TOKEN`
**Mode:** Phase-gated runner (Phase 0 probe → plan-approval → `--apply` → Phase 3 verify → Phase 4 commit+tag)
**Verbal-gate formalization:** `--i-approve=<probe_report_sha256>` flag required for `--apply`
**CPM invariant:** `main.canonical_patient_master` row count = **10,871** (must hold across all phases)
**Repository:** `/Users/loganglosser/THYROID_2026` (git root)
**Script path:** `scripts/apply_dtc_null_n_stage_group_fill.py` (new)
**Tag-prefix:** `v1_0-dtc-null-n-stage-groups-filled-`
**Close-out path:** `cursor_prompts/CLOSE_OUT_396.md`
**Run-log path:** `scripts/output/apply_dtc_null_n_stage_group_fill_run.log`

---

## Context — why this script exists

Scripts 393, 394, and 395 closed the DTC stage_group orphan cohort for rows with BOTH `ajcc8_n_stage` and `ajcc8_m_stage` non-NULL (T3b fill, NULL-T M-decidable fill, and T-sync fill respectively). A completeness probe after 395 surfaced **6 remaining NULL DTC stage_group rows** — 2 of which were routed to `manuscript_workspace.cpm_stage_group_manual_review_v1` (rids 1404, 12198 — CF-395-1 awaiting AJCC-edition adjudication) and **4 new rows** (rids 2480, 2837, 4245, 6772) whose predicate-blindspot was `ajcc8_n_stage IS NULL` (paired with `ajcc8_n_stage_v2 = 'Nx'`). Scripts 393–395 all required N-stage non-NULL and therefore excluded these 4.

Live MotherDuck probe (2026-04-22) confirms per-row signals:

| research_id | age | T (prim/v2/dom) | N (prim/v2) | M (prim/v2/dom) | _corrected | path_raw | projected |
|-------------|-----|-----------------|-------------|-----------------|------------|----------|-----------|
| 2480        | 63  | NULL / T1 / NULL       | NULL / Nx | M0 / M0 / M0  | NULL       | NULL     | **I**  |
| 2837        | 15  | NULL / T1a / T1a       | NULL / Nx | M0 / M0 / M0  | I          | I        | **I**  |
| 4245        | 69  | **T3b / T3a** / NULL   | NULL / Nx | M0 / M0 / M0  | II         | NULL     | **II** |
| 6772        | 49  | NULL / T3a / T1a       | NULL / Nx | **M1 / M0 / M1** | II       | II       | **II** |

Two rows carry cross-column disagreements — **4245 (T3a vs T3b, both yield II)** and **6772 (M1 vs M0, M1 corroborated 2/3 + path_raw)** — that do NOT change the AJCC8 stage_group outcome but must be logged.

**CPM convention check** — 59 precedent rows match `T1/T1a/T1b/T2 + N=NULL + M0 + age≥55 + DTC` all staged **I** in the current CPM, establishing the `Nx→N0` convention for rid 2480's derivation.

**Projected post-state:** DTC completeness 3736/3742 (99.8397%) → 3740/3742 (99.9466%). Remaining 2 NULL stay queued in `cpm_stage_group_manual_review_v1` for AJCC-edition adjudication.

---

## Planned writes (4 UPDATEs on `main.canonical_patient_master`, stage_group column only — no T/N/M edits)

| research_id | SET ajcc8_stage_group | WHERE predicate (tight, idempotent)                                                                                   | Rationale code                     |
|-------------|-----------------------|-----------------------------------------------------------------------------------------------------------------------|------------------------------------|
| 2480        | `'I'`                 | `research_id='2480' AND diagnosis_primary='PTC' AND age_at_surgery=63 AND ajcc8_m_stage='M0' AND ajcc8_stage_group IS NULL` | `derive_t_v2_nx_convention`        |
| 2837        | `'I'`                 | `research_id='2837' AND diagnosis_primary='PTC' AND age_at_surgery=15 AND ajcc8_m_stage='M0' AND ajcc8_stage_group IS NULL AND ajcc8_stage_group_corrected='I'` | `builder_sync_age_lt_55_m0`        |
| 4245        | `'II'`                | `research_id='4245' AND diagnosis_primary='PTC' AND age_at_surgery=69 AND ajcc8_m_stage='M0' AND ajcc8_stage_group IS NULL AND ajcc8_stage_group_corrected='II'` | `builder_sync_t3a_t3b_both_yield_ii` |
| 6772        | `'II'`                | `research_id='6772' AND diagnosis_primary='PTC' AND age_at_surgery=49 AND ajcc8_m_stage='M1' AND ajcc8_stage_group IS NULL AND ajcc8_stage_group_corrected='II'` | `m1_primary_path_corroborated`     |

**Deliberately NOT doing:**
- No T sync (don't write `ajcc8_t_stage` even though 2480/2837/6772 have NULL primary T — Script 395's scope was T-sync; this script is surgical stage_group-only).
- No M reconciliation for rid 6772 (M1 primary stands; M0 v2 logged as disagreement).
- No T reconciliation for rid 4245 (T3b primary stands; T3a v2 logged as disagreement).
- No write to rids 1404, 12198 (those remain queued for manual review).

**Expected effect counts:**
- `cpm_total` unchanged (10,871)
- `dtc_null_stage_before = 6` → `dtc_null_stage_after = 2`
- 4 rows updated total (1×Stage I for age-stratified M0, 1×Stage I for builder-sync <55, 1×Stage II builder-sync age≥55 T3b, 1×Stage II M1)

---

## Script requirements (`scripts/apply_dtc_null_n_stage_group_fill.py`)

Use `_runner_base.py` if available; otherwise match Script 395's runner shape exactly with these rules.

### Runner shape (inherited from Script 395 improvements)

1. **Phase 0 (probe, default)** — print per-row current state, derivation rationale, halt-gate verdicts; emit `probe.md` to `scripts/output/apply_dtc_null_n_stage_group_fill_probe.md` AND its SHA256. Probe markdown body **must not contain any timestamp or wallclock field** inside the hashed region (hash stability across re-runs). A generation-time footer can appear BELOW the `---HASH-BOUNDARY---` sentinel and is excluded from the hash.
2. **Plan-approval gate:** `--apply` requires both:
   - `--i-approve=<sha256>` — must match the current probe markdown's hash to 64 hex chars. Reject with exit 3 if mismatch; rehash and print the expected value.
   - All halt gates passing in Phase 0 (see below). Cached verdict is acceptable if probe was just run.
3. **Phase 2 (apply)** — in a single transaction:
   - **2A Snapshot:** `CREATE TABLE archive_pub_v1_0.cpm_pre_dtc_null_n_stage_group_fill_<ts> AS SELECT * FROM main.canonical_patient_master WHERE research_id IN ('2480','2837','4245','6772');`
   - **2B UPDATEs** (one per row, each with `... RETURNING research_id` so DuckDB returns affected count without relying on `changes()`):
     ```sql
     UPDATE main.canonical_patient_master
     SET ajcc8_stage_group = 'I'
     WHERE research_id='2480' AND diagnosis_primary='PTC' AND age_at_surgery=63
       AND ajcc8_m_stage='M0' AND ajcc8_stage_group IS NULL
     RETURNING research_id;
     ```
     (analogous for 2837, 4245, 6772 per the table above)
   - **2C `__readme` provenance row:** single row with `script_name='apply_dtc_null_n_stage_group_fill.py'`, `script_tag='script_396'`, `run_timestamp=CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`, `summary` including:
     - rows updated (expect 4: 2×'I', 2×'II')
     - per-row rationale codes
     - disagreement log: `rid_4245_t_disagreement:T3b_primary_vs_T3a_v2_both_yield_II`, `rid_6772_m_disagreement:M1_primary_vs_M0_v2_path_stage_II_and_dominant_M1_corroborate_M1`
     - snapshot table FQN
     - probe SHA256 consumed via `--i-approve`
   - Each of 2A/2B/2C is idempotency-guarded: check for existing snapshot table name prefix `cpm_pre_dtc_null_n_stage_group_fill_` AND existing `__readme` row with `script_tag='script_396'` — if both present, report NO-OP and exit 0 WITHOUT overwriting close-out.
4. **Phase 3 (post-apply verify)** — run each halt gate again in read-only mode; fail loudly (exit 2) if any regresses.
5. **Phase 4 (commit + tag)**:
   - Surgical git-add (explicit paths only, never `-A`, never bare `scripts/output/`):
     - `scripts/apply_dtc_null_n_stage_group_fill.py`
     - `scripts/output/apply_dtc_null_n_stage_group_fill_probe.md`
     - `scripts/output/apply_dtc_null_n_stage_group_fill_run.log`
     - `cursor_prompts/CURSOR_PROMPT_DTC_NULL_N_STAGE_GROUP_FILL_20260423_SCRIPT_396.md`
     - `cursor_prompts/CLOSE_OUT_396.md`
   - Commit message: `Script 396: DTC NULL-N stage_group fill (4 rows; 2x Stage I, 2x Stage II)`
   - Tag: `v1_0-dtc-null-n-stage-groups-filled-<YYYYMMDD_HHMMSS>`
   - **Close-out write happens AFTER idempotency check**, not before — a NO-OP re-run must leave the committed close-out intact.

### Halt gates (all must PASS in Phase 0; any FAIL blocks `--apply`)

- **H1 — Scope lock (expect 4):** exactly 4 rows match:
  ```sql
  SELECT COUNT(*) FROM main.canonical_patient_master
  WHERE diagnosis_primary IN ('PTC','FTC','HCC')
    AND ajcc8_stage_group IS NULL
    AND ajcc8_n_stage IS NULL
    AND research_id IN ('2480','2837','4245','6772');
  ```
  FAIL if count ≠ 4.

- **H2 — No crossover into queued cohort:** rids 1404, 12198 must NOT be in this script's write set:
  ```sql
  SELECT COUNT(*) FROM manuscript_workspace.cpm_stage_group_manual_review_v1
  WHERE research_id IN ('2480','2837','4245','6772');
  ```
  FAIL if count > 0.

- **H3 — CPM invariant pre-apply:** `SELECT COUNT(*) FROM main.canonical_patient_master` = 10871. FAIL otherwise.

- **H4 — Per-row signal lock (row-by-row, strict match):**
  - 2480: age=63, `ajcc8_t_stage IS NULL`, `ajcc8_t_stage_v2='T1'`, `ajcc8_n_stage IS NULL`, `ajcc8_n_stage_v2='Nx'`, `ajcc8_m_stage='M0'`, `ajcc8_m_stage_v2='M0'`, `ajcc8_stage_group_corrected IS NULL`
  - 2837: age=15, `ajcc8_t_stage IS NULL`, `ajcc8_t_stage_v2='T1a'`, `ajcc8_n_stage IS NULL`, `ajcc8_n_stage_v2='Nx'`, `ajcc8_m_stage='M0'`, `ajcc8_stage_group_corrected='I'`, `path_stage_raw='I'`
  - 4245: age=69, `ajcc8_t_stage='T3b'`, `ajcc8_t_stage_v2='T3a'`, `ajcc8_n_stage IS NULL`, `ajcc8_n_stage_v2='Nx'`, `ajcc8_m_stage='M0'`, `ajcc8_stage_group_corrected='II'`
  - 6772: age=49, `ajcc8_t_stage IS NULL`, `ajcc8_t_stage_v2='T3a'`, `ajcc8_n_stage IS NULL`, `ajcc8_n_stage_v2='Nx'`, `ajcc8_m_stage='M1'`, `ajcc8_m_stage_v2='M0'`, `dominant_tumor_ajcc8_m_stage='M1'`, `ajcc8_stage_group_corrected='II'`, `path_stage_raw='II'`

  FAIL (exit 2, print offending row) if any single column mismatches.

- **H5 — AJCC8 derivation cross-check (belt + suspenders):** for each of the 4 rows the projected stage_group must equal both (a) the rule-based derivation below and (b) the `ajcc8_stage_group_corrected` where not NULL:
  - 2480: age≥55, M0, T=T1 (from v2), N=Nx→N0 → **I** (precedent-backed; `_corrected IS NULL` tolerated because T is NULL in primary)
  - 2837: age<55, M0 → **I** (matches `_corrected='I'`)
  - 4245: age≥55, M0, T∈{T3a,T3b} → **II** (matches `_corrected='II'`; disagreement logged only)
  - 6772: age<55, M1 (primary dominates v2; path_raw='II' corroborates) → **II** (matches `_corrected='II'`)

  FAIL if any row's rule-derivation disagrees with the planned write.

- **H6 — Convention precedent (for rid 2480 only, since `_corrected IS NULL`):** CPM must contain ≥50 DTC rows matching `age_at_surgery>=55 AND ajcc8_m_stage='M0' AND ajcc8_t_stage IN ('T1','T1a','T1b','T2') AND ajcc8_n_stage IS NULL AND ajcc8_stage_group='I'`. Probe showed 59. FAIL if count <50.

- **H7 — Archive target unused:** snapshot table name base `cpm_pre_dtc_null_n_stage_group_fill_` must have 0 or 1 existing occurrences (0 is fresh; 1 is prior attempt — resume/idempotency).

### Idempotency

- Treat the run as **already applied** iff:
  1. A table named like `archive_pub_v1_0.cpm_pre_dtc_null_n_stage_group_fill_*` exists, AND
  2. A row exists in `main.__readme` with `script_tag='script_396'`, AND
  3. All 4 target rows currently have `ajcc8_stage_group` non-NULL matching the plan.
- If already applied, emit Phase 3 verify only, exit 0, and **DO NOT** overwrite `cursor_prompts/CLOSE_OUT_396.md`.

### Phase 3 post-state gates

- P1 — CPM total still 10871.
- P2 — 4 target rows now have `ajcc8_stage_group` set per plan (2×'I', 2×'II').
- P3 — Total DTC NULL stage_group = 2 (exactly rids 1404 and 12198, both in manual-review queue).
- P4 — `__readme` has exactly one `script_tag='script_396'` row.
- P5 — Snapshot table row count = 4.
- P6 — No other DTC rows' stage_group changed (diff vs snapshot shows only the 4 target rows mutated).

---

## Execution plan for the Composer agent

Run these steps in order, pausing at the plan-approval gate.

1. **Create** `scripts/apply_dtc_null_n_stage_group_fill.py` per the shape above (copy 395's skeleton; adapt cohort, gates, writes, disagreement log).
2. **Phase 0 probe:**
   ```
   python scripts/apply_dtc_null_n_stage_group_fill.py --phase 0
   ```
   Emit probe report + SHA256. Print probe SHA256 clearly for the user to hand back via `--i-approve`.
3. **Plan-approval gate:** post the Phase 0 halt-gate summary + planned writes table + projected post-state back to the user. Wait for the user to reply with explicit approval AND the probe SHA256 (or the `--i-approve=<sha>` CLI reply). Do not proceed without both.
4. **Phase 2 apply:**
   ```
   python scripts/apply_dtc_null_n_stage_group_fill.py --apply --i-approve=<sha256_from_step_2>
   ```
   If SHA mismatch, do not retry silently — rehash, show the current value, ask user to reconfirm.
5. **Phase 3 verify:** automatic inside apply; confirm all P1–P6 pass.
6. **Phase 4 commit + tag:** surgical git-add explicit paths only, commit with message above, create tag `v1_0-dtc-null-n-stage-groups-filled-<YYYYMMDD_HHMMSS>` (same UTC timestamp suffix as snapshot), push tag.
7. **Write close-out** `cursor_prompts/CLOSE_OUT_396.md` AFTER the idempotency check clears and Phase 2/3 succeed — not before.
8. **Report back to user** with: commit SHA, tag, rows updated, per-row before→after stage_group, snapshot FQN, probe SHA256, CF-396 followups (if any disagreement needs downstream work), and the new DTC completeness percentage.

---

## Close-out contents (`cursor_prompts/CLOSE_OUT_396.md`)

Include:
- Commit SHA, tag name, UTC timestamp
- Probe SHA256 (consumed)
- Halt-gate verdict table (H1–H7)
- Writes: 4-row before/after table with stage_group diff
- Disagreement log (4245 T3a/T3b, 6772 M1/M0)
- Snapshot FQN
- DTC completeness: 3736/3742 (99.8397%) → 3740/3742 (99.9466%)
- Remaining NULL DTC rows (1404, 12198) confirmed in `cpm_stage_group_manual_review_v1`
- CF-396 followups if any:
  - **CF-396-1** (proposed): rid 4245 T3b/T3a primary-vs-v2 disagreement — root-cause in 240-builder's T-column source precedence
  - **CF-396-2** (proposed): rid 6772 M1/M0 primary-vs-v2 disagreement — root-cause in M-column builder (primary M1 is correct per path_raw + dominant; v2 M0 appears anomalous)
  - **CF-396-3** (proposed): 240-builder COALESCE fallback from `ajcc8_t_stage_v2` when `ajcc8_t_stage IS NULL` (structural fix deferred across 394/395/396)

---

## Verbal gate — please confirm before Composer begins

Reply with one of:
- **"Approved. Run Phase 0, return probe SHA256 for `--i-approve`."** (standard path)
- **"Hold — adjust scope:"** followed by changes (e.g., "skip rid 6772, route to queue instead")

If you want Composer to route the disagreement rows (4245, 6772) to `cpm_stage_group_manual_review_v1` instead of applying, say so now — the script should NOT default to auto-queueing those because the cross-column corroboration is strong (4245: T3a and T3b both yield II; 6772: path_raw='II' + dominant M1 + primary M1 beat v2 M0).
