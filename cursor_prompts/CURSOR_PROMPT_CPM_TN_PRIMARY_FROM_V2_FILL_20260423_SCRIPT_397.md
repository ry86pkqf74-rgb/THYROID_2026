# CURSOR COMPOSER 2.0 PROMPT — Script 397 — CPM T/N primary-from-v2 COALESCE fill (malignant-only, 236-row cohort)

**Target DB:** `thyroid_canonical_publication_v1_0` (MotherDuck cloud DuckDB)
**Auth:** `.env.motherduck` → `MOTHERDUCK_TOKEN`
**Mode:** Phase-gated runner (Phase 0 probe → plan-approval → `--apply` → Phase 3 verify → Phase 4 commit+tag+push)
**Verbal-gate formalization:** `--i-approve=<probe_report_sha256>` required for `--apply`
**CPM invariant:** `main.canonical_patient_master` row count = **10,871**
**Repository:** `/Users/loganglosser/THYROID_2026`
**Script path:** `scripts/apply_cpm_tn_primary_from_v2_fill.py` (new)
**Tag-prefix:** `v1_0-cpm-tn-primary-from-v2-filled-`
**Close-out path:** `cursor_prompts/CLOSE_OUT_397.md`
**Run-log path:** `scripts/output/apply_cpm_tn_primary_from_v2_fill_run.log`
**Probe-report path:** `scripts/output/apply_cpm_tn_primary_from_v2_fill_probe.md`

---

## Context — CF-396-3: builder-240 rescue gap, in-place patch

Scripts 393/394/395/396 patched residual NULL stage_group rows one cohort at a time. A post-396 completeness probe exposed a **structural gap in builder-240** (or whatever pipeline step produces the primary `ajcc8_t_stage`, `ajcc8_n_stage`, `ajcc8_m_stage` columns): it fails to COALESCE from the phase-4.6 `_v2` columns when the primary is NULL, leaving stage-derivation-capable data stranded.

Live MotherDuck audit (2026-04-22):

**Rescue opportunity (NULL primary + populated v2), malignant diagnoses only:**

| Axis | Rescue rows | Of which currently NULL stage_group | Of which already staged |
|------|-------------|-------------------------------------|-------------------------|
| T    | 26          | 5                                   | 21                      |
| N    | 213         | 0                                   | 213                     |
| M    | 0           | 0                                   | 0                       |
| **Distinct rows touched** | **236** | **5** | **231 cosmetic alignment** |

**Benign diagnoses excluded** (AJCC8 not applicable): NIFTP (41), FTUMP (15), follicular_adenoma (7), multinodular_goiter (1) — total 64 rescue candidates dropped.

**Tier-2 (NULL primary + NULL v2 + populated dominant) scope:** 2 rows, both benign (follicular_adenoma, multinodular_goiter) — dropped.

**Cross-source disagreements (primary populated AND v2 populated AND differ):** T=363 rows / N=2055 rows / M=1838 rows — **NOT touched by this script**. Reserved for Script 398 (disagreement-audit sidecar, read-only materialization).

---

## Malignant allowlist (the ONLY diagnoses eligible for this script's writes)

```
diagnosis_primary IN ('PTC','FTC','HCC','DTC_NOS','MTC','ATC','other_malignant')
```

Any row not matching this allowlist is out of scope regardless of T/N NULL state.

---

## Planned writes (2 UPDATE statements on `main.canonical_patient_master`)

```sql
-- Write A: T primary ← T v2 (26 malignant rows expected)
UPDATE main.canonical_patient_master
SET ajcc8_t_stage = ajcc8_t_stage_v2
WHERE diagnosis_primary IN ('PTC','FTC','HCC','DTC_NOS','MTC','ATC','other_malignant')
  AND ajcc8_t_stage IS NULL
  AND ajcc8_t_stage_v2 IS NOT NULL
RETURNING research_id;

-- Write B: N primary ← N v2 (213 malignant rows expected)
UPDATE main.canonical_patient_master
SET ajcc8_n_stage = ajcc8_n_stage_v2
WHERE diagnosis_primary IN ('PTC','FTC','HCC','DTC_NOS','MTC','ATC','other_malignant')
  AND ajcc8_n_stage IS NULL
  AND ajcc8_n_stage_v2 IS NOT NULL
RETURNING research_id;
```

**Explicitly NOT doing:**
- No writes to `ajcc8_m_stage` (0 rescue candidates exist; builder didn't leave a gap here)
- No writes to any currently-populated primary column (zero risk to the 4256 disagreement rows)
- No dominant-fallback writes (2 candidate rows are benign, dropped)
- No writes to stage_group (5 newly-derivable DTC/MTC rows deferred to CF-397-1 follow-up)
- No schema changes (no `tnm_source_flag` column; provenance lives in `__readme` + snapshot)
- No touch to benign rows

**Expected effect counts:**
- `cpm_total` unchanged (10,871)
- T-NULL+v2-populated malignant rows: 26 → 0
- N-NULL+v2-populated malignant rows: 213 → 0
- 236 distinct research_ids updated (some on both T and N)
- DTC stage_group completeness: 3740/3742 (99.9466%) unchanged (the 5 newly-T-derivable rows are non-DTC or fall outside the AJCC8 rules that would auto-stage them; CF-397-1 will re-probe)

---

## Script requirements (`scripts/apply_cpm_tn_primary_from_v2_fill.py`)

Copy the Script 396 skeleton (`apply_dtc_null_n_stage_group_fill.py`) and adapt cohort, gates, writes. Inherit every runner behavior from 396 including:

- **Stable probe hash** (no timestamps in hashed region; footer below `---HASH-BOUNDARY---` excluded from hash)
- **`--i-approve=<sha256>` required for `--apply`** (exit 3 on mismatch; exit 5 on missing token)
- **`--phase4` NO-OP-safe** — a re-run with `--phase4` after NO-OP DB path still runs git add/commit/tag/push
- **Targeted `-f` for run log only** — use the `FORCE_ADD_PATTERNS = [r"scripts/output/.*_run\.log$"]` convention Composer introduced in 396; every other path is a normal add
- **`git push origin HEAD`** before the tag push
- **Close-out write happens AFTER idempotency check** (NO-OP re-run does NOT overwrite `cursor_prompts/CLOSE_OUT_397.md`)
- **Snapshot table:** `archive_pub_v1_0.cpm_pre_tn_primary_from_v2_fill_<ts>` — SELECT * for all 236 research_ids pre-apply
- **`__readme` provenance row:** `script='script_397'`, `script_name='apply_cpm_tn_primary_from_v2_fill.py'`, `run_timestamp=CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`, content including: rows updated (T count, N count, distinct rids), snapshot FQN, probe SHA256, per-diagnosis breakdown

### Halt gates (Phase 0; all must PASS; any FAIL blocks `--apply`)

- **H1 — Scope lock:** `T_fill`=26, `N_fill`=213, `distinct_rids`=236 — FAIL if any deviates.
- **H2 — Allowlist integrity:** zero eligible rows exist with `diagnosis_primary` NOT IN the allowlist. (Query shape: `SELECT COUNT(*) FROM cpm WHERE ((t_null + v2_populated) OR (n_null + v2_populated)) AND diagnosis NOT IN allowlist` — must be 0 among those we WOULD write. Already-excluded benigns are fine; gate checks none sneak into the write predicate.)
- **H3 — No M writes:** `SELECT COUNT(*) FROM cpm WHERE ajcc8_m_stage IS NULL AND ajcc8_m_stage_v2 IS NOT NULL AND diagnosis IN allowlist` = 0 (confirms scope exclusion is still valid).
- **H4 — No populated-primary overwrite:** pre-compute the write predicate and confirm that for EVERY matched row, `ajcc8_t_stage IS NULL` (for write A) or `ajcc8_n_stage IS NULL` (for write B) at the moment of the UPDATE. This is inherent in the WHERE but H4 adds a post-write invariant: diff snapshot vs. post-apply shows column changes ONLY from NULL→non-NULL, never non-NULL→anything.
- **H5 — CPM invariant:** row count = 10,871 before and after.
- **H6 — No stage_group writes:** snapshot vs. post diff shows zero `ajcc8_stage_group` mutations (this script writes T and N only).
- **H7 — Archive target unused:** table base name `cpm_pre_tn_primary_from_v2_fill_` has 0 or 1 prior occurrences (0 fresh; 1 prior-attempt idempotency resume).
- **H8 — Disagreement rows untouched:** for each of the ~4256 primary↔v2 disagreement rows (T=363, N=2055, M=1838), confirm they are NOT in the write predicate. Gate query: `SELECT COUNT(*) FROM cpm WHERE (t_prim IS NOT NULL AND t_v2 IS NOT NULL AND t_prim <> t_v2) AND research_id IN (<write_set>)` = 0. Same for N. FAIL if any disagreement row appears in the write set.

### Idempotency

Treat as already-applied iff all three hold:
1. A table named `archive_pub_v1_0.cpm_pre_tn_primary_from_v2_fill_*` exists
2. A row with `script='script_397'` exists in `main.__readme`
3. The scope query returns zero rows (no more NULL-primary rescue candidates in the malignant allowlist)

If applied: run Phase 3 verify, print NO-OP, exit 0, **DO NOT** overwrite close-out.

### Phase 3 post-state gates

- P1 — CPM total = 10,871
- P2 — Malignant-allowlist T-NULL+v2-populated = 0
- P3 — Malignant-allowlist N-NULL+v2-populated = 0
- P4 — `__readme script='script_397'` count = 1
- P5 — Snapshot table row count = 236
- P6 — Diff snapshot vs. CPM shows exactly: 26 T NULL→non-NULL mutations, 213 N NULL→non-NULL mutations, zero other column mutations, zero M or stage_group mutations
- P7 — Zero disagreement rows (pre-snapshot) were touched

---

## Execution plan for Composer

1. Create `scripts/apply_cpm_tn_primary_from_v2_fill.py` from 396's skeleton.
2. Run Phase 0 probe:
   ```
   python3 scripts/apply_cpm_tn_primary_from_v2_fill.py --phase 0
   ```
   Emit probe report + SHA256. Print SHA clearly.
3. Pause at plan-approval gate. Post H1–H8 verdicts + planned writes counts back to user.
4. On approval:
   ```
   python3 scripts/apply_cpm_tn_primary_from_v2_fill.py --apply \
     --i-approve=<sha256_from_step_2> --phase4
   ```
5. Phase 4 surgical git-add (5 paths):
   - `scripts/apply_cpm_tn_primary_from_v2_fill.py`
   - `scripts/output/apply_cpm_tn_primary_from_v2_fill_probe.md`
   - `scripts/output/apply_cpm_tn_primary_from_v2_fill_run.log` (expect `-f` via `FORCE_ADD_PATTERNS`)
   - `cursor_prompts/CURSOR_PROMPT_CPM_TN_PRIMARY_FROM_V2_FILL_20260423_SCRIPT_397.md`
   - `cursor_prompts/CLOSE_OUT_397.md`
6. Commit message: `Script 397: CPM T/N primary-from-v2 fill (236 rows; 26 T, 213 N; malignant-only)`
7. Tag: `v1_0-cpm-tn-primary-from-v2-filled-<YYYYMMDD_HHMMSS>` matching the snapshot timestamp
8. Push: `git push origin HEAD` then `git push origin <tag>`
9. Write close-out AFTER idempotency check clears and Phase 2/3 succeed

---

## Close-out contents (`cursor_prompts/CLOSE_OUT_397.md`)

- Commit SHA, tag name, UTC timestamp
- Probe SHA256 (consumed)
- Halt-gate verdict table (H1–H8)
- Writes: 26 T, 213 N, 236 distinct rids
- Per-diagnosis breakdown (how many PTC vs FTC vs MTC vs ATC vs other_malignant vs DTC_NOS got touched)
- Snapshot FQN
- Cosmetic-alignment note: 231/236 rows were already-staged — fill is cosmetic for those, preserving audit trail
- 5 newly-T-derivable rows (the T-NULL rows with NULL stage_group): list research_ids + deferred CF-397-1 note
- Disagreement rows audit: **untouched** (363 T + 2055 N + 1838 M deferred to Script 398)
- CF-397 followups:
  - **CF-397-1:** Stage_group re-derivation for the 5 newly-T-filled NULL-stage rows (3 MTC + 1 FTUMP + 1 other_malignant + 1 NIFTP + 1 DTC_NOS — wait, only 5 total; verify exact list in close-out)
  - **CF-397-2:** Builder-240 source code fix — add COALESCE logic so next CPM rebuild reproduces this fill pattern (code change, separate from DB patches)
  - **CF-397-3 → Script 398:** Cross-source disagreement audit (read-only sidecar materialization of the 4256 rows, per-pattern triage)

---

## Verbal gate — confirm before Composer begins

Reply with:
- **"Approved. Run Phase 0, return SHA256 for `--i-approve`."** — standard path
- **"Hold — narrow further:"** followed by changes (e.g., "DTC-only allowlist", "skip MTC/ATC", "write only the 5 NULL-stage rows")
- **"Hold — broaden:"** (e.g., "include NIFTP/FTUMP despite being borderline")

If you want to flip order and run Script 398 disagreement audit FIRST before touching any primaries, say so — Script 398 is read-only materialization and might surface info that changes Script 397's malignant-allowlist calculus (e.g., reveal that v2 is systematically unreliable for certain diagnoses).
