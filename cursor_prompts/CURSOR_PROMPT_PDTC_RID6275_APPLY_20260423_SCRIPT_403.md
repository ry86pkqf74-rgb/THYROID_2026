# CURSOR COMPOSER 2.0 PROMPT — Script 403 — rid 6275 PDTC stage_group apply (narrow; 1 apply + 1 queue delete)

**Target DB:** `thyroid_canonical_publication_v1_0` (MotherDuck cloud DuckDB)
**Auth:** `.env.motherduck` → `MOTHERDUCK_TOKEN` (use `.venv/bin/python` for DuckDB 1.5.2 parity)
**Mode:** Phase-gated runner (Phase 0 probe → plan-approval → `--apply` → Phase 3 verify → Phase 4 commit+tag+push)
**Verbal-gate formalization:** `--i-approve=<probe_report_sha256>` required for `--apply`
**CPM invariant:** `main.canonical_patient_master` row count = **10,871**
**Repository:** `/Users/loganglosser/THYROID_2026`
**Script path:** `scripts/apply_pdtc_rid6275_stage_group.py` (new)
**Tag-prefix:** `v1_0-pdtc-rid6275-stage-group-applied-`
**Close-out path:** `cursor_prompts/CLOSE_OUT_403.md`
**Run-log path:** `scripts/output/apply_pdtc_rid6275_stage_group_run.log`
**Probe-report path:** `scripts/output/apply_pdtc_rid6275_stage_group_probe.md`
**CPM snapshot:** `archive_pub_v1_0.cpm_pre_pdtc_rid6275_stage_group_<ts>` (1 UPDATE target row)
**Queue snapshot:** `archive_pub_v1_0.queue_pre_pdtc_rid6275_stage_group_<ts>` (1 queue row pre-delete)

---

## Context — rid 6275 PDTC resolution (unwinding CF-401-5)

Script 401 deferred rid 6275 (PDTC via histology, currently classified as `diagnosis_primary='other_malignant'`) pending Script 402's classification audit. Script 402 landed, materializing a PDTC_SCATTER axis of 47 rows. Convention probe against the 46 already-staged PDTC-like rows in CPM confirms **all 46 follow DTC age-stratified AJCC8 staging rules exactly**:

| age bucket | M | Stage | precedent rows |
|-----------|---|-------|----------------|
| <55 | M0 | I | 6 |
| <55 | M1 | II | 10 |
| ≥55 | M0 | I (T1/T2 N0) | 1 |
| ≥55 | M0 | II (T3/N1) | 9 |
| ≥55 | M1 | IVB | 20 |
| **Total** | — | — | **46** |

rid 6275 current state: age 38, M0, T=NULL, N0 primary / N1a v2 (ignored — neither changes the outcome under age<55 rules). `ajcc8_stage_group_corrected='I'` already populated by builder. `path_stage_raw` NULL.

**Derivation under AJCC 8th Ch 73 DTC age-stratified rules (PDTC explicitly grouped with DTC):** age<55 + M0 → **Stage I**, regardless of T and N. Matches `_corrected='I'` and the 6-row age<55/M0 precedent bucket.

**diagnosis_primary normalization (the 47-row PDTC cohort consolidation to `diagnosis_primary='PDTC'`) is explicitly deferred to Script 404.** This script touches only stage_group and queue membership for rid 6275.

---

## Planned writes

### Write A — CPM UPDATE (rid 6275 stage_group only)

```sql
UPDATE main.canonical_patient_master
SET ajcc8_stage_group = 'I'
WHERE research_id='6275'
  AND diagnosis_primary='other_malignant'
  AND age_at_surgery=38
  AND ajcc8_m_stage='M0'
  AND ajcc8_stage_group IS NULL
  AND ajcc8_stage_group_corrected='I'
  AND histology_final='poorly differentiated thyroid carcinoma'
RETURNING research_id;
```

The WHERE predicate is over-specified by design — diagnostic safety. The `histology_final` check prevents accidental application if a future data refresh changes rid 6275's classification. The `_corrected='I'` check corroborates the derivation.

### Write B — Queue DELETE (remove rid 6275 from manual-review queue)

```sql
DELETE FROM manuscript_workspace.cpm_stage_group_manual_review_v1
WHERE research_id='6275' AND source_script='399';
```

### Write C — `__readme` provenance row

`script='script_402'`, wait — that's wrong. Use `script='script_403'`, `script_name='apply_pdtc_rid6275_stage_group.py'`, `run_timestamp=CAST(CURRENT_TIMESTAMP AS TIMESTAMP)`, content including:
- 1 CPM UPDATE (rid 6275 → Stage I)
- 1 queue DELETE (rid 6275)
- Derivation note: AJCC 8th Ch 73 DTC age-stratified rule; PDTC explicitly grouped with DTC
- Convention corroboration: 46/46 already-staged PDTC cohort rows follow DTC rules; 6-row precedent for age<55 M0 → I
- Snapshot FQNs
- Probe SHA256 consumed
- Explicit note: diagnosis_primary unchanged (still 'other_malignant'); cohort consolidation deferred to Script 404

**Explicitly NOT doing:**
- No writes to T/N/M columns
- No writes to `diagnosis_primary`, `diagnosis_variant`, `histology_final`, or any classification column
- No changes to `_corrected` or `path_stage_raw`
- No changes to any of the other 46 PDTC cohort rows
- No changes to any other queue row

---

## Script requirements (`scripts/apply_pdtc_rid6275_stage_group.py`)

Copy Script 401's skeleton (CPM UPDATE + queue DELETE + dual snapshot pattern). Inherit every runner behavior:

- Stable probe hash (no timestamps in hashed region; `---HASH-BOUNDARY---` footer)
- `--i-approve=<sha256>` required (exit 3 mismatch, exit 5 missing)
- `--phase4` NO-OP-safe
- `FORCE_ADD_PATTERNS = [r"scripts/output/.*_run\.log$"]`
- `git push origin HEAD` before tag push, with auto-rebase + `--autostash` retry on rejection (per Script 401/402 operational lesson)
- Close-out write AFTER idempotency check
- **Two snapshots** (CPM 1-row + queue 1-row)
- Use `.venv/bin/python` invocation

### Halt gates (Phase 0; all must PASS)

- **H1 — rid 6275 state lock:** single row in CPM with research_id='6275', diagnosis_primary='other_malignant', age_at_surgery=38, ajcc8_m_stage='M0', ajcc8_stage_group IS NULL, ajcc8_stage_group_corrected='I', histology_final='poorly differentiated thyroid carcinoma'. FAIL if any column drifts.
- **H2 — Queue scope:** rid 6275 currently in `cpm_stage_group_manual_review_v1` with source_script='399'. FAIL otherwise.
- **H3 — Queue total:** currently 7 rows. FAIL otherwise.
- **H4 — CPM invariant:** 10,871.
- **H5 — PDTC convention corroboration:** probe the 46 already-staged PDTC-like rows and verify the age<55 M0 → I bucket has ≥5 rows. FAIL if <5 (convention precedent weakens).
- **H6 — AJCC 8th Ch 73 rule derivation:** age (38) < 55 AND M (M0) = M0 → Stage I per DTC age-stratified rule. Static check; PASS.
- **H7 — Archive targets unused:** neither `cpm_pre_pdtc_rid6275_stage_group_` nor `queue_pre_pdtc_rid6275_stage_group_` prefix present.
- **H8 — No T/N/M/classification writes:** static SQL audit — CPM SET clause contains only `ajcc8_stage_group`; no other columns referenced in SET. FAIL otherwise.
- **H9 — Other PDTC cohort untouched:** static SQL audit — script SQL references only research_id='6275' for writes. Any reference to the other 46 PDTC rids fails the gate (prevents accidental scope creep into Script 404 territory).
- **H10 — No other queue modifications:** static SQL audit — no reference to rids 1404, 12198, 423, 924, 9600, 6768 (the other 6 queue rows). Belt-and-suspenders.

### Idempotency

Treat as applied iff all of:
1. CPM snapshot `cpm_pre_pdtc_rid6275_stage_group_*` exists
2. Queue snapshot `queue_pre_pdtc_rid6275_stage_group_*` exists
3. `__readme script='script_403'` row exists
4. rid 6275 has `ajcc8_stage_group='I'` in CPM
5. rid 6275 absent from `cpm_stage_group_manual_review_v1`
6. Queue row count = 6 (was 7)

If applied → NO-OP, Phase 3 verify only, exit 0. Close-out not overwritten on NO-OP re-run.

### Phase 3 post-state gates

- P1 — CPM total = 10,871
- P2 — rid 6275 has `ajcc8_stage_group='I'`
- P3 — rid 6275 diagnosis_primary STILL 'other_malignant' (normalization deferred to 404)
- P4 — rid 6275 histology_final STILL 'poorly differentiated thyroid carcinoma' (unchanged)
- P5 — Queue row count = 6
- P6 — Queue rows 1404, 12198, 423, 924, 9600, 6768 all present with UNCHANGED reasons
- P7 — Malignant allowlist NULL stage_group count = 6 (down from 7)
- P8 — `__readme script='script_403'` count = 1
- P9 — CPM snapshot row count = 1
- P10 — Queue snapshot row count = 1
- P11 — CPM diff (snapshot vs current) shows only `ajcc8_stage_group` changed NULL→'I' for rid 6275; all other columns identical

---

## Execution plan for Composer

1. Create `scripts/apply_pdtc_rid6275_stage_group.py` from 401's skeleton (dual snapshot + UPDATE + DELETE pattern).
2. Run Phase 0:
   ```
   .venv/bin/python scripts/apply_pdtc_rid6275_stage_group.py --phase 0
   ```
3. Pause at plan-approval gate. Post H1–H10 verdicts + writes summary + PDTC convention precedent table.
4. On approval:
   ```
   .venv/bin/python scripts/apply_pdtc_rid6275_stage_group.py --apply \
     --i-approve=<sha256> --phase4
   ```
5. Phase 4 surgical git-add (5 paths):
   - `scripts/apply_pdtc_rid6275_stage_group.py`
   - `scripts/output/apply_pdtc_rid6275_stage_group_probe.md`
   - `scripts/output/apply_pdtc_rid6275_stage_group_run.log` (`-f`)
   - `cursor_prompts/CURSOR_PROMPT_PDTC_RID6275_APPLY_20260423_SCRIPT_403.md`
   - `cursor_prompts/CLOSE_OUT_403.md`
6. Commit message: `Script 403: rid 6275 PDTC → Stage I (AJCC8 Ch 73 DTC age-stratified rule; convention corroborated by 46/46 PDTC cohort)`
7. Tag: `v1_0-pdtc-rid6275-stage-group-applied-<YYYYMMDD_HHMMSS>`
8. Auto-rebase+autostash on push rejection, single retry, fail loudly on second rejection.
9. Close-out write AFTER idempotency check.
10. **Tag stability rule (per 402 lesson):** tag points at the materialize commit. Close-out or any subsequent doc-only commits are separate commits on `main`, NOT retagged. Close-out content uses stable wording: tag name + `git rev-parse` for the materialize SHA.

---

## Close-out contents (`cursor_prompts/CLOSE_OUT_403.md`)

- Commit SHA (materialize), tag, UTC timestamp
- Probe SHA256 (consumed)
- Halt-gate verdict table (H1–H10)
- Applied: rid 6275 age 38 PDTC → Stage I (AJCC 8th Ch 73 DTC age-stratified rule)
- Convention corroboration table (46/46 PDTC-like rows follow DTC rules)
- Queue DELETE: rid 6275
- Post-state: queue 7 → 6; malignant NULL stage_group in CPM 7 → 6
- Remaining queued rows (6): 1404, 12198, 423, 924, 9600, 6768 — all reasons unchanged
- CF-401-5 marked resolved.
- CF-403 followups:
  - **CF-403-1 → Script 404:** PDTC diagnosis_primary normalization — consolidate the 47-row PDTC cohort to `diagnosis_primary='PDTC'`. 37 `other_malignant` rows are pure PDTC candidates. 6 PTC + 3 FTC rows are likely "mixed" / "with PDTC features" and should stay in their original classification with histology_final as the authoritative PDTC indicator. Script 404 needs careful per-row adjudication.
  - **CF-403-2:** rid 6275 diagnosis_primary could be updated to 'PDTC' as part of Script 404; until then it stays 'other_malignant' but has `histology_final='poorly differentiated thyroid carcinoma'` as the authoritative classification.

---

## Verbal gate — confirm before Composer begins

Reply with:
- **"Approved. Run Phase 0, return SHA256 for `--i-approve`."** — standard path
- **"Hold — expand to Script 404:"** (roll diagnosis_primary normalization into this script instead of deferring; changes scope from 1-row to 47-row apply)
- **"Hold — narrow:"** (don't apply; keep rid 6275 queued; this would mean overruling the 46-row convention precedent)

The 46-row convention precedent is strong enough that applying rid 6275 → Stage I is effectively an alignment-to-existing-CPM-behavior write rather than a novel derivation. If you want to take this all the way and normalize diagnosis_primary for the PDTC cohort now (Option "Hold — expand"), that becomes a 47-row script and I should redraft with broader scope.
