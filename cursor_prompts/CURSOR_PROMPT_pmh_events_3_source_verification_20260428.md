# Cursor Agent Task — `canonical_pmh_events_v1` Verification (Protocol v2, 3-source)

**Generated:** 2026-04-28 (Cowork session, post-mig_102)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — re-pull before starting (Cowork is also pushing).
**Estimated effort:** 1-1.5 hours, mechanical but multi-stream, autonomous (no Logan touchpoints expected unless cross-source drift surfaces)
**Run order:** Run after `cursor_prompt_psh_events_extraction_faithfulness_20260428.md` finishes (sibling lane).

---

## 1. Goal

Verify `canonical_pmh_events_v1` (Past Medical History) under Protocol v2. **THIS TABLE HAS THREE SOURCES** — the verification splits per source and re-merges at table-level signoff.

---

## 2. Why this is harder than PSH

Probed 2026-04-28: `canonical_pmh_events_v1` has 12,690 rows / 4,157 patients across THREE source_table values:

| source_table | n_rows | character |
|---|---|---|
| `note_entities_problem_list` | 11,579 | LEGACY non-LLM extractor (older, NOT JSON-based) |
| `note_entities_llm_past_medical_hx` | 865 | LLM extraction (newer; `parsed_json` available) |
| `mig_98<b/c/d/e/f>_pmh_synthetic` | 246 | Synthetic rows injected by mig_98 close-outs (PMH-attributions from complications) |

Each source needs its own verification approach. PMH is the first 3-source canonical you'll close.

---

## 3. Don't touch (active parallel lanes)

- `canonical_psh_events_v1` — sibling Cursor lane (separate prompt)
- `canonical_parathyroid_patient_rollup_v1` / `canonical_pathology_clinical_events_v1` / `canonical_cervical_ln_clinical_events_v1` — Cowork's queue
- The 246 `mig_98*_pmh_synthetic` rows specifically — **these are Logan-curated**. Do NOT delete or mass-modify them. Verify-as-injected only.

---

## 4. Reference reading (Cowork auto-memory)

- **`feedback_extraction_faithfulness_llm_canonical.md`** — pattern for the LLM source
- **`project_parathyroid_events_mig_102_closeout.md`** — most recent extraction-faithfulness close-out
- **`project_complications_events_verified_2026-04-28.md`** — context on the mig_98 synthetic PMH rows
- **`project_voice_nerve_mig_98c_closeout.md`** — exact PMH-row template used by mig_98 (look for the INSERT INTO canonical_pmh_events_v1 SQL block)
- `feedback_motherduck_direct_check.md`
- `feedback_surgical_git_add.md`
- `feedback_phi_safety.md`
- `reference_protocol_v2_md_accounts.md`

In the repo:
- `qc_framework_v1/migrations/102_parathyroid_events_table_signoff.sql` — close-out structural template
- `qc_framework_v1/scripts/apply_mig_98c_decisions.py` (and 98d/98e/98f) — original synthetic PMH-row writers

---

## 5. Schema reference (probed 2026-04-28)

```
research_id            VARCHAR
source_table           VARCHAR  ← stratifier (3 values)
source_row_id          VARCHAR
source_note_type       VARCHAR
llm_confidence         DOUBLE
extractor_name         VARCHAR
finding_text           VARCHAR
finding_value          VARCHAR
finding_value_norm     VARCHAR
finding_date           DATE
mention_note_date      DATE
finding_status         VARCHAR
evidence_strength      VARCHAR
days_from_first_thyroidectomy  BIGINT
is_preexisting         BOOLEAN
anchor_source          VARCHAR
med_status             VARCHAR
evidence_span_hash     VARCHAR
build_ts               TIMESTAMP
```

Registry status (re-confirm before starting): expect ~15 not_started, ~4 already-na cols.

---

## 6. Methodology — 3 sub-passes

### 6a. Source 1: `note_entities_problem_list` (LEGACY extractor, 11,579 rows)
- This is a non-LLM, non-JSON upstream. Its extraction is from structured problem-list rows in clinical notes (likely a regex/dictionary extractor).
- **Probe upstream first**: `SELECT * FROM main.note_entities_problem_list LIMIT 5;` and `SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.note_entities_problem_list`.
- Adapt the extraction-faithfulness pattern: re-derive each canonical col fresh from upstream cols (not parsed_json) and compare per-row IS DISTINCT FROM. The build SQL likely does straight column copies + minor normalization.
- If the build does normalization (e.g., uppercase → lowercase, ICD code → finding_value_norm string), capture the normalization rule and re-derive faithfully.

### 6b. Source 2: `note_entities_llm_past_medical_hx` (LLM extractor, 865 rows)
- Standard extraction-faithfulness vs `parsed_json` (mig_102 pattern).
- Find the build SQL for these rows. Likely `qc_framework_v1/migrations/<N>_pmh_canonical_*.sql` or similar.
- Re-derive every adjudicated col fresh and compare.

### 6c. Source 3: `mig_98*_pmh_synthetic` (Logan-curated, 246 rows)
- These rows have NO upstream JSON or extractor — they were INSERTed by `apply_mig_98c/d/e/f_decisions.py` from already-verified complication classifier output.
- **Verification approach: verify-as-injected.** Do not re-derive. Sanity check:
  - Every row has `is_preexisting=TRUE` (per template)
  - Every row has `anchor_source` that includes 'mig_98' or 'logan_curated'
  - `evidence_span_hash` is a sha256 of the rationale (per template)
  - finding_value_norm is from the complications domain vocab (e.g., 'rln_injury', 'voice_change', 'hypoparathyroidism', etc.)
- If sanity checks pass for all 246 rows, mark these via `verification_method='verify_as_injected_logan_curated_mig98_attribution'` with a reference to the originating mig_98 sub-mig.
- Cross-validation: `SELECT research_id, finding_value_norm FROM canonical_pmh_events_v1 WHERE source_table LIKE 'mig_98%_pmh_synthetic'` should match (or be subset of) `SELECT research_id, ... FROM canonical_complications_events_v1 WHERE finding_status='present' AND <complication_type filters>`. Confirms no orphan synthetic rows.

### 6d. Cross-validation (LLM-quality CFs)
After per-source verification, scan for inconsistencies:
- Patients where multiple sources record the SAME PMH item with different `finding_status` (e.g., legacy says 'present' but LLM says 'absent') — flag as CF-PMH-MULTISOURCE-DISAGREEMENT
- Patients in `canonical_complications_events_v1` (verified mig_99) with `is_preexisting=TRUE` whose corresponding PMH row is missing — flag as CF-PMH-COMPLICATION-MISS

These are CFs, not blockers. Document counts + sample 5-10 rows.

### 6e. Sign-off migration
Numbered `qc_framework_v1/migrations/<N>_pmh_events_table_signoff.sql` using next available migration number. 5-section structure:
- Section a: flip Source 1 (legacy) cols (per-col verification_method)
- Section b: flip Source 2 (LLM) cols (extraction_faithfulness_vs_upstream_json)
- Section c: flip Source 3 (synthetic) cols (verify_as_injected_logan_curated_mig98_attribution)
- Section d: any cross-source CF notes
- Section e: recompute `canonical_table_signoff_registry_v1` counts + sign off

Important: cols are common across sources (the same 15 not_started cols). The verification per-source is at ROW grain, not col grain. So registry sign-off flips each col ONCE with a verification_method that captures the multi-source nature: e.g., `verification_method='extraction_faithfulness_3_source_legacy_llm_synthetic'` for all 15 cols, with notes detailing the per-source breakdown.

---

## 7. Acceptance gates

- 15 not_started cols flipped to verified or na
- table_status='verified' in `canonical_table_signoff_registry_v1`
- Per-source verification logged in close-out doc with row count match counts
- 246 synthetic rows preserved unchanged
- Cross-source CFs documented (if any)

---

## 8. File / commit conventions

Same as PSH prompt + parathyroid prompt. Author Logan, surgical add, lint Python, never print PHI, CAST CURRENT_TIMESTAMP AS TIMESTAMP.

---

## 9. If something unexpected surfaces

- A 4th source value in source_table you didn't plan for → STOP, investigate, ask Logan
- Source 3 synthetic row count doesn't match the sum from mig_98 close-out documents (246 expected) → STOP, this means rows were lost or duplicated; investigate
- Mass-equivalence on Source 1 shows widespread drift → STOP, the legacy extractor build SQL may be more complex than column-copy
- Cross-validation surfaces large CF-PMH-COMPLICATION-MISS (>50 patients) → document but do not block; this is an LLM extraction gap

---

End of prompt. Update Cowork `MEMORY.md` with a one-line index entry referencing the new mig close-out memory you create. Pattern note: the **3-source verification methodology** is itself a reusable pattern — save as a feedback memory for future multi-source canonicals.
