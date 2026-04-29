# Cursor Agent Task — `canonical_medications_events_v1` Verification (Protocol v2)

**Generated:** 2026-04-28 (Cowork session, post-mig_101)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `9a470e2` — `mig_101: canonical_path_gland_patient_rollup_v1 verified — path_gland family complete (19th table)`
**Estimated effort:** 3-4 hours, substantial but autonomous (Logan touchpoint expected at adjudication-CSV review)
**Run order:** Second after `cursor_prompt_clinical_date_retype_20260428.md` completes successfully.

---

## 1. Goal

Verify `canonical_medications_events_v1` under Protocol v2. Reuses the **note-text REAL/TEMPLATE classifier** pattern established in mig_98b (chyle_leak), 98c (voice/nerve), 98d (seroma), 98e (hematoma), 98f (hypoparathyroidism). Apply bulk priority-rule disposition with date-based attribution (KEEP / PMH-attribute / DELETE). Sign off table.

This will be the 21st canonical table closed under v2 (assuming Cursor 1 + my parallel parathyroid work both succeed first).

---

## 2. Scope

**Target:** `main.canonical_medications_events_v1`
**Shape:** 19 cols / 7,501 rows / 2,070 patients
**Registry status (audit before starting):**
- 15 not_started cols
- 4 already-na cols (likely auto_provenance/identifier)

Don't touch (active parallel lanes):
- `canonical_parathyroid_events_v1` — Cowork's lane
- Any Cursor 1 (date-cleanup) target — leave that lane alone

---

## 3. Schema reference (probed 2026-04-28)

```
research_id            VARCHAR
source_table           VARCHAR    (likely note_entities_medications)
source_row_id          VARCHAR
source_note_type       VARCHAR    (e.g., h_p, op_note)
llm_confidence         DOUBLE
extractor_name         VARCHAR
finding_text           VARCHAR    (raw text mention)
finding_value          VARCHAR    (medication name extracted)
finding_value_norm     VARCHAR    (normalized — e.g., 'levothyroxine', 'rai_dose')
finding_date           DATE       (medication start/mention date)
mention_note_date      DATE       (date of the note containing the mention)
finding_status         VARCHAR    (e.g., present)
evidence_strength      VARCHAR    (e.g., definitive)
days_from_first_thyroidectomy  BIGINT
is_preexisting         BOOLEAN
anchor_source          VARCHAR    (e.g., first_surgery_fallback)
med_status             VARCHAR    (e.g., active, unknown)
evidence_span_hash     VARCHAR
build_ts               TIMESTAMP
```

Sample (3 rows, real LLM output):
```
research_id=10097, finding_value_norm='levothyroxine', med_status='unknown', is_preexisting=TRUE, mention_note_date=2023-06-05
research_id=10105, finding_value_norm='levothyroxine', med_status='active', is_preexisting=TRUE, mention_note_date=2023-07-19
research_id=10154, finding_value_norm='rai_dose', med_status='unknown', is_preexisting=TRUE, mention_note_date=null
```

---

## 4. Methodology — note-text REAL/TEMPLATE classifier (medications domain)

Generalized from mig_98 family. Adapt for medications, which are a different domain than complications.

### 4a. Probe distinct values
First understand the vocab:
```sql
SELECT finding_value_norm, COUNT(*) AS n
FROM main.canonical_medications_events_v1
GROUP BY 1 ORDER BY n DESC LIMIT 50;

SELECT med_status, COUNT(*) AS n
FROM main.canonical_medications_events_v1
GROUP BY 1 ORDER BY n DESC;

SELECT source_note_type, COUNT(*) AS n
FROM main.canonical_medications_events_v1
GROUP BY 1 ORDER BY n DESC;
```

### 4b. Pull text context for every mention
For each row, fetch the surrounding ~500 chars from `clinical_notes_long`. Use the `source_row_id` and `evidence_span_hash` to locate the mention. (See mig_98b builder script `qc_framework_v1/scripts/build_chyle_leak_review.py` for the pattern.)

### 4c. Classify REAL vs TEMPLATE per mention
**REAL signals (medications domain):**
- Treatment vocabulary: "started", "prescribed", "continue", "increase to", "dose", "mg daily", "BID", "QID"
- Active context: "currently taking", "patient on", "home medications include"
- Adjustment: "stopped", "discontinued", "held", "decreased to"
- TSH-suppressive (thyroid-specific): "TSH suppression", "suppressive dose"

**TEMPLATE signals (medications domain — ignore):**
- Allergy lists: "NKDA", "no known allergies", "allergies: levothyroxine"
- Boilerplate consent: "discontinue if pregnant", "side effects include"
- Negated: "not on", "denies", "no medications", "off levothyroxine"
- Family history templates
- Generic preop checklists not tied to this patient

### 4d. Bulk priority-rule disposition (Logan-ratified for mig_98 family — extends to meds)
Priority order for each row:
1. If finding_status='absent' OR text matches negation regex → **DELETE**
2. If REAL signal present + date attribution check (4e) → **KEEP** or **PMH-ATTRIBUTE**
3. If TEMPLATE signal dominates → **DELETE** (template noise)
4. If neither REAL nor TEMPLATE clearly wins (~5-10% expected) → **REVIEW** workbook for Logan

### 4e. Date-based attribution (Logan-ratified, generalizable)
Using `days_from_first_thyroidectomy` (this col IS already populated in canonical_medications_events_v1):
- `< 0` or NULL with mention before surgery → PREEXISTING → leave is_preexisting=TRUE → **KEEP** (legitimate PMH med)
- `0–30` → operative period → **KEEP** (postop med)
- `31–180` → postop late → **KEEP** (e.g., levothyroxine titration)
- `181-365` → postop very late → **KEEP** (chronic suppression therapy is normal here)
- `>365` → likely chronic ongoing → **KEEP** (medications are chronic; behavior differs from complications)
- TSH-suppressive medications post-thyroidectomy are EXPECTED and clinically meaningful — never PMH-attribute these.

**Note**: Medications don't follow the complications PMH attribution rule strictly. A medication taken throughout treatment is legitimate KEEP at every stage. PMH-attribute only when text clearly says "patient has been on X since [pre-surgery date]" with no posto context.

### 4f. Apply dispositions
- **KEEP**: leave row in canonical_medications_events_v1 unchanged.
- **PMH-ATTRIBUTE**: copy to `canonical_pmh_events_v1` with `is_preexisting=TRUE`, anchor_source='mig_<X>_classifier_logan_curated', then DELETE from canonical_medications_events_v1. Use the PMH-row template from `project_voice_nerve_mig_98c_closeout.md`.
- **DELETE**: hard delete from canonical_medications_events_v1.
- **REVIEW**: surface in .xlsx workbook for Logan adjudication (use `feedback_review_csv_formatting.md` — openpyxl + .xlsx, never csv.QUOTE_ALL).

---

## 5. Deliverables

- `qc_framework_v1/scripts/build_medications_review.py` — pulls mentions + text context + classifier candidate
- `qc_framework_v1/scripts/apply_mig_X_medications_decisions.py` — applies bulk dispositions + Logan adjudication CSV (if any)
- `qc_framework_v1/migrations/X_mig_medications_apply.md` — close-out doc (counts: KEEP / PMH / DELETE / REVIEW)
- Audit workbook for any REVIEW rows (saved to `verification_csvs/canonical_medications_events_v1/`, gitignored except force-added Logan-curated copies)

Final state target:
- 15 not_started cols → all flipped to verified or na
- table_status='verified' in `canonical_table_signoff_registry_v1`
- Provenance row in `manuscript_workspace.cpm_reconciliation_provenance_v1` (per existing pattern from prior mig_98 close-outs)

---

## 6. Logan touchpoint protocol

If your classifier surfaces a REVIEW workbook with >50 rows of true ambiguity:
1. Stop. Don't auto-apply.
2. Save the workbook to `verification_csvs/canonical_medications_events_v1/medications_review_<ts>.xlsx`.
3. Commit the workbook file (force-add since `verification_csvs/` is gitignored).
4. Push and exit with a message: "Awaiting Logan adjudication on N rows in medications_review_<ts>.xlsx".

If <50 ambiguous rows: surface them inline in the .md close-out for Logan to skim, but auto-apply best-guess dispositions following the priority rules.

---

## 7. File / commit conventions

- Author: `Logan Glosser <logan.glosser@gmail.com>`
- Surgical `git add` (memory: `feedback_surgical_git_add.md`)
- Lint Python with `python3 -m py_compile` first
- PHI safety: NEVER print clinical notes content in logs/commits/markdown — use research_id only (memory: `feedback_phi_safety.md`)
- DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ — always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts (memory: `reference_duckdb_timestamp_tz.md`)

---

## 8. Reference reading (auto-memory)

Required:
- `project_complications_events_verified_2026-04-28.md` — overall mig_98 close-out summary
- `project_chyle_leak_mig_98b_closeout.md` — first classifier example
- `project_voice_nerve_mig_98c_closeout.md` — date-attribution rule + PMH-row template
- `project_seroma_mig_98d_closeout.md` — bulk priority-rule pattern
- `feedback_review_csv_formatting.md` — workbook format
- `feedback_phi_safety.md` — note-content safety
- `feedback_motherduck_direct_check.md` — re-query MD before recommending

---

## 9. What's done already / not duplicate

- 17 of the 21 expected verified-canonicals (per registry); plus path_gland family + frozen_section closed Cowork-side; date-cleanup running parallel (Cursor 1).
- Don't try to re-derive the upstream LLM extraction itself (`note_entities_medications`) — that's already verified upstream. Operate at the canonical_medications_events_v1 grain.

---

End of prompt. When done, commit the deliverables, push, update Cowork `MEMORY.md` with a close-out memory entry following the mig_98 close-out style.
