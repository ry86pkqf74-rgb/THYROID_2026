# Script 364 — Final Close-Out

**Completion date:** 2026-04-22
**Final tip of `main`:** TBD (this commit)
**Total session commits:** 4 (build cascade) + 1 (this close-out)

---

## 1. SHA chain (this session, oldest → newest)

| SHA | Subject | Phase |
|---|---|---|
| `9ed8cb6` | (baseline at session start — Script 363 close-out) | — |
| `043ac8b` | Script 364B: survival follow-up consolidation canonical (sister to 364) | Sister build |
| `b887e40` | Script 364: complications consolidation canonical (CHANGES A-J + Q-A option 2) | Build |
| `1ddb25f` | Script 364 CPM feeder repoint: 12 repointed + 8 temporal + 36 tiered | CPM repoint |
| **TBD** | **Script 364 phase 7 strip + final close-out** | **Strip (final) + close-out** |

---

## 2. Summary of new canonicals (live on `thyroid_canonical_publication_v1_0`)

### `main.canonical_complications_events_v1`
- **10,954 rows / 2,542 patients**
- One row per finding (event grain). 17 columns total.
- Linkage = `research_id` only. Every row carries `source_table`, `source_row_id`, `finding_date` so future queries can JOIN cross-domain.
- Columns: `research_id`, `source_table`, `source_row_id`, `source_modality`, `source_kind`, `complication_type`, `source_evidence_type`, `evidence_strength`, `onset_class`, `permanence_class`, `finding_status`, `finding_date`, `detection_date_inferred`, `evidence_span_hash`, `confidence`, `lab_value_at_detection`, `lab_units`, `build_ts` (TIMESTAMP per Pattern 9)
- evidence_span_hash is SHA256 of source text (PHI-safe; raw text never persisted)

### `main.canonical_complications_patient_rollup_v1`
- **10,871 rows × 51 columns** (anchored on `canonical_patient_master`)
- Per-patient: 36 ever_*_<tier> BOOL flags (12 complication types × 3 tiers from CHANGE F/G), 8 temporal-classification BOOL flags (CHANGE D — hypoparathyroidism + hypocalcemia × {preexisting, new_postop, transient, permanent}), 4 aggregate counts/dates, build_ts.

### `main.canonical_survival_followup_v1` (built by sister Script 364B)
- **10,871 rows / 1 per CPM patient**
- Replaces the legacy follow-up-time-only schema with vital_status + death_date + last_known_alive_date.
- vital_status: 10,868 alive / 3 deceased / 0 unknown. followup_complete_at_5yr=TRUE: 1,287; _10yr=TRUE: 505.

### `views_readable.complications_events_VIEW_v1` / `complications_patient_rollup_VIEW_v1` / `survival_followup_VIEW_v1`
- Pass-through SELECT * views; refreshed post-strip.

### Per-type tier counts (distinct patients per ever_*_<tier> column)

| complication_type | definitive | probable_or_better | any_evidence | Notes |
|---|---:|---:|---:|---|
| `rln_injury` | **23** | 39 | 709 | 21 intraop_observed + 18 postop_laryngoscopy = 23 distinct (gold-standard) |
| `vocal_cord_paralysis` | 32 | 32 | 107 | All 32 from laryngoscopy_direct |
| `hypocalcemia_clinical` | 5 | 5 | 9 | 6 lab-derived (Ca <8.0 mg/dL within 30d post-op via CHANGE E) — distinct patients = 5 |
| `hypoparathyroidism` | 0 | 1 | 425 | Definitive=0 because no patient has ≥2 PTH<15 dates documented |
| `hematoma` | 0 | 28 | 169 | Probable=28 from structured_chart source_evidence_type |
| `seroma` | 0 | 621 | 873 | Probable=621 from op_note source_modality |
| `chyle_leak` | **0** | **0** | **1,576** | Source NLP extracts only trigger phrase; no context → all rows fall to `possible` |
| `wound_infection` | 0 | 0 | 0 | All 49 rows dropped by CHANGE H anatomic+temporal gate |
| `pneumothorax` / `airway_complication` / `wound_dehiscence` | 0 | 0 | 0 | 0-pop in cohort; columns preserved per fixed 12-enum |
| `mortality` (peri-op only) | **1** | 1 | 1 | rid 8254, died 4 days post-first-surgery |

### Temporal classification (CHANGE D — distinct patients on rollup)

| complication_type | preexisting | new_postop | transient | permanent |
|---|---:|---:|---:|---:|
| `hypoparathyroidism` | 59 | 362 | 321 | 41 |
| `hypocalcemia_clinical` | 0 | 9 | 9 | 0 |

---

## 3. Dropped / deprecated objects

### Live tables dropped this session (phase 7)

| object | type | scope | snapshot |
|---|---|---|---|
| `complication_phenotype_v1` | TABLE | `main` | `archive_pub_v1_0.complication_phenotype_v1_pre364_20260422_050902` |
| `complication_patient_summary_v1` | TABLE | `main` | `archive_pub_v1_0.complication_patient_summary_v1_pre364_20260422_050902` |
| `note_entities_complications` | TABLE | `main` | `archive_pub_v1_0.note_entities_complications_pre364_20260422_050902` |
| `extracted_complications_refined_v5` | TABLE | `main` | `archive_pub_v1_0.extracted_complications_refined_v5_pre364_20260422_050902` |
| `extracted_rln_injury_refined_v2` | TABLE | `main` | `archive_pub_v1_0.extracted_rln_injury_refined_v2_pre364_20260422_050902` |

### Replaced (legacy → new schema)

| object | replaced by | snapshot |
|---|---|---|
| `canonical_survival_followup_v1` (legacy follow-up-time-only) | 364B (vital_status_current + death_date) | (replaced via CREATE OR REPLACE — no pre-build snapshot since this canonical is owned by 364B and predated this script's archive scope) |

### LLM tables retained (consumed but not dropped)

`note_entities_llm_recurrence`, `note_entities_llm_dynamic_risk_response`, `note_entities_llm_survival_followup`. Out-of-scope entity_types (recurrence, ATA risk, voice quality, last_followup_date) are consumed by 364B + Script 367 in subsequent work.

---

## 4. Reusable patterns generated by this script (carrying forward to 365–367)

| # | Pattern | Status | Carry-forward to |
|---|---|---|---|
| 13 | Idempotent registry `DELETE WHERE detail_table_name IN (...)` before `INSERT` | active | 365, 366, 367 |
| 14 | LLM `result_json` UNNEST template — **CRITICAL FIX (CHANGE B/F):** use `UNNEST(...) t(e_json)` aliasing form, NOT `UNNEST(...) AS e`. The latter wraps each element in a struct (`{'unnest': '...'}`) so `json_extract_string(e, '$.x')` silently returns NULL. Cost ~2 hours of debugging. Applied across 364B + 364. | active | 365, 366, 367 |
| 15 | EXCISE non-domain entity_types from canonical with row-counted audit | active | 366 (excise non-RAI), 367 (LLM table cleanup) |

### NEW patterns this session that should be promoted to AGENTS.md

| # | Pattern | Why it matters | Source |
|---|---|---|---|
| **20** | **Per-(source_table, complication_type) preservation gate (Q-A option 2)** — Replace flat `preservation_X_present_count` gates with source-aware variants. For each source_table contributing events, compute archive-eligible count by re-applying the build's filter logic (vocab map + clinical/lab boundary + intentional CHANGE H/I drops removed) and compare to live present count. Hard floor 0.50 catches catastrophic regressions; intentional drops surface as informational. Catches real regressions without false alarms when intentional gates fire. | this script (Q-A option 2) |
| **21** | **Evidence-strength tier ladder (definitive / probable / possible)** — Per-complication-type CASE expressions that stratify by the rigor of the underlying evidence (lab vs treatment-only vs symptom regex vs anatomic-context vs operative observation vs laryngoscopy vs registry vs unspecified). Three tiered rollup BOOL flags per complication type (`ever_<type>_definitive` / `_probable_or_better` / `_any_evidence`); the publication default is `_probable_or_better`, sensitivity analyses opt into `_any_evidence`. Required helper CTEs for cross-row patient lookups (e.g. PTH-low-count, late-voice-finding). | this script (CHANGE F + G) |
| **22** | **Anatomic+temporal gate for site-specific complications** — For complications where attribution is ambiguous (wound_infection, abscess), require BOTH (a) detection_date within Nd of first_surgery_date AND (b) evidence_text matches site-specific include regex AND (c) NOT excluded by exclusion regex (UTIs, line infections, abscesses elsewhere). DuckDB RE2 lacks negative lookahead — approximate "not X-of-Y where Y≠keyword" with two regexes joined by AND/AND-NOT. Surface dropped count as informational for source-data forensics. | this script (CHANGE H) |
| **23** | **Symptom-specificity proximity gate** — For symptom-based complications where generic terminology (paresthesias, swelling) is too noisy, require evidence_text to contain BOTH a specific clinical term AND an explicit attribution within N chars (bidirectional). Both directions checked because RE2 lacks lookbehind. Apply to source_evidence_type='symptomatic_only' only; lab/treatment paths exempt. | this script (CHANGE I) |
| **24** | **PHI forensic sampling sidecar (CHANGE J)** — When a build's tier counts deviate from literature ranges, emit a markdown sample of N=20 random rows per over-counted type to a gitignored `phi_forensic/` directory. Surface (research_id, source_table, finding_date, source_evidence_type, evidence_strength, evidence_text). The forensic answers "is the source NLP bulk over-extracting?" by exposing whether evidence_text carries actual context or just trigger phrases. Files to .gitignore so PHI never reaches the repo. | this script (CHANGE J) |
| **25** | **Date-completeness backfill ladder (CHANGE A)** — `detection_date NOT NULL` is a hard QA gate. Per-source backfill priority: native_date > note_date > first_surgery_date_per_patient > drop. `detection_date_inferred BOOL` flags non-native dates. Surface inferred ratio per source as informational. Critical when raw sources have >90% NULL date columns (e.g. complication_phenotype_v1.detection_date is 96% NULL in this build but first_surgery_date is 99% populated). | this script (CHANGE A) |
| **26** | **Intentional-drop separation in preservation gates** — When the build deliberately drops rows for clinical-validity reasons (CHANGE H wound_infection without anatomic context, CHANGE I hypocalcemia without symptom specificity), the preservation gate must SUBTRACT those expected drops from the archive baseline before comparing. Otherwise the gate fires on the intentional drops and the team learns to ignore it. The Q-A option 2 implementation does this by pre-filtering the archive query with the same EXCLUDED-FOR-CHANGE-H/I clauses. | this script (Q-A option 2 + CHANGE H/I) |

---

## 5. Carry-forward items (NOT addressed in 364; document for follow-up)

### CF-1 (Tier-1): `note_entities_complications` re-extraction with N-character context
- **Issue**: Source NLP extracts only the trigger phrase ("chyle leak", "seroma", "recurrent laryngeal nerve injury") into `entity_value_norm` / `entity_value_raw` / `evidence_span` — with NO surrounding clinical context.
- **Forensic evidence**: `phi_forensic/qa_script_364_source_overextraction_<TS>.md` (PHI; gitignored). N=60 samples (20 per over-counted type) all show 1-3 token evidence_text.
- **Effect on this build**: chyle_leak / seroma / rln_injury (nlp_proxy) cannot be elevated above evidence_strength='possible' because the source data lacks the context required to verify (a) negation, (b) differential, (c) historical mention, (d) hedged language, (e) wrong attribution. Net inflation of 1,576 chyle_leak `any_evidence` patients vs. ~50-200 expected from literature.
- **Recommended action**: Re-extract `note_entities_complications` with a ±N-character window (suggest N=200) of context around each entity mention, OR add a `evidence_context_window` column to the existing schema. Then re-run Script 364 to promote eligible rows from `possible` to `probable`/`definitive`.
- **Owner**: upstream NLP extraction pipeline (notes_extraction_new). Defer to a future re-extraction cycle.

### CF-2 (Tier-1): rln_injury definitive ceiling at 23 patients
- **Issue**: definitive tier requires postop_laryngoscopy OR intraop_observed evidence. Source data has 21 intraop_observed + 18 postop_laryngoscopy rows = 23 distinct patients (after dedup). Literature would suggest 40-200 per cohort of this size at academic thyroidectomy volume.
- **Status**: This is a true source-data limit. Postop laryngoscopy results and intraop nerve-monitoring observations exist in clinical notes but aren't extracted into structured fields by current NLP.
- **Recommended action**: If a future analysis needs definitive-tier N>23, either (a) opt into the `probable_or_better` tier (39 patients) with explicit caveat, (b) re-extract `note_entities_complications` per CF-1 to recover laryngoscopy mentions, OR (c) launch a targeted laryngoscopy-results-from-ENT-notes extraction project. The canonical's definitive count stays honest at 23 — do NOT loosen criteria to hit a target.

### CF-3: Albumin canonical for corrected-Ca computation
- **Issue**: CHANGE E hypocalcemia case definition uses raw measured Ca < 8.0 mg/dL because no `canonical_labs_albumin_v1` exists. ATA 2015 guidelines specify corrected_Ca = measured_Ca + 0.8 × (4.0 − albumin).
- **Effect**: 6 lab-derived hypocalcemia events on this build use raw Ca; some may be over-detection if patient was hypoalbuminemic (would have been in normal range after correction). Sensitivity is acceptable (low false-positive risk on a small N), but a future cohort with more lab data should re-correct.
- **Recommended action**: Extend Script 347's labs canonical to include albumin OR add `lab_value_at_detection_corrected` as a downstream computed column once albumin canonical exists.

### CF-4: 14 unmapped LLM survival_followup entity_types deferred to 367
- **Status**: 364B consumes `vital_status` + `last_followup_date` from `note_entities_llm_survival_followup`. The other entity_types in that LLM table (`voice_quality`, `discharge_disposition`, `surveillance_interval`, `ata_risk_category`, `disease_free`, `distant_recurrence`, `structural_recurrence`, `biochemical_persistence`, `surveillance_interval`, etc.) are out of scope for both 364 and 364B.
- **Recommended action**: Script 367 (LLM table cleanup) decides per-entity disposition. Voice_quality could go into a future canonical_voice_outcomes or be used as a CF-1-context-recovery input for vocal_cord_paralysis. Recurrence/ATA-risk entities belong in a future `canonical_recurrence_risk_v1` or similar.

---

## 6. Memory-update suggestions for AGENTS.md

Recommended additions to the reusable patterns memory:

1. **Add Patterns 20-26** — see Section 4 above. Q-A option 2 preservation gate (20), evidence-strength tier ladder (21), anatomic+temporal gate (22), symptom-specificity proximity gate (23), PHI forensic sampling sidecar (24), date-completeness backfill ladder (25), intentional-drop separation in preservation gates (26).
2. **Update Pattern 14** with the `UNNEST(...) t(e_json)` aliasing fix (was `UNNEST(...) AS e` which wraps elements in a struct).
3. **Promote evidence_strength tier discipline** — for any clinical canonical where literature has well-established prevalence ranges, build an evidence_strength CASE expression and tiered rollup BOOL flags BEFORE the first dry-run. Avoids the "publish inflated numbers and then retrofit" antipattern.

### Workflow patterns to memorialise

- **PHI forensic answers "is source NLP over-extracting?"**: when tier counts deviate from literature, emit a sampled markdown to gitignored phi_forensic/ before considering looser criteria. The forensic typically reveals upstream extraction limits, which then become Tier-1 carry-forwards (not changes to the canonical case definition).
- **4-commit cascade pattern (extends 363's 3-commit pattern)**: sister-canonical build (364B) → primary build (364) → CPM repoint → cascade strip. Each commit independently verifiable.
- **Iterate clinical-validity changes BEFORE first commit**: 364 had A through J (10 changes) plus Q-A option 2 (1 change) before any --commit. The forensic + literature-comparison pass must happen during dry-run, not post-commit.

### Gotchas to add to project memory

- **Evidence_text NULL is the norm, not the exception**: 96% of complication_phenotype_v1.detection_date is NULL; 67% of note_entities_complications has both date columns NULL; phenotype rows have NULL evidence_text entirely. Build NULL-handling FIRST, then layer filters that depend on evidence_text.
- **DuckDB RE2 lacks lookbehind/lookahead**: anatomic exclusion patterns like `abscess of (?!neck)` must be approximated with two-regex AND/AND-NOT logic. Symptom-attribution proximity must be bidirectional (two patterns OR'd) since you can't anchor on prior context.
- **Existing canonical_survival_followup_v1 schema collision**: a legacy Script 201 had built a different-schema table at this name. CREATE OR REPLACE TABLE is destructive for the legacy schema; ensure dependents are revalidated post-replace.

---

## 7. Files in repo for downstream reference

| Path | Purpose |
|---|---|
| `cursor_prompt_script_364_complications.md` | Original v2 spec (Logan's prompt) |
| `scripts/364_complications_consolidation.py` | Build script (12 hard QA gates) |
| `scripts/364B_survival_followup_consolidation.py` | Sister build (5 hard QA gates) |
| `scripts/364_cpm_feeder_repoint.py` | CPM feeder repoint (12 repoint + 8 temporal + 36 tiered + 24 documented skips) |
| `qa/qa_script_364_complications_consolidation.json` | All 12 gate results from final build |
| `qa/qa_script_364B_survival_followup.json` | All 5 gate results from 364B build |
| `complications_cpm_feeder_audit_<TS>.md` | CPM feeder audit (45 heuristic + 55 discovery) |
| `phi_forensic/qa_script_364_source_overextraction_<TS>.md` | **PHI — local only, gitignored.** N=60 sample of source over-extraction |
| `script_364_FINAL_CLOSEOUT_20260422.md` | This document |
