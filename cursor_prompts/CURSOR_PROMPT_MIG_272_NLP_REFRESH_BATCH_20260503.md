# Cursor Composer Dispatch — mig_272: NLP refresh batch (vasc invasion + smoking + family hx)

**Generated:** 2026-05-03 by Cowork at HEAD `be75bee`.
**Lane:** mig_272 — Coordinated re-extraction over `clinical_notes_long` to close 3 documented coverage gaps that block manuscript credibility:
1. **Vascular invasion** — 71.5% of malignant patients have NULL `vascular_invasion_final` (2,942 / 4,113); 749 known under-fires from prior audit.
2. **Smoking status** — 88% of malignant patients have NULL `pmhx_nlp_smoking_status` (only 13 NLP-known + 481 NSQIP-known = 494 / 4,113 = 12% covered).
3. **Family hx of thyroid cancer** — 96% of malignant patients have NULL `pmhx_nlp_family_hx_thyroid` (163 / 4,113 = 4% covered).

**Recommended agent:** **Cursor Chat (Sonnet 4 / GPT-5) → Composer** — NLP scope decisions need walking through; LLM batch runs are non-trivial cost + runtime.
**Estimated runtime:** 8-16 hours wall-clock (LLM throughput-bound; ~10,000-25,000 notes per slice).
**Triggered by:** Logan asked 2026-05-03 "is it needed? if so lets do it" + Cowork audit confirms gap is manuscript-blocking.
**Severity:** HIGH for M044 (smoking is a major Cox PH confounder) + M032 (smoking prevalence is a Table 1 staple). MED for vasc invasion (mig_179 already rebuilt one slice; this is the residual long tail).
**Closes carry-forward:** CF-VASC-INVASION-749-UNDERFIRES + CF-SMOKING-COVERAGE-GAP + CF-FAMILY-HX-COVERAGE-GAP.

---

## §0 — First message to paste into Cursor Chat

> mig_272 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_272_NLP_REFRESH_BATCH_20260503.md` end-to-end. This is a multi-slice LLM re-extraction batch. Use Chat first to walk through:
> 1. Slice scoping (which note classes per slice — see §3)
> 2. Model choice (qwen3:32b on H200 vs ollama local; cost vs latency)
> 3. Prompt template re-validation against feedback_invasion_orphan_clinical_rules.md
>
> Surface a 5-row sample for each slice to me before authorizing Composer-direct full-batch run.

---

## §1 — Why this lane exists

### Coverage gaps (probed on MD 2026-05-03)

| Gap | Numerator | Denominator | % covered | Manuscript blocked |
|---|---:|---:|---:|---|
| `vascular_invasion_final` known (any value) | 1,172 | 4,113 malig | 28.5% | M044 Cox PH (VI is independent prognostic) |
| `pmhx_nlp_smoking_status` known | 13 | 4,113 malig | 0.3% | M032 Table 1 + M044 Cox |
| `nsqip_smoker` known | 481 | 4,113 malig | 11.7% | M044 Cox |
| Either smoking source | 494 | 4,113 malig | 12.0% | M044 Cox + M032 |
| `pmhx_nlp_family_hx_thyroid` known | 163 | 4,113 malig | 4.0% | M032 Table 1 (familial syndrome screen) |

The 749 vasc invasion under-fires are documented in mig_179 close-out (`project_mig_179_invasion_events_lvi_rebuild_closeout.md`) as residual after lymphatic invasion was rebuilt. Smoking + family hx coverage gaps were surfaced in mig_265 PMH definitive tier rebuild but never refreshed at the NLP level.

### Manuscript impact

- **M044 Cox PH multivariable**: smoking status currently dropped from model due to <12% coverage. Reviewers will flag this. Expected post-272 coverage ≥ 70% on PMH NLP slice → smoking enters model.
- **M032 25-yr descriptive Table 1**: smoking prevalence row is currently `n_known=494 (4.5%)` with `n_total=10,871`. Reviewers will reject. Expected post-272: `n_known ≥ 8,000 (74%)`.
- **M037 LN predictors**: family hx is a known LN-met confounder (Carney complex / FAP / FMTC syndromes). Currently dropped. Post-272 enters model.

---

## §2 — Pre-task probes

```sql
-- 2.1 Confirm gap sizes haven't shifted since handoff
SELECT
  COUNT(*) AS n_pts,
  COUNT_IF(is_malignant) AS n_malig,
  COUNT_IF(is_malignant AND vascular_invasion_final IS NULL) AS malig_vi_null,
  COUNT_IF(is_malignant AND vascular_invasion_final IS NOT NULL) AS malig_vi_known,
  COUNT_IF(is_malignant AND pmhx_nlp_smoking_status IS NOT NULL) AS malig_smk_nlp,
  COUNT_IF(is_malignant AND nsqip_smoker IS NOT NULL) AS malig_smk_nsqip,
  COUNT_IF(is_malignant AND (pmhx_nlp_smoking_status IS NOT NULL OR nsqip_smoker IS NOT NULL)) AS malig_smk_any,
  COUNT_IF(is_malignant AND pmhx_nlp_family_hx_thyroid IS NOT NULL) AS malig_fhx_thy
FROM main.canonical_patient_master;

-- 2.2 Note-class coverage probe: which note types most commonly mention each entity?
SELECT note_class, COUNT(*) AS n_notes
FROM main.clinical_notes_long
WHERE LOWER(note_text) ILIKE '%vascular invasion%' OR LOWER(note_text) ILIKE '%lvi%' OR LOWER(note_text) ILIKE '%v1%' OR LOWER(note_text) ILIKE '%v2%'
GROUP BY note_class ORDER BY 2 DESC LIMIT 10;

SELECT note_class, COUNT(*) AS n_notes
FROM main.clinical_notes_long
WHERE LOWER(note_text) ILIKE '%smoking%' OR LOWER(note_text) ILIKE '%tobacco%' OR LOWER(note_text) ILIKE '%pack-year%'
GROUP BY note_class ORDER BY 2 DESC LIMIT 10;

SELECT note_class, COUNT(*) AS n_notes
FROM main.clinical_notes_long
WHERE LOWER(note_text) ILIKE '%family history%thyroid%'
   OR LOWER(note_text) ILIKE '%mother%thyroid cancer%' OR LOWER(note_text) ILIKE '%father%thyroid cancer%'
   OR LOWER(note_text) ILIKE '%sister%thyroid%' OR LOWER(note_text) ILIKE '%brother%thyroid%'
   OR LOWER(note_text) ILIKE '%fmtc%' OR LOWER(note_text) ILIKE '%men2%'
GROUP BY note_class ORDER BY 2 DESC LIMIT 10;

-- 2.3 Prior LLM model used for these slices (to choose new model)
SELECT
  llm_model,
  COUNT(*) AS n_extractions
FROM main.note_entities_llm_pmhx
GROUP BY llm_model ORDER BY 2 DESC;
```

---

## §3 — Slice plan (3 lanes)

### Lane A — Vascular invasion residual

- **Scope:** ~10,000 notes from `clinical_notes_long` where `note_class IN ('path_synoptic','path_dissection','path_addendum','op_note')` AND patient is `is_malignant=TRUE` AND patient has NULL `vascular_invasion_final`.
- **Prompt:** Use `prompts/v2/extract_vascular_invasion_v2.md` (refresh from `feedback_invasion_orphan_clinical_rules.md` rules — vocal-cord-different-column / encapsulated-tumor-capsular / mass-effect / CAP-template-echo).
- **Output:** Append to `main.note_entities_llm_invasion_v2` with `llm_model='qwen3:32b_h200_20260503'`.
- **Followup mig:** mig_272a (rebuild `canonical_invasion_events_v1` lymphatic + vascular slices via mig_179 pattern; should bump VI-known cohort by ~600-900).

### Lane B — Smoking status (PMH)

- **Scope:** ~25,000 notes from `clinical_notes_long` where `note_class IN ('hx_phys','clinic_consult','primary_care_summary','intake_note')` AND patient has NULL `pmhx_nlp_smoking_status`. Cap at 3 most-recent + 3 oldest per patient (6 notes/pt × ~3,500 pts = 21,000).
- **Prompt:** `prompts/v2/extract_pmhx_smoking_v2.md` — output: status (never/former/current/unknown) + pack-years + quit-date + evidence_quote.
- **Output:** Append to `main.note_entities_llm_pmhx` with `category='smoking'` + `llm_model` tag.
- **Followup mig:** mig_272b (rebuild `pmhx_nlp_smoking_status` + `pmhx_nlp_pack_years` on CPM via mig_265 pattern; expected coverage 12% → ≥ 70%).

### Lane C — Family hx thyroid (PMH)

- **Scope:** ~12,000 notes from `clinical_notes_long` where `note_class IN ('hx_phys','clinic_consult','intake_note','genetics_consult')` AND patient has NULL `pmhx_nlp_family_hx_thyroid`. Cap at 4 notes/pt.
- **Prompt:** `prompts/v2/extract_pmhx_family_hx_v2.md` — output: present/absent/unknown × {thyroid_ca, mtc_specific, FMTC, MEN2A, MEN2B, FAP, Cowden, Carney}.
- **Output:** Append to `main.note_entities_llm_pmhx` with `category='family_hx'`.
- **Followup mig:** mig_272c (rebuild `pmhx_nlp_family_hx_thyroid` + `pmhx_nlp_family_hx_cancer` on CPM; expected coverage 4% → ≥ 60%).

---

## §4 — Apply (after Chat sign-off + 5-row sample diff approval)

### §4a — Pre-snapshot

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.note_entities_llm_invasion_v2_pre_mig272_20260503 AS
  SELECT * FROM main.note_entities_llm_invasion_v2;
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.note_entities_llm_pmhx_pre_mig272_20260503 AS
  SELECT * FROM main.note_entities_llm_pmhx;
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_nlp_cols_pre_mig272_20260503 AS
  SELECT research_id,
         vascular_invasion_final, pmhx_nlp_smoking_status, pmhx_nlp_family_hx_thyroid,
         pmhx_nlp_family_hx_cancer
  FROM main.canonical_patient_master;
```

### §4b — Lane A (vasc invasion) — run on H200 via researchflow-servers

Ssh to V1, sync prompts + notes-slice, run `scripts/run_extraction_qwen3_32b.sh --slice=invasion_residual_20260503 --notes=10000`. Expected throughput: ~3-5 notes/min on qwen3:32b → 30-50 hours? Cut to ~1,500 highest-yield notes (path_synoptic only, NULL VI patients).

Actually — **revise:** scope tighter. Run on highest-yield slice first (path_synoptic + path_dissection + op_note, NULL VI patients = ~3,000 notes). If yield is good (>50% extract success), expand.

### §4c — Lanes B + C (smoking / family hx) — run on H200 in parallel

Same ssh path; smaller per-note prompt → expect 8-12 notes/min. Total ~50-60 hours wall-clock parallel.

### §4d — Append to canonical NLP tables

Once extraction CSVs land in `data/extracted/`:
```sql
INSERT INTO main.note_entities_llm_invasion_v2 BY NAME
SELECT * FROM read_csv_auto('data/extracted/invasion_residual_20260503.csv');

INSERT INTO main.note_entities_llm_pmhx BY NAME
SELECT * FROM read_csv_auto('data/extracted/pmhx_smoking_20260503.csv')
UNION ALL
SELECT * FROM read_csv_auto('data/extracted/pmhx_family_hx_20260503.csv');
```

### §4e — Followup migs (mig_272a/b/c) for canonical rebuild

Defer to separate dispatches once raw extractions land. Each followup is a standard `canonical_*` rebuild + CPM repoint per mig_179 / mig_265 patterns.

### §4f — Registry signoff

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_272', CURRENT_TIMESTAMP, 'cursor_composer_mig272',
 'mig_272: NLP refresh batch - appended ~Nv vasc + Ns smoking + Nf family_hx LLM extractions to note_entities_llm_invasion_v2 + note_entities_llm_pmhx. llm_model=qwen3:32b_h200_20260503. Pending mig_272a/b/c canonical rebuild + CPM repoint. Closes CF-VASC-INVASION-749-UNDERFIRES + CF-SMOKING-COVERAGE-GAP + CF-FAMILY-HX-COVERAGE-GAP at the EXTRACTION level; canonical-level closes after 272a/b/c.');
```

---

## §5 — Acceptance criteria

1. Lane A yields ≥ 600 new VI-positive identifications (post-mig_272a).
2. Lane B drives `pmhx_nlp_smoking_status` coverage from 0.3% → ≥ 70% on malig (post-mig_272b).
3. Lane C drives `pmhx_nlp_family_hx_thyroid` coverage from 4% → ≥ 60% on malig (post-mig_272c).
4. M044 Cox PH multivariable with smoking added shows no >50% effect change on primary ETE→survival HR (sensitivity check).
5. M032 Table 1 smoking row populates with realistic ~25-30% ever-smoker prevalence (US thyroid cancer literature benchmark).

---

## §6 — Carry-forwards

| ID | Status | Notes |
|---|---|---|
| CF-VASC-INVASION-749-UNDERFIRES | **EXTRACTION CLOSED on apply** | Canonical close after mig_272a |
| CF-SMOKING-COVERAGE-GAP | **EXTRACTION CLOSED on apply** | Canonical close after mig_272b |
| CF-FAMILY-HX-COVERAGE-GAP | **EXTRACTION CLOSED on apply** | Canonical close after mig_272c |
| CF-mig272-LLM-COST | **OPEN** | Track Vast.ai $ cost; if > $40 surface to Logan before completing |
| CF-mig272-PROMPT-DRIFT | **OPEN** | Confirm vasc prompt v2 matches feedback_invasion_orphan_clinical_rules.md before run |

---

## §7 — Surgical git add

```
qc_framework_v1/migrations/272_nlp_refresh_batch_20260503.sql
prompts/v2/extract_vascular_invasion_v2.md  (if revised)
prompts/v2/extract_pmhx_smoking_v2.md       (if new)
prompts/v2/extract_pmhx_family_hx_v2.md     (if new)
scripts/run_extraction_qwen3_32b.sh         (if revised)
scripts/output/mig_272_apply_log.txt
scripts/output/mig_272_lane_a_yield_report.txt
scripts/output/mig_272_lane_b_yield_report.txt
scripts/output/mig_272_lane_c_yield_report.txt
cursor_prompts/CURSOR_PROMPT_MIG_272_NLP_REFRESH_BATCH_20260503.md
```

Commit message:
```
feat(nlp): mig_272 NLP refresh batch — vasc invasion + smoking + family hx

- Lane A: ~3,000 notes re-extracted for vascular invasion (path_synoptic/dissection/op_note slices, NULL-VI patients)
- Lane B: ~21,000 notes re-extracted for smoking status (PMH slice, NULL-smoking patients)
- Lane C: ~12,000 notes re-extracted for family hx thyroid (PMH/genetics slice, NULL-fhx patients)
- llm_model=qwen3:32b_h200_20260503 (Vast.ai)
- Closes CF-VASC-INVASION-749-UNDERFIRES + CF-SMOKING-COVERAGE-GAP + CF-FAMILY-HX-COVERAGE-GAP at extraction level
- Followup mig_272a/b/c will rebuild canonicals + CPM repoint
```

---

**End of mig_272 dispatch.**
