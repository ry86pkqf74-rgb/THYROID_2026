# Extraction Inventory 2026 — Full Ranked Entity Audit

**Generated**: 2026-03-12  
**Source**: `note_entities_complications` on MotherDuck `thyroid_research_2026`  
**Purpose**: Phase 2 QA audit — establish baseline precision/recall estimates before refinement

---

## Executive Summary

**Critical finding**: Every complication entity extracted by the NLP pipeline suffers from the same boilerplate contamination pattern discovered in the RLN injury audit (96.5% false-positive rate). The primary sources are:

1. **Standardized surgical consent template** — all H&P notes contain a fixed risk-disclosure block:
   > *"...scarring, hypocalcemia, hoarseness, chyle leak, seroma, numbness, orodental trauma, fistula..."*
   This single template accounts for **all 645 h_p chyle_leak mentions**, **all 686 h_p seroma mentions**, and **1,803 of 2,740 (66%) hypocalcemia mentions**.

2. **Op-note Valsalva/hemostasis check phrase** — the operative template includes:
   > *"Valsalva to 20–30 cm H₂O was performed to confirm hemostasis and lack of a chyle leak."*
   This single phrase accounts for approximately **2,300+ of 2,316 op_note chyle_leak mentions** (100% of the 100-sample checked). The regex matches "chyle leak" but the clinical meaning is "confirmed absence of chyle leak."

3. **SSI abbreviation collision** — "SSI" in `wound_infection` extraction matches **sliding scale insulin** (SSI) in diabetic management notes, not surgical site infections.

**Bottom line**: The `nlp_*` complication flags in the H1/H2 models are unreliable as currently extracted. Re-running the models after refinement is required.

---

## Entity Inventory — Ranked by Risk

| Rank | Entity | Total Mentions | Distinct Patients | Present | Negated | Same-Day % | H&P % | Op-Note % | Estimated Precision | Risk Level | Model Impact |
|------|--------|---------------|-------------------|---------|---------|------------|-------|-----------|--------------------|-----------| -------------|
| 1 | chyle_leak | 3,023 | 1,588 | 2,988 | 35 | 27.0% | 21.6% | 77.5% | **~2%** | CRITICAL | H1, H2 |
| 2 | hypocalcemia | 2,806 | 1,877 | 2,740 | 66 | 13.6% | 65.8% | 23.8% | **~15–20%** | CRITICAL | H1, H2, advanced_features |
| 3 | seroma | 1,353 | 846 | 1,351 | 2 | 5.0% | 50.8% | 47.3% | **~5%** | CRITICAL | H1, H2 |
| 4 | rln_injury | 975 | 655 | 973 | 2 | 3.2% | 97.6% | 2.1% | **DONE** | — | Refined to 92 pts |
| 5 | hypoparathyroidism | 550 | 430 | 540 | 10 | 3.6% | 76.1% | 2.4% | **~35–45%** | HIGH | H1, H2 |
| 6 | hematoma | 403 | 225 | 274 | 129 | 12.2% | 44.9% | 30.7% | **~20–30%** | HIGH | H1, H2 |
| 7 | vocal_cord_paralysis | 134 | 88 | 119 | 15 | 14.9% | 65.5% | 21.8% | **DONE** | — | Grouped with RLN |
| 8 | vocal_cord_paresis | 96 | 71 | 61 | 35 | 12.5% | 49.2% | 32.8% | **DONE** | — | Grouped with RLN |
| 9 | wound_infection | 19 | 16 | 18 | 1 | 5.3% | 61.1% | 5.6% | **~15–25%** | MEDIUM | H1 |

---

## Per-Entity Detailed Analysis

### 1. chyle_leak — CRITICAL (Estimated Precision: ~2%)

**Volume**: 2,988 present mentions across 1,576 distinct patients.  
This is the highest-volume entity and by far the most contaminated.

**False positive sources** (confirmed by 100-sample manual review):

| Source | Mechanism | Estimated % of mentions |
|--------|-----------|------------------------|
| Op_note "lack of a chyle leak" | Valsalva/hemostasis check: *"performed to confirm hemostasis and lack of a chyle leak"* | ~77% (2,316 op_note mentions) |
| H&P consent boilerplate | *"scarring, hypocalcemia, hoarseness, chyle leak, seroma..."* | ~22% (645 h_p mentions) |
| True events (chyle visible during dissection) | *"a small amount of chyle was seen during dissection"* | <1% |

**Negation miss**: The phrase "lack of a chyle leak" is NOT caught by the standard 40-char negation pre-window because "lack of" is not in the `NEGATION_CUES` list. The regex matches "chyle leak" but the clinical meaning is confirmed ABSENCE.

**Structured ground truth**: `complications` table has NO hypocalcemia/chyle_leak columns (only seroma/hematoma). No direct structured cross-check available.

**Impact on H1/H2**: `nlp_chyle_leak` flag = 1 for ~1,576 patients (14.5% of all patients) vs expected true rate of <2% based on published literature. After refinement, expected ~50–100 true cases.

**Required action**: Refinement required. Add "lack of" / "prevent" / "confirm no" to negation expansion. Filter h_p boilerplate template. Estimated post-refinement precision: 70–85%.

---

### 2. hypocalcemia — CRITICAL (Estimated Precision: ~15–20%)

**Volume**: 2,740 present mentions across 1,846 distinct patients.

**Note type breakdown**:
- h_p: 1,803 (65.8%) — ALL consent boilerplate
- op_note: 651 (23.8%) — mix of consent template embedded in op notes + monitoring language
- dc_sum: 183 (6.7%) — mixture: "signs and symptoms of hypocalcemia were reviewed" (educational) vs "developed hypocalcemia post surgery" (true event)
- endocrine_note: 52 (1.9%) — likely true events (follow-up management)
- other_history/ed_note: 44 (1.6%) — some true

**Example true positive (dc_sum)**:
> *"Mild post operative hypocalcemia (hypoparathyroidism) noted. It was managed with..."*

**Example false positive (dc_sum)**:
> *"Signs and symptoms of hypocalcemia were reviewed, and the patient was given instructions..."*

**Estimated precision breakdown**:
- h_p: ~0% (consent boilerplate)
- op_note: ~5–10% (embedded consent + monitoring)
- dc_sum: ~30–50% (mixed education vs true events)
- endocrine_note: ~70–80% (follow-up management context)

**Overall estimated precision**: ~15–20%

**Structured cross-check**: `complications.hypocalcemia` is ALL NULL (10,864 rows) — structured data was never populated for this field.

**Impact on H1/H2**: `nlp_hypocalcemia` = 1 for ~1,846 patients (17% of all patients). True rate expected ~5–15% post-thyroidectomy.

**Required action**: Refinement required. Need to distinguish education/consent mentions from true post-op hypocalcemia.

---

### 3. seroma — CRITICAL (Estimated Precision: ~5%)

**Volume**: 1,351 present mentions across 845 distinct patients.

**False positive sources** (confirmed by sample review):

| Source | Mechanism | Estimated % |
|--------|-----------|-------------|
| H&P consent boilerplate | *"...chyle leak, seroma, numbness..."* | ~51% (686 h_p mentions) |
| Op-note embedded consent | Same template appears verbatim in op-note text | ~47% (640 op_note mentions, 3/3 sample confirmed boilerplate) |
| True events | Seroma documented in follow-up | <2% (from dc_sum: 18 mentions) |

**Structured cross-check**: `complications.seroma` = 'x' for 28 patients. ALL 28 structured-positive patients have ZERO NLP extractions. The NLP is capturing a completely different patient set from the true seroma cases. This is a critical finding — **the NLP is both capturing false positives (1,351 boilerplate mentions) and missing all 28 true structured-documented cases**.

**Required action**: Highest priority refinement. True seroma cases are in structured data; NLP is capturing only consent boilerplate.

---

### 4. hypoparathyroidism — HIGH (Estimated Precision: ~35–45%)

**Volume**: 540 present mentions across 430 distinct patients.

**Note type breakdown**:
- h_p: 411 (76.1%) — consent boilerplate (*"hypocalcemia and hypoparathyroidism - temporary or permanent"*)
- endocrine_note: 60 (11.1%) — likely true events (post-thyroidectomy follow-up management)
- other_history: 39 (7.2%) — mixed (PMH references may be true)
- op_note: 13 (2.4%) — likely risk discussions
- other_notes/dc_sum/history_summary: 17 (3.1%) — mixed

**Example true positive (endocrine_note)**:
> *"1. Stable, s/p total thyroidectomy for Graves disease. 2. Post-op hypoparathyroidism — PLAN: Recheck Ca, PTH"*

**Structured cross-check**: `complications.hypoparathyroidism` is ALL NULL — field never populated.

**Estimated precision**: ~35–45% (h_p pulls it down; endocrine/history are higher quality)

**Required action**: Moderate priority. Filter h_p boilerplate. Retain endocrine_note, other_history mentions.

---

### 5. hematoma — HIGH (Estimated Precision: ~20–30%)

**Volume**: 274 present mentions across 141 distinct patients. (129 negated — highest negation ratio at 32%, suggests some true negation is being caught.)

**Note type breakdown**:
- h_p: 123 (44.9%) — consent/risk boilerplate
- op_note: 84 (30.7%) — mixed: some risk discussions, some true events
- dc_sum: 38 (13.9%) — likely more true events
- history_summary: 10 (3.6%) — may be historical references
- endocrine/ed/other: 19 (6.9%) — mixed

**Example op_note true positives found in sample**:
- *"Neck hematoma evacuation"* (procedure listing — confirmed event)
- *"approximately 25 cc of old hematoma in the right resection bed"* (intraoperative finding)
- *"CT neck shows fluid... consistent with postop seroma or hematoma"* (diagnostic work-up)

**Example op_note false positives**:
- *"potential major risk were a post-operative hematoma to occur"* (risk discussion)
- *"including but not limited to... neck hematoma"* (consent list)

**Structured cross-check**: `complications.hematoma` = 'x' for 28 patients. ALL 28 structured-positive patients have ZERO NLP extractions (same pattern as seroma — different patient range).

**Required action**: Moderate priority. Filter consent/risk language. Retain procedure-listing and intraoperative-finding contexts.

---

### 6. wound_infection — MEDIUM (Estimated Precision: ~15–25%)

**Volume**: 18 present mentions across 16 patients.

**Critical false positive**: "SSI" abbreviation matches both:
- **Surgical Site Infection** (intended target)
- **Sliding Scale Insulin** (diabetes management notes — completely unrelated!)

From 18 mention sample:
- SSI = sliding scale insulin (at least 6 mentions in diabetic management context)
- "wound infection" in H&P risk boilerplate (3 mentions)
- Historical wound infection at another site (2 mentions)
- True thyroid surgical wound infection (at least 2 mentions confirmed)

**Required action**: Remove "SSI" abbreviation from `wound_infection` regex. Add temporal/surgical context filter.

---

## Clinical Events Pipeline (extracted_clinical_events_v4)

**Audit summary**: The Excel/clinical-notes-derived event pipeline has different characteristics than the NLP complication pipeline.

| Event Type | Subtype | N | Patients | Excel-Source % | Audit Priority |
|------------|---------|---|----------|----------------|----------------|
| treatment | recurrence | 6,405 | 4,278 | 0% | MEDIUM |
| lab | TSH | 3,196 | 1,784 | 0% | LOW |
| lab | thyroglobulin | 975 | 354 | 0% | MEDIUM |
| lab | calcium | 707 | 566 | 0% | LOW |
| medication | levothyroxine | 1,294 | 806 | 0% | LOW |
| treatment | RAI | 984 | 858 | 0% | LOW (v3 verified) |
| follow_up | extracted_date | 282 | 141 | 0% | LOW |

**Note**: All clinical event sources are named Excel columns (h_p_1, opnote_1, dc_sum_1, etc.), not raw Excel cell text blocks as originally hypothesized. The `from_excel_regex` flag = 0 for all because source_column values are column names, not the `raw_notes_v2_regex` identifier.

**Recurrence flag concern**: 6,405 recurrence events across 4,278 patients (39% of patients with at least one recurrence mention) seems high for thyroid cancer. This warrants audit — likely includes follow-up monitoring language (*"recurrence is expected to be low"*, *"no evidence of recurrence"*).

---

## Downstream Consumers of NLP Flags

| Consumer | File | Flags Used | Impact |
|----------|------|-----------|--------|
| H1 (CLN/Lobectomy) | `scripts/42_hypothesis1_cln_lobectomy.py` L125–136 | nlp_hypocalcemia, nlp_hypoparathyroidism, nlp_hematoma, nlp_seroma, nlp_chyle_leak, nlp_wound_infection | All 6 flags affected |
| H2 (Goiter/SDOH) | `scripts/43_hypothesis2_goiter_sdoh.py` | Same 6 + nlp_rln_injury, nlp_vocal_cord | All flags affected |
| H1/H2 Validation | `scripts/44_hypothesis_validation_extension.py` | Same pattern | All affected |
| advanced_features_v3 | `scripts/11_quality_assurance_crosscheck.py` L563–569 | Structured complications (NOT NLP) | Low impact — uses structured `complications` table |
| Complications Dashboard | `app/` | rln_injury refined, structured data | RLN refined; others unrefined |

---

## Structured Ground Truth Availability

| Entity | Structured Column | Populated? | Values |
|--------|------------------|-----------|--------|
| rln_injury | `complications.rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy` | Partial | 1,314 'x', 24 'yes', rest NULL |
| seroma | `complications.seroma` | Sparse | 28 'x', rest NULL |
| hematoma | `complications.hematoma` | Sparse | 28 'x', rest NULL |
| hypocalcemia | `complications.hypocalcemia` | Empty | ALL NULL |
| hypoparathyroidism | `complications.hypoparathyroidism` | Empty | ALL NULL |
| wound_infection | none | N/A | N/A |
| chyle_leak | none | N/A | N/A |

---

## Risk Assessment Summary

### Immediate Re-run Required After Refinement

- **chyle_leak** (estimated 1,576 → ~50 true patients): H1/H2 chyle_leak prevalence will drop from 14.5% to ~0.5%
- **seroma** (estimated 845 → ~50–100 true patients): H1/H2 seroma prevalence will drop from 7.8% to ~1%
- **hypocalcemia** (estimated 1,846 → ~400–600 true patients): H1/H2 hypocalcemia prevalence will drop from 17% to ~4–6%

### Model Integrity

The current H1/H2 `nlp_*` complication flags should **NOT be used in publication** until refined. The massive false-positive rates would inflate complication rates far above published benchmarks and confound any regression using these as outcomes.

### High-Confidence Flags (No Refinement Needed)

- **RLN injury** (already refined — 92 patients, 0.85% rate)
- **Structured complications** in `advanced_features_v3` (seroma/hematoma as 'x' markers, rln_injury 'yes')
- **Molecular mutation flags** (braf/ras/ret/tert from structured `tumor_pathology`)
- **AJCC staging** (from structured `path_synoptics`/`tumor_pathology`)
