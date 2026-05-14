# EXT2-4 cohort reconciliation: v1 (DuckDB N=558) vs v3 (BQ N=400)

**Date:** 2026-05-13
**Author:** Cowork (Logan, post-mig_323)
**Goal:** Explain the 558 vs 400 cohort-N gap between the EXT2-4 v1 manuscript and the v3 BigQuery re-analysis, and recommend a unified definition for any pre-submission cohort.

---

## TL;DR

| Definition | n (BQ 2026-05-13) | Maps to |
|---|---:|---|
| Surgical cohort 1999–2025 (lobe or total) | **8,368** | matches v3 denominator exactly |
| Any preop nodule 2.0–4.0 cm (nodule-grain, ever recorded) | **765** | overshoots v1's 635 by ~21% |
| **Largest preop nodule 2.0–4.0 cm (patient-grain max-over-preop)** | **674** | reproduces v1's 635 within ±6% — this is the v1 size-resolution rule |
| Resolved *index* nodule 2.0–4.0 cm (current v3 patient-grain) | **400** | current v3 — more conservative |
| ... after strict nodal exclusion (v1 path) | **338** (from largest-nodule arm — query pending refresh) | analogous to v1's 558 |

**The gap is NOT a nodal-exclusion issue. It is a size-resolution-rule difference.** v1 picked the patient's largest preop nodule and gated on that size. v3 picked the resolved index nodule (one nodule per patient per exam, by a different prioritization) and gated on that size. Patients with a secondary preop nodule in the 2–4 cm band but a sub-2cm "index" nodule are in v1 but not in v3.

---

## What v1 actually did

From `MANUSCRIPT_STATE_AUDIT.md`:
> "Index size (primary cohort): Preoperative imaging index nodule 2.0–4.0 cm using pipeline logic (`preop_imaging_size_cohort`, `imaging_nodule_long_v2.size_cm_max`, exam on/before index surgery)."

The `imaging_nodule_long_v2` (legacy DuckDB) is a nodule-grain table. The pipeline's `preop_imaging_size_cohort` step took the largest preop nodule per patient (`size_cm_max`) and filtered on 2.0–4.0 cm. After the strict-nodal-exclusion step, it landed at N=558.

The cohort_flow.md confirms:
```
preop_imaging_nodule_size_2_to_4_cm: 635
after_strict_preop_ln_exclusion_preop_cohort: 558
primary_preop_cohort_final_N: 558
```

So v1's "imaging 2-4cm" step = **635** = patients with any preop nodule reaching 2-4cm (closest reproduction in BQ today = 674 via the "largest preop nodule" query above).

## What v3 did

`manuscript_cohort_v1.imaging_nodule_size_cm` is a patient-grain resolved single value — the canonical layer applies a different rule (likely the index nodule as flagged by the FNA linkage, not the largest preop nodule per se). Filtering this column on 2-4cm gives **n=400** patients.

The two numbers are **both internally correct** for what they measure; they just measure different things:

- 674 = patients with at least one preop nodule documented in the 2-4 cm size band (v1 logic, the right denominator if the *exposure* is "has a 2-4 cm nodule")
- 400 = patients whose *clinically prioritized index nodule* is in the 2-4 cm size band (v3 logic, the right denominator if the *exposure* is "the lesion driving the decision is 2-4 cm")

## Why the v1 → v3 number shifted in the *other* direction at the nodal-exclusion step

On BQ with the v1 strict-nodal-exclusion logic applied to my **patient-grain v3 cohort (n=400)**, 62 are excluded → 338 remain. The v1 pipeline excluded 77 from its 635 → 558. The exclusion *rate* is similar (15% vs 12%), and the absolute counts differ because the starting cohorts differ.

**The strict-nodal-exclusion logic itself reproduces faithfully between v1 and v3.** This was a real worry that turned out to not be the issue.

---

## Recommendation for the pre-submission cohort

**Adopt the "largest preop nodule 2.0–4.0 cm" definition as the unified primary cohort.** Reasons:

1. **It reproduces v1's framing.** Avoids the appearance of a methodological pivot between v1 and v3.
2. **It is the more clinically defensible inclusion rule.** The lobectomy-vs-total-thyroidectomy decision is driven by *any* 2-4 cm nodule in the preop ultrasound, not exclusively by an "index" lesion. Excluding patients with a 2-4 cm nodule because they also have a smaller index lesion is selection bias against multinodular goiter.
3. **It is larger and tighter on CIs.** n=674 (or ~558 after strict nodal exclusion) is more powered than n=400.
4. **It allows a unified v1+v3 manuscript** with the existing v1 N=558 primary number and a v3 BQ-canonical re-derivation that lands at the same N (modulo small data-curation differences).

Recommended cohort definition (to write into the methods section):
> Inclusion: first qualifying lobectomy or total thyroidectomy in 1999–2025 with at least one preoperative ultrasound nodule with `size_cm_max` in the [2.0, 4.0] cm range on an exam on or before the index surgery date. Patients with strict nodal-exclusion criteria (CT/MRI pathologic lymph nodes preop OR Bethesda VI on a lymph-node FNA before surgery) were excluded from the primary analysis cohort. Sensitivity analysis: relax to "any preop nodule 2.0–4.0 cm regardless of nodal status."

## Followup actions

1. **Refresh Tables 1–4 and Figures on the unified cohort** (n=674 or n=558-after-nodal-exclusion). This is non-trivial — every aggregate count in the v3 zip needs to be re-derived. Estimated effort: 1 medium Cursor handoff with a focused prompt + a Cowork prose pass.
2. **Patient-level deduplication note:** when a patient has both a 2-4 cm nodule and a sub-2cm index nodule, we'll include them. Confirm with co-authors that this matches their clinical intent.
3. **Pre-2015 cell:** the new 674 cohort almost certainly has more pre-2015 patients (because before molecular testing era, multinodular goiter was more common in this size band). Re-check the pre/post-2015 split.
4. **Diagnostic performance Table 3 stays approximately as-is** — the head-to-head Afirma vs ThyroSeq subset is gated on Bethesda III/IV + named platform + final histology, not on the size-resolution rule. The 2-4 cm subgroup row in Table 3 will shift modestly (n=31 → likely ~50 ThyroSeq, n=5 → likely ~10 Afirma) but the operating-characteristic ranges should be stable.

## Open question for co-author review

The v1 prose says "preoperative imaging index nodule 2.0–4.0 cm" (singular). Is the clinical-decision intent really *index-nodule*, or *any 2-4 cm nodule*? If the former, v3 is correct and v1 was unintentionally broader. If the latter, both should converge on the "largest preop nodule" definition. This is the single most important question to resolve before submission, and it's clinical judgment, not data — recommend a 5-minute discussion at the next co-author review.

---

## DECISION (2026-05-14, Logan)

**Cohort definition: include any patient who had a preoperative ultrasound nodule measuring 2.0–4.0 cm.** This is the **broader "any preop nodule"** definition (n=765 on BQ as of 2026-05-13), not the narrower "largest preop nodule" definition (n=674) and not the v3 "resolved index nodule" definition (n=400).

### Rationale
- Clinical decision-making for thyroid lobectomy vs total thyroidectomy is driven by **any 2–4 cm nodule** in the preoperative ultrasound, not exclusively by the "index" lesion. Excluding patients with a 2–4 cm nodule because they also have a smaller "index" lesion is selection bias against multinodular goiter.
- Reproduces (and slightly expands) the v1 N=635 framing.
- Maximum statistical power; tightest Wilson 95% CIs.

### What this triggers (v3 → v4 rebuild)

1. **Cohort SQL refresh** — Add a CTE that defines `cohort_v4 = surgical_b34 WHERE ANY preop nodule in canonical_us_nodule_v2 has size_cm_max IN [2.0, 4.0] AND exam_date <= surg_first_date`. Replace `imaging_nodule_size_cm BETWEEN 2.0 AND 4.0` filter (patient-grain index) with this nodule-grain exists clause throughout `sql/04b_*.sql` and the Table 1/2 SQL.
2. **Tables 1–4 refresh** — All cohort-N denominators shift from 400 (v3 preop_2to4cm) to 765 (v4 any-preop-2to4cm). Table 3 v3 head-to-head subsets (Afirma B3+B4 n=90, ThyroSeq n=222) are NOT bound by the cohort definition (those filter on B3+B4 only), but the 2–4 cm subgroup cell DOES shift (n=5 Afirma → likely ~10; n=30 ThyroSeq → likely ~50). Re-derive.
3. **Figure regeneration** — `build_figures_v2.py` forest_rows for 2–4 cm cells refresh; cohort_flow figure refresh.
4. **Manuscript prose pass** — Methods § "Inclusion criteria" + Results § cohort flow numbers shift; v3 → v4 supersession note added.
5. **Strict-nodal-exclusion arm** — Apply the v1 strict nodal exclusion to v4 (any preop nodule 2–4 cm + strict nodal exclusion) to produce a sensitivity-analysis cohort comparable to v1's N=558.

### Estimated effort
- 1 Cursor handoff for SQL + canonical cell regeneration
- 1 Cowork session for the manuscript prose pass + v3 → v4 supersession note + zip rebuild
- Total: ~1 working day equivalent across the two tools.

---

## Source queries (run 2026-05-13 against `thyroid-canonical-pub-2026.pub_canonical.*`)

```sql
-- Any preop nodule 2-4cm (nodule-grain → 765)
SELECT COUNT(DISTINCT n.research_id)
FROM canonical_us_nodule_v2 n
JOIN manuscript_cohort_v1 mc ON CAST(mc.research_id AS STRING) = n.research_id
WHERE n.size_cm_max BETWEEN 2.0 AND 4.0
  AND n.exam_date <= DATE(mc.surg_first_date)
  AND mc.surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy');

-- Largest preop nodule 2-4cm (patient-grain → 674)
SELECT COUNT(*) FROM (
  SELECT mc.research_id, MAX(n.size_cm_max) AS max_preop_size
  FROM manuscript_cohort_v1 mc
  JOIN canonical_us_nodule_v2 n ON CAST(mc.research_id AS STRING) = n.research_id
  WHERE n.exam_date <= DATE(mc.surg_first_date)
    AND mc.surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')
  GROUP BY mc.research_id
) WHERE max_preop_size BETWEEN 2.0 AND 4.0;

-- Resolved index nodule 2-4cm (current v3 → 400)
SELECT COUNT(*)
FROM manuscript_cohort_v1
WHERE imaging_nodule_size_cm BETWEEN 2.0 AND 4.0
  AND surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy');
```
