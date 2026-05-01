# Cursor Composer Dispatch — mig_276: M038 Massive Goiter manuscript draft scaffold

**Generated:** 2026-05-02 by Cowork.
**Lane:** mig_276 — Generate the methods + results sections of the M038 Massive Goiter Definition Paper using the Snowflake outputs already produced (Table 1 + cohort scaffold). Logan does the intro + discussion; Cowork+Cursor draft the methodologically-defensible parts.
**Recommended agent:** **Cursor Chat (Claude Sonnet 4 / GPT-5)** for prose drafting → **Cursor Composer** to commit.
**Estimated runtime:** 60–90 min
**Triggered by:** Manuscript completion roadmap.
**Severity:** MED.

---

## §0 — First message to paste into Cursor Chat

> mig_276 dispatch. Draft the Methods and Results sections of M038_massive_goiter_manuscript_draft_v1.md based on the Snowflake outputs in `snowflake_trial/reports/m038_*.md` and per the manuscript completion roadmap. Use academic surgical-oncology voice; cite specific n's and p-values from the data. Logan handles intro + discussion. Output: a markdown file under `manuscript_outputs/v1_0_20260501/` or matching directory.

---

## §1 — Inputs available

```
snowflake_trial/reports/m038_table1_massive_goiter.md      — Table 1: cohort × weight strata
qc_framework_v1/migrations/273_cohort_m038_view_20260502.sql — Cohort view definition (post mig_273)
THYROID_VALIDATION.PUBLIC.COHORT_M038_MASSIVE_GOITER       — Snowflake cohort view
main.cohort_m038_massive_goiter_v1                         — MotherDuck mirror (post mig_273)
manuscript_workspace.cohort_m038_massive_goiter_v1         — Lane-M extended definition (older)
```

When mig_252 lands (in your queue), strict complications by weight strata becomes available.
When mig_275 lands, surgical-complexity proxies (op time, EBL, LOS) become available.
mig_276 can scaffold the draft NOW with placeholders for those numbers.

## §2 — Draft scaffold (prose template)

### Methods (Cowork-draftable)

> **Cohort definition.** We identified all patients in the canonical thyroid surgery cohort (1999-2025; n=10,871) with non-null gland weight on final pathology (n=9,130; 84.0%). Massive goiter was defined as final gland weight ≥200 g per established surgical literature [REF] and our institutional convention (mig_273); patients were stratified into massive (≥200 g; n=475), moderate (50-199 g; n=2,467), and small (<50 g; n=6,188) strata. Patients with missing weight (n=1,741; 16.0%) were excluded from primary analysis and addressed in Supplementary Materials.
>
> **Outcomes.** The primary outcome was strict-definition any-complication, defined per the institutional canonical complications pipeline (mig_252) requiring `finding_status = 'present'` AND `evidence_strength IN ('definitive','probable')` in the source pathology and operative-note NLP entities. Secondary outcomes included individual complication types (chyle leak, seroma, hematoma, hypocalcemia, hypoparathyroidism, recurrent laryngeal nerve injury, vocal cord paralysis), surgical complexity (operative time, estimated blood loss, length of stay; per mig_275), and 5-year recurrence-free survival among malignant patients.
>
> **Statistical analysis.** Continuous variables compared via Kruskal-Wallis; categorical via chi-square. Multivariable logistic regression for any-strict-complication used massive goiter as the primary exposure, adjusted for age, sex, gland multifocality, malignancy, and surgery type. All analyses run in Snowflake Cortex pipeline; reports archived at `snowflake_trial/reports/m038_*`.

### Results (Cowork-draftable from Table 1)

> **Cohort characteristics.** Among 9,130 patients with documented gland weight, 475 (5.2%) met the massive goiter threshold (≥200 g; median weight 272 g, range 200-2,320 g). [Continue with demographics from `m038_table1_massive_goiter.md`.]
>
> **Surgical complexity.** [Placeholder — fill from mig_275 outputs]
>
> **Complications.** Per the strict definition, [N (X%)] of massive goiter patients experienced any complication, vs [N (Y%)] in the moderate and [N (Z%)] in the small weight strata (p<0.0001 chi-square). [Specific complication types if mig_252 lands.]

## §3 — Files to produce

```
manuscript_outputs/v1_0_20260501/M038_massive_goiter_manuscript_draft_v1.md
```

Or update existing draft if one exists:
```bash
find . -iname "M038*" 2>/dev/null
```

## §4 — Manuscript footnotes (per mig_266 conventions)

Include the round-6 footnotes that apply to M038:
- F2: AJCC stage IVA/IVC collapse
- F4: LN suspicious flag rebuild (mig_262)
- F5: Bethesda 2 enrichment (relevant if Table 1 includes cytology)
- F6: NLP coverage limitations (smoking/family-hx) — esp. relevant if M038 stratifies by these

## §5 — Surgical git add
```
manuscript_outputs/v1_0_20260501/M038_massive_goiter_manuscript_draft_v1.md
```
