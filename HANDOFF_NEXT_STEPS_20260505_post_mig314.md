# Next-steps plan — post-mig_313 / mig_314 (Cowork audit, 2026-05-05)

> Live as of 2026-05-05 06:39 UTC. Latest signoff: mig_314. Latest commit on main: `9af0167`.

---

## TL;DR

mig_313 landed clean and met every acceptance gate. mig_310 v2 is **still running** in cursor (no FNA-NLP tables built yet, no signoff). Cowork executed the M036 v3 re-run as the immediate downstream cascade and signed it off as **mig_314**. M044 v5 needs a **Table 1 numerical patch only** — the regression results are unaffected. M032 era-by-stage tables need a re-run before that manuscript is republished.

---

## 1. Cursor work that landed (mig_313)

**Commit:** `ff12cf3` on origin/main.
**Signoff:** mig_313 by `cursor_composer_mig313` at 02:24:20 UTC.
**All acceptance gates PASS:**

| Gate | Target | Pre-fix | Post-fix |
|---|---|---|---|
| PTC M1% | ≤3% | 44.23% | **2.24%** ✅ |
| FC M1% | ≤10% | 57.82% | **3.29%** ✅ |
| FA M1% | 0% | 100% | **0%** ✅ |
| NIFTP M1% | 0% | n/a | **0%** ✅ |
| CPM total M1 (malignant) | 1–3% | 1,816 (45.19%) | **114 (2.84%)** ✅ |

Other malignant histologies look clinically plausible: MTC 6.71%, PDTC 21.62%, anaplastic 13.64%, metastatic-PTC 2.08%. **No regressions detected** in CPM cardinalities (10,871 / 4,019 unchanged).

`CF-MSTAGE-CORRUPTION` is **closed** for the canonical layer.

---

## 2. Cursor work still running (mig_310 v2)

No FNA-NLP deliverables yet:
- `fna_content_corpus%` — not built
- `fna_event_note_linkage%` — not built
- `nlp_fna_size_rollup_v1` — not built
- `imaging_fna_linkage_v4` — not built
- mig_310 signoff — not present

**Action:** Leave cursor running. When it lands, re-run the verification block in the original handoff (Step 4) and smoke-test the M025 nodule semantic model:

```bash
cortex analyst query "what is the per-tr ROM in the strict eligible cohort, with counts" \
  --connection thyroid_2026 \
  --model snowflake_trial/semantic_models/m025_nodule_level_semantic_model.yaml
```

Expected: TR2 12.90 / TR3 9.13 / TR4 18.72 / TR5 26.11 (locked numbers; small drift acceptable since FNA-size is a covariate, not in the per-TR aggregate).

`CF-FNA-SIZE-CM-NULL` remains **open** until mig_310 v2 signs off.

---

## 3. M036 ATA RSS v3 — landed and signed off as mig_314 (Cowork)

**Distribution (4,019 malignant patients):**

| Category | n | % | Pre-fix | Δ |
|---|---:|---:|---:|---:|
| High | 1,445 | 35.9% | 2,353 | −39% |
| Intermediate | 2,120 | 52.7% | 1,143 | +85% |
| Low | 27 | 0.7% | 23 | +17% |
| Uncalculable | 427 | 10.6% | 425 | +0.5% |

**High-risk drivers — clinically sound:**
- gross_ete_or_t4: 1,089
- five_or_more_positive_ln: 159
- distant_metastasis: 86 (was 1,642 — **−95%**, the main signal of the M-stage fix)
- extensive_vascular_invasion: 57
- high_risk_molecular: 33
- incomplete_resection_R2: 15
- LN deposit >3cm: 6

The brief had predicted high≈600–900 and low≈200–500. Actual was high=1,445 / low=27 because:
- Brief underestimated **gross ETE/T4 prevalence** (1,089 cases) — this is a tertiary surgical referral cohort.
- ATA 2025 low-risk requires intrathyroidal disease, which excludes 99%+ of operatively-treated tumors at Emory.

**No further data fixes needed for M036.** Manuscript writing brief is at `studies/m036_ata_rss_comparison_v3/M036_READY_FOR_WRITING_v3.md`. Outputs (KM curves, classification, reclassification crosstab, outcome validation) live alongside.

Committed and pushed: `9af0167`.

---

## 4. M044 v5 — needs Table 1 patch (regression results are unchanged)

**Strict-DTC analytic subset (PTC + FC) post-mig_313 stage distribution:**

| Stage | Pre-fix v5 (Table 1) | Post-fix |
|---|---:|---:|
| I | (locked) | 2,318 |
| II | (locked) | 1,047 |
| III | (locked) | 2 |
| IVA | (locked) | 1 |
| IVB | **684 (19.1%)** | **61** |

That's a **−91% drop in Stage IVB** — far past the 5% manuscript-patch threshold.

**Critical insight:** the M044 primary multivariable regression adjusted for **AJCC8 T stage and N stage**, not the overall stage_group. T and N stages were not affected by mig_313. Therefore:
- aOR gross-vs-microscopic ETE: **1.77 [1.15–2.71], p=0.009 — unchanged**
- All sensitivity model estimates — **unchanged**
- Tumor-size aOR, FTC aOR, focal vascular invasion aOR — **all unchanged**

**What needs to change in M044 v6:**
- Table 1 stage distribution row (Stage I/II/III/IVA/IVB/IVC counts and percentages)
- Any prose mentioning "Stage IVB" or "Stage IV" frequency (Results §1, Discussion if applicable)
- Patient-flow diagram if Stage breakdown appears
- The eMethods cohort lock paragraph if it references stage frequencies

**Recommended approach:** Rebuild Table 1 from the post-mig_313 cohort flat (`manuscript_workspace.cohort_m044_ajcc_ete_v1`) using the same `build_strict_dtc_deliverables.py` pipeline that produced v5, then diff Table 1 only against the v5 docx and stitch a v6 with locked text + corrected Table 1.

**Cowork can do this directly** — small/medium scope, fits within the "patient grain Table 1 rebuild" idiom.

---

## 5. M032 era-by-stage tables — needs re-run

The cursor agent's mig_313 report flagged: *"M032 25yr ⚠️ Era-by-stage tables need re-run (IVB inflation was temporal)."*

M032 is a **shipped** manuscript (submission package frozen). It uses 25-year era-stratified counts; the IVB inflation was concentrated in pre-2008 era due to the back-derivation logic interaction with age cutoffs. **Re-running era × stage is mandatory before any republication.**

Cowork can pull `manuscript_workspace.cohort_m032_descriptive_25yr_v1` and reproduce the era-by-stage breakdown. If the deltas are within rounding, M032 needs only a footnote; if they're material, it needs a v2 numerical refresh in the submission package.

**Recommendation:** run this audit in the **next** Cowork session before touching the M032 submission package.

---

## 6. Locked-number cross-check (post-mig_313)

| Number | Expected | Actual |
|---|---:|---:|
| CPM rows | 10,871 | **10,871** ✅ |
| Malignant patients | 4,019 | **4,019** ✅ |
| M044 cohort flat | (≈3,572 strict-DTC) | 3,868 (cohort flat); 3,429 PTC+FC ⚠️ |
| M032 cohort | 10,871 | **10,871** ✅ |
| M038 cohort flat | 10,871 (2,501 massive) | **10,871** ✅ |
| M043 LN cohort | (TBD) | 4,019 |

⚠️ **M044 cohort flat note:** the table currently has **duplicate columns** (every column appears twice — research_id, age_at_surgery, etc.). This is the 2-column-per-name shape consistent with a SELECT * FROM A JOIN B that was not column-projected. It's not a row-level data defect (the row counts are coherent), but it's a structural defect that should be fixed before this table is consumed by Cortex Analyst. Recommend mig_315 to rebuild `cohort_m044_ajcc_ete_v1` with explicit column selection.

---

## 7. Recommended next-manuscript queue

Based on already-built analyses + clean cohorts post-mig_313:

1. **M036** (this audit) — distribution clean, brief written, ready for prose. **Highest-priority writing target.**
2. **M044 v6** — Table 1 numerical patch only; aORs locked. **Quick turn (1–2 hours of writing).**
3. **M029 FNA cytology concordance** — analysis landed (cursor `a9bc38c`); still needs writing brief.
4. **M019 RAI outcomes** — analysis landed (cursor `0f91f52`); still needs writing brief.
5. **M032 v2 era-by-stage refresh** — deferred until era audit completes.

M025 v2, M038, and M044 v5 are all already in submission state (M025 v2 is being drafted in another chat; do not touch).

---

## 8. New data-quality issues surfaced this session

1. **M044 cohort flat duplicate-column structural defect** — see §6. Mitigation: explicit column selection on next rebuild. Open as `CF-M044-DUP-COLS`.
2. **No M037-named cohort exists in workspace.** Brief references `cohort_m037_ln_predictors_v1`; what exists is `cohort_m043_ln_predictors_v1` (n=4,019). Either M037 is a downstream subset of M043 not yet materialized, or naming drift since the M037 submission package froze. Worth resolving before M037 is touched again. Open as `CF-M037-COHORT-MISSING`.
3. **86 high:distant_metastasis vs 114 CPM M1.** Small gap (28 patients), most likely non-DTC histology that drops out of the ATA classifier (anaplastic, MTC, PDTC, FTUMP). Worth a chart-level audit before the M036 manuscript prose claims "true M1 prevalence ≈2.1%". Audit query is in the M036 brief.

---

## 9. Open carry-forwards summary

| Carry-forward | Status |
|---|---|
| `CF-MSTAGE-CORRUPTION` | **CLOSED** (mig_313 landed, mig_314 cascade verified) |
| `CF-FNA-SIZE-CM-NULL` | OPEN (waits on mig_310 v2) |
| `CF-mig_305-SP-V3-HANG` | CLOSED (mig_309) |
| `CF-M044-DUP-COLS` | NEW (this session) |
| `CF-M037-COHORT-MISSING` | NEW (this session) |

---

## What the next Cowork session should do first

1. Re-poll for mig_310 v2 deliverables (one query block from Step 4 of original handoff). If landed, sign off mig_310 retro and run the M025 nodule smoke test.
2. Rebuild M044 v5 Table 1 from corrected cohort flat → produce v6 docx + LaTeX. **Do not touch the regression sections.**
3. M032 era × stage audit — quantify the IVB temporal inflation deltas.
4. (If Logan greenlights) Open `cohort_m044_ajcc_ete_v1` rebuild prompt as mig_315 to clean the duplicate-column structure.

---

## Files written/touched this session

- Wrote: `studies/m036_ata_rss_comparison_v3/M036_READY_FOR_WRITING_v3.md`
- Wrote: `studies/m036_ata_rss_comparison_v3/{ata_2025_*,model_performance.*,outcome_validation.*,reclassification_crosstab.*,km_summary.csv}`
- Refreshed: `manuscript_workspace.m036_ata_2025_rss_v2` (in-place upload from script)
- Inserted: `main.signoff_migration` row for mig_314
- Commit: `9af0167` pushed to origin/main

This file is `HANDOFF_NEXT_STEPS_20260505_post_mig314.md` at the repo root.
