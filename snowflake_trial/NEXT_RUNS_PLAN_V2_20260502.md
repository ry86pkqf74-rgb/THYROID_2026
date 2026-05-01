# Next Runs Plan v2 — 2026-05-02 (post round 7; mig_264b/266/267/268 in flight)

**Generated:** 2026-05-02 by Cowork
**HEAD:** `699ec0b` (round 7 close)
**Status:** 5/6 Cursor migs from round-6 closed (mig_260/261/262/263/265). 4 in-flight (mig_264b/266/267/268). 3 new manuscript outputs landed this session (M025 TIRADS, M032 BRAF year trend, M044 Cox PH + sensitivity).

---

## Round-8 Snowflake outputs (just executed, pushed in this commit)

### M025 TIRADS Performance Table (`reports/m025_tirads_performance.md`)
- **3,396 patients** with TIRADS categorization (1,548 malig / 1,848 benign)
- ROM by category: TR1 30.3% / TR2 33.7% / TR3 29.9% / TR4 49.3% / TR5 60.1%
- **Diagnostic performance at TR ≥ TR4** (canonical FNA decision threshold): Sens 0.702, Spec 0.561, PPV 0.573, NPV 0.692, LR+ 1.60, LR- 0.53
- **Operative cohort caveat**: every TR category is over-malignant vs ACR expected (TR1 30% vs <2%). Manuscript footnote already in the report; M025 + M027 cannot publish without this caveat.

### M032 BRAF year continuous trend (`reports/m032_braf_year_trend.md`)
- 29 surgery years (1999-2025), 10,871 patients
- **Testing-rate × year (≥2010): rho=0.915, p<0.0001** — strong adoption curve
- **BRAF+ rate × year (among tested): rho=0.028, p=0.92** — flat. Tested-cohort biology stable across years (testing isn't enriching for high-risk PTC; rate is consistent)

### M044 Cox PH sensitivity on cleaner LN cohort (`reports/m044_cox_sensitivity_ln_clean.md`)
- n=1,703 (events 328) — restricted to ln_status_source ≠ 'staging' subset
- c-index 0.721 (≈ full-cohort 0.717)
- Compare effects to full M044 Cox (n=2,626) for direction-preservation; ETE micro/gross still NS in restricted

---

## Tier 5 — outputs to ship after mig_264b/266/267/268 land

When those 4 close, the cohort numbers shift slightly:
- 22 NIFTP + 2 follicular adenoma reclassified non-malignant (mig_264b) → cohort malignancy rate 38.1% → 37.9%
- 19 negative-FNA-day patients get bethesda_final corrected (mig_264b) → Bethesda 2 ROM drops from 18.9% → ~16.8%
- Histology grouping replaced with SSOT JOIN (mig_267) → no numeric change but consistency across manuscripts
- Manuscript drafts get the 6 round-6 footnotes (mig_266) → reviewer-defensibility complete

**Re-runs to schedule after the 4 migs close:**
1. Re-run Prompt 7 — Bethesda 2 ROM should drop 18.9% → 16.8%; n_malig 385 → ~342
2. Re-run M037 Table 1 + Table 2 against post-mig CPM
3. Re-run M044 Cox PH against post-mig CPM (the 24 NIFTP/FA reclass affects the malignant denominator)
4. Re-run M025 TIRADS Performance — same TR distribution but tightened gold-standard

---

## Tier 6 — new manuscript outputs (not yet built)

### M044 KM curves data export
For publication figures (Kaplan-Meier survival curves stratified by ETE), the Cox PH report has the raw data; this would extract per-time-point survival probabilities + 95% CI for plotting.

### M037 / M044 univariable + multivariable comparison summary table
Current outputs are split: `m037_table2_logreg.md` (full cohort), `m037_sensitivity_ln_both.md` (sensitivity), `m044_cox_ph.md` (Cox), `m044_cox_sensitivity_ln_clean.md` (Cox sensitivity). One synthesis table per manuscript with columns "univariable OR / aOR / sens. univariable OR / sens. aOR" — manuscript-Table-2-ready.

### M027 FNA Performance Table
Mirror of M025 TIRADS but using Bethesda categories. Already have most data points; just needs the Sens/Spec/PPV/NPV computation per Bethesda threshold.

### M032 era × outcomes
Recurrence rate, RAI rate, malignancy rate, mean tumor size, surgery type mix per era. Extends the era Table 1 with outcome columns.

### M037 Race-disparity sub-analysis
Round 5 surfaced Black/AA 13.1% vs White 28% in the M037 LN+ stratum (counter-intuitive). Worth a focused sub-analysis: malignancy rates × race × histology × era — does the gap close in modern era? Is it operative referral pattern?

---

## Tier 7 — Cortex AI deepening

### AI_AGG over `path_synoptics.synoptic_diagnosis`
~5,000 free-text path-report entries. AI_AGG to surface common phrasing patterns + clinical-context themes. Manuscript supplement candidate.

**Setup:** add `path_synoptics` to `01_export_md_to_parquet.py` TABLES list; build a `path_synoptics_FLAT` view; run AI_AGG with `synoptic_diagnosis` as input.

### Cortex Search index over multiple text columns
- `synoptic_diagnosis` (free-text path)
- `nlp_*_evidence_text` columns (NLP entity text)

Lets Logan search "patients with extension into recurrent laryngeal nerve mentioned in path report" or "patients with chyle leak documented in op notes" in seconds. Plan §1.6 deliverable.

### Full-cohort AI_EMBED + cluster export back to MD
Round 6 ran a 500-pt sample. Now run on all 4,137 malignant patients (~$0.30 of trial credits) and export the cluster assignments back to MD as a derived `cpm_phenotype_cluster` column. Becomes a feature for future multivariable models.

---

## Cursor migs proposed for after mig_264b/266/267/268 close

### mig_269 (LOW; Composer): canonical_recurrence_events_v1 SSOT
Optional carry-forward from CF-mig255-RECUR-RESOURCING-FROM-EVENTS. Only worth doing if M044 Cox PH model needs cleaner recurrence input. Currently `time_to_recurrence_days` comes from `canonical_recurrence_v1` (mig_139); a true `canonical_recurrence_events_v1` would key on individual recurrence events with site, date, evidence strength.

### mig_270 (MED; Composer): Re-point Snowflake scripts to histology SSOT (post-mig_267)
After mig_267 lands `canonical_histology_lookup_v1` in MD, the Snowflake scripts that have inline `CASE WHEN histology_final ILIKE 'PTC%'` need updating to JOIN the SSOT.

Files to edit:
- `snowflake_trial/scripts/08_cohort_views.py` (M037 view)
- `snowflake_trial/scripts/09_m037_table1.py`
- `snowflake_trial/scripts/19_m044_table1.py`
- `snowflake_trial/scripts/21_m004_table1.py`
- `snowflake_trial/scripts/22_m037_table2_logreg.py`
- `snowflake_trial/scripts/24_m044_cox_ph.py`
- `snowflake_trial/scripts/25_m037_sensitivity_ln_both.py`
- `snowflake_trial/scripts/29_m044_cox_sensitivity_ln_clean.py`

### mig_271 (LOW; Composer): NIFTP + AJCC stage sweep
After mig_264b reclassifies NIFTP as `IS_MALIGNANT=FALSE`, audit:
- Are there NIFTP patients with `ajcc8_stage_group` populated? (Should now be NULL per AJCC 8.)
- Are there NIFTP patients in M037/M044 cohort views? (Should be excluded.)
- Manuscript text references "10,871 patient cohort, 38.1% malignancy rate" → update to "37.9%" in revisions.

### mig_272 (MED; Chat → Composer): NLP refresh batch coordinator
Standalone workstream for CF-mig260b/c/d + CF-mig261c/d/e:
- Re-run NLP entity extraction against Social History sub-sections (recover smoking 27 → ~7,000 expected)
- Re-run against CAP "Lymph-Vascular Invasion: Present" patterns (recover 749 vasc invasion under-fires)
- Re-run against recurrence note text (recover 1,105 LN discordance + 158 NLP-only recurrences)

This is large enough to warrant a Chat-first strategic session before Composer dispatches per-domain migs.

---

## Calendar

- **PAT expires 2026-05-08** — rotate via Snowsight to 30-day next time (current PAT was 7-day demo)
- **Trial converts 2026-05-29** — set reminder to cancel if not converting
- **Credits used so far:** ~$5-6 of $40 budget. Tier 6+7 work will likely consume another $10-15.

---

## Decision points for Logan

1. **Is M025 TIRADS performance ready to ship in its current form?** ROM rows show operative-cohort enrichment clearly; sens/spec/PPV/NPV for TR ≥ TR4 cutoff are reasonable. Manuscript footnote about cohort enrichment already drafted. → If yes, M025 enters revision phase.

2. **Should mig_269 (canonical_recurrence_events_v1 SSOT) be prioritized?** Currently optional. Becomes worth doing if M044 needs more granular recurrence inputs (site-specific, evidence-strength-weighted) for survival models. → If M044 stays as one-event Cox, skip.

3. **NLP refresh batch (mig_272) — is this in scope for this trial, or post-trial workstream?** Big effort. Currently the validation prompts have surfaced the gaps but not closed them. → Likely defer to post-trial unless the manuscripts can't be drafted without it.
