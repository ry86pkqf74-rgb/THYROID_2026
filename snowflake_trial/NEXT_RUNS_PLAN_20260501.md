# Next Runs Plan — 2026-05-01 (post mig_260/261/262/263)

**Generated:** 2026-05-01 by Cowork
**HEAD:** `470439a` (post Logan's mig_262 NULL-date probe doc)
**Status:** 4 of 6 round-6 Cursor migs applied & verified (260/261/262/263). 2 remain in-flight (mig_264 + mig_265).

---

## Recent commit assessment

| Commit | What | Verdict |
|---|---|---|
| `d7e7fbc` mig_260 | TIRADS re-point to canonical_us_patient_master_VIEW_v2 — Snowflake side | ✅ Verified end-to-end. TIRADS×ROM table now produces real values (TR1 30.3% / TR2 33.7% / TR3 29.9% / TR4 49.3% / TR5 60.1%). Hand-built `CANONICAL_US_PATIENT_MASTER_VIEW_V2_FLAT` to bypass an INFER_SCHEMA-returns-0 quirk on view-derived Parquet. |
| `5351070` mig_263 | AJCC overlay re-derive Option B (IVA/IVC → IVB collapse) | ✅ Verified MD + Snowflake. M1+II=1,018 stable; MTC/ATC/PDTC II=0 across all three; new `AJCC8_STAGE_GROUP_RESOLVED`/`_CORRECTED`/`_V2` columns now on FLAT view. |
| `ca82d8a` mig_261 | path_synoptics CAP label normalization + surg_date DATE | ✅ Verified MD. LVI typos t1-t5=0; ETE 'extesive'=0; focality unifocal=2,581/multifocal=1,410; surg_date=DATE. 2 residual long-tail focality variants (Multifocal/`unifocal*`) — minor follow-up. |
| `b30510f`+`aad47d2`+`aa0c2ac`+`470439a` mig_262 | LN flag rebuild + imaging YY-typo fixes + NULL-date probe doc | ✅ Verified MD + Snowflake. **`any_suspicious_us_ln_ever` 8 → 1,733 (217× rebuild)** — flag was effectively dead, now firing for 42% of US patients. imaging_exam OOB dates = 0 (rids 12048→2002, 10511→2022). The 2,050 NULL exam_dates in `raw_imaging_12_slots_v1` deferred per probe blocker doc — that table doesn't exist in publication DB. |

**Notable:** mig_262's LN-flag rebuild is the largest single data-quality improvement of the session — went from "8 patients flagged suspicious" to "1,733" by sourcing from `canonical_us_thyroid_gland_v2` per-nodule level instead of the original threshold-too-tight definition. M037 / M044 / M076 (any LN-touching paper) now has a usable suspicious-LN signal.

---

## Cursor — what's left (2 prompts)

| Mig | Status | Logan's queue |
|---|---|---|
| mig_264 — Bethesda-2 false-negative audit | In-flight | 385 patients with Bethesda 2 + IS_MALIGNANT (18.9% ROM vs 0-3% expected). Decision pass needed first. |
| mig_265 — PMH `_definitive` rule + manuscript footnote | In-flight | 9 conditions with `_any_evidence > 0 AND _definitive = 0` for all rows; smoking/family-hx/HTN under-extraction footnote. |

After these land, all 6 round-6 migs are closed.

---

## Next Snowflake runs (proposed, in order)

### Tier 1 — close the round-6 verification loop (after mig_264 + mig_265)

1. **Re-export + reload + re-run Prompt 7** to pick up any Bethesda re-mappings from mig_264. The 385 false-negative count should drop if mig_264 finds linkage mismaps.

2. **Re-run Prompt 11** (comorbidity) to verify mig_265 effect on PMH `_definitive` cols. Add a new probe: count of conditions where `_definitive > 0` AND `_any_evidence > 0` (should jump from 0 to all 9 affected).

3. **Re-validate the manuscript Tables 1 + Table 2** that were generated against the pre-mig state:
   - M037 Table 1 (cohort changed? mig_258 ln_status_source already affects this)
   - M037 Table 2 logreg (mig_263 may have flipped some stage_groups; check pseudo R² stable)
   - M044 ETE Table 1 (mig_261 normalized ETE labels — verify the strata counts didn't shift)
   - M032 era Table 1 (no expected change)
   - M004 autoimmune Table 1 (no expected change)

### Tier 2 — new manuscript outputs

4. **M044 Cox proportional hazards model** for time-to-recurrence by ETE strata (none/microscopic/gross). Use `lifelines.CoxPHFitter`. Adjust for age, sex, T-stage, surgery type, RAI, BRAF. Output: forest plot data + survival curves data.

5. **M037 sensitivity analysis** restricted to `ln_status_source = 'both'` (1,126 patients). Re-fit the multivariable logreg from Table 2; compare to full-cohort estimates. Manuscript-methods strengthening.

6. **M032 era × molecular era trend analysis**: BRAF positivity rate × surgery year (cleaner than the era buckets — y=axis, year=x). Likely shows the 2014+ adoption curve.

7. **M025 (TIRADS performance) Table 1**: now that mig_260 cleared the TIRADS pipeline, build a TIRADS performance table — sensitivity, specificity, PPV, NPV for each TR category vs final pathology.

### Tier 3 — Cortex AI deepening

8. **AI_AGG over `path_synoptics.synoptic_diagnosis`** for the ~5,000 patients with raw text. Surface common phrasing patterns, free-text descriptors not captured by structured CAP fields. Manuscript-supplement candidate.

9. **Cortex Search index** over `synoptic_diagnosis` + key NLP text columns. Lets Logan find specific clinical scenarios in seconds ("show me patients with extension into the recurrent laryngeal nerve mentioned in the path report").

10. **AI_EMBED expansion**: rerun on full 4,137 malignant cohort (not just 500-pt sample); export embeddings + per-patient cluster assignment back to MotherDuck as a new column on CPM. Becomes a derived feature for future logreg models.

### Tier 4 — cross-validation against external sources

11. **AI_CLASSIFY full histology cleanup** — re-run on all 10,871 distinct `histology_final` strings, generate a definitive lookup table that maps every variant to {DTC, FTC, MTC, ATC, PDTC, Hurthle, NIFTP, Other-malignant, Benign}. Ship as `canonical_histology_lookup_v1` (Cursor mig). Closes the M025/M037/M044 manuscript footnote requirement around histology naming.

12. **Statistical disclosure risk** — k-anonymity check before any publication export. AI-assisted detection of rare combinations (k<5).

---

## Next Cursor migs (proposed, after mig_264 + mig_265 land)

### mig_266 (HIGH; Composer): Manuscript footnote + filter declarations

After all the migs land, M032/M037/M044/M025 manuscripts need their methods sections updated to declare:
- ln_status_source filter (mig_258/259)
- AJCC stage convention (mig_263 Option B — IVA/IVC collapsed to IVB)
- Bethesda cohort enrichment caveat (mig_264 + cohort-bias note)
- ETE label normalization (mig_261)
- LN flag definition update (mig_262)
- Smoking/family-hx coverage limitation (mig_265)

This is one mig per manuscript or a single bulk dispatch. Composer-direct.

### mig_267 (MED; Chat → Composer): canonical_histology_lookup_v1

Build a SSOT histology mapping table. Source = AI_CLASSIFY against all distinct strings. Replaces the ad-hoc `CASE WHEN ILIKE` derivations scattered across cohort views and Tables.

### mig_268 (MED; Composer): residual focality drift cleanup

The 2 residual focality drift values from mig_261 (`Multifocal`, `unifocal*` long-tail). Trivial follow-up.

### mig_269 (LOW; Composer): canonical_recurrence_events_v1 SSOT

Logan's CF-mig255-RECUR-RESOURCING-FROM-EVENTS optional carry-forward. If recurrence becomes a primary outcome in M044 Cox model (Tier 2 #4 above), this becomes worth doing.

### Future: NLP refresh batch (CF-mig260b/c/d, CF-mig261c/d/e)

Standalone workstream — not a single Cursor mig, but a new round of NLP entity extraction against:
- Social History sub-sections (recover smoking/family-hx coverage)
- CAP "Lymph-Vascular Invasion: Present" + newer separate "Lymphatic Invasion" patterns (CF-mig260b vasc invasion underfire — 749 patients NLP misses)
- Recurrence note text (CF-mig260c LN discordance + recurrence flagging)

This unblocks subgroup analyses currently underpowered.

---

## Calendar

- **PAT expires 2026-05-08** — rotate via Snowsight (Admin → Users → LGLOSSE13 → Generate token, 30-day expiry next time)
- **Trial converts 2026-05-29** — set reminder to cancel if not converting

## Cost estimate (rough)

Trial credits used so far (this session):
- ~600-800 AI SQL calls (mostly Llama 8B; some Sonnet 4 for staging audit)
- 500 AI_EMBED calls (1 batch)
- ~$3-5 of $40 trial budget consumed

Remaining $35+ leaves ample room for Tier 1+2 above. Tier 3 (full-cohort AI_EMBED, AI_AGG over 5,000 free-text rows, Cortex Search index build) might consume another $10-15.
