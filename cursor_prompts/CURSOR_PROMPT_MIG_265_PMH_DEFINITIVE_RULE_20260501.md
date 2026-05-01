# Cursor Composer Dispatch — mig_265: PMH `_definitive` rule audit + smoking/family-hx coverage manuscript footnote

**Generated:** 2026-05-01 by Cowork (Snowflake Cortex round 6).
**Lane:** mig_265 — Two PMH/comorbidity findings: (1) **9 conditions have `_any_evidence > 0` AND `_definitive = 0` for ALL rows** — the rollup builder's strength-promotion rule never fires for them. (2) Cohort coverage of smoking, family history, and bone health is dramatically under-extracted (smoking 27 patients = 0.25% vs expected ~70%+ for any clinical cohort).
**Recommended agent:** **Cursor Composer** for §3 rollup builder rule audit; **Cursor Chat** if Logan wants to scope an upstream NLP refresh for §4. The manuscript footnote (§5) is a text-only edit.
**Estimated runtime:** 90–120 min if both fixes; 30 min if just the rollup builder
**Triggered by:** Round 6 Prompt 11 (CF-mig261b-d).
**Severity:** LOW for rollup; MED for manuscripts (any subgroup analysis depending on smoking, family hx, or osteoporosis cannot be powered).
**Closes carry-forwards:** CF-mig261b (rollup), CF-mig261c (smoking), CF-mig261d (family hx), CF-mig261e (HTN undercount).

---

## §0 — First message to paste into Cursor Composer

> mig_265 dispatch. Two parallel fixes on PMH/comorbidity:
>
> **Part 1 (rollup builder bug, can run now):** 9 conditions have `<cond>_any_evidence > 0` but `<cond>_definitive = 0` for every patient with that condition. The strength-promotion rule in the canonical_pmh_patient_rollup_v1 builder never fires for these conditions. Audit and fix.
>
> **Part 2 (NLP coverage gap, scope-only — no DML this lane):** Smoking 27 patients (0.25%), family_hx_thyroid 30 (0.3%), family_hx_cancer 16 (0.1%). All ~10× under-extracted vs expected community rates. Surface to Logan as a future-NLP-refresh scope; document in manuscript footnotes for any analyses depending on these.

---

## §1 — Why this lane exists

### 1a. Rollup-builder definitive-tier blackout

From round-6 Prompt 11, 9 PMH conditions have `_definitive = 0` for ALL patients with `_any_evidence > 0`:

| Condition | any_n | definitive_n | Pattern |
|---|---|---|---|
| autoimmune_thyroid_hx | 78 | **0** | rollup never promotes |
| radiation_exposure | 33 | **0** | same |
| family_hx_thyroid | 30 | **0** | same |
| osteoporosis | 23 | **0** | same |
| family_hx_cancer | 16 | **0** | same |
| smoking_current | 14 | **0** | same |
| coagulopathy | 13 | **0** | same |
| smoking_never | 9 | **0** | same |
| smoking_former | 6 | **0** | same |
| men_syndrome | 6 | **0** | same |

By contrast, the conditions with normal definitive-tier promotion:
| hypothyroidism | 1,963 | 1,962 | normal — 99.9% of any-evidence rows promoted |
| hypertension | 1,781 | 1,775 | normal |
| diabetes | 1,483 | 1,466 | 98.9% |

The 9 affected conditions cluster in social hx + family hx + bone health + autoimmune. Likely either:
- The rollup builder's strength-promotion CASE statement has explicit category exclusions for these 9
- The threshold logic uses an evidence-strength keyword (e.g. "definitive", "confirmed") that the upstream NLP doesn't tag for these condition types

### 1b. Coverage gaps (NLP under-extraction)

| Condition | Observed % | Expected (US adult clinical cohort) | Gap |
|---|---|---|---|
| smoking (any status) | 0.25% (27/10,871) | 70%+ documented | 280× under |
| family_hx_thyroid | 0.3% | 5-15% in cancer cohort | 17-50× under |
| family_hx_cancer | 0.1% | 5-15% in cancer cohort | 50-150× under |
| hypertension | 16.4% | ~47% US adult | 2.9× under |
| osteoporosis | 0.2% | 6-10% in 50+ cohort | 30× under |

The hypertension undercount is mild and likely just NLP missing extractions in non-PMHx note sections. The smoking/family/osteoporosis numbers are an order of magnitude off — likely a structural NLP gap (e.g., the extractor only runs against the `Past Medical History:` section header and skips Social History / Family History sub-sections).

## §2 — Pre-task probes (Part 1)

```sql
-- Probe A: confirm the definitive-blackout pattern
SELECT
  'autoimmune_thyroid_hx' AS cond,
  COUNT_IF(pmhx_nlp_autoimmune_thyroid_hx_any_evidence > 0) AS any_n,
  COUNT_IF(pmhx_nlp_autoimmune_thyroid_hx_definitive > 0) AS def_n
FROM main.canonical_patient_master
UNION ALL
SELECT 'radiation_exposure',
  COUNT_IF(pmhx_nlp_radiation_exposure_any_evidence > 0),
  COUNT_IF(pmhx_nlp_radiation_exposure_definitive > 0)
FROM main.canonical_patient_master
UNION ALL
SELECT 'osteoporosis',
  COUNT_IF(pmhx_nlp_osteoporosis_any_evidence > 0),
  COUNT_IF(pmhx_nlp_osteoporosis_definitive > 0)
FROM main.canonical_patient_master;
-- Confirm: any > 0 AND def = 0 for all 9 conditions

-- Probe B: source events for one affected condition (autoimmune_thyroid_hx)
SELECT finding_status, evidence_strength, COUNT(*) AS n
FROM main.canonical_pmh_events_v1
WHERE condition_norm = 'autoimmune_thyroid_hx'
GROUP BY 1, 2 ORDER BY 1, 2;
-- Look for: do any events have evidence_strength = 'definitive' / 'probable'?
-- If NO — upstream never tags strength for this condition (extractor gap)
-- If YES — rollup builder is filtering them out (rollup bug)

-- Probe C: rollup builder source — find the SQL that creates _definitive cols
SELECT view_definition FROM information_schema.views
WHERE view_name = 'canonical_pmh_patient_rollup_v1';
```

## §3 — Apply (Part 1 — rollup builder fix)

After Probe B determines the cause:

### Case 1: Upstream events DO have evidence_strength tags but the rollup never reads them
```sql
-- Pre-snapshot
CREATE OR REPLACE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.cpm_pre_mig265_20260501 AS
SELECT research_id,
       pmhx_nlp_autoimmune_thyroid_hx_definitive, pmhx_nlp_radiation_exposure_definitive,
       pmhx_nlp_osteoporosis_definitive, pmhx_nlp_smoking_current_definitive,
       /* ...all 9 affected cols... */
FROM main.canonical_patient_master;

-- Re-derive definitive cols from events (template — adapt per condition)
UPDATE main.canonical_patient_master cpm
SET pmhx_nlp_autoimmune_thyroid_hx_definitive = (
  SELECT COUNT(DISTINCT e.event_id)
  FROM main.canonical_pmh_events_v1 e
  WHERE e.research_id = cpm.research_id
    AND LOWER(e.condition_norm) = 'autoimmune_thyroid_hx'
    AND e.finding_status = 'present'
    AND e.evidence_strength IN ('definitive', 'probable')
);
-- Repeat for radiation_exposure, osteoporosis, smoking_*, family_hx_*, etc.
```

### Case 2: Upstream events do NOT have evidence_strength tags
This isn't a rollup bug — it's an upstream extractor gap. Document as carry-forward; no DML. Move to Part 2.

## §4 — Part 2: Coverage gap manuscript footnote (no DML)

Update the relevant manuscript drafts (`M032`, `M037`, `M044`, etc) to include this caveat in methods/limitations:

> *Smoking status, family history of thyroid cancer, and family history of any cancer were
> derived via NLP extraction of pre-operative clinical notes. Coverage of these signals in
> our extraction pipeline is documented at <0.5% — substantially below the expected community
> rates for an operative cohort. Subgroup analyses stratified by smoking, family history, or
> bone health were therefore underpowered and not pursued. CF-mig261c/d/e tracks future
> NLP-refresh work to recover these signals.*

If Logan wants to scope an upstream NLP refresh, that's a separate workstream (think: "rerun NLP entity extraction on Social History and Family History note sub-sections"). Document as `CF-mig265-NLP-SOCHX-FAMHX-REFRESH-SCOPE`.

## §5 — Verify (Part 1)

```sql
-- After Part 1 apply
SELECT 'autoimmune_thyroid_hx' AS cond,
  COUNT_IF(pmhx_nlp_autoimmune_thyroid_hx_any_evidence > 0) AS any_n,
  COUNT_IF(pmhx_nlp_autoimmune_thyroid_hx_definitive > 0) AS def_n
FROM main.canonical_patient_master;
-- If Case 1 (rollup fix): expect def_n > 0
-- If Case 2 (no upstream tags): def_n still 0; documented as known limitation
```

## §6 — Registry signoff
```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES ('mig_265', CURRENT_TIMESTAMP::TIMESTAMP, 'logan_via_cursor',
  'PMH _definitive col audit (Part 1): rebuilt 9 conditions from canonical_pmh_events_v1 '
  'with evidence_strength promotion rule. Manuscript footnote (Part 2) added to M032/M037/M044 '
  'methods sections re: under-extraction of smoking/family-hx/bone health. CF-mig265-NLP-SOCHX-FAMHX-REFRESH-SCOPE opened for future upstream extraction work.');
```

## §7 — Carry-forwards
- CF-mig261b-PMH-DEFINITIVE-COL-DEAD → CLOSED if Case 1, otherwise documented
- CF-mig261c-d-e → CLOSED via manuscript footnote (Part 2)
- CF-mig265-NLP-SOCHX-FAMHX-REFRESH-SCOPE → NEW (future NLP refresh scope)

## §8 — Surgical git add
```
scripts/output/mig_265_*.md
qc_framework_v1/migrations/265_pmh_definitive_rule_*.sql
M032_*.md   (manuscript footnote edits)
M037_*.md
M044_*.md
```
