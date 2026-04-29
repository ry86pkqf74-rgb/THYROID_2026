# Cursor Agent Task — `canonical_patient_master` NLP CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session)
**Estimated effort:** 4-5 hours (~120 cols — biggest lane in this batch)
**Run order:** Lane 42 of next 4-prompt batch (mig_152)

---

## 1. Goal

Continue patient_master verification with the **NLP cluster** (~120 unverified `nlp_*` cols). Highly uniform pattern: 25 sub-domains × 4 cols each (`nlp_<domain>_has_data`, `nlp_<domain>_key_finding`, `nlp_<domain>_n_entities`, `nlp_<domain>_n_notes`) plus a few outliers.

Probe scope:

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master' AND column_name LIKE 'nlp_%'
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY column_name;
```

Confirm count ~120. The 25 sub-domains:

```
ne, path, rec, tirads, ln, parathyroid, dynrisk, tg, symptoms, frozensec,
survfu, cervln, pmhx, funcoutcome, labs, physexam, pshx, imaging, esoph,
ptdecision, usnodule, airway, radtx, raidetail, vasc
```

---

## 2. Methodology — extraction-faithfulness vs Tier-1 LLM canonicals

For each `nlp_<domain>_*` quad, the SSOT is `note_entities_llm_<domain>` (Tier-1 raw LLM extraction outputs, registry-seeded as `na` raw-mirror exempt).

### 2a. Per-col derivation map (uniform pattern)

For each domain D in {25 listed}:

- `nlp_<D>_has_data` → BOOL: `EXISTS(note_entities_llm_<D> WHERE research_id = pm.research_id AND error = 0)`
- `nlp_<D>_n_entities` → COUNT(*) per pt over `note_entities_llm_<D> WHERE error = 0`
- `nlp_<D>_n_notes` → COUNT(DISTINCT note_id) per pt
- `nlp_<D>_key_finding` → most-confident or most-recent extracted finding (need to read existing PM build SSOT for the rule)

Method: `extraction_faithfulness_vs_note_entities_llm_<D>` per sub-domain — **use distinct method strings per sub-domain so the registry preserves D identity within the single batch_id**.

### 2b. ⚠️ Outlier sub-domain quirks (verify carefully)

- **`nlp_ne` (12 cols)**: NE = "named entities"? Or generic NLP-extraction? Probe Tier-1 source `note_entities_llm_ne` to disambiguate. May not follow the 4-col pattern.
- **`nlp_path` (10 cols)**: pathology NLP — overlaps with structured path canonicals. Document precedence (structured path > NLP path when both have signal).
- **`nlp_rec` (8 cols)**: recurrence NLP — overlaps with `canonical_recurrence_v1` SSOT (mig_123 rebuild). After mig_139 resync, structured recurrence is the SSOT.
- **`nlp_tirads` (5 cols)**: TIRADS-from-NLP — should align with US-structured TIRADS where both exist.

### 2c. ⚠️ Cohort-uniformity sanity check (CRITICAL — 25 sub-domains × `_has_data` BOOLEANs)

For every `nlp_<D>_has_data` BOOLEAN flipped:

```sql
SELECT '<D>' AS domain,
       SUM(CASE WHEN nlp_<D>_has_data THEN 1 ELSE 0 END) AS has_data_TRUE,
       SUM(CASE WHEN NOT nlp_<D>_has_data THEN 1 ELSE 0 END) AS has_data_FALSE,
       SUM(CASE WHEN nlp_<D>_has_data IS NULL THEN 1 ELSE 0 END) AS has_data_NULL
FROM main.canonical_patient_master;
```

Expected:
- High-coverage domains (path, ln, pmhx, pshx, symptoms, ptdecision, physexam, labs, imaging) → moderate-to-high TRUE rate (30-90% of cohort with notes)
- Specialized domains (parathyroid, frozensec, raidetail, esoph) → low-to-moderate (5-30%)
- `nlp_dynrisk_has_data` → check if "dynamic risk stratification" extraction is implemented; may be near-uniform-FALSE if pipeline incomplete

Flag near-uniform-TRUE OR -FALSE per the mig_135/141/144 lessons.

### 2d. ⚠️ NULL semantics

`nlp_<D>_n_entities` should be NULL (or 0) for patients with no notes-of-that-domain. Pick a convention and document.

### 2e. ⚠️ "Tier-1 raw mirror exempt" caveat

`note_entities_llm_<D>` tables are registered as `na` (raw-LLM-mirror exempt) — this is the SSOT chain. Extraction-faithfulness FROM these tables is the verification methodology. Per `feedback_extraction_faithfulness_llm_canonical.md`: re-derive every col fresh from upstream WHERE error=0; mass-equivalence.

### 2f. Cross-validate against any verified overlapping canonical

For sub-domains with structured canonicals, cross-check NLP-vs-structured agreement and document:
- nlp_path vs canonical_path_malignant_events_v1
- nlp_rec vs canonical_recurrence_v1
- nlp_ln / nlp_cervln vs canonical_cervical_ln_clinical_events_v1
- nlp_parathyroid vs canonical_parathyroid_events_v1
- nlp_frozensec vs canonical_frozen_section_events_v1
- nlp_pmhx vs canonical_pmh_* (if exists)
- nlp_radtx vs canonical_radtx_* (if exists)
- nlp_raidetail vs canonical_rai_* (mig_148 in flight)

If structured > NLP (structured says present, NLP says absent), document precedence: structured wins. Open `CF-mig152-NLP-STRUCTURED-DRIFT-<D>` for any > 5% drift.

### 2g. Sign-off SQL

File: `qc_framework_v1/migrations/152_patient_master_nlp_cluster_signoff_20260429.sql`

```
batch_id = 'mig_152_patient_master_nlp_cluster_20260429'
verification_method options (one per sub-domain):
  - 'extraction_faithfulness_vs_note_entities_llm_path'
  - 'extraction_faithfulness_vs_note_entities_llm_rec'
  - ... (25 such method strings)
  - 'extraction_faithfulness_count_aggregate'
  - 'cross_validate_nlp_vs_structured_<D>'
```

---

## 3. Acceptance gates

- ~120 cols flipped
- Distinct verification_method per sub-domain (preserves NLP-domain identity within batch_id)
- 0 drift on extraction-faithfulness against Tier-1 source per col
- Cross-validation against structured canonicals for overlapping sub-domains; CF for > 5% drift
- Cohort-uniformity sweep on every BOOLEAN
- gate 4 = 0
- PM advances by ~120

---

## 4. Don't touch (active parallel lanes)

- mig_142 RAI BLOCKED
- mig_145/146/147 imaging (in flight)
- mig_148 RAI upstream (in flight)
- Sibling lanes 39, 40, 41 (synoptic, parathyroid+postop+TP, meds+radtx+proc)

**Coordinate:** if mig_148 lands during this lane, the `nlp_raidetail_*` sub-domain cross-validate against the newly-verified RAI canonical. If mig_148 still in flight, document NLP raidetail values without cross-validation; flag CF-mig152-NLP-RAIDETAIL-PENDING-MIG148.

---

## 5. Reference reading

Required:
- Auto-memory: `feedback_extraction_faithfulness_llm_canonical.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `project_round2_llm_integration_script_386_closeout.md` (LLM round-2 integration)
- Auto-memory: `project_complications_events_verified_2026-04-28.md`
- Auto-memory: `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md`

---

## 6. File / commit conventions

Same as siblings. Single commit closing the NLP cluster. Even though 120 cols is large, this is one logical commit.

---

## 7. If something unexpected surfaces

- A sub-domain's Tier-1 source `note_entities_llm_<D>` doesn't exist → flag CF-mig152-MISSING-TIER1-<D>; flip those 4 cols to `na` with `verification_method='upstream_tier1_pending'`
- nlp_ne pattern doesn't fit the 4-col mold → STOP, probe and ask Logan before flipping nlp_ne cols
- Cross-validate drift > 30% on any structured-overlapping domain → STOP, that's a real upstream issue (likely stale build); ask Logan
- More than 10 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 42 of 4-prompt batch.

After this batch lands:
- PM `n_verified` advances by ~229 cols (32 syn + 38 para/postop/tp + 39 meds/radtx/proc + 120 nlp)
- Combined with mig_142 RAI (51) + mig_145/146/147 imaging (105) + mig_148 RAI upstream (25 cols at table-grain not PM), the overall PM should reach ~85-90% verified.
