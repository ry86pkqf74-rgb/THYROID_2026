# Cursor Agent Task — `canonical_patient_master` SURVIVAL CLUSTER slice (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_136 PMH+PSH landing)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** at handoff time — `git fetch && git pull` before starting
**Estimated effort:** 2 hours (~25 cols)
**Run order:** Lane 30 of new 4-prompt batch (mig_141)

---

## 1. Goal

Continue patient_master verification with the **survival + follow-up cluster** (~25 unverified cols covering vital status, death dates/sources, follow-up duration/completeness, overall survival, mortality crossover from complications, and PRM follow-up adequacy flags).

Probe scope:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND (column_name ILIKE '%surviv%'
       OR column_name ILIKE '%mortal%'
       OR column_name ILIKE 'death%'
       OR column_name ILIKE 'followup%'
       OR column_name = 'vital_status'
       OR column_name ILIKE 'voice_followup%'
       OR column_name ILIKE 'prm_followup%'
       OR column_name ILIKE 'prm_tg_adequate%')
  AND column_name NOT IN (
    SELECT column_name FROM main.canonical_column_verification_registry_v1
    WHERE table_name='canonical_patient_master' AND verification_status <> 'not_started'
  )
ORDER BY column_name;
```

Confirm count is **exactly 25** before proceeding (Cowork survey 2026-04-29).

Sub-clusters:

- **Death attribution chain** (~5 cols): `death_date`, `death_days_from_surg`, `death_integration_script`, `death_occurred`, `death_source` — derive against `canonical_complications_events_v1` (mig_99) finding_status='present' for category='mortality' + `canonical_survival_followup_v1` (mig_123)
- **Vital status + survival metrics** (~5 cols): `vital_status`, `survival_eligible_flag`, `survival_event`, `overall_survival_days`, `overall_survival_years` — from `canonical_survival_followup_v1`
- **Follow-up chain** (~9 cols): `followup_all_sources`, `followup_category`, `followup_completeness_score`, `followup_days`, `followup_n_contact_sources`, `followup_or_death_date`, `followup_or_death_days_from_surg`, `followup_or_death_years`, `followup_recovery_method`, `followup_years` — from `canonical_survival_followup_v1`
- **PRM follow-up adequacy** (~5 cols): `prm_followup_clinical_events`, `prm_followup_has_complications`, `prm_followup_tg_labs`, `prm_tg_adequate_followup`, `voice_followup_completeness` — derived against complications_v1 + lab canonicals + voice/RLN cluster

---

## 2. Methodology — derivation re-derivation against canonical_survival_followup_v1 + mortality crossover

Pattern reference: `qc_framework_v1/migrations/123_canonical_survival_followup_v1_signoff.sql` + `project_complications_events_verified_2026-04-28.md` (mig_98h mortality close-out).

### 2a. Per-col derivation map (representative)

- `vital_status` → from `canonical_survival_followup_v1.vital_status` (1:1 if PM source maps directly)
- `death_occurred` → BOOL: `vital_status IN ('deceased','dead','died')` per pt; cross-check with complications mortality finding_status='present' counts
- `death_date` → from canonical_survival_followup_v1; **MUST be DATE not TIMESTAMP** per `feedback_clinical_dates_calendar_only.md`. If TIMESTAMP, open `CF-mig141-PM-DEATH-DATE-RETYPE`
- `death_days_from_surg` → derived `(death_date - first_surgery_date)`; **first_surgery_date is anchor** per `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md`
- `death_integration_script` → provenance metadata; passthrough verification
- `death_source` → categorical: which upstream source attributed death (chart vs SSDI vs complications)
- `survival_eligible_flag` → analytic-cohort-eligibility filter; check existing PM build SSOT
- `survival_event` → 1 if death observed before censor, 0 otherwise (Kaplan-Meier event flag)
- `overall_survival_days` / `overall_survival_years` → `(censor_date - first_surgery_date)` for alive, `(death_date - first_surgery_date)` for deceased
- `followup_completeness_score` → multi-source aggregate from canonical_survival_followup_v1
- `followup_n_contact_sources` → COUNT(DISTINCT source) per pt
- `prm_followup_has_complications` → BOOL: any complications row exists per pt
- `prm_followup_tg_labs` → BOOL: any Tg lab rows exist per pt (cross-check Lane 25 labs scope)
- `prm_tg_adequate_followup` → derived rule (Tg every X months); check SSOT
- `voice_followup_completeness` → from voice/RLN follow-up cluster (mig_98c voice_nerve close-out)

### 2b. ⚠️ Mortality crossover with complications

Per `project_complications_events_verified_2026-04-28.md`: mortality is one of the 8 complication categories (mig_98h closed). PM `death_occurred` should agree with `EXISTS(complications_events_v1 WHERE category='mortality' AND finding_status='present')`. If drift exists, document precedence (survival_followup_v1 is SSOT for vital status; complications mortality is event-grain per-clinical-encounter).

### 2c. ⚠️ Survival CF caveats

Per `qc_framework_v1/AGENTS.md` survival CF section + `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md`: `canonical_survival_followup_v1` has known column-registry-vs-information_schema data_type drift. When deriving, use information_schema as source-of-truth for live types, not the registry.

### 2d. ⚠️ Clinical dates calendar-only

Per `feedback_clinical_dates_calendar_only.md` (Logan-ratified 2026-04-28): `death_date`, `followup_or_death_date` MUST be DATE not TIMESTAMP. Audit/provenance timestamps (build_ts, extracted_at) exempt.

### 2e. ⚠️ NULL semantics

`overall_survival_*` is NULL for patients with no follow-up (not 0). `prm_followup_*` BOOLs may be NULL or FALSE — pick a convention and document.

### 2f. ⚠️ Cohort-uniformity sanity check (CRITICAL)

Run §2d sweep on every BOOLEAN col in this lane. `survival_event=0` should be common (most pts alive); `death_occurred=TRUE` should match the documented mortality count from canonical_complications_events_v1 mig_98h (~ a few hundred pts). Flag any unexpected uniformity.

### 2g. Sign-off SQL

File: `qc_framework_v1/migrations/141_patient_master_survival_cluster_signoff_20260429.sql`

```
batch_id = 'mig_141_patient_master_survival_cluster_20260429'
verification_method options:
  - 'derivation_vs_canonical_survival_followup_v1'
  - 'derivation_vs_canonical_complications_events_v1_mortality'
  - 'cross_check_mortality_crossover_survival_complications'
  - 'patient_level_aggregate_followup_per_source'
  - 'prm_rule_followup_adequacy_chain'
  - 'auto_provenance_skip' (death_integration_script)
```

---

## 3. Acceptance gates

- ~25 survival-cluster cols flipped
- 0 drift on derivation re-derivation per col
- Mortality crossover (PM death_occurred ↔ complications mig_98h finding_status='present') agreement ≥ 95%
- All clinical date cols are DATE; CF rows for any TIMESTAMP/VARCHAR violations
- Cohort-uniformity sweep clean
- gate 4 = 0
- PM `n_verified` advances by exactly the cluster count

---

## 4. Don't touch (active parallel lanes)

- MOLECULAR — Lane 27 (mig_137 expected, scope ~3 cols)
- RECURRENCE-RESPONSE — Lane 28 (mig_138 expected, scope ~4 cols)
- ETE — Sibling Lane 29 (mig_140)
- RAI — Sibling Lane 31 (mig_142)
- SMALL-CLUSTERS bundle — Sibling Lane 32 (mig_143)

---

## 5. Reference reading

Required:
- Auto-memory: `feedback_clinical_dates_calendar_only.md`
- Auto-memory: `feedback_motherduck_direct_check.md`
- Auto-memory: `project_complications_events_verified_2026-04-28.md` (mortality is mig_98h)
- Auto-memory: `project_canonical_recurrence_v1_mig_123_rebuild_closeout.md` (data_type drift CFs)
- Auto-memory: `feedback_recurrence_imaging_n_events_null.md`
- Repo: `qc_framework_v1/migrations/123_canonical_survival_followup_v1_signoff.sql`
- Repo: `qc_framework_v1/migrations/99_complications_events_signoff.sql` (or whichever the mig_99 file is)
- Repo: `qc_framework_v1/AGENTS.md` survival CF section

---

## 6. File / commit conventions

Same as Lane 29 ETE: surgical git add, single commit, DATE-typed clinical dates, CAST CURRENT_TIMESTAMP AS TIMESTAMP for build_ts, explicit not_started filter.

---

## 7. If something unexpected surfaces

- Mortality crossover drift > 5% → STOP, ask Logan; canonical_survival_followup_v1 vs complications mig_98h precedence is a clinical decision
- `overall_survival_*` cols are TIMESTAMP-derived (negative or fractional days) → CF retype + verify with note
- `prm_tg_adequate_followup` SSOT rule undocumented → STOP, ask Logan
- More than 3 cols can't be cleanly mapped → STOP, ask Logan

---

End of prompt. Lane 30 of 4-prompt batch (target: PM `n_verified` 805 → 830).
