# Cursor Agent Task — `canonical_patient_master` RISK-SCORING + SURVIVAL + GENETICS-RESIDUAL CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_142b)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting
**Estimated effort:** 2-3 hours (~31 cols)
**Run order:** Lane 44 of next 4-prompt batch (mig_155)

---

## 0. Cleanliness & safety preamble (MUST READ)

Read §0 of the pathology_invasion prompt first — same governance rules apply:
1. `verification_method` strings must name LIVE `main.*` tables (pre-check `information_schema.tables`).
2. **AGENTS governance** — agent commits SQL only; Logan applies after Cowork independent verification.
3. Cohort-uniformity sweep BOTH directions on every BOOLEAN.
4. Pre-snapshot registry rows to `archive_pub_v1_0` before mutations.
5. Surgical git add — explicit paths only.

Lane-specific risk: this cluster has high cross-canonical dependency. Many cols are **derived** from existing verified canonicals (recurrence, dynamic risk response, molecular). Don't accept extraction-faithfulness against a Tier-1 LLM table when a verified canonical is the SSOT (mig_151 mistake).

---

## 1. Goal

Verify the **risk-scoring + survival + genetics-residual cluster** — 31 unverified cols on `canonical_patient_master` covering ATA risk staging, MACIS score, AMES score, survival aggregations, single-gene molecular markers, and resolved-layer provenance.

### 1a. Pre-flight probe (must return exactly 31)

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    'ames_calculable_flag','ames_risk','ames_risk_group',
    'ata_calculable_flag','ata_initial_risk','ata_response_calculable_flag',
    'ata_response_category','ata_response_is_provisional','ata_risk_calculable_flag','ata_risk_category',
    'biochemical_recurrence_flag','distant_mets_proxy','distant_mets_proxy_v2',
    'genetics_master_v1_episode_count','genetics_master_v1_link_flag',
    'macis_calculable_flag','macis_missing_components','macis_risk_group','macis_score',
    'resolved_at','resolved_days_from_surg','resolved_layer_version',
    'scoring_ajcc8_flag','scoring_ata_flag','scoring_macis_flag',
    'structural_recurrence_flag',
    'surv_max_time_days','surv_max_time_days_capped','surv_n_events','surv_recurrence_risk_band','surv_tg_annual_log_slope'
  )
ORDER BY column_name;
```

Confirm count is **exactly 31**. All must be `verification_status='not_started'`.

### 1b. Sub-clusters

- **ATA risk + ATA response (7 cols):** ata_calculable_flag, ata_initial_risk, ata_risk_calculable_flag, ata_risk_category, ata_response_calculable_flag, ata_response_category, ata_response_is_provisional
- **MACIS score (4 cols):** macis_calculable_flag, macis_missing_components, macis_risk_group, macis_score
- **AMES score (3 cols):** ames_calculable_flag, ames_risk, ames_risk_group
- **Scoring eligibility flags (3 cols):** scoring_ajcc8_flag, scoring_ata_flag, scoring_macis_flag
- **Survival aggregations (5 cols):** surv_max_time_days, surv_max_time_days_capped, surv_n_events, surv_recurrence_risk_band, surv_tg_annual_log_slope
- **Recurrence proxies (3 cols):** biochemical_recurrence_flag, structural_recurrence_flag, distant_mets_proxy + distant_mets_proxy_v2 (4 actually)
- **Resolved-layer provenance (3 cols):** resolved_at, resolved_days_from_surg, resolved_layer_version
- **Genetics residual (2 cols):** genetics_master_v1_episode_count, genetics_master_v1_link_flag

---

## 2. Methodology

### 2a. SSOT pointers (verify each lives in `main` first!)

Pre-check:
```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema='main' AND (
  table_name LIKE 'canonical_dynamic_risk%' OR table_name LIKE 'canonical_recurrence%'
  OR table_name LIKE 'canonical_molecular_genetics%' OR table_name LIKE 'canonical_path_malignant%'
  OR table_name LIKE 'canonical_survival%'
) ORDER BY 1;
```

Expected SSOTs:
- `canonical_dynamic_risk_response_*` for ATA response — verified per `project_round2_llm_integration_script_386_closeout.md`. **Critical:** ata_response is the dynamic-risk-response output, not the initial ATA risk.
- `canonical_recurrence_v1` (mig_123 rebuild, 87th canonical) for `biochemical_recurrence_flag` / `structural_recurrence_flag` / `distant_mets_proxy*` — these are recurrence *type* derivations from the recurrence canonical's `recurrence_type` field.
- `canonical_molecular_genetics_v2` for `genetics_master_v1_*` — verified.
- `canonical_path_malignant_events_v1` + `canonical_path_malignant_patient_rollup_v1` for ATA initial risk components (size, ETE, vascular, margin, LN involvement, distant mets).

### 2b. Per-sub-cluster derivation rules

**ATA initial risk (`ata_initial_risk` / `ata_risk_category` / `ata_risk_calculable_flag`):** This is the 2015 ATA Risk Stratification (low / intermediate / high). It's derived from primary tumor + nodal + distant features:
- `ata_calculable_flag` / `ata_risk_calculable_flag` should be the eligibility predicate (has_minimum_components_to_score). Verify against the source script (likely `scripts/53_*` or `scripts/2*_canonical_*`).
- `ata_initial_risk` should match `ata_risk_category` for backwards-compat (or document divergence). If they're identical, document `CF-mig155-ATA-INITIAL-VS-CATEGORY-DUP` as informational.

**ATA response (`ata_response_*`):** From `canonical_dynamic_risk_response_*`. `ata_response_is_provisional` = TRUE means insufficient labs/imaging at the response time horizon. **Don't treat as recurrence.**

**MACIS (`macis_*`):** Score from {Metastasis, Age, Completeness, Invasion, Size}. `macis_calculable_flag` = all components present. `macis_missing_components` = comma-separated list of missing fields. Re-derive against the source script.

**AMES (`ames_*`):** Older risk classification (Age, Metastasis, Extent, Size). `ames_risk` is numeric/binned; `ames_risk_group` is categorical. Verify ladder.

**Scoring eligibility flags (`scoring_*_flag`):** These are simple existence flags — TRUE if patient has all components for that scoring system. Verify these match the equivalent `*_calculable_flag` cols (likely identical / dual-named).

**Survival aggregations (`surv_*`):**
- `surv_max_time_days` = follow-up duration (raw)
- `surv_max_time_days_capped` = capped at study horizon (often 5 or 10 yr)
- `surv_n_events` = recurrence count (or death count — verify SSOT)
- `surv_recurrence_risk_band` = band from `surv_n_events` / time
- `surv_tg_annual_log_slope` = derived analytic from longitudinal Tg measurements

These should align with the `mig_141 survival cluster` outputs — cross-validate.

**Recurrence proxies (`biochemical_recurrence_flag` / `structural_recurrence_flag` / `distant_mets_proxy*`):** From `canonical_recurrence_v1` (514 confirmed) split by `recurrence_type`. Cross-validate sums against the canonical: `biochemical_recurrence_flag=TRUE` should equal patients with `recurrence_type='biochemical'` in the canonical.

**Resolved-layer provenance (`resolved_*`):** These document when the resolved-layer build ran. `resolved_at` is TIMESTAMP (allowlist OK as audit/provenance). `resolved_days_from_surg` is INTEGER. `resolved_layer_version` is VARCHAR — likely a single-value placeholder (e.g., 'v6'). Apply mig_142b VALUE-DEGENERATE-UPSTREAM CF if 1 distinct.

**Genetics residual (`genetics_master_v1_*`):** Provenance / link flags from `canonical_molecular_genetics_v2`.

### 2c. ⚠️ Cohort-uniformity sweep (REQUIRED — both directions)

Run the §2d-style template from the pathology-invasion prompt for every BOOLEAN. Watch for:
- `*_calculable_flag`: should be HIGH-TRUE (most patients have enough data). If <50% TRUE, investigate why.
- `*_provisional`: should be MIXED (some patients have follow-up, some don't).
- `biochemical_recurrence_flag` should be 1-3% TRUE (rare); `structural_recurrence_flag` 4-7% TRUE; `distant_mets_proxy*` <1% TRUE. If 0 TRUE → reclassify to `na`.

### 2d. ⚠️ Numeric measurement data-type sanity

`macis_score` and `ames_risk` should be DOUBLE/INTEGER. `surv_tg_annual_log_slope` MUST be DOUBLE (slope is real-valued). Flag any VARCHAR-with-units.

### 2e. ⚠️ Calendar-only date check

`resolved_at` is TIMESTAMP — allowlist (audit/provenance). No `*_date` cols in this set.

### 2f. Cross-source spot-check (REQUIRED)

- Pick 5 random rids with `ata_risk_category='high'`. Verify the underlying components (ETE, large size, distant mets, etc.) on path/recurrence canonicals.
- Pick 5 rids with `biochemical_recurrence_flag=TRUE`. Verify `canonical_recurrence_v1.recurrence_type='biochemical'` for those rids.
- Pick 5 rids with `surv_n_events > 0`. Verify against canonical_recurrence_v1 row counts.

---

## 3. Sign-off SQL

File: `qc_framework_v1/migrations/155_patient_master_risk_scoring_survival_genetics_cluster_signoff_20260429.sql`

```
batch_id = 'mig_155_patient_master_risk_scoring_survival_genetics_cluster_20260429'
verification_method options:
  derivation_vs_canonical_dynamic_risk_response_v1
  derivation_vs_canonical_recurrence_v1
  derivation_vs_canonical_path_malignant_patient_rollup_v1
  derivation_vs_canonical_molecular_genetics_v2
  derivation_vs_helper_script_<N>_<scoring_system>
  internal_consistency_<rule>
  helper_<placeholder>_pending_real_extraction
```

Sub-blocks:
- 155a — ATA initial risk + risk category (3 cols)
- 155b — ATA response (4 cols)
- 155c — MACIS (4 cols)
- 155d — AMES (3 cols)
- 155e — Scoring eligibility flags (3 cols)
- 155f — Survival aggregations (5 cols)
- 155g — Recurrence proxies (4 cols)
- 155h — Resolved-layer provenance (3 cols)
- 155i — Genetics residual (2 cols)
- 155j — Resync `canonical_table_signoff_registry_v1`

### 3a. Pre-snapshot block at top

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig155_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig155_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE table_name='canonical_patient_master' AND column_name IN (<31 cols>);
```

---

## 4. Required CFs

- `CF-mig155-ATA-INITIAL-VS-CATEGORY-DUP` — open if ata_initial_risk = ata_risk_category 100% rows
- `CF-mig155-COHORT-UNIFORM-FALSE-<col>` — list each near-uniform-FALSE col reclassified
- `CF-mig155-COHORT-NEAR-UNIFORM-TRUE-<col>` — list each Type-A presence flag kept verified
- `CF-mig155-MACIS-MISSING-COMPONENTS-LIST` — single-value VARCHAR? value-degenerate?
- `CF-mig155-RESOLVED-LAYER-VERSION-DEGENERATE` — open if 1 distinct value
- `CF-mig155-RECURRENCE-PROXY-VS-CANONICAL-V1` — confirm flags reconcile against canonical_recurrence_v1
- `CF-mig155-SURV-VS-MIG141-CROSS` — confirm surv_* aligns with mig_141 survival_cluster outputs
- `CF-mig155-DATE-RETYPE-CLEAR` — confirm no `*_date` cols requiring DATE retype (resolved_at TIMESTAMP allowlist OK)

---

## 5. Apply + verify (Logan-only after Cowork independent verification)

Same as Lane 43 §5. NO MD writes from agent.

---

## 6. Git workflow

```bash
git add qc_framework_v1/migrations/155_patient_master_risk_scoring_survival_genetics_cluster_signoff_20260429.sql
git -c user.name="Logan Glosser" -c user.email="logan.glosser@gmail.com" commit -m "qc: mig_155 CPM risk-scoring + survival + genetics cluster sign-off (31 cols)"
git push origin main
```

---

## 7. Done definition

- [ ] Pre-flight probe returns exactly 31
- [ ] All 31 cols flipped
- [ ] Methodology distribution clean (no `_misc` placeholders)
- [ ] Cohort-uniformity sweep documented for every BOOLEAN
- [ ] Cross-canonical reconciliation evidence in migration header (recurrence proxy ↔ canonical_recurrence_v1; surv_* ↔ mig_141)
- [ ] Pre-snapshot created in archive_pub_v1_0
- [ ] No verification_method strings name dead/archived tables
- [ ] SQL file committed + pushed; NO MD writes from agent
