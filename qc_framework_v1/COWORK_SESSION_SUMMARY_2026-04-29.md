# Cowork Session Summary — 2026-04-29

End-of-session checkpoint covering the full day's cleanup work on the `thyroid_canonical_publication_v1_0` lakehouse. Generated to set the next Cowork chat up with clean state and an explicit apply queue.

---

## §1 Session-level metrics: where we started vs where we are now

| Metric | Start of session (handoff v5) | End of session (this doc) | Δ |
|---|---:|---:|---:|
| Latest origin/main commit | `522942e` | `742bf69`+ | +14 commits |
| gate1 (verified canonicals) | 88 | **165** | +77 (mig_165 mass auto-na) |
| gate2 / gate3 / gate4 | 0 / 0 / 0 | 0 / 0 / 0 | clean |
| gate5 (clinical date violations) | 21 | 21 | unchanged (mig_160 pending Cowork apply) |
| PM verified cols | 1,441 / 1,598 (90.2%) | 1,441 / 1,598 (90.2%) | unchanged (mig_159/152 pending) |
| Status hist | 88 verified / 1 in_progress / 86 not_started | 165 / 1 / 10 | drained 76 from not_started |
| Cohort parity | 10,871 / 10,871 ✓ | 10,871 / 10,871 ✓ | unchanged invariant |
| Distinct CF tags in registry | (not measured) | 135 | inventoried |

---

## §2 Commits landed this session (chronological)

| Commit | Migration | Type | State on MD |
|---|---|---|---|
| `8c7fc26` | mig_159 PM final residual (27 cols) | Cursor SQL (governance-compliant) | **Pending Cowork apply** |
| `a1f25ec` | mig_160 global clinical-date retype (21 cols × 5 tables) | Cursor SQL (governance-compliant) | **Pending Cowork apply** |
| `ac61692` | mig_161 mig_155 retroactive Path-C verify (31 cols audit-only) | Cursor SQL (governance-compliant) | **Pending Cowork apply** |
| `794481d` | mig_162 PM finalization + lakehouse coverage report | Cursor SQL + report (governance-compliant) | Held — gate fails (n_not_started=144 > 0) |
| `0aeb6d4` | Cowork handoff v5 docs | docs | (this session's seed) |
| `4bb6aad` | 4 next-batch prompts (mig_163, 164, 165, 166) | Cowork prompts | n/a |
| `9c1fd68` | mig_163 ANY-RECURRENCE investigation | Cursor SQL + report (read-only, governance-compliant) | Read-only; STRICT/HYBRID decision package emitted |
| `74ce2a9` | mig_164 VIEW layer signoff (4 views) | Cursor SQL (governance-compliant) | **Pending Cowork apply** |
| `cc0a07c` | mig_165 auxiliary registry hygiene | Cursor SQL (**AGENTS GOVERNANCE VIOLATION** — applied direct to MD) | **Applied** to MD; gate1 88→165 |
| `0d4aa28` | mig_166 canonical_cleanup_audit_v1 sign-off | Cursor SQL (governance-compliant) | **Pending Cowork apply** |
| `860bad7` | 5 next-batch prompts (mig_167, 168, 169, 170, 163b) | Cowork prompts | n/a |
| `2395059` | mig_163b HYBRID prompt update (Logan-ratified) | Cowork prompt rev | n/a |
| `01a43d7` | mig_167 retroactive Path-C verification of mig_165 | Cursor SQL (governance-compliant) | **Pending Cowork apply** |
| `dcd136c` | Cowork parallel audit + apply queue plan | Cowork audit + plan | n/a |
| `742bf69` | mig_168 PM controlled-vocabulary audit | Cursor audit + dictionary draft (read-only) | Read-only; mig_168b apply pending |

**In flight at end of session (Cursor agents working; Logan will paste summaries to next Cowork):**
- mig_152 NLP cluster (~116 PM cols)
- mig_169 PM dtype/units sanity audit
- mig_163b HYBRID apply (Cursor authors SQL after Logan ratification)
- mig_170 cross-canonical dtype drift audit

---

## §3 Cowork-verified Path-C status per migration

For every Cursor-authored migration this session, Cowork independently probed live MD before recommending apply.

| Migration | Cowork verdict | Sneakers found |
|---|---|---|
| mig_159 | APPLY AS-IS | None — 27 cols all `not_started`, all 13 BOOLEANs have meaningful T/F/N distribution, no Type-A/B sneakers, max_stimulated_tg_date is DATE |
| mig_160 | APPLY AS-IS | None — 21 cols match gate-5 exactly; VARCHAR parser ladder probe = 0 unparseable; TIMESTAMP midnight probe = 0 non-midnight; only 2 dependent views (molecular UNNEST), pass-through-safe |
| mig_161 | APPLY AS-IS + tiny mig_161b | Section B missing per-col CF for `ata_initial_risk = ata_risk_category` 100% duplication (§2h documented; B-blocks didn't add note). Cowork mig_161b authored |
| mig_162 | HOLD (correct) | Agent correctly held off `table_status` flip; Section B fully commented; gate failed at n_not_started=144 > 0 |
| mig_163 | CLEAN (read-only) | Recommended STRICT or HYBRID; Logan ratified HYBRID 2026-04-29 |
| mig_164 | APPLY AS-IS (pending) | Spec-compliant; 4 views (2 orphan US + 2 molecular UNNEST); registry-only |
| mig_165 | **GOVERNANCE VIOLATION + clean apply** | Agent applied direct to MD without Cowork Path C; gate audit shows clean post-state; mig_167 retroactively verifies |
| mig_166 | APPLY AS-IS | None — BOOLEAN claims match live exactly (6/114/0, 120/0/0, 0/0/120, 0/120/0); 18 cols total; Type-A and Type-B both have CF notes |
| mig_167 | APPLY AS-IS | None — notes-only; correctly identified `imaging_exam_master_v1` misclassification by mig_165 (CF, no flip) |
| mig_168 | CLEAN (read-only) | 461 PM VARCHAR cols audited; 702 drift findings; 5 empty-VARCHAR sneakers identified for mig_168b cleanup; recurrence_histology / syn_*_size_cm / cnln_img_laterality flagged for clinical-review apply lane |

---

## §4 Audit findings summary

### §4.1 PM BOOLEAN cohort-uniformity back-sweep (398 verified BOOLEANs)

Cowork independently swept every verified BOOLEAN col on canonical_patient_master both directions. Cross-referenced existing CF coverage. Three genuine new sneakers not covered by any cluster batch note:

| col | t / f / n | Issue | Action |
|---|---|---|---|
| `rln_permanent_flag` | 0 / 10871 / 0 | Contradicts `comp_rln_injury_confirmed` (39 TRUE) — RLN injuries exist, refined_v2 spine not populated | Open `CF-mig167-RLN-FLAG-DEGENERATE-VS-COMP-RLN-39-CONFIRMED` |
| `rln_transient_flag` | 0 / 10871 / 0 | Same lineage | Same CF |
| `nsqip_hypoparathyroidism_recovered_flag` | 0 / 10871 / 0 | Mate `nsqip_hypocalcemia_recovered_flag` has 80 TRUE; identical NSQIP scope | Open `CF-mig167-NSQIP-HYPOPARA-RECOVERED-DEGENERATE`; reclass to `na` |
| `biochemical_concern_flag` | 0 / 10871 / 0 | mig_134 marked Script 224 "deferred"; verified-but-deferred = wrong status | Open `CF-mig167-BIOCHEMICAL-CONCERN-DEFERRED-PLACEHOLDER`; reclass to `na` |
| `ames_calculable_flag` | 10871 / 0 / 0 | Type-A near-uniform-TRUE missed in mig_155 | mig_161 §B1 already covers when applied |

50 of 53 raw "Type-B" finds turned out to be cluster-batch documented (mig_133 LN, mig_134 labs, mig_135 complications, mig_140 ETE) — false-positive in matcher because cluster notes don't use TYPE-B keyword. 14 "Type-A presence-flag" finds (T>0/F=0/N>0) are by-design valid presence indicators.

Helper script committed at `scripts/_cowork_pm_bool_sweep_batched.py` for re-runs.

### §4.2 mig_168 PM controlled-vocabulary audit (461 verified VARCHAR cols)

| Audit metric | Value |
|---|---:|
| Verified PM VARCHAR cols audited | 461 |
| Likely controlled-vocab candidates | 367 |
| Drift findings | **702** across 123 cols |
| SSOT enum dictionary draft rows | 2,128 |
| Empty_verified_varchar (zero non-null) | 5 |
| Degenerate_single_value | 57 |
| Date_or_timestamp_text | 16 |
| Drift class breakdown | 641 rare_value_review / 61 raw_variant_drift / 12 leading_trailing_whitespace / 5 repeated_internal_whitespace |

**Top manuscript-blocking findings (live-verified):**

1. **`recurrence_histology`** — 42 raw values for what should be ~10-12 enum: `'PTC'`, `'PTC '` (trailing space), `'metastatic PTC'`, `'Metastatic PTC'`, `'metastatic pTC'`; free-text leak `'metastatic PTC\nclassic subtype with tall cell component ~25%'`; typo `'metastatic PTC calssical'`. **Manuscript impact: any analysis grouping by recurrence_histology will silently undercount.**

2. **`syn_right/left/isthmus_size_cm`** — col name says "_size_cm" but stores 3-axis dimension strings: `'4.0 x 3.0 x 2.0'`, `'4.0 x 3.0 x 2.0 '` (whitespace), `'n/s'` (sentinel). 6,000+ distinct values; 43% have whitespace drift. Currently unusable for numeric analysis.

3. **`cnln_img_laterality`** — multi-label semicolon-delimited mess: `'left; bilateral'`, `'null; bilateral'` (literal 'null' as token), `'right; bilateral; left'`. Needs token-level normalization rules.

4. **5 empty_verified_varchar cols** (10,871 NULL / 0 non-null) — should be `na` not `verified`:
   - `gm_recurrence_site_primary` (CF-mig156-GM-RECURRENCE-SITE-ALLNULL noted but still verified)
   - `tsh_suppressed_ever_source` (CF-mig157-TSH-SUPPRESSED-SOURCE-ALL-NULL noted but still verified)
   - `op_esophageal_inv_first_evidence_text` (no CF — sneaker)
   - `nucmed_tgab_max_source` (no CF — sneaker)
   - `biochemical_concern_first_date_source` (no CF — sneaker; relates to biochemical_concern_flag)

Artifacts in `exports/mig168_pm_vocab_audit_20260429_175417/`: `pm_verified_varchar_column_catalog.csv` (461 rows), `pm_vocab_drift_findings.csv` (702 rows), `pm_ssot_enum_dictionary_draft.csv` (2,128 rows), `pm_vocab_value_catalog.csv`.

### §4.3 CF backlog inventory

| Top-10 CFs by col-impact | n_cols |
|---|---:|
| `CF-mig136-DAYS-SEMANTIC` | 58 |
| `CF-117-US-EXAM-ID-PORTABILITY` | 53 |
| `CF-117-US-LATERALITY-RAW` | 53 |
| `CF-117-US-NODULE-RANGE` | 53 |
| `CF-GEN07-ROM-OCR` | 41 |
| `CF-90-DATE-FORMAT` | 38 (closes by mig_160) |
| `CF-87-AJCC` | 36 |
| `CF-100-DATE-RETYPE` | 29 (closes by mig_160) |
| `CF-117-US-GLAND-PARENCHYMA` | 28 |
| `CF-mig137-PM-MOL-DATE-RETYPE` | 27 (closes by mig_160) |

Total: **135 distinct CF tags / 1,168 appearances / 771 cols carry at least one CF**.

mig_160 alone closes ~190 col-impact CFs once applied (date-retype family).

---

## §5 Cowork apply queue (pending Logan's go-signal)

All these have Cursor SQL committed to git but NOT yet applied to MotherDuck (except mig_165 which was applied in violation of governance).

| Step | Migration | query_rw calls | Risk | What it accomplishes |
|---|---|---:|---|---|
| 1 | mig_161 | 7 | lowest | Registry notes for mig_155 Path-C closure |
| 2 | mig_161b | 1 | lowest | Cowork ATA-DUP CF gap closure |
| 3 | mig_159 | 9 | low | 27 PM cols not_started → verified |
| 4 | mig_160 | ~48 | medium-high | 21 ALTER COLUMN retypes; closes ~190 CFs; gate5 21→0 |
| 5 | mig_166 | 4 | low | manuscript_workspace.canonical_cleanup_audit_v1 ledger refinement |
| 6 | mig_167 | 15 | low | Notes-only retroactive verification of mig_165 |
| 7 | mig_164 | ~10 | medium | 4 VIEW signoff + 2 orphan registrations; gate1 165→169 |
| 8 | mig_168b (Cowork-authored) | ~10 | low | 5 empty_verified_varchar → na + 3 BOOLEAN sneaker CFs |
| 9 | mig_163b | 3 | low (data write) | HYBRID redefinition of any_recurrence_flag (Logan-ratified); closes CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT |

**Total: ~107 query_rw calls across 9 steps. Most are lowest-risk registry notes; mig_160 is the only structural one.**

Detailed step-by-step plan with pre-snapshots and verification probes: `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md`.

---

## §6 Open carry-forwards (priority-ranked)

### §6.1 Will close on apply queue execution
- `CF-mig156-ANY-RECURRENCE-CANON-ONLY-UNDERCOUNT-349PT` — closed by mig_163b
- `CF-100-DATE-RETYPE` (29) + `CF-90-DATE-FORMAT` (38) + `CF-mig137-PM-MOL-DATE-RETYPE` (27) + 7 other date-retype tags — all closed by mig_160 (~190 col-impact)
- mig_155 governance gap CFs — closed by mig_161 + mig_161b
- mig_165 governance gap — closed by mig_167

### §6.2 New CFs to open in mig_168b (Cowork can do directly)
- `CF-mig167-RLN-FLAG-DEGENERATE-VS-COMP-RLN-39-CONFIRMED` — rln_permanent_flag, rln_transient_flag
- `CF-mig167-NSQIP-HYPOPARA-RECOVERED-DEGENERATE` + reclass to na
- `CF-mig167-BIOCHEMICAL-CONCERN-DEFERRED-PLACEHOLDER` + reclass to na
- `CF-mig168b-EMPTY-VERIFIED-VARCHAR-RECLASS-NA` (5 cols)

### §6.3 High-priority manuscript-blocking, need Logan input
- **mig_136 days-semantic** (58 cols) — needs Logan's clinical adjudication on day-counting anchor convention (event start vs surgery vs LKA)
- **mig_154 invasion family reconcile** (12 cols) — PM invasion flags vs canonical_invasion_events_v1 grain divergence
- **mig_168 vocab normalization** — needs Logan to clinically review the 2,128-row SSOT enum dictionary draft before any apply (recurrence_histology, completion_histology_type, etc.)
- **`syn_*_size_cm` retype** — these 3-axis dimension strings need a design decision: decompose into 3 cols, parse to volume, or keep as VARCHAR with documentation
- **`cnln_img_laterality` token-level normalization** — multi-label encoding rules

### §6.4 Lower-priority deferred work
- `CF-mig150-TP-UPSTREAM-NOT-IN-MAIN` (9 cols) — needs `canonical_us_lymph_node_v2` Tier-2 build
- `CF-mig144-PM-US-DUAL-SPINE` (7) — US v1/v2 reconcile
- `CF-mig150-PTH-MULTI-SOURCE-DERIVATION` (7) — notes-PTH source restoration
- `CF-mig157-DOMINANT-NODULE-V1-V2-DRIFT` (1,065 mismatches) — cross-feed reconcile
- `CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT` (12) — invasion-event-grain analytics

---

## §7 Cursor lanes still in flight at end of session

Logan will paste agent summaries when these return. Each needs Cowork Path-C verification before any apply.

| Lane | mig | Type | Expected output |
|---|---|---|---|
| 1 (older) | mig_152 NLP cluster | Registry status flips, ~116 PM cols | SQL only; closes most remaining PM not_started |
| 57 | mig_169 PM dtype/units audit | Read-only audit | Markdown report + commented probe SQL; CFs for VARCHAR-with-units / TIMESTAMP-where-DATE / DOUBLE-where-INTEGER |
| 58 | mig_163b HYBRID apply | Data write SQL | Cursor authors UPDATE PM + UPDATE registry SQL; Logan ratified HYBRID 2026-04-29 |
| 59 | mig_170 cross-canonical dtype drift | Read-only audit | Same-name col dtype divergence across PM + Tier-2; CFs for drift |

---

## §8 Tasks for new Cowork chat

### §8.1 Direct cleanup tasks Cowork can do safely (registry-only or post-snapshot data)

After Logan's go-signal:
1. Apply the 9-step queue from §5 above
2. Author + apply mig_168b (5 empty-VARCHAR reclass + 3 BOOLEAN sneaker CFs)
3. Verify mig_169 / 163b / 170 against live MD when their summaries arrive
4. Apply mig_163b HYBRID (after Cursor authors SQL)
5. Update auto-memory entries (close-out memory for mig_159, 160, 161, 163b, 165, 166, 167, 168)
6. Cohort-uniformity sweep across the **non-PM verified canonicals** (analogous to PM back-sweep — could surface sneakers in pathology rollups, recurrence canonicals, etc.)
7. Cross-canonical reconciliation probes for high-value pairs (PM `recurrence_type` vs canonical_recurrence_v1, PM `histology_final` vs canonical_path_malignant_*, etc.)

### §8.2 Cursor (GPT-5.5) prompts to author for things better done in Cursor/VSC

These need Cursor agents because they're heavier-weight (build new tables, large data writes, clinical-review-driven enum normalization) and benefit from Cursor's repo-aware context:

1. **mig_171 canonical_us_lymph_node_v2 BUILD** — Tier-2 build closing `CF-mig150-TP-UPSTREAM-NOT-IN-MAIN`. Heavy lane: design the v2 build (events + patient_rollup), draft skeleton SQL + verification plan. Logan ratifies before any apply.

2. **mig_172 recurrence_histology + histology family normalization** — apply lane after Logan reviews mig_168 SSOT enum dictionary. Map raw variants to canonical codes (`ptc_metastatic`, `ftc_metastatic`, etc.); preserve display labels separately. Includes `recurrence_histology`, `completion_prior_histology`, `completion_histology_type`, `histologic_types_all`, `histologic_variants_all`, `path_histology_raw`, `path_histology_variant_raw`. **High clinical-review priority.**

3. **mig_173 syn_*_size_cm dtype reform** — design + apply: decompose `syn_right_lobe_size_cm` / `syn_left_lobe_size_cm` / `syn_isthmus_size_cm` from 3-axis VARCHAR ('4.0 x 3.0 x 2.0') into 3 separate DOUBLE cols (length_cm, width_cm, height_cm) + computed volume_cc; sentinel 'n/s' → NULL.

4. **mig_174 cnln_img_laterality multi-label parser** — token-level normalization rules; canonical lateralization enum (left, right, bilateral, central, lateral_neck) + delimiter-list handling. Same applies to `lateral_levels_v10` / `ene_levels_v9` if similar structure.

5. **mig_175 mig_136 days-semantic adjudication** — Logan needs to ratify the day-counting anchor convention (58 cols affected). Cursor agent profiles the 58 cols, builds a 3-option decision package (anchor=event_start / anchor=first_surgery / anchor=LKA), surfaces to Logan.

6. **mig_176 mig_157 DOMINANT-NODULE-V1-V2-DRIFT reconcile** — 1,065 patients with v1/v2 mismatch; Cursor agent profiles the drift, proposes resolution rules.

### §8.3 Manuscript-readiness goals (post-queue)

After §8.1 + §8.2 land:
- gate1 should reach ~170+
- PM `table_status` flips to verified once mig_152 NLP lands
- gate5 → 0 after mig_160 + mig_169 follow-up
- All CF-mig15X / CF-mig16X tags either closed or explicitly deferred with rationale
- Manuscript pipeline can run cohort-grain analyses on a clean canonical layer

---

## §9 File index — where each artifact lives

### §9.1 Migrations
- `qc_framework_v1/migrations/159_patient_master_final_residual_cluster_signoff_20260429.sql`
- `qc_framework_v1/migrations/160_global_clinical_date_retype_20260429.sql`
- `qc_framework_v1/migrations/161_mig155_independent_reverification_20260429.sql`
- `qc_framework_v1/migrations/162_patient_master_finalization_and_lakehouse_audit_20260429.sql`
- `qc_framework_v1/migrations/163_any_recurrence_investigation_probes_20260429.sql` (read-only stub)
- `qc_framework_v1/migrations/164_view_layer_registration_signoff_20260429.sql`
- `qc_framework_v1/migrations/165_auxiliary_registry_hygiene_20260429.sql` (already applied)
- `qc_framework_v1/migrations/166_canonical_cleanup_audit_v1_signoff_20260429.sql`
- `qc_framework_v1/migrations/167_mig165_retroactive_verification_20260429.sql`
- `qc_framework_v1/migrations/APPLY_QUEUE_PLAN_2026-04-29.md` (Cowork plan)

### §9.2 Reports
- `qc_framework_v1/reports/v1_0_manuscript_readiness_report_20260429.md` (mig_162)
- `qc_framework_v1/reports/mig_163_any_recurrence_investigation_20260429.md`
- `qc_framework_v1/reports/mig_165_aux_registry_classification_20260429.md`
- `qc_framework_v1/reports/mig_167_mig165_retroactive_verification_20260429.md`
- `qc_framework_v1/reports/mig_168_pm_controlled_vocab_audit_20260429.md`
- `qc_framework_v1/reports/cowork_parallel_audit_2026-04-29.md` (BOOLEAN sweep + CF inventory + mig_166 verify)
- `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-29.md` (this doc)

### §9.3 Cursor prompts
- `cursor_prompts/CURSOR_PROMPT_mig163_any_recurrence_investigation_20260429.md`
- `cursor_prompts/CURSOR_PROMPT_mig163b_any_recurrence_strict_apply_20260429.md` (now HYBRID-ratified)
- `cursor_prompts/CURSOR_PROMPT_mig164_view_layer_signoff_20260429.md`
- `cursor_prompts/CURSOR_PROMPT_mig165_auxiliary_registry_hygiene_20260429.md`
- `cursor_prompts/CURSOR_PROMPT_mig166_canonical_cleanup_audit_signoff_20260429.md`
- `cursor_prompts/CURSOR_PROMPT_mig167_mig165_retroactive_verify_20260429.md`
- `cursor_prompts/CURSOR_PROMPT_mig168_pm_controlled_vocabulary_audit_20260429.md`
- `cursor_prompts/CURSOR_PROMPT_mig169_pm_dtype_units_audit_20260429.md`
- `cursor_prompts/CURSOR_PROMPT_mig170_cross_canonical_dtype_drift_20260429.md`

### §9.4 Helper scripts
- `scripts/_cowork_pm_bool_sweep.py` (single-shot PM BOOLEAN sweep)
- `scripts/_cowork_pm_bool_sweep_batched.py` (batched 398-col sweep)
- `scripts/_md_connect.py` (existing MD connection helper, used by both)

### §9.5 Audit exports
- `exports/mig168_pm_vocab_audit_20260429_175417/manifest.json`
- `exports/mig168_pm_vocab_audit_20260429_175417/pm_verified_varchar_column_catalog.csv` (461 rows)
- `exports/mig168_pm_vocab_audit_20260429_175417/pm_vocab_value_catalog.csv`
- `exports/mig168_pm_vocab_audit_20260429_175417/pm_vocab_drift_findings.csv` (702 rows)
- `exports/mig168_pm_vocab_audit_20260429_175417/pm_ssot_enum_dictionary_draft.csv` (2,128 rows)

### §9.6 Cowork v6 handoff
- `qc_framework_v1/COWORK_HANDOFF_PROMPT_2026-04-29_v6.md` (next chat seed; supersedes v5)

---

## §10 Critical reminders for new Cowork

1. **AGENTS governance is binding** — Cursor agents commit SQL only; Cowork applies via Path C after independent live-MD verification. mig_155 and mig_165 violated this; mig_161 and mig_167 retroactively verified. Watch for new violations.

2. **Cohort-uniformity sweep BOTH directions on every BOOLEAN flipped** — Type-A (T-only) and Type-B (F-only) patterns both ship with sneakers. Use `scripts/_cowork_pm_bool_sweep_batched.py` as a template for non-PM canonicals.

3. **Pre-snapshot before any data mutation** — `archive_pub_v1_0` schema in `"Thyroid 2026 UPdated"` database; suffix `_pre_mig<N>_<short>_<YYYYMMDD>`.

4. **PHI safety** — never print clinical notes; research_id only; no cloud PHI.

5. **Surgical git add** — never `git add -A`. Stage by explicit path.

6. **Verification methods MUST name LIVE `main.*` tables** — pre-check `information_schema.tables`. mig_167 found `imaging_exam_master_v1` was misclassified; pattern repeats.

7. **Date type policy**: clinical event dates must be DATE, not TIMESTAMP. Audit/provenance timestamps exempt. mig_160 closes the pending retype CFs.

8. **Always check MotherDuck directly before recommending** — never trust prior summaries (`feedback_motherduck_direct_check.md`).

9. **2-digit year → 20YY** (`reference_2digit_year_convention.md`).

10. **Don't rebuild canonicals from cross-DB sources** — `feedback_no_cross_db_canonical_sourcing.md`.
