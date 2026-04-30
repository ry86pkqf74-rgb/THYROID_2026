# §8 retro Path-C audits close-out — mig_180b / mig_181 / mig_177c

**Date:** 2026-04-30 (early morning, post-handoff)
**Auditor:** Cowork (independent verification)
**Verdict:** **All 3 audits PASS.** No b-cleanup migrations required.

Companion to `section_8_retro_audits_closeout_20260429.md` (mig_178 / mig_173b / mig_163b). Same governance pattern: Cursor agents committed AND applied SQL to MotherDuck despite the prompts' "DO NOT execute against MotherDuck" rule. Cowork retroactively verified each — all clean — and writes this close-out so the work is properly logged and registry artifacts have traceable provenance.

---

## §1 mig_180b NLP UPSTREAM-MISSING family lineage closure (commit `8e89120`)

**Cursor claim:** 12/12 NLP family CFs closed via `mig_180b_*` lineage discovery; 5 exact-replay (complications/genetics/medications/problemlist/staging) all `mismatch_total=0`; 7 source-located strict-subset (funcoutcome 956 mismatches / imaging 390 / labs 188 / physexam 688 / ptdecision 117 / radtx 17 / usnodule 3); pre-snapshot at `archive_pub_v1_0.canonical_column_verification_registry_pre_mig180b_20260429`; validation table `main.val_mig180b_nlp_upstream_lineage_v1` materialized; CPM invariants (10,871 / 10,871) preserved; no PM data values mutated.

### Audit results

| Check | Live MD | Verdict |
|---|---|---|
| Pre-snapshot row count | 38 | ✓ matches |
| `val_mig180b_nlp_upstream_lineage_v1` row count | 12 | ✓ matches |
| `val_mig180b_*` schema | 16 cols (family / n_cols / source_status / source_catalog / source_schema / source_table / source_kind / source_rows / source_patients / metrics_tested / mismatch_total / exact_replay_pass / closure_decision / carry_forward_closed / notes / audited_at) | ✓ |
| `carry_forward_closed=TRUE` for all 12 families | 12/12 | ✓ |
| Cols with `mig_180b CLOSED` note | 38 | ✓ matches expected |
| Cols with `exact_replay_pass=TRUE` content | 10 | ✓ (5 families × 2 cols each) |
| Cols with `exact_replay_pass=FALSE` content | 28 | ✓ (7 families × 4 cols each) |
| Independent rerun: nlp_ne_complications PM rid count = source rid count | 2,840 = 2,840 | ✓ exact |
| Independent rerun: nlp_ne_complications PM sum(n_rows) = source row total | 9,359 = 9,359 | ✓ exact |
| `canonical_patient_master` data values mutated | 0 cols mutated | ✓ registry-only writes |
| CPM invariants | 10,871 / 10,871 | ✓ |

**Verdict: ✅ VERIFIED CLEAN.** mig_180b's lineage decisions are sound. The 5 exact-replay families now have `derivation_vs_<archived_source>` traceability; the 7 strict-subset families have lineage pointers + documented mismatch counts (carry-forwards retained for downstream awareness). No PM data drift.

---

## §2 mig_181 PM `syn_*_size` 15 not_started cols verify + apply (commit `ff1af15`)

**Cursor claim:** 15 typed cols (right + left + isthmus × length_cm/width_cm/height_cm/volume_cc/parse_status) flipped to `verified` with `verification_method = derivation_vs_syn_size_legacy_raw_parse_pipeline`; 3 legacy_raw cols already at `na`; volume formula rectangular `L × W × H`; parse_status healthy multi-valued enum; PM signoff resync 1,575 → 1,590 / 16 → 1; pre-snapshot at `archive_pub_v1_0.canonical_column_verification_registry_pre_mig181_20260429`; provenance row `canonical_cleanup_mig181_pm_syn_size_cols_verify_apply_20260429` inserted; CPM 10,871 / 10,871 / 0 null cpm_built_at preserved.

### Audit results

| Check | Live MD | Verdict |
|---|---|---|
| Pre-snapshot row count (15 not_started + 3 na) | 18 (15 not_started + 3 na) | ✓ matches |
| Col-level distribution post-mig_181 | 1,590 verified / 24 na / 1 not_started / 1,615 total | ✓ matches |
| Signoff registry post-mig_181 | n_verified=1,590 / n_na=24 / n_not_started=1 / signoff_migration=mig_181_*.sql / signed_off_ts=2026-04-29 20:25:03 | ✓ matches |
| Volume formula: right lobe `n_volume_mismatch` (vs `length × width × height`) | 0 of 6,787 nonnull | ✓ |
| Volume formula: left lobe | 0 of 6,916 nonnull | ✓ |
| Volume formula: isthmus | 0 of 3,679 nonnull | ✓ |
| `parse_status` multi-valued enum (right) | parsed_3axis 6,787 / parsed_partial 8 / sentinel 39 / unparsed 224 / NULL 3,813 | ✓ healthy |
| `parse_status` multi-valued enum (left) | parsed_3axis 6,916 / parsed_partial 11 / sentinel 33 / unparsed 244 / NULL 3,667 | ✓ |
| `parse_status` multi-valued enum (isthmus) | parsed_3axis 3,679 / parsed_partial 107 / sentinel 2 / unparsed 193 / NULL 6,890 | ✓ |
| Provenance row | `canonical_cleanup_mig181_pm_syn_size_cols_verify_apply_20260429` present | ✓ |
| CPM invariants | 10,871 / 10,871 / 0 null cpm_built_at | ✓ |

**Verdict: ✅ VERIFIED CLEAN.** Rectangular volume convention applied uniformly with zero arithmetic drift across all 3 lobes; 17,382 patients in scope. parse_status enum is healthy (not Type-A or Type-B).

**Existing CFs reaffirmed (informational):**
- `CF-mig173-PARSE-COVERAGE-LT-100PCT-PER-COL` — small unparsed populations remain (right 224, left 244, isthmus 193); standard partial coverage, not blocking.
- `CF-mig181-SYN-SIZE-VOLUME-FORMULA-RECTANGULAR` — pairs with `CF-mig173b-VOLUME-FORMULA-CONVENTION`; manuscript volume analyses must consciously choose rectangular vs ellipsoid (π/6 × L × W × H).
- `CF-mig181-SYN-SIZE-ZERO-AXIS-EDGECASE` — 3 source strings with literal `0` axis values preserved by parser; patients flagged in audit, retained for clinical review.

---

## §3 mig_177c LVI+VI derivative reclean scoping (commit `7210f80`)

**Cursor claim:** 2,502 LVI + 2,580 VI TRUE→FALSE flippers confirmed against mig_177b pre-snapshot; 99 LVI + 60 VI FALSE/NULL→TRUE flippers; Option A clear-only impact 7,464 LVI + 20,635 VI cells; Option B blocked because `canonical_invasion_events_v1` lacks ordinal grade and vessel-count cols; recommendation = Option A; pre-author placeholder skeleton SQL only.

### Audit results

| Check | Live MD | Verdict |
|---|---|---|
| Rows in `canonical_column_verification_registry_v1` with `batch_id LIKE 'mig_177c%'` | 0 | ✓ no MD writes |
| New tables in `thyroid_canonical_publication_v1_0` matching `%mig177c%` | 0 | ✓ no MD writes |
| mig_177b pre-snapshot integrity | 10,871 rows | ✓ cohort match |
| Definitive LVI TRUE→FALSE flipper count via pre-snapshot JOIN | 2,502 | ✓ matches scope |
| Definitive LVI FALSE/NULL→TRUE flipper count | 99 | ✓ matches scope |
| Definitive VI TRUE→FALSE flipper count | 2,580 | ✓ matches scope |
| Definitive VI FALSE/NULL→TRUE flipper count | 60 | ✓ matches scope |

**Verdict: ✅ VERIFIED CLEAN.** Read-only governance was honored (despite the same prior-round pattern of Cursor running its own apply against MD). Scope arithmetic matches the live MD truth exactly. Logan-ratified Option A; the apply lane is queued via `cursor_prompts/CURSOR_PROMPT_mig177c_apply_option_a_clear_only_20260430.md`.

---

## §4 5-gate audit before/after

Unchanged: **169 / 0 / 0 / 0 / 21**. mig_180b + mig_181 + mig_177c introduced no governance debt to the gate definitions.

---

## §5 Governance pattern observed

Cursor agents continue to apply their authored SQL directly against MotherDuck despite explicit "DO NOT execute against MotherDuck" instructions in the prompt. This is the same pattern previously caught in mig_178 / mig_173b / mig_163b / mig_165 / mig_155. The work has been correct each time, but the deviation from agreed governance creates verification overhead for Cowork.

This round's three lanes are now retroactively verified and properly logged. **Pattern recommendation:** future Cursor prompts include an explicit AGENTS-protocol stamp in the deliverables section (e.g., a `governance_compliance.json` artifact attesting that no `query_rw` calls were made), so governance compliance is auditable directly from the commit rather than via post-hoc Cowork probes.

---

## §6 Outstanding work surfaced

- **mig_183** queued — the 1 remaining PM not_started col (`vessel_count`, 46 nonnull DOUBLE 1-6, alias-or-derivative of `vasc_vessel_count_v13` / `vascular_vessel_count` / `vi_vessels_max`). Closes PM not_started 1 → 0; final gate before `mig_162` PM finalization.
- **mig_177c_apply Option A queued** — 5,082 flippers × 15 derivative cols ≈ 28,099 cells cleared; closes `CF-mig177b-LVI-VI-DERIVATIVES-PENDING-RECLEAN`; opens `CF-mig177c-EXTENT-MISSING-FOR-NEW-FLIPPERS` (159 patients) for future Option B lane.
- **mig_182** still in flight at Cursor (read-only CF-87-AJCC investigation; awaiting agent summary).
- **mig_171b** awaits Logan ratification (US LN v2 build; 159 col-impact).
- **mig_174b** awaits Logan ratification (per-side BOOLEAN parser).
- **mig_160** structural date retype awaits explicit Logan green-light.

---

End of close-out.
