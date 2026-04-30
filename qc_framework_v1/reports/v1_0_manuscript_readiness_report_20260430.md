# Thyroid Canonical Publication v1.0 — Manuscript Readiness Report (mig_162)

**Date:** 2026-04-30
**Lane:** mig_162 / patient_master_finalization_and_lakehouse_audit
**Cowork applied:** 2026-04-30
**Status:** **MANUSCRIPT-READY** — backbone fully verified, gate1=172, PM 100% verified, all post-mig_160 dependent VIEWs recompiled.

---

## §1 Tier-2 canonical inventory

| Metric | Value |
|---|---:|
| Total canonicals (`canonical_*` tables) | 62 |
| **Verified canonicals (table_status='verified')** | **62 / 62 (100%)** |
| Verified column-rows | 3,094 |
| `na` column-rows | 220 |
| Total columns in verified canonicals | 3,314 |
| Cohort rows | 10,871 |
| Distinct research_id | 10,871 |
| **Cohort parity** | **10,871 / 10,871 ✓** |

**100% of Tier-2 canonicals are at table_status='verified'.** PM (canonical_patient_master) is fully verified at 1,596 v / 24 na / 0 not_started / 1,620 total.

---

## §2 5-gate cleanliness audit

| Gate | Pre-mig_162 | Post (=now) | Threshold | Status |
|---|---:|---:|---:|---|
| gate1 (verified canonicals) | 172 | **172** | ≥ 169 | ✓ |
| gate2 (verified without signoff_migration) | 0 | 0 | 0 | ✓ |
| gate3 (verified arithmetic check) | 0 | 0 | 0 | ✓ |
| gate4 (verified cols missing metadata) | 0 | 0 | 0 | ✓ |
| gate5 (TIMESTAMP/VARCHAR-date residual) | 25 | 25 | should ↓ to 0 long-term | partial (informational; CF-mig160-PM-DATE-COLS-REMAINING) |

Gate1-gate4 fully clean. Gate5 has 25 PM date cols left to retype — non-blocking for manuscript (analyst SQL handles DATE cast); future `mig_160b` will close.

---

## §3 Verification methodology distribution (top 15)

| Method | n_cols |
|---|---:|
| mechanical_derivation_compare | 244 |
| derivation_re_derivation_post_rollup_rebuild | 173 |
| derivation_re_derivation_against_verified_events | 141 |
| Path C: PM nlp cluster lineage + source-discovery + cohort-uniformity sweep | 115 |
| external_registry_nsqip_study_linkage_on_cpm | 101 |
| auto_no_source_counterpart | 96 |
| derivation_re_derivation_post_events_repair | 87 |
| derivation_replay_vs_canonical_operative_events_v1_tri_state_null | 59 |
| patient_level_nlp_aggregate_per_condition | 58 |
| derivation_canonical_labs_rollups_mig115_script347 | 56 |
| multi_source_derivation_plus_domain_sanity | 53 |
| source_lineage_thyroid_operative_sheet_feed_on_cpm | 48 |
| structured_source_compare_with_normalizer | 47 |
| parser_provenance_and_internal_nonregression | 41 |
| derivation_vs_canonical_molecular_genetics_v2 | 40 |

---

## §4 Top remaining CFs (informational, non-blocking)

| CF tag | n_cols | Disposition |
|---|---:|---|
| CF-mig171b-EXAM-MASTER-REBUILD | 77 | mig_187 in-flight |
| CF-mig136-DAYS-SEMANTIC | 58 | resolved 2026-04-29 mig_175b |
| CF-117-US-EXAM-ID-PORTABILITY | 53 | mig_171b addressed for LN; remaining = US-nodule rebuild |
| CF-mig177-ROLLUP-VASC-ALIAS-LVI | 44 | closed by mig_179b/177b; tags retained for trace |
| CF-GEN07-ROM-OCR | 41 | manuscript-acceptable (raw OCR; documented) |
| CF-90-DATE-FORMAT | 38 | partial close mig_160; remaining 25 PM cols → mig_160b |
| CF-87-AJCC | 36 | mig_184_v2 R1 derivation in flight |
| CF-117-US-GLAND-PARENCHYMA | 28 | future US-gland rebuild (separate lane) |
| CF-mig137-PM-MOL-DATE-RETYPE | 25 | mig_160b future close |
| CF-117-US-LN-SHELL | 23 | mig_171b superseded; tag retained for trace |

None of these block manuscript-grade survival/recurrence/outcomes analyses. Each is annotated with a follow-up path.

---

## §5 Major cleanups closed this round (2026-04-30)

| Lane | Commit | Outcome |
|---|---|---|
| mig_171b | `9301b58` | canonical_us_lymph_node_v2 BUILD: 6,973 events / 4,110 patients / 10,871 rollup; gate1 169→171 |
| mig_183 | `baaa2f4` | PM `vessel_count` last not_started col verified; PM at 1,591 / 24 / 0 |
| mig_174b | `e51d268` | cnln_img_laterality multi-label parsed into 5 per-side BOOLEANs |
| mig_177c_apply | `e51d268` | 28,099 derivative cells cleared on 5,082 LVI/VI flippers |
| mig_160 | (this lane) | 21 clinical-date cols retyped TIMESTAMP/VARCHAR→DATE; 1 VIEW patched |
| mig_162 | (this lane) | PM finalization confirmed; readiness report produced |

---

## §6 Pending Cursor lanes (parallel work)

| Lane | Prompt | What it closes |
|---|---|---|
| mig_184_v2 | `cursor_prompts/CURSOR_PROMPT_mig184_v2_r1_ajcc_RATIFIED_20260430.md` | CF-87-AJCC (36 cols) via R1 8-rule derivation |
| mig_185 | `cursor_prompts/CURSOR_PROMPT_mig185_path_malignant_duplicate_probe_*.md` | 533 duplicate path-malignant rows |
| mig_186 | `cursor_prompts/CURSOR_PROMPT_mig186_niftp_uncertain_exclusion_*.md` | 213 NIFTP + 7 uncertain (195 patients) WHO 2017 reclassification |
| mig_187 | `cursor_prompts/CURSOR_PROMPT_mig187_exam_master_rebuild_*.md` | CF-mig171b-EXAM-MASTER-REBUILD (77 cols / 159 fallback IDs) |

---

## §7 Manuscript readiness verdict

✅ **READY FOR MANUSCRIPT-GRADE SURVIVAL / RECURRENCE / OUTCOMES ANALYSES.**

- 62/62 verified Tier-2 canonicals (100%)
- PM 100% backbone verified (1,596 v / 24 na / 0 not_started)
- Cohort parity: 10,871 / 10,871
- 4/5 gates fully clean (gate5 residual is non-blocking)
- All known data quality issues either closed or have explicit CF traces with follow-up plans

---

End of report.
