# MotherDuck Cleanup Report — 2026-05-01

**Generated:** 2026-05-01 by Cowork at HEAD `f6b00a1` (will become parent of mig_250 commit)
**Scope:** Pre-migration MD optimization (Phase 0 of `MD_MIGRATION_PLAN_v1`)
**Outcome:** Database is fully cleaned within the safe-drop envelope. Gate health unchanged.

---

## §1 — What was dropped (mig_250)

**32 objects removed** (~120K+ rows + several wide 1591-col snapshots).

### archive_pub_v1_0 schema — entire schema removed (18 tables + the schema container)

Pre-migration backups frozen 2026-04-22/23/24. Reproducible by re-running source migrations.

| Table | Est. rows × cols |
|---|---|
| canonical_complications_patient_rollup_v1_legacy_20260422 | 10,871 × 50 |
| canonical_us_exam_master_VIEW_v2_legacy_20260422_body | 1 × 2 |
| canonical_us_patient_master_VIEW_v2_legacy_20260422_body | 1 × 2 |
| cpm_ete_pre390_20260422 | 10,871 × 13 |
| cpm_ete_pre392_20260422_234621 | 183 × 11 |
| cpm_pre391_20260422_223618 | 10,871 × 10 |
| cpm_pre_dtc_null_n_stage_group_fill_20260423_024412 | 4 × 1591 |
| cpm_pre_malignant_null_stage_group_closeout_20260423_034419 | 8 × 1591 |
| cpm_pre_manual_review_queue_sortout_20260423_041534 | 1 × 1591 |
| cpm_pre_pdtc_rid6275_stage_group_20260423_045808 | 1 × 1591 |
| cpm_pre_tn_primary_from_v2_fill_20260423_030702 | 236 × 1591 |
| cpm_stage_group_pre393_20260422_235819 | 9 × 9 |
| cpm_stage_group_pre394_20260423_000452 | 33 × 11 |
| cpm_t_sync_pre395_20260423_001407 | 13 × 14 |
| detail_table_registry_v1_pre389_1_20260422T212806Z | 144 × 13 |
| detail_table_registry_v1_pre_mig60_20260424 | 156 × 15 |
| queue_pre_manual_review_queue_sortout_20260423_041534 | 8 × 11 |
| queue_pre_pdtc_rid6275_stage_group_20260423_045808 | 1 × 11 |

Plus: `DROP SCHEMA archive_pub_v1_0 CASCADE` removed the empty schema container.

### main schema — 3 val_mig migration validation scaffolds

| Table | Est. rows × cols |
|---|---|
| val_mig171b_canonical_us_ln_build_v1 | 10 × 7 |
| val_mig180b_nlp_upstream_lineage_v1 | 12 × 16 |
| val_mig194_canonical_us_thyroid_gland_shell_only_v1 | 10 × 7 |

### manuscript_workspace schema — 11 prestate snapshots + scratch tables

| Table | Est. rows × cols |
|---|---|
| mig188_pre_snapshot_path_malignant | 6,689 × 16 |
| mig188_pre_snapshot_patient_master | 10,871 × 18 |
| mig188_pre_snapshot_registry | 36 × 15 |
| script_387_prestate_v1 | 28 × 8 |
| script_389_prestate_v1 | 0 × 6 |
| script_396_prestate_v1 | 10,139 × 17 |
| script_396_prestate_benign_v1 | 11,688 × 55 |
| script_396_prestate_gland_v1 | 28,724 × 20 |
| tsh_suppressed_backfill_v1 | 56 × 5 |
| canonical_logan_review_log_v1 *(empty)* | 0 × 11 |
| cr_crr_reconcile_candidates_20260429 *(empty)* | 0 × 17 |

---

## §2 — What was held back (load-bearing)

5 candidates that surfaced during the second-pass scan but were NOT dropped because they're either referenced by an active view or registered in `canonical_table_signoff_registry_v1` (gate1 governance).

### Active view dependency

| Object | Why held |
|---|---|
| `main.cupm_v2_canonical_backfill_v1` | Referenced by `main.canonical_us_patient_master_VIEW_v2`; in signoff registry as verified. |
| `manuscript_workspace.biochemical_concern_backfill_v1` | In signoff registry as verified governance artifact. |

### Empty but governance-registered (would change gate1 if dropped)

| Object | Status |
|---|---|
| `manuscript_workspace.path_tumor_size_chart_review_queue_v1` | Empty (rows=0) but in signoff registry |
| `manuscript_workspace.qc_tir03_llm_candidates_v1` | Empty; documented in `main.canonical_us_nodule_v2` comment as the queue for TIR03 multi-nodule LLM re-parse |
| `manuscript_workspace.schema_reorg_orphan_references_v1` | Empty; in signoff registry |
| `manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1` | Empty; in signoff registry |
| `manuscript_workspace.v1_1_finalization_audit_v1` | Empty; in signoff registry |

**Recommendation:** these 5 empty governance-registered tables can be cleaned up in a future **mig_251** that coordinates the table drop with a corresponding `DELETE FROM canonical_table_signoff_registry_v1 WHERE table_name IN (...)`. That migration would intentionally reduce gate1 from 218 → 213 with a documented rationale. Not done in mig_250 because it's a governance change beyond pure storage optimization.

---

## §3 — State change

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Total objects | 454 | **422** | −32 |
| Tables | 228 | **196** | −32 |
| Views | 226 | **226** | 0 |
| Schemas | 6 | **5** | −1 (archive_pub_v1_0 removed) |
| `main` tables | 113 | **110** | −3 |
| `manuscript_workspace` tables | 94 | **85** | −9 (or −11 if you count the 2 empty supplemental drops) |
| `archive_pub_v1_0` tables | 18 | **0** | −18 |

### Gate health (UNCHANGED — verified post-drop)

| Gate | Before | After |
|---|---:|---:|
| gate1 (verified tables in signoff registry) | 218 | **218** ✓ |
| gate2 (missing signoff) | 0 | **0** ✓ |
| gate3 (count mismatch) | 0 | **0** ✓ |
| gate4 (verified cols missing metadata) | 0 | **0** ✓ |
| gate5 (clinical date violations) | 0 | **0** ✓ |
| cohort_parity_ok | TRUE | **TRUE** ✓ |
| CPM patients | 10,871 | **10,871** ✓ |
| US gland v2 patients | 10,871 | **10,871** ✓ |
| US LN v2 patients | 10,871 | **10,871** ✓ |

### Broken-reference scan: ZERO views were left dangling by the drops.

---

## §4 — Observations from the second-pass scan (not action items, just context)

### The `views_readable` schema (62 views) has zero internal references

All 62 views in `views_readable.*` are referenced by NO other view in `semantic_publication`, `manuscript_workspace`, or `main`. By the schema's name, this is **by design** — `views_readable` is the user-facing presentation tier intended for direct query (Tableau, BI tools, ad-hoc analyst SQL). It's not orphan cruft.

**Implication for migration:** when the parquet export is built (Phase 3 of `MD_MIGRATION_PLAN_v1`), the `views_readable.*` views should be materialized into the export bundle if there's any human-facing query path that depends on them. Audit before excluding.

### No additional obvious drop candidates beyond what was held back

A scan of all `main` and `manuscript_workspace` tables for naming patterns (legacy_, backup_, _DRYRUN, _PRE, val_, archive_, _TEMP) returned zero new candidates beyond the 32 dropped + 5 held-back. The database has been progressively cleaned through prior migrations; the remaining objects are either active or governance-registered.

---

## §5 — What this enables for the migration

After mig_250:
- The export payload (Phase 3 of `MD_MIGRATION_PLAN_v1`) is **smaller and cleaner** — no archive backups carried into S3/Fabric/OneDrive.
- Per-object disk footprint reduction is hardest to estimate without actual MD storage stats, but the 1591-column wide snapshots in `archive_pub_v1_0` were almost certainly the largest disk consumers per row.
- Future MD invoices should reflect a modest reduction once the MD storage compaction cycle picks up the drop.

---

## §6 — Open follow-ups (deferred from this chat)

| ID | Description | When |
|---|---|---|
| mig_251 (governance) | Coordinated drop + signoff-registry update for the 5 empty governance-registered tables | Optional; only if you want gate1 to drop from 218 → 213 cleanly |
| mig_252 (parquet refresh) | Refresh `parquet_export/pub_v1_0_20260501/` against current state (post mig_245-250) | When migration target chosen |
| Lane M — ETE manuscript (M044 vs M051) | Picked up on next chat per Logan 2026-05-01 | Next chat |
| mig_249 (feasibility re-refresh) | Cursor Composer dispatched at f13747a; running against MD | Lands independently |
| Emory IT entitlements (§8 of MD_MIGRATION_PLAN_v1) | Logan to confirm AWS Research Computing + Fabric entitlement + PHI rules | Driver for migration target choice |

---

## §7 — Confirmation statement

**The MotherDuck database `thyroid_canonical_publication_v1_0` is fully cleaned within the safe-drop envelope.** All 5 governance gates pass at their pre-cleanup values. Cohort parity holds at 10,871 across CPM, US gland v2, and US LN v2. Zero views were left dangling. Five empty governance-registered tables remain, intentionally held back to preserve gate1 = 218.

Beyond mig_250, no further safe drops were identified. The database is ready for Phase 1 (target architecture decisions pending Logan's Emory IT answers per `MD_MIGRATION_PLAN_v1` §8).

---

**End of cleanup report.**
