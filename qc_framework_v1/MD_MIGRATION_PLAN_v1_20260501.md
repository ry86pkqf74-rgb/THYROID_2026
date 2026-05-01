# MotherDuck Migration Plan v1 — Thyroid Canonical Publication v1.0

**Generated:** 2026-05-01 by Cowork at HEAD `f13747a`
**Author:** Cowork (drafted from Logan's strategic inputs 2026-05-01)
**Status:** Plan-only. Execution gated on Logan's Emory IT confirmations (§8).
**Companion:** `COWORK_HANDOFF_PROMPT_2026-05-01_v20.md` (current operational state)

---

## §0 — Executive summary

**What:** Migrate `thyroid_canonical_publication_v1_0` (10,871-patient cohort, 454 objects, ~1.36M rows, est. 3-5 GB on disk) out of MotherDuck and into an Emory-sovereign target.

**Why:** Data sovereignty / Emory IT requirement (Logan's stated driver 2026-05-01). Secondary: cost discipline + decoupling from MD pricing.

**Strategic decisions Logan made on 2026-05-01:**
1. **Target arch:** Both Fabric Lakehouse + OneDrive parquet (initial pick), with **Emory AWS S3 explicitly admitted as a cheaper alternative if available**.
2. **Timeline driver:** Data sovereignty / Emory IT (not cost or specific deadline).
3. **PHI scope:** Two-tier — scrubbed primary (`semantic_publication.*`) broadly accessible; PHI (`clinical_notes_long` + `note_entities_*`) in restricted-access store with audit logging.

**Cowork's cost-aware recommendation:** Lead with **Emory AWS S3 + DuckDB-on-parquet** if Emory has Research Computing entitlement. Fall back to **OneDrive parquet** if S3 not available. Add **Fabric Lakehouse** only if Power BI dashboards become a primary requirement *and* Emory tenant is already F-SKU entitled. Reasoning: the dataset is small (≤5 GB), Logan's analytics workflow is Cursor + DuckDB + manuscript scripts (not Power BI dashboards), and S3+parquet preserves SQL semantics for ~$1-2/month vs Fabric's $262/mo F2 floor.

**Manuscript continuity:** Migration does NOT block Lane M drafting. Local DuckDB-on-parquet works as a drop-in for MD MCP from the moment the first parquet snapshot lands.

---

## §1 — Current MotherDuck state (live as of HEAD f13747a)

### Object inventory (from `information_schema.tables`)

| Schema | Tables | Views | Total | Est. rows |
|---|---:|---:|---:|---:|
| `manuscript_workspace` | 94 | 138 | **232** | 280,646 |
| `main` | 113 | 10 | **123** | 1,017,665 |
| `views_readable` | 0 | 62 | **62** | (views only) |
| `archive_pub_v1_0` | 18 | 0 | **18** | 33,412 |
| `semantic_publication` | 1 | 16 | **17** | 9 |
| `raw` | 2 | 0 | **2** | 17,652 |
| **Total** | **228** | **226** | **454** | **~1.36M** |

### Top-25 largest tables (estimated rows)

| Object | Rows |
|---|---:|
| `main.patient_cross_domain_timeline_v2` | 61,055 |
| `main.canonical_invasion_events_v1` | 58,582 |
| `main.canonical_labs_thyroglobulin_v1` | 53,006 |
| `manuscript_workspace.qc_manual_review_queue_v1` | 39,896 |
| `main.canonical_us_nodule_v2` | 37,579 |
| `main.note_entities_llm_frozen_section_detail` | 32,408 |
| `manuscript_workspace.us_raw_index0_conflict_v1` | 32,146 |
| `manuscript_workspace.script_396_prestate_gland_v1` | 28,724 ← drop candidate |
| `main.canonical_path_gland_events_v1` | 28,724 |
| `main.note_entities_procedures` | 21,942 |
| `main.canonical_operative_procedure_codes_v1` | 21,691 |
| `manuscript_workspace.us_nodule_conflict_queue_v1` | 20,126 |
| `manuscript_workspace.qc_violations_v1` | 18,422 |
| `main.tg_postop_surveillance_windows_v1` | 16,184 |
| `main.canonical_complications_events_v1` | 13,935 |
| `main.canonical_us_thyroid_gland_v2` | 13,578 |
| `main.canonical_us_thyroid_gland_events_v2` | 13,578 |
| `main.canonical_pathology_clinical_events_v1` | 13,358 |
| `main.imaging_exam_master_v1` | 13,347 |
| `manuscript_workspace.us_raw_index_mismatch_v1` | 13,166 |
| `main.canonical_pmh_events_v1` | 12,696 |
| `main.note_entities_operative_detail` | 12,151 |
| `manuscript_workspace.lesion_analysis_resolved_v1` | 11,851 |
| `main.canonical_operative_events_v1` | 11,773 |
| `main.canonical_path_benign_events_v1` | 11,688 |

### Confirmed drop candidates (~30 objects, ~80-120K rows)

**Frozen pre-migration backups (safe drop):**
- `archive_pub_v1_0.*` — 18 tables (cpm_pre*, _legacy_, queue_pre_*, detail_table_registry_v1_pre*, _stage_group_pre*) → entire schema is pre-mig snapshots from 2026-04-22/23/24.
- `manuscript_workspace.mig188_pre_snapshot_path_malignant`
- `manuscript_workspace.mig188_pre_snapshot_patient_master`
- `manuscript_workspace.mig188_pre_snapshot_registry`
- `manuscript_workspace.script_387_prestate_v1`
- `manuscript_workspace.script_389_prestate_v1`
- `manuscript_workspace.script_396_prestate_v1`
- `manuscript_workspace.script_396_prestate_benign_v1`
- `manuscript_workspace.script_396_prestate_gland_v1`

**Migration validation scaffolds (safe drop after re-verification):**
- `main.val_mig171b_canonical_us_ln_build_v1`
- `main.val_mig180b_nlp_upstream_lineage_v1`
- `main.val_mig194_canonical_us_thyroid_gland_shell_only_v1`

**Backfill scratch (verify before drop):**
- `manuscript_workspace.tsh_suppressed_backfill_v1`
- `manuscript_workspace.biochemical_concern_backfill_v1`
- `manuscript_workspace.cupm_v2_canonical_backfill_v1` (in `main`)

**Keep (audit trail):**
- `manuscript_workspace.canonical_deprecation_log_v1`
- `manuscript_workspace.archive_candidate_review_v1`
- `manuscript_workspace.archive_move_log_v1`

---

## §2 — Phase 0: MD optimization (this week, pre-migration)

Goal: shrink MD storage + clarify what gets exported, before paying to ship anything.

### 0.1 — Drop confirmed-safe objects (mig_250)
- Drop `archive_pub_v1_0` schema entirely (18 tables, 33,412 rows).
- Drop the 5 prestate snapshots (mig188 + script_387/389/396) and the 3 val_mig scaffolds.
- Estimated savings: ~120K rows, mostly wide tables (some have 1,600+ columns) → likely 0.5-1 GB.
- **Cowork executes** after Logan greenlights.

### 0.2 — Audit `views_readable` schema (62 views)
- Likely overlap with `semantic_publication.*` (mig_240-244 promoted several to publication tier).
- Goal: identify which `views_readable.*` views are no longer reachable from `manuscript_workspace.*` or `semantic_publication.*` and queue for drop in mig_251.
- Cowork lane (~30 min query work).

### 0.3 — Refresh `parquet_export/pub_v1_0_20260430/` mirror
- Existing parquet snapshot is from 2026-04-30; predates v17 round (mig_236-244) AND mig_245-249.
- Refresh against current HEAD: re-export `semantic_publication.*` + `manuscript_workspace.cohort_*_v1` + `main.canonical_*` to a new dated folder `parquet_export/pub_v1_0_20260501/`.
- This artifact becomes the migration payload (no further MD reads needed once it's done).
- Cursor Composer dispatch (mig_251 candidate).

### 0.4 — Final pre-migration object manifest
- Author `qc_framework_v1/PRE_MIGRATION_MANIFEST_20260501.md` — every object with: schema, name, type, est_rows, columns, dependencies, retain/export/drop.
- This becomes the cutover checklist.

---

## §3 — Target architecture comparison

### Option A — Fabric Lakehouse + OneDrive parquet (Logan's initial pick)

```
[ DuckDB / MotherDuck export ]
              │
              ▼
   parquet files staged locally
              │
       ┌──────┴───────┐
       ▼              ▼
[ Fabric Lakehouse ]  [ OneDrive parquet ]
(Delta tables on        (cold backup;
 OneLake; T-SQL          file-system access
 endpoint; Power BI      via Excel/Power
 native; notebooks)      Query/local DuckDB)
       │
       ▼
[ Power BI dashboards / Power Apps / etc. ]
```

**Pros:** Native Power BI, Power Apps, Power Automate. T-SQL endpoint for SQL-comfortable analysts. BAA-covered if Emory tenant is Fabric-entitled. Logan's M365 ecosystem already in place.

**Cons:** ~$262/mo F2 SKU minimum unless Emory tenant already pays it. Lock-in to Microsoft analytics stack. Delta tables are not directly readable by DuckDB without delta-rs (workable but not native).

**When best:** Power BI dashboards become a primary deliverable for Logan's group; multiple non-SQL users; Emory IT pushes Fabric as the standard.

### Option B — Emory AWS S3 + DuckDB-on-parquet (Logan's "reasonably cheap" admit)

```
[ DuckDB / MotherDuck export ]
              │
              ▼
   parquet files staged locally
              │
              ▼
[ Emory AWS S3 bucket (BAA) ]
   │ (encrypted at rest, audit logging,
   │  IAM-controlled access)
   │
   ├─► [ Logan's laptop: DuckDB httpfs ]
   │      SELECT * FROM 's3://emory-thyroid/.../canonical_patient_master.parquet'
   │
   ├─► [ Cursor Composer: same pattern ]
   │
   └─► [ Power BI Desktop: Parquet connector or DuckDB ODBC ]
```

**Pros:** Cheapest option (~$1-2/mo storage; $0 ongoing compute). Emory-sovereign + BAA-covered if Research Computing entitlement exists. DuckDB queries parquet directly (zero infra to manage). Logan owns the entire stack. Trivial backup (sync to OneDrive too if desired).

**Cons:** No managed SQL endpoint for non-DuckDB users. Power BI integration requires connector setup (workable, not instant). Single-writer (parquet snapshots replace, not row-level updates).

**When best:** Logan's analytics is Cursor + DuckDB + manuscript scripts; cohort is read-mostly (manuscript work, not OLTP); cost discipline matters; no Power BI requirement yet.

### Option C — Hybrid (Cowork's recommended split)

```
[ scrubbed publication tier ]
   semantic_publication.*  (15 views + release_manifest_v1)
   manuscript_workspace.*  (cohort views + dashboard + feasibility)
   main.canonical_*_VIEW_v1/v2  (the manuscript-facing tables)
              │
              ▼
   [ Emory AWS S3, primary bucket ] ── synced to OneDrive (offline backup)
              │
              ▼
   [ Logan + Cursor Composer + collaborators query via DuckDB ]


[ PHI tier — restricted ]
   main.clinical_notes_long  (11,050 rows)
   main.note_entities_*  (~25 tables; raw NLP outputs)
              │
              ▼
   [ Emory AWS S3, restricted bucket ]
   - Separate IAM principal
   - Audit logging on every GET
   - Only Logan (+ approved NLP collaborators) have access
   - No OneDrive mirror (PHI doesn't sit in personal cloud)
```

**Pros:** PHI minimum-necessary enforced at storage layer. Publication-tier collaboration unblocked (researchers can read parquet without PHI exposure). Cheapest target. Aligns with the v20 §12 architectural decision that "PHI exposure has a finite-lifetime risk; long-term fix is migration off MD."

**Cons:** Two buckets to manage. NLP re-runs require re-attaching PHI bucket (acceptable since NLP runs are infrequent post-publication).

**Cowork's strong recommendation:** Option C with AWS S3 as the storage substrate. Add Fabric Lakehouse later only if Power BI / Power Apps become a primary deliverable.

---

## §4 — Phased migration roadmap

| Phase | Scope | Owner | Duration | Gating |
|---|---|---|---|---|
| **P1** | Decisions + Emory IT entitlements | Logan + Emory IT | 1 week | §8 questions answered |
| **P2** | MD optimization (drops + parquet refresh) | Cowork | 2-3 days | P1 not blocked |
| **P3** | Export pipeline (DuckDB → parquet) | Cursor Composer | 1 week | P1 target chosen |
| **P4** | Target stand-up + upload | Logan + Cowork | 3-5 days | P3 done, S3/Fabric provisioned |
| **P5** | Validation (query parity tests) | Cowork | 2-3 days | P4 done |
| **P6** | Cutover + 30-day parallel period | Logan | 30 days | P5 passes |
| **P7** | MD detach + final accounting | Logan | 1 day | P6 ends clean |

**Total elapsed time: 4-7 weeks** (assuming Emory IT moves at typical academic pace).

**During all phases:** manuscript work (Lane M) continues. mig_249 re-score lands; M039 first draft; M038 RQ definition; M032 author-input gaps.

---

## §5 — Phase 3: Export pipeline (per-target details)

### Common to all targets — DuckDB COPY to parquet

```sql
-- Per-table export (example for canonical_patient_master)
COPY (SELECT * FROM main.canonical_patient_master)
TO '/path/to/parquet_export/pub_v1_0_20260501/main/canonical_patient_master.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- Per-view export (materialize the view at export time)
COPY (SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1)
TO '/path/to/parquet_export/pub_v1_0_20260501/semantic_publication/vw_publication_qc_status_VIEW_v1.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);
```

Plus sidecar files:
- `<table>.sql` — original CREATE statement (for reproducibility)
- `<table>.json` — column types + comments (for schema discoverability)
- `_manifest.json` — top-level inventory + dependencies

### S3 upload (Option B / C)
```bash
aws s3 sync /path/to/parquet_export/pub_v1_0_20260501/ \
  s3://emory-thyroid-publication-v1/pub_v1_0_20260501/ \
  --sse aws:kms --sse-kms-key-id <emory-kms-key>
```

### Fabric Lakehouse upload (Option A / C if Fabric chosen)
- Use Fabric Notebook with PySpark to read each parquet file → write to Lakehouse Delta table.
- Alternative: ADF Copy Activity with parquet source + Lakehouse Delta sink.
- ~200 tables × 2-5 sec each = 10-15 min runtime.

### Power BI semantic model (optional, Option A)
- Author DAX measures matching `vw_publication_qc_status_VIEW_v1` outputs.
- Dataset refreshes from Lakehouse on schedule.

---

## §6 — Phase 5: Validation protocol (parity tests)

Before declaring cutover, every test below must pass against the migrated target:

### Test 1 — Lakehouse health gates
```sql
-- On migrated target, recreate vw_publication_qc_status_VIEW_v1 logic
-- Expect: gate1=218, gates 2-5=0, cohort_parity TRUE, 10871×3
```

### Test 2 — Cohort parity
```sql
SELECT COUNT(DISTINCT research_id) FROM canonical_patient_master;  -- 10871
SELECT COUNT(DISTINCT research_id) FROM canonical_us_thyroid_gland_patient_rollup_v2;  -- 10871
SELECT COUNT(DISTINCT research_id) FROM canonical_us_lymph_node_patient_rollup_v2;  -- 10871
```

### Test 3 — All 63 cohort views queryable
- Same scan Cowork ran 2026-05-01 against MD; expect identical row counts on migrated target.

### Test 4 — M032 manuscript Tables 1-5 reproducibility
- Re-pull M032's published-draft numbers from the migrated source.
- Compare against the existing `manuscript_outputs/v1_0_20260501/M032_*_DRAFT_v1.md`.
- If any table differs, root-cause before cutover.

### Test 5 — manuscript_feasibility_v1 (post-mig_249) parity
- Color distribution + dashboard signals match between MD and migrated target.

---

## §7 — Phase 6-7: Cutover, parallel period, decommission

### Cutover day
1. Final parquet refresh (capture any HEAD changes since P3).
2. Upload to S3/Fabric.
3. Update Methods doc: "From 2026-XX-XX, the canonical data source is `s3://emory-thyroid-publication-v1/pub_v1_0_<date>/`. MotherDuck retained as read-only sandbox for 30 days for validation."
4. Logan switches Cursor Composer's MCP from MD to local DuckDB-on-parquet (or to S3 directly via httpfs).

### 30-day parallel period
- Both MD and migrated target accessible.
- Any new analysis: write against migrated source; verify against MD.
- Track issues in `qc_framework_v1/POST_MIGRATION_ISSUES_<date>.md`.

### Decommission
- After 30 days clean: `DETACH 'thyroid_canonical_publication_v1_0'` from MD account (or fully delete).
- Stop MD billing.
- Final cost accounting added to this plan as §10.

---

## §8 — Open questions for Logan to surface to Emory IT

These gate Phase 1 decisions:

1. **AWS Research Computing access (HIGHEST priority — cheapest path):**
   - Does Emory have a researcher-allocated AWS account with S3 access?
   - Is the BAA in place for storing PHI in that account?
   - What's the request process (project code, IRB protocol number, etc.)?
   - Does Emory IT mandate KMS-encrypted buckets / specific IAM patterns?

2. **Microsoft Fabric / Power BI Premium tenant entitlement:**
   - Is the Emory M365 tenant on Fabric (F-SKU)?
   - If yes, what's the SKU (F2/F4/F8/etc.) and how does Logan get a Lakehouse provisioned?
   - Is BAA in place for the Emory Fabric tenant?

3. **PHI handling rules:**
   - For `clinical_notes_long` (11,050 rows) + `note_entities_*` (~25 tables):
     - What's the required access control model? (group-level IAM? per-user?)
     - Is audit logging on every GET required, or only modifications?
     - Is there an Emory-specific PHI vault recommendation (e.g., a specific S3 bucket pattern, Azure Storage account, or on-prem option)?

4. **Researcher tooling permissions:**
   - DuckDB CLI on Emory-managed laptops: permitted?
   - Power BI Desktop: permitted?
   - AWS CLI: permitted?
   - Cursor / GitHub Desktop: permitted?

5. **HIPAA tier — does PHI need to stay on Emory-owned hardware?**
   - If "yes, no cloud," that rules out S3/Fabric entirely → would need to plan for on-prem (Emory IT-managed server, attached SQL Server, etc.). Significant scope change.
   - If "cloud OK with BAA," proceed as planned.

---

## §9 — Manuscript continuity during migration

| Lane M task | Status | MD dependency? | Plan |
|---|---|---|---|
| **M032 first draft** (v20) | Pushed at f9f848c | None — already done | Re-pull tables from parquet during validation (P5 Test 4) |
| **mig_249 feasibility re-refresh** (v20 §11) | Dispatched as f13747a | YES — runs against MD | Will land before migration; output applies to either source |
| **M039 first draft** (PTH/Calcium, cohort_n=4,561) | Carry-forward CF-M039-DRAFT | YES (data reads) | **Can start now** in parallel with migration; reads via MD or local parquet |
| **M038 RQ definition** | Carry-forward CF-M038-RQ | None (Logan defines RQ) | Independent; can happen any time |
| **M032 author-input gaps** | 8 flagged in DRAFT_v1 | None (Logan fills in) | Independent |
| **mig_250: per-manuscript Tables generator** | Open suggestion | YES (cohort view reads) | Defer until post-migration; runs cleanly against parquet |
| **CF-METHODS-V17-ADDENDUM** | Open carry-forward | None | Quick Cowork edit — can do during migration |

**Net:** Lane M does NOT block on migration, and migration does NOT block on Lane M. Two parallel tracks.

---

## §10 — Costs (Cowork's estimates, to be verified)

| Item | Today (MD) | Option A (Fabric+OneDrive) | Option B (S3+DuckDB) | Option C (Hybrid recommended) |
|---|---|---|---|---|
| Storage | MD usage tier (need invoice) | $0 (OD included with M365) | ~$1-2/mo (5 GB × $0.023/GB) | ~$1-2/mo |
| Compute | MD usage tier | $262/mo F2 minimum (or $0 if Emory entitled) | $0 (DuckDB on Logan's laptop) | $0 |
| Power BI | n/a | included with Fabric | extra ($10/user/mo Pro) | varies |
| **Total estimated** | **need MD invoice** | **$260-300/mo** | **$1-15/mo** | **$1-15/mo + Fabric optional** |

**Action: Logan to share recent MD invoice so Cowork can compute the actual today-vs-future delta.**

---

## §11 — Outputs from this plan (committed deliverables)

1. **This document** — `qc_framework_v1/MD_MIGRATION_PLAN_v1_20260501.md` (committed)
2. **mig_250: MD optimization drops** — Cowork-direct SQL (after Logan greenlights §2.1)
3. **mig_251: parquet export pipeline** — Cursor Composer dispatch prompt (authored when target chosen in §8)
4. **PRE_MIGRATION_MANIFEST_20260501.md** — full object inventory with retain/export/drop disposition (Cowork lane)
5. **Methods doc cutover addendum** — referenced post-cutover

---

## §12 — Decision log (this plan)

| When | Decision | Made by | Rationale |
|---|---|---|---|
| 2026-05-01 | Migrate off MD | Logan | Data sovereignty / Emory IT |
| 2026-05-01 | Two-tier PHI handling | Logan (Cowork option) | HIPAA minimum-necessary; matches v20 §12 architectural note |
| 2026-05-01 | AWS S3 admitted as cheap-target option | Logan | Cost-aware addition to original Fabric+OD plan |
| 2026-05-01 | Cowork recommends S3+DuckDB-local primary, hold Fabric until Power BI need is concrete | Cowork | Dataset is small + read-mostly; analytics stack is Cursor+DuckDB; Fabric SKU floor is high |
| **(Open)** | Final target choice — pending §8 answers | Logan + Emory IT | Need entitlement confirmations |

---

## §13 — Quick-reference glossary

- **MD** — MotherDuck, the current cloud DuckDB host
- **CPM** — `main.canonical_patient_master` (10,871 patients × 1,630 cols)
- **SSOT** — single source of truth (`semantic_publication.*` is the publication-tier SSOT)
- **PHI tier** — `main.clinical_notes_long` + `main.note_entities_*` (~25 tables; raw narrative + LLM extractions)
- **Scrubbed tier** — everything keyed by `research_id` only (semantic_publication, manuscript_workspace, main.canonical_*)
- **Lane M** — manuscript drafting work track
- **Cohort parity** — invariant: CPM rows = US gland rollup rows = US LN rollup rows = 10,871
- **Gate1** — `canonical_table_signoff_registry_v1` verified-table count (currently 218)

---

**End of plan v1.** Phase 1 starts with Logan answering §8 to Emory IT. Cowork stands ready to execute Phase 2 (MD optimization) on greenlight.
