# Release-candidate blockers and residual risk

**Generated:** 2026-04-07 (audit run)  
**Updated:** 2026-04-07 (post–RC-audit closure)

## Resolved on live MotherDuck (this audit)

- **Manual review queue:** `qa.manual_review_queue` shows **0 pending** for run `mrq_hydrate_gate` (5,622 rows reviewed). Strict `119 --release-mode` **PASS** on review gate.
- **Master verified views:** `125_master_verified_views.py` rebuilt successfully; row counts 123,577 / 5,574 / 123,577.

## Closed follow-ups (this pass)

### 1. Grain (11,037 uniform row count)

Fail-closed on MotherDuck (`COUNT(*)` vs `COUNT(DISTINCT note_row_id)` for every registry v2 stem on **v2_stage** and **main**):

- **Result:** All stems: `COUNT(*) = COUNT(DISTINCT note_row_id) = 11,037`.
- **Conclusion:** **One row per note** across domains for the current cohort — **not** true row duplication. Uniform table width matches shared note grain, not a parity bug.

Evidence: `studies/20260407_release_candidate_audit/grain_note_row_id.md` (also emitted by `126_release_candidate_motherduck_audit.py` as `grain_note_row_id.md` in dated audit dirs).

### 2. Long-form contract on MD (119)

**Decision:** No change to `116` loader shape. Promoted `main.note_entities_llm_*` tables remain **wide / JSON note-level**; long-form analytic truth stays **`main.canonical_extracted_fact_long_v2`** and presentation views.

**119:** `check_schema_completeness` now treats **wide JSON note-level** stems as **PASS**: `entity_type`, `entity_value_raw`, and `entity_value_norm` absent on **both** `v2_stage` and `main` while `research_id` / `note_row_id` remain present — with an explicit *wide note-level v2 contract* message (not WARN). Missing id columns or promotion drift remains **WARN**.

### 3. DuckLake vs named snapshots

**Research (MotherDuck docs, 2026):**

- [CREATE SNAPSHOT](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-snapshot/) — snapshots target MotherDuck **native** databases.
- [ALTER DATABASE](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/alter-database/) — `SNAPSHOT_RETENTION_DAYS` applies to **native** storage; **DuckLake databases do not support these options.**
- [ALTER DATABASE SET SNAPSHOT](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/alter-database-snapshot/) — restore from snapshot ID/time/name is for **native** databases; **DuckLake databases do not support snapshot restore** in the same way.

**Implementation:** None for named `CREATE SNAPSHOT` on DuckLake (expected failure). **Mitigation documented** in `docs/motherduck_release_runbook_v2.md` §6.3: append-only `release_YYYYMMDD`, `qa.release_manifest`, parquet bundle `118`, and optional Business/support for contractual retention.

### 4. Retention (`historical_snapshot_retention` / 7-day)

If **7 days** (or platform default) is **insufficient for RC / legal hold**, treat as **governance**: open **MotherDuck Business / support** and record **ticket ID + outcome** in `docs/motherduck_release_runbook_v2.md` §6.3 (add a dated subsection when confirmed). Repo cannot extend DuckLake retention unilaterally via SQL.

### 5. Final analyst lab pull

**Still pending** operationally (~5–6 days per release plan). When cohort/lab deltas land:

1. `116_md_stage_loader.py --md` (or `--md --md-sa` when supported)
2. Promotion **gate** (`112` with MotherDuck check + run label)
3. `103_fact_lineage_materialize.py --md`
4. `114_qa_schema_setup.py --md --hydrate-from …`
5. `115_release_snapshot.py --md --tag <NEW_YYYYMMDD>` — **new tag only**; never overwrite existing `release_*`
6. `119_md_formalization_validate.py --md --md-sa --release-mode`
7. `126_release_candidate_motherduck_audit.py --md --md-sa` — refresh `studies/<YYYYMMDD>_release_candidate_audit/`

### 6. Formalization gate → `127` QA

If a gate repopulates `qa.manual_review_queue` under `run_label` **`formalization_*`**:

- Resolve **discordant** rows manually.
- Complete **critical-tier sample** per `docs/domain_mapping_rules.md`.
- Run **`127_qa_tier_batch_adjudicate.py --md --md-sa --apply`** (after policy sign-off; use `--include-critical-after-sample` only when approved).
- Insert / record audit rows in **`promotion_review_decisions`** as required by your promotion SOP.

### 7. Dependabot (GitHub Security)

| Alert | Severity | Package | Advisory | Action |
|-------|----------|---------|----------|--------|
| #2 | High | `langchain-core` | [GHSA-qh6h-p6c9-ff54](https://github.com/advisories/GHSA-qh6h-p6c9-ff54) | Bump to **>=1.2.22** in `docker/requirements.txt` |
| #1 | Low | `langchain-core` | [GHSA-2g6r-c272-w58r](https://github.com/advisories/GHSA-2g6r-c272-w58r) | Addressed by same floor (**>=1.2.11**; constraint **>=1.2.22**); partner `langchain-openai` not in docker bundle |

Stack alignment: `langchain-ollama>=1.0`, `langchain-community>=0.4.1` (pulls `langchain-core` 1.x). Re-scan after merge.

## Verdict

**RC READY** pending final labs — `119 --md --md-sa --release-mode` is **16 PASS / 0 WARN / 0 FAIL** (wide v2 schema accepted under documented contract). Remaining items are **operational** (analyst lab pull + optional support ticket for retention).
