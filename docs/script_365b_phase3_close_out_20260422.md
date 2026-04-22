# Script 365b Phase 3 — close-out

**Date**: 2026-04-22
**Commit pair**: this Phase 3 commit + prior Phase 0 (`09e6f9f`) + Phase 1 (`5e3d22c`)
**Scope**: snapshot + DROP of the 2 deprecated entity-row source tables in
`thyroid_canonical_publication_v1_0.main`. **No CPM repoint** (Option C).
**LLM tables (`note_entities_llm_past_*`)**: **STAY LIVE** — Script 367 owns them.

## Option-C decision (no Phase 2 CPM repoint)

The Phase-2 literal-source-reader audit (preserved at
`psh_pmh_meds_cpm_feeder_audit_20260422T064230Z.md`, gitignored) found
**78 CPM cols** literal-sourced from `note_entities_problem_list` /
`note_entities_medications` across 2 historical scripts:

- `scripts/212_nlp_entity_rollup.py` — 4 cols
  (`nlp_ne_problemlist_*` ×2, `nlp_ne_medications_*` ×2)
- `scripts/215_deep_nlp_entity_integration.py` — 74 cols
  (`pmhx_nlp_*` ×59, `med_nlp_*` ×15)

**Both scripts target the older `thyroid_ete_fix_20260413` DB namespace,
not the live publication DB**. They are HISTORICAL one-shots that ran
against an earlier cohort cut and whose outputs were promoted into
`thyroid_canonical_publication_v1_0.main.canonical_patient_master` at the
publication snapshot (likely Script 271). On the live publication DB the
78 cols are **frozen publication values**, not actively re-sourced.

Logan's call (verbatim):

> Repointing would OVERWRITE those frozen values with current canonical
> state — that's a semantic rewrite of CPM, not a cleanup, and it would
> break reproducibility for any analysis already run against those cols.
> "Frozen at publication" is the correct model for a published cohort.
> If live tier-driven CPM signals are ever wanted, the right architecture
> is a VIEW on CPM joining the tier triads — future decision, not this
> cycle.

The new `canonical_pmh_*_v1` and `canonical_medications_*_v1` canonicals
(Script 365 Phase 1, `canonical_version='v1_0_script365_remediated'`)
**supersede** these source tables for new analyses. CPM continues to
serve the prior frozen values for back-compat.

## Pre-Phase-3 safety audit (all clear)

Per Logan's instruction, four dependency checks were run before the drop:

| # | Check | Result |
|---|---|---|
| 1 | VIEWs in `thyroid_canonical_publication_v1_0` (any schema) with definition referencing either table | **0** matches via `duckdb_views()` |
| 2 | Rows in `manuscript_workspace.detail_table_registry_v1` referencing either table (in `detail_table_name` or `description`) | **0** matches |
| 3 | The 6 already-excluded scripts (`210_database_audit_backup`, `213_data_dictionary`, `223_ingest_and_publish`, `223_publish_canonical`, `233_canonical_finalization`, `250_registry_pointer_rebuild`) producing durable artifacts (`CREATE VIEW` / `CREATE TABLE AS` / `INSERT INTO ... SELECT FROM` / bare `FROM legacy_table`) referencing either table | **0** SQL-level matches; all 12 in-script occurrences are string literals inside table-list registries / data-dictionary descriptions / pointer-mapping configs |
| 4 | Refresh-job / scheduled-task substitute scan (`Makefile`, `.github/workflows/`, repo-wide `cron`/`launchd` artifacts) referencing either table | **0** matches. (No `mcp__scheduled-tasks` MCP server is attached to this workspace, so the literal MCP probe is N/A; substitute scan covers the equivalent surface.) |

## SQL operations performed

```sql
-- 1. Idempotent pre-DROP snapshots in archive_pub_v1_0 (NEW timestamp)
CREATE TABLE "Thyroid 2026 UPdated"."archive_pub_v1_0".
    "note_entities_problem_list_pre365b_20260422_122116" AS
    SELECT * FROM main.note_entities_problem_list;   -- 11,579 rows

CREATE TABLE "Thyroid 2026 UPdated"."archive_pub_v1_0".
    "note_entities_medications_pre365b_20260422_122116" AS
    SELECT * FROM main.note_entities_medications;    -- 7,501 rows

-- 2. DROP from main
DROP TABLE main.note_entities_problem_list;
DROP TABLE main.note_entities_medications;
```

Both archives carry a `COMMENT ON TABLE` recording the Option-C
rationale + safety-audit reference so any future operator inspecting
them can trace why the live tables were dropped without a CPM repoint.

## Existing pre365 archives (parity confirmed)

The 6 prior pre365_* snapshots from Script 365 Phase-1 builds remain in
`archive_pub_v1_0`. All match live row counts (11,579 / 7,501) at the
moment of drop:

| archive | rows | source |
|---|---:|---|
| `note_entities_problem_list_pre365_20260422_052723` | 11,579 | first 365 build |
| `note_entities_problem_list_pre365_20260422_064041` | 11,579 | Phase-1 first attempt |
| `note_entities_problem_list_pre365_20260422_064230` | 11,579 | Phase-1 final commit |
| `note_entities_problem_list_pre365b_20260422_122116` | 11,579 | **Phase-3 pre-DROP (this commit)** |
| `note_entities_medications_pre365_20260422_052723` | 7,501 | first 365 build |
| `note_entities_medications_pre365_20260422_064041` | 7,501 | Phase-1 first attempt |
| `note_entities_medications_pre365_20260422_064230` | 7,501 | Phase-1 final commit |
| `note_entities_medications_pre365b_20260422_122116` | 7,501 | **Phase-3 pre-DROP (this commit)** |

Restore path if ever needed:

```sql
CREATE TABLE main.note_entities_problem_list AS
SELECT * FROM "Thyroid 2026 UPdated"."archive_pub_v1_0".
    "note_entities_problem_list_pre365b_20260422_122116";
```

## LLM tables: STAY LIVE

Per Logan + Script 367 ownership, the 2 LLM-extraction sources are NOT
dropped in this commit and remain queryable at full row count:

| live LLM table | rows |
|---|---:|
| `main.note_entities_llm_past_medical_hx` | 11,037 |
| `main.note_entities_llm_past_surgical_hx` | 11,037 |

## Post-drop state

| | live | dropped |
|---|---|---|
| Legacy entity-row sources | (none in main) | `note_entities_{problem_list,medications}` |
| LLM sources (Script 367 owns) | `note_entities_llm_past_{medical,surgical}_hx` | — |
| New 365 canonicals | 6 tables × `v1_0_script365_remediated` | — |
| Frozen CPM cols | 78 historical (212 + 215 lineage) | — |
| Archive snapshots | 8 in `archive_pub_v1_0` (3 pre365 + 1 pre365b per source) | — |

## Open follow-ups (Phase 4 + future cycles)

- **Phase 4** (memory + close-out): NOT this commit; awaits Logan sign-off.
  Logan flagged a memory entry should be added noting that
  `thyroid_ete_fix_20260413` is the historical source-of-truth for the 78
  frozen CPM cols, so future cleanup of that namespace doesn't break
  the publication CPM lineage.
- **Future cycle (deferred)**: if live tier-driven CPM signals ever
  wanted, build a VIEW on CPM joining the new rollup phenotype-BOOL
  triads. NOT in scope for this remediation.
- **Tier-1 CF (open)**: `docs/tier1_cf_procedure_normalized_corruption_20260422.md`
  — upstream `procedure_normalized` corruption that drove the hybrid
  anchor decision.

## Commit traceability

This commit is the third in the 365b remediation cascade:

1. `09e6f9f` — Phase 0: rename LN v2 script + tier2 drop
2. `5e3d22c` — Phase 1: rebuild 6 canonicals (CHANGES A-N + Logan overrides)
3. **this commit** — Phase 3: snapshot + DROP legacy entity-row sources
4. *Phase 4* — memory + close-out (NOT yet committed; awaits sign-off)

**No `git push` until Logan green-lights.** Currently 3 commits ahead of
`origin/main` after this commit lands.
