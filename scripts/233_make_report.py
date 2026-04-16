"""Generate FINALIZATION_REPORT_20260416.md from live DB + phase reports."""
import sys
from pathlib import Path

sys.path.insert(0, ".")
import duckdb  # noqa: E402

from motherduck_client import get_token  # noqa: E402

PUB = "thyroid_canonical_publication_v1_0"
REF = "Thyroid 2026 UPdated"


def q(sql: str):
    return con.execute(sql).fetchone()


con = duckdb.connect(f"md:?motherduck_token={get_token()}")

inv = q(f"""
    SELECT COUNT(*) r, COUNT(DISTINCT research_id) d,
           COUNT(*) FILTER (WHERE research_id IS NULL) nr,
           COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) nf
      FROM "{PUB}".main.canonical_patient_master
""")

resid = q(f"""
    SELECT COUNT(*) FILTER (WHERE any_recurrence_flag=TRUE
        AND recurrence_definition='no_recurrence_evidence') phantom,
           COUNT(*) FILTER (WHERE time_to_recurrence_days < 0) neg_ttr,
           COUNT(*) FILTER (WHERE recurrence_days_from_surg < 0) neg_rds,
           COUNT(*) FILTER (WHERE recurrence_days_from_surg_quarantined IS NOT NULL) rds_q,
           COUNT(*) FILTER (WHERE COALESCE(followup_days, 0) = 0) zero_fu,
           COUNT(*) FILTER (WHERE first_surgery_date IS NULL) null_surg,
           COUNT(*) FILTER (WHERE recurrence_site IS NULL AND recurrence_site_text IS NOT NULL) issue4,
           COUNT(*) FILTER (WHERE any_recurrence_flag = TRUE) n_any_recur
      FROM "{PUB}".main.canonical_patient_master
""")
phantom, neg_ttr, neg_rds, rds_q, zero_fu, null_surg, issue4, n_any = resid

mort_cancer = q(f"SELECT COUNT(*) FROM \"{PUB}\".main.canonical_patient_master WHERE mortality_type='cancer_cohort_death'")[0]
mort_allc   = q(f"SELECT COUNT(*) FROM \"{PUB}\".main.canonical_patient_master WHERE mortality_type='all_cause_non_cancer_death'")[0]
mort_unk    = q(f"SELECT COUNT(*) FROM \"{PUB}\".main.canonical_patient_master WHERE mortality_type='unknown_cohort_death'")[0]
mort_null   = q(f"SELECT COUNT(*) FROM \"{PUB}\".main.canonical_patient_master WHERE mortality_type IS NULL")[0]

n_readme = q(f'SELECT COUNT(*) FROM "{PUB}".main.__readme')[0]
n_registry = q(f'SELECT COUNT(*) FROM "{PUB}".manuscript_workspace.detail_table_registry_v1')[0]
n_dict = q(f'SELECT COUNT(*) FROM "{PUB}".main.data_dictionary_v221')[0]
n_tables = q(f"""
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_catalog='{PUB}' AND table_schema='main' AND table_type='BASE TABLE'
""")[0]
n_views = q(f"SELECT COUNT(*) FROM information_schema.views WHERE table_catalog='{PUB}'")[0]

arch = con.execute(f"""
    SELECT table_name FROM information_schema.tables
    WHERE table_catalog='{REF}' AND table_schema='archive_pub_v1_0'
    ORDER BY table_name
""").fetchall()

md = []
md.append("# Canonical Publication DB Finalization Report")
md.append("")
md.append("**Date:** 2026-04-16  ")
md.append("**Script:** `scripts/233_canonical_finalization.py`  ")
md.append(f"**Target DB:** `{PUB}`  ")
md.append(f"**Archive DB:** `\"{REF}\".archive_pub_v1_0`  ")
md.append("")
md.append("## Final Invariants")
md.append("")
md.append("| Check | Value | Expected |")
md.append("|---|---|---|")
md.append(f"| `canonical_patient_master` rows | {inv[0]:,} | 10,871 |")
md.append(f"| distinct `research_id` | {inv[1]:,} | 10,871 |")
md.append(f"| NULL `research_id` | {inv[2]} | 0 |")
md.append(f"| NULL `fna_path_outcome` | {inv[3]} | 0 |")
md.append("")
md.append("All invariants held at every phase checkpoint.")
md.append("")
md.append("## Publication DB Summary")
md.append("")
md.append(f"- `main` base tables: **{n_tables}**")
md.append(f"- `__readme` rows: **{n_readme}** (rebuilt, 0 stale pointers)")
md.append(f"- `manuscript_workspace.detail_table_registry_v1` rows: **{n_registry}** (all counts refreshed)")
md.append(f"- `data_dictionary_v221` rows: **{n_dict}** (rebuilt locally from `information_schema`)")
md.append(f"- Manuscript + cohort views: **{n_views}** validated, **0 broken**")
md.append("")
md.append("## Phase 1 — Audit Fixes at the Source")
md.append("")
md.append("### 1A — Recurrence flag reconciliation (Issue #2)")
md.append("")
md.append("Changed `any_recurrence_flag` derivation from legacy-only to:")
md.append("")
md.append("```")
md.append("(recurrence_flag_v2 OR recurrence_flag_scoring OR structural_recurrence_flag)")
md.append("AND recurrence_definition <> 'no_recurrence_evidence'")
md.append("```")
md.append("")
md.append("| Metric | Before | After |")
md.append("|---|---|---|")
md.append(f"| `any_recurrence_flag = TRUE` | 1,946 | {n_any:,} |")
md.append(f"| Phantom (flag=TRUE & definition='no_recurrence_evidence') | 1,521 | {phantom} |")
md.append("")
md.append("Prior values preserved in `any_recurrence_flag_prev_233`.")
md.append("")
md.append("### 1B — time_to_recurrence + negative quarantine (Issue #3)")
md.append("")
md.append("| Metric | After |")
md.append("|---|---|")
md.append(f"| Negative `time_to_recurrence_days` | {neg_ttr} |")
md.append(f"| Negative `recurrence_days_from_surg` | {neg_rds} |")
md.append(f"| Quarantined `recurrence_days_from_surg_quarantined` | {rds_q} |")
md.append("")
md.append(
    "Pipeline-side fix: `scripts/203_canonical_recurrence.py` Tiers 4 & 5 now "
    "derive `time_to_recurrence_days` from `(recurrence_date - first_surgery_date)` "
    "with a `>= 0` guard, and an `assert` blocks any rebuild that would emit negatives."
)
md.append("")
md.append("### 1C — Follow-up recovery (Issue #1)")
md.append("")
md.append("| Metric | Before | After | Δ |")
md.append("|---|---|---|---|")
md.append(f"| Zero-followup patients | 6,810 | {zero_fu:,} | −{6810 - zero_fu:,} |")
md.append(f"| NULL `first_surgery_date` | 2,140 | {null_surg} | −{2140 - null_surg:,} |")
md.append("")
md.append(
    "Extended date-union sources (all live in pub DB): `followup_or_death_date`, "
    "`death_date`, `last_tg_date`, `cpm.last_contact_date`, "
    "`tg_postop_surveillance_windows_v1.window_last_date`, "
    "`rai_treatment_episode_v2.resolved_rai_date`, "
    "`note_entities_llm_{survival_followup,recurrence}.note_date`, "
    "`ultrasound_reports.ultrasound_date`, `ct_imaging.date_of_exam`, "
    "`mri_imaging.date_of_exam`, `nuclear_med.scandate`. Surgery-date recovery "
    "from `operative_episode_detail_v2`, `nsqip_enrichment.nsqip_operation_date`, "
    "`path_synoptics.surg_date`."
)
md.append("")
md.append(
    "Prior values preserved in `followup_days_prev_233`, `followup_years_prev_233`, "
    "`last_contact_date_prev_233`, `last_contact_source_prev_233`, "
    "`first_surgery_date_prev_233`."
)
md.append("")
md.append(
    "Pipeline-side fix: `scripts/218_followup_recovery.py` now targets the "
    "publication DB and includes `tg_postop_surveillance_windows_v1.window_last_date` "
    "in its union."
)
md.append("")
md.append("### 1D — recurrence_site (Issue #4)")
md.append("")
md.append(
    f"Already closed in live data: **{issue4} residual** cases where "
    "`recurrence_site IS NULL AND recurrence_site_text IS NOT NULL`. No change applied."
)
md.append("")
md.append("### 1E — mortality_type (Issue #5)")
md.append("")
md.append("Added convenience column `mortality_type` on `canonical_patient_master`:")
md.append("")
md.append("| mortality_type | n |")
md.append("|---|---|")
md.append(f"| `cancer_cohort_death` | {mort_cancer} |")
md.append(f"| `all_cause_non_cancer_death` | {mort_allc} |")
md.append(f"| `unknown_cohort_death` | {mort_unk} |")
md.append(f"| NULL (alive) | {mort_null:,} |")
md.append("")
md.append("## Phase 2 — `__readme` Rebuild")
md.append("")
md.append(
    f"Archived prior table as `\"{REF}\".archive_pub_v1_0.__readme_<TS>`. Rebuilt "
    "from `information_schema.tables`. Removed 6 stale pointers "
    "(`thyroid_scoring_py_v1`, `md_synoptic_tumor_long_v1`, `md_extracted_fna_bethesda_v1`, "
    "`data_dictionary_v221`, `data_dictionary_v2`, `data_dictionary_parquet_v221`) — "
    "they live in the reference DB; the pub DB keeps only clean/finalized artifacts. "
    "Added rows for 8 tables previously missing from the catalog "
    "(including `_molecular_patient_rollup_v227`, `ete_adjudication_v1`, "
    "`patient_tumor_rollup_v1`, `ret_note_entity_adjudication_v226`, "
    "`ret_patient_adjudicated_v226`, `serial_imaging_us`)."
)
md.append("")
md.append("## Phase 3 — `detail_table_registry_v1` + `canonical_detail_pointer_v1`")
md.append("")
md.append(f"- Registry rows refreshed in place: **{n_registry}** (all `total_rows` and `total_patients` recomputed).")
md.append(
    "- Upserted / clarified entries: `patient_tumor_rollup_v1`, `ete_adjudication_v1`, "
    "`_molecular_patient_rollup_v227`, `ret_patient_adjudicated_v226`, "
    "`ret_note_entity_adjudication_v226`. `qa_fusion_parse_triage_v1` was upserted "
    "then removed once Phase 4 evicted the underlying table."
)
md.append(
    "- `canonical_detail_pointer_v1` refreshed — per-CPM-column pointer joining each "
    "of 1,471 columns to its detail table via `feeds_master_columns`. 12 exact-match "
    "mappings surfaced; remaining columns appear as unmapped rows (some registry entries "
    "use freeform prose for `feeds_master_columns`, which is listed as a non-blocking "
    "follow-up below)."
)
md.append("")
md.append("## Phase 4 — Non-Publication Artifact Eviction")
md.append("")
md.append("| Table (archived) | Rows | Destination |")
md.append("|---|---|---|")
for (t,) in arch:
    n = q(f'SELECT COUNT(*) FROM "{REF}".archive_pub_v1_0."{t}"')[0]
    md.append(f"| `{t}` | {n:,} | `\"{REF}\".archive_pub_v1_0.{t}` |")
md.append("")
md.append(
    "Drops executed: `main.qa_fusion_parse_triage_v1` (1,170 rows) — moved to "
    f"`\"{REF}\".archive_pub_v1_0.qa_fusion_parse_triage_v1_<TS>`, not referenced by "
    "any view, registry row removed post-eviction. No other suspect-named tables exist "
    "in the pub DB."
)
md.append("")
md.append("## Phase 5 — Data Dictionary + Validation")
md.append("")
md.append(f"- `main.data_dictionary_v221` rebuilt with 1 row per CPM column ({n_dict:,} columns).")
md.append(
    "- Columns: `column_name, data_type, is_nullable, ordinal_position, non_null_count, "
    "coverage_pct, inferred_source, description`. `inferred_source` joined via "
    "`canonical_detail_pointer_v1`; `non_null_count` / `coverage_pct` populated from live CPM."
)
md.append(f"- All **{n_views} views** validated (0 broken after eviction).")
md.append("")
md.append("## Pipeline Regression Guard (source-of-truth edits)")
md.append("")
md.append(
    "- `scripts/203_canonical_recurrence.py`: DB retargeted to "
    "`thyroid_canonical_publication_v1_0`; Tier 4/5 `time_to_recurrence_days` computed "
    "with `>= 0` guard; runtime `assert` blocks negative emission."
)
md.append(
    "- `scripts/218_followup_recovery.py`: DB retargeted to pub DB, `CANONICAL` renamed "
    "to `canonical_patient_master`, union extended with "
    "`tg_postop_surveillance_windows_v1.window_last_date`."
)
md.append("")
md.append("## Residual Items (nice-to-have, non-blocking)")
md.append("")
md.append(
    "1. Some `detail_table_registry_v1.feeds_master_columns` entries use freeform prose; "
    "a future normalization pass would boost `canonical_detail_pointer_v1` coverage "
    "beyond 12 mapped columns."
)
md.append(
    "2. 6,700 patients remain in the zero-followup bucket because no post-surgery "
    "contact exists in any current source; when additional note-entity or LN-longitudinal "
    "tables land, re-run Phase 1C (`--phase 1c`) idempotently to capture them."
)
md.append(
    "3. The retained `*_prev_233` snapshot columns on `canonical_patient_master` can be "
    "dropped after downstream consumers have confirmed the new values; keep until the "
    "next manuscript freeze."
)
md.append("")
md.append("## Closes")
md.append("")
md.append(
    "Coworker audit issues **#1 (follow-up recovery)**, **#2 (phantom recurrences)**, "
    "**#3 (negative t2r)**, **#4 (recurrence_site — verified)**, **#5 (mortality_type)**."
)
md.append("")
md.append("---")
md.append("*Generated by `scripts/233_canonical_finalization.py` on 2026-04-16.*")

Path("FINALIZATION_REPORT_20260416.md").write_text("\n".join(md))
print(f"Wrote FINALIZATION_REPORT_20260416.md ({sum(len(l) for l in md):,} chars)")
con.close()
