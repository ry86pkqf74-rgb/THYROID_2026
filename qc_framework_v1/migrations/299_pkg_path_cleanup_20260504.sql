-- mig_299: M044/M038/M025/M032/M037 + scripts/ hardcoded path cleanup
-- =============================================================================
-- Generated: 2026-05-04
-- Lane: mig_299 — replace hardcoded `/Users/loganglosser/THYROID_2026` (and
--                 `/Users/loganglosser/Downloads`) prefixes with portable
--                 `Path(__file__).resolve().parents[N]` / env-var overrides
--                 across submission-package build scripts and helper scripts.
-- Severity: LOW (cosmetic / reproducibility).
-- DB-side effects: NONE (this lane edits Python source files only).
-- This SQL note exists for registry signoff + provenance.
-- =============================================================================

-- §1 — Audit (pre-edit): 14 .py / .md code files carried `/Users/loganglosser`
--   M038_submission_package_v1_0/08_analysis_code/build_m038_figures.py
--   M038_submission_package_v1_0/08_analysis_code/m038_docx_post.py
--   M038_submission_package_v1_0/08_analysis_code/split_m038_supporting.py
--   M038_submission_package_v1_0/08_analysis_code/build_m038_per_patient.py
--   M038_submission_package_v1_0/08_analysis_code/build_m038_manuscript_md.py
--   M038_submission_package_v1_0/08_analysis_code/build_m038_tables.py
--   M038_submission_package_v1_0/CLOSEOUT_NOTES.md
--   scripts/nsqip_resolve_unmatched.py
--   scripts/inspect_sources.py
--   scripts/nsqip_investigate_unmatched.py
--   scripts/nsqip_case_details_linkage.py
--   scripts/nsqip_phase2_enrichment.py
--   scripts/nsqip_enrichment.py
--   scripts/retarget_mismatches.py
--   scripts/flatten_tirads_us.py
-- M025/M032/M037/M044 submission packages: zero hits (already portable).

-- §2 — Apply pattern
--   Submission package scripts (M038/08_analysis_code/*.py):
--     PKG  = Path(__file__).resolve().parents[1]   # → M038_submission_package_v1_0/
--     REPO = Path(__file__).resolve().parents[2]   # → repo root (where needed)
--   scripts/*.py:
--     REPO = Path(__file__).resolve().parents[1]   # → repo root
--   External Downloads inputs (nsqip_resolve_unmatched.py, inspect_sources.py):
--     env override (NSQIP_DOWNLOADS_DIR, THYROID_ACTIVE_MASTER_DIR), default ~/Downloads
--   CLOSEOUT_NOTES.md cd command:
--     cd "$(git rev-parse --show-toplevel)"

-- §3 — Test
--   .venv/bin/python -m py_compile <all 14 files>  → OK
--   No DB changes; no rebuild required.

-- §4 — Registry signoff (run via _md_connect.connect_locked)
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary) VALUES
('mig_299', CURRENT_TIMESTAMP, 'cursor_composer_mig299',
 'mig_299: Replaced 14 hardcoded /Users/loganglosser/ paths with portable '
 'Path(__file__).resolve().parents[] (and env-var overrides for ~/Downloads '
 'inputs) across M038 submission package build scripts (6 files), M038 '
 'CLOSEOUT_NOTES.md, and 7 scripts/ helpers (nsqip_*, inspect_sources, '
 'retarget_mismatches, flatten_tirads_us). All 14 files py_compile clean. '
 'M025/M032/M037/M044 packages had zero hits. Reproducibility audit unblocked.');
