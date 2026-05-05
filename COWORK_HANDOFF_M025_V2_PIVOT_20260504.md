# Cowork handoff — M025 v2.0 nodule-level pivot
**Generated:** 2026-05-04 22:55 EDT by Cowork (Claude in Cowork mode)
**Repo HEAD at handoff:** see `git log -1` (push pending after this file lands)
**Active manuscript:** M025 — TI-RADS Performance, pivoted patient-level → nodule-level

---

## ONE-PARAGRAPH STATE-OF-PLAY

Logan was auditing his draft prompt for M025 (TI-RADS Performance, n=6,523 in his prompt vs 3,375 in the validated cohort). Investigation showed his 6,523 was the per-nodule universe denominator (any patient with a US nodule in `canonical_us_nodule_v2`); 3,375 was the validated patient-level operative cohort. The patient-level paper was already drafted at submission stage (M025_submission_package_v1_0/, mig_292 signed). When we built a per-nodule analytic spine via mig_306, the headline finding flipped the manuscript: **per-nodule TR4 ROM 18.7% [16.3–21.5] and TR5 ROM 26.1% [23.7–28.6] land inside ACR-expected ranges, while patient-level analysis of the same data shows 47% and 59%** — so 50–70% of apparent operative-cohort ROM inflation is multinodular attribution error, not selection bias. Discrimination identical (AUC 0.640 nodule vs 0.648 patient); the headline is calibration recovery. M025 has been pivoted to v2.0 with the new framing. Cursor is running mig_260/264/307 to land the build; mig_260 already landed (commit 2f6e0f0). Cowork has pre-baked all the publication numbers (Wilson CIs), figures spec, methods prose, and a Cortex Analyst semantic model into the v2.0 package so the cursor agent only has to mechanically build tables/figures/docx.

---

## DATA WAREHOUSE ACCESS — STATE-OF-PLAY

### MotherDuck (PRIMARY working surface)
- **Connector:** MCP `mcp__eaae7896-f429-40a8-bbb0-9d2f33c76a47__*` — connected, read+write working.
- **Databases:** `thyroid_canonical_publication_v1_0` (writable, primary), `readonly_share` (mirror).
- **Token:** `MOTHERDUCK_TOKEN` in `/Users/loganglosser/THYROID_2026/.env.motherduck` and `.env`.
- **Active publication tag:** `pub_v1_1` (mig_300 signed 2026-05-04).
- **Schemas:** `main` (127 tables / 13 views), `manuscript_workspace` (89 tables / 141 views), `archive_pub_v1_0` (5 retained items post-purge), `views_readable`, `semantic_publication`. Total 447 objects.

### Snowflake (Cortex Code only)
- **CLI installed:** `snow` v3.16.0 + `cortex` v1.0.73+180523 (~/.local/bin added to PATH in .zshrc/.bash_profile).
- **Default connection:** `~/.snowflake/config.toml` `[connections.thyroid_2026]` — account `qcc02515.us-east-1`, user `LGLOSSE13`, role `ACCOUNTADMIN`, WH `COMPUTE_WH`, DB `THYROID_VALIDATION`, schema `PUBLIC`.
- **PAT scope:** Logan-confirmed CORTEX-ONLY. Generic `snow sql` returns `250001 (08001) Programmatic access token is invalid`. Cortex Code commands work fine because they hit Cortex service endpoints not the warehouse session login.
- **Working cortex commands tested:** `cortex search docs <query>`, `cortex search object <query>`, `cortex search table-details`, `cortex connections list`. Pending: `cortex analyst query` requires the staged semantic model to be bound in Snowsight UI first (we wrote the YAML — see file pointer below).
- **NOT working with this token:** `snow sql -q "..."`, `snow connection test`. To enable warehouse SQL, Logan would need a non-Cortex-scoped PAT (role with USAGE on warehouse + DB).
- **Snowflake-side mirror tables (per `snowflake_trial/SF_INFRASTRUCTURE_REGISTRY.md`):** `CANONICAL_PATIENT_MASTER_FLAT`, `COHORT_M025_TIRADS_PERFORMANCE_V1_FLAT` (3,375 rows, patient-level), and 4 other cohort flats. Validation SP `CALL VALIDATE_ALL_COHORTS()` — 17/17 PASS as of 2026-05-04. Cortex Search service: `THYROID_NOTES_SEARCH` over full 11,050-note corpus.

### Cursor agent
- Running independently. Has its own MotherDuck token and Snowflake access (per Logan).
- Most recent landed commit: `2f6e0f0 mig_260: TIRADS SSOT repoint for M025 cohort and Table 1` — landed mid-session, didn't affect the per-nodule view.
- Active queue: mig_264 (Bethesda-2 false-neg audit), mig_307 (M025 v2.0 build).

---

## CURRENT STATE OF DATA CLEANUP / VERIFICATION

### Cohort verification — DONE
- M025 v1.0 patient-level: 3,375 patients verified live MotherDuck = 100% match to submission package (zero drift; report at `M025_submission_package_v1_0/08_analysis_outputs/M025_drift_report_20260504_2205.md`). Matches Snowflake validation baseline (17/17 PASS, 1,479 malignant).
- M025 v2.0 nodule-level: 37,438 rows / 6,523 patients / 3,687 strict-ACR analytic-eligible / 2,216 with FNA Bethesda / 3,973 path-malignant nodules across 1,230 patients. Built mig_306, signed.

### Schema integration — DONE for first analytic pass
- US-nodule → FNA bridged at v2 keys via (research_id + laterality + ±30d). Recovers ~70% of FNA links. **Carry-forward CF-FNA-SIZE-CM-NULL** open: per-nodule FNA size NULL by design in v1.0 linkage; v1_1 NLP extraction will improve recall.
- US-nodule → path-tumor bridged via (rid + laterality, surgery ≤365d post-US). Bilateral path tumors handled conservatively (matched to either side).

### Archive purge — DONE
- 791 legacy tables dropped per commit 7b4c0ac. Verified 2026-05-04: only 5 expected retained items in `archive_pub_v1_0`; zero stale objects (no `_broken`, `_pre_mig`, `_snapshot_2026`, leading-underscore) remaining in non-archive schemas. Missing signoff backfilled as `archive_purge_20260504`.

### Migration registry hygiene
- Latest signoffs (top 5): `mig_306` (us, 2026-05-05), `mig_260` (cursor, 2026-05-04), `mig_303`, `mig_302`, `mig_300` (pub_v1_1 release tag).
- Still UNSIGNED (per cursor prompt vs signoff_migration cross-ref): mig_264, mig_266, mig_270, mig_274, mig_276, mig_278, mig_279, mig_301/301b, mig_303 (just signed by cursor), mig_304, mig_305 (mostly OTHER manuscripts or registry hygiene). For M025 the only one that materially affects v2.0 numbers is **mig_264** (360 residual Bethesda-2 + malignant patients pending audit).

### Open data carry-forwards
- **CF-FNA-SIZE-CM-NULL** — per-nodule FNA size NULL in linkage v1.0 (size_score is flat 0.5 prior). v1_1 NLP extraction needed.
- **CF-mig_264-BETHESDA2-LINKAGE-MISMAP** — 360 Bethesda-2 + malignant patients; if reclassified, re-run mig_306 view + downstream tables.
- **CF-CORTEX-ANALYST-NEEDS-BIND** — semantic model staged but not yet bound in Snowsight UI.
- **CF-mig_305** — VALIDATE_ALL_COHORTS SP v3 hangs on information_schema check inside SP body; SP currently at v2 (17 checks, all PASS).

---

## CURSOR ACTION TO REVIEW WHEN COMPLETE

Cursor is running these prompts in this order (per `cursor_prompts/CURSOR_RUN_ORDER_M025_PIVOT_20260504.md`):

1. **mig_260 ✓ LANDED** (commit 2f6e0f0). Verified by Cowork: per-nodule view unaffected. No further action.

2. **mig_264 — IN FLIGHT.** When complete, review:
   - Was Bethesda_final reclassified for any of the 360 patients?
   - If yes: re-run mig_306 view + Cowork's pre-baked CSVs (`m025v2_per_tr_rom_with_ci.csv`, etc.). Per-TR ROM and threshold metrics may shift slightly.
   - If no (all 360 confirmed true false-negatives): no changes needed; cite findings in M025 v2.0 Discussion.

3. **mig_307 — IN FLIGHT.** When complete, review:
   - `M025_submission_package_v2_0/02_manuscript.docx` — does the headline finding (Table 3, Fig 3) actually appear prominently?
   - `M025_submission_package_v2_0/04_tables.xlsx` — does Table 2 use the pre-baked CSVs verbatim? Verify by spot-checking 3 cells.
   - `M025_submission_package_v2_0/06_figures/` — 4 figures (cohort flow, ROC, patient-vs-nodule bars with ACR bands, attribution-error waterfall) per `08_analysis_code/SPEC_FIGURES_AND_TABLES.md`.
   - Cursor SHOULD insert a signoff_migration row for mig_307 — verify with `SELECT * FROM main.signoff_migration WHERE mig_id='mig_307';`
   - If cursor builds against patient-level data instead of nodule-level (`cohort_m025_tirads_performance_v1` instead of `cohort_m025_nodule_level_v1`) — flag and have it rebuild.

4. **Optional follow-ups for the cursor queue:** mig_270 (histology repoint), mig_278+279 (registry signoff backfills), mig_303 (already signed by cursor). NOT critical for v2.0 submission.

---

## EVERYTHING COWORK PRE-BAKED (so cursor mig_307 is mechanical)

### `M025_submission_package_v2_0/`
- `00_README.md` — pivot rationale, working title, Q1 v2.0, primary endpoints, cohort definition, sister-paper map, open carry-forwards.
- `08_analysis_outputs/` — every publication number locked with Wilson 95% CIs:
  - `m025v2_per_tr_rom_with_ci.csv` — Table 3 source (patient vs nodule per-TR ROM with CIs)
  - `m025v2_threshold_metrics_per_nodule.csv` — Table 2 source (Sens/Spec/PPV/NPV at TR≥TR3/4/5)
  - `m025v2_auc_summary.csv` — 0.6399 nodule vs 0.6478 patient
  - `m025v2_run_snapshot.json` — counts manifest
  - `m025v2_supp_S1A_relaxed_cohort.csv` — Sensitivity arm A
  - `m025v2_supp_S1B_alternate_cohorts.csv` — Sensitivity arms B/C/D
  - `m025v2_figS1_bethesda_x_tirads_heatmap.csv` — Supp Fig S1 source
- `08_analysis_code/`:
  - `SPEC_FIGURES_AND_TABLES.md` — full spec for 4 tables + 4 figs + supplementary, including CSV sources, manuscript prose hooks, sensitivity arm definitions
  - `METHODS_DRAFT.md` — Methods section prose: cohort assembly, TI-RADS predictor, reference standard, FNA Bethesda linkage bridge, statistical analysis (Wilson CI + AUC rank-Mann-Whitney), patient-vs-nodule comparison, sensitivity arms, software/repro, IRB

### `cursor_prompts/`
- `CURSOR_PROMPT_MIG_306_NODULE_LEVEL_SPINE_20260504.md` — provenance for the per-nodule view
- `CURSOR_RUN_ORDER_M025_PIVOT_20260504.md` — sequence + first-messages for mig_260/264/307

### `qc_framework_v1/migrations/`
- `306_nodule_level_spine_20260504.sql` — signoff stub + DDL pointer

### `snowflake_trial/semantic_models/`
- `m025_nodule_level_semantic_model.yaml` — Cortex Analyst semantic model. Logan binds this in Snowsight → AI Studio → Cortex Analyst → Add semantic model. After bind: `cortex analyst query "what's the per-nodule ROM by TR in the strict cohort?"` works.

### `memory/`
- `skill_snowflake_cortex_2026_05_04.md` — Cortex CLI capabilities, deployed SF infrastructure, reusable patterns (cross-source validation, NL→SQL, Cortex Search, AI_CLASSIFY), auth troubleshooting, known carry-forwards. **Save this as a long-term skill.**

---

## HEADLINE NUMBERS (PUBLICATION-READY)

### Per-TR ROM with Wilson 95% CI (THE table)

| TR | Patient ROM (95% CI) | Nodule ROM (95% CI) | Inflation | ACR-expected | In band? |
|---|---|---|---:|---|---|
| TR1 | 28.2 [23.7–33.2] | — | — | <2% | — |
| TR2 | 32.1 [27.1–37.6] | 12.9 [5.1–28.9] | +19.2 pp | <2% | no |
| TR3 | 27.6 [24.7–30.7] | 9.1 [7.8–10.7] | +18.4 pp | <5% | no |
| **TR4** | 47.4 [43.0–51.8] | **18.7 [16.3–21.5]** | +28.6 pp | **5–20%** | **YES** |
| **TR5** | 58.7 [56.1–61.2] | **26.1 [23.7–28.6]** | +32.6 pp | **>20%** | **YES** |

### Per-nodule threshold metrics

| Threshold | Sens (95% CI) | Spec (95% CI) | PPV (95% CI) | NPV (95% CI) |
|---|---|---|---|---|
| TR≥TR3 | 99.4 [98.4–99.8] | 0.9 [0.6–1.3] | 17.1 [16.0–18.4] | 87.1 [71.1–94.9] |
| **TR≥TR4** | **76.9 [73.4–80.0]** | **47.1 [45.4–48.9]** | **23.1 [21.3–24.9]** | **90.8 [89.3–92.1]** |
| TR≥TR5 | 51.3 [47.5–55.2] | 70.0 [68.3–71.6] | 26.1 [23.7–28.6] | 87.4 [86.1–88.7] |

AUC (ordinal TR rank): per-nodule **0.6399**, patient-level 0.6478.

### Sensitivity arms (Supp Table S1)

- Arm A relaxed (n=15,309): TR4 ROM 23.0% PPV, Sens 44.6% Spec 78.0% — *more nodules, less precise scoring*
- Arm B first-US-only: identical to primary (nodule_master_id already deduplicates)
- Arm C single-nodule pts (n=782): TR4 30.7%, TR5 34.9% — *higher; selection effect*
- Arm D unilateral-path-only: TR4 8.5%, TR5 10.7% — *lower; conservative bilateral exclusion*

---

## PROMPT TO PASTE INTO THE NEW COWORK CHAT (so it picks up cleanly)

> Continuing M025 v2.0 (TI-RADS nodule-level pivot) from a prior Cowork session. Read `/Users/loganglosser/THYROID_2026/COWORK_HANDOFF_M025_V2_PIVOT_20260504.md` end-to-end before doing anything else — it has the data-warehouse access state, the headline finding, what cursor is doing, and what's been pre-baked. The MotherDuck MCP is connected. Snowflake auth is Cortex-only (don't bother with `snow sql`; use `cortex` for Cortex services). After reading the handoff, check git for any new commits from cursor (mig_264 or mig_307) and run `SELECT mig_id, signed_off_at, summary FROM main.signoff_migration ORDER BY signed_off_at DESC LIMIT 10` against the MotherDuck `thyroid_canonical_publication_v1_0` database to see latest activity. Then ask me what to do next.
