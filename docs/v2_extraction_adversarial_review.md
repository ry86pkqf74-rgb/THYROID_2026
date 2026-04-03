# Adversarial Review: Registry-Driven V2 Extraction Flow

**Date:** 2026-04-03
**Scope:** Break the design before promotion
**Method:** Code-grounded, severity-ranked findings

---

## Finding 1 — CRITICAL: `prompt_for_domain()` silently drops multi-prompt domains

**File:** `llm_extraction/registry.py` lines 119-123
**Failure mode:** `prompt_for_domain()` returns only `spec.prompts[0]`, discarding all subsequent prompts. For multi-prompt domains (genetics has `genetics_extraction_v1.txt` + `molecular_thyroseq_afirma_extraction_v1.txt`; recurrence has `recurrence_extraction_v1.txt` + `recurrence_detailed_extraction_v1.txt`; complications, medications, operative_detail, parathyroid_detail similarly), the local `run_extraction.py` LLM path only sends the first prompt to the model.

**Why it matters:** The VastAI fleet uses its own `DOMAIN_PROMPT` map (lines 56-93 of `scripts/vastai/run_extraction_concurrent.py`) where each sub-prompt is a separate domain key (e.g. `recurrence_detailed`, `molecular_thyroseq_afirma`). But the local registry-driven `run_extraction.py` `run_llm_for_domain()` function calls `llm.extract(..., domain=domain_name)` which resolves through `_load_system_prompt(domain=domain)` -> `prompt_for_domain(domain)` -> first prompt only. The second/third prompts are never sent. This means local v2 extraction silently produces incomplete results for 6 multi-prompt domains while reporting success.

**Minimal fix:** Either iterate `spec.prompts` in `run_llm_for_domain()` (one pass per prompt, merged output), or split multi-prompt domains into separate registry entries matching the fleet's flat map.

**Blocks promotion:** YES — local extraction for multi-prompt domains is structurally incomplete.

---

## Finding 2 — HIGH: `LLMExtractor.entity_domain` is hardcoded to `"llm"`, leaking v2 into generic bucket

**File:** `llm_extraction/extract_llm.py` line 67
**Failure mode:** `entity_domain = "llm"` is a class attribute on `LLMExtractor`. In `run_extractors()` (the v1 regex pass), when `LLMExtractor` is included among the extractors (line 642-646, 680-681 of `run_extraction.py`), all its output is keyed to domain `"llm"` in `domain_results`. This is correct for v1's legacy audit bucket. However, in `run_llm_for_domain()` (v2 path), the `_stamp_row()` function sets `is_llm=True` and stamps provenance, but the DataFrame's `entity_domain` column (if present) still reflects `"llm"` from the `EntityMatch.to_dict()` output because `BaseExtractor.__init__` sets `self.entity_domain`. The v2 per-domain parquet is written to the correct file, but the row-level `entity_domain` metadata inside the parquet can say `"llm"` rather than the actual domain name.

**Why it matters:** Downstream consumers that filter on `entity_domain == "staging"` (or any v2 domain name) inside the parquet will get zero rows. The `_stamp_row()` function does not overwrite `entity_domain`; it only stamps provenance fields. Script 103 `_expand_v2_fleet_parquet()` also does not set `entity_domain` during expansion (lines 88-149). The `fact_domain` in the canonical fact table is derived from the file stem via `ENTITY_DOMAIN_MAP`, not from the row's `entity_domain` column, which masks this bug — but any direct parquet consumer (script 111 concordance, dashboard ad-hoc queries) will see the wrong domain label.

**Minimal fix:** In `run_llm_for_domain()`, after `rec = m.to_dict()`, set `rec["entity_domain"] = domain_name` before calling `_stamp_row()`.

**Blocks promotion:** YES for any workflow that reads per-domain parquets and filters on `entity_domain`.

---

## Finding 3 — HIGH: Script 111 defaults to monolithic `note_entities_llm.parquet`

**File:** `scripts/111_llm_extraction_validation.py` lines 1648-1651
**Failure mode:** When invoked without `--domain`, `--all-llm-domains`, or `--input`, the validator defaults to `processed/note_entities_llm.parquet` — the legacy merged audit artifact that is only written with `--merge-audit`. In the v2 world, this file either does not exist (validation crashes) or is stale from a previous `--merge-audit` run (validation passes on old data). The error message at line 1332-1334 references "entity-grain note_entities_llm.parquet" as the fix, reinforcing the monolithic assumption.

**Why it matters:** A developer running `python scripts/111_llm_extraction_validation.py` after a v2 extraction will either get a FileNotFoundError or validate stale data — both are misleading success/failure. The `--all-llm-domains` flag exists but is not the default, creating an attractive nuisance.

**Minimal fix:** Change the bare-invocation default to `--all-llm-domains` or error with a message explaining that v2 requires `--domain` or `--all-llm-domains`.

**Blocks promotion:** No (workaround exists via `--all-llm-domains`), but high risk of misleading validation results.

---

## Finding 4 — HIGH: VastAI fleet `DOMAIN_PROMPT` map can drift from registry YAML

**File:** `scripts/vastai/run_extraction_concurrent.py` lines 56-93 vs `config/extraction_domain_registry.yaml`
**Failure mode:** The fleet runner has its own hardcoded `DOMAIN_PROMPT` dict mapping 36 domain keys to prompt filenames. This is independent of the registry YAML. If a new domain is added to the registry, or a prompt file is renamed/updated, the fleet will not pick it up. Conversely, the fleet maps domains like `operative_v2_enrichment`, `parathyroid_per_gland`, `molecular_thyroseq_afirma`, and `complications_rln_laryngoscopy` as top-level domains, while the registry YAML treats these as secondary prompts under parent domains (e.g., `parathyroid_per_gland` is a sub-prompt of `parathyroid_detail`). Script 112's `SUB_PROMPT_STEM_MAP` (lines 96-103) maps these back, but a new sub-prompt domain on the fleet that is not in `SUB_PROMPT_STEM_MAP` will appear as `UNCLAIMED` in the promotion gate inventory and be silently excluded from concordance validation.

**Why it matters:** The YAML comments declare it the "SINGLE SOURCE OF TRUTH" (line 6 of registry YAML), but the fleet does not read it. Any registry-only change creates a silent divergence where the fleet extracts using old prompts while local tooling validates against new ones.

**Minimal fix:** Have the fleet runner load `DOMAIN_PROMPT` from the registry YAML at startup, or at minimum add a CI check that asserts `set(DOMAIN_PROMPT.keys())` matches `{d.parquet_stem for d in registry}` plus known sub-prompt stems.

**Blocks promotion:** No (current fleet and registry are in sync as of this review), but high future-drift risk.

---

## Finding 5 — HIGH: 20+ scripts hardcode `md=True` without `fail_closed=True`

**Files:** `scripts/99_comprehensive_final_verification.py`, `scripts/98_final_verification_pass.py`, `scripts/75_dataset_maturation.py`, `scripts/56_pre_manuscript_audit.py`, `scripts/55_analysis_validation_suite.py`, `scripts/53_longitudinal_lab_hardening.py`, `scripts/52_complication_phenotyping_v2.py`, `scripts/51_thyroid_scoring_systems.py`, `scripts/50_multinodule_imaging.py`, `scripts/49_enhanced_linkage_v3.py`, `scripts/48_build_analysis_resolved_layer.py`, `scripts/46_provenance_audit.py`, `scripts/105_manuscript_freeze_v1.py`, `scripts/100_canonical_metrics_registry.py`, `scripts/108_synoptic_tumor_long_v1.py`, `scripts/110_operative_notes_full_history_scan.py`, `scripts/98_multi_surgery_artifact_linkage_audit.py`, and others.

**Failure mode:** These scripts call `connect_md_or_file(DB_PATH, md=True)` — note `fail_closed` defaults to `False`. Per `utils/md_connect.py` lines 104-122, when `md=True` but the MotherDuck token is missing or the connection fails, the function silently falls back to `duckdb.connect(str(db_path))` (local file) with only a print message. The script then runs against the local DuckDB file believing it is on MotherDuck.

**Why it matters:** Violates AGENTS.md rule: "Never silently overwrite conflicting clinical values." A verification script that intends to audit MotherDuck data but silently reads a stale local file will report clean results against wrong data. The 3 scripts that *do* use `fail_closed=args.md` (103, 113, 02b) prove the pattern is understood but not universally applied.

**Minimal fix:** Change all `connect_md_or_file(DB_PATH, md=True)` calls in scripts that write or verify MotherDuck data to `connect_md_or_file(DB_PATH, md=True, fail_closed=True)`, or use the `connect_md_fail_closed()` convenience alias.

**Blocks promotion:** No (data writes to MotherDuck from scripts 103/113/02b are protected), but verification scripts can produce false-clean reports.

---

## Finding 6 — MEDIUM: Parquet stem shadowing in script 103 hides fleet outputs

**File:** `scripts/103_fact_lineage_materialize.py` lines 573-584 (per subagent report)
**Failure mode:** Script 103 loads domain parquets from `processed/{stem}.parquet` first, then falls back to `processed/output/v2_parquets/{stem}.parquet` only if the first path does not exist. If a stale local-extraction parquet exists at `processed/note_entities_llm_imaging.parquet` from a prior `run_extraction.py` run, it will shadow the fleet's more complete file at `processed/output/v2_parquets/note_entities_llm_imaging.parquet`.

**Why it matters:** The fleet produces 11,037-note-per-domain parquets; a stale local parquet might have only a few hundred rows from a test run. Script 103 will build the canonical fact table from the incomplete local file without warning. No check compares row counts between the two paths.

**Minimal fix:** When both paths exist, log a warning comparing row counts and prefer the larger file, or require an explicit `--prefer-local` flag to use the `processed/` path over `v2_parquets/`.

**Blocks promotion:** YES for any domain where both paths exist with different row counts.

---

## Finding 7 — MEDIUM: Cross-wave Tg dedup uses value-inclusive partition key, missing true clinical repeats

**File:** `scripts/113_tg_lab_ingestion.py` lines 980-1013 (DEDUP_MAP_SQL)
**Failure mode:** The dedup partition key is `(research_id, lab_date, lab_name_standardized, COALESCE(CAST(value_numeric AS VARCHAR), value_raw))`. Including the value in the partition key means two measurements on the same day with *different* values (e.g., pre-dose and post-dose Tg on an RAI treatment day) are each kept as rank-1. This is correct for true repeat specimens. However, the `CROSS_WAVE_REVIEW_SQL` (lines 1017-1053) only flags rows where `COUNT(DISTINCT ingestion_wave) > 1 AND COUNT(DISTINCT value_numeric) > 1` — it requires *both* conditions. A single-wave patient with two different same-day Tg values (pre-dose 2.1, post-dose 0.3) will never appear in the review queue because the wave count is 1.

**Why it matters:** Pre/post-dose Tg on RAI treatment days is a clinically important scenario for the `tg_kinetics` domain. Both values are kept (correct), but neither is flagged for review to determine which represents the clinical baseline vs. treatment response. Downstream Tg trajectory analysis (`tg_rising_flag`, `tg_nadir`) may use the wrong value as the nadir if the post-dose suppressed value is lower.

**Minimal fix:** Add a separate review rule for same-day, same-wave, different-value measurements: `HAVING COUNT(DISTINCT value_numeric) > 1` without requiring multi-wave.

**Blocks promotion:** No (data is preserved, not lost), but can affect Tg kinetics accuracy for RAI-treated patients.

---

## Finding 8 — MEDIUM: `_log_domain_summary` logs top `entity_value_norm` values

**File:** `llm_extraction/run_extraction.py` lines 442-444
**Failure mode:** `_log_domain_summary()` logs the top 5 `entity_value_norm` values by count. For domains like `medications`, `problem_list`, or `past_medical_hx`, these values are clinical terms (drug names, diagnoses) associated with patient counts. While not PHI per se, AGENTS.md line 11 states: "Never print full clinical note text in logs; use truncated snippets (e.g. first 80 chars) for PHI safety." The `entity_value_norm` is derived from clinical note text and may contain identifying clinical details.

**Why it matters:** In a multi-patient extraction log, seeing "entity_value_norm: rare_genetic_condition_name: 1" effectively identifies a single patient with a rare condition, violating the spirit of PHI-safe logging even though it is technically an extracted entity label, not raw note text.

**Minimal fix:** Suppress `entity_value_norm` logging for domains where entity values could be identifying (medications, problem_list, past_medical_hx, genetics), or only log values with count >= N (e.g., 5) to prevent re-identification of rare conditions.

**Blocks promotion:** No, but violates AGENTS.md PHI-safety spirit.

---

## Finding 9 — MEDIUM: `save_parquet()` overwrites without provenance check

**File:** `utils/text_helpers.py` (via `save_parquet`)
**Failure mode:** `save_parquet(df, out_path)` unconditionally calls `df.to_parquet(out_path)`. No check for existing file, no backup, no hash comparison. When `run_extraction.py` does a full run (no `--target`), `_write_domain_parquet()` at line 456 writes to `processed/{stem}.parquet` — replacing whatever was there. If the v1 regex extractor and v2 LLM extractor both write domains that share a stem (they don't currently, but the guard is only the registry mapping, not code-level), one would silently overwrite the other.

**Why it matters:** AGENTS.md line 13: "Preserve original values... never overwrite source data." While extraction outputs are derived data, a full re-run without `--merge-audit` silently replaces all per-domain parquets with no ability to detect regressions. The `_merge_into_existing()` function (lines 364-393) only preserves unaffected patients during targeted runs; full runs (`replace_research_ids is None`) bypass the merge entirely (line 376).

**Minimal fix:** Before overwriting, compute a row-count delta and log a warning if the new file is >10% smaller than the existing one (regression detection). Optionally write a `.bak` sidecar.

**Blocks promotion:** No (by-design for full runs), but silent overwrite risk during development.

---

## Finding 10 — LOW: `_filter_notes_by_scope` treats unknown scopes as `"all"`

**File:** `llm_extraction/run_extraction.py` lines 105-127
**Failure mode:** If a new `note_scope` value is added to the registry YAML (e.g., `"endocrine_note"`) but not to `_NOTE_SCOPE_TYPES` in `run_extraction.py`, the filter logs a warning but returns all notes unfiltered. This means a domain intended to run only on endocrine notes would silently run on all 100K+ notes, wasting API calls and potentially extracting irrelevant entities.

**Why it matters:** The registry `validate_registry()` checks `note_scope` against `_VALID_NOTE_SCOPES` in `registry.py` (line 23), but this validation only runs when explicitly invoked. A registry edit + extraction run without `--validate-only` first would silently bypass the scope filter.

**Minimal fix:** Change the unknown-scope behavior from warn-and-pass-all to raise a `ValueError`, or at minimum fail the extraction with a non-zero exit code.

**Blocks promotion:** No (current scopes are in sync), but latent bug for future domains.

---

## Finding 11 — LOW: Script 112 promotion gate G3 treats provenance as optional

**File:** `scripts/112_v2_domain_promotion_gate.py` (per subagent report, G3 conditional)
**Failure mode:** The promotion gate's provenance check (G3) allows domains to pass without provenance columns (`preprocess_batch_id`, `preprocess_script_version`, `preprocessed_at_utc`). These columns are metadata-tier (line 79-83 of script 112: `ENTITY_METADATA_COLUMNS = set(ENTITY_SCHEMA_COLUMNS) - ENTITY_CORE_COLUMNS`). A domain parquet that has zero provenance metadata will pass G3 without error.

**Why it matters:** AGENTS.md provenance preservation rules require traceability. A promoted domain without `extracted_at`, `llm_model`, or `preprocessed_at_utc` breaks the lineage audit (script 46) and makes it impossible to trace which model/prompt produced a given extraction.

**Minimal fix:** Make `extracted_at` and `llm_model` non-nullable in the G3 check for v2 LLM domains (they are always populated by the fleet runner).

**Blocks promotion:** No (fleet parquets do have provenance), but the gate does not enforce it.

---

## Summary

| # | Severity | Finding | Blocks? | Status |
|---|----------|---------|---------|--------|
| 1 | CRITICAL | `prompt_for_domain()` drops multi-prompt domains | YES | FIXED: added `prompts_for_domain()` returning all prompts |
| 2 | HIGH | `entity_domain = "llm"` leaks into v2 parquets | YES | FIXED: stamp `entity_domain = domain_name` in `run_llm_for_domain` |
| 3 | HIGH | Script 111 defaults to monolithic parquet | No | FIXED: error when legacy file absent, warning when used |
| 4 | HIGH | Fleet DOMAIN_PROMPT can drift from registry | No | OPEN: needs CI sync check (no code fix, process gap) |
| 5 | HIGH | 20+ scripts lack `fail_closed` on MotherDuck | No | FIXED: added `fail_closed=True` to 18 scripts |
| 6 | MEDIUM | Parquet stem shadowing in script 103 | YES | FIXED: prefer larger file + row-count warning when both exist |
| 7 | MEDIUM | Tg cross-wave dedup misses single-wave same-day splits | No | FIXED: added `lab_same_day_value_review_v1` table |
| 8 | MEDIUM | `_log_domain_summary` logs potentially identifying values | No | FIXED: suppress values with <5 occurrences |
| 9 | MEDIUM | `save_parquet` overwrites without regression check | No | FIXED: log warning on >10% row-count drop |
| 10 | LOW | Unknown note_scope silently passes all notes | No | FIXED: raise ValueError instead of warn-and-pass |
| 11 | LOW | Promotion gate G3 treats provenance as optional | No | FIXED: removed conditional pass, provenance now required |

**Verdict (updated 2026-04-03):** All 3 blocking findings resolved. 10 of 11 findings fixed. Finding 4 (fleet/registry sync) remains open as a process gap requiring a CI check.
