# Open items B3 & B5 (post–full-execution re-audit)

These items are **not** “bugs” in the April scoped standard; they are **documented limits** of the strong completeness standard or **future pipeline** work.

## B3 — Numeric Bethesda in `fna_episode_master_v2`

**Situation:** Some rows have `bethesda_category` NULL in the episode master table.

**Analysis SSOT:** Use **`v_fna_episode_bethesda_resolved_v1`** (`scripts/sql/source_truth_confirmation_v1.sql`, deploy `151_source_truth_confirmation_v1.py --md`), which implements `COALESCE(episode.bethesda_category, fna_cytology.category_num)` and documents remaining NULLs in `bethesda_unscorable_reason` (`no_episode_or_cytology_bethesda`, `pathology_present_bethesda_unparsed`, `non_numeric_or_asterisk_bethesda_raw`, etc.).

**Convenience:** **`v_fna_bethesda_episode_vs_resolved_v1`** (SQL `scripts/sql/v_fna_bethesda_episode_vs_resolved_v1.sql`, deploy `scripts/156_md_bethesda_episode_vs_resolved_view.py --md`) exposes episode column vs resolved column side-by-side with `bethesda_analysis_bucket`.

**Policy:** Do **not** silently UPDATE `fna_episode_master_v2` to invent Bethesda; any backfill requires approved rules and source provenance.

## B5 — Structured per-level US cervical lymph nodes

**Situation:** There is **no** manuscript-grade table of lymph-node **level**, **laterality**, and **size** parsed from US in this repository.

**What exists:** Exam-level structured text (e.g. `ultrasound_reports.lymph_node_assessment` where deployed), plus note-derived/heuristic audits described in re-audit exports (`us_lymph_node_db_summary.csv`, `us_lymph_node_audit_expanded.csv`).

**What would be needed:** Institutional radiology feed or a governed NLP pipeline with validation and review queues — out of scope for a read-only re-audit.

**Manuscript scope:** Any claim about “all LN data structured” must be **narrowed** to available fields or deferred until a dedicated LN extraction project lands.
