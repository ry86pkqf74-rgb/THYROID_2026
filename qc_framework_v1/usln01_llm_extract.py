"""
USLN01 — LLM extract pass for US lymph nodes (SKELETON, DO NOT RUN YET)
========================================================================

Issue: `main.canonical_us_lymph_node_v2` is 99.9% shell (6,793 / 6,801). The
parser layer never populated structured LN fields. This script is a scaffold
for the LLM re-parse pass that will extract
(laterality, neck_level, short_axis_mm, long_axis_mm, size_cm_max,
 shape, echogenicity, hilum_preserved, calcifications, cystic_component,
 vascularity_pattern, extranodal_extension_on_us, suspicious_flag,
 suspicion_level, biopsy_recommended, confidence)
from the parent exam's impression text.

Inputs:
  manuscript_workspace.qc_usln01_llm_candidates_v1 — 855 exams

Output (when authorized):
  manuscript_workspace.canonical_us_lymph_node_v2_usln01_patch_v1
  Columns: research_id, us_exam_id, ln_index_in_exam, <fields above>,
           llm_model, llm_run_ts, confidence.

Execution gate (do NOT bypass):
  1. Cost estimate signed off (tokens * 855 exams * model price).
  2. Prompts reviewed on a 20-exam sample draw.
  3. Confidence thresholds agreed (below threshold -> row dropped,
     flagged 'llm_low_conf' in run log).

Author:  Logan Glosser
Opened:  2026-04-23 (prompt 29)
"""

from __future__ import annotations

# import duckdb
# import anthropic   # or openai; model choice pending sign-off


MODEL = None            # set post-approval
BATCH_SIZE = 20
CONF_FLOOR = 0.65


def load_candidates(con):
    """Pull the 855 candidate exams."""
    return con.execute("""
        SELECT research_id, us_exam_id, exam_date,
               impression_excerpt, clinical_excerpt
        FROM manuscript_workspace.qc_usln01_llm_candidates_v1
        ORDER BY research_id, exam_date
    """).fetchdf()


def build_prompt(row) -> str:
    """Assemble the per-exam LLM prompt. DRAFT — needs review."""
    raise NotImplementedError("Prompt drafting awaits sample-review sign-off")


def parse_response(raw: str) -> list[dict]:
    """Parse LLM JSON into structured LN rows. DRAFT."""
    raise NotImplementedError("Response schema awaits sign-off")


def write_patch(con, rows: list[dict]) -> None:
    """Append extracted rows to the patch table. DRAFT."""
    raise NotImplementedError("Patch table DDL defined on first run")


def main():
    raise SystemExit(
        "USLN01 LLM extract is NOT authorized. "
        "Review qc_usln01_llm_candidates_v1 and sign off on cost/prompts first."
    )


if __name__ == "__main__":
    main()
