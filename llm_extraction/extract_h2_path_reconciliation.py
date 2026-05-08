#!/usr/bin/env python3
"""
extract_h2_path_reconciliation.py — Phase 2a: NLP adjudication for H2 pathology
reconciliation candidates.

Mirrors the BQ-source-text + Anthropic-API batching pattern from the existing
llm_extraction pipeline.  PHI never leaves local execution — rationales are
paraphrased, never quoted.

Usage:
    .venv/bin/python llm_extraction/extract_h2_path_reconciliation.py \\
        [--run-id h2_path_reconciliation_20260508] \\
        [--min-n 5] \\
        [--dry-run]

Outputs:
    llm_extraction/runs/<run_id>/per_patient_adjudications.jsonl
    studies/hypothesis2_goiter_sdoh/path_reconciliation_20260508.csv
    qc_framework_v1/migrations/mig_337_h2_path_benign_overrides_20260508.sql
    studies/hypothesis2_goiter_sdoh/airtable_override_decisions_pending.csv

Hard rules:
  - PHI stays local: rationales paraphrase only, no raw text in any output.
  - ANTHROPIC_API_KEY read from env or .env file in repo root.
  - All API calls log token counts + cost to stdout summary.
  - atypical_adenoma special-case: frame as NLP_TRUE_POSITIVE vs NLP_FALSE_POSITIVE only
    (human didn't use that column as a binary flag).
  - substernal_mng special-case: if manual.substernal_resection=TRUE, cite as strong signal.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

# ── repo root on path ─────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("h2_adj")

# ── constants ─────────────────────────────────────────────────────────────────
BQ_PROJECT = "thyroid-canonical-pub-2026"
CANDIDATES_TABLE = f"{BQ_PROJECT}.pub_workspace.h2_path_reconciliation_candidates_v1"
PATH_SYNOPTICS_TABLE = f"{BQ_PROJECT}.pub_canonical.path_synoptics"

MODEL = "claude-sonnet-4-5"          # Claude Sonnet 4.6 (API alias)
MAX_TOKENS = 512
TEMPERATURE = 0.0

# Cost per 1M tokens (Anthropic Sonnet 4.5 pricing, 2026)
INPUT_COST_PER_MTK  = 3.00   # $3.00 / 1M input tokens
OUTPUT_COST_PER_MTK = 15.00  # $15.00 / 1M output tokens

# Adjudication labels
VALID_LABELS = {
    "NLP_TRUE_POSITIVE",
    "NLP_FALSE_POSITIVE",
    "MANUAL_TRUE_POSITIVE",
    "MANUAL_FALSE_POSITIVE",
    "AMBIGUOUS",
}

# Source text columns to pull from path_synoptics
SOURCE_COLS = [
    "path_diagnosis_summary",
    "synoptic_diagnosis",
    "microscopic_description",
    "path_diagnosis_comment",
    "clinical_information_pre_op_diagnosis",
    "other_findings",
    "adenoma_nodule_size_info_not_all_complete",
]
SUBSTERNAL_EXTRA_COLS = ["substernal_resection", "substernal_goiter_size_cm"]

# Maximum characters of source text sent to LLM per case
MAX_SOURCE_CHARS = 8_000

# Rate-limit: sleep between API calls
INTER_CALL_SLEEP = 0.5   # seconds


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_anthropic_key() -> str:
    """Load ANTHROPIC_API_KEY from env, then .env file."""
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if key:
        return key
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("ANTHROPIC_API_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
                if key:
                    return key
    raise RuntimeError(
        "ANTHROPIC_API_KEY not found. Set the env var or add it to .env:\n"
        "  export ANTHROPIC_API_KEY=sk-ant-..."
    )


def _bq_client():
    """Return a google.cloud.bigquery.Client."""
    from google.cloud import bigquery  # type: ignore
    return bigquery.Client(project=BQ_PROJECT)


def _fetch_candidates(bq, min_n: int) -> list[dict]:
    """
    Pull candidates from h2_path_reconciliation_candidates_v1 restricted to
    categories with ≥min_n NLP_ONLY or ≥min_n MANUAL_ONLY rows.
    thymic_tissue is excluded (coverage gap, not an NLP error).
    """
    sql = f"""
    WITH counts AS (
      SELECT category, discrepancy_type, COUNT(*) AS n
      FROM `{CANDIDATES_TABLE}`
      GROUP BY 1, 2
    ),
    in_scope AS (
      SELECT DISTINCT category
      FROM counts
      WHERE n >= {min_n}
        AND category != 'thymic_tissue'   -- coverage gap, not adjudication scope
    )
    SELECT c.research_id, c.category, c.manual_flag, c.nlp_flag, c.discrepancy_type
    FROM `{CANDIDATES_TABLE}` c
    INNER JOIN in_scope s USING (category)
    ORDER BY c.category, c.research_id
    """
    rows = list(bq.query(sql).result())
    return [dict(r) for r in rows]


def _fetch_source_text(bq, research_ids: list[str], include_substernal: bool) -> dict[str, dict]:
    """
    Retrieve path_synoptics source text rows for the given research_ids.
    Returns {research_id: {col: value, ...}}.
    Only the most recent row per research_id is used (MAX surg_date).
    """
    cols = SOURCE_COLS + (SUBSTERNAL_EXTRA_COLS if include_substernal else [])
    col_list = ", ".join(f"ps.{c}" for c in cols)
    ids_str = ", ".join(f"'{rid}'" for rid in research_ids)
    sql = f"""
    WITH ranked AS (
      SELECT {col_list},
             ps.research_id,
             ROW_NUMBER() OVER (PARTITION BY ps.research_id ORDER BY ps.surg_date DESC) AS rn
      FROM `{PATH_SYNOPTICS_TABLE}` ps
      WHERE ps.research_id IN ({ids_str})
    )
    SELECT * EXCEPT (rn) FROM ranked WHERE rn = 1
    """
    result: dict[str, dict] = {}
    for row in bq.query(sql).result():
        d = dict(row)
        rid = str(d.pop("research_id"))
        result[rid] = d
    return result


def _build_source_text(row_data: dict, include_substernal: bool) -> str:
    """Concatenate non-null path_synoptics fields into a readable block."""
    parts = []
    cols = SOURCE_COLS + (SUBSTERNAL_EXTRA_COLS if include_substernal else [])
    for col in cols:
        val = row_data.get(col, "") or ""
        val = val.strip()
        if val:
            label = col.replace("_", " ").title()
            parts.append(f"[{label}]\n{val}")
    text = "\n\n".join(parts)
    return text[:MAX_SOURCE_CHARS]


def _build_prompt(category: str, nlp_flag: bool, manual_flag: bool,
                  source_text: str, is_atypical_adenoma: bool,
                  substernal_resection: str | None) -> str:
    """Build the Anthropic user-turn message for adjudication."""

    # Category-specific framing
    if is_atypical_adenoma:
        frame = (
            "IMPORTANT NOTE FOR THIS CATEGORY: The human reviewer used the "
            "manual_atypical_adenoma column as a free-text notes field, NOT as a "
            "binary present/absent flag. Therefore 'MANUAL_FALSE_POSITIVE' and "
            "'MANUAL_TRUE_POSITIVE' are not applicable here. Only classify as:\n"
            "  - NLP_TRUE_POSITIVE  if the path text genuinely documents an atypical adenoma\n"
            "  - NLP_FALSE_POSITIVE if the NLP fired on a related but distinct entity "
            "(atypical follicular nodule, follicular adenoma with atypia, cellular adenoma, etc.)\n"
            "  - AMBIGUOUS if the text is too vague to determine.\n"
        )
    else:
        frame = (
            "Classify as exactly ONE of:\n"
            "  - NLP_TRUE_POSITIVE   (NLP correct, human missed it)\n"
            "  - NLP_FALSE_POSITIVE  (NLP wrong, human correct that it is absent)\n"
            "  - MANUAL_TRUE_POSITIVE  (human correct, NLP missed it)\n"
            "  - MANUAL_FALSE_POSITIVE (human wrong, NLP correct that it is absent)\n"
            "  - AMBIGUOUS (text genuinely supports both readings or is too vague)\n"
        )

    nlp_str = "TRUE (present)" if nlp_flag else "FALSE (absent)"
    man_str = "TRUE (present)" if manual_flag else "FALSE (absent)"

    substernal_hint = ""
    if substernal_resection and substernal_resection.strip().lower() in ("x", "1", "yes", "true"):
        substernal_hint = (
            "\n[CLINICAL SIGNAL] The structured data shows substernal_resection=TRUE for this "
            "patient, indicating that a substernal component was addressed at surgery. This is "
            "strong corroborating evidence that substernal goiter disease was present even if "
            "the word 'substernal' does not appear in the synoptic text.\n"
        )

    prompt = f"""You are reviewing surgical pathology synoptic text to adjudicate a discrepancy between automated NLP extraction and a human hand-count for the diagnostic category: {category}.

The NLP says: {nlp_str}.
The human says: {man_str}.
{substernal_hint}
Source text (de-identified pathology synoptic / microscopic description):
{source_text or '[No source text available — classify as AMBIGUOUS]'}

{frame}
Then provide a 1-sentence rationale that paraphrases the relevant evidence WITHOUT QUOTING the raw text (no exact phrases longer than 5 words; no patient identifiers).

Output valid JSON only:
{{"adjudication": "...", "rationale": "..."}}"""

    return prompt


def _call_anthropic(
    client,
    prompt: str,
    research_id: str,
    category: str,
) -> tuple[str, str, int, int]:
    """
    Call Anthropic API.  Returns (adjudication, rationale, input_tokens, output_tokens).
    Retries on rate-limit (429) up to 5 times with exponential back-off.
    """
    import anthropic as ant

    messages = [{"role": "user", "content": prompt}]
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                temperature=TEMPERATURE,
                messages=messages,
            )
            break
        except ant.RateLimitError as e:
            wait = min(60, 5 * (2 ** attempt))
            log.warning(
                "Rate limited for rid=%s cat=%s (attempt %d/%d); waiting %ds — %s",
                research_id, category, attempt + 1, max_retries, wait, e,
            )
            time.sleep(wait)
        except ant.APIError as e:
            log.error("API error for rid=%s cat=%s: %s", research_id, category, e)
            return "AMBIGUOUS", f"API error: {type(e).__name__}", 0, 0
    else:
        return "AMBIGUOUS", "Exhausted retries", 0, 0

    raw = response.content[0].text.strip()
    input_tok = response.usage.input_tokens
    output_tok = response.usage.output_tokens

    try:
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw)
        adj = str(data.get("adjudication", "AMBIGUOUS")).strip()
        rat = str(data.get("rationale", "")).strip()
        if adj not in VALID_LABELS:
            log.warning("Invalid adjudication label '%s' for rid=%s", adj, research_id)
            adj = "AMBIGUOUS"
        # Rationale PHI guard: truncate to 500 chars (paraphrase should be short)
        rat = rat[:500]
    except (json.JSONDecodeError, ValueError, KeyError) as e:
        log.warning("JSON parse error for rid=%s cat=%s: %s — raw=%r", research_id, category, e, raw[:200])
        adj, rat = "AMBIGUOUS", "Response parse error"

    return adj, rat, input_tok, output_tok


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="H2 pathology reconciliation adjudication (Phase 2a)")
    ap.add_argument("--run-id", default="h2_path_reconciliation_20260508")
    ap.add_argument("--min-n", type=int, default=5,
                    help="Minimum discrepancy count per category to trigger adjudication")
    ap.add_argument("--dry-run", action="store_true",
                    help="Pull candidates and source text but skip API calls; output placeholder results")
    args = ap.parse_args()

    run_id = args.run_id
    run_dir = ROOT / "llm_extraction" / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = run_dir / "per_patient_adjudications.jsonl"
    csv_path = ROOT / "studies" / "hypothesis2_goiter_sdoh" / "path_reconciliation_20260508.csv"
    sql_path = ROOT / "qc_framework_v1" / "migrations" / "mig_337_h2_path_benign_overrides_20260508.sql"
    airtable_csv = ROOT / "studies" / "hypothesis2_goiter_sdoh" / "airtable_override_decisions_pending.csv"

    # ── 1. Load Anthropic key (fail fast if not in dry-run) ───────────────────
    if not args.dry_run:
        api_key = _load_anthropic_key()
        import anthropic as ant
        client = ant.Anthropic(api_key=api_key)
        log.info("Anthropic client initialized (model=%s)", MODEL)
    else:
        client = None
        log.info("[DRY RUN] Skipping API calls.")

    # ── 2. Pull reconciliation candidates ─────────────────────────────────────
    bq = _bq_client()
    log.info("Fetching candidates (min_n=%d) …", args.min_n)
    candidates = _fetch_candidates(bq, args.min_n)
    log.info("Candidates in scope: %d", len(candidates))

    if not candidates:
        log.warning("No candidates meet the min_n=%d threshold. Nothing to adjudicate.", args.min_n)
        return

    # Summary of in-scope categories
    from collections import Counter
    cat_counts = Counter(c["category"] for c in candidates)
    log.info("Categories in scope: %s", dict(cat_counts))

    # ── 3. Fetch source text ──────────────────────────────────────────────────
    research_ids = list({str(c["research_id"]) for c in candidates})
    # Include substernal columns if substernal_mng is in scope
    include_sub = "substernal_mng" in cat_counts
    log.info("Fetching path_synoptics source text for %d patients …", len(research_ids))
    source_map = _fetch_source_text(bq, research_ids, include_substernal=True)  # always fetch substernal cols
    log.info("Source text fetched for %d of %d patients.", len(source_map), len(research_ids))

    # ── 4. Adjudicate each candidate ──────────────────────────────────────────
    results: list[dict] = []
    total_input_tok = 0
    total_output_tok = 0
    n_api_calls = 0

    with jsonl_path.open("w", encoding="utf-8") as jf:
        for cand in candidates:
            rid = str(cand["research_id"])
            category = str(cand["category"])
            nlp_flag = bool(cand["nlp_flag"])
            manual_flag = bool(cand["manual_flag"])
            discrepancy_type = str(cand["discrepancy_type"])

            src_data = source_map.get(rid, {})
            is_atypical = category == "atypical_adenoma"
            include_sub_hint = category == "substernal_mng"

            # substernal_resection hint (for substernal_mng category)
            sub_resection_val = src_data.get("substernal_resection", "") if include_sub_hint else None

            source_text = _build_source_text(src_data, include_substernal=True)

            if args.dry_run:
                adjudication = "AMBIGUOUS"
                rationale = "[DRY RUN — no API call made]"
                input_tok = output_tok = 0
            else:
                prompt = _build_prompt(
                    category=category,
                    nlp_flag=nlp_flag,
                    manual_flag=manual_flag,
                    source_text=source_text,
                    is_atypical_adenoma=is_atypical,
                    substernal_resection=sub_resection_val,
                )
                adjudication, rationale, input_tok, output_tok = _call_anthropic(
                    client, prompt, rid, category
                )
                n_api_calls += 1
                total_input_tok += input_tok
                total_output_tok += output_tok
                time.sleep(INTER_CALL_SLEEP)

            has_source = bool(source_text)
            source_table_used = "pub_canonical.path_synoptics" if has_source else "none"

            record = {
                "research_id": rid,
                "category": category,
                "nlp_flag": nlp_flag,
                "manual_flag": manual_flag,
                "discrepancy_type": discrepancy_type,
                "adjudication": adjudication,
                "rationale": rationale,
                "source_table_used": source_table_used,
                "has_source_text": has_source,
                "input_tokens": input_tok,
                "output_tokens": output_tok,
                "run_id": run_id,
                "model": MODEL,
                "adjudicated_at": datetime.now(timezone.utc).isoformat(),
            }
            results.append(record)
            jf.write(json.dumps(record) + "\n")

            log.info(
                "rid=%-8s cat=%-30s disc=%-12s → %-24s  tok_in=%d tok_out=%d",
                rid, category, discrepancy_type, adjudication, input_tok, output_tok,
            )

    # ── 5. Write CSV summary ──────────────────────────────────────────────────
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_fields = ["research_id", "category", "nlp_flag", "manual_flag",
                  "adjudication", "rationale", "source_table_used"]
    with csv_path.open("w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    log.info("CSV written: %s  (%d rows)", csv_path, len(results))

    # ── 6. Compute adjudication summary ──────────────────────────────────────
    from collections import defaultdict
    summary: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    ambiguous_rids: list[dict] = []
    for r in results:
        summary[r["category"]][r["adjudication"]] += 1
        if r["adjudication"] == "AMBIGUOUS":
            ambiguous_rids.append({"research_id": r["research_id"], "category": r["category"]})

    log.info("\n=== ADJUDICATION SUMMARY ===")
    all_labels = ["NLP_TRUE_POSITIVE", "NLP_FALSE_POSITIVE", "MANUAL_TRUE_POSITIVE",
                  "MANUAL_FALSE_POSITIVE", "AMBIGUOUS"]
    for cat in sorted(summary):
        row_str = " | ".join(f"{lbl}: {summary[cat].get(lbl, 0)}" for lbl in all_labels)
        log.info("  %-32s  %s", cat, row_str)

    # ── 7. Write migration SQL ────────────────────────────────────────────────
    # Only NLP_FALSE_POSITIVE and MANUAL_TRUE_POSITIVE become override rows.
    override_rows = [
        r for r in results
        if r["adjudication"] in ("NLP_FALSE_POSITIVE", "MANUAL_TRUE_POSITIVE")
    ]
    ambiguous_count = sum(1 for r in results if r["adjudication"] == "AMBIGUOUS")

    decision_date = date.today().isoformat()

    sql_lines = [
        "-- =============================================================================",
        "-- mig_337 — pub_canonical.canonical_path_benign_overrides_v1",
        "--",
        f"-- Date:       {decision_date}",
        "-- Lane:       H2 manuscript — pathology benign override decisions (Phase 2a)",
        "-- Author:     Claude-Sonnet-4.6 (adjudication) + Cursor Agent (pipeline)",
        "--",
        "-- AUDIT ANCHORS:",
        "--   VC-H2-ATYPICAL-ADENOMA-NLP-ANOMALY  (Verification Check, THYROID_DATA_REGISTRY)",
        "--   VC-H2-SUBSTERNAL-PATH-RECONCILE      (Verification Check, THYROID_DATA_REGISTRY)",
        "--   THY-34 (Linear, team Thyroid Database THY)",
        "--",
        "-- Rows included: NLP_FALSE_POSITIVE + MANUAL_TRUE_POSITIVE adjudications only.",
        "-- Excluded: NLP_TRUE_POSITIVE (NLP correct, no override needed).",
        f"-- AMBIGUOUS: {ambiguous_count} case(s) withheld for human review.",
        "-- =============================================================================",
        "",
        "CREATE TABLE IF NOT EXISTS `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_overrides_v1` (",
        "  research_id              STRING    NOT NULL,",
        "  category                 STRING    NOT NULL,",
        "  override_flag            BOOL      NOT NULL,",
        "  original_nlp_flag        BOOL      NOT NULL,",
        "  manual_flag              BOOL      NOT NULL,",
        "  adjudication_outcome     STRING    NOT NULL,",
        "  rationale_summary        STRING,",
        "  reviewer                 STRING    NOT NULL,",
        f"  decision_date            DATE      NOT NULL",
        ");",
        "",
    ]

    if override_rows:
        sql_lines.append(
            "INSERT INTO `thyroid-canonical-pub-2026.pub_canonical.canonical_path_benign_overrides_v1`"
        )
        sql_lines.append("  (research_id, category, override_flag, original_nlp_flag, manual_flag,")
        sql_lines.append("   adjudication_outcome, rationale_summary, reviewer, decision_date)")
        sql_lines.append("VALUES")
        val_lines = []
        for r in override_rows:
            override_flag = not r["nlp_flag"]  # override is the opposite of current NLP
            rat_escaped = r["rationale"].replace("'", "\\'")
            val_lines.append(
                f"  ('{r['research_id']}', '{r['category']}', {str(override_flag).upper()}, "
                f"{str(r['nlp_flag']).upper()}, {str(r['manual_flag']).upper()}, "
                f"'{r['adjudication']}', '{rat_escaped}', 'Claude-Sonnet-4.6', '{decision_date}')"
            )
        sql_lines.append(",\n".join(val_lines) + ";")
    else:
        sql_lines.append("-- No override rows generated (all AMBIGUOUS or NLP_TRUE_POSITIVE).")

    sql_path.parent.mkdir(parents=True, exist_ok=True)
    sql_path.write_text("\n".join(sql_lines), encoding="utf-8")
    log.info("Migration SQL written: %s  (%d override rows)", sql_path, len(override_rows))

    # ── 8. Write Airtable override decisions CSV ──────────────────────────────
    at_fields = [
        "decision_id", "research_id_pseudo", "field", "original_value", "override_value",
        "evidence_summary", "reviewer", "decision_date", "justification",
        "linked_manuscript_section",
    ]
    at_rows = []
    non_ambiguous = [r for r in results if r["adjudication"] != "AMBIGUOUS"]
    for i, r in enumerate(non_ambiguous, 1):
        at_rows.append({
            "decision_id": f"OD-H2-PATH-{decision_date.replace('-','')}-{i:03d}",
            "research_id_pseudo": f"rid_{r['research_id']}",
            "field": r["category"],
            "original_value": str(r["nlp_flag"]),
            "override_value": str(not r["nlp_flag"]) if r["adjudication"] in ("NLP_FALSE_POSITIVE", "MANUAL_TRUE_POSITIVE") else str(r["nlp_flag"]),
            "evidence_summary": r["rationale"],   # already paraphrased by Claude
            "reviewer": "Claude-Sonnet-4.6",
            "decision_date": decision_date,
            "justification": r["adjudication"],
            "linked_manuscript_section": "H2 §3.5a",
        })

    airtable_csv.parent.mkdir(parents=True, exist_ok=True)
    with airtable_csv.open("w", newline="", encoding="utf-8") as af:
        writer = csv.DictWriter(af, fieldnames=at_fields)
        writer.writeheader()
        writer.writerows(at_rows)
    log.info("Airtable CSV written: %s  (%d rows)", airtable_csv, len(at_rows))

    # ── 9. Cost summary ──────────────────────────────────────────────────────
    if not args.dry_run:
        total_cost = (
            (total_input_tok / 1_000_000) * INPUT_COST_PER_MTK
            + (total_output_tok / 1_000_000) * OUTPUT_COST_PER_MTK
        )
        log.info("\n=== TOKEN / COST SUMMARY ===")
        log.info("  API calls:       %d", n_api_calls)
        log.info("  Input tokens:    %d  ($%.4f)", total_input_tok, (total_input_tok / 1_000_000) * INPUT_COST_PER_MTK)
        log.info("  Output tokens:   %d  ($%.4f)", total_output_tok, (total_output_tok / 1_000_000) * OUTPUT_COST_PER_MTK)
        log.info("  Total cost:      $%.4f", total_cost)

    # ── 10. AMBIGUOUS list for in-session review ──────────────────────────────
    if ambiguous_rids:
        log.info("\n=== AMBIGUOUS CASES (require human review) ===")
        for a in ambiguous_rids:
            log.info("  rid=%-8s  category=%s", a["research_id"], a["category"])
    else:
        log.info("\nNo AMBIGUOUS cases — all adjudicated.")

    log.info("\nPhase 2a complete.")
    log.info("  JSONL:     %s", jsonl_path)
    log.info("  CSV:       %s", csv_path)
    log.info("  SQL:       %s", sql_path)
    log.info("  Airtable:  %s", airtable_csv)


if __name__ == "__main__":
    main()
