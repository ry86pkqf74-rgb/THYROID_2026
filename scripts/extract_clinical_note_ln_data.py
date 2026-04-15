#!/usr/bin/env python3
"""
Clinical Note Lymph Node (LN) Extraction — 3 Phases
=====================================================
Extracts, classifies, and re-extracts lymph node data from clinical notes
stored in MotherDuck `"Thyroid 2026".main.note_entities_llm_cervical_ln_detail`.

ALL output is labeled:
  data_source_layer = "clinical_note_extracted"
  source_note_type  = original note type (h_p, op_note, endocrine_note, etc.)
  evidence_source_modality = imaging | pathology | surgical_path | clinical | ambiguous

Phase 1: Parse clean JSONs + regex classify (~3,165 rows → 5,000+ entities, $0, ~30s)
Phase 2: LLM classify ambiguous entities (~400-500 calls, ~$0.10, ~2 min)
Phase 3: Re-extract failed rows from note text (~2,293 calls, ~$1-2, ~20 min)

Usage:
    source /tmp/md_env_setup.sh  (or set env vars manually)
    export ANTHROPIC_API_KEY="sk-ant-..."
    export MOTHERDUCK_TOKEN="eyJ..."
    pip install -r requirements.txt
    python extract_clinical_note_ln_data.py

Checkpoint: every 50 rows in Phase 3. Safe to interrupt and resume.
"""

import os
import sys
import json
import re
import time
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import anthropic
import duckdb
import pandas as pd
from anthropic.types import TextBlock
from tqdm import tqdm


def _first_assistant_text(resp: object) -> str:
    """First text block from a Messages API response (typed for mypy)."""
    content = getattr(resp, "content", None)
    if not content:
        return ""
    for block in content:
        if isinstance(block, TextBlock):
            return block.text.strip()
    return ""

# ─── Configuration ────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MOTHERDUCK_TOKEN  = os.environ.get("MOTHERDUCK_TOKEN", "")

# If token not in env, try motherduck.local.toml
if not MOTHERDUCK_TOKEN:
    _toml_candidates = [
        Path(__file__).parent.parent / "motherduck.local.toml",
        Path.home() / "motherduck.local.toml",
    ]
    for _p in _toml_candidates:
        if _p.exists():
            try:
                import toml as _toml
                _data = _toml.load(str(_p))
                MOTHERDUCK_TOKEN = (
                    _data.get("MOTHERDUCK_TOKEN")
                    or _data.get("MD_SA_TOKEN")
                    or _data.get("motherduck_token", "")
                )
                if MOTHERDUCK_TOKEN:
                    break
            except Exception:
                pass

MODEL_CLASSIFY  = "claude-haiku-4-5-20251001"   # Phase 2: classify ambiguous
MODEL_REEXTRACT = "claude-haiku-4-5-20251001"   # Phase 3: full re-extraction
MAX_WORKERS     = 2
BATCH_SIZE      = 50
MAX_RETRIES     = 3
RETRY_DELAY     = 2

SOURCE_DB    = "Thyroid 2026"
SOURCE_TABLE = "note_entities_llm_cervical_ln_detail"

OUTPUT_DIR           = Path("./output_ln")
CHECKPOINT_P3        = OUTPUT_DIR / "checkpoint_phase3.json"
OUTPUT_PARQUET       = OUTPUT_DIR / "clinical_note_ln_extracted.parquet"
OUTPUT_CSV           = OUTPUT_DIR / "clinical_note_ln_extracted.csv"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(OUTPUT_DIR / "extraction.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ─── Regex patterns for evidence_source_modality classification ───────────────

_IMAGING_PATTERNS = re.compile(
    r"\b(ultrasound|u/?s\b|sonogram|sonography|neck\s+u/?s|ct\s+scan|ct\b|pet\b|pet[-/]ct|"
    r"mri\b|mr\b|imaging|radiol|x-ray|xray|scan\b|tirads|ti-rads|acr\b|"
    r"nodule|echo|doppler|ultrasound-guided|image|radiograph)\b",
    re.I,
)
_PATHOLOGY_PATTERNS = re.compile(
    r"\b(pathol|histol|biopsy|fna\b|fnac|cytol|synoptic|specimen|excis|resect|"
    r"gross\s+exam|micro(scopic|)\s+exam|stain|immunohistochem|ihc\b|"
    r"frozen\s+section|touch\s+prep|block\b|slide\b|smear\b|"
    r"positive\s+node|negative\s+node|node\s+positive|node\s+negative|"
    r"extranodal\s+extension|extracapsular)\b",
    re.I,
)
_SURGICAL_PATH_PATTERNS = re.compile(
    r"\b(dissect|neck\s+dissect|lymph\s+node\s+dissect|central\s+neck|lateral\s+neck|"
    r"level\s+[ivxIVX]+\s+dissect|radical\s+neck|modified\s+neck|selective\s+neck|"
    r"intraop|intra-op|operative|surgical\s+path|sentinel\s+node|"
    r"thyroidect|hemithyroidect|lobectom)\b",
    re.I,
)
_CLINICAL_PATTERNS = re.compile(
    r"\b(clinic|palpat|exam|physical\s+exam|examin|lymphadenopathy|"
    r"palpable|tender|firm|mobile|fixed|lymph\s+node\s+level|"
    r"presentation|symptom|complain|history|hx\b|endocrin|follow.?up|"
    r"surveillance|monitor|recheck|reassess)\b",
    re.I,
)


def classify_modality_by_regex(evidence_text: str, date_keyword: str = "") -> str:
    """Regex-based source modality classification. Returns one of:
    imaging | pathology | surgical_path | clinical | ambiguous
    """
    text = f"{evidence_text or ''} {date_keyword or ''}".strip()
    if not text:
        return "ambiguous"

    scores = {
        "imaging":      len(_IMAGING_PATTERNS.findall(text)),
        "pathology":    len(_PATHOLOGY_PATTERNS.findall(text)),
        "surgical_path":len(_SURGICAL_PATH_PATTERNS.findall(text)),
        "clinical":     len(_CLINICAL_PATTERNS.findall(text)),
    }
    total = sum(scores.values())
    if total == 0:
        return "ambiguous"

    top = max(scores, key=lambda k: scores[k])
    top_score = scores[top]
    second = sorted(scores.values(), reverse=True)[1] if total > 1 else 0

    # Require clear winner
    if top_score == 0:
        return "ambiguous"
    if top_score > 1 and (second == 0 or top_score >= 2 * second):
        return top
    if top_score == 1 and second == 0:
        return top
    return "ambiguous"


# ─── MotherDuck connection ────────────────────────────────────────────────────

def get_conn() -> duckdb.DuckDBPyConnection:
    if not MOTHERDUCK_TOKEN:
        log.error("MOTHERDUCK_TOKEN not set. Cannot connect to MotherDuck.")
        sys.exit(1)
    conn = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")
    return conn


# ─── Anthropic client ────────────────────────────────────────────────────────

def get_client() -> anthropic.Anthropic:
    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set.")
        sys.exit(1)
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ─── Shared output schema ────────────────────────────────────────────────────

OUTPUT_COLUMNS = [
    "data_source_layer",
    "extraction_phase",
    "source_note_type",
    "note_row_id",
    "research_id",
    "note_date",
    "source_workbook",
    "source_sheet",
    "original_llm_model",
    "entity_index",
    "entity_type",
    "entity_value",
    "entity_date",
    "date_confidence",
    "date_source_keyword",
    "present_or_negated",
    "confidence",
    "evidence_text",
    "source_line",
    "evidence_source_modality",
    "modality_classification_method",
    "ln_level",
    "laterality",
    "size_cm",
    "count_positive",
    "count_total_examined",
    "ln_status",
    "extranodal_extension",
    "extraction_status",
    "extraction_error",
]


def empty_row(base: dict) -> dict:
    row = {c: None for c in OUTPUT_COLUMNS}
    row.update(base)
    return row


# ─── Phase 1: Parse clean JSONs ──────────────────────────────────────────────

def extract_ln_fields_from_entity(ent: dict) -> dict:
    """Parse structured LN fields from an entity dict."""
    # LN level
    ln_level = None
    ev = str(ent.get("entity_value") or "")
    for pat in [r"level\s*([ivxIVX]+)", r"(level\s*[ivxIVX]+)", r"(central|lateral|central\s+neck)"]:
        m = re.search(pat, ev, re.I)
        if m:
            ln_level = m.group(1).lower()
            break

    # Laterality
    lat = None
    for field in [ev, str(ent.get("evidence_text") or "")]:
        m = re.search(r"\b(right|left|bilateral|central)\b", field, re.I)
        if m:
            lat = m.group(1).lower()
            break

    # Size
    size_cm = None
    for field in [str(ent.get("evidence_text") or ""), ev]:
        m = re.search(r"(\d+(?:\.\d+)?)\s*(?:cm|mm)", field, re.I)
        if m:
            val = float(m.group(1))
            if "mm" in m.group(0).lower():
                val /= 10
            size_cm = round(val, 2)
            break

    # Count
    count_positive = count_total = None
    for field in [str(ent.get("evidence_text") or ""), ev]:
        m = re.search(r"(\d+)\s*/\s*(\d+)", field)
        if m:
            count_positive = int(m.group(1))
            count_total = int(m.group(2))
            break

    # LN status
    ln_status = None
    ev_text = str(ent.get("evidence_text") or "") + " " + ev
    if re.search(r"\b(positive|malignant|metastat|cancer)\b", ev_text, re.I):
        ln_status = "positive"
    elif re.search(r"\b(negative|benign|no\s+metastas|no\s+malignan)\b", ev_text, re.I):
        ln_status = "negative"
    elif re.search(r"\b(suspicious|suspicious-appearing|abnormal)\b", ev_text, re.I):
        ln_status = "suspicious"

    if ent.get("present_or_negated") == "negated":
        ln_status = "negative"

    # ENE
    ene = None
    if re.search(r"\b(extranodal|extracapsular|ene\b|ece\b)\b", ev_text, re.I):
        ene = True if re.search(r"\b(present|positive|yes)\b", ev_text, re.I) else None

    return {
        "ln_level": ln_level,
        "laterality": lat,
        "size_cm": size_cm,
        "count_positive": count_positive,
        "count_total_examined": count_total,
        "ln_status": ln_status,
        "extranodal_extension": ene,
    }


def run_phase1(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Parse clean JSON rows from note_entities_llm_cervical_ln_detail."""
    log.info("=== Phase 1: Parsing clean JSON rows ===")

    rows_df = conn.execute(f"""
        SELECT
            note_row_id,
            research_id,
            note_date,
            note_type,
            source_workbook,
            source_sheet,
            llm_model,
            result_json
        FROM "{SOURCE_DB}".main.{SOURCE_TABLE}
        WHERE result_json IS NOT NULL
          AND TRIM(result_json) NOT IN ('', '{{}}')
          AND TRY_CAST(result_json AS JSON) IS NOT NULL
          AND result_json NOT LIKE '%parse_error%'
          AND result_json NOT LIKE '%error%'
          AND TRIM(result_json) != '{{}}'
    """).df()

    log.info(f"  Candidate rows with JSON: {len(rows_df)}")

    records = []
    for _, row in rows_df.iterrows():
        try:
            parsed = json.loads(row["result_json"])
        except Exception:
            continue

        entities = parsed.get("entities", [])
        if not entities:
            continue

        for idx, ent in enumerate(entities):
            # Filter for LN-relevant entities only
            etype = str(ent.get("entity_type") or "").lower()
            if not any(kw in etype for kw in ["ln", "lymph", "node", "level", "neck"]):
                # Also check entity_value
                ev = str(ent.get("entity_value") or "").lower()
                if not any(kw in ev for kw in ["ln", "lymph", "node", "level", "neck"]):
                    continue

            evidence_text = str(ent.get("evidence_text") or "")
            date_keyword  = str(ent.get("date_source_keyword") or "")
            modality      = classify_modality_by_regex(evidence_text, date_keyword)

            base = {
                "data_source_layer":            "clinical_note_extracted",
                "extraction_phase":             "phase1_existing_clean",
                "source_note_type":             row.get("note_type"),
                "note_row_id":                  str(row["note_row_id"]),
                "research_id":                  str(row["research_id"]),
                "note_date":                    str(row.get("note_date") or ""),
                "source_workbook":              row.get("source_workbook"),
                "source_sheet":                 row.get("source_sheet"),
                "original_llm_model":           row.get("llm_model"),
                "entity_index":                 idx,
                "entity_type":                  ent.get("entity_type"),
                "entity_value":                 ent.get("entity_value"),
                "entity_date":                  ent.get("entity_date"),
                "date_confidence":              ent.get("date_confidence"),
                "date_source_keyword":          ent.get("date_source_keyword"),
                "present_or_negated":           ent.get("present_or_negated"),
                "confidence":                   ent.get("confidence"),
                "evidence_text":                evidence_text,
                "source_line":                  ent.get("source_line"),
                "evidence_source_modality":     modality,
                "modality_classification_method": "regex",
                "extraction_status":            "ok",
                "extraction_error":             None,
            }
            base.update(extract_ln_fields_from_entity(ent))
            records.append(empty_row(base))

    df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    log.info(f"  Phase 1 complete: {len(df)} LN entities from {len(rows_df)} clean rows")
    return df


# ─── Phase 2: LLM classify ambiguous entities ────────────────────────────────

CLASSIFY_PROMPT = """Classify the source modality for this lymph node mention in a clinical note.
Return ONLY one word: imaging | pathology | surgical_path | clinical | ambiguous

Evidence text: {evidence_text}
Date/context keyword: {date_keyword}

Rules:
- imaging: from ultrasound, CT, MRI, PET, or other radiology reports
- pathology: from pathology/histology reports, FNA, biopsy, cytology
- surgical_path: from operative notes describing neck dissection / intraoperative findings
- clinical: from physical exam, clinical history, surveillance notes
- ambiguous: cannot determine from this text alone"""


def classify_single_entity(client: anthropic.Anthropic, row: pd.Series) -> str:
    """Call Haiku to classify source modality for one ambiguous entity."""
    prompt = CLASSIFY_PROMPT.format(
        evidence_text=str(row.get("evidence_text") or "")[:500],
        date_keyword=str(row.get("date_source_keyword") or "")[:200],
    )
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.messages.create(
                model=MODEL_CLASSIFY,
                max_tokens=10,
                messages=[{"role": "user", "content": prompt}],
            )
            result = _first_assistant_text(resp).lower()
            valid = {"imaging", "pathology", "surgical_path", "clinical", "ambiguous"}
            return result if result in valid else "ambiguous"
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (2 ** attempt))
            else:
                log.warning(f"  Phase 2 classify failed: {e}")
                return "ambiguous"
    return "ambiguous"


def run_phase2(client: anthropic.Anthropic, df_p1: pd.DataFrame) -> pd.DataFrame:
    """LLM-classify entities where regex returned 'ambiguous'."""
    ambiguous_mask = df_p1["evidence_source_modality"] == "ambiguous"
    n_ambiguous = ambiguous_mask.sum()
    log.info(f"=== Phase 2: LLM classify {n_ambiguous} ambiguous entities ===")
    if n_ambiguous == 0:
        return df_p1

    df_amb = df_p1[ambiguous_mask].copy()
    results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(classify_single_entity, client, row): idx
            for idx, row in df_amb.iterrows()
        }
        with tqdm(total=n_ambiguous, desc="Phase 2 classify") as pbar:
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    results[idx] = fut.result()
                except Exception:
                    results[idx] = "ambiguous"
                pbar.update(1)

    df_out = df_p1.copy()
    for idx, modality in results.items():
        df_out.at[idx, "evidence_source_modality"]       = modality
        df_out.at[idx, "modality_classification_method"] = "llm_haiku"

    resolved = sum(1 for v in results.values() if v != "ambiguous")
    log.info(f"  Phase 2 complete: {resolved}/{n_ambiguous} resolved from ambiguous")
    return df_out


# ─── Phase 3: Re-extract failed rows ─────────────────────────────────────────

REEXTRACT_SYSTEM = """You are a clinical NLP expert extracting structured lymph node (LN) data from thyroid surgery clinical notes.
Extract ALL lymph node mentions with full detail. Return ONLY valid JSON, no prose.

CRITICAL: Mark data_source_layer="clinical_note_extracted" and set evidence_source_modality based on where the LN information came from:
- imaging: mentioned in context of ultrasound, CT, MRI, PET, or radiology reports
- pathology: from pathology/histology reports, FNA, biopsy, cytology results
- surgical_path: from operative notes, intraoperative findings, neck dissection descriptions
- clinical: from physical exam, clinical history, surveillance, or general clinical notes
- ambiguous: cannot determine from context"""

REEXTRACT_USER = """Extract all lymph node data from this clinical note excerpt. 

Return JSON with this exact structure:
{{
  "entities": [
    {{
      "entity_type": "lymph_node",
      "entity_value": "level II lymph node",
      "entity_date": "YYYY-MM-DD or null",
      "date_confidence": 0.0-1.0,
      "date_source_keyword": "text fragment that anchors the date or null",
      "present_or_negated": "present or negated",
      "confidence": 0.0-1.0,
      "evidence_text": "verbatim text snippet (max 200 chars)",
      "source_line": line_number_or_null,
      "evidence_source_modality": "imaging|pathology|surgical_path|clinical|ambiguous",
      "ln_level": "level II|level III|level IV|level V|level VI|central neck|lateral neck|null",
      "laterality": "right|left|bilateral|central|null",
      "size_cm": null_or_float,
      "count_positive": null_or_int,
      "count_total_examined": null_or_int,
      "ln_status": "positive|negative|suspicious|indeterminate|null",
      "extranodal_extension": null_or_bool
    }}
  ],
  "no_ln_found": false
}}

If no lymph node data is present, return: {{"entities": [], "no_ln_found": true}}

Clinical note:
{note_text}"""


def reextract_single_note(
    client: anthropic.Anthropic,
    note_row_id: str,
    research_id: str,
    note_text: str,
    note_meta: dict,
) -> list[dict]:
    """Re-extract LN entities from a single failed note."""
    truncated = note_text[:6000]  # match fleet script limit
    prompt = REEXTRACT_USER.format(note_text=truncated)

    for attempt in range(MAX_RETRIES):
        try:
            resp = client.messages.create(
                model=MODEL_REEXTRACT,
                max_tokens=2000,
                system=REEXTRACT_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = _first_assistant_text(resp)
            # Strip markdown fences if present
            if raw.startswith("```"):
                raw = re.sub(r"^```(?:json)?\n?", "", raw)
                raw = re.sub(r"\n?```$", "", raw)
            parsed = json.loads(raw)
        except json.JSONDecodeError as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (2 ** attempt))
                continue
            return [empty_row({
                **note_meta,
                "note_row_id": note_row_id,
                "research_id": research_id,
                "extraction_phase": "phase3_reextracted",
                "extraction_status": "json_error",
                "extraction_error": str(e)[:200],
            })]
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (2 ** attempt))
                continue
            return [empty_row({
                **note_meta,
                "note_row_id": note_row_id,
                "research_id": research_id,
                "extraction_phase": "phase3_reextracted",
                "extraction_status": "api_error",
                "extraction_error": str(e)[:200],
            })]

    entities = parsed.get("entities", [])
    no_ln    = parsed.get("no_ln_found", False)

    if no_ln or not entities:
        return [empty_row({
            **note_meta,
            "note_row_id": note_row_id,
            "research_id": research_id,
            "extraction_phase": "phase3_reextracted",
            "extraction_status": "no_ln_found",
        })]

    rows = []
    for idx, ent in enumerate(entities):
        modality = ent.get("evidence_source_modality") or classify_modality_by_regex(
            str(ent.get("evidence_text") or ""),
            str(ent.get("date_source_keyword") or ""),
        )
        row = {
            "data_source_layer":            "clinical_note_extracted",
            "extraction_phase":             "phase3_reextracted",
            "source_note_type":             note_meta.get("note_type"),
            "note_row_id":                  str(note_row_id),
            "research_id":                  str(research_id),
            "note_date":                    str(note_meta.get("note_date") or ""),
            "source_workbook":              note_meta.get("source_workbook"),
            "source_sheet":                 note_meta.get("source_sheet"),
            "original_llm_model":           MODEL_REEXTRACT,
            "entity_index":                 idx,
            "entity_type":                  ent.get("entity_type", "lymph_node"),
            "entity_value":                 ent.get("entity_value"),
            "entity_date":                  ent.get("entity_date"),
            "date_confidence":              ent.get("date_confidence"),
            "date_source_keyword":          ent.get("date_source_keyword"),
            "present_or_negated":           ent.get("present_or_negated", "present"),
            "confidence":                   ent.get("confidence"),
            "evidence_text":                str(ent.get("evidence_text") or "")[:500],
            "source_line":                  ent.get("source_line"),
            "evidence_source_modality":     modality,
            "modality_classification_method": "llm_haiku_reextract",
            "ln_level":                     ent.get("ln_level"),
            "laterality":                   ent.get("laterality"),
            "size_cm":                      ent.get("size_cm"),
            "count_positive":               ent.get("count_positive"),
            "count_total_examined":         ent.get("count_total_examined"),
            "ln_status":                    ent.get("ln_status"),
            "extranodal_extension":         ent.get("extranodal_extension"),
            "extraction_status":            "ok",
            "extraction_error":             None,
        }
        rows.append(empty_row(row))
    return rows


def load_checkpoint_p3() -> set[str]:
    """Return set of already-processed note_row_ids from Phase 3 checkpoint."""
    if CHECKPOINT_P3.exists():
        try:
            with open(CHECKPOINT_P3) as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()


def save_checkpoint_p3(processed_ids: set[str]) -> None:
    with open(CHECKPOINT_P3, "w") as f:
        json.dump(list(processed_ids), f)


def run_phase3(
    client: anthropic.Anthropic,
    conn: duckdb.DuckDBPyConnection,
    existing_df: pd.DataFrame,
) -> pd.DataFrame:
    """Re-extract failed/empty rows from original note text."""
    log.info("=== Phase 3: Re-extracting failed rows ===")

    # Get failed note_row_ids
    failed_df = conn.execute(f"""
        SELECT
            lnd.note_row_id,
            lnd.research_id,
            lnd.note_date,
            lnd.note_type,
            lnd.source_workbook,
            lnd.source_sheet,
            lnd.llm_model,
            lnd.result_json,
            cn.note_text
        FROM "{SOURCE_DB}".main.{SOURCE_TABLE} lnd
        JOIN "{SOURCE_DB}".main.clinical_notes_long cn
          ON CAST(lnd.research_id AS VARCHAR) = CAST(cn.research_id AS VARCHAR)
         AND CAST(lnd.note_index AS VARCHAR) = CAST(cn.note_index AS VARCHAR)
        WHERE (
            lnd.result_json IS NULL
            OR TRIM(lnd.result_json) = ''
            OR TRIM(lnd.result_json) = '{{}}'
            OR lnd.result_json LIKE '%parse_error%'
            OR lnd.result_json LIKE '%error%'
        )
        AND cn.note_text IS NOT NULL
        AND TRIM(cn.note_text) != ''
        QUALIFY ROW_NUMBER() OVER (PARTITION BY lnd.note_row_id ORDER BY lnd.note_row_id) = 1
    """).df()

    log.info(f"  Failed rows needing re-extraction: {len(failed_df)}")
    if len(failed_df) == 0:
        return existing_df

    # Resume from checkpoint
    done_ids = load_checkpoint_p3()
    remaining = failed_df[~failed_df["note_row_id"].astype(str).isin(done_ids)]
    log.info(f"  Already done: {len(done_ids)}, Remaining: {len(remaining)}")

    new_records: list[dict] = []
    batch_records: list[dict] = []

    def _flush_batch():
        nonlocal batch_records
        if not batch_records:
            return
        _df = pd.DataFrame(batch_records, columns=OUTPUT_COLUMNS)
        # Load any existing phase3 output and append
        p3_path = OUTPUT_DIR / "checkpoint_phase3_data.parquet"
        if p3_path.exists():
            _prev = pd.read_parquet(p3_path)
            _df = pd.concat([_prev, _df], ignore_index=True)
        _df.to_parquet(p3_path, index=False)
        new_records.extend(batch_records)
        batch_records = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures_map = {}
        for _, row in remaining.iterrows():
            meta = {
                "note_type":      row.get("note_type"),
                "note_date":      row.get("note_date"),
                "source_workbook":row.get("source_workbook"),
                "source_sheet":   row.get("source_sheet"),
            }
            fut = executor.submit(
                reextract_single_note,
                client,
                str(row["note_row_id"]),
                str(row["research_id"]),
                str(row.get("note_text") or ""),
                meta,
            )
            futures_map[fut] = str(row["note_row_id"])

        with tqdm(total=len(remaining), desc="Phase 3 re-extract") as pbar:
            for fut in as_completed(futures_map):
                row_id = futures_map[fut]
                try:
                    rows = fut.result()
                    batch_records.extend(rows)
                    done_ids.add(row_id)
                except Exception as e:
                    log.warning(f"  Exception for {row_id}: {e}")
                    done_ids.add(row_id)

                if len(done_ids) % BATCH_SIZE == 0:
                    save_checkpoint_p3(done_ids)
                    _flush_batch()

                pbar.update(1)

    _flush_batch()
    save_checkpoint_p3(done_ids)

    # Load all phase3 data
    p3_path = OUTPUT_DIR / "checkpoint_phase3_data.parquet"
    if p3_path.exists():
        df_p3 = pd.read_parquet(p3_path)
    else:
        df_p3 = pd.DataFrame(new_records, columns=OUTPUT_COLUMNS)

    log.info(f"  Phase 3 complete: {len(df_p3)} entities re-extracted from {len(remaining)} failed rows")
    return df_p3


# ─── Summary statistics ───────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"Total entities: {len(df)}")
    print(f"Unique patients: {df['research_id'].nunique()}")
    print(f"Unique notes: {df['note_row_id'].nunique()}")
    print()

    print("Phase distribution:")
    print(df["extraction_phase"].value_counts().to_string())
    print()

    print("Evidence source modality distribution:")
    print(df["evidence_source_modality"].value_counts().to_string())
    print()

    print("Note type distribution:")
    print(df["source_note_type"].value_counts().head(10).to_string())
    print()

    print("Modality classification method:")
    print(df["modality_classification_method"].value_counts().to_string())
    print()

    ok = (df["extraction_status"] == "ok").sum()
    total = len(df)
    print(f"Extraction success rate: {ok}/{total} ({100*ok/total:.1f}%)")
    print()

    print("Sample rows per modality:")
    for mod in df["evidence_source_modality"].unique():
        sub = df[df["evidence_source_modality"] == mod]
        sample = sub[["research_id", "source_note_type", "evidence_text", "ln_level", "ln_status"]].head(2)
        print(f"\n--- {mod} ({len(sub)} entities) ---")
        for _, r in sample.iterrows():
            ev = str(r.get("evidence_text") or "")[:100]
            print(f"  patient={r['research_id']} note={r['source_note_type']} level={r['ln_level']} status={r['ln_status']} | {ev}")
    print()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("Starting Clinical Note LN Extraction")
    log.info(f"  ANTHROPIC_API_KEY: {'SET (len=' + str(len(ANTHROPIC_API_KEY)) + ')' if ANTHROPIC_API_KEY else 'MISSING'}")
    log.info(f"  MOTHERDUCK_TOKEN:  {'SET (len=' + str(len(MOTHERDUCK_TOKEN)) + ')' if MOTHERDUCK_TOKEN else 'MISSING'}")

    if not ANTHROPIC_API_KEY or not MOTHERDUCK_TOKEN:
        sys.exit(1)

    conn   = get_conn()
    client = get_client()

    # ── Phase 1 ──
    df_p1 = run_phase1(conn)
    df_p1.to_parquet(OUTPUT_DIR / "phase1_clean.parquet", index=False)

    # ── Phase 2 ──
    df_p12 = run_phase2(client, df_p1)
    df_p12.to_parquet(OUTPUT_DIR / "phase12_classified.parquet", index=False)

    # ── Phase 3 ──
    df_p3 = run_phase3(client, conn, df_p12)

    # ── Merge ──
    df_all = pd.concat([df_p12, df_p3], ignore_index=True)

    # Deduplicate (note_row_id, entity_index, extraction_phase)
    df_all = df_all.drop_duplicates(subset=["note_row_id", "entity_index", "extraction_phase"])

    log.info(f"Final merged dataset: {len(df_all)} entities")
    df_all.to_parquet(OUTPUT_PARQUET, index=False)
    df_all.to_csv(OUTPUT_CSV, index=False)
    log.info(f"Saved to {OUTPUT_PARQUET} and {OUTPUT_CSV}")

    print_summary(df_all)

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
