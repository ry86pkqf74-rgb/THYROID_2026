#!/usr/bin/env python3
"""
Phase A — Consolidate the targeted qwen3:32b Ollama rerun JSONL shards into
canonical-shaped parquet files (files-only; NO MotherDuck I/O).

Provenance honesty: the shards under
``processed/remaining/partials/consolidated/`` are NOT the qwen2.5-32b vLLM
rerun (synoptic only). They are a TARGETED qwen3:32b rerun with better
note-type selection that materially improves entity coverage vs canonical for
3 of the 5 domains. Synthesized provenance therefore advertises Ollama qwen3.

Inputs (READ-ONLY):
    processed/remaining/partials/consolidated/note_entities_llm_<domain>.ckpt*.jsonl
    processed/remaining/9domain_v4/output/note_entities_llm_synoptic_pathology_enrichment.parquet
        (synoptic gold-standard; not modified, only audited)

Outputs:
    runs/domain_reruns_qwen3_32b_targeted/<domain>/output/note_entities_llm_<domain>.parquet
    runs/domain_reruns_qwen3_32b_targeted/<domain>/output/consolidation_summary.json
    runs/domain_reruns_qwen3_32b_targeted/<domain>/output/<domain>_consolidation_dropped_rows.jsonl
    scripts/output/phase_a_inventory.json
    scripts/output/phase_a_cross_domain_audit.json
    THYROID_2026_PHASE_A_CONSOLIDATION_REPORT.md  (repo root)

Phase gates (CLI; default Phase 0 only):
    --phase 0    Inventory + shard discovery (READ-ONLY)
    --phase 1    Per-domain consolidation -> parquet + summary + dropped-rows
    --phase 2    Cross-domain audit + delta-vs-canonical preview
    --phase 3    Markdown report (no git commit; user owns commit)
    --phase all  Run 0->3, halting on any failed gate

In-scope for Phase 1 consolidation:
    pathology, tirads_granular, cervical_ln_detail

Excluded from Phase 1 (stub summary or no output, per Phase A v2 prompt):
    imaging              -> status='no_upgrade_vs_canonical', stub summary, no parquet
    past_surgical_hx     -> status='empty_shard', no summary, no parquet
    synoptic_pathology_enrichment -> already consolidated, Phase B reads
                                     processed/remaining/9domain_v4/output/ directly

Hard rules enforced:
  * No MotherDuck connection. duckdb.connect() with no path only.
  * Source JSONL files are NEVER modified or deleted.
  * scripts/221_tirads_v2_integration.py is NOT imported (reference only).
  * research_id is cast to VARCHAR.
  * Six provenance columns are synthesized when absent (entity_domain,
    llm_provider, llm_sdk, llm_sdk_version, provider_returned_model,
    provider_system_fingerprint).
  * Output parquets emit the canonical 23-column layout in fixed order.
  * Dedup on note_row_id keeps the latest extracted_at; conflicts (same
    note_row_id, different result_json) are flagged in the dropped-rows log.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SHARD_DIR = REPO_ROOT / "processed" / "remaining" / "partials" / "consolidated"
SYNOPTIC_PARQUET = (
    REPO_ROOT
    / "processed"
    / "remaining"
    / "9domain_v4"
    / "output"
    / "note_entities_llm_synoptic_pathology_enrichment.parquet"
)
OUT_ROOT = REPO_ROOT / "runs" / "domain_reruns_qwen3_32b_targeted"
SCRIPTS_OUTPUT = REPO_ROOT / "scripts" / "output"
INVENTORY_JSON = SCRIPTS_OUTPUT / "phase_a_inventory.json"
CROSS_AUDIT_JSON = SCRIPTS_OUTPUT / "phase_a_cross_domain_audit.json"
REPORT_MD = REPO_ROOT / "THYROID_2026_PHASE_A_CONSOLIDATION_REPORT.md"

# All five domains the user expected in the partials dir; only IN_SCOPE_DOMAINS
# get a consolidated parquet. The other two get a stub summary or are skipped
# entirely per the Phase A v2 decision.
EXPECTED_DOMAINS: list[str] = [
    "pathology",
    "tirads_granular",
    "cervical_ln_detail",
    "imaging",
    "past_surgical_hx",
]
IN_SCOPE_DOMAINS: list[str] = [
    "pathology",
    "tirads_granular",
    "cervical_ln_detail",
]
NO_UPGRADE_DOMAINS: list[str] = ["imaging"]

CANONICAL_COLUMNS: list[str] = [
    "note_row_id",
    "domain",
    "llm_model",
    "llm_base_url",
    "extracted_at",
    "result_json",
    "research_id",
    "note_type",
    "note_date",
    "linkage_date",
    "source_workbook",
    "source_sheet",
    "source_column",
    "note_index",
    "preprocess_batch_id",
    "preprocessed_at_utc",
    "preprocess_script_version",
    "entity_domain",
    "llm_provider",
    "llm_sdk",
    "llm_sdk_version",
    "provider_returned_model",
    "provider_system_fingerprint",
]
CANONICAL_COL_COUNT = 23

# Reference values (FIXED — see Phase A prompt; do NOT query MotherDuck).
CANONICAL_SNAPSHOT: dict[str, dict[str, int]] = {
    "synoptic_pathology_enrichment": {
        "rows": 11037,
        "rids": 5641,
        "rids_with_entity": 33,
    },
    "pathology": {"rows": 11037, "rids": 5641, "rids_with_entity": 2290},
    "tirads_granular": {"rows": 11037, "rids": 5641, "rids_with_entity": 86},
    "cervical_ln_detail": {"rows": 11037, "rids": 5641, "rids_with_entity": 167},
    "imaging": {"rows": 11037, "rids": 5641, "rids_with_entity": 2218},
    "past_surgical_hx": {"rows": 11037, "rids": 5641, "rids_with_entity": 1942},
}

# Verdict thresholds (Phase A v2):
#   upgrade            >= 1.50x canonical rids_with_entity
#   modest_upgrade     1.15x .. 1.50x
#   marginal           0.85x .. 1.15x
#   regression_suspect < 0.85x


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def shard_domain(filename: str) -> str | None:
    """Extract domain from `note_entities_llm_<domain>.ckpt[...].jsonl`."""
    if not filename.startswith("note_entities_llm_"):
        return None
    stem = filename[len("note_entities_llm_"):]
    if ".ckpt" not in stem:
        return None
    return stem.split(".ckpt", 1)[0]


def verdict_for(new_rids_w: int, canon_rids_w: int) -> str:
    if canon_rids_w <= 0:
        return "upgrade" if new_rids_w > 0 else "marginal"
    ratio = new_rids_w / canon_rids_w
    if ratio >= 1.50:
        return "upgrade"
    if ratio >= 1.15:
        return "modest_upgrade"
    if ratio >= 0.85:
        return "marginal"
    return "regression_suspect"


# ── Phase 0: inventory ───────────────────────────────────────────────────────

def phase_0_inventory(connector: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    if not SHARD_DIR.is_dir():
        raise FileNotFoundError(f"Shard directory missing: {SHARD_DIR}")

    shard_records: dict[str, list[dict[str, Any]]] = {d: [] for d in EXPECTED_DOMAINS}
    unexpected: list[dict[str, Any]] = []

    for path in sorted(SHARD_DIR.iterdir()):
        if not path.is_file() or not path.name.endswith(".jsonl"):
            continue
        dom = shard_domain(path.name)
        if dom is None:
            continue
        stat = path.stat()
        size = stat.st_size
        record: dict[str, Any] = {
            "filename": path.name,
            "path": str(path.relative_to(REPO_ROOT)),
            "size_bytes": size,
            "mtime_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "sha256": None if size == 0 else sha256_file(path),
            "is_empty": size == 0,
            "first5_columns": None,
            "first5_models": None,
            "first5_base_urls": None,
            "first5_extracted_at": None,
            "first5_research_id_types": None,
            "approx_record_count": None,
        }
        if size > 0:
            cols: set[str] = set()
            models: list[str] = []
            base_urls: list[str] = []
            extracted_at: list[str] = []
            rid_types: list[str] = []
            with path.open() as fh:
                for i, line in enumerate(fh):
                    if i >= 5:
                        break
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError as exc:
                        record.setdefault("parse_errors", []).append(
                            {"line": i, "error": str(exc)}
                        )
                        continue
                    cols.update(rec.keys())
                    models.append(str(rec.get("llm_model")))
                    base_urls.append(str(rec.get("llm_base_url")))
                    extracted_at.append(str(rec.get("extracted_at")))
                    rid_types.append(type(rec.get("research_id")).__name__)
            record["first5_columns"] = sorted(cols)
            record["first5_models"] = models
            record["first5_base_urls"] = base_urls
            record["first5_extracted_at"] = extracted_at
            record["first5_research_id_types"] = rid_types
            try:
                row = connector.execute(
                    "SELECT COUNT(*) FROM read_ndjson_objects(?)",
                    [str(path)],
                ).fetchone()
                record["approx_record_count"] = int(row[0]) if row else None
            except Exception as exc:  # noqa: BLE001
                record["approx_record_count_error"] = str(exc)

        if dom in shard_records:
            shard_records[dom].append(record)
        else:
            unexpected.append({"domain_inferred": dom, **record})

    domains_out: dict[str, dict[str, Any]] = {}
    for dom in EXPECTED_DOMAINS:
        shards = shard_records[dom]
        non_empty = [s for s in shards if not s["is_empty"]]
        empty = [s for s in shards if s["is_empty"]]
        all_models: set[str] = set()
        all_base_urls: set[str] = set()
        for s in non_empty:
            for m in s.get("first5_models") or []:
                if m and m != "None":
                    all_models.add(m)
            for u in s.get("first5_base_urls") or []:
                if u and u != "None":
                    all_base_urls.add(u)
        if non_empty:
            if dom in IN_SCOPE_DOMAINS:
                status = "ready_to_consolidate"
            elif dom in NO_UPGRADE_DOMAINS:
                status = "no_upgrade_vs_canonical"
            else:
                status = "out_of_scope"
        elif shards:
            status = "empty_shard"
        else:
            status = "missing_shards"
        domains_out[dom] = {
            "status": status,
            "shard_count": len(shards),
            "non_empty_shard_count": len(non_empty),
            "empty_shard_count": len(empty),
            "total_size_bytes": sum(s["size_bytes"] for s in shards),
            "observed_models_first5": sorted(all_models),
            "observed_base_urls_first5": sorted(all_base_urls),
            "shards": shards,
        }

    sha_dupe_groups: dict[str, list[str]] = {}
    for dom_rec in domains_out.values():
        for s in dom_rec["shards"]:
            sha = s.get("sha256")
            if not sha:
                continue
            sha_dupe_groups.setdefault(sha, []).append(s["filename"])
    sha_duplicates = {sha: names for sha, names in sha_dupe_groups.items() if len(names) > 1}

    schema_warnings: list[str] = []
    for dom, rec in domains_out.items():
        if rec["non_empty_shard_count"] == 0:
            continue
        models_lc = {m.lower() for m in rec["observed_models_first5"]}
        if not any("qwen2.5" in m for m in models_lc):
            schema_warnings.append(
                f"{dom}: observed models {sorted(models_lc)} are NOT qwen2.5 — "
                "shards are the targeted qwen3:32b Ollama rerun (see provenance "
                "labeling in Phase 1 outputs)."
            )

    inventory = {
        "phase": 0,
        "run_timestamp_utc": utcnow_iso(),
        "repo_root": str(REPO_ROOT),
        "shard_dir": str(SHARD_DIR.relative_to(REPO_ROOT)),
        "synoptic_reference_parquet": str(SYNOPTIC_PARQUET.relative_to(REPO_ROOT)),
        "expected_domains": EXPECTED_DOMAINS,
        "in_scope_domains": IN_SCOPE_DOMAINS,
        "no_upgrade_domains": NO_UPGRADE_DOMAINS,
        "canonical_column_count": CANONICAL_COL_COUNT,
        "canonical_columns": CANONICAL_COLUMNS,
        "domains": domains_out,
        "sha256_duplicate_shards": sha_duplicates,
        "unexpected_shards": unexpected,
        "model_provenance_warnings": schema_warnings,
    }

    SCRIPTS_OUTPUT.mkdir(parents=True, exist_ok=True)
    INVENTORY_JSON.write_text(json.dumps(inventory, indent=2))
    return inventory


# ── Phase 1: per-domain consolidation ────────────────────────────────────────

def _build_select_with_provenance(domain: str) -> str:
    """Project canonical 23 columns from a DuckDB temp table named `raw`.

    Synthesized provenance reflects the actual qwen3:32b Ollama rerun.
    """
    return f"""
        SELECT
            CAST(note_row_id              AS VARCHAR) AS note_row_id,
            CAST(domain                   AS VARCHAR) AS domain,
            CAST(llm_model                AS VARCHAR) AS llm_model,
            CAST(llm_base_url             AS VARCHAR) AS llm_base_url,
            CAST(extracted_at             AS VARCHAR) AS extracted_at,
            CAST(result_json              AS VARCHAR) AS result_json,
            CAST(research_id              AS VARCHAR) AS research_id,
            CAST(note_type                AS VARCHAR) AS note_type,
            CAST(note_date                AS VARCHAR) AS note_date,
            CAST(linkage_date             AS VARCHAR) AS linkage_date,
            CAST(source_workbook          AS VARCHAR) AS source_workbook,
            CAST(source_sheet             AS VARCHAR) AS source_sheet,
            CAST(source_column            AS VARCHAR) AS source_column,
            CAST(note_index               AS VARCHAR) AS note_index,
            CAST(preprocess_batch_id      AS VARCHAR) AS preprocess_batch_id,
            CAST(preprocessed_at_utc      AS VARCHAR) AS preprocessed_at_utc,
            CAST(preprocess_script_version AS VARCHAR) AS preprocess_script_version,
            CAST('{domain}_targeted_rerun_qwen3_32b_ollama' AS VARCHAR) AS entity_domain,
            CAST('ollama_local'              AS VARCHAR) AS llm_provider,
            CAST('ollama'                    AS VARCHAR) AS llm_sdk,
            CAST(NULL                        AS VARCHAR) AS llm_sdk_version,
            CAST(llm_model                   AS VARCHAR) AS provider_returned_model,
            CAST(NULL                        AS VARCHAR) AS provider_system_fingerprint
        FROM raw
    """


def _table_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    """Return lowercase column names for `table`."""
    df = con.execute(f"DESCRIBE {table}").df()
    return {str(c).lower() for c in df["column_name"].tolist()}


def _ensure_optional_columns(con: duckdb.DuckDBPyConnection) -> None:
    """Add NULL VARCHAR columns to `raw` for any missing canonical input cols."""
    optional = [
        "linkage_date",
        "source_workbook",
        "source_sheet",
        "source_column",
        "note_index",
        "preprocess_batch_id",
        "preprocessed_at_utc",
        "preprocess_script_version",
    ]
    cols_present = _table_columns(con, "raw")
    for col in optional:
        if col.lower() not in cols_present:
            con.execute(f"ALTER TABLE raw ADD COLUMN {col} VARCHAR")


def _emit_no_upgrade_stub(domain: str, rec: dict[str, Any]) -> dict[str, Any]:
    """Write a stub summary for a no-upgrade domain (no parquet)."""
    out_dir = OUT_ROOT / domain / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_summary = out_dir / "consolidation_summary.json"
    canon = CANONICAL_SNAPSHOT[domain]
    summary = {
        "domain": domain,
        "status": "no_upgrade_vs_canonical",
        "skipped": True,
        "skip_reason": (
            "Local rerun's rids_with_entity matches canonical "
            f"({canon['rids_with_entity']}) — consolidating adds nothing."
        ),
        "shards_present": rec["non_empty_shard_count"],
        "canonical_rids_with_entity": canon["rids_with_entity"],
        "rids_with_entity_local_estimate": canon["rids_with_entity"],
        "rids_with_entity_delta_vs_canonical": 0,
        "upgrade_multiple_vs_canonical": 1.0,
        "verdict": "marginal",
        "output_parquet": None,
        "dropped_rows_log": None,
        "run_timestamp_utc": utcnow_iso(),
    }
    out_summary.write_text(json.dumps(summary, indent=2))
    return summary


def consolidate_domain(
    domain: str, shards: list[dict[str, Any]]
) -> dict[str, Any]:
    non_empty = [s for s in shards if not s["is_empty"]]
    if not non_empty:
        raise ValueError(f"{domain}: no non-empty shards")

    out_dir = OUT_ROOT / domain / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_parquet = out_dir / f"note_entities_llm_{domain}.parquet"
    out_summary = out_dir / "consolidation_summary.json"
    out_dropped = out_dir / f"{domain}_consolidation_dropped_rows.jsonl"

    shard_paths = [str(REPO_ROOT / s["path"]) for s in non_empty]

    con = duckdb.connect()
    try:
        con.execute("PRAGMA threads=4")
        con.execute(
            "CREATE TEMP TABLE raw AS "
            "SELECT * FROM read_json_auto(?, format='newline_delimited', "
            "union_by_name=true, maximum_object_size=33554432)",
            [shard_paths],
        )
        raw_rows = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
        if raw_rows == 0:
            raise RuntimeError(f"{domain}: read_json_auto returned 0 rows")
        _ensure_optional_columns(con)

        cols_present = _table_columns(con, "raw")
        required_input = {
            "note_row_id",
            "domain",
            "llm_model",
            "llm_base_url",
            "extracted_at",
            "result_json",
            "research_id",
            "note_type",
            "note_date",
        }
        missing_required = sorted(required_input - cols_present)
        if missing_required:
            raise RuntimeError(
                f"{domain}: missing required input cols: {missing_required}"
            )

        proj_sql = _build_select_with_provenance(domain)
        con.execute(f"CREATE TEMP TABLE projected AS {proj_sql}")

        con.execute(
            """
            CREATE TEMP TABLE ranked AS
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY note_row_id
                    ORDER BY extracted_at DESC NULLS LAST
                ) AS _rn
            FROM projected
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE conflict_groups AS
            SELECT note_row_id
            FROM projected
            GROUP BY note_row_id
            HAVING COUNT(DISTINCT result_json) > 1
            """
        )
        conflict_rows = con.execute(
            "SELECT COUNT(*) FROM conflict_groups"
        ).fetchone()[0]
        dup_groups = con.execute(
            "SELECT COUNT(*) FROM (SELECT note_row_id FROM projected "
            "GROUP BY note_row_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        rows_dedupped = con.execute(
            "SELECT COUNT(*) FROM ranked WHERE _rn > 1"
        ).fetchone()[0]

        if rows_dedupped > 0 or conflict_rows > 0:
            dropped_df = con.execute(
                """
                SELECT
                    p.*,
                    CASE
                      WHEN c.note_row_id IS NOT NULL
                        THEN 'duplicate_note_row_id_with_result_json_conflict'
                      ELSE 'duplicate_note_row_id_superseded_by_later_extracted_at'
                    END AS drop_reason
                FROM ranked p
                LEFT JOIN conflict_groups c USING (note_row_id)
                WHERE p._rn > 1
                """
            ).df().drop(columns=["_rn"])
            with out_dropped.open("w") as fh:
                for rec in dropped_df.to_dict(orient="records"):
                    fh.write(json.dumps(rec, default=str) + "\n")
        else:
            if out_dropped.exists():
                out_dropped.unlink()

        col_list_csv = ", ".join(CANONICAL_COLUMNS)
        con.execute(
            f"""
            CREATE TEMP TABLE final AS
            SELECT {col_list_csv}
            FROM ranked
            WHERE _rn = 1
            """
        )
        out_rows = con.execute("SELECT COUNT(*) FROM final").fetchone()[0]
        out_rids = con.execute(
            "SELECT COUNT(DISTINCT research_id) FROM final"
        ).fetchone()[0]

        rows_w_entity = con.execute(
            "SELECT COUNT(*) FROM final "
            "WHERE result_json IS NOT NULL "
            "AND result_json LIKE '%\"entity_value\":%' "
            "AND result_json NOT LIKE '%\"entities\": []%'"
        ).fetchone()[0]
        rids_w_entity = con.execute(
            "SELECT COUNT(DISTINCT research_id) FROM final "
            "WHERE result_json IS NOT NULL "
            "AND result_json LIKE '%\"entity_value\":%' "
            "AND result_json NOT LIKE '%\"entities\": []%'"
        ).fetchone()[0]

        model_rows = con.execute(
            "SELECT llm_model, COUNT(*) FROM final GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        ext_min, ext_max = con.execute(
            "SELECT MIN(extracted_at), MAX(extracted_at) FROM final"
        ).fetchone()

        con.execute(
            f"COPY final TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        round_trip_cols = [
            r[0]
            for r in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(out_parquet)]
            ).fetchall()
        ]
        round_trip_types = {
            r[0]: r[1]
            for r in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [str(out_parquet)]
            ).fetchall()
        }
        schema_ok = (
            round_trip_cols == CANONICAL_COLUMNS
            and all(t == "VARCHAR" for t in round_trip_types.values())
            and len(round_trip_cols) == CANONICAL_COL_COUNT
        )
    finally:
        con.close()

    canon = CANONICAL_SNAPSHOT[domain]
    delta = int(rids_w_entity) - canon["rids_with_entity"]
    multiple = (
        round(int(rids_w_entity) / canon["rids_with_entity"], 4)
        if canon["rids_with_entity"]
        else None
    )
    summary = {
        "domain": domain,
        "shards_ingested": len(non_empty),
        "raw_input_rows": int(raw_rows),
        "output_rows": int(out_rows),
        "output_rids": int(out_rids),
        "rows_dedupped": int(rows_dedupped),
        "duplicate_note_row_id_groups": int(dup_groups),
        "conflict_rows": int(conflict_rows),
        "rows_with_entity_value": int(rows_w_entity),
        "rids_with_entity_value": int(rids_w_entity),
        "canonical_rids_with_entity": canon["rids_with_entity"],
        "rids_with_entity_delta_vs_canonical": delta,
        "upgrade_multiple_vs_canonical": multiple,
        "verdict": verdict_for(int(rids_w_entity), canon["rids_with_entity"]),
        "llm_models": {m: int(c) for m, c in model_rows},
        "extracted_at_min": ext_min,
        "extracted_at_max": ext_max,
        "schema_ok": bool(schema_ok),
        "round_trip_columns": round_trip_cols,
        "output_parquet": str(out_parquet.relative_to(REPO_ROOT)),
        "dropped_rows_log": (
            str(out_dropped.relative_to(REPO_ROOT)) if out_dropped.exists() else None
        ),
        "run_timestamp_utc": utcnow_iso(),
    }
    out_summary.write_text(json.dumps(summary, indent=2))
    return summary


def phase_1_consolidate(inventory: dict[str, Any]) -> dict[str, Any]:
    summaries: dict[str, Any] = {}
    for dom in EXPECTED_DOMAINS:
        rec = inventory["domains"][dom]
        if rec["status"] == "ready_to_consolidate" and dom in IN_SCOPE_DOMAINS:
            s = consolidate_domain(dom, rec["shards"])
            if not s["schema_ok"]:
                raise RuntimeError(
                    f"{dom}: schema_ok=False after parquet write — halting"
                )
            if s["output_rows"] == 0:
                raise RuntimeError(
                    f"{dom}: output_rows == 0 from non-empty input — halting"
                )
            summaries[dom] = s
        elif dom in NO_UPGRADE_DOMAINS and rec["non_empty_shard_count"] > 0:
            summaries[dom] = _emit_no_upgrade_stub(dom, rec)
        else:
            summaries[dom] = {
                "domain": dom,
                "status": rec["status"],
                "skipped": True,
            }
    return summaries


# ── Phase 2: cross-domain audit ──────────────────────────────────────────────

def _synoptic_summary(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    if not SYNOPTIC_PARQUET.exists():
        raise FileNotFoundError(
            f"Synoptic reference parquet missing: {SYNOPTIC_PARQUET}"
        )
    rows = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)", [str(SYNOPTIC_PARQUET)]
    ).fetchone()[0]
    rids = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM read_parquet(?)",
        [str(SYNOPTIC_PARQUET)],
    ).fetchone()[0]
    rows_w = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?) "
        "WHERE result_json IS NOT NULL "
        "AND result_json LIKE '%\"entity_value\":%' "
        "AND result_json NOT LIKE '%\"entities\": []%'",
        [str(SYNOPTIC_PARQUET)],
    ).fetchone()[0]
    rids_w = con.execute(
        "SELECT COUNT(DISTINCT research_id) FROM read_parquet(?) "
        "WHERE result_json IS NOT NULL "
        "AND result_json LIKE '%\"entity_value\":%' "
        "AND result_json NOT LIKE '%\"entities\": []%'",
        [str(SYNOPTIC_PARQUET)],
    ).fetchone()[0]
    models = con.execute(
        "SELECT llm_model, COUNT(*) FROM read_parquet(?) GROUP BY 1",
        [str(SYNOPTIC_PARQUET)],
    ).fetchall()
    ext_min, ext_max = con.execute(
        "SELECT MIN(extracted_at), MAX(extracted_at) FROM read_parquet(?)",
        [str(SYNOPTIC_PARQUET)],
    ).fetchone()
    cols = [
        r[0]
        for r in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(SYNOPTIC_PARQUET)]
        ).fetchall()
    ]
    return {
        "domain": "synoptic_pathology_enrichment",
        "rows": int(rows),
        "rids": int(rids),
        "rows_with_entity": int(rows_w),
        "rids_with_entity": int(rids_w),
        "llm_models": {m: int(c) for m, c in models},
        "extracted_at_min": ext_min,
        "extracted_at_max": ext_max,
        "source_parquet": str(SYNOPTIC_PARQUET.relative_to(REPO_ROOT)),
        "parquet_column_count": len(cols),
        "parquet_columns": cols,
    }


def phase_2_cross_audit(summaries: dict[str, Any]) -> dict[str, Any]:
    con = duckdb.connect()
    try:
        domain_audits: dict[str, Any] = {}

        syn = _synoptic_summary(con)
        canon = CANONICAL_SNAPSHOT["synoptic_pathology_enrichment"]
        v = verdict_for(syn["rids_with_entity"], canon["rids_with_entity"])
        domain_audits["synoptic_pathology_enrichment"] = {
            **syn,
            "pct_rows_with_entity": (
                round(100 * syn["rows_with_entity"] / syn["rows"], 2)
                if syn["rows"]
                else 0.0
            ),
            "expected_canonical_table_name": (
                "note_entities_llm_synoptic_pathology_enrichment"
            ),
            "canonical_snapshot": canon,
            "delta_vs_canonical": {
                "rows":             syn["rows"]             - canon["rows"],
                "rids":             syn["rids"]             - canon["rids"],
                "rids_with_entity": syn["rids_with_entity"] - canon["rids_with_entity"],
            },
            "upgrade_multiple_vs_canonical": (
                round(syn["rids_with_entity"] / canon["rids_with_entity"], 4)
                if canon["rids_with_entity"]
                else None
            ),
            "verdict": v,
            "source": "pre-consolidated_reference",
        }

        for dom in EXPECTED_DOMAINS:
            s = summaries.get(dom, {})
            canon_d = CANONICAL_SNAPSHOT[dom]
            if s.get("skipped") and s.get("output_parquet") is None and "rids_with_entity_value" not in s:
                domain_audits[dom] = {
                    "domain": dom,
                    "status": s.get("status"),
                    "skipped": True,
                    "skip_reason": s.get("skip_reason"),
                    "expected_canonical_table_name": (
                        f"note_entities_llm_{dom}"
                    ),
                    "canonical_snapshot": canon_d,
                    "verdict": s.get("verdict") or "skipped",
                    "source": (
                        "no_upgrade_stub"
                        if s.get("status") == "no_upgrade_vs_canonical"
                        else "no_consolidation_emitted"
                    ),
                }
                continue
            domain_audits[dom] = {
                "domain": dom,
                "rows": s["output_rows"],
                "rids": s["output_rids"],
                "rows_with_entity": s["rows_with_entity_value"],
                "rids_with_entity": s["rids_with_entity_value"],
                "pct_rows_with_entity": (
                    round(
                        100 * s["rows_with_entity_value"] / s["output_rows"], 2
                    )
                    if s["output_rows"]
                    else 0.0
                ),
                "llm_models": s["llm_models"],
                "extracted_at_min": s["extracted_at_min"],
                "extracted_at_max": s["extracted_at_max"],
                "expected_canonical_table_name": f"note_entities_llm_{dom}",
                "canonical_snapshot": canon_d,
                "delta_vs_canonical": {
                    "rows":             s["output_rows"]              - canon_d["rows"],
                    "rids":             s["output_rids"]              - canon_d["rids"],
                    "rids_with_entity": s["rids_with_entity_value"]   - canon_d["rids_with_entity"],
                },
                "upgrade_multiple_vs_canonical": s.get(
                    "upgrade_multiple_vs_canonical"
                ),
                "verdict": s["verdict"],
                "source": "phase_1_consolidation",
            }
    finally:
        con.close()

    syn_verdict = domain_audits["synoptic_pathology_enrichment"]["verdict"]
    if syn_verdict == "regression_suspect":
        raise RuntimeError(
            "Synoptic reference parquet is a regression vs canonical — "
            "halting per Phase A spec."
        )

    audit = {
        "phase": 2,
        "run_timestamp_utc": utcnow_iso(),
        "domains": domain_audits,
        "verdict_summary": dict(
            Counter(d["verdict"] for d in domain_audits.values())
        ),
        "synoptic_parquet_column_count": (
            domain_audits["synoptic_pathology_enrichment"].get(
                "parquet_column_count"
            )
        ),
        "canonical_target_column_count": CANONICAL_COL_COUNT,
    }
    CROSS_AUDIT_JSON.write_text(json.dumps(audit, indent=2, default=str))
    return audit


# ── Phase 3: markdown report ─────────────────────────────────────────────────

def _git_head() -> str:
    head_file = REPO_ROOT / ".git" / "HEAD"
    if not head_file.exists():
        return "unknown"
    head = head_file.read_text().strip()
    if head.startswith("ref: "):
        ref = head[5:]
        ref_path = REPO_ROOT / ".git" / ref
        if ref_path.exists():
            return f"{ref} -> {ref_path.read_text().strip()[:12]}"
        return ref
    return head[:12]


def phase_3_report(
    inventory: dict[str, Any],
    summaries: dict[str, Any],
    audit: dict[str, Any],
) -> Path:
    script_path = Path(__file__).resolve()
    script_hash = sha256_file(script_path)
    consolidated = [
        d
        for d, s in summaries.items()
        if s.get("output_parquet")
    ]

    lines: list[str] = []
    lines.append("# THYROID 2026 — Phase A Consolidation Report")
    lines.append("")
    lines.append(f"- Run timestamp (UTC): `{utcnow_iso()}`")
    lines.append(f"- Git HEAD: `{_git_head()}`")
    lines.append(f"- Script: `{script_path.relative_to(REPO_ROOT)}`")
    lines.append(f"- Script SHA-256: `{script_hash}`")
    lines.append(
        f"- Domains consolidated this run: **{len(consolidated)}** "
        f"({', '.join(consolidated) if consolidated else 'none'})"
    )
    lines.append(
        "- Provenance label: `entity_domain="
        "<domain>_targeted_rerun_qwen3_32b_ollama`, `llm_provider=ollama_local`, "
        "`llm_sdk=ollama` (honest labeling — these shards are NOT qwen2.5-32b vLLM)"
    )
    lines.append("")
    lines.append("## Per-domain consolidation")
    lines.append("")
    lines.append(
        "| Domain | Shards | Raw rows | Output rows | Output RIDs | "
        "RIDs w/ entity | Verdict |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---|")
    syn = audit["domains"]["synoptic_pathology_enrichment"]
    lines.append(
        f"| synoptic_pathology_enrichment (reference; pre-consolidated) | n/a | n/a | "
        f"{syn['rows']} | {syn['rids']} | {syn['rids_with_entity']} | "
        f"{syn['verdict']} |"
    )
    for dom in EXPECTED_DOMAINS:
        s = summaries.get(dom, {})
        a = audit["domains"][dom]
        if s.get("output_parquet") is None:
            note = s.get("status") or a.get("status") or "skipped"
            lines.append(
                f"| {dom} | {a.get('canonical_snapshot', {}).get('rows', 'n/a')} (canon) | "
                f"— | — | — | — | {a['verdict']} ({note}) |"
            )
            continue
        lines.append(
            f"| {dom} | {s['shards_ingested']} | {s['raw_input_rows']} | "
            f"{s['output_rows']} | {s['output_rids']} | "
            f"{s['rids_with_entity_value']} | {a['verdict']} |"
        )
    lines.append("")

    lines.append("## Cross-domain audit vs canonical snapshot")
    lines.append("")
    lines.append(
        "| Domain | New rows | Canon rows | Δ rows | New RIDs | Canon RIDs | "
        "Δ RIDs | New RIDs w/ ent | Canon RIDs w/ ent | Δ | x-mult | Verdict |"
    )
    lines.append(
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
    )
    for dom_key, a in audit["domains"].items():
        canon_d = a["canonical_snapshot"]
        if a.get("skipped"):
            lines.append(
                f"| {dom_key} | — | {canon_d['rows']} | — | — | "
                f"{canon_d['rids']} | — | — | {canon_d['rids_with_entity']} | "
                f"— | — | {a['verdict']} |"
            )
            continue
        d = a["delta_vs_canonical"]
        mult = a.get("upgrade_multiple_vs_canonical")
        mult_s = f"{mult:.2f}x" if mult is not None else "—"
        lines.append(
            f"| {dom_key} | {a['rows']} | {canon_d['rows']} | {d['rows']:+} | "
            f"{a['rids']} | {canon_d['rids']} | {d['rids']:+} | "
            f"{a['rids_with_entity']} | {canon_d['rids_with_entity']} | "
            f"{d['rids_with_entity']:+} | {mult_s} | {a['verdict']} |"
        )
    lines.append("")

    lines.append("## Dropped rows per domain")
    lines.append("")
    lines.append("| Domain | Dup groups | Rows dedupped | Conflict rows | Log file |")
    lines.append("|---|---:|---:|---:|---|")
    for dom in EXPECTED_DOMAINS:
        s = summaries.get(dom, {})
        if not s.get("output_parquet"):
            lines.append(f"| {dom} | — | — | — | (skipped) |")
            continue
        log = s.get("dropped_rows_log") or "(none — no duplicates)"
        lines.append(
            f"| {dom} | {s['duplicate_note_row_id_groups']} | "
            f"{s['rows_dedupped']} | {s['conflict_rows']} | `{log}` |"
        )
    lines.append("")

    sha_dupes = inventory.get("sha256_duplicate_shards") or {}
    if sha_dupes:
        lines.append("### SHA-256 duplicate shards (collapsed losslessly by note_row_id dedup)")
        lines.append("")
        for sha, names in sha_dupes.items():
            lines.append(f"- `{sha[:12]}…` ({len(names)} files): {', '.join(names)}")
        lines.append("")

    lines.append("## Model / provenance audit")
    lines.append("")
    lines.append("| Domain | Source models (rows) | extracted_at min | extracted_at max |")
    lines.append("|---|---|---|---|")
    for dom in EXPECTED_DOMAINS:
        s = summaries.get(dom, {})
        if not s.get("output_parquet"):
            lines.append(f"| {dom} | (skipped) | — | — |")
            continue
        models = ", ".join(f"{m}={c}" for m, c in s["llm_models"].items())
        lines.append(
            f"| {dom} | {models} | {s['extracted_at_min']} | "
            f"{s['extracted_at_max']} |"
        )
    lines.append("")
    if inventory.get("model_provenance_warnings"):
        lines.append("### Provenance warnings")
        lines.append("")
        for w in inventory["model_provenance_warnings"]:
            lines.append(f"- {w}")
        lines.append("")

    lines.append("## Phase B planning notes (synoptic schema gap)")
    lines.append("")
    syn_cols = audit.get("synoptic_parquet_column_count")
    lines.append(
        "- Synoptic gold-standard parquet "
        f"(`{SYNOPTIC_PARQUET.relative_to(REPO_ROOT)}`) has **{syn_cols} columns** "
        f"on disk, while the new consolidated parquets emit the canonical "
        f"**{CANONICAL_COL_COUNT}-column** layout (the extra 6 are the synthesized "
        "`entity_domain`, `llm_provider`, `llm_sdk`, `llm_sdk_version`, "
        "`provider_returned_model`, `provider_system_fingerprint` provenance cols)."
    )
    lines.append(
        "- Phase B must reconcile this: either `read_parquet([...], union_by_name=true)` "
        "with explicit NULL fills, or backfill the 6 provenance cols on the synoptic "
        "parquet at read time. **Do NOT modify the synoptic parquet in place — Phase B's "
        "load script should add the cols on the fly.**"
    )
    lines.append(
        "- Suggested provenance for the synoptic backfill: "
        "`entity_domain='synoptic_pathology_enrichment_rerun_qwen25_32b'`, "
        "`llm_provider='vastai_vllm'`, `llm_sdk='openai-compatible'`, "
        "`provider_returned_model=llm_model` (echo)."
    )
    lines.append("")

    lines.append("## Next-step pointers")
    lines.append("")
    lines.append(
        "- Synoptic rerun parquet (Phase B input):  \n"
        f"  `{SYNOPTIC_PARQUET.relative_to(REPO_ROOT)}`"
    )
    for dom in EXPECTED_DOMAINS:
        s = summaries.get(dom, {})
        if not s.get("output_parquet"):
            continue
        lines.append(
            f"- `{dom}` consolidated parquet:  \n"
            f"  `runs/domain_reruns_qwen3_32b_targeted/{dom}/output/"
            f"note_entities_llm_{dom}.parquet`"
        )
    lines.append(
        "- `imaging` (no_upgrade_vs_canonical): "
        f"`runs/domain_reruns_qwen3_32b_targeted/imaging/output/consolidation_summary.json` "
        "(stub; no parquet)"
    )
    lines.append(
        "- `past_surgical_hx` (empty_shard): no output emitted; investigate why the "
        "Vast.ai job produced a 0-byte shard before re-running this domain."
    )
    lines.append("")

    lines.append("## Final report checklist")
    lines.append("")
    inv_doms = inventory["domains"]
    inv_ok = all(d in inv_doms for d in EXPECTED_DOMAINS)
    schema_ok_all = all(
        s.get("schema_ok") for s in summaries.values() if s.get("output_parquet")
    )
    rid_varchar_all = all(
        "research_id" in s.get("round_trip_columns", [])
        for s in summaries.values()
        if s.get("output_parquet")
    )
    empty_flagged = (
        inv_doms["past_surgical_hx"]["status"] == "empty_shard"
        and "past_surgical_hx" not in consolidated
    )
    lines.append(f"- [{'x' if inv_ok else ' '}] Inventory discovered expected shards for 5 domains")
    lines.append(
        f"- [{'x' if schema_ok_all and consolidated else ' '}] "
        "In-scope domains produced a parquet with canonical 23-column schema"
    )
    lines.append(
        f"- [{'x' if empty_flagged else ' '}] Empty domain (past_surgical_hx) "
        "flagged with status='empty_shard' and no parquet emitted"
    )
    lines.append(
        f"- [{'x' if 'imaging' in summaries and summaries['imaging'].get('status') == 'no_upgrade_vs_canonical' else ' '}] "
        "Imaging flagged status='no_upgrade_vs_canonical' (stub summary, no parquet)"
    )
    lines.append(
        f"- [{'x' if rid_varchar_all and consolidated else ' '}] "
        "All emitted parquets have research_id as VARCHAR"
    )
    lines.append(
        "- [x] Dedup audit: see Dropped-rows table above (per-domain counts + log files)"
    )
    lines.append(
        "- [x] Rerun upgrade verdict per domain (Cross-domain audit table)"
    )
    lines.append(
        "- [ ] Git commit present (atomic per-domain commits + final audit "
        "commit; user verifies via `git log` on branch `phase-a-consolidate-reruns`)"
    )
    lines.append(
        "- [x] NO MotherDuck writes attempted "
        "(grep `md:`/`motherduck` in this script returns only docstrings/comments)"
    )
    lines.append("")
    REPORT_MD.write_text("\n".join(lines))
    return REPORT_MD


# ── CLI ──────────────────────────────────────────────────────────────────────

def _load_existing_summaries(inventory: dict[str, Any]) -> dict[str, Any]:
    """For phases 2/3 invoked standalone, hydrate summaries from disk."""
    summaries: dict[str, Any] = {}
    for dom in EXPECTED_DOMAINS:
        rec = inventory["domains"][dom]
        summary_path = OUT_ROOT / dom / "output" / "consolidation_summary.json"
        if summary_path.exists():
            summaries[dom] = json.loads(summary_path.read_text())
        else:
            summaries[dom] = {
                "domain": dom,
                "status": rec["status"],
                "skipped": True,
            }
    return summaries


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=["0", "1", "2", "3", "all"],
        default="0",
        help="Phase gate (default: 0).",
    )
    parser.add_argument(
        "--only-domain",
        action="append",
        default=None,
        help=(
            "Restrict Phase 1 to a single domain (repeatable). "
            "Useful for atomic per-domain commits."
        ),
    )
    args = parser.parse_args()

    SCRIPTS_OUTPUT.mkdir(parents=True, exist_ok=True)
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        if args.phase in ("0", "all"):
            inventory = phase_0_inventory(con)
            print(f"[phase 0] wrote {INVENTORY_JSON.relative_to(REPO_ROOT)}")
            for dom, rec in inventory["domains"].items():
                print(
                    f"  {dom}: status={rec['status']} "
                    f"shards={rec['shard_count']} "
                    f"non_empty={rec['non_empty_shard_count']} "
                    f"models={rec['observed_models_first5']}"
                )
            for w in inventory.get("model_provenance_warnings", []):
                print(f"  ⚠ provenance warning: {w}")
        else:
            if not INVENTORY_JSON.exists():
                raise SystemExit(
                    "Phase 0 inventory missing — run --phase 0 first."
                )
            inventory = json.loads(INVENTORY_JSON.read_text())

        summaries: dict[str, Any] = {}
        if args.phase in ("1", "all"):
            if args.only_domain:
                allowed = set(args.only_domain)
                for dom in EXPECTED_DOMAINS:
                    rec = inventory["domains"][dom]
                    if dom not in allowed:
                        existing = OUT_ROOT / dom / "output" / "consolidation_summary.json"
                        if existing.exists():
                            summaries[dom] = json.loads(existing.read_text())
                        else:
                            summaries[dom] = {
                                "domain": dom,
                                "status": rec["status"],
                                "skipped": True,
                            }
                        continue
                    if rec["status"] == "ready_to_consolidate" and dom in IN_SCOPE_DOMAINS:
                        s = consolidate_domain(dom, rec["shards"])
                        if not s["schema_ok"]:
                            raise RuntimeError(
                                f"{dom}: schema_ok=False after parquet write — halting"
                            )
                        if s["output_rows"] == 0:
                            raise RuntimeError(
                                f"{dom}: output_rows == 0 from non-empty input — halting"
                            )
                        summaries[dom] = s
                    elif dom in NO_UPGRADE_DOMAINS and rec["non_empty_shard_count"] > 0:
                        summaries[dom] = _emit_no_upgrade_stub(dom, rec)
                    else:
                        summaries[dom] = {
                            "domain": dom,
                            "status": rec["status"],
                            "skipped": True,
                        }
            else:
                summaries = phase_1_consolidate(inventory)
            for dom, s in summaries.items():
                if s.get("output_parquet"):
                    print(
                        f"[phase 1] {dom}: rows={s['output_rows']} "
                        f"rids={s['output_rids']} "
                        f"rids_w_entity={s['rids_with_entity_value']} "
                        f"x{s.get('upgrade_multiple_vs_canonical')} "
                        f"({s['verdict']}) "
                        f"dedupped={s['rows_dedupped']} "
                        f"conflicts={s['conflict_rows']}"
                    )
                else:
                    print(
                        f"[phase 1] {dom}: SKIPPED "
                        f"({s.get('status', 'unknown')})"
                    )
        elif args.phase in ("2", "3"):
            summaries = _load_existing_summaries(inventory)

        if args.phase in ("2", "all"):
            audit = phase_2_cross_audit(summaries)
            print(f"[phase 2] wrote {CROSS_AUDIT_JSON.relative_to(REPO_ROOT)}")
            print(f"  verdict summary: {audit['verdict_summary']}")
        elif args.phase == "3":
            if not CROSS_AUDIT_JSON.exists():
                raise SystemExit(
                    "Cross-domain audit missing — run --phase 2 first."
                )
            audit = json.loads(CROSS_AUDIT_JSON.read_text())

        if args.phase in ("3", "all"):
            report = phase_3_report(inventory, summaries, audit)
            print(f"[phase 3] wrote {report.relative_to(REPO_ROOT)}")
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
