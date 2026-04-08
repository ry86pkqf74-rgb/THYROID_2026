#!/usr/bin/env python3
"""
42_ingest_afirma.py — Afirma structured file → normalized molecular layer.

Accepts CSV, XLSX, or JSON (array of row objects or {\"rows\": [...]})
and maps into ``molecular_results`` + ``molecular_variant_long``, with
staging tables and review queue (same pattern as ``41_ingest_thyroseq_excel``).

Usage:
  .venv/bin/python scripts/42_ingest_afirma.py --input /path/to/afirma_export.csv [--md] [--local] [--dry-run]

Crosswalks (exact ``source_code`` match in ``molecular_code_crosswalk``):
  afirma_call, afirma_risk_call, afirma_assay_key, variant_class

Assay metadata: ``molecular_assay_dictionary`` by ``assay_key`` (seeded in 131 DDL).

MotherDuck: use ``--md``; token via motherduck_client / .streamlit/secrets.toml.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
import uuid
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))

from utils.molecular_ingest_common import (
    GOVERNED_MOLECULAR_RESULT_COLUMNS,
    GOVERNED_MOLECULAR_VARIANT_LONG_COLUMNS,
    checksum_sorted_json_payload,
    json_friendly_scalar,
    molecular_result_id_from_parts,
    molecular_variant_id_afirma,
    stamp_afirma_ingestion_metadata,
)
from utils.afirma_helpers import (
    canonicalize_afirma_columns,
    compute_afirma_row_hash,
    effective_assay_dictionary,
    effective_crosswalk_maps,
    expand_xpression_variants,
    harmonize_calls,
    parse_test_date,
    resolve_afirma_assay_key,
    risk_call_from_gec_gsc,
)
from utils.thyroseq_helpers import normalize_dob, normalize_mrn, normalize_name

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

BATCH_ID = str(uuid.uuid4())[:12]
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")

MOLECULAR_SOURCE_TABLE = "42_afirma_structured_file"
AFIRMA_PLATFORM = "Afirma"
AFIRMA_VENDOR = "Veracyte"

_MR_COLS = list(GOVERNED_MOLECULAR_RESULT_COLUMNS)
_MVL_COLS = list(GOVERNED_MOLECULAR_VARIANT_LONG_COLUMNS)


def _load_thyroseq_module():
    p = ROOT / "scripts" / "41_ingest_thyroseq_excel.py"
    spec = importlib.util.spec_from_file_location("ingest_thyroseq_excel", p)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def connect(
    use_md: bool = False,
    use_local: bool = False,
    md_env: str | None = None,
) -> duckdb.DuckDBPyConnection:
    import os as _os

    if use_local or _os.environ.get("USE_LOCAL_DUCKDB"):
        path = _os.environ.get("LOCAL_DUCKDB_PATH", str(ROOT / "thyroid_master_local.duckdb"))
        return duckdb.connect(path)
    from utils.md_connect import connect_md_or_file
    from utils.md_pipeline_attribution import connect_attribution

    ua, hint = connect_attribution(component="42_ingest_afirma", run_kind="ingest")

    return connect_md_or_file(
        DB_PATH,
        md=use_md,
        fail_closed=False,
        env=md_env,
        custom_user_agent=ua,
        motherduck_session_hint=hint,
    )


def load_source_table(path: Path) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf == ".json":
        obj = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        if isinstance(obj, dict) and "rows" in obj:
            return pd.DataFrame(obj["rows"])
        raise ValueError("JSON must be a list of rows or {\"rows\": [...]}")
    if suf in (".xlsx", ".xls"):
        return pd.read_excel(path, sheet_name=0)
    if suf == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported format: {suf}")


def ingest_afirma_frame(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    df = canonicalize_afirma_columns(df.copy())
    stamp_afirma_ingestion_metadata(
        df,
        source_file=source_name,
        batch_id=BATCH_ID,
        row_number_start=2,
    )

    if "patient_name" in df.columns and df["patient_name"].notna().any():
        pname = df["patient_name"].astype(str)
    else:
        ln = df["last_name"] if "last_name" in df.columns else pd.Series([pd.NA] * len(df))
        fn = df["first_name"] if "first_name" in df.columns else pd.Series([pd.NA] * len(df))
        pname = (
            ln.fillna("").astype(str).str.strip()
            + ", "
            + fn.fillna("").astype(str).str.strip()
        )
        pname = pname.str.replace(r"^,\s*|\s*,$", "", regex=True)
    df["patient_name"] = pname

    df["mrn_norm"] = df["mrn"].apply(normalize_mrn) if "mrn" in df.columns else None
    if "dob" in df.columns:
        df["dob_norm"] = df["dob"].apply(normalize_dob)
    else:
        df["dob_norm"] = None

    name_parts = df["patient_name"].apply(normalize_name)
    df["name_norm"] = name_parts.apply(lambda d: d["name_norm"])
    df["last_name_norm"] = name_parts.apply(lambda d: d["last_name_norm"])
    df["first_name_norm"] = name_parts.apply(lambda d: d["first_name_norm"])

    hashes = []
    for _, r in df.iterrows():
        hashes.append(
            compute_afirma_row_hash({
                "research_id": r.get("research_id"),
                "mrn": r.get("mrn_norm"),
                "dob": r.get("dob_norm"),
                "specimen_id": r.get("specimen_id"),
                "accession": r.get("accession"),
                "test_date": r.get("test_date"),
                "gec_call": r.get("gec_call"),
                "gsc_call": r.get("gsc_call"),
                "panel_type": r.get("panel_type"),
                "bethesda": r.get("bethesda"),
                "xpression_variants": r.get("xpression_variants"),
            }),
        )
    df["row_hash"] = hashes

    return df


def attach_thyroseq_match_columns(raw: pd.DataFrame) -> pd.DataFrame:
    """Columns required by ``match_patients`` in 41_ingest_thyroseq_excel."""
    x = raw.copy()
    x["Req Patient/Source Name"] = x["patient_name"]
    x["Pt. MRN"] = x["mrn"] if "mrn" in x.columns else None
    x["Date of Birth"] = x["dob"] if "dob" in x.columns else None
    if "age_at_diagnosis" not in x.columns:
        x["Age at diagnosis"] = None
    else:
        x["Age at diagnosis"] = x["age_at_diagnosis"]
    if "gender" not in x.columns:
        x["Gender"] = None
    else:
        x["Gender"] = x["gender"]
    if "surgery_text" not in x.columns:
        x["Surgery"] = None
    else:
        x["Surgery"] = x["surgery_text"]
    return x


def apply_research_id_overrides(raw: pd.DataFrame, matches: pd.DataFrame) -> pd.DataFrame:
    m = matches.copy()
    for _, r in raw.iterrows():
        rid_src = r.get("research_id")
        if rid_src is None or (isinstance(rid_src, float) and pd.isna(rid_src)):
            continue
        try:
            rid = int(float(str(rid_src).strip().replace(".0", "")))
        except (ValueError, TypeError):
            continue
        sel = m["row_hash"] == r["row_hash"]
        m.loc[sel, "matched_research_id"] = rid
        m.loc[sel, "match_method"] = "source_research_id"
        m.loc[sel, "match_confidence"] = 1.0
        m.loc[sel, "review_required"] = False
        m.loc[sel, "review_reason"] = ""
    return m


def _field_present(x) -> bool:
    """True when a source field carries a non-empty value (NaN / None / blank → False)."""
    if x is None:
        return False
    if isinstance(x, float) and pd.isna(x):
        return False
    if pd.isna(x):
        return False
    return bool(str(x).strip())


def build_normalized_molecular_layers(
    raw: pd.DataFrame,
    matches: pd.DataFrame,
    xwalk: dict[str, dict[str, str]],
    assay_by_key: dict[str, dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    mr_rows: list[dict] = []
    mvl_rows: list[dict] = []
    ing_ts = datetime.now()

    for _, r in raw.iterrows():
        mrow = matches.loc[matches["row_hash"] == r["row_hash"]]
        if mrow.empty:
            continue
        rid = mrow["matched_research_id"].values[0]
        if rid is None or pd.isna(rid):
            continue
        rid = int(rid)
        match_review = bool(mrow["review_required"].values[0])
        match_method = mrow["match_method"].values[0]

        gec_raw = r.get("gec_call")
        gsc_raw = r.get("gsc_call")
        gec_h, gec_ok = harmonize_calls(gec_raw, xwalk)
        gsc_h, gsc_ok = harmonize_calls(gsc_raw, xwalk)

        xa_specs = expand_xpression_variants(r.get("xpression_variants"), xwalk)
        assay_key = resolve_afirma_assay_key(r.get("panel_type"), xwalk)
        ad = assay_by_key.get(assay_key or "", {}) if assay_key else {}
        assay_name = str(ad.get("assay_name") or "Afirma")
        panel_version = str(ad.get("panel_version") or r.get("panel_type") or "")
        loinc_code = ad.get("loinc_code")
        if loinc_code is not None and pd.isna(loinc_code):
            loinc_code = None
        if isinstance(loinc_code, float):
            loinc_code = str(int(loinc_code)) if loinc_code == int(loinc_code) else None

        test_iso, test_native = parse_test_date(r.get("test_date"))
        test_date_parsed = None
        if test_iso:
            _ts = pd.to_datetime(test_iso, errors="coerce")
            if pd.notna(_ts):
                test_date_parsed = _ts.date()

        iparts: list[str] = []
        if gec_h:
            iparts.append(f"GEC:{gec_h}")
        if gsc_h:
            iparts.append(f"GSC:{gsc_h}")
        if pd.notna(r.get("bethesda")) and str(r.get("bethesda")).strip():
            iparts.append(f"Bethesda:{str(r['bethesda']).strip()[:80]}")
        if pd.notna(r.get("fna_cytology")) and str(r.get("fna_cytology")).strip():
            iparts.append(f"FNA:{str(r['fna_cytology']).strip()[:120]}")
        interpretation = " | ".join(iparts) if iparts else None
        risk = risk_call_from_gec_gsc(gec_h, gsc_h, xwalk)

        qc_mr: list[str] = []
        if match_review:
            qc_mr.append("ambiguous_patient_match")
        if _field_present(gec_raw) and not gec_ok:
            qc_mr.append("unmapped_gec_call")
        if _field_present(gsc_raw) and not gsc_ok:
            qc_mr.append("unmapped_gsc_call")
        if not _field_present(gec_raw) and not _field_present(gsc_raw) and not xa_specs:
            qc_mr.append("expression_only_no_classifier_call")

        parse_status = "ok"
        if not gec_ok or not gsc_ok:
            parse_status = "partial"

        if "unmapped_gec_call" in qc_mr or "unmapped_gsc_call" in qc_mr:
            norm_status = "pending_review"
        elif match_review:
            norm_status = "pending_review"
        elif any(v.get("normalization_status") == "pending_review" for v in xa_specs):
            norm_status = "pending_review"
        else:
            norm_status = "normalized"

        molecular_result_id = molecular_result_id_from_parts(
            "afirma", str(r["row_hash"]), assay_key or "NA",
        )

        raw_payload = {
            "source": {
                "file": json_friendly_scalar(r.get("source_file")),
                "row": json_friendly_scalar(r.get("source_row_number")),
                "batch": json_friendly_scalar(r.get("ingestion_batch_id")),
            },
            "identifiers": {
                "specimen_id": json_friendly_scalar(r.get("specimen_id")),
                "accession": json_friendly_scalar(r.get("accession")),
                "test_date_native": json_friendly_scalar(r.get("test_date")),
                "test_date_parsed": test_iso,
            },
            "classifier_raw": {"gec": json_friendly_scalar(gec_raw), "gsc": json_friendly_scalar(gsc_raw)},
            "classifier_harmonized": {"gec": gec_h, "gsc": gsc_h},
            "bethesda": json_friendly_scalar(r.get("bethesda")),
            "fna_cytology": json_friendly_scalar(r.get("fna_cytology")),
            "panel_type": json_friendly_scalar(r.get("panel_type")),
            "assay_key_resolved": assay_key,
            "match": {
                "method": json_friendly_scalar(match_method),
                "confidence": float(mrow["match_confidence"].values[0])
                if pd.notna(mrow["match_confidence"].values[0])
                else None,
                "review_required": match_review,
            },
            "xpression_variants_raw": json_friendly_scalar(r.get("xpression_variants")),
        }
        checksum = checksum_sorted_json_payload(raw_payload)

        mr_rows.append({
            "molecular_result_id": molecular_result_id,
            "research_id": rid,
            "source_patient_id": str(r["mrn_norm"]) if pd.notna(r.get("mrn_norm")) and r.get("mrn_norm") else None,
            "source_specimen_id": str(r["specimen_id"]) if pd.notna(r.get("specimen_id")) else None,
            "source_accession": str(r["accession"]) if pd.notna(r.get("accession")) else None,
            "assay_name": assay_name,
            "panel_version": panel_version or None,
            "platform": str(ad.get("platform") or AFIRMA_PLATFORM),
            "vendor": str(ad.get("vendor") or AFIRMA_VENDOR),
            "loinc_code": loinc_code,
            "test_date_native": str(test_native) if test_native is not None else None,
            "test_date_parsed": test_date_parsed,
            "interpretation_summary": interpretation,
            "risk_call": risk,
            "canonical_hgvs": None,
            "raw_payload_json": json.dumps(raw_payload, default=str),
            "payload_checksum": checksum,
            "parse_status": parse_status,
            "normalization_status": norm_status,
            "qc_flags": json.dumps(sorted(set(qc_mr))),
            "lineage_id": str(rid),
            "ingestion_ts": ing_ts,
            "ingestion_run_id": BATCH_ID,
            "source_table": MOLECULAR_SOURCE_TABLE,
            "source_row_fingerprint": r["row_hash"],
            "molecular_episode_id": None,
            "superseded_by_molecular_result_id": None,
        })

        if not xa_specs:
            continue
        for i, spec in enumerate(xa_specs):
            v_qc = list(spec.get("af_qc_flags") or [])
            if spec.get("parse_status") == "failed":
                v_qc.append("variant_parse_failed")
            vid = molecular_variant_id_afirma(molecular_result_id, i, spec)
            af = spec.get("allele_fraction")
            if af is not None and not isinstance(af, (int, float)):
                af = None
            mvl_rows.append({
                "molecular_variant_id": vid,
                "molecular_result_id": molecular_result_id,
                "research_id": rid,
                "gene_symbol": spec.get("gene_symbol"),
                "transcript_id": spec.get("transcript_id"),
                "genomic_hgvs": spec.get("genomic_hgvs"),
                "cdna_hgvs": spec.get("cdna_hgvs"),
                "protein_hgvs": spec.get("protein_hgvs"),
                "canonical_hgvs": spec.get("canonical_hgvs"),
                "variant_class": spec.get("variant_class"),
                "allele_fraction": float(af) if af is not None else None,
                "zygosity": None,
                "interpretation_text": spec.get("interpretation_text"),
                "risk_call": None,
                "parse_status": spec.get("parse_status") or "ok",
                "normalization_status": spec.get("normalization_status") or "normalized",
                "qc_flags": json.dumps(sorted(set(v_qc))),
                "lineage_id": str(rid),
                "ingestion_ts": ing_ts,
                "partner_gene_symbol": spec.get("partner_gene_symbol"),
                "fusion_partner": spec.get("fusion_partner"),
                "raw_variant_token": spec.get("raw_variant_token"),
            })

    mr_df = pd.DataFrame(mr_rows, columns=_MR_COLS) if mr_rows else pd.DataFrame(columns=_MR_COLS)
    mvl_df = pd.DataFrame(mvl_rows, columns=_MVL_COLS) if mvl_rows else pd.DataFrame(columns=_MVL_COLS)
    return mr_df, mvl_df


def build_afirma_review_queue(
    raw: pd.DataFrame,
    matches: pd.DataFrame,
    mvl_df: pd.DataFrame | None,
    mr_df: pd.DataFrame | None,
) -> pd.DataFrame:
    rows: list[dict] = []
    now = datetime.now().isoformat()
    fingerprint_to_rid: dict[str, str] = {}
    if mr_df is not None and len(mr_df):
        fingerprint_to_rid = {
            str(r["source_row_fingerprint"]): str(int(r["research_id"]))
            for _, r in mr_df.iterrows()
            if pd.notna(r.get("source_row_fingerprint")) and pd.notna(r.get("research_id"))
        }

    for _, m in matches[matches["review_required"]].iterrows():
        rows.append({
            "source_row_hash": m["row_hash"],
            "suspected_research_ids": str(m["matched_research_id"])
            if pd.notna(m["matched_research_id"])
            else None,
            "issue_type": "match_review",
            "issue_detail": m.get("review_reason") or "review_required",
            "recommended_action": str(m.get("match_method") or ""),
            "created_at": now,
        })

    mrid_to_row_hash: dict[str, str] = {}
    if mr_df is not None and len(mr_df) and "source_row_fingerprint" in mr_df.columns:
        mrid_to_row_hash = dict(
            zip(
                mr_df["molecular_result_id"].astype(str),
                mr_df["source_row_fingerprint"].astype(str),
            ),
        )
    if mvl_df is not None and len(mvl_df):
        for _, vr in mvl_df.iterrows():
            flags: list[str] = []
            qcf = vr.get("qc_flags")
            if isinstance(qcf, str) and qcf.strip():
                try:
                    flags = json.loads(qcf)
                except json.JSONDecodeError:
                    flags = []
            if "xa_variant_class_unmapped" in flags:
                detail = "xa_variant_class_unmapped"
                mrid = str(vr.get("molecular_result_id") or "")
                rows.append({
                    "source_row_hash": mrid_to_row_hash.get(mrid),
                    "suspected_research_ids": str(int(vr["research_id"]))
                    if pd.notna(vr.get("research_id"))
                    else None,
                    "issue_type": "molecular_qc",
                    "issue_detail": f"{detail}; variant_id={vr.get('molecular_variant_id')}",
                    "recommended_action": "review_molecular_variant_long",
                    "created_at": now,
                })

    if mr_df is not None and len(mr_df):
        for _, mr in mr_df.iterrows():
            try:
                flags = json.loads(mr["qc_flags"]) if mr.get("qc_flags") else []
            except (json.JSONDecodeError, TypeError):
                flags = []
            details: list[str] = []
            if "unmapped_gec_call" in flags:
                details.append("unmapped_gec_call")
            if "unmapped_gsc_call" in flags:
                details.append("unmapped_gsc_call")
            if not details:
                continue
            fp = str(mr.get("source_row_fingerprint") or "")
            rows.append({
                "source_row_hash": fp,
                "suspected_research_ids": fingerprint_to_rid.get(fp),
                "issue_type": "crosswalk_mapping",
                "issue_detail": ",".join(details) + f"; molecular_result_id={mr.get('molecular_result_id')}",
                "recommended_action": "add_molecular_code_crosswalk_or_fix_source_file",
                "created_at": now,
            })

    cols = [
        "source_row_hash",
        "suspected_research_ids",
        "issue_type",
        "issue_detail",
        "recommended_action",
        "created_at",
    ]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows)


def _molecular_layer_tables_present(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute("SELECT 1 FROM main.molecular_results LIMIT 1")
        con.execute("SELECT 1 FROM main.molecular_variant_long LIMIT 1")
        return True
    except Exception:
        return False


def write_normalized_molecular_layer(
    con: duckdb.DuckDBPyConnection,
    mr_df: pd.DataFrame,
    mvl_df: pd.DataFrame,
    *,
    dry_run: bool,
) -> None:
    if dry_run:
        log.info("Normalized molecular layer — dry-run (no DB writes)")
        return
    if mr_df.empty:
        log.info("Normalized molecular layer skipped (no matched rows)")
        return
    if not _molecular_layer_tables_present(con):
        log.warning(
            "main.molecular_results / molecular_variant_long missing — "
            "run scripts/131_molecular_results_layer.py --execute first.",
        )
        return

    log.info("Writing governed molecular_results + molecular_variant_long (Afirma)")
    started = datetime.now()
    con.execute(
        """
        INSERT INTO main.molecular_ingestion_runs (
            ingestion_run_id, started_at, completed_at, source_system, runner_script, status, notes
        ) VALUES (?, ?, NULL, ?, ?, 'running', NULL)
        """,
        [BATCH_ID, started, "Afirma_struct ingest files", "42_ingest_afirma.py"],
    )
    con.execute(
        """
        DELETE FROM main.molecular_variant_long WHERE molecular_result_id IN (
            SELECT molecular_result_id FROM main.molecular_results WHERE source_table = ?
        )
        """,
        [MOLECULAR_SOURCE_TABLE],
    )
    con.execute("DELETE FROM main.molecular_results WHERE source_table = ?", [MOLECULAR_SOURCE_TABLE])

    con.register("_mr_ins", mr_df)
    cols_csv = ", ".join(_MR_COLS)
    con.execute(f"INSERT INTO main.molecular_results ({cols_csv}) SELECT {cols_csv} FROM _mr_ins")
    con.unregister("_mr_ins")

    if not mvl_df.empty:
        con.register("_mvl_ins", mvl_df)
        cols_m = ", ".join(_MVL_COLS)
        con.execute(
            f"INSERT INTO main.molecular_variant_long ({cols_m}) SELECT {cols_m} FROM _mvl_ins",
        )
        con.unregister("_mvl_ins")

    completed = datetime.now()
    con.execute(
        """
        UPDATE main.molecular_ingestion_runs
        SET completed_at = ?, status = 'completed'
        WHERE ingestion_run_id = ?
        """,
        [completed, BATCH_ID],
    )


def write_staging(
    con: duckdb.DuckDBPyConnection,
    raw: pd.DataFrame,
    matches: pd.DataFrame,
    review: pd.DataFrame,
) -> None:
    for name, df in [
        ("stg_afirma_structured_raw", raw),
        ("stg_afirma_match_results", matches),
        ("afirma_review_queue", review),
    ]:
        if df.empty:
            continue
        df_safe = df.copy()
        for c in df_safe.columns:
            if df_safe[c].dtype == object:
                df_safe[c] = df_safe[c].apply(
                    lambda x: str(x) if pd.notna(x) and x is not None else None,
                )
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.register("_afirma_stg", df_safe)
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _afirma_stg")
        con.unregister("_afirma_stg")


def export_bundle(
    out_dir: Path,
    raw: pd.DataFrame,
    matches: pd.DataFrame,
    mr_df: pd.DataFrame,
    mvl_df: pd.DataFrame,
    review: pd.DataFrame,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    raw.to_csv(out_dir / "afirma_raw.csv", index=False)
    matches.to_csv(out_dir / "match_results.csv", index=False)
    mr_df.to_csv(out_dir / "molecular_results.csv", index=False)
    mvl_df.to_csv(out_dir / "molecular_variant_long.csv", index=False)
    review.to_csv(out_dir / "review_queue.csv", index=False)
    manifest = {
        "pipeline": "42_ingest_afirma",
        "batch_id": BATCH_ID,
        "source_rows": len(raw),
        "matched_molecular_rows": len(mr_df),
        "variant_long_rows": len(mvl_df),
        "review_queue_rows": len(review),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def main() -> None:
    ap = argparse.ArgumentParser(description="Afirma → normalized molecular layer")
    ap.add_argument("--input", required=True, type=Path, help="CSV, XLSX, or JSON path")
    ap.add_argument("--md", action="store_true")
    ap.add_argument("--md-env", default=None)
    ap.add_argument("--local", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    path: Path = args.input.expanduser().resolve()
    if not path.is_file():
        log.error("Input not found: %s", path)
        sys.exit(1)

    con = connect(use_md=args.md, use_local=args.local, md_env=args.md_env)
    xwalk = effective_crosswalk_maps(con)
    assay_by_key = effective_assay_dictionary(con)

    df0 = load_source_table(path)
    raw = ingest_afirma_frame(df0, path.name)

    thy = _load_thyroseq_module()
    xw_dem = thy.build_crosswalk(con)
    raw_match = attach_thyroseq_match_columns(raw)
    matches = thy.match_patients(raw_match, xw_dem, con=con)
    matches = apply_research_id_overrides(raw, matches)

    mr_df, mvl_df = build_normalized_molecular_layers(raw, matches, xwalk, assay_by_key)
    review = build_afirma_review_queue(raw, matches, mvl_df, mr_df)

    out_dir = ROOT / "exports" / f"afirma_integration_{TIMESTAMP}"
    manifest = export_bundle(out_dir, raw, matches, mr_df, mvl_df, review)
    log.info("Manifest: %s", json.dumps(manifest, indent=2))

    if not args.dry_run:
        write_staging(con, raw, matches, review)
        write_normalized_molecular_layer(con, mr_df, mvl_df, dry_run=False)
    else:
        write_normalized_molecular_layer(con, mr_df, mvl_df, dry_run=True)

    con.close()
    log.info("Done — outputs under %s", out_dir)


if __name__ == "__main__":
    main()
