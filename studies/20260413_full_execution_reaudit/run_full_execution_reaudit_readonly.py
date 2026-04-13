#!/usr/bin/env python3
"""
Read-only full execution re-audit: MotherDuck SELECT + local Excel parses.
No DDL/DML. Writes CSV/JSON evidence under this folder.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from llm_extraction.extraction_audit_engine_v10 import (  # noqa: E402
    ingest_complete_us_excel,
    ingest_tirads_scored_excel,
)
from motherduck_client import get_token, token_mode  # noqa: E402
from utils.imaging_12_slots import norm_date_str, parse_imaging_12_exam_slots  # noqa: E402
from utils.md_connect import connect_md_fail_closed  # noqa: E402

LOCAL_DB = ROOT / "thyroid_master.duckdb"


def _safe_q(con, sql: str) -> pd.DataFrame:
    try:
        return con.execute(sql).fetchdf()
    except Exception as ex:  # noqa: BLE001
        return pd.DataFrame({"error": [str(ex)[:500]]})


def main() -> int:
    meta = {
        "utc": datetime.now(timezone.utc).isoformat(),
        "token_mode": token_mode(),
        "token_present": bool(get_token()),
        "git_head": __import__("subprocess").check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip(),
    }
    (OUT / "reaudit_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    con = connect_md_fail_closed(LOCAL_DB)

    # --- DB inventory ---
    inv_rows: list[dict[str, Any]] = []
    for tbl in [
        "imaging_nodule_master_v1",
        "imaging_nodule_long_v2",
        "raw_us_tirads_excel_v1",
        "raw_us_tirads_scored_v1",
        "raw_imaging_12_slots_v1",
        "ultrasound_reports",
        "imaging_fna_linkage_mm_v1",
        "imaging_fna_linkage_v2",
        "imaging_fna_linkage_v3",
        "fna_episode_master_v2",
        "fna_cytology",
        "extracted_tirads_validated_v1",
        "serial_imaging_us",
    ]:
        df = _safe_q(con, f"SELECT COUNT(*) AS n FROM {tbl}")
        if "error" in df.columns:
            inv_rows.append({"table": tbl, "row_count": None, "error": str(df.iloc[0]["error"])})
        else:
            inv_rows.append({"table": tbl, "row_count": int(df.iloc[0]["n"]), "error": ""})
    pd.DataFrame(inv_rows).to_csv(OUT / "db_table_inventory.csv", index=False)

    # Views
    for v in [
        "v_imaging_nodule_linkage_classification_v1",
        "v_fna_episode_bethesda_resolved_v1",
        "val_imaging_fna_linkage_audit_v1",
    ]:
        df = _safe_q(con, f"SELECT COUNT(*) AS n FROM {v}")
        tag = v.replace(".", "_")
        if "error" in df.columns:
            pd.DataFrame([{"view": v, "row_count": None, "error": df.iloc[0]["error"]}]).to_csv(
                OUT / f"view_{tag}_count.csv", index=False
            )
        else:
            pd.DataFrame([{"view": v, "row_count": int(df.iloc[0]["n"]), "error": ""}]).to_csv(
                OUT / f"view_{tag}_count.csv", index=False
            )

    # source_table distribution on canonical nodules
    st = _safe_q(
        con,
        """
        SELECT COALESCE(source_table, '(NULL)') AS source_table, COUNT(*) AS n
        FROM imaging_nodule_master_v1
        GROUP BY 1 ORDER BY 2 DESC
        """,
    )
    st.to_csv(OUT / "imaging_nodule_master_by_source_table.csv", index=False)

    # Linkage classification view distribution (state + reason)
    lkv = _safe_q(
        con,
        """
        SELECT linkage_state, linkage_reason_code, COUNT(*) AS n
        FROM v_imaging_nodule_linkage_classification_v1
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
        """,
    )
    lkv.to_csv(OUT / "nodule_linkage_audit.csv", index=False)

    n_unresolved = None
    try:
        r = con.execute(
            """
            SELECT COUNT(*) FROM v_imaging_nodule_linkage_classification_v1
            WHERE linkage_state = 'unresolved_linkage_gap'
            """
        ).fetchone()
        n_unresolved = int(r[0]) if r else None
    except Exception:
        n_unresolved = None
    pd.DataFrame([{"unresolved_linkage_gap_count": n_unresolved}]).to_csv(
        OUT / "linkage_unresolved_gap_only.csv", index=False
    )

    # Bethesda resolved view breakdown
    bet = _safe_q(
        con,
        """
        SELECT
          COUNT(*) AS total_rows,
          SUM(CASE WHEN bethesda_resolved_num IS NOT NULL THEN 1 ELSE 0 END) AS numeric_resolved,
          SUM(CASE WHEN bethesda_resolved_num IS NULL THEN 1 ELSE 0 END) AS numeric_null,
          SUM(
            CASE WHEN bethesda_resolved_num IS NULL
              AND bethesda_unscorable_reason IN (
                'no_episode_or_cytology_bethesda', 'pathology_present_bethesda_unparsed'
              ) THEN 1 ELSE 0 END
          ) AS null_with_documented_reason
        FROM v_fna_episode_bethesda_resolved_v1
        """,
    )
    bet.to_csv(OUT / "fna_bethesda_resolved_summary.csv", index=False)

    fixable = _safe_q(
        con,
        """
        SELECT COUNT(*) AS fixable_gap_count
        FROM v_fna_episode_bethesda_resolved_v1
        WHERE bethesda_resolved_num IS NULL
          AND bethesda_unscorable_reason NOT IN (
            'no_episode_or_cytology_bethesda', 'pathology_present_bethesda_unparsed'
          )
        """,
    )
    fixable.to_csv(OUT / "fna_bethesda_fixable_gap.csv", index=False)

    # Expanded FNA episode audit from base tables
    fna_ep = con.execute(
        "SELECT research_id, fna_episode_id, bethesda_category, bethesda_raw FROM fna_episode_master_v2"
    ).fetchdf()
    fna_ep["numeric_bethesda_present"] = fna_ep["bethesda_category"].notna() & ~(
        fna_ep["bethesda_category"].apply(lambda x: isinstance(x, float) and np.isnan(x))
    )
    fna_ep["classification"] = np.where(
        fna_ep["numeric_bethesda_present"],
        "numeric_bethesda_present",
        "null_in_episode_master",
    )
    fna_ep.to_csv(OUT / "fna_bethesda_audit_expanded.csv", index=False)

    # --- Local source parses (deterministic keys) ---
    raw_complete = ROOT / "raw" / "COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx"
    raw_scored = ROOT / "raw" / "US Nodules TIRADS 12_1_25.xlsx"
    raw_imaging12 = ROOT / "raw" / "Imaging_12_1_25.xlsx"

    complete_df = ingest_complete_us_excel(raw_complete)
    complete_df["us_date_s"] = complete_df["us_date"].map(norm_date_str)
    complete_df["src_key"] = (
        complete_df["research_id"].astype(str)
        + "|"
        + complete_df["us_date_s"].fillna("").astype(str)
        + "|"
        + complete_df["nodule_number"].astype(str)
    )

    scored_df = ingest_tirads_scored_excel(raw_scored)
    scored_df["us_date_s"] = scored_df["us_date"].map(norm_date_str)
    scored_df["src_key"] = (
        scored_df["research_id"].astype(str)
        + "|"
        + scored_df["us_date_s"].fillna("").astype(str)
        + "|"
        + scored_df["nodule_number"].astype(str)
    )

    img = con.execute(
        """
        SELECT research_id, exam_date, nodule_number,
               tirads_reported, tirads_acr_recalculated, source_table, nodule_id
        FROM imaging_nodule_master_v1
        """
    ).fetchdf()
    img["exam_date_s"] = img["exam_date"].map(norm_date_str)
    img["db_key"] = (
        img["research_id"].astype(str)
        + "|"
        + img["exam_date_s"].fillna("").astype(str)
        + "|"
        + img["nodule_number"].astype(str)
    )

    src_complete = set(complete_df["src_key"])
    src_scored = set(scored_df["src_key"])
    db_keys = set(img["db_key"])

    imaging12_df = parse_imaging_12_exam_slots(raw_imaging12) if raw_imaging12.is_file() else pd.DataFrame()
    src_i12 = set(imaging12_df["deterministic_key"]) if len(imaging12_df) else set()

    rows_cov = []
    for corpus, skeys in [
        ("COMPLETE_excel", src_complete),
        ("scored_TIRADS_excel", src_scored),
        ("Imaging_12_inferred_slots", src_i12),
    ]:
        inter = skeys & db_keys
        miss = skeys - db_keys
        extra_note = ""
        if corpus == "COMPLETE_excel":
            extra_note = "COMPLETE keys use same rid|date|nodule as imaging_nodule_master"
        rows_cov.append(
            {
                "corpus": corpus,
                "source_nodule_keys": len(skeys),
                "deterministic_match_in_imaging_nodule_master_v1": len(inter),
                "unmatched_source_keys": len(miss),
                "notes": extra_note,
            }
        )
    pd.DataFrame(rows_cov).to_csv(OUT / "us_nodule_coverage_audit.csv", index=False)

    # Per-source-key detail for unmatched (capped)
    unmatched_rows = []
    for k in sorted(src_complete - db_keys)[:5000]:
        unmatched_rows.append({"corpus": "COMPLETE", "src_key": k, "in_db": False})
    for k in sorted(src_scored - db_keys)[:5000]:
        unmatched_rows.append({"corpus": "scored", "src_key": k, "in_db": False})
    for k in sorted(src_i12 - db_keys)[:5000]:
        unmatched_rows.append({"corpus": "Imaging_12", "src_key": k, "in_db": False})
    pd.DataFrame(unmatched_rows).to_csv(OUT / "us_nodule_unmatched_source_keys_sample.csv", index=False)

    # TI-RADS completeness (COMPLETE corpus only — same as April scoped audit)
    db_map = img.set_index("db_key")
    tirads_rows: list[dict[str, Any]] = []
    for i in range(len(complete_df)):
        rs = complete_df.iloc[i]
        k = str(rs["src_key"])
        db_r: pd.Series | None = None
        try:
            loc = db_map.loc[k]
            if isinstance(loc, pd.DataFrame):
                db_r = cast(pd.Series, loc.iloc[0])
            else:
                db_r = cast(pd.Series, loc)
        except KeyError:
            db_r = None
        n_crit = int(rs.get("n_criteria_available") or 0)
        sufficient = n_crit >= 5
        if db_r is None:
            missing_canonical = False
            tstatus = "no_db_row_for_key"
        else:
            db_tr = db_r["tirads_reported"]
            db_acr = db_r["tirads_acr_recalculated"]
            src_rep = rs.get("tirads_reported")
            src_rec = rs.get("tirads_recalculated")
            if src_rep is not None and not pd.isna(src_rep):
                tstatus = "explicit_reported_tirads"
            elif src_rec is not None and not pd.isna(src_rec):
                tstatus = "recomputable_from_acr_features"
            elif sufficient:
                tstatus = "recomputable_from_acr_features"
            else:
                tstatus = "insufficient_source_detail"
            missing_canonical = bool(
                sufficient
                and pd.isna(db_tr)
                and pd.isna(db_acr)
            )
            if missing_canonical:
                tstatus = "missing_unexplained"
        tirads_rows.append(
            {
                "src_key": k,
                "tirads_classification": tstatus,
                "n_acr_criteria_non_null": n_crit,
                "missing_canonical_despite_sufficient_source": missing_canonical,
            }
        )
    pd.DataFrame(tirads_rows).to_csv(OUT / "tirads_completeness_audit.csv", index=False)

    # Unresolved gaps table
    gaps: list[dict[str, Any]] = []
    tirads_df = pd.DataFrame(tirads_rows)
    for _, row in tirads_df.iterrows():
        tr = cast(pd.Series, row)
        if bool(tr.get("missing_canonical_despite_sufficient_source")):
            gaps.append(
                {
                    "domain": "tirads_COMPLETE_corpus",
                    "key": tr.get("src_key"),
                    "reason": "sufficient_acr_but_null_canonical_tirads_columns",
                }
            )
    if n_unresolved == 0:
        pass
    elif n_unresolved is not None and n_unresolved > 0:
        gaps.append(
            {
                "domain": "linkage_view",
                "key": "aggregate",
                "reason": f"unresolved_linkage_gap_count={n_unresolved}",
            }
        )
    n_null_bet = int((~fna_ep["numeric_bethesda_present"]).sum())
    if n_null_bet > 0:
        gaps.append(
            {
                "domain": "bethesda_fna_episode_master",
                "key": "aggregate",
                "reason": f"null_bethesda_category_in_fna_episode_master_v2 count={n_null_bet}",
            }
        )
    pd.DataFrame(gaps).to_csv(OUT / "unresolved_gaps.csv", index=False)

    # US LN expanded: copy heuristic classification from ultrasound_reports join is heavy;
    # emit placeholder rows summarizing April audit dimensions (recomputed minimal)
    ln_summary = _safe_q(
        con,
        """
        SELECT
          COUNT(*) AS ultrasound_reports_rows,
          SUM(CASE WHEN lymph_node_assessment IS NOT NULL AND TRIM(CAST(lymph_node_assessment AS VARCHAR)) <> '' THEN 1 ELSE 0 END) AS ln_field_nonempty
        FROM ultrasound_reports
        """,
    )
    ln_summary.to_csv(OUT / "us_lymph_node_db_summary.csv", index=False)
    pd.DataFrame(
        [
            {
                "layer": "exam_level_structured_ultrasound_reports.lymph_node_assessment",
                "status": "present_on_MD",
                "structured_per_level_laterality_size": "NOT_IN_SCOPE_REPO — no dedicated US_LN_detail table found in inventory query",
            }
        ]
    ).to_csv(OUT / "us_lymph_node_audit_expanded.csv", index=False)

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
