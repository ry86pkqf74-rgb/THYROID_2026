#!/usr/bin/env python3
"""
mig_320: ThyroSeq parser routing + freeform variant fallback (M083 / CF-M083-PARSER-BUG).

1) Parser code (molecular_consolidation_20260421/thyroseq_detailed_parser.py):
   - platform=ThyroSeq never uses Afirma parser.
   - Missing DETAILED RESULTS block: scan full report for gene tokens (fallback).

2) MotherDuck: re-parse report text for all canonical_molecular_genetics_v2 rows
   with platform='ThyroSeq', UPDATE parser-derived columns + braf_flag/braf_variant.

3) Re-apply mig_319 cohort view DDL; signoff row; optional M083 CSV refresh.

Usage:
  .venv/bin/python scripts/mig_320_thyroseq_parser_fix.py --dry-run
  .venv/bin/python scripts/mig_320_thyroseq_parser_fix.py --md
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

MCONS = REPO / "molecular_consolidation_20260421"
sys.path.insert(0, str(MCONS))

from _md_connect import PUBLICATION_DB, connect_locked  # noqa: E402

ARCHIVE_DB = '"Thyroid 2026 UPdated"'
ARCHIVE_DB_PLAIN = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
SCRIPT_TAG = "mig_320_thyroseq_parser_fix"
TIMESTAMP = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

THYROSEQ_SOURCE_QUERY = """
WITH ts_cmg AS (
    SELECT *
    FROM main.canonical_molecular_genetics_v2
    WHERE platform = 'ThyroSeq'
),
cand AS (
    SELECT
        cmg.research_id AS cmg_research_id,
        cmg.molecular_episode_id AS cmg_molecular_episode_id,
        cmg.report_source_table AS cmg_report_source_table,
        e.research_id,
        e.molecular_episode_id,
        e.platform,
        e.platform_raw,
        e.platform_version,
        e.test_date_native,
        e.resolved_test_date,
        e.detailed_findings_raw,
        e.result,
        e.mutation,
        e.source_table,
        e.adjudication_status,
        e.molecular_confidence,
        e.bethesda_category,
        e.specimen_site_normalized,
        e.linked_fna_episode_id,
        e.linked_nodule_id,
        e.linked_surgery_episode_id,
        e.braf_flag,
        e.braf_variant,
        e.ras_flag,
        e.ras_subtype,
        e.ret_flag,
        e.ret_fusion_flag,
        e.tert_flag,
        e.ntrk_flag,
        e.eif1ax_flag,
        e.tp53_flag,
        e.pax8_pparg_flag,
        e.cna_flag,
        e.fusion_flag,
        e.loh_flag,
        e.alk_flag,
        e.high_risk_marker_flag,
        e.inadequate_flag,
        e.cancelled_flag,
        e.overall_result_class,
        ROW_NUMBER() OVER (
            PARTITION BY cmg.research_id, cmg.molecular_episode_id
            ORDER BY
                CASE WHEN e.molecular_episode_id IS NOT DISTINCT FROM cmg.molecular_episode_id THEN 0 ELSE 1 END,
                LENGTH(COALESCE(e.detailed_findings_raw, '')) DESC
        ) AS rn
    FROM ts_cmg cmg
    INNER JOIN readonly_share.molecular_test_episode_v2 e
        ON CAST(e.research_id AS VARCHAR) = CAST(cmg.research_id AS VARCHAR)
        AND e.platform = 'ThyroSeq'
        AND (
            cmg.molecular_episode_id IS NULL
            OR e.molecular_episode_id = cmg.molecular_episode_id
        )
),
eps AS (
    SELECT
        research_id,
        molecular_episode_id,
        cmg_report_source_table,
        platform,
        platform_raw,
        platform_version,
        test_date_native,
        resolved_test_date,
        detailed_findings_raw,
        result,
        mutation,
        source_table,
        adjudication_status,
        molecular_confidence,
        bethesda_category,
        specimen_site_normalized,
        linked_fna_episode_id,
        linked_nodule_id,
        linked_surgery_episode_id,
        braf_flag,
        braf_variant,
        ras_flag,
        ras_subtype,
        ret_flag,
        ret_fusion_flag,
        tert_flag,
        ntrk_flag,
        eif1ax_flag,
        tp53_flag,
        pax8_pparg_flag,
        cna_flag,
        fusion_flag,
        loh_flag,
        alk_flag,
        high_risk_marker_flag,
        inadequate_flag,
        cancelled_flag,
        overall_result_class
    FROM cand
    WHERE rn = 1
),
enrich AS (
    SELECT research_id,
           pathology_raw AS enrich_pathology_raw,
           mutation_raw AS enrich_mutation_raw,
           fusion_raw AS enrich_fusion_raw
    FROM readonly_share.thyroseq_molecular_enrichment
),
testing AS (
    SELECT research_id,
           ARG_MAX(detailed_findings, length(detailed_findings)) AS testing_detailed_findings
    FROM readonly_share.molecular_testing
    WHERE detailed_findings IS NOT NULL AND length(detailed_findings) > 50
    GROUP BY research_id
)
SELECT
    eps.*,
    enrich.enrich_pathology_raw,
    enrich.enrich_mutation_raw,
    enrich.enrich_fusion_raw,

    testing.testing_detailed_findings
FROM eps
LEFT JOIN enrich ON CAST(enrich.research_id AS VARCHAR) = CAST(eps.research_id AS VARCHAR)
LEFT JOIN testing ON CAST(testing.research_id AS VARCHAR) = CAST(eps.research_id AS VARCHAR)
"""

# CMG ThyroSeq rows with no matching molecular_test_episode_v2 (patient-level / backfill-only).
THYROSEQ_ORPHAN_SOURCE_QUERY = """
WITH orph AS (
    SELECT cmg.*
    FROM main.canonical_molecular_genetics_v2 cmg
    WHERE cmg.platform = 'ThyroSeq'
      AND NOT EXISTS (
          SELECT 1
          FROM readonly_share.molecular_test_episode_v2 e
          WHERE CAST(e.research_id AS VARCHAR) = CAST(cmg.research_id AS VARCHAR)
            AND e.platform = 'ThyroSeq'
            AND (
                cmg.molecular_episode_id IS NULL
                OR e.molecular_episode_id = cmg.molecular_episode_id
            )
      )
),
enrich AS (
    SELECT research_id,
           pathology_raw AS enrich_pathology_raw,
           mutation_raw AS enrich_mutation_raw,
           fusion_raw AS enrich_fusion_raw
    FROM readonly_share.thyroseq_molecular_enrichment
),
testing AS (
    SELECT research_id,
           ARG_MAX(detailed_findings, length(detailed_findings)) AS testing_detailed_findings
    FROM readonly_share.molecular_testing
    WHERE detailed_findings IS NOT NULL AND length(detailed_findings) > 50
    GROUP BY research_id
)
SELECT
    o.research_id,
    o.molecular_episode_id,
    o.report_source_table AS cmg_report_source_table,
    o.platform,
    o.platform_raw,
    o.platform_version,
    o.test_date_native,
    o.resolved_test_date,
    CAST(NULL AS VARCHAR) AS detailed_findings_raw,
    CAST(NULL AS VARCHAR) AS result,
    CAST(NULL AS VARCHAR) AS mutation,
    o.report_source_table AS source_table,
    o.adjudication_status,
    o.molecular_confidence,
    o.bethesda_category,
    o.specimen_site_normalized,
    o.linked_fna_episode_id,
    o.linked_nodule_id,
    o.linked_surgery_episode_id,
    o.braf_flag,
    o.braf_variant,
    o.ras_flag,
    o.ras_subtype,
    o.ret_flag,
    o.ret_fusion_flag,
    o.tert_flag,
    o.ntrk_flag,
    o.eif1ax_flag,
    o.tp53_flag,
    o.pax8_pparg_flag,
    o.cna_flag,
    o.fusion_flag,
    o.loh_flag,
    o.alk_flag,
    o.high_risk_marker_flag,
    o.inadequate_flag,
    o.cancelled_flag,
    o.overall_result_class,
    enrich.enrich_pathology_raw,
    enrich.enrich_mutation_raw,
    enrich.enrich_fusion_raw,
    testing.testing_detailed_findings
FROM orph o
LEFT JOIN enrich ON CAST(enrich.research_id AS VARCHAR) = CAST(o.research_id AS VARCHAR)
LEFT JOIN testing ON CAST(testing.research_id AS VARCHAR) = CAST(o.research_id AS VARCHAR)
"""


def _safe_molecular_episode_id(val) -> int | None:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        if hasattr(val, "item"):
            val = val.item()
    except (ValueError, AttributeError):
        return None
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return int(val)


def _load_build_master():
    spec = importlib.util.spec_from_file_location("build_master_mig320", MCONS / "08_build_master.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load 08_build_master.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pick_report_text(row, pick_report_text_fn) -> tuple[str, str]:
    return pick_report_text_fn(row)


def _braf_augment(parsed_variants: list, braf_flag: bool, braf_variant: str | None) -> tuple[bool, str | None]:
    has_braf = any((v.get("gene") or "").upper() == "BRAF" for v in (parsed_variants or []))
    out_flag = bool(braf_flag) or has_braf
    bv = (braf_variant or "").strip() or None
    if has_braf:
        for v in parsed_variants or []:
            if (v.get("gene") or "").upper() != "BRAF":
                continue
            prot = (v.get("protein") or "").upper().replace(" ", "")
            if "V600" in prot or "V600E" in prot:
                bv = bv or "V600E"
            elif prot and not bv:
                bv = (v.get("protein") or "").strip() or None
            break
    return out_flag, bv


def _legacy_archive_db_attached(con: duckdb.DuckDBPyConnection) -> bool:
    names = {
        str(r[0])
        for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    }
    return ARCHIVE_DB_PLAIN in names


def phase_archive(con: duckdb.DuckDBPyConnection, apply: bool) -> None:
    snap = f"{ARCHIVE_DB}.{ARCHIVE_SCHEMA}.canonical_molecular_genetics_v2_pre_mig320_{TIMESTAMP}"
    print(f"[{SCRIPT_TAG}] archive snapshot -> {snap}")
    if not apply:
        return
    if not _legacy_archive_db_attached(con):
        print(
            f"[{SCRIPT_TAG}] WARN: database '{ARCHIVE_DB_PLAIN}' not attached; "
            "skip archive snapshot (open publication workspace with legacy DB attached to archive)"
        )
        return
    sql = f"CREATE OR REPLACE TABLE {snap} AS SELECT * FROM main.canonical_molecular_genetics_v2"
    con.execute(sql)


def phase_update(con: duckdb.DuckDBPyConnection, apply: bool) -> pd.DataFrame:
    bm = _load_build_master()
    df_ep = con.execute(THYROSEQ_SOURCE_QUERY).fetchdf()
    df_or = con.execute(THYROSEQ_ORPHAN_SOURCE_QUERY).fetchdf()
    src = pd.concat([df_ep, df_or], ignore_index=True)
    print(
        f"[{SCRIPT_TAG}] ThyroSeq rows to reparse: episode-backed={len(df_ep):,}, "
        f"orphan_cmg={len(df_or):,}, total={len(src):,}"
    )

    rows_out: list[dict] = []
    for r in src.itertuples(index=False):
        text, src_label = _pick_report_text(r, bm.pick_report_text)
        parsed = bm.parse(text or "", platform=r.platform)
        pvars = parsed.get("gene_mutations_variants") or []
        pfus = parsed.get("gene_fusions_list") or []
        extra_v, xfus = bm.synthesize_from_flags(r, pvars, pfus)
        merged_v = bm.variants_to_struct_list(pvars + extra_v)
        merged_f = bm.fusions_to_struct_list(pfus + xfus)

        gms = parsed.get("gene_mutations_status")
        if extra_v and (gms in (None, "", "Negative")):
            gms = "Positive"
        gfs = parsed.get("gene_fusions_status")
        if xfus and (gfs in (None, "", "Negative")):
            gfs = "Positive"

        merged_tert_present = any((v.get("gene") == "TERT") for v in merged_v)
        merged_tert_variant = parsed.get("tert_promoter_variant")
        if not merged_tert_variant and merged_tert_present:
            merged_tert_variant = "OTHER"

        b0 = bm._b(getattr(r, "braf_flag", None))
        bvar0 = bm._norm_str(getattr(r, "braf_variant", None))
        b1, bvar1 = _braf_augment(merged_v, b0, bvar0)

        afirma_null = {
            "afirma_braf_result": None,
            "afirma_mtc_result": None,
            "afirma_tert_c228t_result": None,
            "afirma_tert_c250t_result": None,
            "afirma_retptc_result": None,
        }
        if str(parsed.get("parser") or "").lower() == "thyroseq":
            # ThyroSeq parse must not retain Afirma section calls from mis-routed rows.
            afirma_vals = afirma_null
        else:
            afirma_vals = {
                "afirma_braf_result": parsed.get("afirma_braf_result"),
                "afirma_mtc_result": parsed.get("afirma_mtc_result"),
                "afirma_tert_c228t_result": parsed.get("afirma_tert_c228t_result"),
                "afirma_tert_c250t_result": parsed.get("afirma_tert_c250t_result"),
                "afirma_retptc_result": parsed.get("afirma_retptc_result"),
            }

        rows_out.append({
            "research_id": str(getattr(r, "research_id", "") or "").strip(),
            "molecular_episode_id": _safe_molecular_episode_id(getattr(r, "molecular_episode_id", None)),
            "cmg_report_source_table": bm._norm_str(getattr(r, "cmg_report_source_table", None)) or None,
            "parser": parsed.get("parser"),
            "parse_status": parsed.get("parse_status"),
            "n_fields_parsed": parsed.get("n_fields_parsed"),
            "test_result_summary": parsed.get("test_result_summary"),
            "rom_descriptor": parsed.get("rom_descriptor"),
            "rom_percent_raw": parsed.get("rom_percent_raw"),
            "rom_percent_low": parsed.get("rom_percent_low"),
            "rom_percent_high": parsed.get("rom_percent_high"),
            "rom_percent_point": parsed.get("rom_percent_point"),
            "rom_description": parsed.get("rom_description"),
            "specimen_adequacy_raw": parsed.get("specimen_adequacy_raw"),
            "specimen_adequacy_norm": parsed.get("specimen_adequacy_norm"),
            "gene_mutations_raw": parsed.get("gene_mutations_raw"),
            "gene_mutations_status": gms,
            "gene_fusions_raw": parsed.get("gene_fusions_raw"),
            "gene_fusions_status": gfs,
            "cna_raw": parsed.get("cna_raw"),
            "cna_status": parsed.get("cna_status"),
            "gep_raw": parsed.get("gep_raw"),
            "gep_status": parsed.get("gep_status"),
            "gep_detail": parsed.get("gep_detail"),
            "parathyroid_raw": parsed.get("parathyroid_raw"),
            "parathyroid_status": parsed.get("parathyroid_status"),
            "medullary_raw": parsed.get("medullary_raw"),
            "medullary_status": parsed.get("medullary_status"),
            "gene_mutations_variants_json": json.dumps(merged_v),
            "gene_fusions_list_json": json.dumps(merged_f),
            "tert_present": bool(merged_tert_present) if merged_v else parsed.get("tert_present"),
            "tert_promoter_variant": merged_tert_variant,
            **afirma_vals,
            "braf_flag": b1,
            "braf_variant": bvar1,
            "report_text_source": src_label,
            "report_text_length": int(len(text)) if text else 0,
        })

    up = pd.DataFrame(rows_out)
    n_mismatch_before = con.execute(
        """SELECT COUNT(*) FROM main.canonical_molecular_genetics_v2
           WHERE platform='ThyroSeq' AND LOWER(COALESCE(parser,'')) != 'thyroseq'"""
    ).fetchone()[0]
    print(f"[{SCRIPT_TAG}] Gate pre: ThyroSeq rows with parser != thyroseq: {n_mismatch_before}")

    if not apply:
        print(f"[{SCRIPT_TAG}] DRY-RUN: would UPDATE {len(up)} ThyroSeq episodes")
        return up

    con.register("mig320_cmg_updates", up)
    con.execute(
        """
        UPDATE main.canonical_molecular_genetics_v2 AS c
        SET
          parser = u.parser,
          parse_status = u.parse_status,
          n_fields_parsed = u.n_fields_parsed,
          test_result_summary = u.test_result_summary,
          rom_descriptor = u.rom_descriptor,
          rom_percent_raw = u.rom_percent_raw,
          rom_percent_low = u.rom_percent_low,
          rom_percent_high = u.rom_percent_high,
          rom_percent_point = u.rom_percent_point,
          rom_description = u.rom_description,
          specimen_adequacy_raw = u.specimen_adequacy_raw,
          specimen_adequacy_norm = u.specimen_adequacy_norm,
          gene_mutations_raw = u.gene_mutations_raw,
          gene_mutations_status = u.gene_mutations_status,
          gene_fusions_raw = u.gene_fusions_raw,
          gene_fusions_status = u.gene_fusions_status,
          cna_raw = u.cna_raw,
          cna_status = u.cna_status,
          gep_raw = u.gep_raw,
          gep_status = u.gep_status,
          gep_detail = u.gep_detail,
          parathyroid_raw = u.parathyroid_raw,
          parathyroid_status = u.parathyroid_status,
          medullary_raw = u.medullary_raw,
          medullary_status = u.medullary_status,
          gene_mutations_variants = CAST(
            from_json(u.gene_mutations_variants_json,
              '[{"gene":"VARCHAR","protein":"VARCHAR","cdna":"VARCHAR","af_pct":"INTEGER","source_call":"VARCHAR"}]')
            AS STRUCT(gene VARCHAR, protein VARCHAR, cdna VARCHAR, af_pct INTEGER, source_call VARCHAR)[]),
          gene_fusions_list = CAST(
            from_json(u.gene_fusions_list_json,
              '[{"gene1":"VARCHAR","gene2":"VARCHAR","source_call":"VARCHAR"}]')
            AS STRUCT(gene1 VARCHAR, gene2 VARCHAR, source_call VARCHAR)[]),
          tert_present = u.tert_present,
          tert_promoter_variant = u.tert_promoter_variant,
          afirma_braf_result = u.afirma_braf_result,
          afirma_mtc_result = u.afirma_mtc_result,
          afirma_tert_c228t_result = u.afirma_tert_c228t_result,
          afirma_tert_c250t_result = u.afirma_tert_c250t_result,
          afirma_retptc_result = u.afirma_retptc_result,
          braf_flag = u.braf_flag,
          braf_variant = u.braf_variant,
          report_text_source = u.report_text_source,
          report_text_length = u.report_text_length,
          built_at = CURRENT_TIMESTAMP,
          builder_version = ?
        FROM mig320_cmg_updates u
        WHERE c.platform = 'ThyroSeq'
          AND CAST(c.research_id AS VARCHAR) = u.research_id
          AND c.molecular_episode_id IS NOT DISTINCT FROM u.molecular_episode_id
          AND c.report_source_table IS NOT DISTINCT FROM u.cmg_report_source_table
        """,
        [f"mig320_{TIMESTAMP}"],
    )
    n_mismatch_after = con.execute(
        """SELECT COUNT(*) FROM main.canonical_molecular_genetics_v2
           WHERE platform='ThyroSeq' AND LOWER(COALESCE(parser,'')) != 'thyroseq'"""
    ).fetchone()[0]
    print(f"[{SCRIPT_TAG}] Gate post: ThyroSeq parser != thyroseq: {n_mismatch_after} (expect 0)")
    return up


def phase_rebuild_m083_view(con: duckdb.DuckDBPyConnection, apply: bool) -> None:
    ddl_path = REPO / "qc_framework_v1/migrations/319_m083_braf_dual_platform_cohort_view_20260505.sql"
    ddl = ddl_path.read_text()
    print(f"[{SCRIPT_TAG}] re-apply {ddl_path.name}")
    if apply:
        con.execute(ddl)


def phase_signoff(con: duckdb.DuckDBPyConnection, apply: bool, summary: str) -> None:
    sql = f"""
    INSERT INTO "{PUBLICATION_DB}".main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
    SELECT 'mig_320', CURRENT_TIMESTAMP, 'cursor_mig_320', ?
    WHERE NOT EXISTS (SELECT 1 FROM "{PUBLICATION_DB}".main.signoff_migration WHERE mig_id = 'mig_320')
    """
    print(f"[{SCRIPT_TAG}] signoff_migration mig_320")
    if apply:
        con.execute(sql, [summary])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--md", action="store_true", help="Apply to MotherDuck publication DB")
    ap.add_argument("--dry-run", action="store_true", help="Log only (no writes)")
    args = ap.parse_args()
    apply = bool(args.md and not args.dry_run)

    if not args.md:
        print(f"[{SCRIPT_TAG}] local parser smoke (pass --md to run MotherDuck migration)")
        bm = _load_build_master()
        sample = (
            "INTERPRETATION: suspicious. TERT promoter region sequencing.\n"
            "BRAF V600E (positive, AF 12%)\n"
        )
        r = bm.parse(sample, platform="ThyroSeq")
        print("sample parse:", {k: r.get(k) for k in ("parser", "parse_status", "gene_mutations_status")})
        assert r.get("parser") == "thyroseq"
        return

    con = connect_locked()
    if args.dry_run:
        print(f"[{SCRIPT_TAG}] --md --dry-run: validation + staging only (no archive/update/signoff)")

    phase_archive(con, apply)
    up_df = phase_update(con, apply)
    n_bad = 0
    if apply:
        n_bad = con.execute(
            """SELECT COUNT(*) FROM main.canonical_molecular_genetics_v2
               WHERE platform='ThyroSeq' AND LOWER(COALESCE(parser,'')) != 'thyroseq'"""
        ).fetchone()[0]

    phase_rebuild_m083_view(con, apply)

    # M083 gate counts (99-patient subset — informational)
    if apply:
        gate_sql = """
        WITH affected AS (
          SELECT research_id
          FROM manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1
          WHERE afirma_braf='positive' AND thyroseq_braf='negative' AND path_braf_status='positive'
        )
        SELECT
          COUNT(DISTINCT a.research_id) AS n_affected,
          COUNT(DISTINCT CASE WHEN cmg.parse_status IN ('ok','partial') THEN a.research_id END) AS n_okish,
          COUNT(DISTINCT CASE WHEN cmg.braf_flag THEN a.research_id END) AS n_braf_pos,
          COUNT(DISTINCT CASE WHEN LOWER(COALESCE(cmg.braf_variant,'')) LIKE '%v600e%'
                               OR LOWER(COALESCE(cmg.braf_variant,'')) LIKE '%v600%'
            THEN a.research_id END) AS n_v600e_hint
        FROM affected a
        JOIN main.canonical_molecular_genetics_v2 cmg
          ON CAST(cmg.research_id AS VARCHAR) = a.research_id
        WHERE cmg.platform='ThyroSeq'
        """
        g = con.execute(gate_sql).fetchdf().iloc[0].to_dict()
        print(f"[{SCRIPT_TAG}] M083 affected subset gates: {g}")
        summary = (
            f"mig_320 ThyroSeq parser fix (routing + freeform fallback). "
            f"Rows reparser={len(up_df)}; post gate parser mismatch ThyroSeq={n_bad}. "
            f"M083 99-pt gate snapshot: {g}"
        )
    else:
        summary = f"mig_320 dry-run; staging rows={len(up_df)}"

    phase_signoff(con, apply, summary)


if __name__ == "__main__":
    main()
