"""Steps 4-7 - build the consolidated molecular tables and views.

Creates:
  - molecular_genetics_test_v2          (1 row per real genetics test episode)
  - molecular_genetics_from_notes_v2    (NLP-derived facts from notes)
  - molecular_variant_flat_v2           (view, UNNESTed variants)
  - molecular_fusion_flat_v2            (view, UNNESTed fusions)

Text-source priority for each episode (best-available cascade):
  1. molecular_test_episode_v2.detailed_findings_raw       (when len > 50)
  2. thyroseq_molecular_enrichment.pathology_raw           (1:1 by research_id)
  3. molecular_testing.detailed_findings                   (1:N by research_id, MAX-length picked)
  4. Synthesized fallback from short fields (mutation, result)
"""
from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, HERE)

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
from motherduck_client import get_token  # type: ignore  # noqa: E402
from thyroseq_detailed_parser import parse  # noqa: E402
from utils.molecular_report_derivation import derive_platform_from_report_header  # noqa: E402

DB = "thyroid_canonical_publication_v1_0"
BUILDER_VERSION = "v3_2026-04-21"


SOURCE_QUERY = """
WITH eps AS (
    SELECT
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
        e.ingestion_source,
        e.adjudication_status,
        e.molecular_confidence,
        e.bethesda_category,
        e.specimen_site_normalized,
        e.linked_fna_episode_id,
        e.linked_nodule_id,
        e.linked_surgery_episode_id,
        e.braf_flag, e.braf_variant,
        e.ras_flag,  e.ras_subtype,
        e.ret_flag,  e.ret_fusion_flag,
        e.tert_flag, e.ntrk_flag, e.eif1ax_flag, e.tp53_flag, e.pax8_pparg_flag,
        e.cna_flag,  e.fusion_flag, e.loh_flag, e.alk_flag,
        e.high_risk_marker_flag, e.inadequate_flag, e.cancelled_flag,
        e.overall_result_class
    FROM molecular_test_episode_v2 e
    WHERE e.platform IS NOT NULL
      AND (
          UPPER(TRIM(CAST(e.platform AS VARCHAR))) <> 'OTHER'
          OR (
              LENGTH(TRIM(COALESCE(e.detailed_findings_raw, ''))) > 50
              OR LENGTH(TRIM(COALESCE(e.mutation, ''))) > 3
              OR COALESCE(e.braf_flag, FALSE)
              OR COALESCE(e.ras_flag, FALSE)
              OR COALESCE(e.tert_flag, FALSE)
              OR LOWER(TRIM(COALESCE(e.overall_result_class, ''))) IN ('positive', 'suspicious')
          )
      )
),
enrich AS (
    SELECT research_id,
           pathology_raw AS enrich_pathology_raw,
           mutation_raw  AS enrich_mutation_raw,
           fusion_raw    AS enrich_fusion_raw,
           gep_raw       AS enrich_gep_raw,
           source_file   AS enrich_source_file
    FROM thyroseq_molecular_enrichment
),
testing AS (
    SELECT research_id,
           ARG_MAX(detailed_findings, length(detailed_findings)) AS testing_detailed_findings,
           MAX(length(detailed_findings))                        AS testing_detailed_findings_len,
           ANY_VALUE(thyroseq_afirma)                            AS testing_thyroseq_afirma,
           ANY_VALUE(genetic_test)                               AS testing_genetic_test
    FROM molecular_testing
    WHERE detailed_findings IS NOT NULL AND length(detailed_findings) > 50
    GROUP BY research_id
)
SELECT
    eps.*,
    enrich.enrich_pathology_raw,
    enrich.enrich_mutation_raw,
    enrich.enrich_fusion_raw,
    enrich.enrich_gep_raw,
    enrich.enrich_source_file,
    testing.testing_detailed_findings,
    testing.testing_detailed_findings_len,
    testing.testing_thyroseq_afirma,
    testing.testing_genetic_test
FROM eps
LEFT JOIN enrich  ON enrich.research_id  = eps.research_id
LEFT JOIN testing ON testing.research_id = eps.research_id
"""


def pick_report_text(row) -> tuple[str, str]:
    """Choose the best available text for parsing; return (text, source_label)."""
    candidates: list[tuple[str, object]] = [
        ("episode.detailed_findings_raw", row.detailed_findings_raw),
        ("enrichment.pathology_raw",       row.enrich_pathology_raw),
        ("testing.detailed_findings",      row.testing_detailed_findings),
    ]
    blocks = [
        (label, str(val))
        for label, val in candidates
        if val is not None and isinstance(val, (str, bytes)) and len(str(val).strip()) > 0
    ]
    if not blocks:
        return "", ""
    for label, txt in blocks:
        if "DETAILED RESULTS" in txt.upper():
            return txt, label
    blocks.sort(key=lambda lt: len(lt[1]), reverse=True)
    label, txt = blocks[0]
    if len(txt.strip()) >= 50:
        return txt, label
    pieces = [str(row.mutation or ""), str(row.result or ""),
              str(row.enrich_mutation_raw or ""), str(row.enrich_fusion_raw or "")]
    synth = "\n".join(p for p in pieces if p.strip())
    if synth.strip():
        return synth, "synthesized_short_fields"
    return txt, label


CREATE_SQL = """
DROP TABLE IF EXISTS molecular_genetics_test_v2;

CREATE TABLE molecular_genetics_test_v2 (
    research_id                 VARCHAR NOT NULL,
    molecular_episode_id        BIGINT,
    test_date_native            TIMESTAMP,
    resolved_test_date          VARCHAR,

    platform                    VARCHAR,
    platform_raw                VARCHAR,
    platform_version            INTEGER,
    bethesda_category           INTEGER,
    specimen_site_normalized    VARCHAR,

    linked_fna_episode_id       VARCHAR,
    linked_nodule_id            INTEGER,
    linked_surgery_episode_id   INTEGER,

    parser                      VARCHAR,
    parse_status                VARCHAR,
    n_fields_parsed             INTEGER,

    test_result_summary         VARCHAR,
    rom_descriptor              VARCHAR,
    rom_percent_raw             VARCHAR,
    rom_percent_low             DOUBLE,
    rom_percent_high            DOUBLE,
    rom_percent_point           DOUBLE,
    rom_description             VARCHAR,

    specimen_adequacy_raw       VARCHAR,
    specimen_adequacy_norm      VARCHAR,
    gene_mutations_raw          VARCHAR,
    gene_mutations_status       VARCHAR,
    gene_fusions_raw            VARCHAR,
    gene_fusions_status         VARCHAR,
    cna_raw                     VARCHAR,
    cna_status                  VARCHAR,
    gep_raw                     VARCHAR,
    gep_status                  VARCHAR,
    gep_detail                  VARCHAR,
    parathyroid_raw             VARCHAR,
    parathyroid_status          VARCHAR,
    medullary_raw               VARCHAR,
    medullary_status            VARCHAR,

    gene_mutations_variants     STRUCT(gene VARCHAR, protein VARCHAR, cdna VARCHAR, af_pct INTEGER, source_call VARCHAR)[],
    gene_fusions_list           STRUCT(gene1 VARCHAR, gene2 VARCHAR, source_call VARCHAR)[],

    tert_present                BOOLEAN,
    tert_promoter_variant       VARCHAR,

    afirma_braf_result          VARCHAR,
    afirma_mtc_result           VARCHAR,
    afirma_tert_c228t_result    VARCHAR,
    afirma_tert_c250t_result    VARCHAR,
    afirma_retptc_result        VARCHAR,

    -- Structured flags carried forward from molecular_test_episode_v2 (preserves
    -- signals from short reports / notes-derived rollups that the text parser
    -- alone would miss).
    braf_flag                   BOOLEAN,
    braf_variant                VARCHAR,
    ras_flag                    BOOLEAN,
    ras_subtype                 VARCHAR,
    ret_flag                    BOOLEAN,
    ret_fusion_flag             BOOLEAN,
    tert_flag                   BOOLEAN,
    ntrk_flag                   BOOLEAN,
    eif1ax_flag                 BOOLEAN,
    tp53_flag                   BOOLEAN,
    pax8_pparg_flag             BOOLEAN,
    cna_flag                    BOOLEAN,
    fusion_flag                 BOOLEAN,
    loh_flag                    BOOLEAN,
    alk_flag                    BOOLEAN,
    high_risk_marker_flag       BOOLEAN,
    inadequate_flag             BOOLEAN,
    cancelled_flag              BOOLEAN,
    overall_result_class        VARCHAR,

    report_text_ref             VARCHAR,
    report_text_source          VARCHAR,
    report_text_length          INTEGER,
    report_source_table         VARCHAR,
    ingestion_source            VARCHAR,
    adjudication_status         VARCHAR,
    molecular_confidence        DOUBLE,

    built_at                    TIMESTAMP DEFAULT current_timestamp,
    builder_version             VARCHAR  DEFAULT 'v3_2026-04-21'
);
"""

# Normalize OCR-derived protein notation: collapse internal whitespace,
# fix common drift like "p.C2287" -> "p.C228T", "p. V600E" -> "p.V600E",
# add missing "p." prefix on bare HGVS strings like "V600E" or "Q61R".
import re as _re

_PROT_SPACE_RX  = _re.compile(r"^p\.\s+")
_PROT_C228_RX   = _re.compile(r"^p\.\s*C\s*228\s*7\s*$", _re.I)
_HGVS_LIKE_RX   = _re.compile(r"^[A-Z][a-zA-Z]?\d{1,4}[A-Za-z_*][A-Za-z_*0-9]*$")
_TAUTOLOGY_GENES = {"BRAF", "NRAS", "KRAS", "HRAS", "TERT", "RET", "TP53", "EIF1AX",
                    "NTRK1", "NTRK3", "ALK", "PAX8", "PPARG", "TSHR"}


def _norm_protein(p: str | None, gene: str | None = None) -> str | None:
    if p is None:
        return None
    s = _PROT_SPACE_RX.sub("p.", str(p)).strip()
    s = _re.sub(r"\s+", "", s)
    if not s:
        return None
    if gene and s.upper() == gene.upper():
        return None
    if s.upper() in _TAUTOLOGY_GENES:
        return None
    if _PROT_C228_RX.match(s):
        return "p.C228T"
    if _HGVS_LIKE_RX.match(s):
        return f"p.{s}"
    return s


def variants_to_struct_list(items):
    norm = []
    seen: set[tuple[str | None, str | None]] = set()
    for it in (items or []):
        gene = it.get("gene")
        protein = _norm_protein(it.get("protein"), gene)
        key = (gene, protein)
        if key in seen:
            continue
        seen.add(key)
        norm.append({
            "gene": gene,
            "protein": protein,
            "cdna": it.get("cdna"),
            "af_pct": int(it["af_pct"]) if it.get("af_pct") is not None else None,
            "source_call": it.get("source_call"),
        })
    return norm


def fusions_to_struct_list(items):
    norm = []
    seen: set[tuple[str | None, str | None]] = set()
    for it in (items or []):
        key = (it.get("gene1"), it.get("gene2"))
        if key in seen:
            continue
        seen.add(key)
        norm.append({
            "gene1": it.get("gene1"),
            "gene2": it.get("gene2"),
            "source_call": it.get("source_call"),
        })
    return norm


def _b(v):
    """Best-effort bool coercion that treats NA / NaN as None."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return bool(v)


# Episode-flag fields and the corresponding gene to synthesize when no parsed
# variant is found in text. (Fusion flags become entries in gene_fusions_list.)
FLAG_TO_GENE = [
    ("braf_flag",       "BRAF",   "snv"),
    ("ras_flag",        "RAS",    "snv"),
    ("tert_flag",       "TERT",   "snv"),
    ("ntrk_flag",       "NTRK",   "fusion"),
    ("eif1ax_flag",     "EIF1AX", "snv"),
    ("tp53_flag",       "TP53",   "snv"),
    ("pax8_pparg_flag", "PAX8",   "fusion"),
    ("ret_flag",        "RET",    "snv"),
    ("ret_fusion_flag", "RET",    "fusion"),
    ("alk_flag",        "ALK",    "fusion"),
]


def synthesize_from_flags(row, parsed_variants, parsed_fusions):
    """Add minimal variant / fusion entries from structured episode flags
    when the text parser did not already record them. Preserves signal from
    rows whose report text has no DETAILED RESULTS block."""
    have_genes = {(v.get("gene") or "").upper() for v in (parsed_variants or [])}
    have_fusion_genes = set()
    for f in (parsed_fusions or []):
        have_fusion_genes.add((f.get("gene1") or "").upper())
        have_fusion_genes.add((f.get("gene2") or "").upper())

    extra_v: list[dict] = []
    extra_f: list[dict] = []
    for col, gene, kind in FLAG_TO_GENE:
        flag_val = _b(getattr(row, col, None))
        if not flag_val:
            continue
        if kind == "snv":
            if gene == "RAS":
                subtype = _norm_str(getattr(row, "ras_subtype", None))
                ras_gene = "NRAS"
                if subtype:
                    up = subtype.upper()
                    if "KRAS" in up:
                        ras_gene = "KRAS"
                    elif "HRAS" in up:
                        ras_gene = "HRAS"
                if ras_gene not in have_genes:
                    extra_v.append({
                        "gene": ras_gene,
                        "protein": _norm_protein(subtype, ras_gene),
                        "cdna": None, "af_pct": None,
                        "source_call": "episode_flag:ras_flag",
                    })
                    have_genes.add(ras_gene)
            elif gene == "BRAF" and "BRAF" not in have_genes:
                braf_var = _norm_str(getattr(row, "braf_variant", None))
                prot = _norm_protein(braf_var, "BRAF") if braf_var else "p.V600E"
                extra_v.append({
                    "gene": "BRAF",
                    "protein": prot or "p.V600E",
                    "cdna": None, "af_pct": None,
                    "source_call": "episode_flag:braf_flag",
                })
                have_genes.add("BRAF")
            elif gene not in have_genes:
                extra_v.append({
                    "gene": gene, "protein": None, "cdna": None, "af_pct": None,
                    "source_call": f"episode_flag:{col}",
                })
                have_genes.add(gene)
        else:  # fusion
            if gene not in have_fusion_genes:
                extra_f.append({
                    "gene1": gene, "gene2": "?",
                    "source_call": f"episode_flag:{col}",
                })
                have_fusion_genes.add(gene)
    return extra_v, extra_f


def _norm_str(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return str(v).strip() or None


def main() -> None:
    os.environ["motherduck_token"] = get_token() or ""
    con = duckdb.connect(f"md:{DB}")

    print("Creating molecular_genetics_test_v2 ...")
    con.execute(CREATE_SQL)

    print("Pulling source episodes + linked text ...")
    src = con.execute(SOURCE_QUERY).fetchdf()
    print(f"source rows: {len(src):,}")

    def _i(v):
        return None if v is None or pd.isna(v) else int(v)

    def _f(v):
        return None if v is None or pd.isna(v) else float(v)

    def _s(v, n=None):
        if v is None or pd.isna(v):
            return None
        s = str(v)
        return s[:n] if n else s

    text_source_counts: dict[str, int] = {}
    flag_synth_counts = {"variants": 0, "fusions": 0, "rows_augmented": 0}
    rows = []
    for r in src.itertuples(index=False):
        text, src_label = pick_report_text(r)
        text_source_counts[src_label or "EMPTY"] = text_source_counts.get(src_label or "EMPTY", 0) + 1
        derived_platform, derived_version = derive_platform_from_report_header(
            text or "",
            fallback_platform=_s(r.platform),
            fallback_version=_i(r.platform_version),
        )
        parsed = parse(text or "", platform=derived_platform or r.platform)
        parsed_variants = parsed.get("gene_mutations_variants") or []
        parsed_fusions  = parsed.get("gene_fusions_list") or []
        extra_v, extra_f = synthesize_from_flags(r, parsed_variants, parsed_fusions)
        if extra_v or extra_f:
            flag_synth_counts["rows_augmented"] += 1
            flag_synth_counts["variants"] += len(extra_v)
            flag_synth_counts["fusions"]  += len(extra_f)
        merged_variants = parsed_variants + extra_v
        merged_fusions  = parsed_fusions  + extra_f
        # Recompute TERT presence based on merged variant set.
        merged_tert_present = any((v.get("gene") == "TERT") for v in merged_variants)
        merged_tert_variant = parsed.get("tert_promoter_variant")
        if not merged_tert_variant and merged_tert_present:
            merged_tert_variant = "OTHER"
        # Status downgrades caught by flags: if parser said Negative but flag is True, prefer Positive.
        gms = parsed.get("gene_mutations_status")
        if extra_v and (gms in (None, "", "Negative")):
            gms = "Positive"
        gfs = parsed.get("gene_fusions_status")
        if extra_f and (gfs in (None, "", "Negative")):
            gfs = "Positive"
        rows.append({
            "research_id": _s(r.research_id),
            "molecular_episode_id": _i(r.molecular_episode_id),
            "test_date_native": None if pd.isna(r.test_date_native) else r.test_date_native,
            "resolved_test_date": _s(r.resolved_test_date),
            "platform": derived_platform or _s(r.platform),
            "platform_raw": _s(r.platform_raw, 200),
            "platform_version": derived_version,
            "bethesda_category": _i(r.bethesda_category),
            "specimen_site_normalized": _s(r.specimen_site_normalized),
            "linked_fna_episode_id": _s(r.linked_fna_episode_id),
            "linked_nodule_id": _i(r.linked_nodule_id),
            "linked_surgery_episode_id": _i(r.linked_surgery_episode_id),
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
            "gene_mutations_variants_json": json.dumps(variants_to_struct_list(merged_variants)),
            "gene_fusions_list_json": json.dumps(fusions_to_struct_list(merged_fusions)),
            "tert_present": bool(merged_tert_present) if (merged_variants or extra_v) else parsed.get("tert_present"),
            "tert_promoter_variant": merged_tert_variant,
            "afirma_braf_result": parsed.get("afirma_braf_result"),
            "afirma_mtc_result": parsed.get("afirma_mtc_result"),
            "afirma_tert_c228t_result": parsed.get("afirma_tert_c228t_result"),
            "afirma_tert_c250t_result": parsed.get("afirma_tert_c250t_result"),
            "afirma_retptc_result": parsed.get("afirma_retptc_result"),
            "braf_flag":             _b(r.braf_flag),
            "braf_variant":          _norm_str(r.braf_variant),
            "ras_flag":              _b(r.ras_flag),
            "ras_subtype":           _norm_str(r.ras_subtype),
            "ret_flag":              _b(r.ret_flag),
            "ret_fusion_flag":       _b(r.ret_fusion_flag),
            "tert_flag":             _b(r.tert_flag),
            "ntrk_flag":             _b(r.ntrk_flag),
            "eif1ax_flag":           _b(r.eif1ax_flag),
            "tp53_flag":             _b(r.tp53_flag),
            "pax8_pparg_flag":       _b(r.pax8_pparg_flag),
            "cna_flag":              _b(r.cna_flag),
            "fusion_flag":           _b(r.fusion_flag),
            "loh_flag":              _b(r.loh_flag),
            "alk_flag":              _b(r.alk_flag),
            "high_risk_marker_flag": _b(r.high_risk_marker_flag),
            "inadequate_flag":       _b(r.inadequate_flag),
            "cancelled_flag":        _b(r.cancelled_flag),
            "overall_result_class": (
                parsed.get("overall_result_class")
                or parsed.get("overall_result_class_inferred")
                or _norm_str(r.overall_result_class)
            ),
            "report_text_ref": f"molecular_test_episode_v2#{_i(r.molecular_episode_id)}",
            "report_text_source": src_label or None,
            "report_text_length": int(len(text)) if text else 0,
            "report_source_table": _s(r.source_table),
            "ingestion_source": _s(r.ingestion_source),
            "adjudication_status": _s(r.adjudication_status),
            "molecular_confidence": _f(r.molecular_confidence),
        })

    print("\ntext_source distribution (which column the parsed text came from):")
    for k, v in sorted(text_source_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {k or 'EMPTY':40s} {v:>5}")
    print(
        f"\nstructured-flag synthesis: rows_augmented={flag_synth_counts['rows_augmented']:,}  "
        f"variants_added={flag_synth_counts['variants']:,}  fusions_added={flag_synth_counts['fusions']:,}"
    )

    df = pd.DataFrame(rows)
    print(f"\nstaging dataframe shape: {df.shape}")

    con.register("staging", df)
    con.execute(
        """
        INSERT INTO molecular_genetics_test_v2
        SELECT
            research_id,
            molecular_episode_id,
            test_date_native,
            resolved_test_date,
            platform, platform_raw, platform_version,
            bethesda_category, specimen_site_normalized,
            linked_fna_episode_id, linked_nodule_id, linked_surgery_episode_id,
            parser, parse_status, n_fields_parsed,
            test_result_summary, rom_descriptor, rom_percent_raw,
            rom_percent_low, rom_percent_high, rom_percent_point, rom_description,
            specimen_adequacy_raw, specimen_adequacy_norm,
            gene_mutations_raw, gene_mutations_status,
            gene_fusions_raw, gene_fusions_status,
            cna_raw, cna_status,
            gep_raw, gep_status, gep_detail,
            parathyroid_raw, parathyroid_status,
            medullary_raw, medullary_status,
            CAST(from_json(gene_mutations_variants_json,
                 '[{"gene":"VARCHAR","protein":"VARCHAR","cdna":"VARCHAR","af_pct":"INTEGER","source_call":"VARCHAR"}]')
                 AS STRUCT(gene VARCHAR, protein VARCHAR, cdna VARCHAR, af_pct INTEGER, source_call VARCHAR)[]),
            CAST(from_json(gene_fusions_list_json,
                 '[{"gene1":"VARCHAR","gene2":"VARCHAR","source_call":"VARCHAR"}]')
                 AS STRUCT(gene1 VARCHAR, gene2 VARCHAR, source_call VARCHAR)[]),
            tert_present, tert_promoter_variant,
            afirma_braf_result, afirma_mtc_result, afirma_tert_c228t_result,
            afirma_tert_c250t_result, afirma_retptc_result,
            braf_flag, braf_variant, ras_flag, ras_subtype,
            ret_flag, ret_fusion_flag, tert_flag, ntrk_flag, eif1ax_flag,
            tp53_flag, pax8_pparg_flag, cna_flag, fusion_flag, loh_flag,
            alk_flag, high_risk_marker_flag, inadequate_flag, cancelled_flag,
            overall_result_class,
            report_text_ref, report_text_source, report_text_length,
            report_source_table, ingestion_source, adjudication_status, molecular_confidence,
            current_timestamp, ?
        FROM staging
        """,
        [BUILDER_VERSION],
    )
    n = con.execute("SELECT COUNT(*) FROM molecular_genetics_test_v2").fetchone()[0]
    print(f"\nmolecular_genetics_test_v2 inserted rows: {n:,}")

    print("\nCreating molecular_genetics_from_notes_v2 ...")
    con.execute("DROP TABLE IF EXISTS molecular_genetics_from_notes_v2")
    con.execute(
        f"""
        CREATE TABLE molecular_genetics_from_notes_v2 AS
        SELECT
            CAST(research_id AS VARCHAR)             AS research_id,
            note_row_id,
            CAST(note_date AS VARCHAR)               AS note_date,
            note_type,
            entity_type,
            entity_value_raw,
            entity_value_norm,
            present_or_negated,
            confidence,
            confidence_score,
            evidence_span,
            evidence_start,
            evidence_end,
            extraction_method,
            extractor_name,
            extractor_version,
            llm_model,
            llm_provider,
            llm_prompt_version,
            extraction_run_id,
            extracted_at,
            verification_status,
            verification_step,
            entity_domain,
            episode_id                               AS source_episode_id,
            CAST(NULL AS VARCHAR)                    AS linked_test_episode_id,
            current_timestamp                        AS built_at,
            '{BUILDER_VERSION}'                      AS builder_version
        FROM note_entities_genetics
        """
    )
    n_notes = con.execute("SELECT COUNT(*) FROM molecular_genetics_from_notes_v2").fetchone()[0]
    print(f"molecular_genetics_from_notes_v2 rows: {n_notes:,}")

    print("\nCreating flat views ...")
    con.execute(
        """
        CREATE OR REPLACE VIEW molecular_variant_flat_v2 AS
        SELECT
            t.research_id,
            t.molecular_episode_id,
            t.test_date_native,
            t.platform,
            v.gene           AS gene,
            v.protein        AS protein,
            v.cdna           AS cdna,
            v.af_pct         AS af_pct,
            v.source_call    AS source_call,
            t.gene_mutations_status AS mutations_status_for_test,
            t.tert_present,
            t.rom_percent_point,
            t.rom_descriptor
        FROM molecular_genetics_test_v2 AS t,
             UNNEST(t.gene_mutations_variants) AS u(v)
        WHERE t.gene_mutations_variants IS NOT NULL
          AND len(t.gene_mutations_variants) > 0
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW molecular_fusion_flat_v2 AS
        SELECT
            t.research_id,
            t.molecular_episode_id,
            t.test_date_native,
            t.platform,
            f.gene1                              AS gene1,
            f.gene2                              AS gene2,
            f.gene1 || '-' || f.gene2            AS fusion_pair,
            f.source_call                        AS source_call,
            t.gene_fusions_status                AS fusions_status_for_test,
            t.rom_percent_point,
            t.rom_descriptor
        FROM molecular_genetics_test_v2 AS t,
             UNNEST(t.gene_fusions_list) AS u(f)
        WHERE t.gene_fusions_list IS NOT NULL
          AND len(t.gene_fusions_list) > 0
        """
    )

    n_var = con.execute("SELECT COUNT(*) FROM molecular_variant_flat_v2").fetchone()[0]
    n_fus = con.execute("SELECT COUNT(*) FROM molecular_fusion_flat_v2").fetchone()[0]
    print(f"molecular_variant_flat_v2 rows: {n_var:,}")
    print(f"molecular_fusion_flat_v2  rows: {n_fus:,}")


if __name__ == "__main__":
    main()
