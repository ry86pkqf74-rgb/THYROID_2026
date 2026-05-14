#!/usr/bin/env python3
"""
mig_329 / af_fix_20260514 — Backfill AF% on canonical_molecular_genetics_v2 (BigQuery)
from thyroseq dry-run export with extended extract_af_pct.

Scope (UPDATE) — default platforms:
  ('Afirma', 'NGS_unspecified', 'ThyroSeq') so thyroseq_integration exports work
  correctly.

  Platform-label mismatch (important):
    script-41 (41_ingest_thyroseq_excel.py) tags every exported variant row as
    platform='ThyroSeq' — a vendor constant in that parser — regardless of how
    BigQuery labels the same patient's test (Afirma, NGS_unspecified, etc.).
    mig_329 uses BQ's platform column (not the CSV column) for all filtering,
    and pre-filters the BQ query to only research_ids present in the CSV, so
    Afirma/NGS rows for patients who also appear in the ThyroSeq export are
    found and enriched correctly.

    If --platforms Afirma,NGS_unspecified is used without ThyroSeq and the
    export contains few Afirma/NGS research_ids, coverage will be low (the
    script now warns in that case).  Include ThyroSeq in --platforms for
    maximum AF backfill from a thyroseq_integration export.

Per research_id, selects exactly one CMG row (same priority as ROW_NUMBER pass):
  thyroseq_molecular_enrichment > molecular_testing > manual THYROSEQ insert > else.

Merge rule:
  Start from existing gene_mutations_variants; fill af_pct from export SNV rows where
  BQ had NULL; add CSV-only SNV rows. Apply when new non-null AF count > old.

Staging: pub_workspace.cmg_af_fix_staging_20260514 (dropped after successful MERGE).

Usage:
  .venv/bin/python scripts/mig_329_cmg_af_pct_enrichment_bq.py --export-dir exports/thyroseq_integration_20260514_1127 --dry-run
  .venv/bin/python scripts/mig_329_cmg_af_pct_enrichment_bq.py --export-dir exports/thyroseq_integration_20260514_1127 --apply
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
RUN_ID = "af_fix_20260514"
STAGING_TABLE = "cmg_af_fix_staging_20260514"
# Issue 3 strict slice is only Afirma + NGS_unspecified; thyroseq_integration
# improved-AF rows overwhelmingly map to CMG ThyroSeq — include it by default.
DEFAULT_PLATFORMS = ("Afirma", "NGS_unspecified", "ThyroSeq")
PROJECT = "thyroid-canonical-pub-2026"
CANONICAL_DS = "pub_canonical"
WORKSPACE_DS = "pub_workspace"
ARCHIVE_DS = "pub_archive"
CMG = f"`{PROJECT}.{CANONICAL_DS}.canonical_molecular_genetics_v2`"
STAGING = f"`{PROJECT}.{WORKSPACE_DS}.{STAGING_TABLE}`"


def _platform_list_sql(platforms: tuple[str, ...]) -> str:
    return ", ".join(f"'{p}'" for p in platforms)


def _pick_rows_sql(
    platforms: tuple[str, ...],
    csv_rids: frozenset[str] | None = None,
) -> str:
    """Return SQL that picks ALL CMG rows for the given platforms (and,
    optionally, only for research_ids present in the CSV export).

    NOTE — platform-label mismatch: script-41 dry-run exports tag every
    variant row as platform='ThyroSeq' (a vendor constant in that parser).
    In BigQuery the same patient's row may be labeled Afirma or
    NGS_unspecified.  That is why --platforms must include 'ThyroSeq' when
    running against a thyroseq_integration export; the csv_rids pre-filter
    ensures BQ only fetches rows that actually have CSV data, avoiding a
    silent empty result when strict --platforms Afirma,NGS_unspecified is
    specified without 'ThyroSeq'.

    The old ROW_NUMBER / one-per-research_id logic has been removed: every
    CMG row that matches the platform list (and has a CSV entry) is now
    returned, so a patient with both an Afirma row and a ThyroSeq row will
    have both rows enriched independently.
    """
    pl = _platform_list_sql(platforms)
    rid_clause = ""
    if csv_rids:
        # BQ IN-list limit is 10 000 items; typical run is <2 000 rids.
        rid_list = ", ".join(f"'{r}'" for r in sorted(csv_rids))
        rid_clause = f"AND research_id IN ({rid_list})"
    return f"""
SELECT
  research_id,
  molecular_episode_id,
  platform,
  report_source_table,
  TO_JSON_STRING(gene_mutations_variants) AS gmv_json
FROM {CMG}
WHERE platform IN ({pl})
  AND ingestion_source IS DISTINCT FROM 'retroactive_insert_missing_other_platform'
  {rid_clause}
"""


def _merge_sql(
    platforms: tuple[str, ...],
) -> str:
    pl = _platform_list_sql(platforms)
    return f"""
    MERGE {CMG} T
    USING (
      SELECT
        research_id,
        molecular_episode_id,
        report_source_table,
        STRUCT(
          ARRAY(
            SELECT AS STRUCT
              STRUCT(
                JSON_VALUE(v, '$.gene') AS gene,
                JSON_VALUE(v, '$.protein') AS protein,
                JSON_VALUE(v, '$.cdna') AS cdna,
                SAFE_CAST(JSON_VALUE(v, '$.af_pct') AS INT64) AS af_pct,
                JSON_VALUE(v, '$.source_call') AS source_call
              ) AS element
            FROM UNNEST(JSON_QUERY_ARRAY(s.variants_json)) AS v
          ) AS list
        ) AS new_gmv
      FROM {STAGING} s
    ) S
    ON T.research_id = S.research_id
   AND T.molecular_episode_id IS NOT DISTINCT FROM S.molecular_episode_id
   AND T.report_source_table = S.report_source_table
   AND T.platform IN ({pl})
   AND T.ingestion_source IS DISTINCT FROM 'retroactive_insert_missing_other_platform'
    WHEN MATCHED THEN UPDATE SET
      gene_mutations_variants = S.new_gmv,
      built_at = CURRENT_TIMESTAMP(),
      completeness_pass_run_id = '{RUN_ID}'
    """

_PROT_SPACE_RX = re.compile(r"^p\.\s+")
_PROT_C228_RX = re.compile(r"^p\.\s*C\s*228\s*7\s*$", re.I)
_HGVS_LIKE_RX = re.compile(r"^[A-Z][a-zA-Z]?\d{1,4}[A-Za-z_*][A-Za-z_*0-9]*$")
_TAUTOLOGY_GENES = {
    "BRAF", "NRAS", "KRAS", "HRAS", "TERT", "RET", "TP53", "EIF1AX",
    "NTRK1", "NTRK3", "ALK", "PAX8", "PPARG", "TSHR",
}


def _norm_protein(p: object, gene: str | None = None) -> str | None:
    if p is None or (isinstance(p, float) and pd.isna(p)):
        return None
    s = _PROT_SPACE_RX.sub("p.", str(p)).strip()
    s = re.sub(r"\s+", "", s)
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


def _bq_client():
    from google.cloud import bigquery

    return bigquery.Client(project=PROJECT)


def _run(client, sql: str) -> None:
    client.query(sql).result()


def parse_gmv_json(gmv_json: str | None) -> list[dict]:
    if gmv_json is None or gmv_json == "":
        return []
    d = json.loads(gmv_json)
    if not isinstance(d, dict):
        return []
    out: list[dict] = []
    for it in d.get("list") or []:
        el = it.get("element") or it
        ap = el.get("af_pct")
        if isinstance(ap, float):
            ap = int(round(ap))
        elif isinstance(ap, int):
            pass
        elif isinstance(ap, str) and ap.strip():
            try:
                ap = int(round(float(ap)))
            except ValueError:
                ap = None
        else:
            ap = None
        out.append({
            "gene": el.get("gene"),
            "protein": _norm_protein(el.get("protein"), el.get("gene")),
            "cdna": el.get("cdna"),
            "af_pct": ap,
            "source_call": el.get("source_call"),
        })
    return out


def _variant_key(gene: str | None, protein: str | None) -> tuple[str, str | None]:
    g = (gene or "").strip().upper()
    p = protein
    if p:
        p = p.strip().upper()
    return (g, p)


def csv_snv_variants(long_df: pd.DataFrame, research_id: str) -> list[dict]:
    sub = long_df[long_df["research_id"].astype(str) == str(research_id)]
    sub = sub[sub["variant_class"].astype(str) == "SNV"]
    seen: set[tuple[str, str | None]] = set()
    out: list[dict] = []
    for r in sub.itertuples(index=False):
        gene = r.gene_symbol
        if pd.isna(gene) or str(gene).strip() == "":
            continue
        gene = str(gene).strip()
        prot = _norm_protein(getattr(r, "protein_hgvs", None), gene)
        key = _variant_key(gene, prot)
        if key in seen:
            continue
        seen.add(key)
        cdna = getattr(r, "cdna_hgvs", None)
        if pd.isna(cdna):
            cdna = None
        else:
            cdna = str(cdna).strip() or None
        af = getattr(r, "allele_fraction", None)
        if pd.isna(af):
            af_pct = None
        else:
            v = float(af)
            if 0 <= v <= 1.0:
                af_pct = int(round(v * 100.0))
            else:
                af_pct = int(round(v))
        out.append({
            "gene": gene,
            "protein": prot,
            "cdna": cdna,
            "af_pct": af_pct,
            "source_call": "parser_enrichment",
        })
    return out


def merge_variants(bq_vars: list[dict], csv_vars: list[dict]) -> tuple[list[dict], bool]:
    """Return merged list and whether non-null AF count increased."""
    def _norm_gene(g: object) -> str:
        if g is None or (isinstance(g, float) and pd.isna(g)):
            return ""
        return str(g).strip().upper()

    old_n = sum(1 for v in bq_vars if v.get("af_pct") is not None)
    by_key: dict[tuple[str, str | None], dict] = {}
    order: list[tuple[str, str | None]] = []
    for v in bq_vars:
        g = v.get("gene")
        p = v.get("protein")
        k = _variant_key(str(g) if g else "", p)
        if k not in by_key:
            order.append(k)
        by_key[k] = {
            "gene": g,
            "protein": p,
            "cdna": v.get("cdna"),
            "af_pct": v.get("af_pct"),
            "source_call": v.get("source_call"),
        }

    def _apply_af_to_key(k: tuple[str, str | None], af: int, src_csv: dict) -> bool:
        cur = by_key.get(k)
        if not cur:
            return False
        if cur.get("af_pct") is not None:
            return False
        cur["af_pct"] = af
        cur["source_call"] = "parser_enrichment"
        if not cur.get("protein") and src_csv.get("protein"):
            cur["protein"] = src_csv["protein"]
        if not cur.get("cdna") and src_csv.get("cdna"):
            cur["cdna"] = src_csv["cdna"]
        return True

    changed = False
    for cv in csv_vars:
        if cv.get("af_pct") is None:
            continue
        gcsv = _norm_gene(cv.get("gene"))
        if not gcsv:
            continue
        ck = _variant_key(cv["gene"], cv.get("protein"))
        if ck in by_key and _apply_af_to_key(ck, cv["af_pct"], cv):
            changed = True
            continue
        # Gene-level fallback when CSV omits HGVS but gene matches a single AF-null slot
        candidates = [
            k for k in order
            if k[0] == gcsv and by_key[k].get("af_pct") is None
        ]
        if len(candidates) == 1:
            if _apply_af_to_key(candidates[0], cv["af_pct"], cv):
                changed = True
            continue
        if not candidates:
            if cv.get("gene") and (
                cv.get("protein") or cv.get("cdna")
            ):
                k_new = _variant_key(cv["gene"], cv.get("protein"))
                if k_new not in by_key:
                    by_key[k_new] = dict(cv)
                    order.append(k_new)
                    changed = True

    merged = [by_key[k] for k in order]
    new_n = sum(1 for v in merged if v.get("af_pct") is not None)
    improved = new_n > old_n and changed
    return merged, improved


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--export-dir",
        type=Path,
        default=REPO_ROOT / "exports/thyroseq_integration_20260514_1127",
        help="Directory with molecular_variant_long.csv",
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument(
        "--platforms",
        type=str,
        default=",".join(DEFAULT_PLATFORMS),
        help="Comma-separated CMG platform values to update (default: Afirma,NGS_unspecified,ThyroSeq)",
    )
    ap.add_argument(
        "--snapshot",
        action="store_true",
        help=f"Before MERGE, archive touched rids to {ARCHIVE_DS}.canonical_molecular_genetics_v2_pre_{RUN_ID}",
    )
    args = ap.parse_args()
    if args.dry_run == args.apply:
        print("Specify exactly one of --dry-run or --apply", file=sys.stderr)
        sys.exit(2)

    platforms = tuple(p.strip() for p in args.platforms.split(",") if p.strip())
    if not platforms:
        print("--platforms must list at least one platform", file=sys.stderr)
        sys.exit(2)

    exp = args.export_dir.resolve()
    long_path = exp / "molecular_variant_long.csv"
    if not long_path.is_file():
        print(f"Missing {long_path}", file=sys.stderr)
        sys.exit(1)

    long_df = pd.read_csv(long_path)
    client = _bq_client()
    # Pre-filter: collect research_ids that have at least one SNV variant in
    # the CSV so the BQ query skips rows that can never improve.  This also
    # fixes the platform-label mismatch: Afirma/NGS rows for patients whose
    # CSV entry is labeled ThyroSeq are now found correctly.
    csv_all_rids: frozenset[str] = frozenset(
        long_df["research_id"].astype(str).unique()
    )
    csv_af_rids: frozenset[str] = frozenset(
        long_df.loc[
            (long_df["variant_class"].astype(str) == "SNV")
            & long_df["allele_fraction"].notna(),
            "research_id",
        ]
        .astype(str)
        .unique()
    )
    print(
        f"[mig_329] CSV rids total={len(csv_all_rids)}  with SNV+AF={len(csv_af_rids)}"
    )
    if csv_af_rids and not any(p == "ThyroSeq" for p in platforms):
        # Warn when the export is ThyroSeq-labelled but platforms omit ThyroSeq.
        # The pre-filter still works — BQ rows are looked up by research_id,
        # not by CSV platform label — but coverage may be limited if few
        # Afirma/NGS research_ids share rids with the ThyroSeq export.
        print(
            "[mig_329] WARNING: --platforms does not include 'ThyroSeq' but the "
            "CSV export originates from script-41 which labels all rows as "
            "ThyroSeq.  If few Afirma/NGS_unspecified patients appear in this "
            "export, very few rows will be updated.  Add ThyroSeq to --platforms "
            "to maximise AF coverage (BQ platform filter still restricts which "
            "rows are written).",
        )
    bq_rows = list(client.query(_pick_rows_sql(platforms, csv_af_rids)).result())
    print(f"[mig_329] platforms={platforms}  Picked CMG rows: {len(bq_rows)}")

    updates: list[dict] = []
    stats = {"examined": 0, "eligible_merge": 0, "skip_no_gain": 0}
    for row in bq_rows:
        stats["examined"] += 1
        rid = str(row.research_id)
        bq_vars = parse_gmv_json(row.gmv_json)
        csv_vars = csv_snv_variants(long_df, rid)
        merged, ok = merge_variants(bq_vars, csv_vars)
        if not ok:
            stats["skip_no_gain"] += 1
            continue
        stats["eligible_merge"] += 1
        payload = []
        for v in merged:
            payload.append({
                "gene": v.get("gene"),
                "protein": v.get("protein"),
                "cdna": v.get("cdna"),
                "af_pct": v.get("af_pct"),
                "source_call": v.get("source_call"),
            })
        updates.append({
            "research_id": rid,
            "molecular_episode_id": row.molecular_episode_id,
            "report_source_table": row.report_source_table,
            "variants_json": json.dumps(payload),
        })

    print(json.dumps(stats, indent=2))
    print(f"[mig_329] Rows to update: {len(updates)}")

    if args.dry_run:
        print("[mig_329] dry-run: no BigQuery writes")
        return

    arch = f"`{PROJECT}.{ARCHIVE_DS}.canonical_molecular_genetics_v2_pre_{RUN_ID}`"
    if args.snapshot and updates:
        pl = _platform_list_sql(platforms)
        rids = ", ".join(f"'{u['research_id']}'" for u in updates)
        snap_sql = f"""
        CREATE OR REPLACE TABLE {arch} AS
        SELECT *
        FROM {CMG}
        WHERE research_id IN ({rids})
          AND platform IN ({pl})
          AND ingestion_source IS DISTINCT FROM 'retroactive_insert_missing_other_platform'
        """
        print(f"[mig_329] snapshot → {arch}")
        _run(client, snap_sql)

    ddl = f"""
    CREATE OR REPLACE TABLE {STAGING} (
      research_id STRING NOT NULL,
      molecular_episode_id INT64,
      report_source_table STRING NOT NULL,
      variants_json STRING NOT NULL
    )
    """
    _run(client, ddl)
    if updates:
        from google.cloud import bigquery

        job = client.load_table_from_json(
            updates,
            f"{PROJECT}.{WORKSPACE_DS}.{STAGING_TABLE}",
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=[
                    bigquery.SchemaField("research_id", "STRING"),
                    bigquery.SchemaField("molecular_episode_id", "INTEGER"),
                    bigquery.SchemaField("report_source_table", "STRING"),
                    bigquery.SchemaField("variants_json", "STRING"),
                ],
            ),
        )
        job.result()

    merge_sql = _merge_sql(platforms)
    print("[mig_329] MERGE …")
    _run(client, merge_sql)

    _run(client, f"DROP TABLE IF EXISTS {STAGING}")
    out_path = REPO_ROOT / "scripts" / "output" / f"mig_329_{RUN_ID}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps({"platforms": platforms, "stats": stats, "n_updated": len(updates)}, indent=2)
    )
    print(f"[mig_329] wrote {out_path}")


if __name__ == "__main__":
    main()
