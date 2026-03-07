#!/usr/bin/env python3
"""
06_advanced_extraction.py — Phase 5

Focused advanced extraction:
  1. Nuclear med impression → metastasis sites, RAI avidity
  2. Pathology excerpt → mutation keyword flags (BRAF, RAS, RET, TERT, NTRK, ALK)
  3. Parathyroid adenoma filter tuning (~94 strict match)
  4. US impression → suspicious LN, vascular invasion, substernal flags
  5. Updates is_parathyroid_adenoma in 05_histology_qa.py-compatible way

All original columns are preserved; only new columns are appended.
"""

from __future__ import annotations

import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"


def _col_exists(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    return col in {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}


def _ensure_col(con: duckdb.DuckDBPyConnection, table: str, col: str, dtype: str) -> None:
    if not _col_exists(con, table, col):
        default = "DEFAULT FALSE" if dtype == "BOOLEAN" else ""
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype} {default}")


# ── 1. Nuclear Med Parsing ───────────────────────────────────────

METASTASIS_PATTERNS: list[tuple[str, str]] = [
    ("lung", r"\b(lung|pulmonary)\b"),
    ("bone", r"\b(bone|osseous|skeletal|femur|rib|spine|vertebr|sclerotic)\b"),
    ("lymph_node", r"\b(lymph\s*node|adenopath|cervical\s*node|neck\s*node|supraclavicular)\b"),
    ("liver", r"\b(liver|hepatic)\b"),
    ("brain", r"\b(brain|cerebr|intracranial)\b"),
    ("mediastin", r"\b(mediast|thorax|chest\s*node)\b"),
]

RAI_AVID_POS = re.compile(
    r"(abnormal\s+(focus|uptake|accumulation))"
    r"|(suspicious\s+(focus|uptake))"
    r"|(avid\s+(lesion|focus|nodule))"
    r"|(increased\s+radiotracer)"
    r"|(metast\w*\s+(disease|uptake|focus))",
    re.IGNORECASE,
)
RAI_AVID_NEG = re.compile(
    r"(no\s+(suspicious|abnormal|evidence\s+of))"
    r"|(no\s+other\s+abnormal)"
    r"|(no\s+significant\s+uptake)"
    r"|(no\s+(scintigraphic|radiotracer)\s+evidence)"
    r"|(negative\s+scan)",
    re.IGNORECASE,
)


def add_nuclear_med_columns(con: duckdb.DuckDBPyConnection) -> None:
    print("  Nuclear med parsing …")
    _ensure_col(con, "nuclear_med", "rai_avid_flag", "VARCHAR")
    _ensure_col(con, "nuclear_med", "metastasis_sites", "VARCHAR")

    rows = con.execute(
        "SELECT rowid, COALESCE(impression_text, '') || ' ' || COALESCE(findings_text, '') "
        "FROM nuclear_med"
    ).fetchall()

    updates: list[tuple[str, str, int]] = []
    for rowid, text in rows:
        text_lower = text.lower()
        sites = []
        for site_name, pat in METASTASIS_PATTERNS:
            if re.search(pat, text_lower):
                neg_ctx = re.search(
                    rf"no\s+.{{0,30}}{pat}", text_lower
                )
                if not neg_ctx:
                    sites.append(site_name)

        if RAI_AVID_POS.search(text):
            avid = "positive"
        elif RAI_AVID_NEG.search(text):
            avid = "negative"
        else:
            avid = "unknown" if text.strip() else None

        updates.append((avid, ",".join(sites) if sites else None, rowid))

    con.executemany(
        "UPDATE nuclear_med SET rai_avid_flag = ?, metastasis_sites = ? WHERE rowid = ?",
        updates,
    )


# ── 2. Mutation Keyword Flags ────────────────────────────────────

MUTATION_KEYWORDS: dict[str, str] = {
    "braf_mutation_mentioned": r"\bBRAF\b",
    "ras_mutation_mentioned": r"\bRAS\b|NRAS|HRAS|KRAS",
    "ret_mutation_mentioned": r"\bRET\b|RET/PTC",
    "tert_mutation_mentioned": r"\bTERT\b",
    "ntrk_mutation_mentioned": r"\bNTRK\b",
    "alk_mutation_mentioned": r"\bALK\b",
}


def add_mutation_flags(con: duckdb.DuckDBPyConnection) -> None:
    print("  Mutation keyword scanning …")
    for col in MUTATION_KEYWORDS:
        _ensure_col(con, "tumor_pathology", col, "BOOLEAN")

    rows = con.execute(
        "SELECT rowid, COALESCE(pathology_excerpt, '') FROM tumor_pathology"
    ).fetchall()

    batch: dict[str, list[tuple[bool, int]]] = {c: [] for c in MUTATION_KEYWORDS}
    for rowid, text in rows:
        for col, pat in MUTATION_KEYWORDS.items():
            batch[col].append((bool(re.search(pat, text, re.IGNORECASE)), rowid))

    for col, vals in batch.items():
        con.executemany(
            f"UPDATE tumor_pathology SET {col} = ? WHERE rowid = ?", vals
        )


# ── 3. Parathyroid Adenoma Tuning ────────────────────────────────

def tune_parathyroid_adenoma(con: duckdb.DuckDBPyConnection) -> None:
    print("  Parathyroid adenoma filter tuning …")
    _ensure_col(con, "parathyroid", "is_parathyroid_adenoma", "BOOLEAN")
    con.execute("""
        UPDATE parathyroid SET is_parathyroid_adenoma = (
            parathyroid_abnormality = 'adenoma'
            AND incidental_gland_excision = 'no'
            AND removal_intent = 'intentional'
        )
    """)


# ── 4. Ultrasound Impression Parsing ─────────────────────────────

US_FLAGS: dict[str, re.Pattern] = {
    "us_suspicious_ln": re.compile(
        r"(suspicious\s+(lymph\s*node|adenopath|cervical\s*node|node))"
        r"|(pathologic\s+(lymph|node))"
        r"|(abnormal\s+(lymph|node))"
        r"|(metastat\w*\s+(lymph|node|adenopath))",
        re.IGNORECASE,
    ),
    "us_vascular_invasion_suspected": re.compile(
        r"(vascular\s+invasion)"
        r"|(extrathyroid\w*\s+extension)"
        r"|(invasion\s+of\s+(strap|trachea|esophag|carotid|jugular))"
        r"|(locally\s+invasive)",
        re.IGNORECASE,
    ),
    "us_substernal_suspected": re.compile(
        r"(substernal)"
        r"|(retrosternal)"
        r"|(extend\w*\s+(into|below)\s+(the\s+)?(thorac|mediast|stern))",
        re.IGNORECASE,
    ),
    "us_calcification_noted": re.compile(
        r"(calcific)"
        r"|(microcalcif)"
        r"|(punctate\s+echogenic\s+foci)"
        r"|(coarse\s+calcif)",
        re.IGNORECASE,
    ),
}


def add_us_impression_flags(con: duckdb.DuckDBPyConnection) -> None:
    print("  Ultrasound impression parsing …")
    for col in US_FLAGS:
        _ensure_col(con, "ultrasound_reports", col, "BOOLEAN")

    rows = con.execute(
        "SELECT rowid, COALESCE(source_us_impression, '') || ' ' || "
        "COALESCE(clinical_impression, '') FROM ultrasound_reports"
    ).fetchall()

    batch: dict[str, list[tuple[bool, int]]] = {c: [] for c in US_FLAGS}
    for rowid, text in rows:
        for col, pat in US_FLAGS.items():
            batch[col].append((bool(pat.search(text)), rowid))

    for col, vals in batch.items():
        con.executemany(
            f"UPDATE ultrasound_reports SET {col} = ? WHERE rowid = ?", vals
        )


# ── Main ─────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 72)
    print("PHASE 5: Advanced focused extraction")
    print("=" * 72)

    con = duckdb.connect(str(DB_PATH))

    add_nuclear_med_columns(con)
    add_mutation_flags(con)
    tune_parathyroid_adenoma(con)
    add_us_impression_flags(con)

    # Verification
    print("\n  Verification:")

    n_rai = con.execute(
        "SELECT rai_avid_flag, COUNT(*) FROM nuclear_med GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    print(f"    nuclear_med.rai_avid_flag: {n_rai}")

    n_met = con.execute(
        "SELECT COUNT(*) FROM nuclear_med WHERE metastasis_sites IS NOT NULL"
    ).fetchone()[0]
    print(f"    nuclear_med rows with metastasis_sites: {n_met}")

    for col in MUTATION_KEYWORDS:
        n = con.execute(
            f"SELECT SUM(CASE WHEN {col} THEN 1 ELSE 0 END) FROM tumor_pathology"
        ).fetchone()[0]
        print(f"    tumor_pathology.{col}: {n}")

    n_para = con.execute(
        "SELECT SUM(CASE WHEN is_parathyroid_adenoma THEN 1 ELSE 0 END) FROM parathyroid"
    ).fetchone()[0]
    print(f"    parathyroid.is_parathyroid_adenoma (tuned): {n_para}")

    for col in US_FLAGS:
        n = con.execute(
            f"SELECT SUM(CASE WHEN {col} THEN 1 ELSE 0 END) FROM ultrasound_reports"
        ).fetchone()[0]
        print(f"    ultrasound_reports.{col}: {n}")

    con.close()
    print("\n" + "=" * 72)
    print("Done.")


if __name__ == "__main__":
    main()
