#!/usr/bin/env python3
"""
01_ingest_all_files.py — Ingest raw Excel → standardized Parquet

Thyroid Cancer Research Lakehouse
Handles: column standardization, research_id unification,
         wide-to-long melts for lab and nuclear med data.
"""

import polars as pl
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "raw"
PROCESSED = ROOT / "processed"
PROCESSED.mkdir(exist_ok=True)

RESEARCH_ID_ALIASES = {
    "research_id",
    "research_id_number",
    "researchid",
    "record_id",
}

PHI_COLUMNS = {
    "patient_first_nm", "patient_last_nm", "patient_id",
    "empi_nbr", "euh_mrn", "tec_mrn", "dob", "date_of_birth",
    "surgeon",
}


def standardize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Lowercase snake_case all columns; unify research_id variants."""
    rename = {}
    for col in df.columns:
        clean = re.sub(r"[^\w]+", "_", col.strip()).lower().strip("_")
        clean = re.sub(r"_+", "_", clean)
        if clean in RESEARCH_ID_ALIASES:
            clean = "research_id"
        rename[col] = clean

    seen: dict[str, int] = {}
    final: dict[str, str] = {}
    for old, new in rename.items():
        if new in seen:
            seen[new] += 1
            final[old] = f"{new}_{seen[new]}"
        else:
            seen[new] = 0
            final[old] = new
    return df.rename(final)


def cast_research_id(df: pl.DataFrame) -> pl.DataFrame:
    """Cast research_id to clean string (strip whitespace, trailing .0)."""
    if "research_id" not in df.columns:
        return df
    return df.with_columns(
        pl.col("research_id")
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(r"\.0$", "")
        .alias("research_id"),
    )


def strip_phi_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Drop any column whose snake_case name matches PHI denylist."""
    to_drop = [c for c in df.columns if c in PHI_COLUMNS]
    if to_drop:
        print(f"      PHI stripped: {to_drop}")
        df = df.drop(to_drop)
    return df


# ── Wide-to-Long Melt Helpers ───────────────────────────────────


def melt_wide_labs(df: pl.DataFrame) -> pl.DataFrame:
    """
    Melt lab{N}_test_name / specimen_collect_dt / result / units → long format.
    Keeps static demographic columns alongside each measurement.
    """
    static = [c for c in ("research_id", "race", "gender", "dob") if c in df.columns]
    pat = re.compile(r"^lab(\d+)_")
    indices = sorted({int(pat.match(c).group(1)) for c in df.columns if pat.match(c)})
    if not indices:
        return df

    frames = []
    for i in indices:
        prefix = f"lab{i}_"
        group = {c: c[len(prefix) :] for c in df.columns if c.startswith(prefix)}
        if not group:
            continue
        avail = [c for c in static + list(group.keys()) if c in df.columns]
        rename_map = {k: v for k, v in group.items() if k in avail}
        sub = df.select(avail).rename(rename_map)
        sub = sub.with_columns(pl.lit(i).cast(pl.Int32).alias("lab_index"))
        frames.append(sub)

    result = pl.concat(frames, how="diagonal_relaxed")
    if "result" in result.columns:
        result = result.filter(
            pl.col("result").is_not_null()
            & (pl.col("result").cast(pl.Utf8) != "None")
            & (pl.col("result").cast(pl.Utf8) != "")
        )
    return result


def melt_nuclear_med(df: pl.DataFrame) -> pl.DataFrame:
    """
    Melt nucmed_{N}_field → long format with scan_index.
    Handles inconsistent fields across scans (diagonal_relaxed).
    """
    pat = re.compile(r"^nucmed_(\d+)(?:_|$)")
    indices = sorted({int(pat.match(c).group(1)) for c in df.columns if pat.match(c)})
    if not indices:
        return df

    frames = []
    for i in indices:
        prefix = f"nucmed_{i}_"
        base = f"nucmed_{i}"
        group: dict[str, str] = {}
        for c in df.columns:
            if c == base:
                group[c] = "scan_present"
            elif c.startswith(prefix):
                group[c] = c[len(prefix) :]
        if not group:
            continue
        avail = [c for c in ["research_id"] + list(group.keys()) if c in df.columns]
        rename_map = {k: v for k, v in group.items() if k in avail}
        sub = df.select(avail).rename(rename_map)
        sub = sub.with_columns(pl.lit(i).cast(pl.Int32).alias("scan_index"))
        frames.append(sub)

    result = pl.concat(frames, how="diagonal_relaxed")
    if "scan_present" in result.columns:
        result = result.filter(
            pl.col("scan_present").is_not_null()
            & (pl.col("scan_present").cast(pl.Utf8) != "None")
        )
    return result


def build_clinical_notes_long(df: pl.DataFrame) -> pl.DataFrame:
    """Unpivot Sheet2 of Notes workbook into long-format clinical notes.

    Expects columns already standardized to snake_case by standardize_columns().
    Returns rows: research_id, note_type, note_index, note_text, source_sheet, source_column.
    """
    if "research_id" not in df.columns:
        return df

    # Candidate note columns in standardized form
    direct_map = {
        "other_history": ("OTHER_HISTORY", None),
        "last_endocrine_fm_note": ("ENDOCRINE_FM", None),
        "other_notes": ("OTHER_NOTES", None),
        "death": ("DEATH", None),
        "ed_note_1": ("ED_NOTE", 1),
        "ed_note_2": ("ED_NOTE", 2),
    }

    rows = []

    # direct columns
    for col, (nt, ni) in direct_map.items():
        if col in df.columns:
            rows.append(
                df.select(
                    [
                        pl.col("research_id"),
                        pl.lit(nt).alias("note_type"),
                        (pl.lit(ni).cast(pl.Int32) if ni is not None else pl.lit(None).cast(pl.Int32)).alias("note_index"),
                        pl.col(col).cast(pl.Utf8).alias("note_text"),
                        pl.lit("Sheet2").alias("source_sheet"),
                        pl.lit(col).alias("source_column"),
                    ]
                )
            )

    # repeated note families
    def add_family(prefix: str, note_type: str, max_n: int = 4):
        for i in range(1, max_n + 1):
            c = f"{prefix}{i}"
            if c in df.columns:
                rows.append(
                    df.select(
                        [
                            pl.col("research_id"),
                            pl.lit(note_type).alias("note_type"),
                            pl.lit(i).cast(pl.Int32).alias("note_index"),
                            pl.col(c).cast(pl.Utf8).alias("note_text"),
                            pl.lit("Sheet2").alias("source_sheet"),
                            pl.lit(c).alias("source_column"),
                        ]
                    )
                )

    add_family("h_p_", "HP", 4)
    add_family("op_note_", "OPNOTE", 4)
    add_family("dc_sum_", "DC_SUM", 4)

    if not rows:
        return df

    out = pl.concat(rows, how="diagonal_relaxed")
    out = cast_research_id(out)

    # Drop empty note_text
    out = out.filter(
        pl.col("note_text").is_not_null()
        & (pl.col("note_text").str.strip_chars() != "")
        & (pl.col("research_id").is_not_null())
        & (pl.col("research_id") != "")
    )
    return out


# ── File Manifest ────────────────────────────────────────────────
# (filename, table_name, sheet_name_or_None, melt_kind_or_None)

FILE_MAP = [
    (
        "THyroid Sizes, Stanardized_12_2_25.xlsx",
        "thyroid_sizes",
        None,
        None,
    ),
    (
        "Nuclear_Med_final.xlsx",
        "nuclear_med",
        None,
        "nucmed",
    ),
    (
        "FINAL_UPDATE_TumorPath_12_8_CLEANED.xlsx",
        "tumor_pathology",
        None,
        None,
    ),
    (
        "FINAL_UPDATE_BenignPath_12_8_WithText.xlsx",
        "benign_pathology",
        None,
        None,
    ),
    (
        "Thyroid_Weight_Data_12_2_25.xlsx",
        "thyroid_weights",
        None,
        None,
    ),
    (
        "anti_thyroglobulin_antibody_wide_by_research_id_split.xlsx",
        "anti_thyroglobulin_labs",
        None,
        "labs",
    ),
    (
        "thyroglobulin_wide_by_research_id_split.xlsx",
        "thyroglobulin_labs",
        None,
        "labs",
    ),
    (
        "FNAs_Rescored_Long_Format.xlsx",
        "fna_cytology",
        None,
        None,
    ),
    (
        "Frozen sectin parsed.xlsx",
        "frozen_sections",
        None,
        None,
    ),
    (
        "COMPLETE_MULTI_SHEET_ULTRASOUND_REPORTS.xlsx",
        "ultrasound_reports",
        "All_Ultrasound_Reports",
        None,
    ),
    (
        "CT_thyroid_extraction_FINAL_11_20_25.xlsx",
        "ct_imaging",
        "All_Data",
        None,
    ),
    (
        "mri_extraction__FINAL_11_20_25.xlsx",
        "mri_imaging",
        "All_Data",
        None,
    ),
    (
        "Nuclear_Med_final.xlsx",
        "nuclear_med",
        None,
        "nucmed",
    ),
    (
        "parathyroid_notes_intent.xlsx",
        "parathyroid",
        None,
        None,
    ),
    (
        "FNAs 12_5_2025.xlsx",
        "fnas_detailed",
        "FNA Bethesda",
        None,
    ),
    (
        "US Nodules TIRADS 12_1_25.xlsx",
        "us_nodules_tirads",
        "US-1_Nodules_ TIRADS",
        None,
    ),
    (
        "All Diagnoses & synoptic 12_1_2025.xlsx",
        "synoptic_pathology",
        "synoptics + Dx merged",
        None,
    ),
    (
        "Imaging_12_1_25.xlsx",
        "imaging_reports",
        "Thyroid US",
        None,
    ),
    (
        "Notes 12_1_25.xlsx",
        "clinical_notes",
        "Sheet1",
        None,
    ),
    (
        "Notes 12_1_25.xlsx",
        "clinical_notes_long",
        "Sheet2",
        "notes_long",
    ),
]


def main() -> None:
    print("=" * 70)
    print("  THYROID RESEARCH LAKEHOUSE — STEP 1: INGEST RAW FILES")
    print("=" * 70)

    success, fail = 0, 0

    for fname, table, sheet, melt in FILE_MAP:
        path = RAW / fname
        if not path.exists():
            print(f"\n  ⚠️  MISSING: {fname}")
            fail += 1
            continue

        print(f"\n  📄  {fname}")
        print(f"      → {table}")

        try:
            kw = {"sheet_name": sheet} if sheet else {}
            df = pl.read_excel(str(path), **kw)
            print(f"      raw: {df.shape[0]:>7,} rows × {df.shape[1]:>3} cols")

            df = standardize_columns(df)
            df = cast_research_id(df)
            df = strip_phi_columns(df)

            if melt == "labs":
                df = melt_wide_labs(df)
                print(f"      melted → {df.shape[0]:>7,} rows × {df.shape[1]:>3} cols")
            elif melt == "nucmed":
                df = melt_nuclear_med(df)
                print(f"      melted → {df.shape[0]:>7,} rows × {df.shape[1]:>3} cols")
            elif melt == "notes_long":
                df = build_clinical_notes_long(df)
                print(f"      melted → {df.shape[0]:>7,} rows × {df.shape[1]:>3} cols")

            out = PROCESSED / f"{table}.parquet"
            df.write_parquet(out)
            size_mb = out.stat().st_size / (1024 * 1024)
            print(f"      ✅  {out.name}  ({size_mb:.2f} MB)")
            success += 1

        except Exception as exc:
            print(f"      ❌  ERROR: {exc}")
            import traceback

            traceback.print_exc()
            fail += 1

    print(f"\nDone. Success: {success} | Fail: {fail}")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
