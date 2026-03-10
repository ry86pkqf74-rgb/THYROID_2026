import pandas as pd
import os
import sys

SOURCE_DIR = "/Users/loganglosser/Downloads/Active Master Files"
FILES = [
    "Thyroid all_Complications 12_1_25.xlsx",
    "THYROSEQ_AFIRMA_12_5.xlsx",
    "Thyroid OP Sheet data.xlsx",
    "FNAs 12_5_2025.xlsx",
    "US Nodules TIRADS 12_1_25.xlsx",
    "Imaging_12_1_25.xlsx",
    "All Diagnoses & synoptic 12_1_2025.xlsx",
    "Notes 12_1_25.xlsx",
]

ID_KEYWORDS = [
    "research", "id", "patient", "mrn", "subject", "record", "case",
    "study", "acct", "account", "number", "num",
]

SEPARATOR = "=" * 100


def detect_id_columns(df):
    candidates = []
    for col in df.columns:
        col_lower = str(col).lower().strip()
        if any(kw in col_lower for kw in ID_KEYWORDS):
            nunique = df[col].nunique()
            total = len(df[col].dropna())
            ratio = nunique / total if total > 0 else 0
            candidates.append((col, nunique, total, f"{ratio:.2%}"))
    return candidates


def detect_freetext_columns(df, sample_n=50, threshold=80):
    freetext = []
    for col in df.columns:
        if df[col].dtype != "object":
            continue
        sample = df[col].dropna().head(sample_n)
        if len(sample) == 0:
            continue
        avg_len = sample.astype(str).str.len().mean()
        max_len = sample.astype(str).str.len().max()
        if avg_len > threshold or max_len > 500:
            freetext.append((col, f"avg_len={avg_len:.0f}", f"max_len={max_len}"))
    return freetext


def inspect_file(filepath):
    fname = os.path.basename(filepath)
    print(f"\n{SEPARATOR}")
    print(f"FILE: {fname}")
    print(SEPARATOR)

    try:
        xls = pd.ExcelFile(filepath, engine="openpyxl")
    except Exception as e:
        print(f"  ERROR opening file: {e}")
        return

    sheet_names = xls.sheet_names
    print(f"Sheet names ({len(sheet_names)}): {sheet_names}\n")

    for sheet in sheet_names:
        print(f"  {'─' * 80}")
        print(f"  SHEET: '{sheet}'")
        print(f"  {'─' * 80}")

        try:
            df = pd.read_excel(xls, sheet_name=sheet, engine="openpyxl")
        except Exception as e:
            print(f"    ERROR reading sheet: {e}")
            continue

        nrows, ncols = df.shape
        print(f"  Shape: {nrows} rows × {ncols} cols\n")

        print("  COLUMNS & DTYPES:")
        for i, (col, dtype) in enumerate(zip(df.columns, df.dtypes)):
            print(f"    [{i:3d}] {str(col):50s}  dtype={dtype}")
        print()

        print("  NULL COUNTS:")
        nulls = df.isnull().sum()
        for col in df.columns:
            nc = nulls[col]
            pct = nc / nrows * 100 if nrows > 0 else 0
            if nc > 0:
                print(f"    {str(col):50s}  {nc:6d} nulls  ({pct:5.1f}%)")
        all_null = nulls.sum()
        if all_null == 0:
            print("    (no nulls)")
        print()

        print("  FIRST 3 ROWS (sample):")
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", 200)
        pd.set_option("display.max_colwidth", 60)
        sample = df.head(3)
        print(sample.to_string(index=True))
        print()

        id_cols = detect_id_columns(df)
        if id_cols:
            print("  POTENTIAL ID/KEY COLUMNS:")
            for col, nunique, total, ratio in id_cols:
                print(f"    → {col}  (unique={nunique}, non-null={total}, uniqueness={ratio})")
        else:
            print("  POTENTIAL ID/KEY COLUMNS: (none detected)")
        print()

        ft_cols = detect_freetext_columns(df)
        if ft_cols:
            print("  FREE-TEXT / NOTES COLUMNS:")
            for col, avg, mx in ft_cols:
                print(f"    → {col}  ({avg}, {mx})")
        else:
            print("  FREE-TEXT / NOTES COLUMNS: (none detected)")
        print()

    xls.close()


if __name__ == "__main__":
    print("THYROID SOURCE FILE INSPECTION")
    print(f"Directory: {SOURCE_DIR}")
    print(f"Files to inspect: {len(FILES)}\n")

    for fname in FILES:
        fpath = os.path.join(SOURCE_DIR, fname)
        if not os.path.isfile(fpath):
            print(f"\n  WARNING: File not found: {fpath}")
            continue
        inspect_file(fpath)

    print(f"\n{SEPARATOR}")
    print("INSPECTION COMPLETE")
    print(SEPARATOR)
