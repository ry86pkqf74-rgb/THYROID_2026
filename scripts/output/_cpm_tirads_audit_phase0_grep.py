#!/usr/bin/env python3
"""
Phase 0: writer/reader grep for every legacy/v2 TIRADS column on CPM.

Scans:
- scripts/  (Python and .sql)
- sql/      (top-level repo SQL)
- *.py at repo root
- Excludes scripts/output/ (our scratch dir), .venv, .git

Heuristic classification:
- writer: regex hits include INSERT INTO ... (col), UPDATE ... SET col=, ALTER TABLE ... ADD col,
          CREATE TABLE ... (col, ...), or `f"... {col} ..."` inside an INSERT/UPDATE/ALTER block,
          plus assignment patterns like `df["col"] = ...`, `frame[col]=...`, parquet write of col.
- reader: any other reference (SELECT col, WHERE col=..., JOIN ... USING(col), `df["col"]` read).

This is a fuzzy heuristic — not a parser. The agent will eyeball ambiguous cases.

Output: scripts/output/_cpm_tirads_audit_writers_readers.json (and .md)
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

OUT = Path(__file__).resolve().parent
INV_PATH = OUT / "_cpm_tirads_audit_inventory.json"

inventory = json.loads(INV_PATH.read_text())
all_cols = [c["name"] for c in inventory["audit_columns"]] + [
    c["name"] for c in inventory["nlp_columns"]
]

# Search in: scripts/, sql/, repo-root *.py, manuscripts/ (for view-like refs in revision_*).
# Exclude scripts/output (scratch), archive (deprecated), .venv, .git
SEARCH_DIRS = ["scripts", "sql", "manuscripts", "studies", "lakehouse", "utils", "app"]

# Build one big alternation; rg supports very long patterns
pattern = "|".join(rf"\b{re.escape(c)}\b" for c in all_cols)

# Run ripgrep with --json for structured output
cmd = [
    "rg",
    "--no-heading",
    "--with-filename",
    "--line-number",
    "-e",
    pattern,
    *SEARCH_DIRS,
    "--glob",
    "!scripts/output/**",
    "--glob",
    "!scripts/archive/**",
    "--glob",
    "!**/.venv/**",
    "--glob",
    "!**/__pycache__/**",
    "--glob",
    "!**/_*.py",  # exclude scratch helpers prefixed with _
    "--glob",
    "!**/_*.sql",
    "--glob",
    "!**/*.parquet",
    "--glob",
    "!**/*.json",
]

proc = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
if proc.returncode not in (0, 1):
    raise RuntimeError(f"rg failed: {proc.stderr}")

# Parse hits: format is "path:lineno:line"
hits_by_col: dict[str, list[tuple[str, int, str]]] = defaultdict(list)
col_set = set(all_cols)

# Pre-compile per-column regex for hit assignment (line may match multiple)
col_regex = {c: re.compile(rf"\b{re.escape(c)}\b") for c in all_cols}

# Per-column writer signal regex builder
def writer_regex_for(col: str) -> re.Pattern:
    c = re.escape(col)
    parts = [
        rf"\bADD\s+(?:COLUMN\s+)?{c}\b",     # ALTER TABLE ... ADD col TYPE
        rf"\bSET\s+{c}\s*=",                  # UPDATE SET col=
        rf"^\s*{c}\s*=\s*[^=]",               # standalone assignment line (UPDATE SET ... col=...)
        rf",\s*{c}\s*=\s*[^=]",               # multi-col SET col=
        rf"\bAS\s+{c}\b",                     # SQL alias projecting INTO col
        rf"\(\s*[\"']{c}[\"']\s*,",            # ("col", "TYPE") column-spec tuple
        rf"\b{c}\s+(BIGINT|INTEGER|DOUBLE|VARCHAR|BOOLEAN|TEXT|TIMESTAMP|DATE)\b",
        rf"INSERT[^;]*\b{c}\b",
    ]
    return re.compile("|".join(parts), re.IGNORECASE)


writer_regex = {c: writer_regex_for(c) for c in all_cols}

# Process each ripgrep line
for raw in proc.stdout.splitlines():
    m = re.match(r"^([^:]+):(\d+):(.*)$", raw)
    if not m:
        continue
    path, ln, line = m.group(1), int(m.group(2)), m.group(3)
    for c, rgx in col_regex.items():
        if rgx.search(line):
            hits_by_col[c].append((path, ln, line))

# File-level writer detection: any file that does CTAS/INSERT/UPDATE on canonical_patient_master
# is treated as a CPM writer for any column it mentions.
CPM_WRITER_FILE_RGX = re.compile(
    r"(CREATE\s+OR\s+REPLACE\s+TABLE\s+(\w+\.)?canonical_patient_master|"
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+\.)?canonical_patient_master|"
    r"INSERT\s+INTO\s+(\w+\.)?canonical_patient_master|"
    r"UPDATE\s+(\w+\.)?canonical_patient_master|"
    r"ALTER\s+TABLE\s+(\w+\.)?canonical_patient_master|"
    r"COPY\s+(\w+\.)?canonical_patient_master\s+FROM)",
    re.IGNORECASE,
)

cpm_writer_files: set[str] = set()
unique_files: set[str] = set()
for c in all_cols:
    for path, _, _ in hits_by_col.get(c, []):
        unique_files.add(path)
for path in unique_files:
    try:
        text = (REPO / path).read_text(errors="ignore")
    except Exception:
        continue
    if CPM_WRITER_FILE_RGX.search(text):
        cpm_writer_files.add(path)

# For each column, classify each hit as writer or reader
report: dict[str, dict] = {}
for c in all_cols:
    hits = hits_by_col.get(c, [])
    writers: dict[str, int] = defaultdict(int)
    readers: dict[str, int] = defaultdict(int)
    writer_examples: list[str] = []
    reader_examples: list[str] = []
    wrgx = writer_regex[c]
    for path, ln, line in hits:
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith("--"):
            continue
        # Promote line to writer if file is a CPM writer (CTAS/INSERT/UPDATE on CPM)
        # OR the per-line writer pattern matches.
        is_writer = bool(wrgx.search(line)) or (path in cpm_writer_files)
        if is_writer:
            writers[path] += 1
            if len(writer_examples) < 3:
                writer_examples.append(f"{path}:{ln}: {stripped[:140]}")
        else:
            readers[path] += 1
            if len(reader_examples) < 3:
                reader_examples.append(f"{path}:{ln}: {stripped[:140]}")
    report[c] = {
        "n_hits_total": len(hits),
        "n_writer_files": len(writers),
        "n_reader_files": len(readers),
        "writer_files": sorted(writers.keys()),
        "reader_files": sorted(readers.keys()),
        "writer_examples": writer_examples,
        "reader_examples": reader_examples,
    }

(OUT / "_cpm_tirads_audit_writers_readers.json").write_text(
    json.dumps(report, indent=2)
)

# Markdown summary table
def short_files(files: list[str], cap: int = 6) -> str:
    if not files:
        return "(none)"
    files = sorted(files)
    if len(files) <= cap:
        return ", ".join(Path(f).name for f in files)
    return ", ".join(Path(f).name for f in files[:cap]) + f", +{len(files) - cap} more"


lines = ["# CPM TIRADS — Phase 0 writer/reader grep", ""]
lines.append(
    f"Search dirs: {', '.join(SEARCH_DIRS)} | excluded: scripts/output, scripts/archive, "
    f".venv, __pycache__, leading-underscore scratch files"
)
lines.append("")
lines.append("| column | writers (file count) | readers (file count) | writer files | reader files |")
lines.append("|---|---:|---:|---|---|")
for c in all_cols:
    r = report[c]
    lines.append(
        f"| `{c}` | {r['n_writer_files']} | {r['n_reader_files']} | "
        f"{short_files(r['writer_files'])} | {short_files(r['reader_files'])} |"
    )
(OUT / "_cpm_tirads_audit_writers_readers.md").write_text("\n".join(lines) + "\n")

# Also produce a hitlist of orphaned columns (no writers + no readers) and dead columns (writers but no readers)
orphan = [c for c in all_cols if report[c]["n_writer_files"] == 0 and report[c]["n_reader_files"] == 0]
no_writer = [c for c in all_cols if report[c]["n_writer_files"] == 0 and report[c]["n_reader_files"] > 0]
no_reader = [c for c in all_cols if report[c]["n_writer_files"] > 0 and report[c]["n_reader_files"] == 0]

print("Phase 0 grep complete.")
print(f"- columns: {len(all_cols)}")
print(f"- orphaned (no writers, no readers): {len(orphan)}")
print(f"- no_writer (read-only / orphan-write): {len(no_writer)}")
print(f"- no_reader (write-only / dead-read): {len(no_reader)}")
print()
print("Orphaned:")
for c in orphan:
    print(f"  - {c}")
print()
print("No writer (only readers):")
for c in no_writer:
    print(f"  - {c}: readers={report[c]['n_reader_files']}")
print()
print("No reader (only writers):")
for c in no_reader:
    print(f"  - {c}: writers={report[c]['n_writer_files']}")
