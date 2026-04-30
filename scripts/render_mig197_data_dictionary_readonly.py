#!/usr/bin/env python3
"""mig_197 — Export data_dictionary.csv + canonical_methods_footnotes/*.md.

Read-only SELECT against MotherDuck publication DB via connect_locked().
Does not INSERT/UPDATE/DELETE/DDL.

Cf. cursor_prompts/CURSOR_PROMPT_mig197_data_dictionary_refresh_with_cf_annotations_20260430.md
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import PUBLICATION_DB, connect_locked  # noqa: E402

CF_TAG_RE = re.compile(r"CF-[A-Za-z0-9_-]+")
SCRIPT_TAG = "render_mig197_data_dictionary_readonly.py"


def cf_tags_col(notes: object) -> str:
    if notes is None or pd.isna(notes):
        return ""
    txt = str(notes).strip()
    if txt == "":
        return ""
    found = sorted(set(CF_TAG_RE.findall(txt)))
    return "; ".join(found)


def excerpt(s: object, n: int = 280) -> str:
    if s is None or pd.isna(s):
        return ""
    raw = str(s).strip()
    if raw == "":
        return ""
    t = " ".join(raw.split())
    return t[: n - 1] + "…" if len(t) > n else t


def grain_for_table(table: str) -> str:
    if table == "canonical_patient_master":
        return "One row per patient (`research_id`)"
    if table.endswith("_patient_rollup_v1"):
        return "One row per patient (rollup summaries)"
    if table.endswith("_events_v1"):
        return "Event grain (multiple rows per patient possible)"
    if "us_exam" in table or "exam_master" in table:
        return "Exam grain (typically one row per imaging encounter)"
    if "tumor_long" in table or "synoptic_tumor" in table:
        return "Tumor-slot / lesion-adjacent grain"
    return "See table keys in data dictionary"


def fetch_verified_tables(con) -> list[dict]:
    rows = con.execute(
        """
        SELECT table_name, signoff_migration, notes, n_columns_total, priority_tier
        FROM main.canonical_table_signoff_registry_v1
        WHERE schema_name = 'main'
          AND table_status = 'verified'
        ORDER BY table_name
        """
    ).fetchall()
    return [
        {
            "table_name": r[0],
            "signoff_migration": r[1],
            "notes": r[2],
            "n_columns_total": r[3],
            "priority_tier": r[4],
        }
        for r in rows
    ]


def table_has_column(con, table: str, col: str) -> bool:
    n = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_catalog = ?
          AND table_schema = 'main'
          AND table_name = ?
          AND column_name = ?
        """,
        [PUBLICATION_DB, table, col],
    ).fetchone()[0]
    return n > 0


def fetch_row_counts(con, table: str) -> tuple[int | None, int | None, str | None]:
    fq = f'"{PUBLICATION_DB}".main."{table.replace(chr(34), chr(34)+chr(34))}"'
    err: str | None = None
    try:
        total = int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0])
    except Exception as exc:  # BinderException on stale views, etc.
        return None, None, str(exc).split("\n")[0][:220]

    n_pts = None
    if table_has_column(con, table, "research_id"):
        try:
            n_pts = int(
                con.execute(
                    f"SELECT COUNT(DISTINCT research_id) FROM {fq}"
                ).fetchone()[0]
            )
        except Exception:
            n_pts = None
    return total, n_pts, err


def fetch_methods_for_table(con, table: str) -> list[str]:
    rows = con.execute(
        """
        SELECT DISTINCT verification_method
        FROM main.canonical_column_verification_registry_v1
        WHERE schema_name = 'main'
          AND table_name = ?
          AND verification_method IS NOT NULL
          AND TRIM(CAST(verification_method AS VARCHAR)) <> ''
        ORDER BY 1
        """,
        [table],
    ).fetchall()
    return [str(r[0]) for r in rows]


def fetch_key_columns(con, table: str, limit: int = 10) -> list[tuple[str, str]]:
    rows = con.execute(
        """
        SELECT column_name, COALESCE(category, ''),
        ordinal_position,
        CASE WHEN verification_status = 'verified' THEN 0 ELSE 1 END AS pri
        FROM main.canonical_column_verification_registry_v1
        WHERE schema_name = 'main'
          AND table_name = ?
          AND verification_status IN ('verified', 'na')
        ORDER BY pri, ordinal_position
        LIMIT ?
        """,
        [table, limit + 8],
    ).fetchall()
    out = []
    for col, cat, _, _ in rows:
        desc = cat or ""
        suffix = (
            ""
            if not desc
            else f" — verification category `{desc}` registry seed"
        )
        out.append((col, f"Column `{col}`{suffix}".strip()))
        if len(out) >= limit:
            break
    return out


def cfs_for_table(con, table: str) -> tuple[list[str], list[str]]:
    notes_rows = con.execute(
        """
        SELECT notes FROM main.canonical_column_verification_registry_v1
        WHERE schema_name = 'main'
          AND table_name = ?
          AND notes IS NOT NULL AND TRIM(notes) <> ''
        """,
        [table],
    ).fetchall()
    tags: set[str] = set()
    snippets: list[str] = []
    for (txt,) in notes_rows:
        if not txt:
            continue
        tags.update(CF_TAG_RE.findall(str(txt)))
        if len(snippets) < 12:
            snippets.append(excerpt(str(txt), 220))
    return sorted(tags), snippets


def data_dictionary_sql_select(con):
    sql = rf"""
SELECT
  c.table_schema AS schema_name,
  c.table_name,
  c.column_name,
  c.data_type,
  c.is_nullable,
  r.verification_status,
  r.verification_method,
  r.batch_id,
  r.verified_ts,
  r.notes AS registry_notes
FROM information_schema.columns AS c
INNER JOIN main.canonical_column_verification_registry_v1 AS r
  ON r.schema_name = c.table_schema
 AND r.table_name = c.table_name
 AND r.column_name = c.column_name
INNER JOIN main.canonical_table_signoff_registry_v1 AS ts
  ON ts.schema_name = c.table_schema
 AND ts.table_name = c.table_name
WHERE c.table_catalog = '{PUBLICATION_DB}'
  AND c.table_schema = 'main'
  AND ts.table_status = 'verified'
  AND r.verification_status IN ('verified', 'na')
ORDER BY c.table_name, c.ordinal_position
"""
    return con.execute(sql)


def md_header() -> str:
    return "<!-- READY FOR LOGAN MANUSCRIPT REFINEMENT -->\n\n"


def main() -> int:
    manuscript = REPO_ROOT / "qc_framework_v1" / "manuscript"
    footnotes_dir = manuscript / "canonical_methods_footnotes"
    footnotes_dir.mkdir(parents=True, exist_ok=True)

    con = connect_locked()
    dd_df = data_dictionary_sql_select(con).fetchdf()
    dd_df["cf_tags"] = dd_df["registry_notes"].map(cf_tags_col)
    dd_df["notes_excerpt"] = dd_df["registry_notes"].map(excerpt)
    out_cols = [
        "schema_name",
        "table_name",
        "column_name",
        "data_type",
        "is_nullable",
        "verification_status",
        "verification_method",
        "batch_id",
        "verified_ts",
        "cf_tags",
        "notes_excerpt",
    ]
    csv_path = manuscript / "data_dictionary.csv"
    dd_df[out_cols].to_csv(csv_path, index=False)

    verified = fetch_verified_tables(con)
    n_files = 0
    for t in verified:
        name = t["table_name"]
        total, n_pts, count_err = fetch_row_counts(con, name)
        methods = fetch_methods_for_table(con, name)
        key_cols = fetch_key_columns(con, name)
        cf_tags, cf_snippets = cfs_for_table(con, name)

        methods_bullets = (
            "\n".join(f"- `{m}`" for m in methods[:25])
            or "- *(See column registry verification_method fills.)*"
        )
        keys_bullets = (
            "\n".join(f"- {blurb}" for _, blurb in key_cols)
            or "- *(Key columns intentionally minimal — Logan may expand.)*"
        )
        lim_bullets = ""
        if cf_tags:
            lim_bullets = "\n".join(f"- **`{tg}`:** carry-forward / limitation referenced in registry notes." for tg in cf_tags[:28])
            if lim_bullets and cf_snippets:
                lim_bullets += "\n- **Representative excerpts (verbatim trims):**\n"
                for sn in cf_snippets[:8]:
                    lim_bullets += f"  - {sn}\n"
        else:
            lim_bullets = "- *(No CF-* tokens found in registry notes — limitations may appear in prose only.)*\n"

        purpose = (
            excerpt(t["notes"], 560)
            if t["notes"] is not None and not pd.isna(t["notes"]) and str(t["notes"]).strip()
            else excerpt(
                f"Verified canonical `{name}` in publication DB (`{PUBLICATION_DB}`). "
                "Operational meaning is anchored in Logan verification batches referenced on column rows.",
                560,
            )
        )
        build_stub = excerpt(
            f"Rolling signoff `{t['signoff_migration']}`; Tier `{t['priority_tier']}`. "
            f"Rebuild per latest Path-C batch; derivation scripts referenced alongside "
            "column-level verification_method entries in canonical_column_verification_registry_v1.",
            440,
        )
        rows_line = "**Total rows:** unavailable — MotherDuck could not SCAN this object (recover view DDL / re-deploy)."
        if count_err:
            rows_line += f"\n\n*Binder detail (trimmed):* `{count_err}`"
        if total is not None:
            rows_line = f"**Total rows:** `{total:,}`"

        pts_line = "**Distinct patients:** n/a (no `research_id` grain)"
        if n_pts is not None:
            pts_line = f"**Distinct patients:** `{n_pts:,}`"
        elif count_err:
            pts_line = "**Distinct patients:** unavailable (same COUNT error as row total)"

        md = md_header()
        md += (
            f"# `{name}`\n\n"
            f"**Grain:** {grain_for_table(name)}\n\n"
            f"{rows_line}\n\n"
            f"{pts_line}\n\n"
            f"**Verification status:** verified\n\n"
            f"**Signoff migration:** `{t['signoff_migration'] if t['signoff_migration'] is not None else 'NULL'}`\n\n"
            "## Purpose\n\n"
            f"{purpose}\n\n"
            "## Build pipeline\n\n"
            f"{build_stub}\n\n"
            "## Key columns\n\n"
            f"{keys_bullets}\n\n"
            "## Known limitations\n\n"
            f"{lim_bullets}\n\n"
            "## Verification methods used\n\n"
            f"{methods_bullets}\n\n"
            "---\n"
            f"_Starter generated by `{SCRIPT_TAG}` ({PUBLICATION_DB}). Logan refines voice._\n"
        )

        fp = footnotes_dir / f"{name}.md"
        fp.write_text(md, encoding="utf-8")
        n_files += 1

    readme = manuscript / "canonical_methods_footnotes" / "README.md"
    readme_text = md_header()
    readme_text += """# Canonical methods footnotes — supplementary appendix starters

These Markdown files (**one per `table_status = verified`** canonical table) are scaffolded for citation in Supplementary Methods.

## How to cite

Manuscript Supplement: cite as `canonical_methods_footnotes/<table_name>.md` keyed to the analytic table referenced in prose (e.g. `canonical_survival_followup_v1`).

Do **not** treat machine-generated stubs as immutable — Logan performs final clinical voice edits.

## How to regenerate (Path-C friendly)

Run read-only exporter (MotherDuck token via `motherduck_client.get_token()` / `motherduck.local.toml`; see `_md_connect.connect_locked()`):

```
.venv/bin/python scripts/render_mig197_data_dictionary_readonly.py
```

This refreshes:

- `qc_framework_v1/manuscript/data_dictionary.csv`
- Every `canonical_methods_footnotes/<verified_table>.md`

Companion SQL-only pull (no CSV massaging):

- `qc_framework_v1/manuscript/data_dictionary.sql` — augment with spreadsheet CF tagging if scripting changes.

## Update cadence

Regenerate whenever `canonical_column_verification_registry_v1` or `canonical_table_signoff_registry_v1` changes after a Lane close-out (verification batch).

## Preconditions / caveats

If upstream apply lanes (**mig_185b / mig_186b / mig_187 / mig_188b / …**) lag the registry snapshot, placeholders still reflect whichever rows are CURRENTLY `verified`; check `signoff_migration` per footnote footer for batch provenance.

---

_Author: Logan Glosser <logan.glosser@gmail.com> — starter scaffolding for mig_197._
"""
    readme.write_text(readme_text, encoding="utf-8")

    print(
        f"Wrote data_dictionary.csv rows={len(dd_df)} verified_tables={n_files}",
        flush=True,
    )
    print(f"CSV path: {csv_path}", flush=True)
    print(f"Footnotes dir: {footnotes_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
