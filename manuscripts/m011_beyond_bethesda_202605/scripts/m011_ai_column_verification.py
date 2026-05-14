#!/usr/bin/env python3
"""
M011 — Google Cloud AI column-sourcing verification.
Independent second pass (beyond the human-ratified verification registry): asks
Vertex AI Gemini whether each M011 manuscript column is the correct source for how
the manuscript uses it, given the BigQuery data-dictionary description.

Writes tables/m011_ai_column_verification.csv.
Usage: python3 m011_ai_column_verification.py
Requires: google-genai, google-cloud-bigquery, pandas; ADC with Vertex AI access.
"""
import os, pandas as pd
from google.cloud import bigquery
from google import genai

PROJECT = "thyroid-canonical-pub-2026"
OUT = os.path.join(os.path.dirname(__file__), "..", "tables")
bq = bigquery.Client(project=PROJECT)
ai = genai.Client(vertexai=True, project=PROJECT, location="global")
MODEL = "gemini-2.5-flash"

audit = bq.query(
    "SELECT m011_column, source_table, sot_status, competing_source_flag, linked_issue, note "
    "FROM `thyroid-canonical-pub-2026.pub_workspace.m011_column_source_audit`").to_dataframe()

# data dictionary context (defensive: pull whatever description-like columns exist)
dd_cols = [c.name for c in bq.get_table(f"{PROJECT}.pub_canonical.data_dictionary_v279").schema]
desc_col = next((c for c in dd_cols if "desc" in c.lower() or "definition" in c.lower() or "note" in c.lower()), None)
name_col = next((c for c in dd_cols if c.lower() in ("column_name", "field_name", "field", "column")), None)
tbl_col  = next((c for c in dd_cols if "table" in c.lower()), None)
dd = pd.DataFrame()
if desc_col and name_col:
    sel = f"{tbl_col + ', ' if tbl_col else ''}{name_col}, {desc_col}"
    dd = bq.query(f"SELECT {sel} FROM `{PROJECT}.pub_canonical.data_dictionary_v279`").to_dataframe()

def dd_context(source_table, m011_column):
    if dd.empty:
        return "(data dictionary unavailable)"
    cols = [t.strip() for t in m011_column.replace("/", " ").split() if t.islower() or "_" in t]
    hits = dd[dd[name_col].astype(str).str.lower().isin([c.lower() for c in cols])]
    if hits.empty:
        return "(no matching data-dictionary entry)"
    return " | ".join(f"{r[name_col]}: {str(r[desc_col])[:200]}" for _, r in hits.iterrows())

rows = []
for _, a in audit.iterrows():
    ctx = dd_context(a.source_table, a.m011_column)
    prompt = (
        "You are auditing column sourcing in a thyroid-cancer surgical-registry research "
        "manuscript (M011, 'Beyond Bethesda?'). Decide whether the chosen column is the "
        "correct source for the stated manuscript use.\n\n"
        f"Column(s): {a.m011_column}\nSource table: {a.source_table}\n"
        f"Manuscript use / analyst note: {a.note}\n"
        f"Data-dictionary context: {ctx}\n\n"
        "Reply on ONE line: 'CONFIRM — <=20-word reason' or 'REVIEW — <=20-word reason'.")
    try:
        v = ai.models.generate_content(model=MODEL, contents=prompt).text.strip().replace("\n", " ")
    except Exception as e:
        v = f"ERROR — {repr(e)[:120]}"
    rows.append(dict(m011_column=a.m011_column, source_table=a.source_table,
                     registry_status=a.sot_status, competing_source_flag=bool(a.competing_source_flag),
                     linked_issue=a.linked_issue, ai_model=MODEL, ai_verdict=v))
    print(f"{('CONFIRM' if v.startswith('CONFIRM') else 'REVIEW ' if v.startswith('REVIEW') else 'ERR    ')} | {a.m011_column[:48]:48} | {v[:90]}")

df = pd.DataFrame(rows)
df.to_csv(os.path.join(OUT, "m011_ai_column_verification.csv"), index=False)
print(f"\n{(df.ai_verdict.str.startswith('CONFIRM')).sum()}/{len(df)} CONFIRM. Written to "
      + os.path.abspath(os.path.join(OUT, "m011_ai_column_verification.csv")))
