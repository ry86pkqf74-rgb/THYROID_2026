"""
Validation Prompt 1: Demographics + Table 1 + AI_CLASSIFY histology standardization.

Demonstrates the full Cortex AI pipeline on the loaded CPM. Saves output as a
markdown report under snowflake_trial/reports/.

Tables are stored as VARIANT $1 columns from CTAS — we project fields via
$1:column_name::TYPE.
"""
import os, json, time
from pathlib import Path
import snowflake.connector
import snowflake.connector.network as _net

REPO_ROOT = Path(__file__).resolve().parents[2]
try:
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env", override=False)
except ImportError:
    pass

PAT = os.environ["SNOWFLAKE_PAT"]
DOTTED = "qcc02515.us-east-1"

_orig = _net.SnowflakeRestful._post_request
def _patched(self, url, headers, body, *args, **kwargs):
    if "/session/v1/login-request" in url:
        try:
            d = json.loads(body) if isinstance(body, str) else json.loads(body.decode())
            d["data"]["ACCOUNT_NAME"] = DOTTED
            if not d["data"].get("TOKEN"):
                d["data"]["TOKEN"] = PAT
            body = json.dumps(d)
        except Exception:
            pass
    return _orig(self, url, headers, body, *args, **kwargs)
_net.SnowflakeRestful._post_request = _patched

ctx = snowflake.connector.connect(
    account="qcc02515", host=f"{DOTTED}.snowflakecomputing.com",
    user="LGLOSSE13", password=PAT,
    authenticator="PROGRAMMATIC_ACCESS_TOKEN",
    warehouse="COMPUTE_WH", database="THYROID_VALIDATION",
    schema="PUBLIC", role="ACCOUNTADMIN")
cur = ctx.cursor()

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/01_demographics_validation.md")
OUT.parent.mkdir(parents=True, exist_ok=True)

def run(label, sql, fetch=True):
    print(f"\n=== {label} ===")
    print(sql.strip().split("\n")[0][:80] + " ...")
    t0 = time.time()
    cur.execute(sql)
    if fetch:
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        print(f"  {len(rows)} rows in {time.time()-t0:.1f}s")
        return cols, rows
    return None, None

report = ["# Snowflake Cortex Validation — Prompt 1: Demographics + Table 1\n"]
report.append(f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
report.append(f"**Source:** THYROID_VALIDATION.PUBLIC.CANONICAL_PATIENT_MASTER (10,871 patients)\n\n---\n")

# 1. Cohort N + malignancy split
cols, rows = run("Cohort overview", """
SELECT
  COUNT(*) AS n_total,
  COUNT_IF($1:is_malignant::BOOLEAN) AS n_malignant,
  COUNT_IF(NOT $1:is_malignant::BOOLEAN) AS n_benign,
  ROUND(100.0 * COUNT_IF($1:is_malignant::BOOLEAN) / COUNT(*), 1) AS pct_malignant
FROM CANONICAL_PATIENT_MASTER
""")
report.append("## Cohort overview\n\n")
report.append("| " + " | ".join(cols) + " |\n")
report.append("| " + " | ".join("---" for _ in cols) + " |\n")
report.append("| " + " | ".join(str(v) for v in rows[0]) + " |\n\n")

# 2. Demographics
cols, rows = run("Age stats", """
SELECT
  ROUND(AVG($1:age_at_surgery::DOUBLE), 1) AS mean_age,
  MEDIAN($1:age_at_surgery::DOUBLE) AS median_age,
  MIN($1:age_at_surgery::DOUBLE) AS min_age,
  MAX($1:age_at_surgery::DOUBLE) AS max_age,
  COUNT_IF($1:age_at_surgery IS NULL) AS n_null
FROM CANONICAL_PATIENT_MASTER
""")
report.append("## Age at surgery\n\n")
report.append("| " + " | ".join(cols) + " |\n| " + " | ".join("---" for _ in cols) + " |\n")
report.append("| " + " | ".join(str(v) for v in rows[0]) + " |\n\n")

# 3. Sex distribution
cols, rows = run("Sex", """
SELECT $1:sex::VARCHAR AS sex, COUNT(*) AS n,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM CANONICAL_PATIENT_MASTER GROUP BY 1 ORDER BY 2 DESC
""")
report.append("## Sex distribution\n\n| sex | n | pct |\n| --- | --- | --- |\n")
for r in rows:
    report.append("| " + " | ".join(str(v) for v in r) + " |\n")
report.append("\n")

# 4. Race
cols, rows = run("Race", """
SELECT $1:race::VARCHAR AS race, COUNT(*) AS n,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM CANONICAL_PATIENT_MASTER GROUP BY 1 ORDER BY 2 DESC
""")
report.append("## Race distribution\n\n| race | n | pct |\n| --- | --- | --- |\n")
for r in rows:
    report.append("| " + " | ".join(str(v) for v in r) + " |\n")
report.append("\n")

# 5. AJCC stage distribution (malignant only)
cols, rows = run("AJCC stage", """
SELECT $1:ajcc8_stage_group::VARCHAR AS stage, COUNT(*) AS n
FROM CANONICAL_PATIENT_MASTER
WHERE $1:is_malignant::BOOLEAN = TRUE
GROUP BY 1 ORDER BY 1
""")
report.append("## AJCC 8 stage group (malignant only)\n\n| stage | n |\n| --- | --- |\n")
for r in rows:
    report.append("| " + " | ".join(str(v) for v in r) + " |\n")
report.append("\n")

# 6. Histology — top 10
cols, rows = run("Top histologies", """
SELECT $1:histology_final::VARCHAR AS histology, COUNT(*) AS n
FROM CANONICAL_PATIENT_MASTER
WHERE $1:histology_final IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 10
""")
report.append("## Histology — top 10 (raw)\n\n| histology | n |\n| --- | --- |\n")
for r in rows:
    report.append("| " + " | ".join(str(v) for v in r) + " |\n")
report.append("\n")

# 7. AI_CLASSIFY: standardize 50 sample histologies via Llama 3.1 70B
print("\n=== AI_CLASSIFY histology standardization (50 sample, llama3.1-70b) ===")
cur.execute("""
SELECT
  $1:histology_final::VARCHAR AS raw_histology,
  AI_CLASSIFY(
    $1:histology_final::VARCHAR,
    ARRAY_CONSTRUCT(
      'Papillary thyroid carcinoma (classic/conventional)',
      'Papillary thyroid carcinoma — follicular variant',
      'Papillary thyroid carcinoma — tall cell variant',
      'Papillary thyroid carcinoma — other variant',
      'Follicular thyroid carcinoma',
      'Hurthle cell / oncocytic carcinoma',
      'Medullary thyroid carcinoma',
      'Anaplastic thyroid carcinoma',
      'Poorly differentiated thyroid carcinoma',
      'NIFTP',
      'Other malignant',
      'Benign'
    )
  ) AS classification
FROM CANONICAL_PATIENT_MASTER
WHERE $1:histology_final IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY $1:histology_final::VARCHAR ORDER BY HASH($1:research_id)) = 1
LIMIT 50
""")
ai_rows = cur.fetchall()

report.append("## AI_CLASSIFY: histology standardization (50 unique samples, llama3.1-70b)\n\n")
report.append("| raw_histology | classified_label |\n| --- | --- |\n")
for raw, cls in ai_rows:
    try:
        d = json.loads(cls)
        labels = d.get("labels", [])
        label = labels[0] if labels else "?"
    except Exception:
        label = str(cls)[:80]
    report.append(f"| {raw} | {label} |\n")
report.append(f"\n*{len(ai_rows)} rows classified via Cortex AI.*\n\n")

# 8. AI_FILTER: implausible age check
print("\n=== AI_FILTER: implausible age check (sample 100) ===")
cur.execute("""
SELECT
  $1:research_id::VARCHAR AS rid,
  $1:age_at_surgery::INT AS age,
  AI_FILTER(
    PROMPT('A thyroid surgery patient with age={0}. Is this implausible (i.e. <18 or >100)? Answer yes/no.',
           $1:age_at_surgery::VARCHAR)
  ) AS implausible
FROM CANONICAL_PATIENT_MASTER
WHERE $1:age_at_surgery IS NOT NULL
QUALIFY ROW_NUMBER() OVER (ORDER BY HASH($1:research_id)) <= 100
""")
flag_rows = cur.fetchall()
n_flagged = sum(1 for r in flag_rows if r[2])
report.append(f"## AI_FILTER: implausible age (100-row sample)\n\n")
report.append(f"- Sample size: {len(flag_rows)}\n")
report.append(f"- Flagged implausible: {n_flagged}\n\n")
if n_flagged:
    report.append("| rid | age |\n| --- | --- |\n")
    for r in flag_rows:
        if r[2]:
            report.append(f"| {r[0]} | {r[1]} |\n")
    report.append("\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}")
print(f"[size] {OUT.stat().st_size:,} bytes")

ctx.close()
