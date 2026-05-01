"""AI_EMBED phenotype clustering on malignant cohort.

Approach:
  1. Concatenate key clinical fields per patient into a phenotype string.
  2. AI_EMBED to 768-dim vectors via Snowflake Cortex.
  3. K-means clustering (sklearn) on the embeddings.
  4. Per-cluster: cohort sizes, dominant clinical features, outlier patients.
"""
import sys, time, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from _sf_client import get_cursor

OUT = Path("/Users/ros/THyroid 2026/snowflake_trial/reports/ai_embed_phenotype_clustering.md")
ctx, cur = get_cursor()

# 1. Build phenotype strings on a 500-pt malignant sample
print("=== Pulling 500 malignant patients with phenotype string ===")
cur.execute("""
SELECT
  RESEARCH_ID,
  CONCAT(
    'Age ', COALESCE(AGE_AT_SURGERY::VARCHAR,'NULL'),
    '; Sex ', COALESCE(SEX,'NULL'),
    '; Histology ', COALESCE(HISTOLOGY_FINAL,'NULL'),
    '; Stage ', COALESCE(AJCC8_STAGE_GROUP,'NULL'),
    '; T ', COALESCE(AJCC8_T_STAGE,'NULL'),
    '; N ', COALESCE(AJCC8_N_STAGE,'NULL'),
    '; M ', COALESCE(AJCC8_M_STAGE,'NULL'),
    '; Size ', COALESCE(TUMOR_SIZE_CM_MAX::VARCHAR,'NULL'), 'cm',
    '; ETE ', COALESCE(ETE_GRADE,'NULL'),
    '; LN+ ', COALESCE(LN_TOTAL_POSITIVE::VARCHAR,'NULL'),
    '; Surgery ', COALESCE(SURG_PROCEDURE_TYPE,'NULL'),
    '; RAI ', COALESCE(RAI_RECEIVED_FLAG::VARCHAR,'NULL'),
    '; BRAF ', COALESCE(BRAF_POSITIVE_FINAL::VARCHAR,'NULL'),
    '; Recurrence ', COALESCE(ANY_RECURRENCE_FLAG::VARCHAR,'NULL')
  ) AS phenotype_text,
  HISTOLOGY_FINAL, AJCC8_STAGE_GROUP, ANY_RECURRENCE_FLAG, AGE_AT_SURGERY, BRAF_POSITIVE_FINAL
FROM CANONICAL_PATIENT_MASTER_FLAT
WHERE IS_MALIGNANT = TRUE
QUALIFY ROW_NUMBER() OVER (ORDER BY HASH(RESEARCH_ID)) <= 500
""")
rows = cur.fetchall()
print(f"  {len(rows)} patients")

# 2. AI_EMBED — Snowflake Cortex returns 768-d embeddings via embed-m-v1.5 / e5-base-v2 etc.
print("=== AI_EMBED via Cortex (snowflake-arctic-embed-m-v1.5) ===")
t0 = time.time()
# Build one giant SELECT that returns rid + embedding per row
import pandas as pd
df = pd.DataFrame(rows, columns=['rid', 'phenotype_text', 'histology', 'stage', 'recurrence', 'age', 'braf'])

# Stage to a temp Snowflake table for AI_EMBED
cur.execute("""
CREATE OR REPLACE TEMP TABLE tmp_phenotypes (rid INT, txt VARCHAR)
""")
import csv, io
buf = io.StringIO()
w = csv.writer(buf)
for r in rows:
    w.writerow([r[0], r[1]])
buf.seek(0)
# Bulk insert via INSERT VALUES — small enough at 500 rows
inserts = []
for r in rows:
    safe_txt = r[1].replace("'", "''")
    inserts.append(f"({r[0]}, '{safe_txt}')")
# Insert in batches of 100
for i in range(0, len(inserts), 100):
    batch = inserts[i:i+100]
    cur.execute(f"INSERT INTO tmp_phenotypes (rid, txt) VALUES {', '.join(batch)}")

cur.execute("SELECT COUNT(*) FROM tmp_phenotypes")
print(f"  staged {cur.fetchone()[0]} rows; running AI_EMBED")

cur.execute("""
SELECT rid, AI_EMBED('snowflake-arctic-embed-m-v1.5', txt) AS emb
FROM tmp_phenotypes
""")
emb_rows = cur.fetchall()
print(f"  embedded {len(emb_rows)} in {time.time()-t0:.1f}s")

# 3. Cluster with sklearn KMeans
import numpy as np
embeddings = np.array([json.loads(r[1]) if isinstance(r[1], str) else r[1] for r in emb_rows])
print(f"  embedding shape: {embeddings.shape}")

from sklearn.cluster import KMeans
n_clusters = 6
km = KMeans(n_clusters=n_clusters, n_init=10, random_state=42)
labels = km.fit_predict(embeddings)
print(f"  KMeans assigned {n_clusters} clusters")

# 4. Build per-cluster summary
df['cluster'] = labels
report = ["# AI_EMBED Phenotype Clustering — Malignant Cohort (n=500 sample)\n",
          f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
          f"**Method:** Snowflake AI_EMBED ('snowflake-arctic-embed-m-v1.5') → 768-d vectors → sklearn KMeans (k={n_clusters})\n\n"]
report.append(f"## Cluster sizes\n\n")
sizes = df['cluster'].value_counts().sort_index()
report.append("| cluster | n |\n| --- | --- |\n")
for c, n in sizes.items():
    report.append(f"| {c} | {n} |\n")
report.append("\n")

# Per-cluster dominant features
report.append("## Per-cluster phenotype profile\n\n")
for c in sorted(df['cluster'].unique()):
    sub = df[df['cluster'] == c]
    n = len(sub)
    top_hist = sub['histology'].value_counts().head(3).to_dict()
    top_stage = sub['stage'].value_counts().head(3).to_dict()
    pct_recur = 100.0 * sub['recurrence'].fillna(False).astype(bool).sum() / n
    pct_braf = 100.0 * (sub['braf'] == True).sum() / n
    mean_age = pd.to_numeric(sub['age'], errors='coerce').mean()
    report.append(f"### Cluster {c} (n={n})\n\n")
    report.append(f"- **Mean age:** {mean_age:.1f}\n")
    report.append(f"- **Top histologies:** " + ", ".join(f"{k} ({v})" for k, v in top_hist.items()) + "\n")
    report.append(f"- **Top stages:** " + ", ".join(f"{k} ({v})" for k, v in top_stage.items()) + "\n")
    report.append(f"- **BRAF+:** {pct_braf:.1f}%\n")
    report.append(f"- **Recurrence:** {pct_recur:.1f}%\n\n")

# Outlier sample (1 patient per cluster, closest to centroid)
report.append("## Cluster centroid representatives\n\n")
report.append("| cluster | rid | phenotype |\n| --- | --- | --- |\n")
for c in sorted(df['cluster'].unique()):
    sub_idx = np.where(labels == c)[0]
    centroid = km.cluster_centers_[c]
    dists = np.linalg.norm(embeddings[sub_idx] - centroid, axis=1)
    rep_idx = sub_idx[np.argmin(dists)]
    rep = df.iloc[rep_idx]
    report.append(f"| {c} | {rep['rid']} | {rep['phenotype_text'][:120]}... |\n")

OUT.write_text("".join(report))
print(f"\n[saved] {OUT}  ({OUT.stat().st_size:,} bytes)")
ctx.close()
