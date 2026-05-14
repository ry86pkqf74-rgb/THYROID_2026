#!/usr/bin/env python3
"""M011 sensitivity-analysis DeLong tests.
Pulls m011_sensitivity_predictions from BigQuery and runs paired DeLong tests for
C/D/E vs A within each outcome (clinically-significant malignancy, NIFTP-as-benign,
NIFTP-as-malignant). Writes tables/m011_sensitivity_delong.csv.
Usage: python3 m011_sensitivity_delong.py
"""
import numpy as np, pandas as pd, os
from google.cloud import bigquery
from scipy.stats import norm

PROJECT = "thyroid-canonical-pub-2026"
OUT = os.path.join(os.path.dirname(__file__), "..", "tables")
bq = bigquery.Client(project=PROJECT)
pred = bq.query("SELECT * FROM `thyroid-canonical-pub-2026.pub_workspace.m011_sensitivity_predictions`").to_dataframe()

def delong_var(y, p):
    pos, neg = p[y == 1], p[y == 0]
    m, n = len(pos), len(neg)
    def midrank(x):
        order = np.argsort(x); ranked = np.empty(len(x)); xs = x[order]; i = 0
        while i < len(xs):
            j = i
            while j < len(xs) and xs[j] == xs[i]:
                j += 1
            ranked[order[i:j]] = 0.5 * (i + j - 1) + 1; i = j
        return ranked
    tx, ty, tz = midrank(pos), midrank(neg), midrank(np.r_[pos, neg])
    auc = (tz[:m].sum() / m - (m + 1) / 2) / n
    return auc, (tz[:m] - tx) / n, 1 - (tz[m:] - ty) / m

def delong_test(y, p1, p2):
    a1, v01a, v10a = delong_var(y, p1)
    a2, v01b, v10b = delong_var(y, p2)
    m, n = int((y == 1).sum()), int((y == 0).sum())
    s = np.cov(np.c_[v01a, v01b].T) / m + np.cov(np.c_[v10a, v10b].T) / n
    var = s[0, 0] + s[1, 1] - 2 * s[0, 1]
    z = (a2 - a1) / np.sqrt(var) if var > 0 else 0.0
    return a1, a2, a2 - a1, 2 * (1 - norm.cdf(abs(z)))

rows = []
for outcome in pred.outcome.unique():
    sub = pred[pred.outcome == outcome]
    a = sub[sub.model == "A_Bethesda"][["research_id", "label", "prob"]].rename(columns={"prob": "p_ref"})
    for test in ["C_Bethesda_TIRADS", "D_Bethesda_TIRADS_clinical", "E_Bethesda_USfeatures"]:
        b = sub[sub.model == test][["research_id", "prob"]].rename(columns={"prob": "p_test"})
        d = a.merge(b, on="research_id")
        ar, at, delta, pv = delong_test(d.label.to_numpy(), d.p_ref.to_numpy(), d.p_test.to_numpy())
        rows.append(dict(outcome=outcome, ref="A_Bethesda", test=test,
                         auc_ref=round(ar, 4), auc_test=round(at, 4),
                         delta=round(delta, 4), p_value=pv))
df = pd.DataFrame(rows)
df.to_csv(os.path.join(OUT, "m011_sensitivity_delong.csv"), index=False)
print(df.to_string(index=False))
print("\nWritten to", os.path.abspath(os.path.join(OUT, "m011_sensitivity_delong.csv")))
