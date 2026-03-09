#!/usr/bin/env python3
"""
Thyroid Cohort Explorer — Enhanced Dashboard v2
Powered by MotherDuck cloud DuckDB.

New in v2:
  • 🔬 Genetics & Molecular tab   — ThyroSeq / Afirma per-gene results
  • 🫀 Specimen Details tab        — gross pathology, dimensions, weight,
                                     capsule, margins, frozen sections
  • 📡 Pre-Op Imaging tab          — ultrasound TI-RADS/nodule features,
                                     CT/MRI structured findings
  • Expanded benign diagnoses      — 15+ specific subtypes
  • ✨ AI Insights                 — Claude-powered cohort analysis

Run locally:
    export MOTHERDUCK_TOKEN='your_token'
    streamlit run dashboard.py
"""
from __future__ import annotations
import os, sys, requests
from datetime import datetime
from pathlib import Path

import duckdb, pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from motherduck_client import MotherDuckClient, MotherDuckConfig

# ── Page config ───────────────────────────────────────────────────────────
st.set_page_config(page_title="Thyroid Cohort Explorer", page_icon="🔬",
                   layout="wide", initial_sidebar_state="expanded")

# ── Dark theme ────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@300;400;500&family=DM+Sans:wght@300;400;500;600&display=swap');
  :root{--bg:#07090f;--surface:#0e1219;--surface2:#141923;--border:#1e2535;--teal:#2dd4bf;--teal-dim:#1a8a7a;--amber:#f59e0b;--rose:#f43f5e;--sky:#38bdf8;--violet:#a78bfa;--green:#34d399;--text-hi:#f0f4ff;--text-mid:#8892a4;--text-lo:#4a5568;--font-d:'DM Serif Display',serif;--font-b:'DM Sans',sans-serif;--font-m:'DM Mono',monospace}
  html,body,[data-testid="stAppViewContainer"]{background:var(--bg)!important;font-family:var(--font-b)!important;color:var(--text-hi)!important}
  [data-testid="stSidebar"]{background:var(--surface)!important;border-right:1px solid var(--border)!important}
  [data-testid="stSidebar"] *{color:var(--text-hi)!important}
  h1,h2,h3{font-family:var(--font-d)!important;color:var(--text-hi)!important}
  h1{font-size:2.2rem!important;letter-spacing:-0.02em}
  h2{font-size:1.4rem!important;color:var(--teal)!important}
  .stTabs [data-baseweb="tab-list"]{background:var(--surface)!important;border-radius:10px;padding:4px;gap:2px;border:1px solid var(--border)}
  .stTabs [data-baseweb="tab"]{background:transparent!important;color:var(--text-mid)!important;border-radius:7px!important;font-family:var(--font-b)!important;font-size:0.82rem!important;font-weight:500!important;padding:7px 14px!important;transition:all .15s ease}
  .stTabs [aria-selected="true"]{background:var(--teal)!important;color:var(--bg)!important}
  .stTabs [data-baseweb="tab-panel"]{padding-top:1.2rem!important}
  .metric-card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1rem 1.1rem;position:relative;overflow:hidden;transition:border-color .2s ease}
  .metric-card:hover{border-color:var(--teal-dim)}
  .metric-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--teal),var(--sky));opacity:.7}
  .metric-label{font-family:var(--font-m);font-size:.6rem;letter-spacing:.12em;text-transform:uppercase;color:var(--text-mid);margin-bottom:5px}
  .metric-value{font-family:var(--font-d);font-size:1.8rem;color:var(--text-hi);line-height:1}
  .metric-delta{font-size:.7rem;color:var(--teal);margin-top:3px}
  .section-label{font-family:var(--font-m);font-size:.62rem;letter-spacing:.15em;text-transform:uppercase;color:var(--teal);margin:1.4rem 0 .5rem 0;display:block}
  .insight-box{background:linear-gradient(135deg,#0a1a20,#0e1219);border:1px solid var(--teal-dim);border-left:3px solid var(--teal);border-radius:10px;padding:1.2rem 1.4rem;margin-top:.8rem}
  .insight-header{font-family:var(--font-m);font-size:.62rem;letter-spacing:.15em;text-transform:uppercase;color:var(--teal);margin-bottom:8px}
  .stButton>button{background:var(--teal)!important;color:var(--bg)!important;border:none!important;border-radius:8px!important;font-family:var(--font-b)!important;font-weight:600!important;padding:.45rem 1.2rem!important;font-size:.83rem!important;transition:all .2s ease!important}
  .stButton>button:hover{background:#22c4ac!important;transform:translateY(-1px);box-shadow:0 4px 18px rgba(45,212,191,.3)!important}
  .stSelectbox>div>div,.stMultiSelect>div>div,.stTextInput>div>div{background:var(--surface2)!important;border:1px solid var(--border)!important;border-radius:8px!important;color:var(--text-hi)!important}
  ::-webkit-scrollbar{width:4px;height:4px}::-webkit-scrollbar-track{background:var(--bg)}::-webkit-scrollbar-thumb{background:var(--border);border-radius:10px}
  #MainMenu,footer,header{visibility:hidden}.block-container{padding-top:1.5rem!important;max-width:1420px}
</style>""", unsafe_allow_html=True)

PL = dict(paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(14,18,25,0.8)",
          font=dict(family="DM Sans",color="#8892a4",size=12),
          title_font=dict(family="DM Serif Display",color="#f0f4ff",size=15),
          xaxis=dict(gridcolor="#1e2535",linecolor="#1e2535",zerolinecolor="#1e2535"),
          yaxis=dict(gridcolor="#1e2535",linecolor="#1e2535",zerolinecolor="#1e2535"),
          legend=dict(bgcolor="rgba(14,18,25,0.8)",bordercolor="#1e2535",borderwidth=1),
          margin=dict(l=16,r=16,t=36,b=16),
          colorway=["#2dd4bf","#38bdf8","#a78bfa","#f59e0b","#f43f5e","#34d399","#fb923c"],
          hoverlabel=dict(bgcolor="#141923",bordercolor="#1e2535",font_color="#f0f4ff"))
SEQ_TEAL = [[0,"#0a1a20"],[0.5,"#1a8a7a"],[1,"#2dd4bf"]]
SHARE_PATH = "md:_share/thyroid_research_ro/7962a053-3581-4ebf-abf6-57af957efb1c"
DATABASE   = "thyroid_research_2026"

# ─────────────────────────────────────────────────────────────────────────
# CONNECTION
# ─────────────────────────────────────────────────────────────────────────
def _ensure_token():
    if os.getenv("MOTHERDUCK_TOKEN"): return True
    try:
        os.environ["MOTHERDUCK_TOKEN"] = st.secrets["MOTHERDUCK_TOKEN"]; return True
    except (KeyError,FileNotFoundError): return False

@st.cache_resource(show_spinner="Connecting to MotherDuck…")
def _get_con():
    cfg = MotherDuckConfig(database=DATABASE, share_path=SHARE_PATH)
    cli = MotherDuckClient(cfg)
    try: return cli.connect_ro_share()
    except: return cli.connect_rw()

@st.cache_data(ttl=300, show_spinner=False)
def qdf(_con, sql):  return _con.execute(sql).fetchdf()
@st.cache_data(ttl=300, show_spinner=False)
def qs(_con, sql):
    r = _con.execute(sql).fetchone(); return r[0] if r else 0

def sqdf(con, sql):
    try: return qdf(con, sql)
    except Exception as e: st.warning(f"Query failed: {e}",icon="⚠️"); return pd.DataFrame()
def sqs(con, sql):
    try: return qs(con, sql)
    except: return 0
def tbl_exists(con, name):
    try: return bool(sqs(con,f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{name}'"))
    except: return False

def mc(label, value, delta=None):
    d = f'<div class="metric-delta">{delta}</div>' if delta else ""
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div>{d}</div>'
def sl(t): return f'<span class="section-label">{t}</span>'

# ─────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────
def build_sidebar(df):
    with st.sidebar:
        st.markdown(sl("⚡ Filters"), unsafe_allow_html=True)
        h_opts = sorted(df["histology_1_type"].dropna().unique().tolist())
        sel_h = st.multiselect("Histology", h_opts, default=[], placeholder="All")
        s_opts = sorted(df["sex"].dropna().unique().tolist())
        sel_s = st.multiselect("Sex", s_opts, default=[])
        braf_only = st.checkbox("BRAF mutation mentioned")
        para_only = st.checkbox("Has parathyroid data")
        tumor_only = st.checkbox("Tumor pathology only")
        ages = df["age_at_surgery"].dropna()
        if not ages.empty and ages.min() < ages.max():
            age_r = st.slider("Age",int(ages.min()),int(ages.max()),(int(ages.min()),int(ages.max())))
        else: age_r = (0,120)
        st.markdown("---")
        st.markdown('<div style="font-family:monospace;font-size:.6rem;color:#4a5568">DATABASE<br><span style="color:#2dd4bf">thyroid_research_2026</span></div>',unsafe_allow_html=True)
        if st.button("Clear filters"): st.rerun()

    f = df.copy()
    if sel_h: f = f[f["histology_1_type"].isin(sel_h)]
    if sel_s: f = f[f["sex"].isin(sel_s)]
    if braf_only and "braf_mutation_mentioned" in f.columns: f = f[f["braf_mutation_mentioned"]==True]
    if para_only and "has_parathyroid" in f.columns: f = f[f["has_parathyroid"]==True]
    if tumor_only and "has_tumor_pathology" in f.columns: f = f[f["has_tumor_pathology"]==True]
    f = f[f["age_at_surgery"].isna()|((f["age_at_surgery"]>=age_r[0])&(f["age_at_surgery"]<=age_r[1]))]
    return f

# ─────────────────────────────────────────────────────────────────────────
# TAB: OVERVIEW
# ─────────────────────────────────────────────────────────────────────────
def render_overview(con):
    q = sqs
    m = dict(
        total=q(con,"SELECT COUNT(DISTINCT research_id) FROM master_cohort"),
        tumor_path=q(con,"SELECT COUNT(*) FROM master_cohort WHERE has_tumor_pathology"),
        benign_path=q(con,"SELECT COUNT(*) FROM master_cohort WHERE has_benign_pathology"),
        fna=q(con,"SELECT COUNT(*) FROM master_cohort WHERE has_fna_cytology"),
        braf=q(con,"SELECT COALESCE(SUM(CASE WHEN braf_mutation_mentioned THEN 1 ELSE 0 END),0) FROM tumor_pathology"),
        rai_pos=q(con,"SELECT COUNT(*) FROM nuclear_med WHERE rai_avid_flag='positive'"),
        nuclear=q(con,"SELECT COUNT(*) FROM master_cohort WHERE has_nuclear_med"),
        us=q(con,"SELECT COUNT(*) FROM master_cohort WHERE has_ultrasound_reports"),
        ct=q(con,"SELECT COUNT(*) FROM master_cohort WHERE has_ct_imaging"),
        tg=q(con,"SELECT COUNT(*) FROM master_cohort WHERE has_thyroglobulin_labs"),
        atg=q(con,"SELECT COUNT(*) FROM master_cohort WHERE has_anti_thyroglobulin_labs"),
        comp=q(con,"SELECT COUNT(DISTINCT research_id) FROM complications") if tbl_exists(con,"complications") else 0,
        rln=q(con,"SELECT COUNT(*) FROM complications WHERE LOWER(CAST(rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy AS VARCHAR)) NOT IN ('nan','','0') AND rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy IS NOT NULL") if tbl_exists(con,"complications") else 0,
        gen=q(con,"SELECT COUNT(DISTINCT research_id) FROM genetic_testing") if tbl_exists(con,"genetic_testing") else 0,
    )
    st.markdown(f'<div style="background:linear-gradient(135deg,#0a1a20,#0e1219);border:1px solid #1e2535;border-left:3px solid #2dd4bf;border-radius:12px;padding:.9rem 1.4rem;margin-bottom:1.2rem"><div style="font-family:\'DM Mono\',monospace;font-size:.62rem;letter-spacing:.15em;color:#2dd4bf;text-transform:uppercase">Total Cohort</div><div style="font-family:\'DM Serif Display\',serif;font-size:2.4rem;color:#f0f4ff;line-height:1">{m["total"]:,} <span style="font-size:.9rem;color:#8892a4;font-family:sans-serif">patients</span></div></div>',unsafe_allow_html=True)

    grid = [
        [("Tumor Pathology",f"{m['tumor_path']:,}",None),("Benign Pathology",f"{m['benign_path']:,}",None),("FNA Cytology",f"{m['fna']:,}",None),("BRAF Mentioned",f"{m['braf']:,}",None)],
        [("RAI Positive",f"{m['rai_pos']:,}",None),("Nuclear Med",f"{m['nuclear']:,}",None),("Ultrasound",f"{m['us']:,}",None),("CT Imaging",f"{m['ct']:,}",None)],
        [("Tg Labs",f"{m['tg']:,}",None),("Anti-Tg Labs",f"{m['atg']:,}",None),("Complications Data",f"{m['comp']:,}",None),("Genetic Testing",f"{m['gen']:,}","ThyroSeq / Afirma")],
    ]
    for row in grid:
        cols = st.columns(4)
        for i,(l,v,d) in enumerate(row):
            with cols[i]: st.markdown(mc(l,v,d),unsafe_allow_html=True)
        st.markdown("<br>",unsafe_allow_html=True)

    st.markdown(sl("Data Completeness by Surgery Year"),unsafe_allow_html=True)
    df_c = sqdf(con,"SELECT * FROM data_completeness_by_year ORDER BY surgery_year")
    if not df_c.empty:
        fig = go.Figure()
        for name,(col,color) in {"Tumor Path":("n_tumor_pathology","#2dd4bf"),"FNA":("n_fna_cytology","#38bdf8"),"Ultrasound":("n_ultrasound_reports","#a78bfa"),"Tg Labs":("n_thyroglobulin_labs","#f59e0b")}.items():
            if col in df_c.columns:
                fig.add_trace(go.Bar(x=df_c["surgery_year"],y=df_c[col],name=name,marker_color=color,
                    hovertemplate=f"<b>{name}</b><br>Year: %{{x}}<br>Patients: %{{y:,}}<extra></extra>"))
        fig.update_layout(**PL,barmode="group",height=340,xaxis_title="Surgery Year")
        st.plotly_chart(fig,use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────
# TAB: DATA EXPLORER
# ─────────────────────────────────────────────────────────────────────────
def render_explorer(df):
    st.markdown(f"**{len(df):,} patients** after current filters")
    def_cols = [c for c in ["research_id","age_at_surgery","sex","histology_1_type","variant_standardized","overall_stage_ajcc8","largest_tumor_cm","braf_mutation_mentioned","has_parathyroid"] if c in df.columns]
    cols = st.multiselect("Columns",df.columns.tolist(),default=def_cols)
    srch = st.text_input("🔍 Search all columns",placeholder="type to filter…")
    disp = df[cols] if cols else df
    if srch:
        mask = disp.apply(lambda s: s.astype(str).str.contains(srch,case=False,na=False)).any(axis=1)
        disp = disp[mask]; st.caption(f"{len(disp):,} matching rows")
    st.dataframe(disp,use_container_width=True,height=520)
    st.download_button("⬇ Download CSV",df.to_csv(index=False),f"thyroid_{datetime.now():%Y%m%d}.csv","text/csv")

# ─────────────────────────────────────────────────────────────────────────
# TAB: VISUALIZATIONS
# ─────────────────────────────────────────────────────────────────────────
def render_viz(con):
    c1,c2 = st.columns(2)
    with c1:
        st.markdown("#### Histology Distribution")
        df_h = sqdf(con,"SELECT COALESCE(histology_1_type,'Not specified') AS histology,COUNT(*) AS n FROM tumor_pathology WHERE histology_1_type IS NOT NULL AND TRIM(histology_1_type)!='' GROUP BY 1 ORDER BY n DESC")
        if not df_h.empty:
            fig = px.bar(df_h.head(15),x="n",y="histology",orientation="h",color="n",color_continuous_scale=SEQ_TEAL)
            fig.update_layout(**PL,showlegend=False,coloraxis_showscale=False,yaxis=dict(autorange="reversed",gridcolor="#1e2535"),height=420)
            st.plotly_chart(fig,use_container_width=True)
    with c2:
        st.markdown("#### AJCC 8th Edition Stage")
        df_s = sqdf(con,"SELECT COALESCE(histology_1_overall_stage_ajcc8,'Unknown') AS stage,COUNT(*) AS n FROM tumor_pathology WHERE histology_1_overall_stage_ajcc8 IS NOT NULL AND TRIM(histology_1_overall_stage_ajcc8)!='' GROUP BY 1 ORDER BY n DESC")
        if not df_s.empty:
            stage_c = {"I":"#2dd4bf","II":"#38bdf8","III":"#a78bfa","IVA":"#f59e0b","IVB":"#f43f5e","IVC":"#dc2626"}
            fig = px.bar(df_s,x="stage",y="n",color="stage",color_discrete_map=stage_c)
            fig.update_layout(**PL,showlegend=False,height=420,xaxis_title="")
            st.plotly_chart(fig,use_container_width=True)
    c3,c4 = st.columns(2)
    with c3:
        st.markdown("#### Sex Distribution")
        df_sx = sqdf(con,"SELECT COALESCE(sex,'Unknown') AS sex,COUNT(*) AS n FROM master_cohort WHERE sex IS NOT NULL AND TRIM(sex)!='' GROUP BY 1 ORDER BY n DESC")
        if not df_sx.empty:
            fig = go.Figure(go.Pie(labels=df_sx["sex"],values=df_sx["n"],hole=0.55,marker=dict(colors=["#2dd4bf","#38bdf8","#a78bfa"],line=dict(color="#07090f",width=3)),textinfo="label+percent"))
            fig.update_layout(**PL,showlegend=False,height=340)
            st.plotly_chart(fig,use_container_width=True)
    with c4:
        st.markdown("#### Recurrence Risk Bands")
        df_rr = sqdf(con,"SELECT recurrence_risk_band AS band,COUNT(*) AS n FROM recurrence_risk_cohort GROUP BY 1 ORDER BY n DESC")
        if not df_rr.empty:
            rc = {"low":"#34d399","intermediate":"#f59e0b","high":"#f43f5e"}
            fig = go.Figure(go.Pie(labels=df_rr["band"],values=df_rr["n"],hole=0.55,marker=dict(colors=[rc.get(str(b).lower(),"#8892a4") for b in df_rr["band"]],line=dict(color="#07090f",width=3)),textinfo="label+percent"))
            fig.update_layout(**PL,showlegend=False,height=340)
            st.plotly_chart(fig,use_container_width=True)
    st.markdown("#### Age at Surgery by AJCC Stage")
    df_ab = sqdf(con,"SELECT tp.histology_1_overall_stage_ajcc8 AS stage,mc.age_at_surgery FROM master_cohort mc JOIN tumor_pathology tp ON mc.research_id=tp.research_id WHERE tp.histology_1_overall_stage_ajcc8 IS NOT NULL AND mc.age_at_surgery IS NOT NULL")
    if not df_ab.empty:
        colors = ["#2dd4bf","#38bdf8","#a78bfa","#f59e0b","#f43f5e"]
        fig = go.Figure()
        for i,s in enumerate(sorted(df_ab["stage"].unique())):
            sub = df_ab[df_ab["stage"]==s]["age_at_surgery"].dropna()
            fig.add_trace(go.Box(y=sub,name=str(s),marker_color=colors[i%len(colors)],line_color=colors[i%len(colors)],fillcolor="rgba(45,212,191,.07)",boxmean="sd"))
        fig.update_layout(**PL,height=360,yaxis_title="Age",showlegend=False)
        st.plotly_chart(fig,use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────
# TAB: ADVANCED
# ─────────────────────────────────────────────────────────────────────────
def render_advanced(con):
    MUT_C = {"BRAF":"#2dd4bf","RAS":"#38bdf8","RET":"#a78bfa","TERT":"#f59e0b","NTRK":"#f43f5e","ALK":"#34d399"}
    c1,c2 = st.columns(2)
    with c1:
        st.markdown("#### Mutation Flags (Pathology Reports)")
        df_m = sqdf(con,
            "SELECT 'BRAF' AS m,COALESCE(SUM(CASE WHEN braf_mutation_mentioned THEN 1 ELSE 0 END),0) AS n FROM tumor_pathology "
            "UNION ALL SELECT 'RAS',COALESCE(SUM(CASE WHEN ras_mutation_mentioned THEN 1 ELSE 0 END),0) FROM tumor_pathology "
            "UNION ALL SELECT 'RET',COALESCE(SUM(CASE WHEN ret_mutation_mentioned THEN 1 ELSE 0 END),0) FROM tumor_pathology "
            "UNION ALL SELECT 'TERT',COALESCE(SUM(CASE WHEN tert_mutation_mentioned THEN 1 ELSE 0 END),0) FROM tumor_pathology "
            "UNION ALL SELECT 'NTRK',COALESCE(SUM(CASE WHEN ntrk_mutation_mentioned THEN 1 ELSE 0 END),0) FROM tumor_pathology "
            "UNION ALL SELECT 'ALK',COALESCE(SUM(CASE WHEN alk_mutation_mentioned THEN 1 ELSE 0 END),0) FROM tumor_pathology")
        if not df_m.empty:
            df_m = df_m.sort_values("n",ascending=True)
            fig = go.Figure(go.Bar(x=df_m["n"],y=df_m["m"],orientation="h",marker_color=[MUT_C.get(x,"#2dd4bf") for x in df_m["m"]],text=[f"{x:,}" for x in df_m["n"]],textposition="outside"))
            fig.update_layout(**PL,height=320,xaxis_title="Reports mentioning")
            st.plotly_chart(fig,use_container_width=True)
    with c2:
        st.markdown("#### RAI Avidity")
        df_r = sqdf(con,"SELECT COALESCE(rai_avid_flag,'not assessed') AS status,COUNT(*) AS n FROM nuclear_med GROUP BY 1 ORDER BY n DESC")
        if not df_r.empty:
            rai_c = {"positive":"#f43f5e","negative":"#34d399","unknown":"#f59e0b","not assessed":"#4a5568"}
            fig = go.Figure(go.Pie(labels=df_r["status"],values=df_r["n"],hole=0.55,marker=dict(colors=[rai_c.get(str(s).lower(),"#8892a4") for s in df_r["status"]],line=dict(color="#07090f",width=3)),textinfo="label+percent"))
            fig.update_layout(**PL,showlegend=False,height=320)
            st.plotly_chart(fig,use_container_width=True)

    st.markdown("#### Benign Pathology — Core Diagnoses")
    df_b = sqdf(con,"""
    SELECT label,n FROM (VALUES
      ('Multinodular Goiter',(SELECT COALESCE(SUM(CASE WHEN is_mng THEN 1 ELSE 0 END),0) FROM benign_pathology)),
      ('Hashimoto Thyroiditis',(SELECT COALESCE(SUM(CASE WHEN is_hashimoto THEN 1 ELSE 0 END),0) FROM benign_pathology)),
      ('Graves Disease',(SELECT COALESCE(SUM(CASE WHEN is_graves THEN 1 ELSE 0 END),0) FROM benign_pathology)),
      ('Follicular Adenoma',(SELECT COALESCE(SUM(CASE WHEN is_follicular_adenoma THEN 1 ELSE 0 END),0) FROM benign_pathology)),
      ('Hurthle Cell Adenoma',(SELECT COALESCE(SUM(CASE WHEN is_hurthle_adenoma THEN 1 ELSE 0 END),0) FROM benign_pathology)),
      ('Hyalinizing Trabecular',(SELECT COALESCE(SUM(CASE WHEN is_hyalinizing_trabecular THEN 1 ELSE 0 END),0) FROM benign_pathology)),
      ('TGDC',(SELECT COALESCE(SUM(CASE WHEN is_tgdc THEN 1 ELSE 0 END),0) FROM benign_pathology)),
      ('Colloid Nodule',(SELECT COALESCE(SUM(CASE WHEN LOWER(CAST(colloid_nodule AS VARCHAR)) IN ('true','1','yes') THEN 1 ELSE 0 END),0) FROM benign_pathology)),
      ('Diffuse Hyperplasia',(SELECT COALESCE(SUM(CASE WHEN LOWER(CAST(diffuse_hyperplasia AS VARCHAR)) IN ('true','1','yes') THEN 1 ELSE 0 END),0) FROM benign_pathology)),
      ('Focal Lymphocytic Thyroiditis',(SELECT COALESCE(SUM(CASE WHEN LOWER(CAST(focal_lymphocytic_thyroiditis AS VARCHAR)) IN ('true','1','yes') THEN 1 ELSE 0 END),0) FROM benign_pathology))
    ) t(label,n) WHERE n > 0 ORDER BY n DESC""")
    if not df_b.empty:
        fig = px.bar(df_b,x="n",y="label",orientation="h",color="n",color_continuous_scale=SEQ_TEAL)
        fig.update_layout(**PL,showlegend=False,coloraxis_showscale=False,yaxis=dict(autorange="reversed",gridcolor="#1e2535"),height=380)
        st.plotly_chart(fig,use_container_width=True)

    # Extended benign detail view
    if tbl_exists(con,"benign_detail_view"):
        st.markdown("#### Rare & Specific Benign Diagnoses")
        df_rare = sqdf(con,"""
        SELECT label,n FROM (VALUES
          ('C-Cell Hyperplasia',(SELECT SUM(CASE WHEN c_cell_hyperplasia THEN 1 ELSE 0 END) FROM benign_detail_view)),
          ('Adenomatoid / Hyperplastic Nodule',(SELECT SUM(CASE WHEN adenomatoid_hyperplastic_nodule THEN 1 ELSE 0 END) FROM benign_detail_view)),
          ('Amyloid Goiter',(SELECT SUM(CASE WHEN amyloid_goiter THEN 1 ELSE 0 END) FROM benign_detail_view)),
          ('Black Thyroid',(SELECT SUM(CASE WHEN black_thyroid THEN 1 ELSE 0 END) FROM benign_detail_view)),
          ('Fibrosing / Riedel Thyroiditis',(SELECT SUM(CASE WHEN fibrosing_hashimoto_or_riedel THEN 1 ELSE 0 END) FROM benign_detail_view)),
          ('Lipoadenoma',(SELECT SUM(CASE WHEN lipoadenoma THEN 1 ELSE 0 END) FROM benign_detail_view)),
          ('Radiation-Associated Changes',(SELECT SUM(CASE WHEN radiation_associated_changes THEN 1 ELSE 0 END) FROM benign_detail_view)),
          ('Thyroglossal Duct Cyst',(SELECT SUM(CASE WHEN thyroglossal_duct_cyst THEN 1 ELSE 0 END) FROM benign_detail_view))
        ) t(label,n) WHERE n IS NOT NULL AND CAST(n AS INTEGER)>0 ORDER BY CAST(n AS INTEGER) DESC""")
        if not df_rare.empty:
            fig = px.bar(df_rare,x="n",y="label",orientation="h",color="n",color_continuous_scale=SEQ_TEAL)
            fig.update_layout(**PL,showlegend=False,coloraxis_showscale=False,yaxis=dict(autorange="reversed",gridcolor="#1e2535"),height=300)
            st.plotly_chart(fig,use_container_width=True)

    st.markdown("#### Recurrence Risk Bands")
    df_rr = sqdf(con,"SELECT recurrence_risk_band AS band,COUNT(*) AS n FROM recurrence_risk_cohort GROUP BY 1 ORDER BY n DESC")
    if not df_rr.empty:
        c1,c2 = st.columns([1,2])
        with c1: st.dataframe(df_rr,use_container_width=True)
        with c2:
            rc = {"low":"#34d399","intermediate":"#f59e0b","high":"#f43f5e"}
            fig = go.Figure(go.Pie(labels=df_rr["band"],values=df_rr["n"],hole=0.5,
                marker=dict(colors=[rc.get(str(b).lower(),"#8892a4") for b in df_rr["band"]],line=dict(color="#07090f",width=3)),textinfo="label+percent"))
            fig.update_layout(**PL,height=300,showlegend=False)
            st.plotly_chart(fig,use_container_width=True)

    # ── Cancer co-occurrence by benign diagnosis ──────────────────
    st.markdown(sl("Cancer Co-Occurrence by Benign Diagnosis"),unsafe_allow_html=True)
    st.caption("Among patients with each benign diagnosis, what fraction also had a concurrent malignancy?")

    cooccur_sql = """
    WITH base AS (
        SELECT
            mc.research_id,
            CASE WHEN mc.has_tumor_pathology THEN 1 ELSE 0 END AS is_malignant,
            bp.is_mng,
            bp.is_hashimoto,
            bp.is_graves,
            bp.is_follicular_adenoma,
            bp.is_hurthle_adenoma,
            bp.is_hyalinizing_trabecular,
            bp.is_tgdc,
            COALESCE(LOWER(CAST(bp.colloid_nodule AS VARCHAR)) IN ('true','1','yes'), FALSE)          AS is_colloid_nodule,
            COALESCE(LOWER(CAST(bp.diffuse_hyperplasia AS VARCHAR)) IN ('true','1','yes'), FALSE)     AS is_diffuse_hyperplasia,
            COALESCE(LOWER(CAST(bp.focal_lymphocytic_thyroiditis AS VARCHAR)) IN ('true','1','yes'), FALSE) AS is_focal_lt
        FROM master_cohort mc
        LEFT JOIN benign_pathology bp ON mc.research_id = bp.research_id
        WHERE mc.has_benign_pathology = TRUE
    )
    SELECT
        label,
        total,
        malignant,
        ROUND(100.0 * malignant / NULLIF(total, 0), 1) AS pct_malignant,
        total - malignant AS benign_only
    FROM (
        SELECT 'Multinodular Goiter'         AS label, SUM(CASE WHEN is_mng THEN 1 ELSE 0 END) AS total, SUM(CASE WHEN is_mng AND is_malignant=1 THEN 1 ELSE 0 END) AS malignant FROM base UNION ALL
        SELECT 'Hashimoto Thyroiditis',       SUM(CASE WHEN is_hashimoto THEN 1 ELSE 0 END),   SUM(CASE WHEN is_hashimoto AND is_malignant=1 THEN 1 ELSE 0 END) FROM base UNION ALL
        SELECT 'Graves Disease',              SUM(CASE WHEN is_graves THEN 1 ELSE 0 END),      SUM(CASE WHEN is_graves AND is_malignant=1 THEN 1 ELSE 0 END) FROM base UNION ALL
        SELECT 'Follicular Adenoma',          SUM(CASE WHEN is_follicular_adenoma THEN 1 ELSE 0 END), SUM(CASE WHEN is_follicular_adenoma AND is_malignant=1 THEN 1 ELSE 0 END) FROM base UNION ALL
        SELECT 'Hurthle Cell Adenoma',        SUM(CASE WHEN is_hurthle_adenoma THEN 1 ELSE 0 END),   SUM(CASE WHEN is_hurthle_adenoma AND is_malignant=1 THEN 1 ELSE 0 END) FROM base UNION ALL
        SELECT 'Hyalinizing Trabecular',      SUM(CASE WHEN is_hyalinizing_trabecular THEN 1 ELSE 0 END), SUM(CASE WHEN is_hyalinizing_trabecular AND is_malignant=1 THEN 1 ELSE 0 END) FROM base UNION ALL
        SELECT 'TGDC',                        SUM(CASE WHEN is_tgdc THEN 1 ELSE 0 END),       SUM(CASE WHEN is_tgdc AND is_malignant=1 THEN 1 ELSE 0 END) FROM base UNION ALL
        SELECT 'Colloid Nodule',              SUM(CASE WHEN is_colloid_nodule THEN 1 ELSE 0 END), SUM(CASE WHEN is_colloid_nodule AND is_malignant=1 THEN 1 ELSE 0 END) FROM base UNION ALL
        SELECT 'Diffuse Hyperplasia',         SUM(CASE WHEN is_diffuse_hyperplasia THEN 1 ELSE 0 END), SUM(CASE WHEN is_diffuse_hyperplasia AND is_malignant=1 THEN 1 ELSE 0 END) FROM base UNION ALL
        SELECT 'Focal Lymphocytic Thyroiditis', SUM(CASE WHEN is_focal_lt THEN 1 ELSE 0 END), SUM(CASE WHEN is_focal_lt AND is_malignant=1 THEN 1 ELSE 0 END) FROM base
    ) sub
    WHERE total > 0
    ORDER BY pct_malignant DESC
    """
    df_cooc = sqdf(con, cooccur_sql)
    if not df_cooc.empty:
        # ── Stacked bar: benign-only vs concurrent malignancy ──
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Benign Only",
            y=df_cooc["label"],
            x=df_cooc["benign_only"],
            orientation="h",
            marker=dict(color="#2dd4bf", line=dict(color="#07090f",width=1)),
            hovertemplate="<b>%{y}</b><br>Benign only: %{x:,}<extra></extra>"
        ))
        fig.add_trace(go.Bar(
            name="Concurrent Malignancy",
            y=df_cooc["label"],
            x=df_cooc["malignant"],
            orientation="h",
            marker=dict(color="#f43f5e", line=dict(color="#07090f",width=1)),
            hovertemplate="<b>%{y}</b><br>Concurrent cancer: %{x:,} (%{customdata:.1f}%)<extra></extra>",
            customdata=df_cooc["pct_malignant"]
        ))
        fig.update_layout(**PL,
            barmode="stack",
            height=420,
            xaxis_title="Number of Patients",
            yaxis=dict(autorange="reversed", gridcolor="#1e2535"),
            legend=dict(orientation="h", y=1.05),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Cancer rate % bar ──────────────────────────────────
        st.markdown("##### Concurrent Cancer Rate (%) by Benign Diagnosis")
        df_cooc_sorted = df_cooc.sort_values("pct_malignant", ascending=True)
        CANCER_RATE_COLORS = [
            "#34d399" if p < 5 else "#f59e0b" if p < 20 else "#f43f5e"
            for p in df_cooc_sorted["pct_malignant"]
        ]
        fig2 = go.Figure(go.Bar(
            x=df_cooc_sorted["pct_malignant"],
            y=df_cooc_sorted["label"],
            orientation="h",
            marker_color=CANCER_RATE_COLORS,
            text=[f"{p:.1f}%" for p in df_cooc_sorted["pct_malignant"]],
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>Cancer rate: %{x:.1f}%<br>n=%{customdata:,}<extra></extra>",
            customdata=df_cooc_sorted["total"]
        ))
        fig2.add_vline(x=5,  line_dash="dot", line_color="#34d399", annotation_text="5%",  annotation_font_color="#34d399", annotation_font_size=10)
        fig2.add_vline(x=20, line_dash="dot", line_color="#f59e0b", annotation_text="20%", annotation_font_color="#f59e0b", annotation_font_size=10)
        fig2.update_layout(**PL,
            height=360,
            xaxis_title="Concurrent Cancer Rate (%)",
            xaxis=dict(range=[0, max(df_cooc_sorted["pct_malignant"]) * 1.25], gridcolor="#1e2535"),
            yaxis=dict(gridcolor="#1e2535"),
        )
        st.plotly_chart(fig2, use_container_width=True)

        # ── Summary table ──────────────────────────────────────
        with st.expander("📋 Full co-occurrence table"):
            display_df = df_cooc[["label","total","malignant","benign_only","pct_malignant"]].rename(columns={
                "label":"Benign Diagnosis","total":"Total Patients",
                "malignant":"Concurrent Cancer","benign_only":"Benign Only","pct_malignant":"Cancer Rate (%)"
            })
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            st.download_button("⬇ Download CSV", display_df.to_csv(index=False), "cancer_cooccurrence.csv", "text/csv")

# ─────────────────────────────────────────────────────────────────────────
# TAB: GENETICS & MOLECULAR
# ─────────────────────────────────────────────────────────────────────────
def render_genetics(con):
    has_gen = tbl_exists(con,"genetic_testing")
    has_view = tbl_exists(con,"genetic_testing_summary_view")

    if not has_gen:
        st.info("**Genetic testing table not yet loaded.**\n\nRun `scripts/07_phase3_genetics_specimen.py` to ingest `THYROSEQ_AFIRMA_12_5.xlsx`.",icon="🧬")
        st.markdown("#### Expected Schema After Ingestion")
        exp = pd.DataFrame({"Field":["test_platform","result_category","braf_v600e","nras","hras","kras","ret_ptc1","ret_ptc3","pax8_pparg","tert_promoter","ntrk_any","alk_fusion","dicer1","pten","tp53","molecular_concordance_class"],"Type":["VARCHAR"]*2+["BOOLEAN"]*13+["VARCHAR"],"Description":["ThyroSeq v3 / Afirma GSC / Afirma XA / Foundation One","benign / suspicious / positive / indeterminate","BRAF V600E point mutation","NRAS mutation","HRAS mutation","KRAS mutation","RET/PTC1 fusion","RET/PTC3 fusion","PAX8-PPARG fusion","TERT promoter mutation","NTRK1 or NTRK3 fusion","ALK fusion","DICER1 mutation","PTEN mutation","TP53 mutation","TP / FP / FN / TN vs final pathology"]})
        st.dataframe(exp,use_container_width=True,hide_index=True)
        return

    st.markdown(sl("Testing Platform & Result Category"),unsafe_allow_html=True)
    c1,c2 = st.columns(2)
    with c1:
        df_plat = sqdf(con,"SELECT COALESCE(test_platform,'Unknown') AS platform,COUNT(*) AS n FROM genetic_testing GROUP BY 1 ORDER BY n DESC")
        if not df_plat.empty:
            fig = px.pie(df_plat,names="platform",values="n",hole=0.5,color_discrete_sequence=["#2dd4bf","#38bdf8","#a78bfa","#f59e0b","#f43f5e"])
            fig.update_traces(textinfo="label+percent",marker=dict(line=dict(color="#07090f",width=2)))
            fig.update_layout(**PL,showlegend=False,height=300,title="Platform")
            st.plotly_chart(fig,use_container_width=True)
    with c2:
        df_res = sqdf(con,"SELECT COALESCE(result_category,'Unknown') AS result,COUNT(*) AS n FROM genetic_testing GROUP BY 1 ORDER BY n DESC")
        if not df_res.empty:
            rc = {"benign":"#34d399","suspicious":"#f59e0b","positive":"#f43f5e","indeterminate":"#a78bfa","other":"#8892a4"}
            fig = go.Figure(go.Pie(labels=df_res["result"],values=df_res["n"],hole=0.55,
                marker=dict(colors=[rc.get(str(r).lower(),"#8892a4") for r in df_res["result"]],line=dict(color="#07090f",width=3)),textinfo="label+percent"))
            fig.update_layout(**PL,showlegend=False,height=300,title="Result Category")
            st.plotly_chart(fig,use_container_width=True)

    if has_view:
        st.markdown(sl("Per-Gene Mutation / Fusion Prevalence"),unsafe_allow_html=True)
        gene_sql = """
        SELECT gene,SUM(pos) AS n_positive,COUNT(*) AS total,ROUND(100.0*SUM(pos)/NULLIF(COUNT(*),0),1) AS pct
        FROM (
          SELECT 'BRAF V600E' AS gene,CASE WHEN braf_v600e THEN 1 ELSE 0 END AS pos FROM genetic_testing_summary_view UNION ALL
          SELECT 'BRAF Other',CASE WHEN braf_other THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'NRAS',CASE WHEN nras THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'HRAS',CASE WHEN hras THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'KRAS',CASE WHEN kras THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'RET/PTC1',CASE WHEN ret_ptc1 THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'RET/PTC3',CASE WHEN ret_ptc3 THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'PAX8-PPARG',CASE WHEN pax8_pparg THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'TERT Promoter',CASE WHEN tert_promoter THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'NTRK',CASE WHEN ntrk_any THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'ALK Fusion',CASE WHEN alk_fusion THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'DICER1',CASE WHEN dicer1 THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'PTEN',CASE WHEN pten THEN 1 ELSE 0 END FROM genetic_testing_summary_view UNION ALL
          SELECT 'TP53',CASE WHEN tp53 THEN 1 ELSE 0 END FROM genetic_testing_summary_view
        ) GROUP BY gene HAVING SUM(pos)>0 ORDER BY n_positive DESC"""
        df_genes = sqdf(con,gene_sql)
        if not df_genes.empty:
            GC = {"BRAF V600E":"#2dd4bf","BRAF Other":"#1a8a7a","NRAS":"#38bdf8","HRAS":"#0ea5e9","KRAS":"#0284c7","RET/PTC1":"#a78bfa","RET/PTC3":"#7c3aed","PAX8-PPARG":"#f59e0b","TERT Promoter":"#f43f5e","NTRK":"#34d399","ALK Fusion":"#10b981","DICER1":"#fb923c","PTEN":"#ef4444","TP53":"#dc2626"}
            fig = go.Figure(go.Bar(x=df_genes["pct"],y=df_genes["gene"],orientation="h",marker_color=[GC.get(g,"#2dd4bf") for g in df_genes["gene"]],text=[f"{p}%" for p in df_genes["pct"]],textposition="outside"))
            fig.update_layout(**PL,height=420,xaxis_title="Prevalence (%)",yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig,use_container_width=True)

        st.markdown(sl("Molecular Concordance with Final Pathology"),unsafe_allow_html=True)
        df_conc = sqdf(con,"SELECT molecular_concordance_class AS cls,COUNT(*) AS n FROM genetic_testing_summary_view WHERE molecular_concordance_class IS NOT NULL GROUP BY 1")
        if not df_conc.empty:
            cc = {"TP":"#34d399","TN":"#2dd4bf","FP":"#f59e0b","FN":"#f43f5e"}
            c1,c2 = st.columns(2)
            with c1:
                fig = go.Figure(go.Pie(labels=df_conc["cls"],values=df_conc["n"],hole=0.55,marker=dict(colors=[cc.get(c,"#8892a4") for c in df_conc["cls"]],line=dict(color="#07090f",width=3)),textinfo="label+percent"))
                fig.update_layout(**PL,showlegend=False,height=280,title="Confusion Matrix vs Final Pathology")
                st.plotly_chart(fig,use_container_width=True)
            with c2:
                rm = {r["cls"]:r["n"] for _,r in df_conc.iterrows()}
                tp,fp,tn,fn = rm.get("TP",0),rm.get("FP",0),rm.get("TN",0),rm.get("FN",0)
                t = tp+fp+tn+fn
                if t:
                    for lbl,val in [("Sensitivity",f"{100*tp/max(tp+fn,1):.1f}%"),("Specificity",f"{100*tn/max(tn+fp,1):.1f}%"),("PPV",f"{100*tp/max(tp+fp,1):.1f}%"),("NPV",f"{100*tn/max(tn+fn,1):.1f}%")]:
                        st.markdown(mc(lbl,val),unsafe_allow_html=True)
                        st.markdown("<div style='height:6px'></div>",unsafe_allow_html=True)

    with st.expander("🗃 Browse genetic testing records"):
        df_raw = sqdf(con,"SELECT * FROM genetic_testing LIMIT 500")
        if not df_raw.empty:
            st.dataframe(df_raw,use_container_width=True,height=340)
            st.download_button("⬇ Download CSV",df_raw.to_csv(index=False),"genetic_testing.csv","text/csv")

# ─────────────────────────────────────────────────────────────────────────
# TAB: SPECIMEN DETAILS
# ─────────────────────────────────────────────────────────────────────────
def render_specimen(con):
    has_v = tbl_exists(con,"specimen_detail_view")
    st.markdown(sl("Specimen Weight Distribution"),unsafe_allow_html=True)
    c1,c2 = st.columns(2)
    with c1:
        wt_sql = ("SELECT specimen_weight_g FROM specimen_detail_view WHERE specimen_weight_g IS NOT NULL" if has_v
                  else "SELECT TRY_CAST(specimen_weight_combined AS DOUBLE) AS specimen_weight_g FROM thyroid_weights WHERE specimen_weight_combined IS NOT NULL")
        df_wt = sqdf(con,wt_sql)
        if not df_wt.empty and "specimen_weight_g" in df_wt.columns:
            df_wt = df_wt[df_wt["specimen_weight_g"]<=df_wt["specimen_weight_g"].quantile(0.99)]
            fig = px.histogram(df_wt,x="specimen_weight_g",nbins=40,color_discrete_sequence=["#2dd4bf"])
            fig.update_layout(**PL,height=280,xaxis_title="Weight (g)",yaxis_title="Specimens")
            st.plotly_chart(fig,use_container_width=True)
    with c2:
        lobe_sql = ("SELECT right_lobe_weight_g AS right_lobe,left_lobe_weight_g AS left_lobe FROM specimen_detail_view WHERE right_lobe_weight_g IS NOT NULL AND left_lobe_weight_g IS NOT NULL" if has_v
                    else "SELECT TRY_CAST(right_lobe_weight AS DOUBLE) AS right_lobe,TRY_CAST(left_lobe_weight AS DOUBLE) AS left_lobe FROM thyroid_weights WHERE right_lobe_weight IS NOT NULL AND left_lobe_weight IS NOT NULL")
        df_lobe = sqdf(con,lobe_sql)
        if not df_lobe.empty:
            fig = go.Figure()
            for col,color,name in [("right_lobe","#2dd4bf","Right Lobe"),("left_lobe","#38bdf8","Left Lobe")]:
                if col in df_lobe.columns:
                    q99 = df_lobe[col].quantile(0.99)
                    fig.add_trace(go.Violin(y=df_lobe[df_lobe[col]<=q99][col],name=name,marker_color=color,box_visible=True,meanline_visible=True,opacity=0.7))
            fig.update_layout(**PL,height=280,yaxis_title="Weight (g)")
            st.plotly_chart(fig,use_container_width=True)

    st.markdown(sl("Frozen Sections · Margins · Capsule · Vascular Invasion"),unsafe_allow_html=True)
    c3,c4,c5,c6 = st.columns(4)
    pie_pairs = [
        (c3,"Frozen Section",
         "SELECT frozen_section_obtained AS s,COUNT(*) AS n FROM specimen_detail_view WHERE frozen_section_obtained IS NOT NULL GROUP BY 1" if has_v
         else "SELECT CAST(frozen_section_obtained AS VARCHAR) AS s,COUNT(*) AS n FROM frozen_sections WHERE frozen_section_obtained IS NOT NULL GROUP BY 1",
         {"true":"#2dd4bf","false":"#f43f5e","yes":"#2dd4bf","no":"#f43f5e"}),
        (c4,"Margins",
         "SELECT COALESCE(surgical_margin_status,'Unknown') AS s,COUNT(*) AS n FROM specimen_detail_view GROUP BY 1" if has_v
         else "SELECT COALESCE(CAST(surgical_margins AS VARCHAR),'Unknown') AS s,COUNT(*) AS n FROM tumor_pathology GROUP BY 1",
         {"positive":"#f43f5e","negative":"#34d399"}),
        (c5,"Capsular Invasion",
         "SELECT COALESCE(capsular_invasion,'Unknown') AS s,COUNT(*) AS n FROM specimen_detail_view GROUP BY 1" if has_v
         else "SELECT COALESCE(CAST(capsular_invasion AS VARCHAR),'Unknown') AS s,COUNT(*) AS n FROM tumor_pathology WHERE capsular_invasion IS NOT NULL GROUP BY 1",
         {"true":"#f43f5e","false":"#34d399","yes":"#f43f5e","no":"#34d399"}),
        (c6,"Vascular Invasion",
         "SELECT COALESCE(vascular_invasion,'Unknown') AS s,COUNT(*) AS n FROM specimen_detail_view GROUP BY 1" if has_v
         else "SELECT COALESCE(CAST(tumor_1_vascular_invasion AS VARCHAR),'Unknown') AS s,COUNT(*) AS n FROM tumor_pathology WHERE tumor_1_vascular_invasion IS NOT NULL GROUP BY 1",
         {"true":"#f43f5e","false":"#34d399","yes":"#f43f5e","no":"#34d399"}),
    ]
    for col,title,sql,color_map in pie_pairs:
        with col:
            df_p = sqdf(con,sql)
            if not df_p.empty:
                fig = go.Figure(go.Pie(labels=df_p.iloc[:,0],values=df_p.iloc[:,1],hole=0.5,
                    marker=dict(colors=[color_map.get(str(v).lower(),"#8892a4") for v in df_p.iloc[:,0]],line=dict(color="#07090f",width=2)),textinfo="label+percent"))
                fig.update_layout(**PL,showlegend=False,height=230,title=title,margin=dict(l=8,r=8,t=30,b=8))
                st.plotly_chart(fig,use_container_width=True)

    st.markdown(sl("Frozen Section Concordance with Final Pathology"),unsafe_allow_html=True)
    conc_sql = ("SELECT fs_concordance_with_final AS c,COUNT(*) AS n FROM specimen_detail_view WHERE fs_concordance_with_final IS NOT NULL GROUP BY 1 ORDER BY n DESC" if has_v
                else "SELECT CAST(concordance_with_final AS VARCHAR) AS c,COUNT(*) AS n FROM frozen_sections WHERE concordance_with_final IS NOT NULL GROUP BY 1 ORDER BY n DESC")
    df_fsc = sqdf(con,conc_sql)
    if not df_fsc.empty:
        fig = px.bar(df_fsc,x="c",y="n",color="n",color_continuous_scale=SEQ_TEAL)
        fig.update_layout(**PL,height=260,xaxis_title="",yaxis_title="Cases",coloraxis_showscale=False)
        st.plotly_chart(fig,use_container_width=True)

    with st.expander("🗃 Browse specimen detail records"):
        src = "specimen_detail_view" if has_v else "thyroid_weights"
        df_spec = sqdf(con,f"SELECT * FROM {src} LIMIT 500")
        if not df_spec.empty: st.dataframe(df_spec,use_container_width=True,height=320)

# ─────────────────────────────────────────────────────────────────────────
# TAB: PRE-OP IMAGING
# ─────────────────────────────────────────────────────────────────────────
def render_imaging(con):
    has_v = tbl_exists(con,"preop_imaging_detail_view")
    c1,c2,c3 = st.columns(3)
    for col,(label,sql) in zip([c1,c2,c3],[("Ultrasound Studies","SELECT COUNT(*) FROM ultrasound_reports"),("CT Studies","SELECT COUNT(*) FROM ct_imaging"),("MRI Studies","SELECT COUNT(*) FROM mri_imaging")]):
        with col: st.markdown(mc(label,f"{sqs(con,sql):,}"),unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)

    st.markdown(sl("TI-RADS Distribution — Ultrasound"),unsafe_allow_html=True)
    if has_v:
        ti_sql = """SELECT CASE WHEN us_max_tirads IS NULL THEN 'Not Scored' WHEN us_max_tirads<2 THEN 'TR1 (Benign)' WHEN us_max_tirads<3 THEN 'TR2 (Not Suspicious)' WHEN us_max_tirads<4 THEN 'TR3 (Mildly Suspicious)' WHEN us_max_tirads<5 THEN 'TR4 (Moderately Suspicious)' ELSE 'TR5 (Highly Suspicious)' END AS cat,COUNT(*) AS n FROM preop_imaging_detail_view GROUP BY 1 ORDER BY n DESC"""
    else:
        ti_sql = """SELECT CASE WHEN m<2 THEN 'TR1 (Benign)' WHEN m<3 THEN 'TR2 (Not Suspicious)' WHEN m<4 THEN 'TR3 (Mildly Suspicious)' WHEN m<5 THEN 'TR4 (Moderately Suspicious)' ELSE 'TR5 (Highly Suspicious)' END AS cat,COUNT(*) AS n FROM (SELECT MAX(GREATEST(COALESCE(TRY_CAST(nodule_1_ti_rads AS DOUBLE),0),COALESCE(TRY_CAST(nodule_2_ti_rads AS DOUBLE),0),COALESCE(TRY_CAST(nodule_3_ti_rads AS DOUBLE),0))) AS m FROM ultrasound_reports GROUP BY research_id) GROUP BY 1 ORDER BY n DESC"""
    c_ti,c_comp = st.columns(2)
    with c_ti:
        df_ti = sqdf(con,ti_sql)
        if not df_ti.empty:
            ti_c = {"TR1 (Benign)":"#34d399","TR2 (Not Suspicious)":"#2dd4bf","TR3 (Mildly Suspicious)":"#38bdf8","TR4 (Moderately Suspicious)":"#f59e0b","TR5 (Highly Suspicious)":"#f43f5e","Not Scored":"#4a5568"}
            fig = go.Figure(go.Pie(labels=df_ti.iloc[:,0],values=df_ti.iloc[:,1],hole=0.5,
                marker=dict(colors=[ti_c.get(str(t),"#8892a4") for t in df_ti.iloc[:,0]],line=dict(color="#07090f",width=2)),textinfo="label+percent"))
            fig.update_layout(**PL,showlegend=False,height=320,title="ACR TI-RADS Categories")
            st.plotly_chart(fig,use_container_width=True)
    with c_comp:
        df_comp2 = sqdf(con,"SELECT COALESCE(CAST(nodule_1_composition AS VARCHAR),'Unknown') AS comp,COUNT(*) AS n FROM ultrasound_reports WHERE nodule_1_composition IS NOT NULL AND TRIM(CAST(nodule_1_composition AS VARCHAR))!='' GROUP BY 1 ORDER BY n DESC LIMIT 8")
        if not df_comp2.empty:
            fig = px.bar(df_comp2,x="n",y="comp",orientation="h",color="n",color_continuous_scale=SEQ_TEAL)
            fig.update_layout(**PL,showlegend=False,coloraxis_showscale=False,height=320,xaxis_title="Nodules",yaxis=dict(autorange="reversed",gridcolor="#1e2535"),title="Nodule 1 Composition")
            st.plotly_chart(fig,use_container_width=True)

    st.markdown(sl("Nodule Features — Echogenicity & Calcifications"),unsafe_allow_html=True)
    c5,c6 = st.columns(2)
    with c5:
        df_echo = sqdf(con,"SELECT COALESCE(CAST(nodule_1_echogenicity AS VARCHAR),'Unknown') AS echo,COUNT(*) AS n FROM ultrasound_reports WHERE nodule_1_echogenicity IS NOT NULL AND TRIM(CAST(nodule_1_echogenicity AS VARCHAR))!='' GROUP BY 1 ORDER BY n DESC LIMIT 8")
        if not df_echo.empty:
            fig = px.bar(df_echo,x="echo",y="n",color="n",color_continuous_scale=SEQ_TEAL)
            fig.update_layout(**PL,coloraxis_showscale=False,height=280,xaxis_title="",yaxis_title="Nodules",title="Echogenicity")
            st.plotly_chart(fig,use_container_width=True)
    with c6:
        df_calc = sqdf(con,"SELECT COALESCE(CAST(nodule_1_calcifications AS VARCHAR),'None/Unknown') AS calc,COUNT(*) AS n FROM ultrasound_reports WHERE nodule_1_calcifications IS NOT NULL AND TRIM(CAST(nodule_1_calcifications AS VARCHAR))!='' GROUP BY 1 ORDER BY n DESC LIMIT 8")
        if not df_calc.empty:
            fig = px.bar(df_calc,x="calc",y="n",color="n",color_continuous_scale=SEQ_TEAL)
            fig.update_layout(**PL,coloraxis_showscale=False,height=280,xaxis_title="",yaxis_title="Nodules",title="Calcification Type")
            st.plotly_chart(fig,use_container_width=True)

    st.markdown(sl("CT / MRI Structured Findings"),unsafe_allow_html=True)
    c7,c8 = st.columns(2)
    with c7:
        df_ct_findings = sqdf(con,"""
        SELECT label,n FROM (VALUES
          ('Thyroid Nodule',(SELECT SUM(CASE WHEN LOWER(CAST(thyroid_nodule AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) FROM ct_imaging)),
          ('Enlarged Thyroid',(SELECT SUM(CASE WHEN LOWER(CAST(thyroid_enlarged AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) FROM ct_imaging)),
          ('Goiter Present',(SELECT SUM(CASE WHEN LOWER(CAST(goiter_present AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) FROM ct_imaging)),
          ('Pathologic LN',(SELECT SUM(CASE WHEN LOWER(CAST(pathologic_lymph_nodes AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) FROM ct_imaging)),
          ('Suspicious LN',(SELECT SUM(CASE WHEN LOWER(CAST(lymph_nodes_suspicious AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) FROM ct_imaging))
        ) t(label,n) WHERE n IS NOT NULL AND CAST(n AS INTEGER)>0 ORDER BY CAST(n AS INTEGER) DESC""")
        if not df_ct_findings.empty:
            fig = px.bar(df_ct_findings,x="n",y="label",orientation="h",color="n",color_continuous_scale=SEQ_TEAL)
            fig.update_layout(**PL,showlegend=False,coloraxis_showscale=False,height=260,xaxis_title="CT Reports",yaxis=dict(autorange="reversed"),title="CT Findings")
            st.plotly_chart(fig,use_container_width=True)
    with c8:
        df_mri_findings = sqdf(con,"""
        SELECT label,n FROM (VALUES
          ('Thyroid Nodule',(SELECT SUM(CASE WHEN LOWER(CAST(thyroid_nodule AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) FROM mri_imaging)),
          ('Enlarged Thyroid',(SELECT SUM(CASE WHEN LOWER(CAST(thyroid_enlarged AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) FROM mri_imaging)),
          ('Substernal Extension',(SELECT SUM(CASE WHEN LOWER(CAST(substernal_extension AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) FROM mri_imaging)),
          ('Pathologic LN',(SELECT SUM(CASE WHEN LOWER(CAST(pathologic_lymph_nodes AS VARCHAR)) IN ('true','yes','1') THEN 1 ELSE 0 END) FROM mri_imaging))
        ) t(label,n) WHERE n IS NOT NULL AND CAST(n AS INTEGER)>0 ORDER BY CAST(n AS INTEGER) DESC""")
        if not df_mri_findings.empty:
            fig = px.bar(df_mri_findings,x="n",y="label",orientation="h",color="n",color_continuous_scale=SEQ_TEAL)
            fig.update_layout(**PL,showlegend=False,coloraxis_showscale=False,height=260,xaxis_title="MRI Reports",yaxis=dict(autorange="reversed"),title="MRI Findings")
            st.plotly_chart(fig,use_container_width=True)

    st.markdown(sl("CT Largest Lymph Node Size Distribution"),unsafe_allow_html=True)
    df_ln = sqdf(con,"SELECT TRY_CAST(largest_lymph_node_short_axis_mm AS DOUBLE) AS ln_mm FROM ct_imaging WHERE largest_lymph_node_short_axis_mm IS NOT NULL")
    if not df_ln.empty and "ln_mm" in df_ln.columns:
        df_ln = df_ln.dropna()
        if len(df_ln):
            df_ln = df_ln[df_ln["ln_mm"]<=df_ln["ln_mm"].quantile(0.99)]
            fig = px.histogram(df_ln,x="ln_mm",nbins=30,color_discrete_sequence=["#38bdf8"])
            fig.add_vline(x=10,line_dash="dash",line_color="#f43f5e",annotation_text="10mm threshold",annotation_font_color="#f43f5e")
            fig.update_layout(**PL,height=260,xaxis_title="Short Axis (mm)",yaxis_title="CT Reports")
            st.plotly_chart(fig,use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────
# TAB: COMPLICATIONS
# ─────────────────────────────────────────────────────────────────────────
def render_complications(con):
    st.markdown("#### Post-Operative Complications")
    if tbl_exists(con,"complications"):
        df_comp = sqdf(con,"SELECT COUNT(DISTINCT research_id) AS total_patients,SUM(CASE WHEN LOWER(CAST(rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy AS VARCHAR)) NOT IN ('nan','','0') AND rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy IS NOT NULL THEN 1 ELSE 0 END) AS rln_injuries,SUM(CASE WHEN LOWER(CAST(seroma AS VARCHAR)) NOT IN ('nan','','0') AND seroma IS NOT NULL THEN 1 ELSE 0 END) AS seromas,SUM(CASE WHEN LOWER(CAST(hematoma AS VARCHAR)) NOT IN ('nan','','0') AND hematoma IS NOT NULL THEN 1 ELSE 0 END) AS hematomas FROM complications")
        if not df_comp.empty and df_comp.iloc[0]["total_patients"]>0:
            row = df_comp.iloc[0]
            cols = st.columns(4)
            for col,(l,k) in zip(cols,[("Total w/ Complication Data","total_patients"),("RLN Injuries","rln_injuries"),("Seromas","seromas"),("Hematomas","hematomas")]):
                with col: st.markdown(mc(l,f"{int(row[k]):,}"),unsafe_allow_html=True)
        else: st.info("No complication records found.")
    else: st.info("Complications table not yet loaded. Run ingestion pipeline first.")

# ─────────────────────────────────────────────────────────────────────────
# TAB: AI INSIGHTS
# ─────────────────────────────────────────────────────────────────────────
def render_ai_insights(con):
    st.markdown('<div style="font-family:\'DM Mono\',monospace;font-size:.65rem;color:#2dd4bf;letter-spacing:.1em;text-transform:uppercase;margin-bottom:.4rem">✨ Powered by Claude</div><p style="color:#8892a4;font-size:.86rem;margin-bottom:1.2rem">Ask any clinical research question. Claude receives current cohort statistics and returns evidence-grounded insights.</p>',unsafe_allow_html=True)
    def ctx():
        lines = []
        n = sqs(con,"SELECT COUNT(DISTINCT research_id) FROM master_cohort")
        lines.append(f"Cohort: {n:,} patients.")
        try:
            r = qdf(con,"SELECT AVG(age_at_surgery),MIN(age_at_surgery),MAX(age_at_surgery) FROM master_cohort").iloc[0]
            lines.append(f"Age: mean {r.iloc[0]:.0f}, range {r.iloc[1]:.0f}–{r.iloc[2]:.0f}.")
        except: pass
        for label,sql in [("Stage dist","SELECT histology_1_overall_stage_ajcc8,COUNT(*) FROM tumor_pathology GROUP BY 1 ORDER BY 2 DESC LIMIT 5"),("Histology","SELECT histology_1_type,COUNT(*) FROM tumor_pathology GROUP BY 1 ORDER BY 2 DESC LIMIT 5")]:
            try:
                df = qdf(con,sql)
                lines.append(f"{label}: "+", ".join(f"{r.iloc[0]}={r.iloc[1]:,}" for _,r in df.iterrows())+".")
            except: pass
        if tbl_exists(con,"genetic_testing"):
            ng = sqs(con,"SELECT COUNT(DISTINCT research_id) FROM genetic_testing")
            lines.append(f"Genetic testing: {ng:,} patients.")
        return " ".join(lines)

    presets = ["Summarize the key clinical characteristics of this thyroid cancer cohort","What do the mutation rates suggest about disease aggressiveness?","How does stage distribution compare to population-level thyroid cancer data?","What are the implications of BRAF V600E prevalence for clinical management?","Describe the pre-op imaging findings and their diagnostic utility"]
    st.markdown("**Quick questions:**")
    sel_p = None
    for i,cols_ in enumerate([st.columns(3),st.columns(2)]):
        ps = presets[i*3:(i+1)*3] if i==0 else presets[3:]
        for j,(q,col) in enumerate(zip(ps,cols_)):
            with col:
                if st.button(q[:46]+"…",key=f"p_{i}_{j}"): sel_p = q
    st.markdown("<br>",unsafe_allow_html=True)
    user_q = st.text_input("Or ask your own question:",value=sel_p or "",placeholder="e.g. What's BRAF+ rate in Stage III patients?")
    if st.button("Generate Insight"):
        if not user_q.strip(): st.warning("Please enter a question.")
        else:
            with st.spinner("Asking Claude…"):
                try:
                    c = ctx()
                    resp = requests.post("https://api.anthropic.com/v1/messages",headers={"Content-Type":"application/json"},
                        json={"model":"claude-sonnet-4-20250514","max_tokens":1000,
                              "system":"You are a clinical research analyst specializing in thyroid cancer. Provide evidence-based insights in 2–4 paragraphs. No bullet points.",
                              "messages":[{"role":"user","content":f"Cohort stats:\n{c}\n\nQuestion: {user_q}"}]},timeout=30)
                    resp.raise_for_status()
                    ans = resp.json()["content"][0]["text"]
                    st.markdown(f'<div class="insight-box"><div class="insight-header">✨ Claude · Research Insight</div><p style="color:#f0f4ff;font-size:.9rem;line-height:1.65;margin:0">{ans.replace(chr(10),"<br>")}</p></div>',unsafe_allow_html=True)
                    with st.expander("📋 Context sent to Claude"): st.code(c)
                except Exception as e: st.error(f"API error: {e}")

# ─────────────────────────────────────────────────────────────────────────
# TAB: RECOMMENDATIONS & SENSITIVITIES
# ─────────────────────────────────────────────────────────────────────────
def render_recommendations():
    STUDY = Path(__file__).resolve().parent / "studies" / "proposal2_ete_staging"

    st.markdown(sl("Proposal 2 — ETE Staging Recommendations (2025 ATA Guideline-Aligned)"),unsafe_allow_html=True)

    # Key guideline messages
    st.markdown('<div class="insight-box"><div class="insight-header">2025 ATA Guideline Alignment</div>'
        '<p style="color:#f0f4ff;font-size:.88rem;line-height:1.65;margin:0">'
        '<b>Key message:</b> The 2025 ATA Management Guidelines (Ringel et al., <i>Thyroid</i> 2025;35(8):841–985) '
        'reaffirm AJCC 8th edition exclusion of microscopic ETE from T-staging. Our sensitivity analyses confirm '
        'that mETE is not an independent predictor of higher recurrence risk across all tested subgroups '
        '(age ≥55, tumor ≤4 cm, multiply-imputed, complete-case).<br><br>'
        '<b>Clinical bottom line:</b> mETE alone should not trigger surgical escalation (completion thyroidectomy), '
        'RAI dose intensification, or heightened surveillance in low-risk patients.</p></div>',unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)

    c1,c2,c3 = st.columns(3)
    with c1: st.markdown(mc("mETE OR (Primary)","0.42","95% CI: 0.28–0.64, p<0.001"),unsafe_allow_html=True)
    with c2: st.markdown(mc("mETE OR (Imputed)","0.51","95% CI: 0.34–0.76, p<0.001"),unsafe_allow_html=True)
    with c3: st.markdown(mc("ΔAUC (mETE added)","0.014","Negligible discriminative value"),unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)

    # Sensitivity table
    st.markdown(sl("Sensitivity Analysis — Odds Ratios Across Subgroups"),unsafe_allow_html=True)
    tbl5_path = STUDY / "tables" / "table5_sensitivity.csv"
    if tbl5_path.exists():
        df_tbl5 = pd.read_csv(tbl5_path)
        view_mode = st.radio("View",["Microscopic ETE only","Gross ETE only","All variables"],horizontal=True,key="sens_view")
        if view_mode == "Microscopic ETE only":
            disp = df_tbl5[df_tbl5["Variable"]=="ete_micro"]
        elif view_mode == "Gross ETE only":
            disp = df_tbl5[df_tbl5["Variable"]=="ete_gross"]
        else:
            disp = df_tbl5

        # Interactive table with color coding
        styled = disp.copy()
        st.dataframe(styled,use_container_width=True,hide_index=True,height=min(400,35*len(styled)+40))

        # Interactive OR comparison chart
        mete = df_tbl5[df_tbl5["Variable"]=="ete_micro"].copy()
        if not mete.empty:
            mete["CI_lo_num"] = mete["95% CI"].str.extract(r"\(([0-9.]+)")[0].astype(float)
            mete["CI_hi_num"] = mete["95% CI"].str.extract(r"–([0-9.]+)\)")[0].astype(float)
            mete["OR_num"] = mete["OR"].astype(float)

            fig = go.Figure()
            for i,(_,row) in enumerate(mete.iterrows()):
                fig.add_trace(go.Scatter(
                    x=[row["OR_num"]], y=[row["Subgroup"]],
                    error_x=dict(type="data",symmetric=False,
                        array=[row["CI_hi_num"]-row["OR_num"]],
                        arrayminus=[row["OR_num"]-row["CI_lo_num"]]),
                    mode="markers", marker=dict(size=12,color="#f59e0b",line=dict(color="#07090f",width=1)),
                    name=row["Subgroup"],showlegend=False,
                    hovertemplate=f"<b>{row['Subgroup']}</b><br>OR: {row['OR_num']:.2f} ({row['CI_lo_num']:.2f}–{row['CI_hi_num']:.2f})<br>p: {row['p-value']}<extra></extra>"
                ))
            fig.add_vline(x=1,line_dash="dash",line_color="#f43f5e",annotation_text="OR = 1.0",annotation_font_color="#f43f5e")
            fig.update_layout(**PL,height=300,xaxis_title="Odds Ratio (Microscopic ETE)",title="mETE OR Across Sensitivity Analyses")
            st.plotly_chart(fig,use_container_width=True)

        st.download_button("⬇ Download Sensitivity Table",df_tbl5.to_csv(index=False),"table5_sensitivity.csv","text/csv")
    else:
        st.info("Run `proposal2_recommendations.py` to generate sensitivity data.")

    # Forest plot
    st.markdown(sl("Forest Plot — ETE Odds Ratios"),unsafe_allow_html=True)
    fig6_path = STUDY / "figures" / "fig6_forest_plot_ORs.png"
    if fig6_path.exists():
        st.image(str(fig6_path),caption="Figure 6. Forest plot of microscopic and gross ETE odds ratios across sensitivity analyses.",use_container_width=True)
    else:
        st.info("Forest plot not yet generated.")

    # KM curves bonus
    fig7_path = STUDY / "figures" / "fig7_kaplan_meier.png"
    if fig7_path.exists():
        st.markdown(sl("Kaplan–Meier Curves (Supplementary)"),unsafe_allow_html=True)
        st.image(str(fig7_path),caption="Figure 7. Kaplan–Meier event-free curves by ETE group (proxy follow-up).",use_container_width=True)

    # Recommendations summary
    st.markdown(sl("Clinical Recommendations Summary"),unsafe_allow_html=True)
    recs = [
        ("🔬 Pathology Reporting","Explicitly distinguish microscopic from gross ETE in all surgical pathology reports. Specify anatomic extent of gross ETE (T3b vs T4a)."),
        ("🔪 Surgical Planning","Gross ETE → total thyroidectomy + neck imaging. mETE alone → lobectomy feasible in low-risk patients (tumor ≤4 cm, N0, no aggressive histology)."),
        ("☢️ Adjuvant Therapy","mETE alone should not trigger RAI dose escalation. Low-risk mETE-only patients may be considered for observation or low-dose ablation."),
        ("📊 Risk Stratification","Gross ETE remains a high-risk feature. mETE does not independently predict higher recurrence risk (OR 0.42–0.51 across all specifications)."),
        ("🔮 Future Directions","Multi-center validation with time-to-event endpoints; BRAF/TERT integration; standardized mETE reporting criteria."),
    ]
    for icon_title,desc in recs:
        st.markdown(f'<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:.8rem 1rem;margin-bottom:.5rem">'
            f'<div style="font-weight:600;color:var(--teal);margin-bottom:4px">{icon_title}</div>'
            f'<div style="color:var(--text-mid);font-size:.85rem;line-height:1.5">{desc}</div></div>',unsafe_allow_html=True)

    # Full recommendations document
    rec_path = STUDY / "recommendations.md"
    if rec_path.exists():
        with st.expander("📄 Full Recommendations Document"):
            st.markdown(rec_path.read_text())


# ─────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────
def main():
    if not _ensure_token():
        st.title("🔬 Thyroid Cohort Explorer")
        st.error("**MotherDuck token not found.**\n\nSet `MOTHERDUCK_TOKEN` in `.streamlit/secrets.toml` or as an environment variable.")
        st.stop()
    try: con = _get_con()
    except Exception as exc: st.error(f"Failed to connect to MotherDuck: {exc}"); st.stop()

    ci,ct = st.columns([1,11])
    with ci: st.markdown('<div style="font-size:2.8rem;margin-top:4px">🔬</div>',unsafe_allow_html=True)
    with ct: st.markdown('<h1 style="margin:0;padding:0">THYROID_2026</h1><p style="margin:2px 0 0 2px;color:#8892a4;font-size:.78rem;font-family:\'DM Mono\',monospace;letter-spacing:.08em">THYROID CANCER RESEARCH LAKEHOUSE · 11,673 PATIENTS · 13 TABLES · MOTHERDUCK</p>',unsafe_allow_html=True)
    st.markdown("---")

    df_full = sqdf(con,"SELECT * FROM advanced_features_view")
    if df_full.empty: st.error("Could not load `advanced_features_view`."); st.stop()
    df_filt = build_sidebar(df_full)

    (t_ov,t_ex,t_vz,t_adv,t_gen,t_spec,t_img,t_comp,t_rec,t_ai) = st.tabs([
        "📊 Overview","🗃 Data Explorer","📈 Visualizations","🧬 Advanced",
        "🔬 Genetics & Molecular","🫀 Specimen Details","📡 Pre-Op Imaging",
        "⚕ Complications","📋 Recommendations & Sensitivities","✨ AI Insights"
    ])
    with t_ov:   render_overview(con)
    with t_ex:   render_explorer(df_filt)
    with t_vz:   render_viz(con)
    with t_adv:  render_advanced(con)
    with t_gen:  render_genetics(con)
    with t_spec: render_specimen(con)
    with t_img:  render_imaging(con)
    with t_comp: render_complications(con)
    with t_rec:  render_recommendations()
    with t_ai:   render_ai_insights(con)

    st.markdown("---")
    st.caption(f"**Data source:** MotherDuck `{DATABASE}` · Share: `{SHARE_PATH[:40]}…` · Loaded: {datetime.now():%Y-%m-%d %H:%M} · Built with Streamlit + DuckDB + Plotly + Claude")

if __name__ == "__main__":
    main()
