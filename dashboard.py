#!/usr/bin/env python3
"""
Thyroid Cohort Explorer — Enhanced Dashboard v3
Powered by MotherDuck cloud DuckDB (Business trial).

New in v3:
  • 🕐 Patient Timeline Explorer   — master_timeline + Tg/TSH trend per patient
  • 📋 Extracted Clinical Events   — searchable extracted_clinical_events_v4
  • 🔍 QA Dashboard                — qa_issues summary + drill-down
  • 📉 Risk & Survival             — Kaplan-Meier with lifelines + stratification
  • 🧩 Advanced Features v3        — 60+ engineered features, full column selector
  • Sidebar: surgery count + QA status filters
  • MotherDuck compute-tier controls (Jumbo toggle)

Run locally:
    export MOTHERDUCK_TOKEN='your_token'
    streamlit run dashboard.py
"""
from __future__ import annotations
import io
import os, sys, requests
from datetime import datetime
from pathlib import Path

import duckdb, pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from lifelines import KaplanMeierFitter
    HAS_LIFELINES = True
except ImportError:
    HAS_LIFELINES = False

try:
    import openpyxl  # noqa: F401
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

sys.path.insert(0, str(Path(__file__).resolve().parent))
from motherduck_client import MotherDuckClient, MotherDuckConfig

from app.cohort_qc import render_cohort_qc
from app.patient_audit import render_patient_audit
from app.review_histology import render_review_histology
from app.review_molecular import render_review_molecular
from app.review_rai import render_review_rai
from app.review_timeline import render_review_timeline
from app.review_queue import render_review_queue
from app.diagnostics import render_diagnostics
from app.extraction_completeness import render_extraction_completeness
from app.molecular_dashboard import render_molecular_dashboard
from app.rai_dashboard import render_rai_dashboard
from app.imaging_nodule_dashboard import render_imaging_nodule_dashboard
from app.operative_dashboard import render_operative_dashboard
from app.adjudication_summary import render_adjudication_summary
from app.validation_engine import render_validation_engine

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

# Read-replica note: for read-only dashboards, append ?access_mode=read_only
# to the connection string. MotherDuck read replicas reduce load on the
# primary instance during concurrent dashboard access.

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

def multi_export(df, prefix, key_sfx=""):
    """Render CSV + Excel + Parquet download buttons in a 3-column row."""
    ts = datetime.now().strftime("%Y%m%d")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("⬇ CSV", df.to_csv(index=False),
                           f"{prefix}_{ts}.csv", "text/csv",
                           key=f"csv_{key_sfx}")
    with c2:
        if HAS_OPENPYXL:
            buf = io.BytesIO()
            df.to_excel(buf, index=False, engine="openpyxl")
            st.download_button(
                "⬇ Excel", buf.getvalue(), f"{prefix}_{ts}.xlsx",
                "application/vnd.openxmlformats-officedocument"
                ".spreadsheetml.sheet", key=f"xlsx_{key_sfx}")
        else:
            st.caption("Install openpyxl for Excel export")
    with c3:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        st.download_button("⬇ Parquet", buf.getvalue(),
                           f"{prefix}_{ts}.parquet",
                           "application/octet-stream",
                           key=f"pq_{key_sfx}")

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
        st.markdown(sl("🔧 Timeline & QA"), unsafe_allow_html=True)
        sel_surg_count = "All"
        if "total_surgeries" in df.columns:
            surg_vals = sorted([int(x) for x in df["total_surgeries"].dropna().unique() if x > 0])
            if surg_vals:
                sel_surg_count = st.selectbox("Surgery count", ["All"] + [str(s) for s in surg_vals])
        qa_mode = "All"
        if "qa_issue_count" in df.columns:
            qa_mode = st.radio("QA status", ["All", "Clean only", "Flagged only"], horizontal=True)
        st.markdown(sl("📅 Days Since Surgery"), unsafe_allow_html=True)
        days_opts = ["All", "<30d", "30-90d", "90-365d", ">1y"]
        sel_days = st.radio("Days range", days_opts, horizontal=True,
                            key="days_filt")
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
    if sel_surg_count != "All" and "total_surgeries" in f.columns:
        f = f[f["total_surgeries"] == int(sel_surg_count)]
    if qa_mode == "Clean only" and "qa_issue_count" in f.columns:
        f = f[f["qa_issue_count"] == 0]
    elif qa_mode == "Flagged only" and "qa_issue_count" in f.columns:
        f = f[f["qa_issue_count"] > 0]
    if sel_days != "All" and "latest_tg_days_from_surgery" in f.columns:
        days_col = f["latest_tg_days_from_surgery"].abs()
        if sel_days == "<30d":
            f = f[days_col.notna() & (days_col < 30)]
        elif sel_days == "30-90d":
            f = f[days_col.notna() & (days_col >= 30) & (days_col < 90)]
        elif sel_days == "90-365d":
            f = f[days_col.notna() & (days_col >= 90) & (days_col < 365)]
        elif sel_days == ">1y":
            f = f[days_col.notna() & (days_col >= 365)]
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

    rescue_tbl = "date_rescue_rate_summary" if tbl_exists(con, "date_rescue_rate_summary") else (
        "md_date_rescue_rate_summary" if tbl_exists(con, "md_date_rescue_rate_summary") else None)
    if rescue_tbl:
        st.markdown(sl("Date Rescue Rate by Domain"),unsafe_allow_html=True)
        df_r = sqdf(con, f"SELECT * FROM {rescue_tbl} WHERE entity_table != 'ALL_DOMAINS' ORDER BY rescue_rate_pct DESC")
        df_all = sqdf(con, f"SELECT * FROM {rescue_tbl} WHERE entity_table = 'ALL_DOMAINS'")
        if not df_all.empty:
            r = df_all.iloc[0]
            c1,c2,c3 = st.columns(3)
            with c1: st.markdown(mc("Overall Rescue Rate",f"{r.get('rescue_rate_pct',0):.1f}%"),unsafe_allow_html=True)
            with c2: st.markdown(mc("Rescued Entities",f"{int(r.get('rescued',0)):,}",f"of {int(r.get('total_entities',0)):,}"),unsafe_allow_html=True)
            with c3: st.markdown(mc("Avg Confidence (rescued)",f"{r.get('avg_confidence_rescued',0):.0f}/100"),unsafe_allow_html=True)
        if not df_r.empty:
            fig_r = go.Figure(go.Bar(
                y=df_r["entity_table"].str.replace("note_entities_","",regex=False),
                x=df_r["rescue_rate_pct"], orientation="h", marker_color="#2dd4bf",
                text=df_r["rescue_rate_pct"].apply(lambda v: f"{v:.1f}%"), textposition="auto",
                customdata=df_r["avg_confidence_rescued"],
                hovertemplate="<b>%{y}</b><br>Rescue rate: %{x:.1f}%<br>Avg confidence: %{customdata:.0f}<extra></extra>"))
            fig_r.update_layout(**PL, height=240, xaxis_title="% Dates Rescued",
                                yaxis=dict(autorange="reversed",gridcolor="#1e2535",linecolor="#1e2535",zerolinecolor="#1e2535"))
            st.plotly_chart(fig_r,use_container_width=True)

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
# TAB: EXPANDED COHORT
# ─────────────────────────────────────────────────────────────────────────
def render_expanded_cohort():
    STUDY = Path(__file__).resolve().parent / "studies" / "proposal2_ete_staging"

    st.markdown(sl("Cohort Expansion — All PTC Variants (N = 3,278)"), unsafe_allow_html=True)

    st.markdown(
        '<div class="insight-box"><div class="insight-header">Expansion Summary</div>'
        '<p style="color:#f0f4ff;font-size:.88rem;line-height:1.65;margin:0">'
        'The original Proposal 2 analysis was limited to <b>596 classic-variant PTC</b> patients '
        '(complete-case). This expansion evaluates the robustness of the mETE non-significance '
        'finding across <b>4 cohort definitions</b> ranging from the original classic-only cohort '
        'to all PTC variants (N=3,278), including aggressive subtypes (tall cell, columnar, solid, '
        'diffuse sclerosing). <b>Main conclusion unchanged:</b> mETE OR remains 0.50–0.60 across all cohorts.</p></div>',
        unsafe_allow_html=True,
    )
    st.markdown("<br>", unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(mc("Cohort A: All PTC", "3,278", "mETE OR = 0.60, p<0.001"), unsafe_allow_html=True)
    with c2:
        st.markdown(mc("Cohort B: Classic+Unspec", "2,166", "mETE OR = 0.52, p<0.001"), unsafe_allow_html=True)
    with c3:
        st.markdown(mc("Cohort C: Original", "589", "mETE OR = 0.50, p<0.001"), unsafe_allow_html=True)
    with c4:
        st.markdown(mc("Cohort D: Relaxed", "3,278", "mETE OR = 0.60, p<0.001"), unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    tbl7_path = STUDY / "tables" / "table7_cohort_comparison.csv"
    if tbl7_path.exists():
        st.markdown(sl("Cohort Comparison — Including Aggressive Variant Safety Check"), unsafe_allow_html=True)
        df_tbl7 = pd.read_csv(tbl7_path)
        st.dataframe(df_tbl7, use_container_width=True, hide_index=True)

        mete_data = df_tbl7[~df_tbl7["Cohort"].str.startswith("Safety")].copy()
        if not mete_data.empty and "mETE_OR" in mete_data.columns:
            mete_data["OR_num"] = pd.to_numeric(mete_data["mETE_OR"], errors="coerce")
            mete_data["CI_lo"] = mete_data["mETE_95%_CI"].str.extract(r"\(([0-9.]+)")[0].astype(float)
            mete_data["CI_hi"] = mete_data["mETE_95%_CI"].str.extract(r"–([0-9.]+)\)")[0].astype(float)

            fig = go.Figure()
            for i, (_, row) in enumerate(mete_data.iterrows()):
                fig.add_trace(go.Scatter(
                    x=[row["OR_num"]], y=[row["Cohort"]],
                    error_x=dict(type="data", symmetric=False,
                                 array=[row["CI_hi"] - row["OR_num"]],
                                 arrayminus=[row["OR_num"] - row["CI_lo"]]),
                    mode="markers",
                    marker=dict(size=14, color="#f59e0b",
                                line=dict(color="#07090f", width=1)),
                    name=row["Cohort"], showlegend=False,
                    hovertemplate=(
                        f"<b>{row['Cohort']}</b> (N={row['N']})<br>"
                        f"OR: {row['OR_num']:.2f} ({row['CI_lo']:.2f}–{row['CI_hi']:.2f})<br>"
                        f"Method: {row['Method']}<extra></extra>"
                    ),
                ))

            safety_data = df_tbl7[df_tbl7["Cohort"].str.startswith("Safety")].copy()
            safety_data["OR_num"] = pd.to_numeric(safety_data["mETE_OR"], errors="coerce")
            safety_data["CI_lo"] = safety_data["mETE_95%_CI"].str.extract(r"\(([0-9.]+)")[0].astype(float)
            safety_data["CI_hi"] = safety_data["mETE_95%_CI"].str.extract(r"–([0-9.]+)\)")[0].astype(float)
            for _, row in safety_data.iterrows():
                if pd.notna(row["OR_num"]):
                    fig.add_trace(go.Scatter(
                        x=[row["OR_num"]], y=[row["Cohort"]],
                        error_x=dict(type="data", symmetric=False,
                                     array=[min(row["CI_hi"], 5) - row["OR_num"]],
                                     arrayminus=[row["OR_num"] - row["CI_lo"]]),
                        mode="markers",
                        marker=dict(size=14, color="#f43f5e", symbol="diamond",
                                    line=dict(color="#07090f", width=1)),
                        name=row["Cohort"], showlegend=False,
                    ))

            fig.add_vline(x=1, line_dash="dash", line_color="#f43f5e",
                          annotation_text="OR = 1.0",
                          annotation_font_color="#f43f5e")
            fig.update_layout(
                **PL, height=350,
                xaxis_title="Odds Ratio (Microscopic ETE)",
                title="mETE OR Across Expanded Cohorts (all < 1.0 → non-prognostic)",
            )
            st.plotly_chart(fig, use_container_width=True)

        st.download_button(
            "⬇ Download Cohort Comparison",
            df_tbl7.to_csv(index=False),
            "table7_cohort_comparison.csv", "text/csv",
        )

    st.markdown("<br>", unsafe_allow_html=True)

    fig8_path = STUDY / "figures" / "fig8_cohort_size_flow.png"
    fig9_path = STUDY / "figures" / "fig9_forest_expanded.png"

    c1, c2 = st.columns(2)
    with c1:
        if fig8_path.exists():
            st.markdown(sl("Cohort Size Flow"), unsafe_allow_html=True)
            st.image(str(fig8_path),
                     caption="Figure 8. Cohort size comparison across expansion strategies.",
                     use_container_width=True)
    with c2:
        if fig9_path.exists():
            st.markdown(sl("Forest Plot — Expanded Cohorts"), unsafe_allow_html=True)
            st.image(str(fig9_path),
                     caption="Figure 9. mETE ORs across all expanded cohorts (MI and CC).",
                     use_container_width=True)

    st.markdown(sl("Variant Breakdown — N Gain"), unsafe_allow_html=True)
    variant_data = pd.DataFrame({
        "Variant": ["Classic/Unspecified", "Follicular", "Tall cell",
                     "Oncocytic/Warthin-like", "Diffuse sclerosing",
                     "Solid", "Cribriform-morular", "Columnar cell"],
        "N": [2166, 769, 166, 140, 14, 12, 6, 5],
        "Type": ["Indolent", "Indolent", "Aggressive", "Indolent",
                 "Aggressive", "Aggressive", "Indolent", "Aggressive"],
    })
    fig = go.Figure(go.Bar(
        x=variant_data["N"], y=variant_data["Variant"],
        orientation="h",
        marker_color=["#2dd4bf" if t == "Indolent" else "#f43f5e"
                       for t in variant_data["Type"]],
        text=[f"N={n:,}" for n in variant_data["N"]],
        textposition="outside",
    ))
    fig.update_layout(
        **PL, height=360, xaxis_title="Number of Patients",
        yaxis=dict(autorange="reversed", gridcolor="#1e2535"),
        title="PTC Variant Distribution (Red = Aggressive)",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        '<div class="insight-box"><div class="insight-header">Clinical Safety Note</div>'
        '<p style="color:#f0f4ff;font-size:.88rem;line-height:1.65;margin:0">'
        'Aggressive variants (tall cell, columnar, solid, diffuse sclerosing; N=197) were '
        'specifically tested as a safety subgroup. The mETE OR in aggressive variants was '
        '<b>0.94 (0.35–2.54, p=0.901)</b> — the wide CI reflects limited power, but the '
        'point estimate does not reverse the finding. mETE remains non-prognostic even '
        'among histologically aggressive PTC subtypes.</p></div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────
# TAB: PATIENT TIMELINE EXPLORER
# ─────────────────────────────────────────────────────────────────────────
def render_timeline(con):
    if not tbl_exists(con, "master_timeline"):
        st.info("Timeline data not available. Run script 11 first.", icon="🕐")
        return
    st.markdown(sl("Select Patient"), unsafe_allow_html=True)
    c1, c2 = st.columns([1, 3])
    with c1:
        pid_input = st.number_input("Research ID", min_value=1, step=1, value=1, key="tl_pid")
    with c2:
        multi_df = sqdf(con, "SELECT research_id, total_surgeries FROM master_timeline "
                         "WHERE total_surgeries > 1 GROUP BY 1, 2 ORDER BY total_surgeries DESC LIMIT 20")
        if not multi_df.empty:
            st.caption("Patients with multiple surgeries:")
            st.dataframe(multi_df, height=120, hide_index=True)
    pid = int(pid_input)
    tl = sqdf(con, f"SELECT * FROM master_timeline WHERE research_id = {pid} ORDER BY surgery_number")
    if tl.empty:
        st.warning(f"No timeline data for patient {pid}")
        return
    st.markdown(sl("Surgery Timeline"), unsafe_allow_html=True)
    n_surg = len(tl)
    cols = st.columns(min(n_surg, 4))
    for i, (_, row) in enumerate(tl.iterrows()):
        with cols[i % len(cols)]:
            delta = f"{row['surgery_type']}"
            if row["surgery_number"] > 1 and pd.notna(row.get("days_since_prior_surgery")):
                delta += f" · {int(row['days_since_prior_surgery'])}d from prior"
            st.markdown(mc(f"Surgery {int(row['surgery_number'])}", str(row["surgery_date"]), delta), unsafe_allow_html=True)
    if tbl_exists(con, "extracted_clinical_events_v4"):
        labs = sqdf(con, f"""
            SELECT event_subtype, event_value, followup_date,
                   days_since_nearest_surgery, nearest_surgery_number
            FROM extracted_clinical_events_v4
            WHERE research_id = {pid}
              AND event_type = 'lab'
              AND event_subtype IN ('thyroglobulin', 'tsh')
              AND event_value IS NOT NULL
            ORDER BY followup_date, days_since_nearest_surgery""")
        if not labs.empty:
            st.markdown(sl("Thyroglobulin / TSH Trend"), unsafe_allow_html=True)
            fig = go.Figure()
            for sub, color, name in [("thyroglobulin", "#2dd4bf", "Tg (ng/mL)"), ("tsh", "#f59e0b", "TSH (mIU/L)")]:
                s = labs[labs["event_subtype"] == sub]
                if not s.empty:
                    x = s["followup_date"] if s["followup_date"].notna().any() else s["days_since_nearest_surgery"]
                    fig.add_trace(go.Scatter(x=x, y=s["event_value"], mode="lines+markers",
                                             name=name, marker_color=color, line=dict(color=color)))
            for _, r in tl.iterrows():
                if pd.notna(r["surgery_date"]):
                    fig.add_vline(x=r["surgery_date"], line_dash="dash", line_color="#f43f5e",
                                  annotation_text=f"Surg {int(r['surgery_number'])}",
                                  annotation_font_color="#f43f5e", annotation_font_size=10)
            fig.update_layout(**PL, height=400, xaxis_title="Date",
                              yaxis_title="Value (log scale)", yaxis_type="log")
            st.plotly_chart(fig, use_container_width=True)
        events = sqdf(con, f"""
            SELECT event_type, event_subtype, event_value, event_unit, event_text,
                   days_since_nearest_surgery, nearest_surgery_number, confidence_score
            FROM extracted_clinical_events_v4
            WHERE research_id = {pid}
            ORDER BY days_since_nearest_surgery NULLS LAST""")
        if not events.empty:
            st.markdown(sl("All Clinical Events"), unsafe_allow_html=True)
            st.dataframe(events, use_container_width=True, hide_index=True, height=400)

# ─────────────────────────────────────────────────────────────────────────
# TAB: EXTRACTED CLINICAL EVENTS
# ─────────────────────────────────────────────────────────────────────────
def render_events(con):
    if not tbl_exists(con, "extracted_clinical_events_v4"):
        st.info("Extracted events not available. Run script 11 first.", icon="📋")
        return
    c1, c2, c3 = st.columns(3)
    with c1:
        types = sqdf(con, "SELECT DISTINCT event_type FROM extracted_clinical_events_v4 "
                          "WHERE event_type IS NOT NULL ORDER BY 1")
        type_opts = types["event_type"].tolist() if not types.empty else []
        sel_type = st.selectbox("Event type", ["All"] + type_opts, key="ev_type")
    with c2:
        if sel_type != "All":
            subs = sqdf(con, f"SELECT DISTINCT event_subtype FROM extracted_clinical_events_v4 "
                             f"WHERE event_type = '{sel_type}' AND event_subtype IS NOT NULL ORDER BY 1")
            sub_opts = subs["event_subtype"].tolist() if not subs.empty else []
        else:
            sub_opts = []
        sel_sub = st.selectbox("Subtype", ["All"] + sub_opts, key="ev_sub")
    with c3:
        srch = st.text_input("Search text", placeholder="Filter event text…", key="ev_search")
    wheres = ["1=1"]
    if sel_type != "All":
        wheres.append(f"event_type = '{sel_type}'")
    if sel_sub != "All":
        wheres.append(f"event_subtype = '{sel_sub}'")
    where = " AND ".join(wheres)
    df_ev = sqdf(con, f"SELECT * FROM extracted_clinical_events_v4 WHERE {where} "
                       "ORDER BY research_id, days_since_nearest_surgery NULLS LAST LIMIT 5000")
    if srch:
        mask = df_ev.apply(lambda s: s.astype(str).str.contains(srch, case=False, na=False)).any(axis=1)
        df_ev = df_ev[mask]
    st.markdown(f"**{len(df_ev):,} events** (max 5,000)")
    st.dataframe(df_ev, use_container_width=True, height=500, hide_index=True)
    st.download_button("⬇ Download filtered events", df_ev.to_csv(index=False),
                       f"events_{datetime.now():%Y%m%d}.csv", "text/csv", key="ev_dl")

# ─────────────────────────────────────────────────────────────────────────
# TAB: QA DASHBOARD
# ─────────────────────────────────────────────────────────────────────────
def render_qa_dashboard(con):
    qa_tbl = "qa_issues_v2" if tbl_exists(con, "qa_issues_v2") else (
        "md_qa_issues_v2" if tbl_exists(con, "md_qa_issues_v2") else (
        "qa_issues" if tbl_exists(con, "qa_issues") else None))
    if qa_tbl is None:
        st.info("QA data not available. Run script 11 or 25 first.", icon="🔍")
        return
    total = sqs(con, f"SELECT COUNT(*) FROM {qa_tbl}")
    patients_flagged = sqs(con, f"SELECT COUNT(DISTINCT research_id) FROM {qa_tbl}")
    by_sev = sqdf(con, f"SELECT severity, COUNT(*) AS n FROM {qa_tbl} GROUP BY 1 ORDER BY n DESC")
    by_check = sqdf(con, f"SELECT check_id, severity, COUNT(*) AS n FROM {qa_tbl} GROUP BY 1, 2 ORDER BY n DESC")
    st.markdown(sl("QA Summary"), unsafe_allow_html=True)
    sev_map = {r["severity"]: r["n"] for _, r in by_sev.iterrows()} if not by_sev.empty else {}
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.markdown(mc("Total Issues", f"{total:,}"), unsafe_allow_html=True)
    with c2: st.markdown(mc("Patients Flagged", f"{patients_flagged:,}"), unsafe_allow_html=True)
    with c3: st.markdown(mc("Errors", f'{sev_map.get("error", 0):,}'), unsafe_allow_html=True)
    with c4: st.markdown(mc("Warnings", f'{sev_map.get("warning", 0):,}'), unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        if not by_sev.empty:
            sc = {"error": "#f43f5e", "warning": "#f59e0b", "info": "#38bdf8"}
            fig = go.Figure(go.Pie(
                labels=by_sev["severity"], values=by_sev["n"], hole=0.55,
                marker=dict(colors=[sc.get(s, "#8892a4") for s in by_sev["severity"]],
                            line=dict(color="#07090f", width=3)),
                textinfo="label+percent"))
            fig.update_layout(**PL, showlegend=False, height=300, title="Issues by Severity")
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        ck = (by_check.groupby("check_id")["n"].sum().reset_index().sort_values("n", ascending=True)
              if not by_check.empty else pd.DataFrame())
        if not ck.empty:
            fig = px.bar(ck, x="n", y="check_id", orientation="h", color="n", color_continuous_scale=SEQ_TEAL)
            fig.update_layout(**PL, showlegend=False, coloraxis_showscale=False, height=300,
                              xaxis_title="Issues", yaxis=dict(autorange="reversed"),
                              title="Issues by Check Type")
            st.plotly_chart(fig, use_container_width=True)
    st.markdown(sl("Issue Details"), unsafe_allow_html=True)
    sev_f = st.selectbox("Filter by severity", ["All", "error", "warning", "info"], key="qa_sev")
    where_qa = f"WHERE severity = '{sev_f}'" if sev_f != "All" else ""
    df_issues = sqdf(con, f"SELECT * FROM {qa_tbl} {where_qa} ORDER BY severity, check_id, research_id LIMIT 2000")
    st.dataframe(df_issues, use_container_width=True, height=400, hide_index=True)
    multi_export(df_issues, "qa_issues", key_sfx="qa_main")

    # ── Cross-File Validation Tables (from script 11.5) ──
    st.markdown(sl("Cross-File Validation (Script 11.5)"), unsafe_allow_html=True)

    if tbl_exists(con, "qa_laterality_mismatches"):
        with st.expander("Check A — Laterality Consistency (Op vs Path)", expanded=False):
            lat_df = sqdf(con,
                "SELECT laterality_flag, COUNT(*) AS n "
                "FROM qa_laterality_mismatches GROUP BY 1 ORDER BY 2 DESC")
            if not lat_df.empty:
                cs = st.columns(len(lat_df))
                flag_colors = {"MATCH": "green", "LATERALITY_MISMATCH": "red",
                               "INCOMPLETE": "orange"}
                for i, (_, row) in enumerate(lat_df.iterrows()):
                    with cs[i]:
                        st.metric(row["laterality_flag"], f"{int(row['n']):,}")
            lat_detail = sqdf(con,
                "SELECT * FROM qa_laterality_mismatches "
                "WHERE laterality_flag = 'LATERALITY_MISMATCH' "
                "ORDER BY research_id LIMIT 500")
            if not lat_detail.empty:
                st.caption(f"{len(lat_detail):,} mismatches shown (max 500)")
                st.dataframe(lat_detail, use_container_width=True,
                             height=300, hide_index=True)
                multi_export(lat_detail, "laterality_mismatches",
                             key_sfx="lat")
    else:
        st.info("Laterality data not available. Run script 11.5 first.",
                icon="🔍")

    if tbl_exists(con, "qa_report_matching"):
        with st.expander("Check B — Report Matching (FNA↔Path + US↔Op)", expanded=False):
            rm_df = sqdf(con, "SELECT * FROM qa_report_matching")
            if not rm_df.empty:
                for _, row in rm_df.iterrows():
                    ct = row.get("check_type", "")
                    tp = int(row.get("total_pairs", 0))
                    m = int(row.get("matched", 0))
                    pct = row.get("match_pct", 0)
                    label = "FNA → Pathology" if ct == "fna_path" else "US → Operative"
                    st.markdown(
                        mc(label, f"{pct}%",
                           f"{m:,} / {tp:,} pairs matched"),
                        unsafe_allow_html=True)
                st.markdown("<br>", unsafe_allow_html=True)
                st.dataframe(rm_df, use_container_width=True, hide_index=True)
    else:
        st.info("Report matching data not available. Run script 11.5 first.",
                icon="🔍")

    if tbl_exists(con, "qa_missing_demographics"):
        with st.expander("Check C — Missing Demographics", expanded=False):
            demo_summary = sqdf(con, """
                SELECT
                    SUM(CASE WHEN age_flag = 'MISSING_AGE' THEN 1 ELSE 0 END)
                        AS missing_age,
                    SUM(CASE WHEN sex_flag = 'MISSING_SEX' THEN 1 ELSE 0 END)
                        AS missing_sex,
                    SUM(CASE WHEN race_flag = 'MISSING_RACE' THEN 1 ELSE 0 END)
                        AS missing_race,
                    COUNT(*) AS total_flagged
                FROM qa_missing_demographics""")
            if not demo_summary.empty:
                r = demo_summary.iloc[0]
                cs = st.columns(4)
                for c, (lbl, k) in zip(cs, [
                    ("Total Flagged", "total_flagged"),
                    ("Missing Age", "missing_age"),
                    ("Missing Sex", "missing_sex"),
                    ("Missing Race", "missing_race"),
                ]):
                    with c:
                        st.markdown(mc(lbl, f"{int(r[k]):,}"),
                                    unsafe_allow_html=True)
            demo_df = sqdf(con,
                "SELECT * FROM qa_missing_demographics "
                "WHERE age_flag != 'OK' OR sex_flag != 'OK' "
                "ORDER BY research_id LIMIT 1000")
            if not demo_df.empty:
                st.markdown("<br>", unsafe_allow_html=True)
                st.dataframe(demo_df, use_container_width=True,
                             height=300, hide_index=True)
                multi_export(demo_df, "missing_demographics",
                             key_sfx="demo")
    else:
        st.info("Demographics QA not available. Run script 11.5 first.",
                icon="🔍")

    # ── V2 QA Validation Links ──
    st.markdown(sl("V2 QA Validation (Script 25)"), unsafe_allow_html=True)
    qa_v2_links = [
        ("qa_date_completeness_v2", "md_date_quality_summary_v2", "Date Completeness"),
        ("qa_summary_by_domain_v2", "md_qa_summary_v2", "QA Summary by Domain"),
        ("qa_high_priority_review_v2", "md_qa_high_priority_v2", "High Priority Review"),
    ]
    for local_v, md_v, label in qa_v2_links:
        view = local_v if tbl_exists(con, local_v) else (
            md_v if tbl_exists(con, md_v) else None)
        if view:
            cnt = sqs(con, f"SELECT COUNT(*) FROM {view}")
            with st.expander(f"{label} ({cnt:,} rows)", expanded=False):
                qa_v2_df = sqdf(con, f"SELECT * FROM {view} LIMIT 1000")
                st.dataframe(qa_v2_df, use_container_width=True,
                             height=300, hide_index=True)
                multi_export(qa_v2_df, label.lower().replace(" ", "_"),
                             key_sfx=f"qav2_{local_v[:8]}")
        else:
            st.info(f"{label} not available. Run `scripts/25_qa_validation_v2.py`.",
                    icon="🔍")

# ─────────────────────────────────────────────────────────────────────────
# TAB: RISK & SURVIVAL
# ─────────────────────────────────────────────────────────────────────────
def render_survival(con):
    if not tbl_exists(con, "survival_cohort_ready_mv"):
        st.info("Survival data not available. Run script 10 first.", icon="📉")
        return
    if not HAS_LIFELINES:
        st.warning("Install `lifelines` for Kaplan-Meier plots: `pip install lifelines`")
    st.markdown(sl("Kaplan-Meier: Recurrence-Free Survival"), unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        strat = st.selectbox("Stratify by",
                             ["None", "Overall Stage", "Histology", "Sex"],
                             key="km_strat")
    with c2:
        max_yr = st.slider("Max follow-up (years)", 1, 30, 15, key="km_yr")
    base_sql = """
        SELECT s.time_to_event_days, s.event_occurred, s.overall_stage_ajcc8,
               s.histology_1_type, s.sex,
               r.braf_positive, r.tg_annual_log_slope, r.recurrence_risk_band
        FROM survival_cohort_ready_mv s
        LEFT JOIN recurrence_risk_features_mv r ON s.research_id = r.research_id
        WHERE s.time_to_event_days > 0"""
    df = sqdf(con, base_sql)
    if df.empty:
        st.warning("No survival data available.")
        return
    df["time_years"] = df["time_to_event_days"] / 365.25
    df = df[df["time_years"] <= max_yr]
    colors = ["#2dd4bf", "#38bdf8", "#a78bfa", "#f59e0b", "#f43f5e", "#34d399"]
    fig = go.Figure()
    if HAS_LIFELINES:
        col_map = {"Overall Stage": "overall_stage_ajcc8", "Histology": "histology_1_type",
                   "Sex": "sex", "BRAF Status": "braf_positive",
                   "Risk Band": "recurrence_risk_band"}
        if strat == "None":
            kmf = KaplanMeierFitter()
            kmf.fit(df["time_years"], event_observed=df["event_occurred"])
            sf = kmf.survival_function_
            fig.add_trace(go.Scatter(
                x=sf.index, y=sf.iloc[:, 0], mode="lines",
                name=f"Overall (n={len(df)})", line=dict(color="#2dd4bf", width=2)))
        else:
            col = col_map.get(strat, "overall_stage_ajcc8")
            if col in df.columns:
                for i, grp in enumerate(sorted(df[col].dropna().unique(), key=str)):
                    sub = df[df[col] == grp]
                    if len(sub) >= 5:
                        kmf = KaplanMeierFitter()
                        kmf.fit(sub["time_years"], event_observed=sub["event_occurred"])
                        sf = kmf.survival_function_
                        fig.add_trace(go.Scatter(
                            x=sf.index, y=sf.iloc[:, 0], mode="lines",
                            name=f"{grp} (n={len(sub)})",
                            line=dict(color=colors[i % len(colors)], width=2)))
    else:
        st.caption("Install lifelines for KM curves. Showing summary instead.")
    fig.update_layout(**PL, height=500,
                      xaxis_title="Years from Surgery",
                      yaxis_title="Event-Free Probability",
                      yaxis=dict(range=[0, 1.05], gridcolor="#1e2535"),
                      title="Kaplan-Meier: Recurrence-Free Survival")
    st.plotly_chart(fig, use_container_width=True)
    st.markdown(sl("Risk Feature Summary"), unsafe_allow_html=True)
    if tbl_exists(con, "recurrence_risk_features_mv"):
        sm = sqdf(con, """
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN recurrence_flag THEN 1 ELSE 0 END) AS recurrences,
                   ROUND(100.0 * SUM(CASE WHEN recurrence_flag THEN 1 ELSE 0 END)
                         / NULLIF(COUNT(*), 0), 1) AS recurrence_pct,
                   ROUND(AVG(tg_annual_log_slope), 4) AS mean_tg_slope,
                   SUM(CASE WHEN braf_positive THEN 1 ELSE 0 END) AS braf_pos,
                   SUM(CASE WHEN tert_positive THEN 1 ELSE 0 END) AS tert_pos
            FROM recurrence_risk_features_mv""")
        if not sm.empty:
            r = sm.iloc[0]
            cs = st.columns(6)
            for c, (l, k) in zip(cs, [("Patients", "n"), ("Recurrences", "recurrences"),
                                       ("Recurrence %", "recurrence_pct"),
                                       ("Mean Tg Slope", "mean_tg_slope"),
                                       ("BRAF+", "braf_pos"), ("TERT+", "tert_pos")]):
                with c:
                    st.markdown(mc(l, f"{r[k]}" if pd.notna(r[k]) else "N/A"), unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────
# TAB: ADVANCED FEATURES V3 EXPLORER
# ─────────────────────────────────────────────────────────────────────────
def render_afv3_explorer(con):
    if not tbl_exists(con, "advanced_features_v3"):
        st.info("Advanced features v3 not available. Run script 11 first.", icon="🧩")
        return
    df = sqdf(con, "SELECT * FROM advanced_features_v3 LIMIT 5000")
    if df.empty:
        st.warning("No data in advanced_features_v3.")
        return
    st.markdown(f"**{len(df):,} rows** (limit 5,000) · **{len(df.columns)} columns**")
    pref = [c for c in ["research_id", "histology_1_type", "overall_stage_ajcc8",
                         "total_surgeries", "qa_issue_count", "tg_last",
                         "tg_annual_log_slope", "braf_positive", "tumor_size_cm",
                         "ln_positive", "recurrence_flag"] if c in df.columns]
    cols = st.multiselect("Columns", df.columns.tolist(), default=pref[:10], key="afv3_cols")
    srch = st.text_input("Search", placeholder="Search all columns…", key="afv3_search")
    disp = df[cols] if cols else df
    if srch:
        mask = disp.apply(lambda s: s.astype(str).str.contains(srch, case=False, na=False)).any(axis=1)
        disp = disp[mask]
        st.caption(f"{len(disp):,} matching rows")
    st.dataframe(disp, use_container_width=True, height=520, hide_index=True)
    dl = df[cols].to_csv(index=False) if cols else df.to_csv(index=False)
    st.download_button("⬇ Download CSV", dl,
                       f"advanced_features_v3_{datetime.now():%Y%m%d}.csv", "text/csv", key="afv3_dl")


# ─────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────
def _check_critical_tables(con) -> None:
    """Surface warnings for missing critical v3 tables."""
    critical = [
        ("molecular_episode_v3", "scripts/18_adjudication_framework.py"),
        ("rai_episode_v3", "scripts/18_adjudication_framework.py"),
        ("validation_failures_v3", "scripts/17_semantic_cleanup_v3.py"),
        ("tumor_episode_master_v2", "scripts/22_canonical_episodes_v2.py"),
        ("linkage_summary_v2", "scripts/23_cross_domain_linkage_v2.py"),
    ]
    missing = [(name, script) for name, script in critical if not tbl_exists(con, name)]
    if missing:
        names = ", ".join(f"`{n}`" for n, _ in missing)
        scripts = ", ".join(f"`{s}`" for _, s in set((n, s) for n, s in missing))
        st.warning(
            f"Missing critical tables: {names}. "
            f"Run {scripts} then `scripts/26_motherduck_materialize_v2.py --md` "
            f"to create them.",
            icon="⚠️",
        )


def main():
    if not _ensure_token():
        st.title("🔬 Thyroid Cohort Explorer")
        st.error(
            "**MotherDuck token not found.**\n\n"
            "Set `MOTHERDUCK_TOKEN` in `.streamlit/secrets.toml` or as an environment variable.\n\n"
            "```bash\n"
            "# Option A: environment variable\n"
            "export MOTHERDUCK_TOKEN='your_token'\n\n"
            "# Option B: Streamlit secrets\n"
            "mkdir -p .streamlit\n"
            "echo 'MOTHERDUCK_TOKEN = \"your_token\"' > .streamlit/secrets.toml\n"
            "```"
        )
        st.stop()
    try: con = _get_con()
    except Exception as exc: st.error(f"Failed to connect to MotherDuck: {exc}"); st.stop()

    _check_critical_tables(con)

    st.info("**Publication-ready v2026.03.10** — local DuckDB backup available · "
            "[Release Notes](RELEASE_NOTES.md)", icon="📦")
    ci,ct = st.columns([1,11])
    with ci: st.markdown('<div style="font-size:2.8rem;margin-top:4px">🔬</div>',unsafe_allow_html=True)
    with ct: st.markdown('<h1 style="margin:0;padding:0">THYROID_2026</h1><p style="margin:2px 0 0 2px;color:#8892a4;font-size:.78rem;font-family:\'DM Mono\',monospace;letter-spacing:.08em">THYROID CANCER RESEARCH LAKEHOUSE · 11,673 PATIENTS · 13 TABLES · MOTHERDUCK</p>',unsafe_allow_html=True)
    st.markdown("---")

    df_full = sqdf(con,"SELECT * FROM advanced_features_v3") if tbl_exists(con,"advanced_features_v3") else sqdf(con,"SELECT * FROM advanced_features_view")
    if df_full.empty: st.error("Could not load data view."); st.stop()
    df_filt = build_sidebar(df_full)

    with st.sidebar:
        st.markdown(sl("⚡ Compute Tier"), unsafe_allow_html=True)
        st.markdown('<div style="font-family:monospace;font-size:.65rem;color:#2dd4bf">'
                    'MotherDuck Business Trial</div>', unsafe_allow_html=True)
        if st.button("Switch to Jumbo 🚀", key="jumbo_btn"):
            try:
                con.execute("SET motherduck_default_server_instance_type = 'jumbo'")
                st.success("Switched to Jumbo compute!")
            except Exception as e:
                st.error(f"Could not switch: {e}")

        st.markdown(sl("📸 Publication"), unsafe_allow_html=True)
        if st.button("Publication Snapshot", key="pub_snap"):
            with st.spinner("Exporting all MVs…"):
                snap_ts = datetime.now().strftime("%Y%m%d")
                snap_dir = Path(__file__).resolve().parent / "exports" / f"snapshot_{snap_ts}"
                snap_dir.mkdir(parents=True, exist_ok=True)
                snap_mvs = [
                    "patient_level_summary_mv", "tg_trend_long_mv",
                    "recurrence_risk_features_mv", "serial_nodule_tracking_mv",
                    "survival_cohort_ready_mv", "molecular_path_risk_mv",
                    "complication_severity_mv", "advanced_features_v3",
                ]
                exported = []
                for mv in snap_mvs:
                    if not tbl_exists(con, mv):
                        continue
                    try:
                        snap_df = qdf(con, f"SELECT * FROM {mv}")
                        snap_df.to_csv(snap_dir / f"{mv}.csv", index=False)
                        snap_df.to_parquet(snap_dir / f"{mv}.parquet", index=False)
                        exported.append(mv)
                    except Exception:
                        pass
                st.success(f"Exported {len(exported)} tables → exports/snapshot_{snap_ts}/")

        # ── Review Mode ──────────────────────────────────────────────
        st.markdown(sl("🔎 Review Mode"), unsafe_allow_html=True)
        review_mode = st.toggle("Enable Review Mode (RW)", key="review_mode_toggle")
        rw_con = None
        if review_mode:
            try:
                cfg = MotherDuckConfig(database=DATABASE)
                rw_con = MotherDuckClient(cfg).connect_rw()
                st.markdown(
                    '<div style="font-family:monospace;font-size:.6rem;color:#34d399">'
                    '● REVIEW MODE ACTIVE (read-write)</div>',
                    unsafe_allow_html=True,
                )
            except Exception as e:
                st.error(f"Could not establish RW connection: {e}")
                rw_con = None
        else:
            st.caption("Read-only mode. Toggle to enter decisions.")

        # ── Data Build Info ──────────────────────────────────────────
        with st.expander("📋 Data Build Info"):
            db_mode = "Read-Write" if review_mode and rw_con else "Read-Only Share"
            st.markdown(f"**Database mode:** {db_mode}")
            st.markdown(f"**Database:** `{DATABASE}`")
            st.markdown(f"**Deploy order:** 15→20 (adjudication) · 22→27 (v2 canonical)")
            for vname, label in [
                ("molecular_episode_v3", "Molecular v3"),
                ("rai_episode_v3", "RAI v3"),
                ("validation_failures_v3", "Validation v3"),
                ("adjudication_decisions", "Reviewer Decisions"),
                ("tumor_episode_master_v2", "Canonical Episodes v2"),
                ("linkage_summary_v2", "Cross-Domain Linkage"),
                ("qa_issues_v2", "QA Validation v2"),
                ("date_rescue_rate_summary", "Date Provenance"),
            ]:
                avail = tbl_exists(con, vname)
                icon = "✅" if avail else "❌"
                st.markdown(f"{icon} {label}")
            st.caption(f"Last refresh: {datetime.now():%Y-%m-%d %H:%M}")

        # ── Connection Help ──────────────────────────────────────────
        with st.expander("❓ Connection Help"):
            st.markdown(
                "Set `MOTHERDUCK_TOKEN` before running the dashboard:\n\n"
                "**Option A** — environment variable:\n"
                "```bash\nexport MOTHERDUCK_TOKEN='your_token'\n```\n\n"
                "**Option B** — Streamlit secrets:\n"
                "```bash\nmkdir -p .streamlit\n"
                "echo 'MOTHERDUCK_TOKEN = \"your_token\"' > .streamlit/secrets.toml\n```\n\n"
                "If critical v3 tables are missing, run:\n"
                "```bash\npython scripts/26_motherduck_materialize_v2.py --md\n"
                "python scripts/29_validation_engine.py --md\n```"
            )

    (t_ov,t_ex,t_vz,t_adv,t_gen,t_spec,t_img,t_comp,t_rec,t_exp,t_ai,
     t_tl,t_ev,t_qa,t_surv,t_afv3,
     t_cqc,t_pat,t_rh,t_rm,t_rr,t_rtl,t_rq,t_diag,
     t_ec,t_md,t_rd,t_ind,t_od,t_as,t_ve) = st.tabs([
        "📊 Overview","🗃 Data Explorer","📈 Visualizations","🧬 Advanced",
        "🔬 Genetics & Molecular","🫀 Specimen Details","📡 Pre-Op Imaging",
        "⚕ Complications","📋 Recommendations & Sensitivities",
        "📐 Expanded Cohort","✨ AI Insights",
        "🕐 Timeline","📋 Events","🔍 QA","📉 Survival","🧩 Features v3",
        "📋 Cohort QC","🧑‍⚕️ Patient Audit","🔬 Histology Review",
        "🧬 Molecular Review","☢️ RAI Review","🕐 Timeline Review",
        "📝 Review Queue","⚙️ Diagnostics",
        "📊 Extraction Completeness","🧬 Molecular Episodes","☢️ RAI Episodes",
        "📡 Imaging & Nodules","🔪 Operative Detail","📋 QA & Adjudication",
        "🛡 Validation Engine",
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
    with t_exp:  render_expanded_cohort()
    with t_ai:   render_ai_insights(con)
    with t_tl:   render_timeline(con)
    with t_ev:   render_events(con)
    with t_qa:   render_qa_dashboard(con)
    with t_surv: render_survival(con)
    with t_afv3: render_afv3_explorer(con)
    with t_cqc:  render_cohort_qc(con)
    with t_pat:  render_patient_audit(con, rw_con)
    with t_rh:   render_review_histology(con, rw_con)
    with t_rm:   render_review_molecular(con, rw_con)
    with t_rr:   render_review_rai(con, rw_con)
    with t_rtl:  render_review_timeline(con, rw_con)
    with t_rq:   render_review_queue(con)
    with t_diag: render_diagnostics(con)
    with t_ec:   render_extraction_completeness(con)
    with t_md:   render_molecular_dashboard(con)
    with t_rd:   render_rai_dashboard(con)
    with t_ind:  render_imaging_nodule_dashboard(con)
    with t_od:   render_operative_dashboard(con)
    with t_as:   render_adjudication_summary(con)
    with t_ve:   render_validation_engine(con)

    st.markdown("---")
    st.caption(f"**Data source:** MotherDuck `{DATABASE}` · Share: `{SHARE_PATH[:40]}…` · Loaded: {datetime.now():%Y-%m-%d %H:%M} · Built with Streamlit + DuckDB + Plotly + Claude")

if __name__ == "__main__":
    main()
