import streamlit as st, json, glob, pandas as pd, plotly.express as px, plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

st.set_page_config(page_title="DubaiIntel", layout="wide", page_icon="🏙️")
st.markdown("""<style>
.stApp{background:#0A0C0F}
.block-container{padding-top:1.5rem;max-width:1400px}
[data-testid="stMetricValue"]{font-size:26px;color:#C9A84C}
</style>""", unsafe_allow_html=True)

ROOT=Path(__file__).resolve().parent
BASE={"Dubai Marina":{"avg_psf":2100,"yield":6.5,"growth":14.77},"Downtown Dubai":{"avg_psf":2800,"yield":4.5,"growth":14.77},"Palm Jumeirah":{"avg_psf":3200,"yield":3.5,"growth":20.67},"Business Bay":{"avg_psf":1900,"yield":7.0,"growth":14.77},"Jumeirah Beach Residence":{"avg_psf":2000,"yield":5.5,"growth":14.77}}

@st.cache_data(ttl=60)
def load_dld():
    for p in [ROOT/"data/processed/dld_intelligence.json",ROOT/"dld_intelligence.json"]:
        if p.exists():
            with open(p) as f: return json.load(f)
    return {}

@st.cache_data(ttl=60)
def load_listings():
    files=sorted(glob.glob(str(ROOT/"data/processed/weekly_*.json")))
    if not files: return pd.DataFrame()
    with open(files[-1]) as f: d=json.load(f)
    if isinstance(d,dict):
        for k in ["records","listings","data"]:
            if k in d and isinstance(d[k],list) and d[k]: return pd.DataFrame(d[k])
    return pd.DataFrame(d) if isinstance(d,list) and d else pd.DataFrame()

dld=load_dld(); ry=dld.get("rental_yields",{}); pi=dld.get("price_index",{}); val=dld.get("valuations",{}); sup=dld.get("supply_pipeline",{}); prm=dld.get("building_permits",{})
BM={k:dict(v) for k,v in BASE.items()}
for a in BM:
    if a in ry and ry[a].get("estimated_yield_pct",0)>0: BM[a]["yield"]=ry[a]["estimated_yield_pct"]
    if a in val and val[a].get("median_val_psf_aed",0)>0: BM[a]["avg_psf"]=val[a]["median_val_psf_aed"]
if pi.get("flat_yoy_growth_pct"): [BM.__setitem__(a,{**BM[a],"growth":pi["flat_yoy_growth_pct"]}) for a in ["Dubai Marina","Downtown Dubai","Business Bay","Jumeirah Beach Residence"]]
if pi.get("villa_yoy_growth_pct"): BM["Palm Jumeirah"]["growth"]=pi["villa_yoy_growth_pct"]

def enrich(df):
    for c in ["price","price_per_sqft","size_sqft"]: df[c]=pd.to_numeric(df.get(c),errors="coerce")
    df["area_avg_psf"]=df["area"].map({k:v["avg_psf"] for k,v in BM.items()})
    df["yield_pct"]=df["area"].map({k:v["yield"] for k,v in BM.items()})
    df["growth_pct"]=df["area"].map({k:v["growth"] for k,v in BM.items()})
    df["gap_pct"]=((df["area_avg_psf"]-df["price_per_sqft"])/df["area_avg_psf"]*100).round(1)
    df["gap_label"]=df["gap_pct"].apply(lambda g:"🟢 Undervalued" if g>=20 else("🔴 Overvalued" if g<=-10 else "🟡 Fair"))
    df["inv_score"]=(df["yield_pct"].fillna(0)*0.40+df["growth_pct"].fillna(0)/3*0.35+5*0.25).round(1)
    return df

df_raw=load_listings()
if df_raw.empty: st.error("No data. Run pipeline first."); st.stop()
df=enrich(df_raw.copy())
src_count=len(dld.get("data_sources",[]))
has_dld=src_count>0

with st.sidebar:
    st.markdown("## 🏙️ DubaiIntel")
    st.markdown(f"{'🟢' if has_dld else '🟡'} **{''+str(src_count)+' DLD sources' if has_dld else 'Benchmark data'}**")
    if has_dld:
        for s in dld.get("data_sources",[]): st.markdown(f"  ✓ {s}")
    st.markdown("---")
    page=st.radio("",["📊 Overview","🔍 Price Gap","🏆 Investment","📈 DLD Analytics","📋 Listings"])
    st.markdown("---")
    sel=st.multiselect("Areas",list(BM.keys()),default=list(BM.keys()))
    pr=st.slider("Price AED M",0.1,30.0,(0.1,15.0),0.1)
    gf=st.selectbox("Signal",["All","🟢 Undervalued","🟡 Fair","🔴 Overvalued"])

df_f=df[df["area"].isin(sel)&df["price"].between(pr[0]*1e6,pr[1]*1e6)].copy()
if gf!="All": df_f=df_f[df_f["gap_label"]==gf]

st.markdown(f"# 🏙️ Dubai Real Estate Intelligence")
st.caption(f"{len(df_f):,} listings · {datetime.now().strftime('%d %b %Y')} · {'🟢 Live DLD' if has_dld else '🟡 Demo data'}")
st.divider()

if page=="📊 Overview":
    c1,c2,c3,c4,c5=st.columns(5)
    c1.metric("Listings",f"{len(df_f):,}")
    c2.metric("Avg Price",f"AED {df_f['price'].mean()/1e6:.1f}M")
    c3.metric("Avg AED/sqft",f"{df_f['price_per_sqft'].mean():,.0f}")
    c4.metric("🟢 Opportunities",f"{len(df_f[df_f['gap_label']=='🟢 Undervalued'])}")
    c5.metric("Flat Growth YoY",f"+{pi.get('flat_yoy_growth_pct',14.77)}%")
    if pi: st.info(f"📊 DLD Price Index ({pi.get('latest_date','')}) — Flats +{pi.get('flat_yoy_growth_pct')}% · Villas +{pi.get('villa_yoy_growth_pct')}% YoY")
    c_a,c_b=st.columns(2)
    with c_a:
        st.subheader("Price/sqft by Area")
        aa=df_f.groupby("area")["price_per_sqft"].mean().reset_index().sort_values("price_per_sqft")
        fig=px.bar(aa,x="price_per_sqft",y="area",orientation="h",text="price_per_sqft",color="price_per_sqft",color_continuous_scale=["#1a3a2a","#22c55e"])
        fig.update_traces(texttemplate="AED %{text:,.0f}",textposition="outside")
        fig.update_layout(paper_bgcolor="#0A0C0F",plot_bgcolor="#111418",font_color="#E8E6E0",height=300,coloraxis_showscale=False,margin=dict(l=0,r=90,t=10,b=10))
        st.plotly_chart(fig,use_container_width=True)
    with c_b:
        st.subheader(f"Rental Yield {'(DLD)' if ry else '(Benchmark)'}")
        yd=pd.DataFrame([{"area":a,"yield":BM[a]["yield"],"src":"🟢 DLD" if a in ry else "🟡 Est"} for a in sel]).sort_values("yield")
        fig2=px.bar(yd,x="yield",y="area",orientation="h",text="yield",color="src",color_discrete_map={"🟢 DLD":"#C9A84C","🟡 Est":"#555"})
        fig2.update_traces(texttemplate="%{text:.1f}%",textposition="outside")
        fig2.update_layout(paper_bgcolor="#0A0C0F",plot_bgcolor="#111418",font_color="#E8E6E0",height=300,margin=dict(l=0,r=60,t=10,b=10),legend=dict(bgcolor="#111418"))
        st.plotly_chart(fig2,use_container_width=True)
    st.subheader("Price Distribution")
    fig3=px.histogram(df_f[df_f["price"]<15e6],x="price",color="area",nbins=40,color_discrete_sequence=px.colors.qualitative.Safe)
    fig3.update_layout(paper_bgcolor="#0A0C0F",plot_bgcolor="#111418",font_color="#E8E6E0",height=260,margin=dict(l=0,r=0,t=10,b=10))
    st.plotly_chart(fig3,use_container_width=True)

elif page=="🔍 Price Gap":
    st.subheader("Price Gap Detector")
    st.caption("Gap = (Area Avg − Listing) ÷ Area Avg × 100%")
    g1,g2,g3=st.columns(3)
    g1.metric("🟢 Undervalued",len(df_f[df_f["gap_label"]=="🟢 Undervalued"]))
    g2.metric("🟡 Fair",len(df_f[df_f["gap_label"]=="🟡 Fair"]))
    g3.metric("🔴 Overvalued",len(df_f[df_f["gap_label"]=="🔴 Overvalued"]))
    st.divider()
    ds=df_f[df_f["price_per_sqft"].notna()&df_f["area_avg_psf"].notna()]
    cm={"🟢 Undervalued":"#22c55e","🟡 Fair":"#C9A84C","🔴 Overvalued":"#ef4444"}
    fig=px.scatter(ds,x="area_avg_psf",y="price_per_sqft",color="gap_label",color_discrete_map=cm,size="price",size_max=18,hover_data=["title","area","price","gap_pct"],labels={"area_avg_psf":"Area Avg AED/sqft","price_per_sqft":"Listing AED/sqft","gap_label":"Signal"})
    mn=ds[["area_avg_psf","price_per_sqft"]].min().min(); mx=ds[["area_avg_psf","price_per_sqft"]].max().max()
    fig.add_trace(go.Scatter(x=[mn,mx],y=[mn,mx],mode="lines",line=dict(dash="dash",color="#555",width=1),name="Fair Value"))
    fig.update_layout(paper_bgcolor="#0A0C0F",plot_bgcolor="#111418",font_color="#E8E6E0",height=400,margin=dict(l=0,r=0,t=10,b=10),legend=dict(bgcolor="#111418"))
    st.plotly_chart(fig,use_container_width=True)
    st.subheader("🟢 Top Opportunities")
    ops=df_f[df_f["gap_label"]=="🟢 Undervalued"].sort_values("gap_pct",ascending=False)
    if ops.empty: st.info("No undervalued listings in current filter.")
    for _,r in ops.head(10).iterrows():
        up=""
        if pd.notna(r.get("size_sqft")) and r.get("size_sqft",0)>0: up=f" · Upside AED {(r['area_avg_psf']-r['price_per_sqft'])*r['size_sqft']:,.0f}"
        st.markdown(f"""<div style='background:#0a2018;border-left:3px solid #22c55e;border-radius:0 8px 8px 0;padding:12px 16px;margin:6px 0'>
        <span style='font-size:22px;font-weight:700;color:#22c55e'>-{r['gap_pct']:.0f}%</span>
        <span style='color:#E8E6E0;font-size:14px;margin-left:10px'>{str(r.get('title',''))[:55]}</span><br>
        <span style='color:#8A8070;font-size:12px'>📍 {r['area']} | AED {r['price']:,.0f} | {r['price_per_sqft']:,.0f}/sqft vs {r['area_avg_psf']:,.0f} avg{up}</span>
        </div>""",unsafe_allow_html=True)

elif page=="🏆 Investment":
    st.subheader("Investment Score per Area")
    st.caption("Score = Yield×40% + Growth×35% + Liquidity×25%")
    rk=[]
    for a,b in BM.items():
        if a not in sel: continue
        cnt=len(df_f[df_f["area"]==a]); liq=min(10,cnt/5)
        sc=round(b["yield"]*0.40+b["growth"]/3*0.35+liq*0.25,1)
        rk.append({"area":a,"score":sc,"stars":"⭐⭐⭐⭐" if sc>=8 else "⭐⭐⭐" if sc>=6 else "⭐⭐","yield":b["yield"],"growth":b["growth"],"avg_psf":b["avg_psf"],"cnt":cnt,"dld":"🟢" if a in ry else "🟡"})
    rk=sorted(rk,key=lambda x:x["score"],reverse=True)
    cols=st.columns(len(rk))
    for col,r in zip(cols,rk):
        c="#22c55e" if r["score"]>=8 else "#C9A84C" if r["score"]>=6 else "#ef4444"
        col.markdown(f"""<div style='background:#111418;border:1px solid {c}44;border-top:3px solid {c};border-radius:0 0 12px 12px;padding:16px;text-align:center'>
        <div style='font-size:11px;color:#8A8070'>{r['area']}</div>
        <div style='font-size:38px;font-weight:700;color:{c}'>{r['score']}</div>
        <div style='font-size:18px'>{r['stars']}</div>
        <div style='font-size:11px;color:#8A8070;margin-top:6px'>Yield {r['yield']:.1f}% {r['dld']}<br>Growth +{r['growth']}%<br>AED {r['avg_psf']:,}/sqft</div>
        </div>""",unsafe_allow_html=True)
    st.divider()
    cats=["Yield","Growth","Liquidity"]
    fig=go.Figure()
    clrs=["#22c55e","#C9A84C","#3b82f6","#ef4444","#a855f7"]
    for i,r in enumerate(rk):
        cnt=len(df_f[df_f["area"]==r["area"]])
        fig.add_trace(go.Scatterpolar(r=[r["yield"]*10,r["growth"],min(10,cnt/5)*10],theta=cats,fill="toself",name=r["area"],line_color=clrs[i%len(clrs)],fillcolor=clrs[i%len(clrs)],opacity=0.3))
    fig.update_layout(polar=dict(bgcolor="#111418",radialaxis=dict(visible=True,range=[0,100],gridcolor="#333"),angularaxis=dict(gridcolor="#333",color="#E8E6E0")),paper_bgcolor="#0A0C0F",font_color="#E8E6E0",height=380,margin=dict(l=40,r=40,t=20,b=20),legend=dict(bgcolor="#111418"))
    st.plotly_chart(fig,use_container_width=True)
    st.info("🏆 Best yield → Business Bay · Best growth → Palm Jumeirah · Best balanced → Business Bay")

elif page=="📈 DLD Analytics":
    st.subheader("Dubai Land Department — Official Data")
    if not has_dld:
        st.warning("DLD data not loaded. Run the processor script first.")
    else:
        if pi:
            c1,c2,c3=st.columns(3)
            c1.metric("Index Date",pi.get("latest_date",""))
            c2.metric("Flat Index",pi.get("flat_index",""),delta=f"+{pi.get('flat_yoy_growth_pct','')}% YoY")
            c3.metric("Villa Index",pi.get("villa_index",""),delta=f"+{pi.get('villa_yoy_growth_pct','')}% YoY")
            if pi.get("monthly_series"):
                ms=pd.DataFrame(pi["monthly_series"])
                ms["date"]=pd.to_datetime(ms["date"])
                fig=go.Figure()
                fig.add_trace(go.Scatter(x=ms["date"],y=ms["flat_index"],name="Flat Index",line=dict(color="#C9A84C",width=2)))
                if "villa_index" in ms.columns: fig.add_trace(go.Scatter(x=ms["date"],y=ms["villa_index"],name="Villa Index",line=dict(color="#22c55e",width=2)))
                fig.update_layout(paper_bgcolor="#0A0C0F",plot_bgcolor="#111418",font_color="#E8E6E0",height=300,margin=dict(l=0,r=0,t=20,b=10),legend=dict(bgcolor="#111418"),title="Price Index Trend (3 Years)")
                st.plotly_chart(fig,use_container_width=True)
        if ry:
            st.divider(); st.subheader("Real Rental Yields — EJARI Contracts")
            ry_df=pd.DataFrame([{"Area":a,"Yield %":d["estimated_yield_pct"],"Avg Annual Rent":f"AED {d['avg_annual_rent_aed']:,}","Avg sqft":d["avg_sqft"],"Renewal %":d["renewal_rate_pct"],"Contracts":d["sample_size"]} for a,d in ry.items() if a in sel])
            if not ry_df.empty: st.dataframe(ry_df,use_container_width=True,hide_index=True)
        if val:
            st.divider(); st.subheader("Official Property Valuations (DLD)")
            vd=pd.DataFrame([{"Area":a,"Official AED/sqft":d["median_val_psf_aed"],"Median Value":f"AED {d['median_property_value']:,}","Sample":d["sample_size"]} for a,d in val.items() if a in sel])
            if not vd.empty: st.dataframe(vd,use_container_width=True,hide_index=True)
        if sup:
            st.divider(); st.subheader("Supply Pipeline — Active Projects")
            sd=pd.DataFrame([{"Area":a,"Total Projects":d["total_projects"],"Active":d["active_projects"],"Pipeline Units":d["pipeline_units"],"Avg Completion":f"{d['avg_completion_pct']}%" if d.get("avg_completion_pct") else "N/A"} for a,d in sup.items() if a in sel])
            if not sd.empty: st.dataframe(sd,use_container_width=True,hide_index=True)
        if prm:
            st.divider(); st.subheader("Building Permits Activity")
            c1,c2=st.columns(2)
            c1.metric("2024 Permits",f"{prm.get('recent_count_2024',0):,}")
            c2.metric("2023 Permits",f"{prm.get('recent_count_2023',0):,}")
            if prm.get("permits_by_year"):
                pd_=pd.DataFrame([{"year":int(k),"permits":v} for k,v in prm["permits_by_year"].items()])
                fig=px.bar(pd_,x="year",y="permits",color_discrete_sequence=["#C9A84C"])
                fig.update_layout(paper_bgcolor="#0A0C0F",plot_bgcolor="#111418",font_color="#E8E6E0",height=250,margin=dict(l=0,r=0,t=10,b=10))
                st.plotly_chart(fig,use_container_width=True)

elif page=="📋 Listings":
    search=st.text_input("Search","")
    dt=df_f.copy()
    if search: dt=dt[dt.apply(lambda r:search.lower() in str(r).lower(),axis=1)]
    sc=[c for c in ["gap_label","area","title","price","price_per_sqft","size_sqft","bedrooms","gap_pct","source"] if c in dt.columns]
    dp=dt[sc].copy().rename(columns={"gap_label":"Signal","area":"Area","title":"Title","price":"Price AED","price_per_sqft":"AED/sqft","size_sqft":"sqft","bedrooms":"Beds","gap_pct":"Gap %","source":"Source"})
    if "Price AED" in dp.columns: dp["Price AED"]=dp["Price AED"].apply(lambda x:f"{x:,.0f}" if pd.notna(x) else "")
    st.caption(f"{len(dp):,} listings")
    st.dataframe(dp.reset_index(drop=True),use_container_width=True,hide_index=True,height=500)
