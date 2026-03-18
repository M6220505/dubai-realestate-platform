import streamlit as st
import json, glob, pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

st.set_page_config(page_title="DubaiIntel", layout="wide", page_icon="🏙️")
st.markdown("""<style>
.stApp{background:#0A0C0F}
.block-container{padding-top:1.5rem;max-width:1400px}
[data-testid="stMetricValue"]{font-size:26px;color:#C9A84C}
</style>""", unsafe_allow_html=True)

ROOT = Path(__file__).resolve().parent

AREA_MAP = {
    'Marsa Dubai':'Dubai Marina','Business Bay':'Business Bay',
    'Palm Jumeirah':'Palm Jumeirah','Al Merkadh':'Downtown Dubai',
    'Burj Khalifa':'Downtown Dubai','Downtown Dubai':'Downtown Dubai',
    'Jumeirah Beach Residence':'Jumeirah Beach Residence',
}

@st.cache_data(ttl=300)
def load_transactions():
    f = ROOT/"data/raw/Transactions_2026-03-18.csv"
    if not f.exists():
        return pd.DataFrame()
    df = pd.read_csv(f)
    df['our_area'] = df['area_name_en'].map(AREA_MAP)
    df = df[df['our_area'].notna()].copy()
    df['meter_sale_price'] = pd.to_numeric(df['meter_sale_price'], errors='coerce')
    df['actual_worth']     = pd.to_numeric(df['actual_worth'],     errors='coerce')
    df['procedure_area']   = pd.to_numeric(df['procedure_area'],   errors='coerce')
    return df[df['trans_group_en']=='Sales'].copy()

@st.cache_data(ttl=300)
def load_dld():
    p = ROOT/"data/processed/dld_intelligence.json"
    return json.load(open(p)) if p.exists() else {}

@st.cache_data(ttl=300)
def load_movements():
    f = ROOT/"data/raw/Movement_of_Real_Estate_Transactions_by_Type_of_Treatment_2026-03-18.csv"
    if not f.exists(): return pd.DataFrame()
    return pd.read_csv(f)

@st.cache_data(ttl=300)
def load_brokers():
    f = ROOT/"data/raw/Brokers_2026-03-18.csv"
    if not f.exists(): return pd.DataFrame()
    return pd.read_csv(f)

@st.cache_data(ttl=300)
def load_housing():
    f = ROOT/"data/raw/Housing_Units_2026-03-18.csv"
    if not f.exists(): return pd.DataFrame()
    return pd.read_csv(f)

tx  = load_transactions()
dld = load_dld()
mv  = load_movements()
br  = load_brokers()
hu  = load_housing()

TARGET = ['Dubai Marina','Downtown Dubai','Palm Jumeirah','Business Bay','Jumeirah Beach Residence']

def area_stats(df):
    rows = []
    for area in TARGET:
        a = df[df['our_area']==area]
        psf   = a['meter_sale_price'].dropna()
        worth = a['actual_worth'].dropna()
        rows.append({
            'area':         area,
            'sales':        len(a),
            'avg_psf':      round(psf.mean()) if len(psf)>0 else 0,
            'median_psf':   round(psf.median()) if len(psf)>0 else 0,
            'avg_value':    round(worth.mean()) if len(worth)>0 else 0,
            'median_value': round(worth.median()) if len(worth)>0 else 0,
        })
    return pd.DataFrame(rows)

def inv_score(row):
    dld_data = dld.get('rental_yields',{}).get(row['area'],{})
    yield_pct = dld_data.get('estimated_yield_pct', 5.0) if dld_data else 5.0
    growth    = 17.05
    liq       = min(10, row['sales'] / 30)
    return round(yield_pct*0.40 + growth/3*0.35 + liq*0.25, 1)

stats = area_stats(tx)
if not stats.empty:
    stats['inv_score'] = stats.apply(inv_score, axis=1)
    stats['gap_vs_avg'] = ((stats['avg_psf'].mean() - stats['avg_psf']) / stats['avg_psf'].mean() * 100).round(1)

# SIDEBAR
with st.sidebar:
    st.markdown("## 🏙️ DubaiIntel")
    st.success(f"✅ {len(tx):,} DLD transactions")
    st.caption("Source: Dubai Land Department")
    st.markdown("---")
    page = st.radio("", ["📊 Market Overview","🔍 Price Gap","🏆 Investment Ranking","📈 Trends","🤝 Brokers","📋 Transactions"])
    st.markdown("---")
    sel = st.multiselect("Areas", TARGET, default=TARGET)

tx_f = tx[tx['our_area'].isin(sel)]
stats_f = stats[stats['area'].isin(sel)] if not stats.empty else stats

st.markdown("# 🏙️ Dubai Real Estate Intelligence")
st.caption(f"{len(tx_f):,} DLD transactions · Source: Dubai Land Department · {datetime.now().strftime('%d %b %Y')}")
st.divider()

# PAGE: OVERVIEW
if page == "📊 Market Overview":
    total_val = tx_f['actual_worth'].sum()
    avg_psf   = tx_f['meter_sale_price'].mean()
    avg_val   = tx_f['actual_worth'].mean()

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Transactions",   f"{len(tx_f):,}")
    c2.metric("Total Value",    f"AED {total_val/1e9:.1f}B")
    c3.metric("Avg Price",      f"AED {avg_val/1e6:.1f}M")
    c4.metric("Avg AED/sqft",   f"{avg_psf:,.0f}")
    c5.metric("Growth YoY",     "+17.05%", delta="DLD Index")

    st.info("📊 Data source: Dubai Land Department (DLD) — real transaction records, not listings")
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Avg Sale Price/sqft by Area")
        fig = px.bar(
            stats_f.sort_values('avg_psf'),
            x='avg_psf', y='area', orientation='h',
            text='avg_psf', color='avg_psf',
            color_continuous_scale=['#1a3a2a','#22c55e']
        )
        fig.update_traces(texttemplate='AED %{text:,.0f}', textposition='outside')
        fig.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                          font_color='#E8E6E0', height=300,
                          coloraxis_showscale=False, margin=dict(l=0,r=90,t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Median Sale Value by Area")
        fig2 = px.bar(
            stats_f.sort_values('median_value'),
            x='median_value', y='area', orientation='h',
            text='median_value', color='median_value',
            color_continuous_scale=['#1a2a3a','#C9A84C']
        )
        fig2.update_traces(texttemplate='AED %{text:,.0f}', textposition='outside')
        fig2.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                           font_color='#E8E6E0', height=300,
                           coloraxis_showscale=False, margin=dict(l=0,r=110,t=10,b=10))
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Transaction Volume by Area")
    fig3 = px.bar(
        stats_f.sort_values('sales', ascending=False),
        x='area', y='sales', color='sales',
        text='sales', color_continuous_scale=['#1a2a3a','#C9A84C']
    )
    fig3.update_traces(texttemplate='%{text}', textposition='outside')
    fig3.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                       font_color='#E8E6E0', height=280,
                       coloraxis_showscale=False, margin=dict(l=0,r=0,t=10,b=10))
    st.plotly_chart(fig3, use_container_width=True)

# PAGE: PRICE GAP
elif page == "🔍 Price Gap":
    st.subheader("Price Gap — Which Areas Are Undervalued?")
    st.caption("Based on actual DLD transaction prices vs Dubai average")

    if not stats_f.empty:
        dubai_avg = stats_f['avg_psf'].mean()
        st.info(f"Dubai average (target areas): AED {dubai_avg:,.0f}/sqft")

        fig = go.Figure()
        for _, row in stats_f.iterrows():
            gap = row['avg_psf'] - dubai_avg
            color = '#22c55e' if gap < 0 else '#ef4444'
            fig.add_trace(go.Bar(
                x=[row['area']], y=[gap],
                marker_color=color,
                text=[f"AED {gap:+,.0f}"],
                textposition='outside',
                name=row['area'],
                showlegend=False,
            ))
        fig.add_hline(y=0, line_dash='dash', line_color='#555')
        fig.update_layout(
            paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
            font_color='#E8E6E0', height=380,
            title=f"Price gap vs Dubai avg (AED {dubai_avg:,.0f}/sqft)",
            margin=dict(l=0,r=0,t=40,b=10)
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Area Details")
        display = stats_f[['area','avg_psf','median_psf','avg_value','sales']].copy()
        display['vs_avg'] = ((display['avg_psf'] - dubai_avg) / dubai_avg * 100).round(1)
        display['signal'] = display['vs_avg'].apply(
            lambda x: '🟢 Undervalued' if x < -5 else ('🔴 Overvalued' if x > 5 else '🟡 Fair'))
        display.columns = ['Area','Avg AED/sqft','Median AED/sqft','Avg Value AED','Sales','vs Avg %','Signal']
        for col in ['Avg AED/sqft','Median AED/sqft','Avg Value AED']:
            display[col] = display[col].apply(lambda x: f"{x:,.0f}")
        st.dataframe(display.reset_index(drop=True), use_container_width=True, hide_index=True)

# PAGE: INVESTMENT RANKING
elif page == "🏆 Investment Ranking":
    st.subheader("Investment Score per Area")
    st.caption("Score = Yield 40% + Growth 35% + Liquidity 25% · Based on DLD data")

    if not stats_f.empty:
        ranked = stats_f.sort_values('inv_score', ascending=False)
        cols = st.columns(len(ranked))
        for col, (_, row) in zip(cols, ranked.iterrows()):
            sc = row['inv_score']
            color = '#22c55e' if sc>=8 else '#C9A84C' if sc>=6 else '#ef4444'
            stars = '⭐⭐⭐⭐' if sc>=8 else '⭐⭐⭐' if sc>=6 else '⭐⭐'
            col.markdown(f"""
            <div style='background:#111418;border:1px solid {color}44;border-top:3px solid {color};
                        border-radius:0 0 12px 12px;padding:16px;text-align:center'>
              <div style='font-size:11px;color:#8A8070'>{row['area']}</div>
              <div style='font-size:36px;font-weight:700;color:{color}'>{sc}</div>
              <div style='font-size:18px'>{stars}</div>
              <div style='font-size:11px;color:#8A8070;margin-top:6px'>
                {row['sales']} sales<br>
                AED {row['avg_psf']:,}/sqft<br>
                Avg AED {row['avg_value']/1e6:.1f}M
              </div>
            </div>""", unsafe_allow_html=True)

        st.markdown("---")
        categories = ["Liquidity","Value","Growth"]
        fig = go.Figure()
        colors = ['#22c55e','#C9A84C','#3b82f6','#ef4444','#a855f7']
        max_sales = ranked['sales'].max()
        dubai_avg = ranked['avg_psf'].mean()
        for i, (_, row) in enumerate(ranked.iterrows()):
            liq_s    = row['sales'] / max(max_sales, 1) * 100
            val_s    = max(0, min(100, (1 - row['avg_psf']/max(dubai_avg,1)) * 50 + 50))
            growth_s = 57
            fig.add_trace(go.Scatterpolar(
                r=[liq_s, val_s, growth_s],
                theta=categories, fill='toself', name=row['area'],
                line_color=colors[i%len(colors)],
                fillcolor=colors[i%len(colors)], opacity=0.3
            ))
        fig.update_layout(
            polar=dict(bgcolor='#111418',
                       radialaxis=dict(visible=True, range=[0,100], gridcolor='#333'),
                       angularaxis=dict(gridcolor='#333', color='#E8E6E0')),
            paper_bgcolor='#0A0C0F', font_color='#E8E6E0',
            height=380, margin=dict(l=40,r=40,t=20,b=20),
            legend=dict(bgcolor='#111418')
        )
        st.plotly_chart(fig, use_container_width=True)

# PAGE: TRENDS
elif page == "📈 Trends":
    st.subheader("Transaction Volume Trends")

    if not mv.empty:
        sales_mv = mv[mv['Title']=='Sales'].copy()
        by_year  = sales_mv.groupby('Year')['Value'].sum().reset_index()
        fig = px.bar(by_year, x='Year', y='Value',
                     text='Value', color='Value',
                     color_continuous_scale=['#1a2a3a','#C9A84C'])
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                          font_color='#E8E6E0', height=360,
                          coloraxis_showscale=False, margin=dict(l=0,r=0,t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)

        latest = int(sales_mv['Year'].max())
        latest_val = int(sales_mv[sales_mv['Year']==latest]['Value'].sum())
        c1,c2 = st.columns(2)
        c1.metric(f"Latest Year ({latest})", f"AED {latest_val/1e9:.1f}B")
        if latest-1 in sales_mv['Year'].values:
            prev = int(sales_mv[sales_mv['Year']==latest-1]['Value'].sum())
            yoy  = round((latest_val/max(prev,1)-1)*100,1)
            c2.metric("YoY Change", f"{yoy:+.1f}%")

    if not hu.empty:
        st.subheader("Housing Units by Type")
        fig2 = px.bar(hu, x='unit_type_en', y='count',
                      text='count', color='count',
                      color_continuous_scale=['#1a3a2a','#22c55e'])
        fig2.update_traces(texttemplate='%{text:,}', textposition='outside')
        fig2.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                           font_color='#E8E6E0', height=300,
                           coloraxis_showscale=False, margin=dict(l=0,r=0,t=10,b=10))
        st.plotly_chart(fig2, use_container_width=True)

# PAGE: BROKERS
elif page == "🤝 Brokers":
    st.subheader("Licensed Real Estate Brokers — UAE")
    if not br.empty:
        br['license_end_date'] = pd.to_datetime(br['license_end_date'], errors='coerce')
        active = br[br['license_end_date'] > pd.Timestamp.now()]
        c1,c2,c3 = st.columns(3)
        c1.metric("Total Licensed", f"{len(br):,}")
        c2.metric("Active Now",     f"{len(active):,}")
        c3.metric("Expiring Soon",  f"{len(br[(br['license_end_date']>pd.Timestamp.now()) & (br['license_end_date']<pd.Timestamp.now()+pd.DateOffset(months=3))]):,}")

        st.subheader("Broker Directory")
        search = st.text_input("Search broker name", "")
        df_show = active[['broker_name_en','phone','license_start_date','license_end_date','real_estate_number']].copy()
        df_show.columns = ['Name','Phone','License Start','License End','RE Number']
        if search:
            df_show = df_show[df_show['Name'].str.contains(search, case=False, na=False)]
        st.caption(f"{len(df_show):,} brokers")
        st.dataframe(df_show.reset_index(drop=True), use_container_width=True, hide_index=True, height=500)

# PAGE: TRANSACTIONS
elif page == "📋 Transactions":
    st.subheader("DLD Transaction Records")
    search = st.text_input("Search area or property type", "")
    df_t = tx_f.copy()
    if search:
        df_t = df_t[df_t.apply(lambda r: search.lower() in str(r).lower(), axis=1)]

    cols_show = [c for c in ['our_area','property_sub_type_en','rooms_en','actual_worth',
                              'meter_sale_price','procedure_area','instance_date',
                              'nearest_metro_en','trans_group_en'] if c in df_t.columns]
    display = df_t[cols_show].copy()
    rename = {'our_area':'Area','property_sub_type_en':'Type','rooms_en':'Rooms',
               'actual_worth':'Value AED','meter_sale_price':'AED/sqft',
               'procedure_area':'sqm','instance_date':'Date',
               'nearest_metro_en':'Metro','trans_group_en':'Transaction'}
    display = display.rename(columns={k:v for k,v in rename.items() if k in display.columns})
    if 'Value AED' in display.columns:
        display['Value AED'] = display['Value AED'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "")
    if 'AED/sqft' in display.columns:
        display['AED/sqft'] = display['AED/sqft'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "")
    st.caption(f"{len(display):,} transactions")
    st.dataframe(display.reset_index(drop=True), use_container_width=True, hide_index=True, height=500)
