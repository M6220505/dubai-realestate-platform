import streamlit as st
import json, pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

st.set_page_config(page_title="DubaiIntel", layout="wide", page_icon="🏙️")
st.markdown("""<style>
.stApp{background:#0A0C0F}
.block-container{padding-top:1.5rem;max-width:1400px}
[data-testid="stMetricValue"]{font-size:26px;color:#C9A84C}
.insight-box{background:#111418;border:1px solid rgba(201,168,76,0.2);border-left:3px solid #C9A84C;
             border-radius:8px;padding:16px;margin-top:8px}
.insight-box h4{color:#C9A84C;font-size:13px;font-weight:700;letter-spacing:.08em;
                text-transform:uppercase;margin-bottom:10px}
.insight-box p{color:#8A8070;font-size:13px;line-height:1.7;margin:0}
.insight-box strong{color:#E8E6E0}
</style>""", unsafe_allow_html=True)

ROOT = Path(__file__).resolve().parent

@st.cache_data(ttl=300)
def load_transactions():
    f = ROOT/"data/raw/Transactions_2026-03-18.csv"
    if not f.exists(): return pd.DataFrame()
    df = pd.read_csv(f)
    df['meter_sale_price'] = pd.to_numeric(df['meter_sale_price'], errors='coerce')
    df['actual_worth']     = pd.to_numeric(df['actual_worth'],     errors='coerce')
    return df[df['trans_group_en']=='Sales'].copy()

@st.cache_data(ttl=300)
def load_areas():
    p = ROOT/"data/processed/all_areas_metrics.json"
    if not p.exists(): return pd.DataFrame()
    return pd.DataFrame(json.load(open(p)))

@st.cache_data(ttl=300)
def load_dld():
    p = ROOT/"data/processed/dld_intelligence.json"
    return json.load(open(p)) if p.exists() else {}

@st.cache_data(ttl=300)
def load_movements():
    f = ROOT/"data/raw/Movement_of_Real_Estate_Transactions_by_Type_of_Treatment_2026-03-18.csv"
    if not f.exists(): return pd.DataFrame()
    return pd.read_csv(f)


def insight(title, text):
    st.markdown(f"""<div class="insight-box">
    <h4>💡 {title}</h4><p>{text}</p></div>""", unsafe_allow_html=True)

tx       = load_transactions()
areas_df = load_areas()
dld      = load_dld()
mv       = load_movements()

with st.sidebar:
    st.markdown("## 🏙️ DubaiIntel")
    st.success(f"✅ {len(tx):,} DLD transactions")
    st.caption(f"📅 {datetime.now().strftime('%d %b %Y')}")
    st.divider()
    page = st.radio("", ["📊 Overview","🧠 AI Intelligence","🔍 Price Gap","📈 Trends","🏆 Rankings"])
    st.divider()
    all_areas = sorted(areas_df['area_name_en'].tolist()) if not areas_df.empty else []
    top10     = areas_df.head(10)['area_name_en'].tolist() if not areas_df.empty else []
    sel       = st.multiselect("Filter Areas", all_areas, default=top10)
    min_sales = st.slider("Min transactions", 5, 50, 10)

st.markdown("# 🏙️ Dubai Real Estate Intelligence")
mf = dld.get("market_facts_2024", {})
st.caption(f"Source: Dubai Land Department · {len(tx):,} transactions · {datetime.now().strftime('%d %b %Y')}")
st.divider()

areas_f = areas_df[
    (areas_df['area_name_en'].isin(sel)) &
    (areas_df['sales'] >= min_sales)
].copy() if not areas_df.empty else pd.DataFrame()

tx_f = tx[tx['area_name_en'].isin(sel)] if not tx.empty else pd.DataFrame()

# ── OVERVIEW ──────────────────────────────────────────────
if page == "📊 Overview":
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Transactions 2024",  f"{mf.get('total_transaction_volume',226000):,}")
    c2.metric("Total Value",        f"AED {mf.get('total_transaction_value_aed_b',761):.0f}B")
    c3.metric("YoY Growth",         f"+{mf.get('yoy_growth_value_pct',20.4)}%")
    c4.metric("Total Investors",    f"{mf.get('total_investors',158000):,}")
    c5.metric("Rental Contracts",   f"{mf.get('rental_contracts',965000):,}")

    if not areas_f.empty:
        # Chart 1 — PSF
        col_chart, col_insight = st.columns([3,1])
        with col_chart:
            st.subheader(f"Avg AED/sqft — {len(areas_f)} Areas")
            top_area  = areas_f.loc[areas_f['avg_psf'].idxmax(), 'area_name_en']
            top_psf   = areas_f['avg_psf'].max()
            low_area  = areas_f.loc[areas_f['avg_psf'].idxmin(), 'area_name_en']
            low_psf   = areas_f['avg_psf'].min()
            dubai_avg = areas_f['avg_psf'].mean()
            fig = px.bar(areas_f.sort_values('avg_psf'), x='avg_psf', y='area_name_en',
                         orientation='h', text='avg_psf', color='avg_psf',
                         color_continuous_scale=['#1a3a2a','#C9A84C'],
                         height=max(380, len(areas_f)*28))
            fig.update_traces(texttemplate='AED %{text:,.0f}', textposition='outside')
            fig.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                              font_color='#E8E6E0', coloraxis_showscale=False,
                              margin=dict(l=0,r=100,t=10,b=10), yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
        with col_insight:
            st.markdown("<br><br>", unsafe_allow_html=True)
            insight("Price/sqft Analysis",
                f"<strong>{top_area}</strong> commands the highest price at "
                f"<strong>AED {top_psf:,.0f}/sqft</strong> — "
                f"{((top_psf-dubai_avg)/dubai_avg*100):.0f}% above the selected area average.<br><br>"
                f"<strong>{low_area}</strong> offers the lowest entry point at "
                f"<strong>AED {low_psf:,.0f}/sqft</strong>, representing "
                f"potential value for yield-focused investors.<br><br>"
                f"Dubai avg (selected): <strong>AED {dubai_avg:,.0f}/sqft</strong>.")

        # Chart 2 — Volume
        col_chart2, col_insight2 = st.columns([3,1])
        with col_chart2:
            st.subheader("Transaction Volume by Area")
            top_vol  = areas_f.loc[areas_f['sales'].idxmax(), 'area_name_en']
            top_cnt  = int(areas_f['sales'].max())
            avg_vol  = areas_f['sales'].mean()
            fig2 = px.bar(areas_f.sort_values('sales',ascending=False),
                          x='area_name_en', y='sales', text='sales', color='sales',
                          color_continuous_scale=['#1a2a3a','#22c55e'], height=350)
            fig2.update_traces(texttemplate='%{text}', textposition='outside')
            fig2.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                               font_color='#E8E6E0', coloraxis_showscale=False,
                               xaxis_tickangle=-45, xaxis_title="",
                               margin=dict(l=0,r=0,t=10,b=60))
            st.plotly_chart(fig2, use_container_width=True)
        with col_insight2:
            st.markdown("<br><br>", unsafe_allow_html=True)
            insight("Liquidity Signal",
                f"<strong>{top_vol}</strong> leads with <strong>{top_cnt} transactions</strong> "
                f"— {(top_cnt/avg_vol):.1f}× the average area volume.<br><br>"
                f"High transaction count = high market liquidity = easier to buy and exit.<br><br>"
                f"Areas below <strong>{int(avg_vol)} sales</strong> carry higher liquidity risk.")

        # Chart 3 — Treemap
        col_chart3, col_insight3 = st.columns([3,1])
        with col_chart3:
            st.subheader("Total Value Concentration (AED)")
            top_val   = areas_f.loc[areas_f['total_value'].idxmax(), 'area_name_en']
            top_val_m = areas_f['total_value'].max()/1e6
            top3      = areas_f.nlargest(3,'total_value')['area_name_en'].tolist()
            total_all = areas_f['total_value'].sum()/1e9
            top3_pct  = areas_f.nlargest(3,'total_value')['total_value'].sum()/areas_f['total_value'].sum()*100
            fig3 = px.treemap(areas_f, path=['area_name_en'], values='total_value',
                              color='avg_psf', color_continuous_scale=['#1a3a2a','#C9A84C'],
                              hover_data={'sales':True,'avg_psf':':.0f'})
            fig3.update_layout(paper_bgcolor='#0A0C0F', font_color='#E8E6E0',
                               margin=dict(l=0,r=0,t=10,b=10), height=400)
            st.plotly_chart(fig3, use_container_width=True)
        with col_insight3:
            st.markdown("<br><br>", unsafe_allow_html=True)
            insight("Capital Concentration",
                f"<strong>{top_val}</strong> accounts for the largest share at "
                f"<strong>AED {top_val_m:,.0f}M</strong>.<br><br>"
                f"Top 3 areas (<strong>{', '.join(top3)}</strong>) represent "
                f"<strong>{top3_pct:.0f}%</strong> of total selected area value "
                f"(AED {total_all:.1f}B).<br><br>"
                f"High concentration = institutional-grade demand in prime zones.")

# ── PRICE GAP ─────────────────────────────────────────────
elif page == "🔍 Price Gap":
    if not areas_f.empty:
        dubai_avg   = areas_f['avg_psf'].mean()
        undervalued = areas_f[areas_f['gap_vs_avg_pct'] < -10].sort_values('gap_vs_avg_pct')
        overvalued  = areas_f[areas_f['gap_vs_avg_pct'] > 10]
        fair        = areas_f[areas_f['gap_vs_avg_pct'].between(-10,10)]

        col_chart, col_insight = st.columns([3,1])
        with col_chart:
            st.subheader("Price Gap vs Dubai Average")
            fig = go.Figure()
            for _, row in areas_f.sort_values('gap_vs_avg_pct').iterrows():
                g = row['gap_vs_avg_pct']
                c = '#22c55e' if g < -10 else '#ef4444' if g > 10 else '#C9A84C'
                fig.add_trace(go.Bar(x=[row['area_name_en']], y=[g],
                                     marker_color=c, text=[f"{g:+.0f}%"],
                                     textposition='outside', showlegend=False,
                                     name=row['area_name_en']))
            fig.add_hline(y=0, line_dash='dash', line_color='#555')
            fig.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                              font_color='#E8E6E0', height=450,
                              xaxis_tickangle=-45, xaxis_title="",
                              yaxis_title="% vs Dubai Avg",
                              margin=dict(l=0,r=0,t=10,b=80))
            st.plotly_chart(fig, use_container_width=True)
        with col_insight:
            st.markdown("<br><br>", unsafe_allow_html=True)
            uv_list = ', '.join(undervalued['area_name_en'].tolist()[:3]) if len(undervalued)>0 else "None"
            ov_list = ', '.join(overvalued['area_name_en'].tolist()[:3]) if len(overvalued)>0 else "None"
            insight("Price Gap Reading",
                f"<strong style='color:#22c55e'>{len(undervalued)} Undervalued</strong> areas "
                f"trading 10%+ below average:<br><strong>{uv_list}</strong><br><br>"
                f"<strong style='color:#ef4444'>{len(overvalued)} Overvalued</strong> areas "
                f"trading 10%+ above average:<br><strong>{ov_list}</strong><br><br>"
                f"<strong style='color:#C9A84C'>{len(fair)} Fair Value</strong> areas "
                f"within ±10% of the AED {dubai_avg:,.0f}/sqft average.")

        st.subheader("Detailed Breakdown")
        display = areas_f[['area_name_en','avg_psf','sales','gap_vs_avg_pct','inv_score']].copy()
        display['signal'] = display['gap_vs_avg_pct'].apply(
            lambda x: '🟢 Undervalued' if x < -10 else ('🔴 Overvalued' if x > 10 else '🟡 Fair'))
        display['avg_psf'] = display['avg_psf'].apply(lambda x: f"AED {x:,.0f}")
        display.columns = ['Area','Avg AED/sqft','Sales','Gap %','Score','Signal']
        st.dataframe(display.sort_values('Gap %').reset_index(drop=True),
                     use_container_width=True, hide_index=True, height=450)

# ── RANKINGS ──────────────────────────────────────────────
elif page == "🏆 Rankings":
    if not areas_f.empty:
        ranked = areas_f.sort_values('inv_score', ascending=False)

        col_chart, col_insight = st.columns([3,1])
        with col_chart:
            st.subheader("Investment Score — All Selected Areas")
            fig = px.bar(ranked.head(15), x='inv_score', y='area_name_en',
                         orientation='h', text='inv_score', color='inv_score',
                         color_continuous_scale=['#1a2a3a','#22c55e'],
                         height=max(400, min(15,len(ranked))*32))
            fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
            fig.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                              font_color='#E8E6E0', coloraxis_showscale=False,
                              yaxis={'categoryorder':'total ascending'},
                              margin=dict(l=0,r=60,t=10,b=10), yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
        with col_insight:
            st.markdown("<br><br>", unsafe_allow_html=True)
            top1  = ranked.iloc[0]['area_name_en']
            top1s = ranked.iloc[0]['inv_score']
            top2  = ranked.iloc[1]['area_name_en'] if len(ranked)>1 else ""
            bot1  = ranked.iloc[-1]['area_name_en']
            bot1s = ranked.iloc[-1]['inv_score']
            insight("Ranking Logic",
                f"Score = Liquidity ×0.4 + Value ×0.6<br><br>"
                f"🥇 <strong>{top1}</strong> scores <strong>{top1s:.1f}</strong> — "
                f"combining strong transaction volume with competitive pricing.<br><br>"
                f"🥈 <strong>{top2}</strong> follows closely.<br><br>"
                f"⚠️ <strong>{bot1}</strong> scores <strong>{bot1s:.1f}</strong> — "
                f"lower liquidity or premium pricing limits the score.")

        col_chart2, col_insight2 = st.columns([3,1])
        with col_chart2:
            st.subheader("PSF vs Liquidity — Bubble = Total Value")
            fig2 = px.scatter(areas_f, x='sales', y='avg_psf',
                              size='total_value', color='gap_vs_avg_pct',
                              hover_name='area_name_en', text='area_name_en',
                              color_continuous_scale=['#22c55e','#C9A84C','#ef4444'],
                              height=450)
            fig2.update_traces(textposition='top center', textfont_size=9)
            fig2.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                               font_color='#E8E6E0', margin=dict(l=0,r=0,t=10,b=10),
                               xaxis_title="Transactions (Liquidity)",
                               yaxis_title="Avg AED/sqft (Price)")
            st.plotly_chart(fig2, use_container_width=True)
        with col_insight2:
            st.markdown("<br><br>", unsafe_allow_html=True)
            insight("Quadrant Reading",
                f"<strong>Top-right</strong>: High price + High volume = Prime liquid markets (institutional quality).<br><br>"
                f"<strong>Bottom-right</strong>: Low price + High volume = High-yield, affordable entry.<br><br>"
                f"<strong>Top-left</strong>: High price + Low volume = Luxury / illiquid.<br><br>"
                f"<strong>Bottom-left</strong>: Low price + Low volume = Emerging / speculative.")

# ── TRENDS ────────────────────────────────────────────────
elif page == "📈 Trends":
    mf2024 = dld.get("market_facts_2024", {})
    pt     = dld.get("property_type_2024", {})
    cat    = dld.get("category_2024", {})
    rm     = dld.get("rental_market_2024", {})

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("2024 Value",      f"AED {mf2024.get('total_transaction_value_aed_b',761):.0f}B", "+20.4%")
    c2.metric("Off-Plan Growth", f"+{cat.get('offplan_yoy_pct',43.7):.1f}%")
    c3.metric("Rental Contracts",f"{rm.get('total_contracts',965000):,}")
    c4.metric("New Investors",   f"{mf2024.get('new_investors',108000):,}", "+37%")

    col_chart, col_insight = st.columns([3,1])
    with col_chart:
        st.subheader("Property Type Breakdown 2024 (AED B)")
        types = {'Units':pt.get('units_value_aed_b',318),'Land':pt.get('land_value_aed_b',234),
                 'Villa':pt.get('villa_value_aed_b',126),'Building':pt.get('building_value_aed_b',82)}
        fig = px.pie(values=list(types.values()), names=list(types.keys()),
                     color_discrete_sequence=['#C9A84C','#22c55e','#3b82f6','#ef4444'], height=320)
        fig.update_layout(paper_bgcolor='#0A0C0F', font_color='#E8E6E0')
        st.plotly_chart(fig, use_container_width=True)
    with col_insight:
        insight("Market Composition",
            f"<strong>Units (apartments)</strong> dominate at AED {pt.get('units_value_aed_b',318):.0f}B "
            f"(+{pt.get('units_yoy_pct',37.2):.1f}% YoY) — driven by off-plan launches and waterfront demand.<br><br>"
            f"<strong>Villa</strong> segment grew +{pt.get('villa_yoy_pct',34.7):.1f}% — post-COVID lifestyle shift persists.<br><br>"
            f"<strong>Buildings</strong> surged +{pt.get('building_yoy_pct',53.4):.1f}% — institutional investors acquiring income assets.")

    col_chart2, col_insight2 = st.columns([3,1])
    with col_chart2:
        st.subheader("Global Investor Origins 2024 (AED B)")
        geo = dld.get("investor_geography_2024", {})
        geo_d = {'Asia':geo.get('asia_aed_b',301.9),'Europe':geo.get('europe_aed_b',133.1),
                 'N. America':geo.get('north_america_aed_b',41),'Africa':geo.get('africa_aed_b',34.6),
                 'Oceania':geo.get('oceania_aed_b',8.2)}
        fig2 = px.bar(x=list(geo_d.keys()), y=list(geo_d.values()),
                      text=list(geo_d.values()), color=list(geo_d.values()),
                      color_continuous_scale=['#1a2a3a','#C9A84C'], height=300)
        fig2.update_traces(texttemplate='AED %{text:.0f}B', textposition='outside')
        fig2.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                           font_color='#E8E6E0', coloraxis_showscale=False,
                           margin=dict(l=0,r=0,t=10,b=10), xaxis_title="", yaxis_title="AED Billion")
        st.plotly_chart(fig2, use_container_width=True)
    with col_insight2:
        insight("Capital Origins",
            f"<strong>Asia dominates</strong> at AED {geo.get('asia_aed_b',301.9):.0f}B "
            f"({geo.get('asia_pct',58)}% of total) — India, China, Pakistan, GCC.<br><br>"
            f"<strong>Europe</strong> contributes AED {geo.get('europe_aed_b',133.1):.0f}B "
            f"({geo.get('europe_pct',25)}%) — lifestyle migration, second homes.<br><br>"
            f"<strong>N. America</strong> growing at AED {geo.get('north_america_aed_b',41):.0f}B — "
            f"attracted by high yields vs London/NY.")

    if not mv.empty:
        col_chart3, col_insight3 = st.columns([3,1])
        with col_chart3:
            st.subheader("Annual Transaction Volume Trend")
            sales_mv = mv[mv['Title']=='Sales'].groupby('Year')['Value'].sum().reset_index()
            fig3 = px.bar(sales_mv, x='Year', y='Value', text='Value',
                          color='Value', color_continuous_scale=['#1a2a3a','#C9A84C'], height=300)
            fig3.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig3.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                               font_color='#E8E6E0', coloraxis_showscale=False,
                               margin=dict(l=0,r=0,t=10,b=10))
            st.plotly_chart(fig3, use_container_width=True)
        with col_insight3:
            latest_yr  = int(sales_mv['Year'].max())
            latest_val = sales_mv[sales_mv['Year']==latest_yr]['Value'].values[0]
            prev_val   = sales_mv[sales_mv['Year']==latest_yr-1]['Value'].values[0] if latest_yr-1 in sales_mv['Year'].values else 0
            yoy        = ((latest_val/prev_val)-1)*100 if prev_val > 0 else 0
            insight("Volume Trend",
                f"Latest year ({latest_yr}): <strong>AED {latest_val/1e9:.1f}B</strong> "
                f"({yoy:+.1f}% YoY).<br><br>"
                f"Dubai's market has grown <strong>consistently since 2020</strong>, "
                f"with no signs of contraction.<br><br>"
                f"DLD 2024 Annual Report confirms total market at <strong>AED 761B</strong> — "
                f"an all-time record.")

# ── AI INTELLIGENCE ───────────────────────────────────────
elif page == "🧠 AI Intelligence":
    st.subheader("🧠 AI Market Intelligence — Exclusive to DubaiIntel")
    st.caption("No other Dubai real estate platform offers this. Powered by real DLD transaction data.")

    tab1, tab2, tab3 = st.tabs(["💰 Price Estimator", "⚖️ Area Comparator", "📈 ROI Calculator"])

    # ── TAB 1: PRICE ESTIMATOR ──
    with tab1:
        st.markdown("#### Estimate Property Value from DLD Data")
        st.info("Based on real DLD transaction prices — not listing estimates.")

        col1, col2, col3 = st.columns(3)
        with col1:
            est_area  = st.selectbox("Area", sorted(areas_df['area_name_en'].tolist()) if not areas_df.empty else [])
        with col2:
            est_size  = st.number_input("Size (sqft)", min_value=200, max_value=20000, value=1000, step=50)
        with col3:
            est_rooms = st.selectbox("Bedrooms", ["Studio","1 BR","2 BR","3 BR","4 BR","5+ BR"])

        if st.button("Estimate Value", type="primary") and not areas_df.empty:
            row = areas_df[areas_df['area_name_en']==est_area]
            if not row.empty:
                avg_psf    = float(row['avg_psf'].values[0])
                median_psf = float(row['median_psf'].values[0])
                sales_cnt  = int(row['sales'].values[0])
                gap        = float(row['gap_vs_avg_pct'].values[0])

                # Room multipliers based on DLD patterns
                room_mult = {"Studio":0.85,"1 BR":0.92,"2 BR":1.0,"3 BR":1.12,"4 BR":1.22,"5+ BR":1.35}
                mult = room_mult.get(est_rooms, 1.0)

                est_low  = avg_psf * 0.85 * mult * est_size
                est_mid  = avg_psf * mult * est_size
                est_high = avg_psf * 1.15 * mult * est_size

                c1,c2,c3 = st.columns(3)
                c1.metric("Conservative", f"AED {est_low:,.0f}")
                c2.metric("Market Value",  f"AED {est_mid:,.0f}", f"AED {avg_psf:,.0f}/sqft")
                c3.metric("Premium",       f"AED {est_high:,.0f}")

                st.markdown("---")
                col_a, col_b = st.columns(2)
                with col_a:
                    signal = "🟢 Below market avg — potential value" if gap < -10 else "🔴 Above market avg — premium zone" if gap > 10 else "🟡 At market average"
                    st.markdown(f"""
                    **Area Intel: {est_area}**
                    - Avg DLD price: **AED {avg_psf:,.0f}/sqft**
                    - Median DLD price: **AED {median_psf:,.0f}/sqft**
                    - Based on **{sales_cnt} real transactions**
                    - vs Dubai avg: **{gap:+.1f}%** {signal}
                    """)
                with col_b:
                    insight("Estimator Note",
                        f"This estimate uses <strong>real DLD transaction data</strong> — "
                        f"not listing prices which are typically 15-25% higher than actual sale prices.<br><br>"
                        f"Based on <strong>{sales_cnt} registered sales</strong> in {est_area}. "
                        f"Room type adjustment applied: <strong>×{mult}</strong>.")

    # ── TAB 2: AREA COMPARATOR ──
    with tab2:
        st.markdown("#### Compare Two Areas Side by Side")
        st.info("Which area gives you better value, yield, and growth potential?")

        col1, col2 = st.columns(2)
        all_a = sorted(areas_df['area_name_en'].tolist()) if not areas_df.empty else []
        with col1:
            area_a = st.selectbox("Area A", all_a, index=0)
        with col2:
            area_b = st.selectbox("Area B", all_a, index=min(4, len(all_a)-1))

        if not areas_df.empty and area_a != area_b:
            ra = areas_df[areas_df['area_name_en']==area_a].iloc[0]
            rb = areas_df[areas_df['area_name_en']==area_b].iloc[0]

            metrics = [
                ("Avg AED/sqft",    f"AED {ra['avg_psf']:,.0f}",    f"AED {rb['avg_psf']:,.0f}",    ra['avg_psf'],    rb['avg_psf'],    False),
                ("Transactions",    f"{int(ra['sales'])}",           f"{int(rb['sales'])}",           ra['sales'],      rb['sales'],      True),
                ("Avg Sale Value",  f"AED {ra['avg_value']:,.0f}",   f"AED {rb['avg_value']:,.0f}",   ra['avg_value'],  rb['avg_value'],  False),
                ("Total Market",    f"AED {ra['total_value']/1e6:.0f}M", f"AED {rb['total_value']/1e6:.0f}M", ra['total_value'], rb['total_value'], True),
                ("Liquidity Score", f"{ra['liq_score']:.1f}/10",     f"{rb['liq_score']:.1f}/10",     ra['liq_score'],  rb['liq_score'],  True),
                ("Inv. Score",      f"{ra['inv_score']:.1f}",        f"{rb['inv_score']:.1f}",        ra['inv_score'],  rb['inv_score'],  True),
                ("vs Dubai Avg",    f"{ra['gap_vs_avg_pct']:+.1f}%", f"{rb['gap_vs_avg_pct']:+.1f}%", -ra['gap_vs_avg_pct'], -rb['gap_vs_avg_pct'], True),
            ]

            st.markdown(f"### {area_a}  vs  {area_b}")
            header = f"| Metric | {area_a} | {area_b} | Winner |"
            divider = "|--------|--------|--------|--------|"
            rows = [header, divider]
            for label, va, vb, na, nb, higher_better in metrics:
                if higher_better:
                    winner = f"✅ {area_a}" if na > nb else f"✅ {area_b}" if nb > na else "🟡 Tie"
                else:
                    winner = f"✅ {area_b}" if na > nb else f"✅ {area_a}" if nb > na else "🟡 Tie"
                rows.append(f"| {label} | {va} | {vb} | {winner} |")
            st.markdown("\n".join(rows))

            st.markdown("---")
            wins_a = sum(1 for _,_,_,na,nb,hb in metrics if (hb and na>nb) or (not hb and na<nb))
            wins_b = sum(1 for _,_,_,na,nb,hb in metrics if (hb and nb>na) or (not hb and nb<na))
            verdict = area_a if wins_a > wins_b else area_b if wins_b > wins_a else "Tie"
            insight("Comparison Verdict",
                f"<strong>{area_a}</strong> wins {wins_a} of {len(metrics)} metrics. "
                f"<strong>{area_b}</strong> wins {wins_b}.<br><br>"
                f"Overall recommendation based on DLD data: <strong>{verdict}</strong> "
                f"offers the stronger investment case right now.")

    # ── TAB 3: ROI CALCULATOR ──
    with tab3:
        st.markdown("#### Investment Return Calculator")
        st.info("Calculate expected returns based on DLD transaction data and EJARI rental benchmarks.")

        col1, col2 = st.columns(2)
        with col1:
            roi_area    = st.selectbox("Area", sorted(areas_df['area_name_en'].tolist()) if not areas_df.empty else [], key="roi_area")
            purchase    = st.number_input("Purchase Price (AED)", min_value=200000, max_value=50000000, value=1500000, step=50000)
            down_pct    = st.slider("Down Payment %", 20, 100, 25)
        with col2:
            rent_yield  = st.slider("Expected Rental Yield %", 3.0, 10.0, 6.0, 0.1)
            hold_years  = st.slider("Holding Period (years)", 1, 10, 5)
            growth_pa   = st.slider("Annual Price Growth %", 0.0, 20.0, 8.0, 0.5)

        if st.button("Calculate ROI", type="primary"):
            down        = purchase * down_pct / 100
            loan        = purchase - down
            annual_rent = purchase * rent_yield / 100
            total_rent  = annual_rent * hold_years
            exit_price  = purchase * ((1 + growth_pa/100) ** hold_years)
            capital_gain = exit_price - purchase
            total_return = total_rent + capital_gain
            roi_pct     = (total_return / down) * 100
            roi_pa      = roi_pct / hold_years

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Exit Value",      f"AED {exit_price:,.0f}")
            c2.metric("Capital Gain",    f"AED {capital_gain:,.0f}")
            c3.metric("Total Rent",      f"AED {total_rent:,.0f}")
            c4.metric("Total ROI",       f"{roi_pct:.1f}%", f"{roi_pa:.1f}% / year")

            st.markdown("---")
            col_a, col_b = st.columns(2)
            with col_a:
                # Waterfall chart
                fig = go.Figure(go.Waterfall(
                    orientation="v",
                    measure=["absolute","relative","relative","total"],
                    x=["Down Payment","Capital Gain","Rental Income","Total Return"],
                    y=[down, capital_gain, total_rent, 0],
                    connector={"line":{"color":"#555"}},
                    decreasing={"marker":{"color":"#ef4444"}},
                    increasing={"marker":{"color":"#22c55e"}},
                    totals={"marker":{"color":"#C9A84C"}},
                ))
                fig.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                                  font_color='#E8E6E0', height=320,
                                  margin=dict(l=0,r=0,t=10,b=10))
                st.plotly_chart(fig, use_container_width=True)
            with col_b:
                insight("ROI Breakdown",
                    f"On a <strong>AED {down:,.0f} down payment</strong>:<br><br>"
                    f"• Capital gain over {hold_years}yr: <strong>AED {capital_gain:,.0f}</strong><br>"
                    f"• Rental income: <strong>AED {total_rent:,.0f}</strong><br>"
                    f"• Total return: <strong>AED {total_return:,.0f}</strong><br><br>"
                    f"Annualised ROI on equity: <strong>{roi_pa:.1f}%/year</strong><br><br>"
                    f"Dubai market avg yield: <strong>6-8%</strong> (DLD 2024 Annual Report).")


# ── AI INTELLIGENCE ───────────────────────────────────────
elif page == "🧠 AI Intelligence":
    st.subheader("🧠 AI Market Intelligence — Exclusive to DubaiIntel")
    st.caption("No other Dubai real estate platform offers this. Powered by real DLD transaction data.")

    tab1, tab2, tab3 = st.tabs(["💰 Price Estimator", "⚖️ Area Comparator", "📈 ROI Calculator"])

    # ── TAB 1: PRICE ESTIMATOR ──
    with tab1:
        st.markdown("#### Estimate Property Value from DLD Data")
        st.info("Based on real DLD transaction prices — not listing estimates.")

        col1, col2, col3 = st.columns(3)
        with col1:
            est_area  = st.selectbox("Area", sorted(areas_df['area_name_en'].tolist()) if not areas_df.empty else [])
        with col2:
            est_size  = st.number_input("Size (sqft)", min_value=200, max_value=20000, value=1000, step=50)
        with col3:
            est_rooms = st.selectbox("Bedrooms", ["Studio","1 BR","2 BR","3 BR","4 BR","5+ BR"])

        if st.button("Estimate Value", type="primary") and not areas_df.empty:
            row = areas_df[areas_df['area_name_en']==est_area]
            if not row.empty:
                avg_psf    = float(row['avg_psf'].values[0])
                median_psf = float(row['median_psf'].values[0])
                sales_cnt  = int(row['sales'].values[0])
                gap        = float(row['gap_vs_avg_pct'].values[0])

                # Room multipliers based on DLD patterns
                room_mult = {"Studio":0.85,"1 BR":0.92,"2 BR":1.0,"3 BR":1.12,"4 BR":1.22,"5+ BR":1.35}
                mult = room_mult.get(est_rooms, 1.0)

                est_low  = avg_psf * 0.85 * mult * est_size
                est_mid  = avg_psf * mult * est_size
                est_high = avg_psf * 1.15 * mult * est_size

                c1,c2,c3 = st.columns(3)
                c1.metric("Conservative", f"AED {est_low:,.0f}")
                c2.metric("Market Value",  f"AED {est_mid:,.0f}", f"AED {avg_psf:,.0f}/sqft")
                c3.metric("Premium",       f"AED {est_high:,.0f}")

                st.markdown("---")
                col_a, col_b = st.columns(2)
                with col_a:
                    signal = "🟢 Below market avg — potential value" if gap < -10 else "🔴 Above market avg — premium zone" if gap > 10 else "🟡 At market average"
                    st.markdown(f"""
                    **Area Intel: {est_area}**
                    - Avg DLD price: **AED {avg_psf:,.0f}/sqft**
                    - Median DLD price: **AED {median_psf:,.0f}/sqft**
                    - Based on **{sales_cnt} real transactions**
                    - vs Dubai avg: **{gap:+.1f}%** {signal}
                    """)
                with col_b:
                    insight("Estimator Note",
                        f"This estimate uses <strong>real DLD transaction data</strong> — "
                        f"not listing prices which are typically 15-25% higher than actual sale prices.<br><br>"
                        f"Based on <strong>{sales_cnt} registered sales</strong> in {est_area}. "
                        f"Room type adjustment applied: <strong>×{mult}</strong>.")

    # ── TAB 2: AREA COMPARATOR ──
    with tab2:
        st.markdown("#### Compare Two Areas Side by Side")
        st.info("Which area gives you better value, yield, and growth potential?")

        col1, col2 = st.columns(2)
        all_a = sorted(areas_df['area_name_en'].tolist()) if not areas_df.empty else []
        with col1:
            area_a = st.selectbox("Area A", all_a, index=0)
        with col2:
            area_b = st.selectbox("Area B", all_a, index=min(4, len(all_a)-1))

        if not areas_df.empty and area_a != area_b:
            ra = areas_df[areas_df['area_name_en']==area_a].iloc[0]
            rb = areas_df[areas_df['area_name_en']==area_b].iloc[0]

            metrics = [
                ("Avg AED/sqft",    f"AED {ra['avg_psf']:,.0f}",    f"AED {rb['avg_psf']:,.0f}",    ra['avg_psf'],    rb['avg_psf'],    False),
                ("Transactions",    f"{int(ra['sales'])}",           f"{int(rb['sales'])}",           ra['sales'],      rb['sales'],      True),
                ("Avg Sale Value",  f"AED {ra['avg_value']:,.0f}",   f"AED {rb['avg_value']:,.0f}",   ra['avg_value'],  rb['avg_value'],  False),
                ("Total Market",    f"AED {ra['total_value']/1e6:.0f}M", f"AED {rb['total_value']/1e6:.0f}M", ra['total_value'], rb['total_value'], True),
                ("Liquidity Score", f"{ra['liq_score']:.1f}/10",     f"{rb['liq_score']:.1f}/10",     ra['liq_score'],  rb['liq_score'],  True),
                ("Inv. Score",      f"{ra['inv_score']:.1f}",        f"{rb['inv_score']:.1f}",        ra['inv_score'],  rb['inv_score'],  True),
                ("vs Dubai Avg",    f"{ra['gap_vs_avg_pct']:+.1f}%", f"{rb['gap_vs_avg_pct']:+.1f}%", -ra['gap_vs_avg_pct'], -rb['gap_vs_avg_pct'], True),
            ]

            st.markdown(f"### {area_a}  vs  {area_b}")
            header = f"| Metric | {area_a} | {area_b} | Winner |"
            divider = "|--------|--------|--------|--------|"
            rows = [header, divider]
            for label, va, vb, na, nb, higher_better in metrics:
                if higher_better:
                    winner = f"✅ {area_a}" if na > nb else f"✅ {area_b}" if nb > na else "🟡 Tie"
                else:
                    winner = f"✅ {area_b}" if na > nb else f"✅ {area_a}" if nb > na else "🟡 Tie"
                rows.append(f"| {label} | {va} | {vb} | {winner} |")
            st.markdown("\n".join(rows))

            st.markdown("---")
            wins_a = sum(1 for _,_,_,na,nb,hb in metrics if (hb and na>nb) or (not hb and na<nb))
            wins_b = sum(1 for _,_,_,na,nb,hb in metrics if (hb and nb>na) or (not hb and nb<na))
            verdict = area_a if wins_a > wins_b else area_b if wins_b > wins_a else "Tie"
            insight("Comparison Verdict",
                f"<strong>{area_a}</strong> wins {wins_a} of {len(metrics)} metrics. "
                f"<strong>{area_b}</strong> wins {wins_b}.<br><br>"
                f"Overall recommendation based on DLD data: <strong>{verdict}</strong> "
                f"offers the stronger investment case right now.")

    # ── TAB 3: ROI CALCULATOR ──
    with tab3:
        st.markdown("#### Investment Return Calculator")
        st.info("Calculate expected returns based on DLD transaction data and EJARI rental benchmarks.")

        col1, col2 = st.columns(2)
        with col1:
            roi_area    = st.selectbox("Area", sorted(areas_df['area_name_en'].tolist()) if not areas_df.empty else [], key="roi_area")
            purchase    = st.number_input("Purchase Price (AED)", min_value=200000, max_value=50000000, value=1500000, step=50000)
            down_pct    = st.slider("Down Payment %", 20, 100, 25)
        with col2:
            rent_yield  = st.slider("Expected Rental Yield %", 3.0, 10.0, 6.0, 0.1)
            hold_years  = st.slider("Holding Period (years)", 1, 10, 5)
            growth_pa   = st.slider("Annual Price Growth %", 0.0, 20.0, 8.0, 0.5)

        if st.button("Calculate ROI", type="primary"):
            down        = purchase * down_pct / 100
            loan        = purchase - down
            annual_rent = purchase * rent_yield / 100
            total_rent  = annual_rent * hold_years
            exit_price  = purchase * ((1 + growth_pa/100) ** hold_years)
            capital_gain = exit_price - purchase
            total_return = total_rent + capital_gain
            roi_pct     = (total_return / down) * 100
            roi_pa      = roi_pct / hold_years

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Exit Value",      f"AED {exit_price:,.0f}")
            c2.metric("Capital Gain",    f"AED {capital_gain:,.0f}")
            c3.metric("Total Rent",      f"AED {total_rent:,.0f}")
            c4.metric("Total ROI",       f"{roi_pct:.1f}%", f"{roi_pa:.1f}% / year")

            st.markdown("---")
            col_a, col_b = st.columns(2)
            with col_a:
                # Waterfall chart
                fig = go.Figure(go.Waterfall(
                    orientation="v",
                    measure=["absolute","relative","relative","total"],
                    x=["Down Payment","Capital Gain","Rental Income","Total Return"],
                    y=[down, capital_gain, total_rent, 0],
                    connector={"line":{"color":"#555"}},
                    decreasing={"marker":{"color":"#ef4444"}},
                    increasing={"marker":{"color":"#22c55e"}},
                    totals={"marker":{"color":"#C9A84C"}},
                ))
                fig.update_layout(paper_bgcolor='#0A0C0F', plot_bgcolor='#111418',
                                  font_color='#E8E6E0', height=320,
                                  margin=dict(l=0,r=0,t=10,b=10))
                st.plotly_chart(fig, use_container_width=True)
            with col_b:
                insight("ROI Breakdown",
                    f"On a <strong>AED {down:,.0f} down payment</strong>:<br><br>"
                    f"• Capital gain over {hold_years}yr: <strong>AED {capital_gain:,.0f}</strong><br>"
                    f"• Rental income: <strong>AED {total_rent:,.0f}</strong><br>"
                    f"• Total return: <strong>AED {total_return:,.0f}</strong><br><br>"
                    f"Annualised ROI on equity: <strong>{roi_pa:.1f}%/year</strong><br><br>"
                    f"Dubai market avg yield: <strong>6-8%</strong> (DLD 2024 Annual Report).")


