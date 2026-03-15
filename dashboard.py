import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from datetime import timedelta # Needed for the 7-day default math!

# --- PAGE CONFIGURATION & TABLEAU CSS THEME ---
st.set_page_config(page_title="Commissary Business Intelligence Dashboard", page_icon="🏭", layout="wide")

st.markdown("""
    <style>
    h1, h2, h3 { font-family: 'Arial', sans-serif; color: #2C3E50; }
    [data-testid="stMetricValue"] { color: #1f77b4; font-weight: 700; }
    th { background-color: #f8f9fa !important; color: #333 !important; }
    </style>
""", unsafe_allow_html=True)

# --- DATA LOADING ---
@st.cache_data(ttl=600)
def load_data():
    try:
        conn = sqlite3.connect("factory_operations.db")
        query = "SELECT production_date as date, sku, category, qty_produced, qty_defective FROM production_metrics"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        df['date'] = pd.to_datetime(df['date'], format='mixed').dt.date
        df['qty_good'] = df['qty_produced'] - df['qty_defective']
        return df.sort_values('date')
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return pd.DataFrame()

df_raw = load_data()

if df_raw.empty:
    st.warning("No data found in the database.")
    st.stop()

# --- HELPER FUNCTION: TABLEAU STYLE FOR PLOTLY ---
def apply_tableau_style(fig):
    fig.update_layout(
        template="plotly_white",
        font=dict(family="Arial, sans-serif", size=13, color="#333"),
        margin=dict(l=0, r=50, t=40, b=0), 
        xaxis=dict(showgrid=True, gridcolor='#f0f0f0', zeroline=False),
        yaxis=dict(showgrid=False, zeroline=False),
        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial")
    )
    fig.update_traces(textposition='outside', textangle=0, cliponaxis=False)
    return fig

# --- THE SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Dashboard Controls")
    min_date = df_raw['date'].min()
    max_date = df_raw['date'].max()
    
    # 1. DAILY CONTROL
    st.subheader("1. Daily View")
    selected_date = st.date_input("Select Daily Report Date", value=max_date, min_value=min_date, max_value=max_date)
    
    # 2. PERIOD CONTROL (Default to last 7 days)
    st.subheader("2. Date Range View")
    default_start = max_date - timedelta(days=7)
    if default_start < min_date:
        default_start = min_date
        
    period_selection = st.date_input(
        "Select Reporting Period",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # 3. SKU FILTER
    st.subheader("3. Global Filters")
    all_skus = sorted(df_raw['sku'].unique())
    selected_skus = st.multiselect("Filter by SKU", options=all_skus, default=all_skus)

# Handle the Date Range Tuple (Streamlit returns a tuple of 1 if only start date is clicked)
if len(period_selection) == 2:
    period_start, period_end = period_selection
else:
    period_start = period_selection[0]
    period_end = period_selection[0]

# --- DATA FILTERING ---
# 1. Daily
mask_daily = (df_raw['date'] == selected_date) & (df_raw['sku'].isin(selected_skus))
df_daily = df_raw[mask_daily]

# 2. Period (The New Feature!)
mask_period = (df_raw['date'] >= period_start) & (df_raw['date'] <= period_end) & (df_raw['sku'].isin(selected_skus))
df_period = df_raw[mask_period]

# 3. Cumulative Running
mask_running = (df_raw['date'] <= selected_date) & (df_raw['sku'].isin(selected_skus))
df_running = df_raw[mask_running]


# ==========================================
# --- TIER 1: DAILY ACTUALS SUMMARY ---
# ==========================================
st.title("Commissary Business Intelligence Dashboard")
st.markdown(f"### Daily Report: {selected_date.strftime('%B %d, %Y')}")

if not df_daily.empty:
    daily_produced = df_daily['qty_produced'].sum()
    daily_good = df_daily['qty_good'].sum()
    daily_yield_pct = (daily_good / daily_produced) * 100 if daily_produced > 0 else 0
    
    col1, col2 = st.columns(2)
    with col1:
        formatted_good = f"{daily_good / 1000:.1f}k" if daily_good >= 1000 else f"{daily_good}"
        st.metric(label="Total Yield (Good Units)", value=formatted_good)
    with col2:
        st.metric(label="Percentage Yield (Good Production)", value=f"{daily_yield_pct:.1f}%")
        
    st.markdown("---")
    
    st.markdown("#### SKU Rejects (%)")
    df_rejects = df_daily.groupby('sku')[['qty_produced', 'qty_defective']].sum().reset_index()
    df_rejects['Raw_Defect_Rate'] = (df_rejects['qty_defective'] / df_rejects['qty_produced']) * 100
    df_rejects = df_rejects.sort_values('Raw_Defect_Rate', ascending=False)
    
    df_display = df_rejects[['sku', 'Raw_Defect_Rate']].copy()
    df_display['Defect Rate'] = df_display['Raw_Defect_Rate'].map("{:.1f}%".format)
    df_display = df_display[['sku', 'Defect Rate']] 
    df_display.rename(columns={'sku': 'SKU Name'}, inplace=True)
    st.dataframe(df_display, use_container_width=True, hide_index=True)

else:
    st.info(f"No production recorded on {selected_date}.")

st.divider()

# ==========================================
# --- TIER 2: DAILY SKU PERFORMANCE DEEP DIVE ---
# ==========================================
if not df_daily.empty:
    st.markdown(f"### Daily SKU Performance Deep Dive ({selected_date})")
    deep_col1, deep_col2 = st.columns(2)
    
    df_daily_grouped = df_daily.groupby('sku')[['qty_produced', 'qty_defective']].sum().reset_index()
    df_daily_grouped['defect_rate'] = (df_daily_grouped['qty_defective'] / df_daily_grouped['qty_produced']) * 100
    
    with deep_col1:
        df_vol_sorted = df_daily_grouped.sort_values('qty_produced', ascending=True)
        fig_vol = px.bar(df_vol_sorted, x='qty_produced', y='sku', orientation='h', 
                         title="Daily Volume Produced", color_discrete_sequence=['#1f77b4'])
        fig_vol.update_traces(texttemplate='%{x:.3s}') 
        fig_vol = apply_tableau_style(fig_vol)
        fig_vol.update_yaxes(title_text="")
        st.plotly_chart(fig_vol, use_container_width=True)

    with deep_col2:
        df_def_sorted = df_daily_grouped.sort_values('defect_rate', ascending=True)
        fig_def = px.bar(df_def_sorted, x='defect_rate', y='sku', orientation='h', 
                         title="Daily Defect Rate (%)", color_discrete_sequence=['#d62728'])
        fig_def.update_traces(texttemplate='%{x:.1f}%')
        fig_def = apply_tableau_style(fig_def)
        fig_def.update_yaxes(title_text="")
        st.plotly_chart(fig_def, use_container_width=True)

st.divider()

# ==========================================
# --- TIER 3: PERIOD PERFORMANCE (NEW!) ---
# ==========================================
st.markdown(f"### Reporting Period Performance ({period_start.strftime('%b %d')} to {period_end.strftime('%b %d, %Y')})")

if not df_period.empty:
    per_col1, per_col2 = st.columns(2)
    
    df_per_grouped = df_period.groupby('sku')[['qty_produced', 'qty_defective']].sum().reset_index()
    df_per_grouped['defect_rate'] = (df_per_grouped['qty_defective'] / df_per_grouped['qty_produced']) * 100
    
    with per_col1:
        # Tableau Cyan (#17becf) to differentiate from Daily and Cumulative
        df_per_vol = df_per_grouped.sort_values('qty_produced', ascending=True)
        fig_per_vol = px.bar(df_per_vol, x='qty_produced', y='sku', orientation='h', 
                             title=f"Period Volume Produced", color_discrete_sequence=['#17becf'])
        fig_per_vol.update_traces(texttemplate='%{x:.3s}')
        fig_per_vol = apply_tableau_style(fig_per_vol)
        fig_per_vol.update_yaxes(title_text="")
        st.plotly_chart(fig_per_vol, use_container_width=True)

    with per_col2:
        # Tableau Pink (#e377c2)
        df_per_def = df_per_grouped.sort_values('defect_rate', ascending=True)
        fig_per_def = px.bar(df_per_def, x='defect_rate', y='sku', orientation='h', 
                             title=f"Period Defect Rate (%)", color_discrete_sequence=['#e377c2'])
        fig_per_def.update_traces(texttemplate='%{x:.1f}%')
        fig_per_def = apply_tableau_style(fig_per_def)
        fig_per_def.update_yaxes(title_text="")
        st.plotly_chart(fig_per_def, use_container_width=True)
else:
    st.info("No production recorded in this date range.")

st.divider()

# ==========================================
# --- TIER 4: RUNNING CUMULATIVE DATA ---
# ==========================================
if not df_running.empty:
    st.markdown("### Cumulative Performance (Running Total)")
    st.caption(f"Showing all historical data from **{min_date}** up to **{selected_date}**")
    
    run_col1, run_col2, run_col3 = st.columns(3)
    
    df_run_grouped = df_running.groupby('sku')[['qty_produced', 'qty_defective']].sum().reset_index()
    df_run_grouped['defect_rate'] = (df_run_grouped['qty_defective'] / df_run_grouped['qty_produced']) * 100
    
    with run_col1:
        df_run_vol = df_run_grouped.sort_values('qty_produced', ascending=True)
        fig_run_vol = px.bar(df_run_vol, x='qty_produced', y='sku', orientation='h', 
                             title="Running Volume Produced", color_discrete_sequence=['#2ca02c'])
        fig_run_vol.update_traces(texttemplate='%{x:.3s}')
        fig_run_vol = apply_tableau_style(fig_run_vol)
        fig_run_vol.update_yaxes(title_text="")
        st.plotly_chart(fig_run_vol, use_container_width=True)
        
    with run_col2:
        df_run_def = df_run_grouped.sort_values('qty_defective', ascending=True)
        fig_run_def = px.bar(df_run_def, x='qty_defective', y='sku', orientation='h', 
                             title="Running Defective Units", color_discrete_sequence=['#ff7f0e'])
        fig_run_def.update_traces(texttemplate='%{x:.3s}')
        fig_run_def = apply_tableau_style(fig_run_def)
        fig_run_def.update_yaxes(title_text="")
        fig_run_def.update_xaxes(title_text="") 
        st.plotly_chart(fig_run_def, use_container_width=True)

    with run_col3:
        df_run_rate = df_run_grouped.sort_values('defect_rate', ascending=True)
        fig_run_rate = px.bar(df_run_rate, x='defect_rate', y='sku', orientation='h', 
                             title="Running Defect Rate (%)", color_discrete_sequence=['#9467bd'])
        fig_run_rate.update_traces(texttemplate='%{x:.1f}%')
        fig_run_rate = apply_tableau_style(fig_run_rate)
        fig_run_rate.update_yaxes(title_text="")
        fig_run_rate.update_xaxes(title_text="") 
        st.plotly_chart(fig_run_rate, use_container_width=True)
