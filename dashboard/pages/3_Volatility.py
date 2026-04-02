import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import plotly.express as px
import streamlit as st
from utils import get_engine

st.set_page_config(page_title="Volatility", layout="wide")
st.title("Volatility Analysis")

engine = get_engine()


@st.cache_data(ttl=3600)
def load_volatility():
    return pd.read_sql(
        "SELECT ticker, trade_date, company_name, sector, rolling_vol_20d, annualised_vol "
        "FROM analytics.volatility_metrics ORDER BY ticker, trade_date",
        engine,
    )


df = load_volatility()

if df.empty:
    st.warning("No volatility data. Run dbt models after the historical backfill.")
    st.stop()

df["trade_date"] = pd.to_datetime(df["trade_date"])

st.subheader("Rolling 20-Day Volatility")
tickers = sorted(df["ticker"].unique())
selected = st.multiselect("Tickers", tickers, default=tickers)

if not selected:
    st.info("Select at least one ticker.")
    st.stop()

filtered = df[df["ticker"].isin(selected)]

fig_roll = px.line(
    filtered,
    x="trade_date",
    y="rolling_vol_20d",
    color="ticker",
    labels={"rolling_vol_20d": "20-Day Rolling Vol (%)", "trade_date": "Date"},
    title="Rolling 20-Day Realised Volatility",
)
fig_roll.update_layout(height=420, legend_title="Ticker")
st.plotly_chart(fig_roll, use_container_width=True)

st.subheader("Annualised Volatility — Latest Reading")
latest = (
    filtered.sort_values("trade_date")
    .groupby("ticker")
    .last()
    .reset_index()[["ticker", "company_name", "annualised_vol"]]
    .sort_values("annualised_vol", ascending=True)
)

fig_bar = px.bar(
    latest,
    x="annualised_vol",
    y="company_name",
    orientation="h",
    color="annualised_vol",
    color_continuous_scale="Reds",
    labels={"annualised_vol": "Annualised Vol (%)", "company_name": "Company"},
    title="Annualised Volatility by Company (latest 20-day window)",
)
fig_bar.update_layout(height=350, coloraxis_showscale=False)
st.plotly_chart(fig_bar, use_container_width=True)

st.subheader("Volatility Data Table")
st.dataframe(
    latest.rename(columns={"annualised_vol": "Annualised Vol (%)"}),
    use_container_width=True,
)
