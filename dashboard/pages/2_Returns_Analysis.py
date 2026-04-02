import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from utils import get_engine

st.set_page_config(page_title="Returns Analysis", layout="wide")
st.title("Returns Analysis")

engine = get_engine()


@st.cache_data(ttl=3600)
def load_returns():
    return pd.read_sql(
        "SELECT ticker, trade_date, close_price, daily_return_pct, log_return_pct, company_name, sector "
        "FROM analytics.daily_returns ORDER BY ticker, trade_date",
        engine,
    )


df = load_returns()

if df.empty:
    st.warning("No returns data. Run dbt models after the historical backfill.")
    st.stop()

df["trade_date"] = pd.to_datetime(df["trade_date"])

tickers = sorted(df["ticker"].unique())
selected = st.multiselect("Tickers", tickers, default=tickers[:3])

if not selected:
    st.info("Select at least one ticker.")
    st.stop()

filtered = df[df["ticker"].isin(selected)].copy()

st.subheader("Cumulative Returns")
filtered = filtered.sort_values(["ticker", "trade_date"])
filtered["cumulative_return"] = filtered.groupby("ticker")["log_return_pct"].cumsum()

fig_cum = px.line(
    filtered,
    x="trade_date",
    y="cumulative_return",
    color="ticker",
    labels={"cumulative_return": "Cumulative Log Return (%)", "trade_date": "Date"},
)
fig_cum.update_layout(height=400, legend_title="Ticker")
st.plotly_chart(fig_cum, use_container_width=True)

st.subheader("Daily Returns — Selected Ticker")
single = st.selectbox("Ticker for daily bar chart", selected)
daily = filtered[filtered["ticker"] == single].copy()

colors = ["#ef5350" if r < 0 else "#26a69a" for r in daily["daily_return_pct"]]
fig_daily = go.Figure(
    go.Bar(
        x=daily["trade_date"],
        y=daily["daily_return_pct"],
        marker_color=colors,
        name=single,
    )
)
fig_daily.update_layout(
    height=350,
    title=f"{single} — Daily Returns (%)",
    xaxis_title="Date",
    yaxis_title="Return (%)",
)
st.plotly_chart(fig_daily, use_container_width=True)

st.subheader("Monthly Returns Heatmap")
heat_df = filtered.copy()
heat_df["year_month"] = heat_df["trade_date"].dt.to_period("M").astype(str)
monthly = (
    heat_df.groupby(["ticker", "year_month"])["daily_return_pct"]
    .sum()
    .reset_index()
    .pivot(index="ticker", columns="year_month", values="daily_return_pct")
)

fig_heat = px.imshow(
    monthly,
    color_continuous_scale="RdYlGn",
    color_continuous_midpoint=0,
    aspect="auto",
    labels={"color": "Return (%)"},
    title="Monthly Cumulative Returns by Ticker (%)",
)
fig_heat.update_layout(height=300)
st.plotly_chart(fig_heat, use_container_width=True)
