import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from utils import get_engine

st.set_page_config(page_title="Price History", layout="wide")
st.title("Price History")

engine = get_engine()


@st.cache_data(ttl=3600)
def load_prices():
    return pd.read_sql(
        "SELECT ticker, trade_date, open_price, high_price, low_price, close_price, volume "
        "FROM analytics.stg_nse_prices ORDER BY ticker, trade_date",
        engine,
    )


df = load_prices()

if df.empty:
    st.warning("No price data found. Run the historical backfill DAG first.")
    st.stop()

df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

tickers = sorted(df["ticker"].unique())
selected = st.selectbox("Ticker", tickers)

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("From", value=df["trade_date"].min())
with col2:
    end_date = st.date_input("To", value=df["trade_date"].max())

filtered = df[
    (df["ticker"] == selected)
    & (df["trade_date"] >= start_date)
    & (df["trade_date"] <= end_date)
].copy()

if filtered.empty:
    st.info("No data for the selected range.")
    st.stop()

fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    row_heights=[0.75, 0.25],
    vertical_spacing=0.03,
)

fig.add_trace(
    go.Candlestick(
        x=filtered["trade_date"],
        open=filtered["open_price"],
        high=filtered["high_price"],
        low=filtered["low_price"],
        close=filtered["close_price"],
        name=selected,
        increasing_line_color="#26a69a",
        decreasing_line_color="#ef5350",
    ),
    row=1, col=1,
)

fig.add_trace(
    go.Bar(
        x=filtered["trade_date"],
        y=filtered["volume"],
        name="Volume",
        marker_color="steelblue",
        opacity=0.7,
    ),
    row=2, col=1,
)

fig.update_layout(
    height=600,
    title=f"{selected} — OHLCV",
    xaxis_rangeslider_visible=False,
    showlegend=False,
)
fig.update_yaxes(title_text="Price (KES)", row=1, col=1)
fig.update_yaxes(title_text="Volume", row=2, col=1)

st.plotly_chart(fig, use_container_width=True)

with st.expander("Raw Data"):
    st.dataframe(
        filtered.sort_values("trade_date", ascending=False).reset_index(drop=True),
        use_container_width=True,
    )
