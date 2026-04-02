"""
Bootstrap historical NSE OHLCV data using Geometric Brownian Motion.
Calibrated to realistic price ranges and volatility for each listed ticker.
Run once to seed the pipeline; incremental fetches handle updates thereafter.
"""
import numpy as np
import pandas as pd
from datetime import date
from pandas.tseries.offsets import BDay

SEED = 42

TICKER_PARAMS = {
    "EABL.NR": {
        "start_price": 195.0,
        "annual_drift": -0.06,
        "annual_vol": 0.22,
        "avg_volume": 180_000,
    },
    "BAMB.NR": {
        "start_price": 50.0,
        "annual_drift": -0.08,
        "annual_vol": 0.28,
        "avg_volume": 120_000,
    },
    "KPLC.NR": {
        "start_price": 4.5,
        "annual_drift": -0.12,
        "annual_vol": 0.35,
        "avg_volume": 900_000,
    },
    "CARB.NR": {
        "start_price": 14.0,
        "annual_drift": 0.02,
        "annual_vol": 0.18,
        "avg_volume": 50_000,
    },
    "UNGA.NR": {
        "start_price": 30.0,
        "annual_drift": -0.04,
        "annual_vol": 0.25,
        "avg_volume": 80_000,
    },
}

START_DATE = "2019-01-01"
END_DATE = "2024-12-31"


def _generate_ohlcv(params: dict, dates: pd.DatetimeIndex, rng: np.random.Generator) -> pd.DataFrame:
    n = len(dates)
    dt = 1 / 252

    mu = params["annual_drift"]
    sigma = params["annual_vol"]
    s0 = params["start_price"]
    avg_vol = params["avg_volume"]

    daily_returns = np.exp(
        (mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * rng.standard_normal(n)
    )
    closes = s0 * np.cumprod(daily_returns)

    daily_range = sigma * np.sqrt(dt) * closes * rng.uniform(0.5, 1.5, n)
    highs = closes + daily_range * rng.uniform(0.3, 0.7, n)
    lows = closes - daily_range * rng.uniform(0.3, 0.7, n)
    opens = lows + (highs - lows) * rng.uniform(0.2, 0.8, n)

    volumes = (avg_vol * rng.lognormal(0, 0.5, n)).astype(int)

    return pd.DataFrame({
        "trade_date": dates.date,
        "open": np.round(opens, 2),
        "high": np.round(highs, 2),
        "low": np.round(lows, 2),
        "close": np.round(closes, 2),
        "volume": volumes,
    })


def generate_all(start: str = START_DATE, end: str = END_DATE) -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    dates = pd.bdate_range(start=start, end=end)

    frames = []
    for ticker, params in TICKER_PARAMS.items():
        df = _generate_ohlcv(params, dates, rng)
        df.insert(0, "ticker", ticker)
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


if __name__ == "__main__":
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from ingestion.load_postgres import load_prices

    print("Generating historical NSE data...")
    df = generate_all()
    print(f"  Generated {len(df):,} rows across {df['ticker'].nunique()} tickers")
    print(f"  Date range: {df['trade_date'].min()} to {df['trade_date'].max()}")

    loaded = load_prices(df)
    print(f"  Loaded {loaded:,} rows into raw.nse_prices")
