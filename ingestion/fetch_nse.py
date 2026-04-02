import time
import requests
import pandas as pd
from datetime import datetime, timedelta

NSE_TICKERS = [
    "EABL.NR",
    "BAMB.NR",
    "KPLC.NR",
    "CARB.NR",
    "UNGA.NR",
]

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/39.0.2171.95 Safari/537.36"
)


def _build_session() -> tuple[requests.Session, str]:
    session = requests.Session()
    session.headers.update({"User-Agent": _UA})

    session.get("https://fc.yahoo.com", timeout=15, allow_redirects=True)
    session.get("https://finance.yahoo.com", timeout=15, allow_redirects=True)
    time.sleep(1)

    crumb_r = session.get(
        "https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=15
    )
    crumb = crumb_r.text.strip()

    if not crumb or "Too Many" in crumb or "<" in crumb:
        raise RuntimeError(f"Failed to get valid crumb: {crumb[:80]}")

    return session, crumb


def _fetch_ticker(
    ticker: str, start_date: str, end_date: str, session: requests.Session, crumb: str
) -> pd.DataFrame:
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {
        "interval": "1d",
        "period1": start_ts,
        "period2": end_ts,
        "crumb": crumb,
    }
    r = session.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    chart = data.get("chart", {})
    error = chart.get("error")
    if error:
        print(f"No data for {ticker}: {error.get('description', error)}")
        return pd.DataFrame()

    result = chart.get("result")
    if not result:
        print(f"No data returned for {ticker}")
        return pd.DataFrame()

    result = result[0]
    timestamps = result.get("timestamp", [])
    if not timestamps:
        print(f"No data returned for {ticker}")
        return pd.DataFrame()

    quotes = result["indicators"]["quote"][0]
    adj_close = result["indicators"].get("adjclose", [{}])[0].get("adjclose", quotes["close"])

    df = pd.DataFrame({
        "ticker": ticker,
        "trade_date": [datetime.utcfromtimestamp(ts).date() for ts in timestamps],
        "open": quotes["open"],
        "high": quotes["high"],
        "low": quotes["low"],
        "close": adj_close,
        "volume": quotes["volume"],
    })
    return df.dropna(subset=["close"])


def fetch_historical(start_date: str, end_date: str = None) -> pd.DataFrame:
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")

    try:
        session, crumb = _build_session()
    except Exception as exc:
        print(f"Failed to initialise Yahoo Finance session: {exc}")
        return pd.DataFrame()

    frames = []
    for ticker in NSE_TICKERS:
        try:
            df = _fetch_ticker(ticker, start_date, end_date, session, crumb)
            if not df.empty:
                frames.append(df)
            time.sleep(0.5)
        except Exception as exc:
            print(f"Error fetching {ticker}: {exc}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)[
        ["ticker", "trade_date", "open", "high", "low", "close", "volume"]
    ]


def fetch_incremental(days: int = 2) -> pd.DataFrame:
    start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    return fetch_historical(start_date=start)
