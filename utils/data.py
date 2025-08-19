import time
from typing import List, Dict
import pandas as pd
import yfinance as yf

BENCHMARKS = {"IN":"^NSEI","US":"^GSPC"}

def is_india_ticker(t: str) -> bool:
    t = (t or "").upper()
    return t.endswith(".NS") or t.endswith(".BO")

def infer_market(tickers: List[str]) -> str:
    tickers = [str(t).strip() for t in tickers if str(t).strip()]
    has_in = any(is_india_ticker(t) for t in tickers)
    has_us = any(not is_india_ticker(t) for t in tickers)
    if has_in and has_us: return "MIX"
    return "IN" if has_in else "US"

def _batch_download(tickers: List[str], period: str) -> pd.DataFrame:
    # yfinance sometimes returns empty for mixed markets; try batch first
    try:
        df = yf.download(
            tickers,
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="column",
            threads=True,
        )
    except Exception:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        close = df.get("Close")
        if close is None:
            return pd.DataFrame()
        return close.dropna(how="all")
    # Single column dataframe case
    if "Close" in df:
        return df["Close"].to_frame().dropna(how="all")
    return pd.DataFrame()

def _per_ticker_download(tickers: List[str], period: str) -> pd.DataFrame:
    frames = []
    for t in tickers:
        try:
            df = yf.download(t, period=period, interval="1d", auto_adjust=True, progress=False)
            if "Close" in df and not df["Close"].dropna().empty:
                frames.append(df["Close"].rename(t))
        except Exception:
            continue
        time.sleep(0.2)  # be gentle with Yahoo
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=1).dropna(how="all")

def download_prices(tickers: List[str], period: str = "1y") -> pd.DataFrame:
    tickers = list(dict.fromkeys([str(t).strip() for t in tickers if str(t).strip()]))  # unique & clean
    if not tickers:
        return pd.DataFrame()
    # Try batch first
    prices = _batch_download(tickers, period)
    if prices.empty or prices.shape[1] < max(1, int(0.4*len(tickers))):
        # Fallback to per-ticker
        pt = _per_ticker_download(tickers, period)
        if not pt.empty:
            prices = pt
    return prices.dropna(how="all")

def latest_prices(prices: pd.DataFrame) -> pd.Series:
    return prices.ffill().iloc[-1]

def fetch_benchmark(market: str, period: str = "1y") -> pd.Series:
    sym = BENCHMARKS.get(market, "^GSPC")
    try:
        df = yf.download(sym, period=period, interval="1d", auto_adjust=True, progress=False)
        s = df["Close"].dropna()
        if not s.empty:
            return s
    except Exception:
        pass
    # Fallback to S&P 500
    df2 = yf.download("^GSPC", period=period, interval="1d", auto_adjust=True, progress=False)
    return df2["Close"].dropna()

def get_fx_series(period: str = "1y") -> pd.Series:
    df = yf.download("USDINR=X", period=period, interval="1d", auto_adjust=True, progress=False)
    return df["Close"].dropna()

def fetch_sector_for_tickers(tickers: List[str]) -> Dict[str, str]:
    out = {}
    for t in tickers:
        sector = None
        try:
            info = yf.Ticker(t).info or {}
            sector = info.get("sector")
        except Exception:
            pass
        out[t] = sector or "Unknown"
    return out
