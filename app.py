import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

from utils.data import infer_market, download_prices, latest_prices, fetch_benchmark, get_fx_series, fetch_sector_for_tickers, is_india_ticker
from utils.risk import value_from_positions, weights, daily_returns, portfolio_series, drawdown, herfindahl_hirschman_index, compute_correlation, risk_score, scenario_impact
from utils import visuals

BASE_DIR = Path(__file__).parent

st.set_page_config(page_title="DHANAM — Cloud Build", page_icon="💹", layout="wide")

# Header
c1,c2,c3 = st.columns([0.12,0.6,0.28])
with c1: st.image(str((BASE_DIR / "assets" / "logo.svg").resolve()))
with c2: st.markdown("### **DHANAM — Cloud Build**")
with c3: base_ccy = st.selectbox("Base Currency", ["INR","USD"], index=0)

st.caption("Lightweight build for Streamlit Cloud (PDF features disabled by default)")
st.markdown("---")

# Sidebar
st.sidebar.markdown("## Portfolio")
up = st.sidebar.file_uploader("Upload CSV", type=["csv"])
if up is not None:
    positions = pd.read_csv(up)
else:
    if st.sidebar.button("Load sample"):
        positions = pd.read_csv((BASE_DIR / "sample_portfolio.csv").resolve())
    else:
        positions = pd.DataFrame(columns=["exchange","symbol","quantity","avg_price","sector"])

if st.sidebar.checkbox("Edit in app", value=False):
    positions = st.sidebar.data_editor(positions, num_rows="dynamic")

if positions.empty or positions["symbol"].dropna().empty:
    st.info("Upload a CSV with `symbol, quantity, [avg_price], [sector]`. Use .NS/.BO for India tickers.")
    st.stop()

# Sector autofill
if "sector" not in positions.columns or positions["sector"].fillna("").eq("").any():
    st.sidebar.caption("Filling sector labels (best-effort)…")
    sectors = fetch_sector_for_tickers(positions["symbol"].tolist())
    positions["sector"] = positions.apply(lambda r: r.get("sector") if pd.notna(r.get("sector")) and str(r.get("sector")).strip() else sectors.get(r["symbol"], "Unknown"), axis=1)

period = st.sidebar.selectbox("History", ["6mo","1y","2y","5y"], index=1)
prices = download_prices(positions["symbol"].tolist(), period=period)
if prices.empty:
    st.error("Could not fetch prices; check symbols or try again."); st.stop()

bench_market = infer_market(positions["symbol"].tolist())
bench = fetch_benchmark("IN" if bench_market in ("IN","MIX") and base_ccy=="INR" else "US", period=period)
bench_r = bench.pct_change().dropna()

# FX
fx = get_fx_series(period=period)
fx_latest = float(fx.ffill().iloc[-1]) if not fx.empty else None

# Valuation
latest = latest_prices(prices)
values = value_from_positions(latest, positions, fx_latest, base_ccy)
w = weights(values)

# Metrics
rets = daily_returns(prices)
port_r = portfolio_series(rets, w)
vol_d = float(port_r.std())
idx = port_r.index.intersection(bench_r.index)
beta = float(np.cov(port_r.loc[idx], bench_r.loc[idx])[0,1] / np.var(bench_r.loc[idx])) if len(idx)>5 and np.var(bench_r.loc[idx])>0 else 1.0
dd = float(drawdown(port_r).min())
hhi = float(herfindahl_hirschman_index(w))
score = risk_score({"volatility": vol_d, "beta": beta, "max_drawdown": dd, "hhi": hhi})

# Layout
a,b,c = st.columns([0.33,0.33,0.34])
with a: st.metric("Portfolio Value", f"{float(values.sum()):,.2f} {base_ccy}")
with b: st.metric("Risk Score (0-100)", score)
with c: st.metric("Beta vs Benchmark", f"{beta:.2f}")
st.markdown("---")

x,y = st.columns([0.5,0.5])
with x:
    by_sector = positions.assign(value=values).groupby(positions["sector"].replace({np.nan:"Unknown"}))["value"].sum()
    w_sector = (by_sector/by_sector.sum()).fillna(0.0)
    fig1 = visuals.donut_series(w_sector, "Exposure by Sector")
    st.pyplot(fig1, use_container_width=True)
with y:
    corr = compute_correlation(rets)
    fig2 = visuals.heatmap_corr(corr, "Correlation Heatmap")
    st.pyplot(fig2, use_container_width=True)

m1,m2 = st.columns([0.6,0.4])
with m1:
    perf = pd.DataFrame({"Portfolio":(1+port_r).cumprod(),"Benchmark":(1+bench_r.reindex_like(port_r).fillna(0)).cumprod()}).dropna()
    fig3 = visuals.line_series(perf, "Performance vs Benchmark")
    st.pyplot(fig3, use_container_width=True)
with m2:
    tbl = positions.copy()
    last_conv = []
    for s in positions["symbol"]:
        px = latest.get(s, np.nan)
        if base_ccy=="INR" and not s.upper().endswith((".NS",".BO")): px*=fx_latest
        if base_ccy=="USD" and s.upper().endswith((".NS",".BO")): px/=fx_latest if fx_latest else np.nan
        last_conv.append(px)
    tbl["last_price_base"] = last_conv
    tbl["value"] = values
    tbl["weight_%"] = (w*100).round(2)
    st.dataframe(tbl, use_container_width=True)
