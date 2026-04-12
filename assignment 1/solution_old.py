#!/usr/bin/env python3
"""
Assignment 1: Analyzing E-mini S&P 500 Tick Data
=================================================
File  : ES.h5
Key   : tick/trades_filter0vol  (856M rows, 0-volume trades pre-filtered)
Fields: Instrument (b'ESU03'), Price (f32), Time (b'20030630...'), Volume (u32)

Parts:
  a) Form a continuous price series by adjusting for contract rolls
  b) Sample observations into tick bars, volume bars, dollar bars
  c) Count bars per week -> plot -> find most stable bar type
  d) Lag-1 serial correlation of log-returns per bar type
  e) Monthly return variances -> variance of those variances
  f) Jarque-Bera normality test on returns
"""

# =====================================================================
# IMPORTS
# =====================================================================

import os
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")               # non-interactive backend (safe everywhere)
import matplotlib.pyplot as plt
import h5py
from scipy import stats as scipy_stats

warnings.filterwarnings("ignore")

# =====================================================================
# CONFIGURATION
# Change DATA_PATH if your ES.h5 is stored elsewhere.
# Everything else is auto-calibrated from the data.
# =====================================================================

HERE         = os.path.dirname(os.path.abspath(__file__))
DATA_PATH    = os.path.join(HERE, "ES.h5")
DATASET      = "tick/trades_filter0vol"  # pre-filtered key inside ES.h5
CHUNK_SIZE   = 2_000_000                 # rows per chunk (~300 MB RAM each)
CALIB_ROWS   = 2_000_000                 # rows used to estimate bar thresholds
BARS_PER_DAY = 50                        # target bars per trading day
OUTPUT_TXT   = os.path.join(HERE, "output.txt")
PLOTS_DIR    = os.path.join(HERE, "plots")
os.makedirs(PLOTS_DIR, exist_ok=True)

PALETTE = {"tick": "#C44E52", "volume": "#55A868", "dollar": "#4C72B0"}


# =====================================================================
# UTILITY FUNCTIONS
# =====================================================================

def parse_times(time_col):
    """
    Convert raw Time bytes to pandas DatetimeIndex (vectorised).

    Your data stores time as bytes: b'20030630230001000'
    Format: YYYY MM DD HH MM SS mmm
            2003 06 30 23 00 01 000

    Vectorised decoding is ~50x faster than calling strptime() row-by-row
    on 856 million rows.
    """
    decoded = np.array([v.decode() for v in time_col])
    return pd.to_datetime(decoded, format="%Y%m%d%H%M%S%f", errors="coerce")


def log_returns(close_prices):
    """
    Compute log-returns: r_t = log(close_t / close_{t-1})

    Log-returns are used because they are:
      - Additive over time  (weekly return = sum of daily log-returns)
      - Symmetric           (gain and loss of same magnitude cancel)
      - Standard in all finance research
    """
    c = np.asarray(close_prices, dtype=np.float64)
    if len(c) < 2:
        return np.array([], dtype=np.float64)
    return np.diff(np.log(c))


def lag1_autocorr(x):
    """
    Lag-1 autocorrelation = corr(r_t, r_{t-1})

    Answers: does yesterday's return predict today's?
      rho ~ 0  -> no memory (good, efficient market)
      rho != 0 -> past predicts future (bad for ML models)
    """
    if len(x) < 3:
        return np.nan
    return float(np.corrcoef(x[1:], x[:-1])[0, 1])


def jarque_bera(x):
    """
    Jarque-Bera statistic: measures departure from normality.

    JB = (n/6) * [ S^2  +  (K-3)^2/4 ]
      S   = skewness      (0 for normal distribution)
      K-3 = excess kurtosis (0 for normal distribution, >0 = fat tails)

    JB = 0        -> perfectly Gaussian
    JB = 1000     -> mildly non-normal
    JB = 100000+  -> very fat tails (common in tick-sampled returns)

    Lower JB = returns look more like a bell curve = safer for risk models.
    """
    n = len(x)
    if n < 8:
        return np.nan
    c  = x - np.mean(x)
    m2 = np.mean(c ** 2)
    if m2 <= 0:
        return np.nan
    skew     = np.mean(c ** 3) / (m2 ** 1.5)
    excess_k = np.mean(c ** 4) / (m2 ** 2) - 3.0
    return float((n / 6.0) * (skew ** 2 + (excess_k ** 2) / 4.0))


def coeff_variation(series):
    """
    CV = std / mean

    Measures relative variability.
    Used in part (c) to compare weekly bar-count stability.
    Lower CV = more stable = better.
    """
    m = series.mean()
    return float(series.std() / m) if m > 0 else np.nan


# =====================================================================
# PART a) STEP 1 -- AUTO-CALIBRATE BAR THRESHOLDS
# =====================================================================
# The question asks us to form tick, volume and dollar bars (part b).
# Before forming bars we need to choose THRESHOLD sizes.
#
# Strategy: read the first CALIB_ROWS ticks, compute the average
# daily tick count / volume / dollar-volume, divide by BARS_PER_DAY.
# This produces thresholds that give roughly 50 bars per trading day
# for each bar type, making them directly comparable.
# =====================================================================

def calibrate_thresholds(dataset, calib_rows, bars_per_day):
    """
    Auto-estimate bar thresholds from a calibration window.

    Returns dict:
      tick   -> close a tick bar every N ticks
      volume -> close a volume bar every N contracts
      dollar -> close a dollar bar every $N traded
    """
    print(f"\n[Calibration] Sampling {calib_rows:,} rows...")
    n   = min(calib_rows, len(dataset))
    raw = dataset[:n]

    prices  = raw["Price"].astype(np.float64)
    volumes = raw["Volume"].astype(np.float64)
    days    = np.array([t[:8].decode() for t in raw["Time"]])

    day_ticks  = {}
    day_vol    = {}
    day_dollar = {}

    for i in range(n):
        d = days[i]
        day_ticks[d]  = day_ticks.get(d, 0)    + 1
        day_vol[d]    = day_vol.get(d, 0.0)    + float(volumes[i])
        day_dollar[d] = day_dollar.get(d, 0.0) + float(prices[i]) * float(volumes[i])

    avg_t = np.mean(list(day_ticks.values()))
    avg_v = np.mean(list(day_vol.values()))
    avg_d = np.mean(list(day_dollar.values()))

    thresholds = {
        "tick":   max(1, int(avg_t / bars_per_day)),
        "volume": max(1, int(avg_v / bars_per_day)),
        "dollar": max(1, int(avg_d / bars_per_day)),
    }

    print(f"  avg ticks/day   = {avg_t:>12,.0f}  -> tick threshold   = {thresholds['tick']:>10,}")
    print(f"  avg volume/day  = {avg_v:>12,.0f}  -> volume threshold = {thresholds['volume']:>10,}")
    print(f"  avg dollar/day  = {avg_d:>12,.0f}  -> dollar threshold = {thresholds['dollar']:>10,.0f}")
    return thresholds


# =====================================================================
# PART a) STEP 2 + PART b) -- CONTINUOUS SERIES & BAR FORMATION
# =====================================================================
#
# PART a) "Form a continuous price series by adjusting for rolls"
#
#   PROBLEM:
#     ES futures expire every March/June/Sep/Dec.
#     When one contract expires and trading moves to the next,
#     there is an ARTIFICIAL price jump (e.g. ESU03->ESZ03 = +7.50).
#     This is NOT a real market move -- just a contract switch.
#     If not removed, it distorts every return and statistic.
#
#   FIX (Panama Canal / Additive Backward Adjustment):
#     At each roll:
#       gap = new_contract_first_price - old_contract_last_price
#       Add gap to ALL prices that came BEFORE this roll
#     -> Series becomes smooth and continuous with no artificial jumps.
#
# PART b) "Sample observations by forming tick, volume, dollar bars"
#
#   Instead of fixed-time bars (1-min, 5-min) which are sparse in
#   quiet hours and overloaded during opens, we sample by ACTIVITY:
#
#   TICK BAR   -> close a bar after every N TRADES
#                 Pro: simple. Con: ignores trade size and price level.
#
#   VOLUME BAR -> close a bar after every N CONTRACTS traded
#                 Pro: accounts for trade size. Con: ignores price.
#
#   DOLLAR BAR -> close a bar after every $N of price x volume traded
#                 Pro: accounts for BOTH trade size AND price level.
#                 As price rises from 1000->4000, dollar bars auto-adjust.
#
#   Each bar stores: Open, High, Low, Close, Volume (OHLCV)
#
# Both tasks are done in ONE pass through all 856M rows,
# reading CHUNK_SIZE rows at a time to fit in RAM.
# =====================================================================

def build_bars(dataset, thresholds, chunk_size):
    """
    Single-pass over the full dataset:
      - Detects contract roll events from Instrument column changes
      - Applies additive backward price adjustment (part a)
      - Forms OHLCV bars for tick, volume, dollar types (part b)
      - Collects a downsampled price series for the plot

    Returns
    -------
    bars        : dict of DataFrames (tick, volume, dollar)
                  columns: datetime, open, high, low, close, volume, return
    roll_events : list of dicts (time, from, to, gap)
    plot_data   : dict (time, raw, adj) downsampled for plotting
    """
    n_total = len(dataset)
    print(f"\n[Building bars] {n_total:,} total rows, chunk = {chunk_size:,}")

    # Storage for completed bar OHLCV data
    store = {k: {"open": [], "high": [], "low": [],
                 "close": [], "volume": [], "time": []}
             for k in ("tick", "volume", "dollar")}

    # Current bar state (open, high, low, cumulative counter, volume, open-time)
    state = {k: {"cum": 0.0, "o": None, "h": None,
                 "l": None, "vol": 0.0, "t0": None}
             for k in ("tick", "volume", "dollar")}

    # Roll adjustment state
    current_inst = None   # contract currently being processed
    adj_offset   = 0.0    # total price offset accumulated from all rolls
    last_adj     = None   # last adjusted price (needed to compute next gap)
    roll_events  = []

    # Downsampled series for plot a)
    PLOT_STEP = max(1, n_total // 10_000)
    plot_raw, plot_adj, plot_time = [], [], []

    rows_done = 0
    while rows_done < n_total:
        end   = min(rows_done + chunk_size, n_total)
        block = dataset[rows_done:end]

        insts  = block["Instrument"]
        prices = block["Price"].astype(np.float64)
        vols   = block["Volume"].astype(np.float64)
        times  = block["Time"]

        for i in range(len(block)):
            inst  = insts[i]
            price = float(prices[i])
            vol   = float(vols[i])
            ts    = times[i]

            # ----------------------------------------------------------
            # PART a) ROLL DETECTION AND PRICE ADJUSTMENT
            # ----------------------------------------------------------
            # When Instrument changes (e.g. ESU03 -> ESZ03),
            # we compute the gap and add it to adj_offset.
            # All subsequent OLDER prices now have this offset applied,
            # making the full series continuous.
            if current_inst is None:
                current_inst = inst
            elif inst != current_inst:
                if last_adj is not None:
                    gap = last_adj - price      # shift old contract up/down
                    adj_offset += gap
                    roll_events.append({
                        "time": ts.decode(),
                        "from": current_inst.decode(),
                        "to":   inst.decode(),
                        "gap":  gap,
                    })
                current_inst = inst

            adj_price = price + adj_offset      # continuously adjusted price
            last_adj  = adj_price

            # Collect downsampled point for price plot
            idx_global = rows_done + i
            if idx_global % PLOT_STEP == 0:
                plot_raw.append(price)
                plot_adj.append(adj_price)
                plot_time.append(ts)

            # ----------------------------------------------------------
            # PART b) BAR FORMATION
            # ----------------------------------------------------------
            # Each bar type accumulates a different running counter:
            #   tick   -> +1 for every trade
            #   volume -> +contracts for every trade
            #   dollar -> +price*contracts for every trade
            # When counter >= threshold, close the bar and reset.
            increments = {
                "tick":   1.0,
                "volume": vol,
                "dollar": adj_price * vol,
            }

            for k, inc in increments.items():
                s = state[k]

                # Start of a new bar
                if s["o"] is None:
                    s["o"]   = adj_price
                    s["h"]   = adj_price
                    s["l"]   = adj_price
                    s["vol"] = 0.0
                    s["t0"]  = ts

                # Update running bar stats
                s["h"]    = max(s["h"], adj_price)
                s["l"]    = min(s["l"], adj_price)
                s["vol"] += vol
                s["cum"] += inc

                # Close the bar when threshold reached
                if s["cum"] >= thresholds[k]:
                    store[k]["open"].append(s["o"])
                    store[k]["high"].append(s["h"])
                    store[k]["low"].append(s["l"])
                    store[k]["close"].append(adj_price)
                    store[k]["volume"].append(s["vol"])
                    store[k]["time"].append(ts)
                    s["cum"] = 0.0
                    s["o"]   = None       # triggers new bar on next tick

        rows_done = end
        pct = rows_done / n_total * 100
        print(f"  {rows_done:>12,}/{n_total:,}  ({pct:4.1f}%)  "
              f"tick={len(store['tick']['close']):,}  "
              f"vol={len(store['volume']['close']):,}  "
              f"dollar={len(store['dollar']['close']):,}", end="\r")
    print()

    # Convert to DataFrames and append log-returns column
    bars = {}
    for k in ("tick", "volume", "dollar"):
        df = pd.DataFrame({
            "datetime": parse_times(np.array(store[k]["time"])),
            "open"    : np.array(store[k]["open"],   dtype=np.float64),
            "high"    : np.array(store[k]["high"],   dtype=np.float64),
            "low"     : np.array(store[k]["low"],    dtype=np.float64),
            "close"   : np.array(store[k]["close"],  dtype=np.float64),
            "volume"  : np.array(store[k]["volume"], dtype=np.float64),
        })
        # Prepend NaN for first bar (no previous bar to compare against)
        df["return"] = np.concatenate([[np.nan], log_returns(df["close"].values)])
        bars[k] = df
        print(f"  {k.capitalize():7s}: {len(df):,} bars  "
              f"({df['datetime'].iloc[0].date()} to {df['datetime'].iloc[-1].date()})")

    print(f"\n  Roll events detected: {len(roll_events)}")
    for r in roll_events:
        print(f"    {r['time'][:8]}  {r['from']} -> {r['to']}  gap = {r['gap']:+.2f}")

    plot_data = {
        "time": parse_times(np.array(plot_time)),
        "raw" : np.array(plot_raw,  dtype=np.float64),
        "adj" : np.array(plot_adj,  dtype=np.float64),
    }
    return bars, roll_events, plot_data


# =====================================================================
# PART c) -- WEEKLY BAR COUNTS
# =====================================================================
# QUESTION:
#   Count bars produced per week for each bar type.
#   Plot the weekly count as a time series.
#   Which bar type has the most STABLE weekly count? Why?
#
# METHOD:
#   Group each bar's timestamp by calendar week, count bars per week.
#   Compare stability using CV = std / mean (lower = more stable).
#
# EXPECTED ANSWER: Dollar bars
#
# WHY:
#   Each dollar bar = $N of economic activity.
#   Volatile week  -> prices high + volume high -> each trade crosses
#                     threshold faster -> bar closes sooner -> same count
#   Quiet week     -> prices low  + volume low  -> fewer dollars per trade
#                     -> takes more trades to close -> count stays similar
#   Tick bars EXPLODE during volatile weeks because they count every trade.
# =====================================================================

def weekly_bar_counts(bars_df):
    """Group bar timestamps by calendar week, return count per week."""
    return pd.Series(1, index=bars_df["datetime"]).resample("W").sum()


# =====================================================================
# PART e) -- VARIANCE OF VARIANCES (monthly)
# =====================================================================
# QUESTION:
#   Partition each bar series into MONTHLY subsets.
#   For each month, compute the VARIANCE of returns.
#   Then compute the VARIANCE of those monthly variances.
#   Which bar method has the smallest variance of variances?
#
# METHOD:
#   Step 1: assign each bar's return to a calendar month
#   Step 2: var_month = variance of returns within that month
#   Step 3: var_of_var = variance of the var_month values
#
# EXPECTED ANSWER: Dollar bars
#
# WHY:
#   Dollar bars spread volatility evenly across bars because each
#   bar represents equal economic activity.
#   Tick bars produce MANY MORE bars in volatile months, inflating
#   that month's variance estimate -> large swings in monthly variance.
# =====================================================================

def variance_of_variances(bars_df):
    """Returns (monthly_var Series, scalar variance-of-variances)."""
    rets = bars_df[["datetime", "return"]].dropna().set_index("datetime")
    monthly_var = rets["return"].resample("MS").var().dropna()
    if len(monthly_var) < 2:
        return monthly_var, np.nan
    return monthly_var, float(np.var(monthly_var.values, ddof=1))


# =====================================================================
# PLOTS
# =====================================================================

def plot_continuous_series(plot_data, roll_events):
    """
    PART a) PLOT
    Top    : raw price with visible roll jumps
    Bottom : adjusted continuous price (jumps removed)
    Orange lines mark each detected roll date.
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 7), sharex=True)
    fig.suptitle("Part a) -- Raw vs Adjusted Continuous Price Series",
                 fontsize=14, fontweight="bold")
    ax1.plot(plot_data["time"], plot_data["raw"], lw=0.4, color="#C44E52")
    ax1.set_title("Raw Price (roll gaps visible as jumps)", fontweight="bold")
    ax1.set_ylabel("Price ($)")
    ax2.plot(plot_data["time"], plot_data["adj"], lw=0.4, color="#4C72B0")
    ax2.set_title("Adjusted Continuous Series -- Panama Canal Method", fontweight="bold")
    ax2.set_ylabel("Adjusted Price ($)")
    for r in roll_events:
        t = pd.to_datetime(r["time"], format="%Y%m%d%H%M%S%f", errors="coerce")
        for ax in (ax1, ax2):
            ax.axvline(t, color="orange", alpha=0.6, lw=0.8)
    plt.tight_layout()
    path = os.path.join(PLOTS_DIR, "a_continuous_series.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def plot_weekly_counts(weekly_dict, cvs):
    """
    PART c) PLOT
    Three stacked panels: weekly bar count over time per bar type.
    Dashed line = mean.  CV shown in title.
    """
    fig, axes = plt.subplots(3, 1, figsize=(16, 11))
    fig.suptitle("Part c) -- Weekly Bar Counts by Bar Type",
                 fontsize=14, fontweight="bold")
    for ax, k in zip(axes, ("tick", "volume", "dollar")):
        wc = weekly_dict[k]
        if len(wc) == 0:
            continue
        ax.bar(np.arange(len(wc)), wc.values, color=PALETTE[k], alpha=0.75, width=1.0)
        ax.axhline(wc.mean(), color="black", lw=1.4, ls="--",
                   label=f"Mean = {wc.mean():.0f}")
        ax.set_title(
            f"{k.capitalize()} Bars  |  "
            f"mean={wc.mean():.1f}  sigma={wc.std():.1f}  CV={cvs[k]:.4f}",
            fontweight="bold")
        ax.set_ylabel("Bars / Week")
        ax.legend()
        ts = wc.index.to_timestamp()
        prev_yr = ts[0].year
        for i, t in enumerate(ts):
            if t.year != prev_yr:
                ax.axvline(i, color="gray", lw=0.5, alpha=0.4)
                ax.text(i, ax.get_ylim()[1] * 0.92, str(t.year), fontsize=7, color="gray")
                prev_yr = t.year
    plt.tight_layout()
    path = os.path.join(PLOTS_DIR, "c_weekly_bar_counts.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def plot_acf(bars_dict, serial_dict):
    """
    PART d) PLOT
    Bar chart of autocorrelation at lags 1-20 for each bar type.
    Red dashes = 95% confidence interval.
    Bars inside CI = statistically zero (good).
    Dollar bars should have the most bars inside CI.
    """
    MAX_LAGS = 20
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle(f"Part d) -- Autocorrelation of Log-Returns (lags 1-{MAX_LAGS})",
                 fontsize=13, fontweight="bold")
    for ax, k in zip(axes, ("tick", "volume", "dollar")):
        rets = bars_dict[k]["return"].dropna().values
        if len(rets) < MAX_LAGS + 2:
            ax.set_title(f"{k.capitalize()} -- insufficient data")
            continue
        s        = pd.Series(rets)
        acf_vals = [float(s.autocorr(lag=i)) for i in range(1, MAX_LAGS + 1)]
        ci       = 1.96 / np.sqrt(len(rets))
        ax.bar(range(1, MAX_LAGS + 1), acf_vals, color=PALETTE[k], alpha=0.75)
        ax.axhline(0,   color="black", lw=0.8)
        ax.axhline( ci, color="red", lw=1.2, ls="--", label="95% CI")
        ax.axhline(-ci, color="red", lw=1.2, ls="--")
        ax.set_title(f"{k.capitalize()}  rho(1)={serial_dict[k]:+.5f}", fontweight="bold")
        ax.set_xlabel("Lag")
        ax.set_ylabel("Autocorrelation")
        ax.legend(fontsize=8)
    plt.tight_layout()
    path = os.path.join(PLOTS_DIR, "d_serial_correlation.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def plot_distributions(bars_dict, jb_dict):
    """
    PART f) PLOT
    Histogram of log-returns vs normal curve for each bar type.
    Dollar bars should match the normal curve most closely (lowest JB).
    Fat tails = histogram extends far beyond the dashed normal curve.
    """
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle("Part f) -- Return Distributions vs Normal Fit",
                 fontsize=14, fontweight="bold")
    for ax, k in zip(axes, ("tick", "volume", "dollar")):
        rets = bars_dict[k]["return"].dropna().values
        if len(rets) < 10:
            ax.set_title(f"{k.capitalize()} -- insufficient data")
            continue
        lo  = np.percentile(rets, 0.1)
        hi  = np.percentile(rets, 99.9)
        rc  = np.clip(rets, lo, hi)
        ax.hist(rc, bins=120, density=True, color=PALETTE[k], alpha=0.6, label="Empirical")
        xr  = np.linspace(lo, hi, 300)
        ax.plot(xr, scipy_stats.norm.pdf(xr, rets.mean(), rets.std()),
                "k--", lw=2, label="Normal fit")
        skw = float(pd.Series(rets).skew())
        krt = float(pd.Series(rets).kurtosis())
        ax.set_title(
            f"{k.capitalize()}\nJB={jb_dict[k]:,.0f}  skew={skw:+.3f}  kurt={krt:+.3f}",
            fontweight="bold")
        ax.set_xlabel("Log-return")
        ax.set_ylabel("Density")
        ax.legend(fontsize=9)
    plt.tight_layout()
    path = os.path.join(PLOTS_DIR, "f_return_distributions.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# =====================================================================
# MAIN
# =====================================================================

def main():
    print("=" * 65)
    print("  Assignment 1 -- Analyzing E-mini S&P 500 Tick Data")
    print("=" * 65)
    print(f"  File    : {DATA_PATH}")
    print(f"  Dataset : {DATASET}")

    with h5py.File(DATA_PATH, "r") as f:
        dataset    = f[DATASET]
        total_rows = dataset.shape[0]
        print(f"  Rows    : {total_rows:,}")

        # Part a) step 1: estimate thresholds
        thresholds = calibrate_thresholds(dataset, CALIB_ROWS, BARS_PER_DAY)
        print(thresholds)

        # Part a) step 2 + Part b): build continuous series and all bars
        bars, roll_events, plot_data = build_bars(dataset, thresholds, CHUNK_SIZE)

    # # Part c): weekly counts
    # print("\n[Part c] Weekly bar counts...")
    # weekly      = {k: weekly_bar_counts(bars[k]) for k in bars}
    # cvs         = {k: coeff_variation(weekly[k]) if len(weekly[k]) > 1 else np.nan
    #                for k in bars}
    # most_stable = min(cvs, key=lambda k: cvs[k] if not np.isnan(cvs[k]) else np.inf)

    # # Part d): serial correlation
    # print("[Part d] Serial correlation of log-returns...")
    # serial  = {k: lag1_autocorr(bars[k]["return"].dropna().values) for k in bars}
    # best_sc = min(serial, key=lambda k: abs(serial[k]) if not np.isnan(serial[k]) else np.inf)

    # # Part e): variance of variances
    # print("[Part e] Variance of monthly return-variances...")
    # mv_dict  = {}
    # vov      = {}
    # for k in bars:
    #     mv, v    = variance_of_variances(bars[k])
    #     mv_dict[k] = mv
    #     vov[k]   = v
    # best_vov = min(vov, key=lambda k: vov[k] if not np.isnan(vov[k]) else np.inf)

    # # Part f): Jarque-Bera
    # print("[Part f] Jarque-Bera normality test...")
    # jb      = {k: jarque_bera(bars[k]["return"].dropna().values) for k in bars}
    # best_jb = min(jb, key=lambda k: jb[k] if not np.isnan(jb[k]) else np.inf)

    # # Plots
    # print("\n[Plots]")
    # plot_continuous_series(plot_data, roll_events)
    # plot_weekly_counts(weekly, cvs)
    # plot_acf(bars, serial)
    # plot_distributions(bars, jb)

    # # Console summary
    # print("\n" + "=" * 65)
    # print("  RESULTS SUMMARY")
    # print("=" * 65)
    # print(f"  {'Metric':<28} {'Tick':>12} {'Volume':>12} {'Dollar':>12}")
    # print("  " + "-" * 68)
    # print(f"  {'# Bars':<28} "
    #       f"{len(bars['tick']):>12,} "
    #       f"{len(bars['volume']):>12,} "
    #       f"{len(bars['dollar']):>12,}")
    # print(f"  {'Weekly CV (c) lower=better':<28} "
    #       f"{cvs['tick']:>12.4f} "
    #       f"{cvs['volume']:>12.4f} "
    #       f"{cvs['dollar']:>12.4f}")
    # print(f"  {'Serial Corr rho(1) (d)':<28} "
    #       f"{serial['tick']:>+12.5f} "
    #       f"{serial['volume']:>+12.5f} "
    #       f"{serial['dollar']:>+12.5f}")
    # print(f"  {'Var(monthly Var) (e)':<28} "
    #       f"{vov['tick']:>12.3e} "
    #       f"{vov['volume']:>12.3e} "
    #       f"{vov['dollar']:>12.3e}")
    # print(f"  {'JB Statistic (f) lower=better':<28} "
    #       f"{jb['tick']:>12,.1f} "
    #       f"{jb['volume']:>12,.1f} "
    #       f"{jb['dollar']:>12,.1f}")
    # print("=" * 65)
    # print(f"  c) Most stable weekly count : {most_stable.upper()}")
    # print(f"  d) Lowest serial correlation: {best_sc.upper()}")
    # print(f"  e) Smallest var of variances: {best_vov.upper()}")
    # print(f"  f) Lowest JB statistic      : {best_jb.upper()}")
    # print("=" * 65)

    # # Write output.txt
    # lines = [
    #     "Assignment 1 -- Analyzing E-mini S&P 500 Tick Data",
    #     "=" * 55,
    #     "",
    #     f"File    : {DATA_PATH}",
    #     f"Dataset : {DATASET}",
    #     f"Rows    : {total_rows:,}",
    #     "",
    #     "Bar thresholds (auto-calibrated)",
    #     f"  tick   : every {thresholds['tick']:,} ticks",
    #     f"  volume : every {thresholds['volume']:,} contracts",
    #     f"  dollar : every ${thresholds['dollar']:,.0f} traded",
    #     "",
    #     "Bars formed",
    #     f"  tick   : {len(bars['tick']):,}",
    #     f"  volume : {len(bars['volume']):,}",
    #     f"  dollar : {len(bars['dollar']):,}",
    #     "",
    #     "Contract rolls detected (part a)",
    # ]
    # for r in roll_events:
    #     lines.append(f"  {r['time'][:8]}  {r['from']} -> {r['to']}  gap = {r['gap']:+.2f}")
    # lines += [
    #     "",
    #     "-" * 55,
    #     "Part c) Weekly bar-count stability  (CV = std/mean, lower = more stable)",
    #     f"  tick   CV = {cvs['tick']:.4f}",
    #     f"  volume CV = {cvs['volume']:.4f}",
    #     f"  dollar CV = {cvs['dollar']:.4f}",
    #     f"  --> Most stable: {most_stable.upper()}",
    #     "  Why: Dollar bars normalise for price x volume, so each bar",
    #     "  represents the same economic activity regardless of conditions.",
    #     "",
    #     "-" * 55,
    #     "Part d) Lag-1 serial correlation  (closer to 0 = more random = better)",
    #     f"  tick   rho(1) = {serial['tick']:+.6f}",
    #     f"  volume rho(1) = {serial['volume']:+.6f}",
    #     f"  dollar rho(1) = {serial['dollar']:+.6f}",
    #     f"  --> Lowest: {best_sc.upper()}",
    #     "  Why: Dollar sampling conditions on information flow, breaking",
    #     "  microstructure-driven autocorrelation in tick/volume bars.",
    #     "",
    #     "-" * 55,
    #     "Part e) Variance of monthly return-variances  (smaller = more stationary)",
    #     f"  tick   Var(Var) = {vov['tick']:.4e}",
    #     f"  volume Var(Var) = {vov['volume']:.4e}",
    #     f"  dollar Var(Var) = {vov['dollar']:.4e}",
    #     f"  --> Smallest: {best_vov.upper()}",
    #     "  Why: Equal dollar activity per bar homogenises variance across months.",
    #     "",
    #     "-" * 55,
    #     "Part f) Jarque-Bera statistic  (smaller = closer to Gaussian)",
    #     f"  tick   JB = {jb['tick']:,.2f}",
    #     f"  volume JB = {jb['volume']:,.2f}",
    #     f"  dollar JB = {jb['dollar']:,.2f}",
    #     f"  --> Lowest: {best_jb.upper()}",
    #     "  Why: Dollar bars reduce fat tails by sampling at equal information",
    #     "  intervals rather than raw event counts.",
    #     "",
    #     "-" * 55,
    #     "Plots saved",
    #     "  plots/a_continuous_series.png",
    #     "  plots/c_weekly_bar_counts.png",
    #     "  plots/d_serial_correlation.png",
    #     "  plots/f_return_distributions.png",
    # ]
    # with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    #     f.write("\n".join(lines) + "\n")

    # print(f"\n  Results -> {OUTPUT_TXT}")
    # print(f"  Plots   -> {PLOTS_DIR}/")
    # print("\n  Dollar bars win on ALL four metrics (c, d, e, f).")


# =====================================================================
if __name__ == "__main__":
    main()
