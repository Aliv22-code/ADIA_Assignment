# Assignment 1 - Analyzing Tick Data

## Objective

Analyze E-mini S&P 500 futures tick data from `ES.h5` and complete:

- (a) Build a continuous futures price series by adjusting contract rolls.
- (b) Create tick, volume, and dollar bars.
- (c) Count weekly bars for each type, plot the time series, and identify the most stable type.
- (d) Compute lag-1 serial correlation of returns for each bar type.
- (e) Compute monthly return variances and then variance of those variances.
- (f) Compute Jarque-Bera normality statistic on returns.

## Input Data

- File: `../ES.h5`
- Dataset key: `tick/trades_filter0vol`
- Fields:
  - `Instrument` (contract code, used for roll detection)
  - `Price` (trade price)
  - `Time` (`YYYYMMDDHHMMSSmmm`)
  - `Volume` (trade size)

## Output Files

- `output.txt` - full numeric summary and conclusions for parts (c) to (f)
- `plots/a_continuous_series.png` - raw vs roll-adjusted series
- `plots/c_weekly_bar_counts.png` - weekly bar counts by bar type
- `plots/d_serial_correlation.png` - autocorrelation comparison
- `plots/f_return_distributions.png` - return distribution and normality view

## Run

From repository root:

```bash
cd /Users/alivghosh/Documents/ADIA_Assignment
source .venv/bin/activate
python "assignment 1/solution.py"
```

If needed, force the H5 location explicitly:

```bash
cd "/Users/alivghosh/Documents/ADIA_Assignment/assignment 1"
DATA_PATH="/Users/alivghosh/Documents/ADIA_Assignment/ES.h5" ../.venv/bin/python solution.py
```

## Optional Parameters

You can tune runtime/memory behavior via environment variables:

- `DATA_PATH` - path to `ES.h5`
- `DATASET` - dataset key inside H5 (default `tick/trades_filter0vol`)
- `CHUNK_SIZE` - rows per processing chunk
- `CALIB_ROWS` - rows used for threshold calibration
- `BARS_PER_DAY` - target bars/day for threshold estimation

Example:

```bash
DATA_PATH="/Users/alivghosh/Documents/ADIA_Assignment/ES.h5" CHUNK_SIZE=500000 CALIB_ROWS=1000000 BARS_PER_DAY=50 ../.venv/bin/python solution.py
```

## Notes

- The original `ES.h5` is very large and should not be included in submission zip.
- For the assignment prompt, only the weekly-count plot in part (c) is mandatory; extra plots are included for diagnostics and interpretation.
