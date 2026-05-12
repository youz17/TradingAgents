"""AKShare-based OHLCV stock data fetching."""

import logging
import time
from datetime import datetime
from typing import Annotated

import akshare as ak
import pandas as pd

from .akshare_common import (
    AkShareError,
    akshare_retry,
    detect_market,
    normalize_symbol_us,
    normalize_symbol_hk,
    akshare_date,
    _fetch_cn_ohlcv,
    _fetch_hk_ohlcv,
    _normalise_cn_columns,
)

logger = logging.getLogger(__name__)


def get_stock(
    symbol: Annotated[str, "ticker symbol of the company"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
) -> str:
    """Fetch daily OHLCV data via AKShare and return a CSV string.

    Automatically routes to the correct AKShare API based on the symbol
    format (A-share / US / HK).
    """
    datetime.strptime(start_date, "%Y-%m-%d")
    datetime.strptime(end_date, "%Y-%m-%d")

    market = detect_market(symbol)
    logger.info("[AKSHARE] get_stock(%s, %s, %s) market=%s", symbol, start_date, end_date, market)
    t0 = time.perf_counter()

    try:
        if market == "cn":
            data = _fetch_cn_ohlcv(symbol, start_date, end_date)
        elif market == "hk":
            data = _fetch_hk_ohlcv(symbol, start_date, end_date)
        else:
            raw = akshare_retry(
                ak.stock_us_hist,
                symbol=normalize_symbol_us(symbol),
                period="daily",
                start_date=akshare_date(start_date),
                end_date=akshare_date(end_date),
                adjust="qfq",
            )
            data = _normalise_cn_columns(raw)
    except Exception as e:
        logger.error("[AKSHARE] get_stock FAILED for %s in %.2fs: %s", symbol, time.perf_counter() - t0, e)
        raise AkShareError(f"AKShare data fetch failed for {symbol}: {e}") from e

    if data.empty:
        return f"No data found for symbol '{symbol}' between {start_date} and {end_date}"

    numeric_columns = ["Open", "High", "Low", "Close"]
    for col in numeric_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce").round(2)

    csv_string = data.to_csv(index=False)

    logger.info("[AKSHARE] get_stock OK: %s, %d rows in %.2fs", symbol, len(data), time.perf_counter() - t0)

    header = f"# Stock data for {symbol.upper()} from {start_date} to {end_date}\n"
    header += f"# Total records: {len(data)}\n"
    header += f"# Data source: AKShare\n"
    header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + csv_string
