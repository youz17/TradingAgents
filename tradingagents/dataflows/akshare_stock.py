"""AKShare-based OHLCV stock data fetching."""

from datetime import datetime
from typing import Annotated

import akshare as ak
import pandas as pd

from .akshare_common import (
    AkShareError,
    detect_market,
    normalize_symbol_cn,
    normalize_symbol_us,
    normalize_symbol_hk,
    akshare_date,
    _normalise_cn_columns,
)


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

    try:
        if market == "cn":
            raw = ak.stock_zh_a_hist(
                symbol=normalize_symbol_cn(symbol),
                period="daily",
                start_date=akshare_date(start_date),
                end_date=akshare_date(end_date),
                adjust="qfq",
            )
        elif market == "hk":
            raw = ak.stock_hk_hist(
                symbol=normalize_symbol_hk(symbol),
                period="daily",
                start_date=akshare_date(start_date),
                end_date=akshare_date(end_date),
                adjust="qfq",
            )
        else:
            raw = ak.stock_us_hist(
                symbol=normalize_symbol_us(symbol),
                period="daily",
                start_date=akshare_date(start_date),
                end_date=akshare_date(end_date),
                adjust="qfq",
            )
    except Exception as e:
        raise AkShareError(f"AKShare data fetch failed for {symbol}: {e}") from e

    data = _normalise_cn_columns(raw)

    if data.empty:
        return f"No data found for symbol '{symbol}' between {start_date} and {end_date}"

    numeric_columns = ["Open", "High", "Low", "Close"]
    for col in numeric_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce").round(2)

    csv_string = data.to_csv(index=False)

    header = f"# Stock data for {symbol.upper()} from {start_date} to {end_date}\n"
    header += f"# Total records: {len(data)}\n"
    header += f"# Data source: AKShare\n"
    header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + csv_string
