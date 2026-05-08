import re
import logging
import os
import pandas as pd

from .config import get_config
from .utils import safe_ticker_component

logger = logging.getLogger(__name__)

# Market code prefixes used by East Money (AKShare's upstream source for US stocks)
_US_MARKET_PREFIXES = {
    "NASDAQ": "105",
    "NYSE": "106",
    "AMEX": "105",
}

# Well-known NASDAQ-listed tickers (non-exhaustive, used as a heuristic)
_KNOWN_NYSE = {
    "BRK.A", "BRK.B", "JPM", "JNJ", "V", "WMT", "PG", "UNH", "HD", "BAC",
    "MA", "DIS", "KO", "PFE", "VZ", "MRK", "T", "ABBV", "ABT", "CVX",
    "XOM", "CMCSA", "NKE", "LLY", "TMO", "MCD", "DHR", "NEE", "PM", "BMY",
    "RTX", "HON", "UPS", "IBM", "CAT", "GE", "BA", "MMM", "GS", "AXP",
    "LOW", "BLK", "CI", "C", "GM", "F", "WFC",
}

_A_SHARE_RE = re.compile(r"^\d{6}$")


class AkShareError(Exception):
    """Raised on transient AKShare failures (network, rate-limit, etc.)."""
    pass


def detect_market(symbol: str) -> str:
    """Return ``'cn'``, ``'us'``, or ``'hk'`` based on the symbol pattern."""
    s = symbol.strip()
    if _A_SHARE_RE.match(s):
        return "cn"
    if s.upper().endswith(".HK") or s.upper().startswith("HK."):
        return "hk"
    return "us"


def normalize_symbol_cn(symbol: str) -> str:
    """Ensure an A-share code is a 6-digit string."""
    return symbol.strip().zfill(6)


def normalize_symbol_us(symbol: str) -> str:
    """Convert a plain US ticker like ``AAPL`` to the AKShare format ``105.AAPL``.

    If the symbol already contains a dot-prefix (e.g. ``105.AAPL``), return as-is.
    """
    s = symbol.strip().upper()
    if re.match(r"^\d+\.", s):
        return s
    prefix = "106" if s in _KNOWN_NYSE else "105"
    return f"{prefix}.{s}"


def normalize_symbol_hk(symbol: str) -> str:
    """Normalise HK ticker to a 5-digit code string (e.g. ``00700``)."""
    s = symbol.strip().upper()
    s = s.replace("HK.", "").replace(".HK", "")
    return s.zfill(5)


def akshare_date(date_str: str) -> str:
    """Convert ``YYYY-MM-DD`` to ``YYYYMMDD`` as expected by many AKShare APIs."""
    return date_str.replace("-", "")


def _cache_path(symbol: str, start: str, end: str) -> str:
    safe_symbol = safe_ticker_component(symbol)
    config = get_config()
    os.makedirs(config["data_cache_dir"], exist_ok=True)
    return os.path.join(
        config["data_cache_dir"],
        f"{safe_symbol}-AKShare-data-{start}-{end}.csv",
    )


def load_ohlcv_akshare(symbol: str, curr_date: str) -> pd.DataFrame:
    """Download OHLCV from AKShare with local CSV cache (same semantics as the
    yfinance ``load_ohlcv``).  Returns a DataFrame with columns
    ``Date, Open, High, Low, Close, Volume``.
    """
    import akshare as ak

    safe_symbol = safe_ticker_component(symbol)
    config = get_config()
    curr_date_dt = pd.to_datetime(curr_date)

    today = pd.Timestamp.today()
    start_date = today - pd.DateOffset(years=5)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = today.strftime("%Y-%m-%d")

    os.makedirs(config["data_cache_dir"], exist_ok=True)
    data_file = os.path.join(
        config["data_cache_dir"],
        f"{safe_symbol}-AKShare-data-{start_str}-{end_str}.csv",
    )

    if os.path.exists(data_file):
        data = pd.read_csv(data_file, on_bad_lines="skip", encoding="utf-8")
    else:
        market = detect_market(symbol)
        try:
            if market == "cn":
                raw = ak.stock_zh_a_hist(
                    symbol=normalize_symbol_cn(symbol),
                    period="daily",
                    start_date=akshare_date(start_str),
                    end_date=akshare_date(end_str),
                    adjust="qfq",
                )
                data = _normalise_cn_columns(raw)
            elif market == "hk":
                raw = ak.stock_hk_hist(
                    symbol=normalize_symbol_hk(symbol),
                    period="daily",
                    start_date=akshare_date(start_str),
                    end_date=akshare_date(end_str),
                    adjust="qfq",
                )
                data = _normalise_cn_columns(raw)
            else:
                raw = ak.stock_us_hist(
                    symbol=normalize_symbol_us(symbol),
                    period="daily",
                    start_date=akshare_date(start_str),
                    end_date=akshare_date(end_str),
                    adjust="qfq",
                )
                data = _normalise_cn_columns(raw)
        except Exception as e:
            raise AkShareError(f"AKShare data fetch failed for {symbol}: {e}") from e

        data.to_csv(data_file, index=False, encoding="utf-8")

    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")
    data = data.dropna(subset=["Date"])
    price_cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in data.columns]
    data[price_cols] = data[price_cols].apply(pd.to_numeric, errors="coerce")
    data = data.dropna(subset=["Close"])
    data[price_cols] = data[price_cols].ffill().bfill()

    data = data[data["Date"] <= curr_date_dt]
    return data


# Column mapping: AKShare Chinese headers -> English
_CN_COL_MAP = {
    "日期": "Date",
    "开盘": "Open",
    "收盘": "Close",
    "最高": "High",
    "最低": "Low",
    "成交量": "Volume",
    "成交额": "Amount",
    "振幅": "Amplitude",
    "涨跌幅": "Change_Pct",
    "涨跌额": "Change_Amt",
    "换手率": "Turnover",
}


def _normalise_cn_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Chinese columns to English and keep a standard subset."""
    df = df.rename(columns=_CN_COL_MAP)
    # If columns are already English (e.g. US stocks), just standardize
    keep = ["Date", "Open", "High", "Low", "Close", "Volume"]
    present = [c for c in keep if c in df.columns]
    return df[present].copy()
