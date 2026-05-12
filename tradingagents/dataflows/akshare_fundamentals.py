"""AKShare-based company fundamentals, balance sheet, cashflow, and income statement.

A-share: Sina Finance (``stock_financial_report_sina``).
HK:      East Money  (``stock_financial_hk_report_em``) + Eniu (valuation).
US:      Not supported — fallback to yfinance / alpha_vantage.
"""

import logging
import time
from datetime import datetime
from typing import Annotated

import pandas as pd

from .akshare_common import (
    AkShareError, akshare_retry, detect_market,
    normalize_symbol_cn, normalize_symbol_hk,
)

logger = logging.getLogger(__name__)

_SINA_REPORT_MAP = {
    "balance_sheet": "资产负债表",
    "income_statement": "利润表",
    "cashflow": "现金流量表",
}

_HK_REPORT_MAP = {
    "balance_sheet": "资产负债表",
    "income_statement": "利润表",
    "cashflow": "现金流量表",
}


def _filter_by_date(df: pd.DataFrame, date_col: str, curr_date: str | None) -> pd.DataFrame:
    """Drop rows whose report date is after *curr_date* to prevent look-ahead bias."""
    if not curr_date or df.empty or date_col not in df.columns:
        return df
    cutoff = pd.Timestamp(curr_date)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", format="mixed")
    return df[df[date_col] <= cutoff]


def _fetch_sina_report(symbol: str, report_type: str) -> pd.DataFrame:
    """Fetch a financial report from Sina via AKShare.

    ``report_type`` is one of the keys in ``_SINA_REPORT_MAP``.
    """
    import akshare as ak

    sina_indicator = _SINA_REPORT_MAP[report_type]
    df = akshare_retry(ak.stock_financial_report_sina, stock=symbol, symbol=sina_indicator)
    return df


def _fetch_hk_report(symbol: str, report_type: str) -> pd.DataFrame:
    """Fetch a HK financial report from East Money via AKShare.

    The EM API returns a tall table (SECUCODE, REPORT_DATE, STD_ITEM_NAME, AMOUNT).
    We pivot it into a wide table with items as columns for readability.
    """
    import akshare as ak

    em_indicator = _HK_REPORT_MAP[report_type]
    code = normalize_symbol_hk(symbol)
    df = akshare_retry(ak.stock_financial_hk_report_em, stock=code, symbol=em_indicator)
    if df is None or df.empty:
        return pd.DataFrame()

    needed = ["REPORT_DATE", "STD_ITEM_NAME", "AMOUNT"]
    if not all(c in df.columns for c in needed):
        return df

    pivot = df.pivot_table(
        index="REPORT_DATE", columns="STD_ITEM_NAME",
        values="AMOUNT", aggfunc="first",
    ).reset_index()
    pivot = pivot.rename(columns={"REPORT_DATE": "报告日"})
    pivot = pivot.sort_values("报告日", ascending=False)
    return pivot


def _get_hk_valuation(symbol: str) -> str:
    """Fetch PE / PB / dividend yield from Eniu for an HK stock."""
    import akshare as ak

    code = normalize_symbol_hk(symbol)
    eniu_sym = f"hk{code}"
    lines = []
    for indicator, label in [("市盈率", "PE"), ("市净率", "PB"), ("股息率", "Dividend Yield (%)")]:
        try:
            df = akshare_retry(ak.stock_hk_indicator_eniu, symbol=eniu_sym, indicator=indicator)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                val_col = [c for c in df.columns if c not in ("date", "price")][0]
                lines.append(f"{label}: {latest[val_col]}  (date: {latest.get('date', 'N/A')})")
        except Exception as e:
            logger.warning("[AKSHARE] HK valuation %s failed for %s: %s", indicator, symbol, e)
    return "\n".join(lines) if lines else ""


def get_fundamentals(
    ticker: Annotated[str, "ticker symbol of the company"],
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None,
) -> str:
    """Return key financial-analysis indicators (A-share or HK)."""
    import akshare as ak

    market = detect_market(ticker)

    if market == "hk":
        return _get_fundamentals_hk(ticker, curr_date)

    if market != "cn":
        return (
            f"AKShare fundamentals are only available for Chinese A-share and HK stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker. "
            f"Please use yfinance or alpha_vantage for this symbol."
        )

    symbol = normalize_symbol_cn(ticker)

    logger.info("[AKSHARE] get_fundamentals(%s) calling stock_financial_analysis_indicator", ticker)
    t0 = time.perf_counter()
    try:
        df = akshare_retry(ak.stock_financial_analysis_indicator, symbol=symbol, start_year="2020")
    except Exception as e:
        logger.error("[AKSHARE] get_fundamentals FAILED for %s in %.2fs: %s", ticker, time.perf_counter() - t0, e)
        raise AkShareError(f"Failed to fetch fundamentals for {ticker}: {e}") from e
    logger.info("[AKSHARE] get_fundamentals OK: %s, %d rows in %.2fs", ticker, len(df) if df is not None else 0, time.perf_counter() - t0)

    if df is None or df.empty:
        return f"No fundamentals data found for symbol '{ticker}'"

    df = _filter_by_date(df, "日期", curr_date)
    if df.empty:
        return f"No fundamentals data found for symbol '{ticker}' before {curr_date}"

    latest = df.iloc[0]

    fields = [
        ("Report Date", latest.get("日期")),
        ("EPS (Diluted)", latest.get("摊薄每股收益(元)")),
        ("Weighted ROE (%)", latest.get("加权净资产收益率(%)")),
        ("Net Profit Margin (%)", latest.get("销售净利率(%)")),
        ("Gross Margin (%)", latest.get("销售毛利率(%)")),
        ("Asset-Liability Ratio (%)", latest.get("资产负债比率(%)")),
        ("Current Ratio", latest.get("流动比率")),
        ("Quick Ratio", latest.get("速动比率")),
        ("Total Asset Turnover", latest.get("总资产周转率(次)")),
        ("Inventory Turnover", latest.get("存货周转率(次)")),
        ("Receivables Turnover", latest.get("应收账款周转率(次)")),
        ("Net Profit YoY Growth (%)", latest.get("净利润增长率(%)")),
        ("Revenue YoY Growth (%)", latest.get("主营业务收入增长率(%)")),
    ]

    lines = [f"{label}: {value}" for label, value in fields if value is not None]

    header = f"# Financial Analysis Indicators for {symbol}\n"
    header += f"# Data source: AKShare (Sina Finance)\n"
    header += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + "\n".join(lines)


def _get_fundamentals_hk(ticker: str, curr_date: str | None) -> str:
    """HK fundamentals: income-statement highlights + Eniu valuation."""
    code = normalize_symbol_hk(ticker)
    logger.info("[AKSHARE] _get_fundamentals_hk(%s)", ticker)
    t0 = time.perf_counter()

    parts = []

    try:
        income = _fetch_hk_report(ticker, "income_statement")
        if not income.empty:
            income = _filter_by_date(income, "报告日", curr_date)
        if not income.empty:
            latest = income.iloc[0]
            date_val = latest.get("报告日", "N/A")
            fields = [
                ("Report Date", date_val),
                ("Revenue (营业额)", latest.get("营业额")),
                ("Gross Profit (毛利)", latest.get("毛利")),
                ("Operating Profit (经营溢利)", latest.get("经营溢利")),
                ("Pre-tax Profit (除税前溢利)", latest.get("除税前溢利")),
                ("Net Profit (本期溢利)", latest.get("本期溢利")),
                ("EPS (每股基本盈利)", latest.get("每股基本盈利")),
            ]
            parts.extend(f"{k}: {v}" for k, v in fields if v is not None)
    except Exception as e:
        logger.warning("[AKSHARE] HK income fetch failed for %s: %s", ticker, e)

    valuation = _get_hk_valuation(ticker)
    if valuation:
        parts.append("")
        parts.append(valuation)

    elapsed = time.perf_counter() - t0
    logger.info("[AKSHARE] _get_fundamentals_hk OK: %s in %.2fs", ticker, elapsed)

    if not parts:
        return f"No fundamentals data found for HK symbol '{ticker}'"

    header = f"# Financial Indicators for HK:{code}\n"
    header += f"# Data source: AKShare (East Money + Eniu)\n"
    header += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    return header + "\n".join(parts)


def _get_hk_financial_report(ticker: str, report_type: str, curr_date: str | None) -> str:
    """Generic HK financial report fetcher (balance_sheet / income_statement / cashflow)."""
    code = normalize_symbol_hk(ticker)
    label = _HK_REPORT_MAP[report_type]
    logger.info("[AKSHARE] HK %s(%s)", report_type, ticker)
    t0 = time.perf_counter()
    try:
        df = _fetch_hk_report(ticker, report_type)
    except Exception as e:
        logger.error("[AKSHARE] HK %s FAILED for %s in %.2fs: %s", report_type, ticker, time.perf_counter() - t0, e)
        raise AkShareError(f"Failed to fetch HK {report_type} for {ticker}: {e}") from e
    logger.info("[AKSHARE] HK %s OK: %s, %d rows in %.2fs", report_type, ticker, len(df) if df is not None else 0, time.perf_counter() - t0)

    if df is None or df.empty:
        return f"No {label} data found for HK symbol '{ticker}'"

    date_col = "报告日" if "报告日" in df.columns else df.columns[0]
    df = _filter_by_date(df, date_col, curr_date)
    if df.empty:
        return f"No {label} data found for HK symbol '{ticker}' before {curr_date}"

    csv_string = df.head(8).to_csv(index=False)
    header = f"# {label} for HK:{code}\n"
    header += f"# Data source: AKShare (East Money)\n"
    header += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    return header + csv_string


def get_balance_sheet(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None,
) -> str:
    """Retrieve balance-sheet data via AKShare (A-share: Sina, HK: East Money)."""
    market = detect_market(ticker)
    if market == "hk":
        return _get_hk_financial_report(ticker, "balance_sheet", curr_date)
    if market != "cn":
        return (
            f"AKShare balance sheet is only available for Chinese A-share and HK stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker."
        )

    symbol = normalize_symbol_cn(ticker)

    logger.info("[AKSHARE] get_balance_sheet(%s) calling stock_financial_report_sina", ticker)
    t0 = time.perf_counter()
    try:
        df = _fetch_sina_report(symbol, "balance_sheet")
    except Exception as e:
        logger.error("[AKSHARE] get_balance_sheet FAILED for %s in %.2fs: %s", ticker, time.perf_counter() - t0, e)
        raise AkShareError(f"Failed to fetch balance sheet for {ticker}: {e}") from e
    logger.info("[AKSHARE] get_balance_sheet OK: %s, %d rows in %.2fs", ticker, len(df) if df is not None else 0, time.perf_counter() - t0)

    if df is None or df.empty:
        return f"No balance sheet data found for symbol '{ticker}'"

    date_col = "报告日" if "报告日" in df.columns else df.columns[0]
    df = _filter_by_date(df, date_col, curr_date)

    if df.empty:
        return f"No balance sheet data found for symbol '{ticker}' before {curr_date}"

    csv_string = df.head(8).to_csv(index=False)

    header = f"# Balance Sheet for {symbol} ({freq})\n"
    header += f"# Data source: AKShare (Sina Finance)\n"
    header += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + csv_string


def get_cashflow(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None,
) -> str:
    """Retrieve cash-flow statement via AKShare (A-share: Sina, HK: East Money)."""
    market = detect_market(ticker)
    if market == "hk":
        return _get_hk_financial_report(ticker, "cashflow", curr_date)
    if market != "cn":
        return (
            f"AKShare cash flow is only available for Chinese A-share and HK stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker."
        )

    symbol = normalize_symbol_cn(ticker)

    logger.info("[AKSHARE] get_cashflow(%s) calling stock_financial_report_sina", ticker)
    t0 = time.perf_counter()
    try:
        df = _fetch_sina_report(symbol, "cashflow")
    except Exception as e:
        logger.error("[AKSHARE] get_cashflow FAILED for %s in %.2fs: %s", ticker, time.perf_counter() - t0, e)
        raise AkShareError(f"Failed to fetch cash flow for {ticker}: {e}") from e
    logger.info("[AKSHARE] get_cashflow OK: %s, %d rows in %.2fs", ticker, len(df) if df is not None else 0, time.perf_counter() - t0)

    if df is None or df.empty:
        return f"No cash flow data found for symbol '{ticker}'"

    date_col = "报告日" if "报告日" in df.columns else df.columns[0]
    df = _filter_by_date(df, date_col, curr_date)

    if df.empty:
        return f"No cash flow data found for symbol '{ticker}' before {curr_date}"

    csv_string = df.head(8).to_csv(index=False)

    header = f"# Cash Flow Statement for {symbol} ({freq})\n"
    header += f"# Data source: AKShare (Sina Finance)\n"
    header += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + csv_string


def get_income_statement(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None,
) -> str:
    """Retrieve income statement via AKShare (A-share: Sina, HK: East Money)."""
    market = detect_market(ticker)
    if market == "hk":
        return _get_hk_financial_report(ticker, "income_statement", curr_date)
    if market != "cn":
        return (
            f"AKShare income statement is only available for Chinese A-share and HK stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker."
        )

    symbol = normalize_symbol_cn(ticker)

    logger.info("[AKSHARE] get_income_statement(%s) calling stock_financial_report_sina", ticker)
    t0 = time.perf_counter()
    try:
        df = _fetch_sina_report(symbol, "income_statement")
    except Exception as e:
        logger.error("[AKSHARE] get_income_statement FAILED for %s in %.2fs: %s", ticker, time.perf_counter() - t0, e)
        raise AkShareError(f"Failed to fetch income statement for {ticker}: {e}") from e
    logger.info("[AKSHARE] get_income_statement OK: %s, %d rows in %.2fs", ticker, len(df) if df is not None else 0, time.perf_counter() - t0)

    if df is None or df.empty:
        return f"No income statement data found for symbol '{ticker}'"

    date_col = "报告日" if "报告日" in df.columns else df.columns[0]
    df = _filter_by_date(df, date_col, curr_date)

    if df.empty:
        return f"No income statement data found for symbol '{ticker}' before {curr_date}"

    csv_string = df.head(8).to_csv(index=False)

    header = f"# Income Statement for {symbol} ({freq})\n"
    header += f"# Data source: AKShare (Sina Finance)\n"
    header += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + csv_string
