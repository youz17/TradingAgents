"""AKShare-based company fundamentals, balance sheet, cashflow, and income statement."""

from datetime import datetime
from typing import Annotated

import pandas as pd

from .akshare_common import AkShareError, detect_market, normalize_symbol_cn


def _filter_by_date(df: pd.DataFrame, date_col: str, curr_date: str | None) -> pd.DataFrame:
    """Drop rows whose report date is after *curr_date* to prevent look-ahead bias."""
    if not curr_date or df.empty or date_col not in df.columns:
        return df
    cutoff = pd.Timestamp(curr_date)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df[df[date_col] <= cutoff]


def get_fundamentals(
    ticker: Annotated[str, "ticker symbol of the company"],
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None,
) -> str:
    """Return key financial-analysis indicators for an A-share stock.

    For non-CN symbols a friendly message is returned because AKShare
    fundamentals are currently limited to the Chinese market.
    """
    import akshare as ak

    market = detect_market(ticker)
    if market != "cn":
        return (
            f"AKShare fundamentals are only available for Chinese A-share stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker. "
            f"Please use yfinance or alpha_vantage for this symbol."
        )

    symbol = normalize_symbol_cn(ticker)

    try:
        df = ak.stock_financial_analysis_indicator(symbol=symbol, start_year="2020")
    except Exception as e:
        raise AkShareError(f"Failed to fetch fundamentals for {ticker}: {e}") from e

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


def get_balance_sheet(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None,
) -> str:
    """Retrieve balance-sheet data via AKShare (East Money)."""
    import akshare as ak

    market = detect_market(ticker)
    if market != "cn":
        return (
            f"AKShare balance sheet is only available for Chinese A-share stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker."
        )

    symbol = normalize_symbol_cn(ticker)

    try:
        df = ak.stock_balance_sheet_by_report_em(symbol=symbol)
    except Exception as e:
        raise AkShareError(f"Failed to fetch balance sheet for {ticker}: {e}") from e

    if df is None or df.empty:
        return f"No balance sheet data found for symbol '{ticker}'"

    date_col = "REPORT_DATE" if "REPORT_DATE" in df.columns else df.columns[0]
    df = _filter_by_date(df, date_col, curr_date)

    if df.empty:
        return f"No balance sheet data found for symbol '{ticker}' before {curr_date}"

    csv_string = df.head(20).to_csv(index=False)

    header = f"# Balance Sheet for {symbol} ({freq})\n"
    header += f"# Data source: AKShare (East Money)\n"
    header += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + csv_string


def get_cashflow(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None,
) -> str:
    """Retrieve cash-flow statement via AKShare (East Money)."""
    import akshare as ak

    market = detect_market(ticker)
    if market != "cn":
        return (
            f"AKShare cash flow is only available for Chinese A-share stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker."
        )

    symbol = normalize_symbol_cn(ticker)

    try:
        df = ak.stock_cash_flow_sheet_by_report_em(symbol=symbol)
    except Exception as e:
        raise AkShareError(f"Failed to fetch cash flow for {ticker}: {e}") from e

    if df is None or df.empty:
        return f"No cash flow data found for symbol '{ticker}'"

    date_col = "REPORT_DATE" if "REPORT_DATE" in df.columns else df.columns[0]
    df = _filter_by_date(df, date_col, curr_date)

    if df.empty:
        return f"No cash flow data found for symbol '{ticker}' before {curr_date}"

    csv_string = df.head(20).to_csv(index=False)

    header = f"# Cash Flow Statement for {symbol} ({freq})\n"
    header += f"# Data source: AKShare (East Money)\n"
    header += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + csv_string


def get_income_statement(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None,
) -> str:
    """Retrieve income statement via AKShare (East Money)."""
    import akshare as ak

    market = detect_market(ticker)
    if market != "cn":
        return (
            f"AKShare income statement is only available for Chinese A-share stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker."
        )

    symbol = normalize_symbol_cn(ticker)

    try:
        df = ak.stock_profit_sheet_by_report_em(symbol=symbol)
    except Exception as e:
        raise AkShareError(f"Failed to fetch income statement for {ticker}: {e}") from e

    if df is None or df.empty:
        return f"No income statement data found for symbol '{ticker}'"

    date_col = "REPORT_DATE" if "REPORT_DATE" in df.columns else df.columns[0]
    df = _filter_by_date(df, date_col, curr_date)

    if df.empty:
        return f"No income statement data found for symbol '{ticker}' before {curr_date}"

    csv_string = df.head(20).to_csv(index=False)

    header = f"# Income Statement for {symbol} ({freq})\n"
    header += f"# Data source: AKShare (East Money)\n"
    header += f"# Retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return header + csv_string
