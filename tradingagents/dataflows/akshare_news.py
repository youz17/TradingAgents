"""AKShare-based news data fetching (East Money stock news + CCTV macro news)."""

from datetime import datetime
from typing import Annotated

import pandas as pd
from dateutil.relativedelta import relativedelta

from .akshare_common import AkShareError, detect_market, normalize_symbol_cn


def get_news(
    ticker: Annotated[str, "ticker symbol"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
) -> str:
    """Fetch stock-specific news from East Money via AKShare.

    For non-CN symbols a fallback message is returned.
    """
    import akshare as ak

    market = detect_market(ticker)
    if market != "cn":
        return (
            f"AKShare stock news is primarily available for Chinese A-share stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker. "
            f"Consider using yfinance or alpha_vantage for news on this symbol."
        )

    symbol = normalize_symbol_cn(ticker)
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    try:
        df = ak.stock_news_em(symbol=symbol)
    except Exception as e:
        raise AkShareError(f"Failed to fetch news for {ticker}: {e}") from e

    if df is None or df.empty:
        return f"No news found for {ticker}"

    # East Money news columns: 关键词, 新闻标题, 新闻内容, 发布时间, 文章来源, 新闻链接
    date_col = "发布时间" if "发布时间" in df.columns else None
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])
        df = df[(df[date_col] >= start_dt) & (df[date_col] <= end_dt + relativedelta(days=1))]

    if df.empty:
        return f"No news found for {ticker} between {start_date} and {end_date}"

    news_str = ""
    for _, row in df.head(20).iterrows():
        title = row.get("新闻标题", row.get("关键词", "No title"))
        source = row.get("文章来源", "Unknown")
        content = row.get("新闻内容", "")
        link = row.get("新闻链接", "")

        news_str += f"### {title} (source: {source})\n"
        if content:
            news_str += f"{str(content)[:300]}\n"
        if link:
            news_str += f"Link: {link}\n"
        news_str += "\n"

    return f"## {ticker} News, from {start_date} to {end_date}:\n\n{news_str}"


def get_global_news(
    curr_date: Annotated[str, "current date in yyyy-mm-dd format"],
    look_back_days: Annotated[int, "number of days to look back"] = 7,
    limit: Annotated[int, "max articles to return"] = 10,
) -> str:
    """Fetch macro/global news from CCTV via AKShare."""
    import akshare as ak

    curr_dt = datetime.strptime(curr_date, "%Y-%m-%d")
    start_dt = curr_dt - relativedelta(days=look_back_days)

    all_news: list[dict] = []

    current = curr_dt
    while current >= start_dt and len(all_news) < limit:
        date_str = current.strftime("%Y%m%d")
        try:
            df = ak.news_cctv(date=date_str)
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    all_news.append({
                        "date": current.strftime("%Y-%m-%d"),
                        "title": row.get("title", ""),
                        "content": row.get("content", ""),
                    })
                    if len(all_news) >= limit:
                        break
        except Exception:
            pass
        current -= relativedelta(days=1)

    if not all_news:
        return f"No global news found for {curr_date}"

    news_str = ""
    for item in all_news:
        news_str += f"### {item['title']} ({item['date']})\n"
        if item["content"]:
            news_str += f"{str(item['content'])[:300]}\n"
        news_str += "\n"

    start_date = start_dt.strftime("%Y-%m-%d")
    return f"## Global / Macro News (CCTV), from {start_date} to {curr_date}:\n\n{news_str}"


def get_insider_transactions(
    ticker: Annotated[str, "ticker symbol of the company"],
) -> str:
    """AKShare does not provide a direct insider-transactions equivalent.

    Returns a descriptive message so the agent can proceed gracefully.
    """
    return (
        f"Insider transaction data is not available through AKShare for '{ticker}'. "
        f"Consider using yfinance or alpha_vantage for insider transaction information."
    )
