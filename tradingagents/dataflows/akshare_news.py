"""AKShare-based news data fetching (East Money stock news + CCTV macro news)."""

import logging
import time
from datetime import datetime
from typing import Annotated

import pandas as pd
from dateutil.relativedelta import relativedelta

from .akshare_common import AkShareError, akshare_retry, detect_market, normalize_symbol_cn, normalize_symbol_hk

logger = logging.getLogger(__name__)


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
    if market == "hk":
        symbol = normalize_symbol_hk(ticker)
    elif market == "cn":
        symbol = normalize_symbol_cn(ticker)
    else:
        return (
            f"AKShare stock news is available for Chinese A-share and HK stocks. "
            f"Symbol '{ticker}' appears to be a {market.upper()} ticker. "
            f"Consider using yfinance or alpha_vantage for news on this symbol."
        )
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    logger.info("[AKSHARE] get_news(%s, %s, %s)", ticker, start_date, end_date)
    t0 = time.perf_counter()
    try:
        df = akshare_retry(ak.stock_news_em, symbol=symbol)
    except Exception as e:
        logger.error("[AKSHARE] get_news FAILED for %s in %.2fs: %s", ticker, time.perf_counter() - t0, e)
        raise AkShareError(f"Failed to fetch news for {ticker}: {e}") from e
    logger.info("[AKSHARE] get_news OK: %s, %d rows in %.2fs", ticker, len(df) if df is not None else 0, time.perf_counter() - t0)

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
    """Fetch macro/global news, trying multiple sources in order.

    Sources (in priority order):
    1. East Money global finance news (``stock_info_global_em``)
    2. Sina global finance news (``stock_info_global_sina``)
    3. CCTV news (``news_cctv``) — often unstable
    """
    import akshare as ak

    logger.info("[AKSHARE] get_global_news(%s, lookback=%d, limit=%d)", curr_date, look_back_days, limit)
    t0 = time.perf_counter()

    # --- Try East Money first ---
    try:
        df = akshare_retry(ak.stock_info_global_em)
        if df is not None and not df.empty:
            all_news = []
            for _, row in df.head(limit).iterrows():
                all_news.append({
                    "date": str(row.get("发布时间", ""))[:10],
                    "title": row.get("标题", ""),
                    "content": str(row.get("摘要", ""))[:300],
                })
            logger.info("[AKSHARE] get_global_news via EastMoney: %d articles in %.2fs", len(all_news), time.perf_counter() - t0)
            return _format_global_news(all_news, curr_date, "East Money")
    except Exception as e:
        logger.warning("[AKSHARE] EastMoney global news failed: %s", e)

    # --- Try Sina ---
    try:
        df = akshare_retry(ak.stock_info_global_sina)
        if df is not None and not df.empty:
            all_news = []
            for _, row in df.head(limit).iterrows():
                all_news.append({
                    "date": str(row.get("时间", ""))[:10],
                    "title": "",
                    "content": str(row.get("内容", ""))[:300],
                })
            logger.info("[AKSHARE] get_global_news via Sina: %d articles in %.2fs", len(all_news), time.perf_counter() - t0)
            return _format_global_news(all_news, curr_date, "Sina")
    except Exception as e:
        logger.warning("[AKSHARE] Sina global news failed: %s", e)

    # --- Last resort: CCTV (per-day, slow) ---
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
                        "content": str(row.get("content", ""))[:300],
                    })
                    if len(all_news) >= limit:
                        break
        except Exception:
            pass
        current -= relativedelta(days=1)

    logger.info("[AKSHARE] get_global_news via CCTV: %d articles in %.2fs", len(all_news), time.perf_counter() - t0)
    if all_news:
        return _format_global_news(all_news, curr_date, "CCTV")

    return f"No global news found for {curr_date}"


def _format_global_news(articles: list[dict], curr_date: str, source: str) -> str:
    news_str = ""
    for item in articles:
        title = item.get("title") or ""
        content = item.get("content") or ""
        date = item.get("date") or ""
        if title:
            news_str += f"### {title} ({date})\n"
        else:
            news_str += f"### ({date})\n"
        if content:
            news_str += f"{content}\n"
        news_str += "\n"
    return f"## Global / Macro News ({source}), as of {curr_date}:\n\n{news_str}"


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
