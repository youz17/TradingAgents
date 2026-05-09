import logging
import time
from collections import Counter
from typing import Annotated

logger = logging.getLogger(__name__)

_call_stats: Counter = Counter()
_call_timings: dict[str, list[float]] = {}

# Import from vendor-specific modules
from .y_finance import (
    get_YFin_data_online,
    get_stock_stats_indicators_window,
    get_fundamentals as get_yfinance_fundamentals,
    get_balance_sheet as get_yfinance_balance_sheet,
    get_cashflow as get_yfinance_cashflow,
    get_income_statement as get_yfinance_income_statement,
    get_insider_transactions as get_yfinance_insider_transactions,
)
from .yfinance_news import get_news_yfinance, get_global_news_yfinance
from .alpha_vantage import (
    get_stock as get_alpha_vantage_stock,
    get_indicator as get_alpha_vantage_indicator,
    get_fundamentals as get_alpha_vantage_fundamentals,
    get_balance_sheet as get_alpha_vantage_balance_sheet,
    get_cashflow as get_alpha_vantage_cashflow,
    get_income_statement as get_alpha_vantage_income_statement,
    get_insider_transactions as get_alpha_vantage_insider_transactions,
    get_news as get_alpha_vantage_news,
    get_global_news as get_alpha_vantage_global_news,
)
from .alpha_vantage_common import AlphaVantageRateLimitError
from .akshare import (
    get_stock as get_akshare_stock,
    get_indicator as get_akshare_indicator,
    get_fundamentals as get_akshare_fundamentals,
    get_balance_sheet as get_akshare_balance_sheet,
    get_cashflow as get_akshare_cashflow,
    get_income_statement as get_akshare_income_statement,
    get_news as get_akshare_news,
    get_global_news as get_akshare_global_news,
    get_insider_transactions as get_akshare_insider_transactions,
)
from .akshare_common import AkShareError

# Configuration and routing logic
from .config import get_config

# Tools organized by category
TOOLS_CATEGORIES = {
    "core_stock_apis": {
        "description": "OHLCV stock price data",
        "tools": [
            "get_stock_data"
        ]
    },
    "technical_indicators": {
        "description": "Technical analysis indicators",
        "tools": [
            "get_indicators"
        ]
    },
    "fundamental_data": {
        "description": "Company fundamentals",
        "tools": [
            "get_fundamentals",
            "get_balance_sheet",
            "get_cashflow",
            "get_income_statement"
        ]
    },
    "news_data": {
        "description": "News and insider data",
        "tools": [
            "get_news",
            "get_global_news",
            "get_insider_transactions",
        ]
    }
}

VENDOR_LIST = [
    "yfinance",
    "alpha_vantage",
    "akshare",
]

# Mapping of methods to their vendor-specific implementations
VENDOR_METHODS = {
    # core_stock_apis
    "get_stock_data": {
        "alpha_vantage": get_alpha_vantage_stock,
        "yfinance": get_YFin_data_online,
        "akshare": get_akshare_stock,
    },
    # technical_indicators
    "get_indicators": {
        "alpha_vantage": get_alpha_vantage_indicator,
        "yfinance": get_stock_stats_indicators_window,
        "akshare": get_akshare_indicator,
    },
    # fundamental_data
    "get_fundamentals": {
        "alpha_vantage": get_alpha_vantage_fundamentals,
        "yfinance": get_yfinance_fundamentals,
        "akshare": get_akshare_fundamentals,
    },
    "get_balance_sheet": {
        "alpha_vantage": get_alpha_vantage_balance_sheet,
        "yfinance": get_yfinance_balance_sheet,
        "akshare": get_akshare_balance_sheet,
    },
    "get_cashflow": {
        "alpha_vantage": get_alpha_vantage_cashflow,
        "yfinance": get_yfinance_cashflow,
        "akshare": get_akshare_cashflow,
    },
    "get_income_statement": {
        "alpha_vantage": get_alpha_vantage_income_statement,
        "yfinance": get_yfinance_income_statement,
        "akshare": get_akshare_income_statement,
    },
    # news_data
    "get_news": {
        "alpha_vantage": get_alpha_vantage_news,
        "yfinance": get_news_yfinance,
        "akshare": get_akshare_news,
    },
    "get_global_news": {
        "yfinance": get_global_news_yfinance,
        "alpha_vantage": get_alpha_vantage_global_news,
        "akshare": get_akshare_global_news,
    },
    "get_insider_transactions": {
        "alpha_vantage": get_alpha_vantage_insider_transactions,
        "yfinance": get_yfinance_insider_transactions,
        "akshare": get_akshare_insider_transactions,
    },
}

def get_category_for_method(method: str) -> str:
    """Get the category that contains the specified method."""
    for category, info in TOOLS_CATEGORIES.items():
        if method in info["tools"]:
            return category
    raise ValueError(f"Method '{method}' not found in any category")

def get_vendor(category: str, method: str = None) -> str:
    """Get the configured vendor for a data category or specific tool method.
    Tool-level configuration takes precedence over category-level.
    """
    config = get_config()

    # Check tool-level configuration first (if method provided)
    if method:
        tool_vendors = config.get("tool_vendors", {})
        if method in tool_vendors:
            return tool_vendors[method]

    # Fall back to category-level configuration
    return config.get("data_vendors", {}).get(category, "default")

def route_to_vendor(method: str, *args, **kwargs):
    """Route method calls to appropriate vendor implementation with fallback support."""
    category = get_category_for_method(method)
    vendor_config = get_vendor(category, method)
    primary_vendors = [v.strip() for v in vendor_config.split(',')]

    if method not in VENDOR_METHODS:
        raise ValueError(f"Method '{method}' not supported")

    # Build fallback chain: primary vendors first, then remaining available vendors
    all_available_vendors = list(VENDOR_METHODS[method].keys())
    fallback_vendors = primary_vendors.copy()
    for vendor in all_available_vendors:
        if vendor not in fallback_vendors:
            fallback_vendors.append(vendor)

    call_key = f"{method}"
    _call_stats[call_key] += 1
    call_num = _call_stats[call_key]
    args_summary = ", ".join(str(a)[:60] for a in args)
    logger.info(
        "[ROUTE] #%d %s(%s) — vendors: %s",
        call_num, method, args_summary, " → ".join(fallback_vendors),
    )
    t0 = time.perf_counter()

    last_exc = None
    for vendor in fallback_vendors:
        if vendor not in VENDOR_METHODS[method]:
            continue

        vendor_impl = VENDOR_METHODS[method][vendor]
        impl_func = vendor_impl[0] if isinstance(vendor_impl, list) else vendor_impl

        vt0 = time.perf_counter()
        try:
            result = impl_func(*args, **kwargs)
            elapsed = time.perf_counter() - t0
            v_elapsed = time.perf_counter() - vt0
            _call_timings.setdefault(call_key, []).append(elapsed)
            result_len = len(result) if isinstance(result, str) else 0
            logger.info(
                "[ROUTE] #%d %s ✓ vendor=%s  %.2fs (vendor %.2fs)  result=%d chars  total_calls=%d",
                call_num, method, vendor, elapsed, v_elapsed, result_len, call_num,
            )
            return result
        except Exception as exc:
            v_elapsed = time.perf_counter() - vt0
            last_exc = exc
            logger.warning(
                "[ROUTE] #%d %s ✗ vendor=%s failed in %.2fs: %s",
                call_num, method, vendor, v_elapsed, exc,
            )
            continue

    elapsed = time.perf_counter() - t0
    logger.error(
        "[ROUTE] #%d %s ALL VENDORS FAILED in %.2fs: %s", call_num, method, elapsed, last_exc
    )
    return (
        f"[DATA UNAVAILABLE] All data vendors failed for '{method}'. "
        f"Last error: {last_exc}. "
        f"Please proceed with the analysis using whatever data is available."
    )


def get_call_stats_summary() -> str:
    """Return a human-readable summary of all route_to_vendor calls and timings."""
    lines = ["=== Data Vendor Call Statistics ==="]
    for method, count in sorted(_call_stats.items()):
        timings = _call_timings.get(method, [])
        if timings:
            avg_t = sum(timings) / len(timings)
            max_t = max(timings)
            total_t = sum(timings)
            lines.append(
                f"  {method}: {count} calls, {len(timings)} ok, "
                f"avg={avg_t:.2f}s, max={max_t:.2f}s, total={total_t:.1f}s"
            )
        else:
            lines.append(f"  {method}: {count} calls, 0 ok")
    return "\n".join(lines)