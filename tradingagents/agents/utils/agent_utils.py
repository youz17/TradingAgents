import logging
import time

from langchain_core.messages import HumanMessage, RemoveMessage

logger = logging.getLogger(__name__)

# Import tools from separate utility files
from tradingagents.agents.utils.core_stock_tools import (
    get_stock_data
)
from tradingagents.agents.utils.technical_indicators_tools import (
    get_indicators
)
from tradingagents.agents.utils.fundamental_data_tools import (
    get_fundamentals,
    get_balance_sheet,
    get_cashflow,
    get_income_statement
)
from tradingagents.agents.utils.news_data_tools import (
    get_news,
    get_insider_transactions,
    get_global_news
)


def get_language_instruction() -> str:
    """Return a prompt instruction for the configured output language.

    Returns empty string when English (default), so no extra tokens are used.
    Only applied to user-facing agents (analysts, portfolio manager).
    Internal debate agents stay in English for reasoning quality.
    """
    from tradingagents.dataflows.config import get_config
    lang = get_config().get("output_language", "English")
    if lang.strip().lower() == "english":
        return ""
    return f" Write your entire response in {lang}."


def build_instrument_context(ticker: str) -> str:
    """Describe the exact instrument so agents preserve exchange-qualified tickers."""
    return (
        f"The instrument to analyze is `{ticker}`. "
        "Use this exact ticker in every tool call, report, and recommendation, "
        "preserving any exchange suffix (e.g. `.TO`, `.L`, `.HK`, `.T`)."
    )

_llm_call_count = 0
_llm_total_time = 0.0


def llm_retry(fn, *args, max_retries: int = 5, base_delay: float = 4.0, **kwargs):
    """Call *fn* with exponential backoff on transient LLM API errors (429, 5xx).

    Re-raises the last exception if all retries are exhausted so the caller
    still sees the original error.
    """
    global _llm_call_count, _llm_total_time
    _llm_call_count += 1
    call_num = _llm_call_count
    fn_name = getattr(fn, "__qualname__", getattr(fn, "__name__", str(fn)))
    logger.info("[LLM] #%d calling %s", call_num, fn_name)
    t0 = time.perf_counter()

    for attempt in range(max_retries + 1):
        try:
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - t0
            _llm_total_time += elapsed
            content_len = len(getattr(result, "content", "")) if hasattr(result, "content") else 0
            logger.info(
                "[LLM] #%d %s ✓ %.2fs (attempt %d) response=%d chars  cumulative: %d calls, %.1fs total",
                call_num, fn_name, elapsed, attempt + 1, content_len, _llm_call_count, _llm_total_time,
            )
            return result
        except Exception as exc:
            exc_str = str(exc).lower()
            transient = any(k in exc_str for k in (
                "rate limit", "rate_limit", "too many requests",
                "429", "500", "502", "503", "504",
                "service_unavailable", "service unavailable",
                "overloaded", "server_error", "internal server error",
                "service is too busy",
            ))
            if not transient or attempt >= max_retries:
                elapsed = time.perf_counter() - t0
                logger.error("[LLM] #%d %s ✗ FAILED in %.2fs after %d attempts: %s", call_num, fn_name, elapsed, attempt + 1, exc)
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(
                "[LLM] #%d %s transient error (attempt %d/%d), retrying in %.0fs: %s",
                call_num, fn_name, attempt + 1, max_retries, delay, exc,
            )
            time.sleep(delay)


def create_msg_delete():
    def delete_messages(state):
        """Clear messages and add placeholder for Anthropic compatibility"""
        messages = state["messages"]

        # Remove all messages
        removal_operations = [RemoveMessage(id=m.id) for m in messages]

        # Add a minimal placeholder message
        placeholder = HumanMessage(content="Continue")

        return {"messages": removal_operations + [placeholder]}

    return delete_messages


        
