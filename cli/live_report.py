"""Live HTML report that auto-refreshes during analysis.

Writes an HTML file to disk that the user opens in a browser.  The page
polls via ``<meta http-equiv="refresh">`` and shows the full intermediate
reports with smooth scrolling.  When the analysis finishes, the auto-refresh
stops.
"""

from __future__ import annotations

import html
import time
from pathlib import Path
from typing import Dict, Optional


def _md_to_html(text: str) -> str:
    try:
        import markdown
        return markdown.markdown(
            text,
            extensions=["tables", "fenced_code", "nl2br", "sane_lists"],
        )
    except ImportError:
        import re
        escaped = html.escape(text)
        escaped = re.sub(r"^### (.+)$", r"<h3>\1</h3>", escaped, flags=re.MULTILINE)
        escaped = re.sub(r"^## (.+)$", r"<h2>\1</h2>", escaped, flags=re.MULTILINE)
        escaped = re.sub(r"^# (.+)$", r"<h1>\1</h1>", escaped, flags=re.MULTILINE)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
        escaped = re.sub(r"\*(.+?)\*", r"<em>\1</em>", escaped)
        escaped = re.sub(r"^- (.+)$", r"<li>\1</li>", escaped, flags=re.MULTILINE)
        escaped = re.sub(r"\n{2,}", "</p><p>", escaped)
        return f"<p>{escaped}</p>"


_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
{refresh_tag}
<title>TradingAgents Live - {ticker}</title>
<style>
:root {{
  --bg: #0d1117; --card: #161b22; --border: #30363d;
  --text: #e6edf3; --dim: #8b949e; --accent: #58a6ff;
  --green: #3fb950; --red: #f85149; --yellow: #d29922; --blue: #388bfd;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--text); line-height: 1.6;
}}
header {{
  position: sticky; top: 0; z-index: 100;
  background: var(--bg); border-bottom: 1px solid var(--border);
  padding: 12px 24px;
  display: flex; align-items: center; gap: 20px;
}}
header h1 {{ font-size: 18px; color: var(--accent); }}
.badge {{
  display: inline-block; padding: 2px 10px; border-radius: 12px;
  font-size: 12px; font-weight: 600;
}}
.badge.running {{ background: rgba(56,139,253,.15); color: var(--blue); }}
.badge.done {{ background: rgba(63,185,80,.15); color: var(--green); }}
.stats {{
  display: flex; gap: 16px; margin-left: auto; font-size: 13px; color: var(--dim);
}}
.stats span {{ white-space: nowrap; }}
/* Progress bar */
.progress-bar {{
  height: 3px; background: var(--border);
}}
.progress-bar .fill {{
  height: 100%; background: var(--accent); transition: width .3s;
}}
/* Main layout */
.container {{ max-width: 1000px; margin: 0 auto; padding: 24px; }}
/* Agent status grid */
.agent-grid {{
  display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 8px; margin-bottom: 24px;
}}
.agent-chip {{
  padding: 8px 12px; border-radius: 6px; font-size: 13px;
  border: 1px solid var(--border); background: var(--card);
  display: flex; align-items: center; gap: 8px;
}}
.dot {{ width: 8px; height: 8px; border-radius: 50%; }}
.dot.pending {{ background: var(--dim); }}
.dot.running {{ background: var(--blue); animation: pulse 1s infinite; }}
.dot.done {{ background: var(--green); }}
.dot.error {{ background: var(--red); }}
@keyframes pulse {{ 0%,100% {{ opacity:1; }} 50% {{ opacity:.4; }} }}
/* Report sections */
.report-section {{
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; margin-bottom: 16px; overflow: hidden;
}}
.report-section summary {{
  padding: 14px 20px; cursor: pointer; font-size: 16px;
  font-weight: 600; color: var(--accent); list-style: none;
  display: flex; align-items: center; gap: 10px;
}}
.report-section summary::before {{
  content: "\\25B6"; font-size: 10px; transition: transform .2s;
  color: var(--dim);
}}
.report-section[open] summary::before {{ transform: rotate(90deg); }}
.report-body {{
  padding: 4px 20px 20px; border-top: 1px solid var(--border);
}}
.report-body h2 {{ font-size: 16px; color: var(--accent); margin: 16px 0 8px; }}
.report-body h3 {{ font-size: 15px; color: var(--green); margin: 14px 0 6px; }}
.report-body h4 {{ font-size: 14px; color: var(--yellow); margin: 12px 0 6px; }}
.report-body p {{ margin: 6px 0; }}
.report-body ul, .report-body ol {{ margin: 6px 0 6px 20px; }}
.report-body li {{ margin: 3px 0; }}
.report-body table {{
  width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 13px;
}}
.report-body th, .report-body td {{
  padding: 6px 10px; border: 1px solid var(--border); text-align: left;
}}
.report-body th {{ background: rgba(88,166,255,.06); color: var(--accent); }}
.report-body code {{
  background: rgba(110,118,129,.15); padding: 2px 5px; border-radius: 3px; font-size: 13px;
}}
.report-body pre {{
  background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
  padding: 10px 14px; overflow-x: auto; font-size: 13px; margin: 10px 0;
}}
.report-body pre code {{ background: none; padding: 0; }}
.empty {{ color: var(--dim); font-style: italic; padding: 20px; text-align: center; }}
/* Messages log */
.messages-section {{
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; margin-bottom: 16px;
}}
.messages-section summary {{
  padding: 14px 20px; cursor: pointer; font-size: 16px;
  font-weight: 600; color: var(--yellow); list-style: none;
  display: flex; align-items: center; gap: 10px;
}}
.messages-section summary::before {{
  content: "\\25B6"; font-size: 10px; transition: transform .2s; color: var(--dim);
}}
.messages-section[open] summary::before {{ transform: rotate(90deg); }}
.msg-table {{
  width: 100%; border-collapse: collapse; font-size: 13px;
}}
.msg-table td {{
  padding: 4px 12px; border-bottom: 1px solid var(--border); vertical-align: top;
}}
.msg-table .time {{ color: var(--dim); width: 70px; white-space: nowrap; }}
.msg-table .type {{ color: var(--green); width: 90px; }}
.msg-table .content {{ word-break: break-word; }}
</style>
</head>
<body>

<header>
  <h1>TradingAgents Live</h1>
  <span class="badge {status_class}">{status_label}</span>
  <div class="stats">
    <span>Ticker: <strong>{ticker}</strong></span>
    <span>Elapsed: <strong>{elapsed}</strong></span>
    <span>Agents: <strong>{agents_done}/{agents_total}</strong></span>
    <span>Reports: <strong>{reports_done}/{reports_total}</strong></span>
  </div>
</header>
<div class="progress-bar"><div class="fill" style="width:{progress_pct}%"></div></div>

<div class="container">
  <div class="agent-grid">
    {agent_chips}
  </div>

  {report_sections}

  <details class="messages-section" open>
    <summary>Messages &amp; Tool Calls ({msg_count})</summary>
    <table class="msg-table">
      {message_rows}
    </table>
  </details>
</div>

<script>
// Preserve scroll position across refreshes
(function() {{
  const key = 'ta-scroll-' + location.pathname;
  window.addEventListener('beforeunload', () => {{
    sessionStorage.setItem(key, window.scrollY);
  }});
  const saved = sessionStorage.getItem(key);
  if (saved) window.scrollTo(0, parseInt(saved));
}})();
</script>
</body>
</html>
"""


class LiveReportWriter:
    """Writes and periodically updates an HTML file showing analysis progress."""

    def __init__(self, out_dir: Path, ticker: str):
        self.out_dir = out_dir
        self.ticker = ticker
        self.html_path = out_dir / "live_report.html"
        self.start_time = time.time()
        self._finished = False

        self.agent_status: Dict[str, str] = {}
        self.report_sections: Dict[str, Optional[str]] = {}
        self.messages: list[tuple[str, str, str]] = []
        self.tool_calls: list[tuple[str, str, dict]] = []

        out_dir.mkdir(parents=True, exist_ok=True)

    def sync_from_buffer(self, message_buffer) -> None:
        """Pull latest state from the CLI's MessageBuffer."""
        self.agent_status = dict(message_buffer.agent_status)
        self.report_sections = dict(message_buffer.report_sections)
        self.messages = list(message_buffer.messages)
        self.tool_calls = list(message_buffer.tool_calls)

    def finish(self) -> None:
        self._finished = True
        self.write()

    def write(self) -> None:
        elapsed_s = time.time() - self.start_time
        elapsed = f"{int(elapsed_s // 60):02d}:{int(elapsed_s % 60):02d}"

        agents_done = sum(1 for s in self.agent_status.values() if s == "completed")
        agents_total = max(len(self.agent_status), 1)
        reports_done = sum(1 for v in self.report_sections.values() if v)
        reports_total = max(len(self.report_sections), 1)
        progress_pct = int(agents_done / agents_total * 100) if agents_total else 0

        status_class = "done" if self._finished else "running"
        status_label = "Complete" if self._finished else "Running..."
        refresh_tag = "" if self._finished else '<meta http-equiv="refresh" content="3">'

        # Agent chips
        _status_map = {
            "pending": ("pending", "Pending"),
            "in_progress": ("running", "Running"),
            "completed": ("done", "Done"),
            "error": ("error", "Error"),
        }
        agent_chips = []
        for agent, status in self.agent_status.items():
            dot_cls, label = _status_map.get(status, ("pending", status))
            agent_chips.append(
                f'<div class="agent-chip"><span class="dot {dot_cls}"></span>{html.escape(agent)}</div>'
            )
        agent_chips_html = "\n    ".join(agent_chips) if agent_chips else ""

        # Report sections
        _section_titles = {
            "market_report": ("I. Market Analysis", "market"),
            "sentiment_report": ("II. Social Sentiment", "sentiment"),
            "news_report": ("III. News Analysis", "news"),
            "fundamentals_report": ("IV. Fundamentals", "fundamentals"),
            "investment_plan": ("V. Research Team Decision", "research"),
            "trader_investment_plan": ("VI. Trading Plan", "trading"),
            "final_trade_decision": ("VII. Portfolio Decision", "portfolio"),
        }
        report_html_parts = []
        for key, (title, _css) in _section_titles.items():
            content = self.report_sections.get(key)
            if content:
                body_html = _md_to_html(content)
                report_html_parts.append(
                    f'<details class="report-section" open>'
                    f"<summary>{html.escape(title)}</summary>"
                    f'<div class="report-body">{body_html}</div>'
                    f"</details>"
                )
            elif key in self.report_sections:
                report_html_parts.append(
                    f'<details class="report-section">'
                    f"<summary>{html.escape(title)}</summary>"
                    f'<div class="empty">Waiting for data...</div>'
                    f"</details>"
                )
        report_sections_html = "\n  ".join(report_html_parts)

        # Messages
        all_msgs = []
        for ts, msg_type, content in self.messages:
            all_msgs.append((ts, msg_type, str(content)[:500] if content else ""))
        for ts, tool_name, args in self.tool_calls:
            args_str = ", ".join(f"{k}={v}" for k, v in args.items())
            all_msgs.append((ts, "Tool", f"{tool_name}({args_str})"))
        all_msgs.sort(key=lambda x: x[0], reverse=True)

        msg_rows = []
        for ts, mtype, content in all_msgs[:50]:
            msg_rows.append(
                f'<tr><td class="time">{html.escape(ts)}</td>'
                f'<td class="type">{html.escape(mtype)}</td>'
                f'<td class="content">{html.escape(content)}</td></tr>'
            )
        message_rows_html = "\n      ".join(msg_rows) if msg_rows else '<tr><td colspan="3" class="empty">No messages yet</td></tr>'

        final_html = _TEMPLATE.format(
            refresh_tag=refresh_tag,
            ticker=html.escape(self.ticker),
            status_class=status_class,
            status_label=status_label,
            elapsed=elapsed,
            agents_done=agents_done,
            agents_total=agents_total,
            reports_done=reports_done,
            reports_total=reports_total,
            progress_pct=progress_pct,
            agent_chips=agent_chips_html,
            report_sections=report_sections_html,
            message_rows=message_rows_html,
            msg_count=len(all_msgs),
        )

        self.html_path.write_text(final_html, encoding="utf-8")
