"""Generate a self-contained HTML report from a complete_report.md file.

The HTML includes:
- Fixed sidebar with table-of-contents navigation
- Smooth scrolling between sections
- Collapsible subsections
- Responsive layout with modern styling
- Markdown rendered to HTML via the ``markdown`` stdlib or a minimal fallback
"""

from __future__ import annotations

import html
import re
from pathlib import Path


def _md_to_html(md_text: str) -> str:
    """Convert Markdown to HTML. Uses the ``markdown`` package if available,
    otherwise falls back to a lightweight regex converter."""
    try:
        import markdown
        return markdown.markdown(
            md_text,
            extensions=["tables", "fenced_code", "nl2br", "sane_lists"],
        )
    except ImportError:
        return _simple_md_to_html(md_text)


def _simple_md_to_html(text: str) -> str:
    """Bare-minimum Markdown -> HTML for environments without the markdown pkg."""
    escaped = html.escape(text)
    # Headings
    escaped = re.sub(r"^##### (.+)$", r"<h5>\1</h5>", escaped, flags=re.MULTILINE)
    escaped = re.sub(r"^#### (.+)$", r"<h4>\1</h4>", escaped, flags=re.MULTILINE)
    escaped = re.sub(r"^### (.+)$", r"<h3>\1</h3>", escaped, flags=re.MULTILINE)
    escaped = re.sub(r"^## (.+)$", r"<h2>\1</h2>", escaped, flags=re.MULTILINE)
    escaped = re.sub(r"^# (.+)$", r"<h1>\1</h1>", escaped, flags=re.MULTILINE)
    # Bold / italic
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"\*(.+?)\*", r"<em>\1</em>", escaped)
    # Lists
    escaped = re.sub(r"^- (.+)$", r"<li>\1</li>", escaped, flags=re.MULTILINE)
    # Paragraphs
    escaped = re.sub(r"\n{2,}", "</p><p>", escaped)
    return f"<p>{escaped}</p>"


_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{
  --sidebar-w: 260px;
  --bg: #0d1117;
  --bg-card: #161b22;
  --bg-sidebar: #0d1117;
  --border: #30363d;
  --text: #e6edf3;
  --text-dim: #8b949e;
  --accent: #58a6ff;
  --accent-hover: #79c0ff;
  --green: #3fb950;
  --red: #f85149;
  --yellow: #d29922;
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
html {{ scroll-behavior: smooth; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.6;
}}
/* Sidebar */
.sidebar {{
  position: fixed;
  top: 0; left: 0;
  width: var(--sidebar-w);
  height: 100vh;
  overflow-y: auto;
  background: var(--bg-sidebar);
  border-right: 1px solid var(--border);
  padding: 24px 16px;
  z-index: 100;
}}
.sidebar h2 {{
  font-size: 14px;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text-dim);
  margin-bottom: 16px;
}}
.sidebar a {{
  display: block;
  padding: 6px 12px;
  margin: 2px 0;
  color: var(--text-dim);
  text-decoration: none;
  font-size: 13px;
  border-radius: 6px;
  transition: all .15s;
}}
.sidebar a:hover,
.sidebar a.active {{
  color: var(--accent);
  background: rgba(88,166,255,.08);
}}
.sidebar a.sub {{ padding-left: 28px; font-size: 12px; }}
/* Main content */
.main {{
  margin-left: var(--sidebar-w);
  padding: 40px 48px 80px;
  max-width: 900px;
}}
.main h1 {{
  font-size: 28px;
  margin-bottom: 8px;
  color: var(--text);
}}
.main .meta {{
  color: var(--text-dim);
  font-size: 14px;
  margin-bottom: 32px;
}}
.section {{
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 8px;
  margin-bottom: 24px;
  overflow: hidden;
}}
.section-header {{
  padding: 16px 20px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 10px;
  user-select: none;
}}
.section-header h2 {{
  font-size: 18px;
  color: var(--accent);
  flex: 1;
}}
.section-header .chevron {{
  transition: transform .2s;
  color: var(--text-dim);
}}
.section.collapsed .chevron {{ transform: rotate(-90deg); }}
.section.collapsed .section-body {{ display: none; }}
.section-body {{
  padding: 0 20px 20px;
}}
.section-body h3 {{
  font-size: 16px;
  color: var(--green);
  margin: 20px 0 10px;
  padding-bottom: 6px;
  border-bottom: 1px solid var(--border);
}}
.section-body h4 {{ font-size: 14px; color: var(--yellow); margin: 16px 0 8px; }}
.section-body p  {{ margin: 8px 0; color: var(--text); }}
.section-body ul, .section-body ol {{ margin: 8px 0 8px 24px; }}
.section-body li {{ margin: 4px 0; }}
.section-body table {{
  width: 100%;
  border-collapse: collapse;
  margin: 12px 0;
  font-size: 13px;
}}
.section-body th, .section-body td {{
  padding: 8px 12px;
  border: 1px solid var(--border);
  text-align: left;
}}
.section-body th {{ background: rgba(88,166,255,.06); color: var(--accent); }}
.section-body code {{
  background: rgba(110,118,129,.15);
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 13px;
}}
.section-body pre {{
  background: #0d1117;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 12px 16px;
  overflow-x: auto;
  font-size: 13px;
  margin: 12px 0;
}}
.section-body pre code {{ background: none; padding: 0; }}
.section-body strong {{ color: var(--text); }}
/* Back to top */
.back-top {{
  position: fixed;
  bottom: 24px;
  right: 24px;
  width: 40px;
  height: 40px;
  border-radius: 50%;
  background: var(--accent);
  color: var(--bg);
  border: none;
  cursor: pointer;
  font-size: 20px;
  display: none;
  align-items: center;
  justify-content: center;
  box-shadow: 0 2px 8px rgba(0,0,0,.4);
  z-index: 200;
}}
.back-top.visible {{ display: flex; }}
/* Responsive */
@media (max-width: 800px) {{
  .sidebar {{ display: none; }}
  .main {{ margin-left: 0; padding: 20px; }}
}}
</style>
</head>
<body>

<nav class="sidebar">
  <h2>Navigation</h2>
  {toc}
</nav>

<main class="main">
  <h1>{title}</h1>
  <div class="meta">{meta}</div>
  {body}
</main>

<button class="back-top" onclick="window.scrollTo({{top:0,behavior:'smooth'}})" title="Back to top">&#8593;</button>

<script>
// Collapsible sections
document.querySelectorAll('.section-header').forEach(h => {{
  h.addEventListener('click', () => h.parentElement.classList.toggle('collapsed'));
}});
// Scroll spy for sidebar
const links = document.querySelectorAll('.sidebar a');
const sections = document.querySelectorAll('.section[id]');
const backTop = document.querySelector('.back-top');
window.addEventListener('scroll', () => {{
  backTop.classList.toggle('visible', window.scrollY > 400);
  let current = '';
  sections.forEach(s => {{ if (window.scrollY >= s.offsetTop - 100) current = s.id; }});
  links.forEach(a => a.classList.toggle('active', a.getAttribute('href') === '#' + current));
}});
</script>
</body>
</html>
"""


def generate_html_report(md_path: Path, out_path: Path | None = None) -> Path:
    """Read *md_path*, convert to a styled HTML report, and write next to it.

    Returns the path to the generated HTML file.
    """
    md_text = md_path.read_text(encoding="utf-8")
    if out_path is None:
        out_path = md_path.with_suffix(".html")

    # Extract title from first H1
    title_match = re.search(r"^#\s+(.+)$", md_text, re.MULTILINE)
    title = title_match.group(1) if title_match else "Trading Analysis Report"

    # Extract generated timestamp
    meta_match = re.search(r"Generated:\s*(.+)", md_text)
    meta = f"Generated: {meta_match.group(1)}" if meta_match else ""

    # Split into sections by H2
    raw_sections = re.split(r"(?=^## )", md_text, flags=re.MULTILINE)
    # First element is the header / intro before any H2
    raw_sections = [s.strip() for s in raw_sections if s.strip()]
    # Filter out the title block (starts with #)
    content_sections = [s for s in raw_sections if s.startswith("## ")]

    toc_lines = []
    body_parts = []

    for idx, sec in enumerate(content_sections):
        # Parse section heading
        heading_match = re.match(r"^## (.+)$", sec, re.MULTILINE)
        heading = heading_match.group(1) if heading_match else f"Section {idx+1}"
        sec_id = f"section-{idx}"

        toc_lines.append(f'<a href="#{sec_id}">{heading}</a>')

        # Find H3 subsections for nested TOC
        for sub_match in re.finditer(r"^### (.+)$", sec, re.MULTILINE):
            sub_name = sub_match.group(1)
            toc_lines.append(f'<a href="#{sec_id}" class="sub">{sub_name}</a>')

        # Convert section body (everything after the H2 line) to HTML
        sec_body = re.sub(r"^## .+$", "", sec, count=1, flags=re.MULTILINE).strip()
        sec_html = _md_to_html(sec_body)

        body_parts.append(
            f'<div class="section" id="{sec_id}">'
            f'  <div class="section-header">'
            f'    <h2>{html.escape(heading)}</h2>'
            f'    <span class="chevron">&#9660;</span>'
            f'  </div>'
            f'  <div class="section-body">{sec_html}</div>'
            f'</div>'
        )

    final_html = _HTML_TEMPLATE.format(
        title=html.escape(title),
        meta=html.escape(meta),
        toc="\n  ".join(toc_lines),
        body="\n".join(body_parts),
    )

    out_path.write_text(final_html, encoding="utf-8")
    return out_path
