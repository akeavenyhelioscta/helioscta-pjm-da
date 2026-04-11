"""Master HTML report — aggregates sub-report HTML files into a single
dashboard with top toolbar, left sidebar navigation, and iframe content area.

The master and sub-report HTML files live in the same output/ directory,
so iframe src paths are simple filenames (no server needed).
"""
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# ── Sidebar grouping config ──────────────────────────────────────────
# Maps report keys to (group_label, display_label).
# Add new entries here when new fragments are registered.

REPORT_GROUPS: dict[str, tuple[str, str]] = {
    "like_day_forecast_results": ("DA Model", "Like Day Model"),
    "like_day_forecast_market_adjusted": ("DA Model", "Market Adjusted"),
    "like_day_forecast_regression_adjusted": ("DA Model", "Regression Adjusted"),
    "meteologica_da_price_forecast": ("DA Model", "Meteologica DA Price"),
    "like_day_forecast_strip": ("DA Model", "Strip Forecast"),
    "balday": ("RT BalDay", "Bal Day"),
    "rt_load_metered_rto": ("Input Validation", "RT Load Metered RTO"),
    "load_forecast_vintage_combined": ("Load Forecasts", "Load Forecast Vintages"),
    "pjm_rto_forecast_snapshot": ("Load Forecasts", "PJM RTO Snapshot"),
    "net_load_forecast_vintage_combined": ("Load Forecasts", "Net Load Forecast Vintages"),
    "solar_forecast_vintage_combined": ("Renewable Forecasts", "Solar Forecast Vintages"),
    "wind_forecast_vintage_combined": ("Renewable Forecasts", "Wind Forecast Vintages"),
    "regional_spark_spreads": ("Fuel Mix", "Regional Spark Spreads"),
    "fuel_mix": ("Fuel Mix", "Fuel Mix"),
    "lmp_history": ("Input Validation", "LMP History"),
    "outages_forecast_rto": ("Outages", "Forecast Outages RTO"),
    "outages_term_bible": ("Outages", "Term Bible"),
    "outages_seasonal": ("Outages", "Seasonal Outages RTO"),
    "tie_flows": ("Tie Flows", "Tie Flows"),
    "data_inspection": ("Data Inspection", "Data Inspection"),
    # "lmp":   ("Input Validation", "LMP Data"),
    # "gas":   ("Input Validation", "Gas Prices"),
}


def build_master(
    reports: dict[str, Path],
    output_dir: Path,
    title: str = "DA Model",
) -> Path:
    """Build a master HTML that navigates between sub-reports via iframe.

    Args:
        reports: Dict mapping report key (e.g. "load") to its HTML file path.
                 The key "combined" is excluded automatically.
        output_dir: Directory where master HTML is written (same as sub-reports).
        title: Title shown in the top bar.

    Returns:
        Path to the written master HTML file.
    """
    # Filter out the combined report — it's redundant with the master
    sub_reports = {k: v for k, v in reports.items() if k != "combined"}

    # Build sidebar groups: {group_label: [(key, display_label, filename), ...]}
    groups: dict[str, list[tuple[str, str, str]]] = {}
    for key, path in sub_reports.items():
        group_label, display_label = REPORT_GROUPS.get(key, ("Other", key))
        groups.setdefault(group_label, []).append((key, display_label, path.name))

    sidebar_html = _build_sidebar(groups)
    report_options_html = _build_report_options(sub_reports)

    # First report to show by default
    first_key = next(iter(sub_reports), None)
    default_src = sub_reports[first_key].name if first_key else ""
    default_label = REPORT_GROUPS.get(first_key, ("", first_key))[1] if first_key else ""

    timestamp = datetime.now(ZoneInfo("America/Denver")).strftime("%a %b %d, %Y %H:%M MST")

    html = _MASTER_TEMPLATE.format(
        title=title,
        timestamp=timestamp,
        sidebar_html=sidebar_html,
        report_options_html=report_options_html,
        default_src=default_src,
        default_label=default_label,
    )

    output_path = output_dir / "master_report.html"
    output_path.write_text(html, encoding="utf-8")
    return output_path


# ── Builders ─────────────────────────────────────────────────────────


def _build_sidebar(groups: dict[str, list[tuple[str, str, str]]]) -> str:
    """Build sidebar nav HTML with collapsible groups."""
    html = ""
    for group_label, items in groups.items():
        html += f'''
        <div class="nav-group">
            <div class="nav-group-header" onclick="this.parentElement.classList.toggle('collapsed')">
                <span class="nav-chevron">&#9662;</span>
                <span>{group_label}</span>
            </div>
            <div class="nav-group-items">
        '''
        for key, display_label, filename in items:
            html += f'''
                <a class="nav-item" data-key="{key}" data-src="{filename}"
                   onclick="selectReport(this)">
                    {display_label}
                </a>
            '''
        html += '''
            </div>
        </div>
        '''
    return html


def _build_report_options(sub_reports: dict[str, Path]) -> str:
    """Build <option> tags for the report filter dropdown."""
    html = ""
    for key, path in sub_reports.items():
        label = REPORT_GROUPS.get(key, ("", key))[1]
        html += f'<option value="{path.name}" data-key="{key}">{label} — {path.name}</option>\n'
    return html


# ── HTML Template ────────────────────────────────────────────────────

_MASTER_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@600;700&display=swap" rel="stylesheet">
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
html, body {{ height:100%; overflow:hidden; }}

body {{
    font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, sans-serif;
    font-size: 13px;
    background: #070b14;
    color: #dbe7ff;
}}

/* ── Top Bar ──────────────────────────────────── */

.top-bar {{
    height: 44px;
    background: rgba(8, 14, 24, 0.95);
    border-bottom: 1px solid #2a3f60;
    display: flex;
    align-items: center;
    padding: 0 16px;
    gap: 12px;
    backdrop-filter: blur(8px);
    z-index: 200;
    position: relative;
}}

.top-bar .menu-toggle {{
    background: none; border: none;
    color: #c5d8f2; font-size: 18px;
    cursor: pointer; padding: 4px 8px;
}}
.top-bar .menu-toggle:hover {{ background: #1a2a42; border-radius: 3px; }}

.top-bar .title {{
    font-family: 'Space Grotesk', sans-serif;
    font-size: 15px; font-weight: 700;
    color: #8dd9ff; letter-spacing: 0.2px;
    white-space: nowrap;
}}

.top-bar .filter-group {{
    display: flex; align-items: center; gap: 8px;
    margin-left: 24px; flex: 1; min-width: 0;
}}
.top-bar .filter-label {{
    font-size: 12px; color: #6f8db1;
    font-weight: 600; white-space: nowrap;
}}
.top-bar select {{
    flex: 1; max-width: 420px;
    padding: 5px 10px; font-size: 12px;
    background: #101d31; color: #dbe7ff;
    border: 1px solid #2a3f60; border-radius: 4px;
    font-family: inherit;
}}

.top-bar .actions {{ display:flex; gap:8px; margin-left:auto; }}
.top-bar .btn {{
    padding: 5px 14px; font-size: 12px; font-weight: 600;
    border: none; border-radius: 4px; cursor: pointer;
    font-family: inherit; white-space: nowrap;
}}
.btn-primary {{ background: #1a73e8; color: #fff; }}
.btn-primary:hover {{ background: #1557b0; }}

.top-bar .timestamp {{
    font-size: 11px; color: #6f8db1;
    white-space: nowrap; margin-left: 8px;
}}

/* ── Layout ───────────────────────────────────── */

.layout {{
    display: flex;
    height: calc(100vh - 44px);
}}

/* ── Sidebar ──────────────────────────────────── */

.sidebar {{
    width: 220px; min-width: 220px;
    background: #0f1a2b;
    border-right: 1px solid #253b59;
    overflow-y: auto;
    transition: width 0.2s, min-width 0.2s, opacity 0.2s;
}}
.sidebar.collapsed {{ width:0; min-width:0; overflow:hidden; opacity:0; }}

.nav-group {{ }}
.nav-group.collapsed .nav-group-items {{ display:none; }}
.nav-group.collapsed .nav-chevron {{ transform: rotate(-90deg); }}

.nav-group-header {{
    padding: 10px 14px 4px;
    font-size: 11px; font-weight: 700;
    color: #6f8db1; text-transform: uppercase;
    letter-spacing: 0.5px; cursor: pointer;
    display: flex; align-items: center; gap: 6px;
    user-select: none;
    border-top: 1px solid #243a57;
    margin-top: 4px;
}}
.nav-group:first-child .nav-group-header {{ border-top: none; margin-top: 0; padding-top: 12px; }}

.nav-chevron {{
    font-size: 10px; transition: transform 0.15s;
    color: #4a6a8f;
}}

.nav-group-items {{ padding: 2px 0 6px; }}

.nav-item {{
    display: block;
    padding: 7px 14px 7px 28px;
    color: #9eb4d3; font-size: 13px; font-weight: 500;
    text-decoration: none; cursor: pointer;
    border-left: 3px solid transparent;
    transition: all 0.12s;
}}
.nav-item:hover {{ background: #1a2b44; color: #dbe7ff; }}
.nav-item.active {{
    background: #20314d;
    color: #8dd9ff;
    border-left-color: #4cc9f0;
    font-weight: 600;
}}

/* ── Content ──────────────────────────────────── */

.content {{
    flex: 1; min-width: 0;
    background: #0b1220;
}}
.content iframe {{
    width: 100%; height: 100%;
    border: none;
}}

/* ── Scrollbar ────────────────────────────────── */

::-webkit-scrollbar {{ width:8px; height:8px; }}
::-webkit-scrollbar-track {{ background:#0f1a2b; }}
::-webkit-scrollbar-thumb {{ background:#335074; border-radius:4px; }}
::-webkit-scrollbar-thumb:hover {{ background:#4b6e96; }}

/* ── Responsive ───────────────────────────────── */

@media (max-width: 768px) {{
    .sidebar {{ position:absolute; z-index:100; height:calc(100vh - 44px); }}
    .sidebar.collapsed {{ width:0; }}
    .top-bar .filter-group {{ display:none; }}
    .top-bar .timestamp {{ display:none; }}
}}
</style>
</head>
<body>

<!-- Top Bar -->
<div class="top-bar">
    <button class="menu-toggle" onclick="toggleSidebar()" title="Toggle sidebar">&#9776;</button>
    <span class="title">{title}</span>

    <div class="filter-group">
        <span class="filter-label">Report Filter</span>
        <select id="reportSelect" onchange="selectFromDropdown(this)">
            {report_options_html}
        </select>
    </div>

    <div class="actions">
        <button class="btn btn-primary" onclick="openInNewTab()">Open in New Tab</button>
    </div>

    <span class="timestamp">{timestamp}</span>
</div>

<!-- Layout -->
<div class="layout">
    <aside class="sidebar" id="sidebar">
        {sidebar_html}
    </aside>

    <main class="content">
        <iframe id="reportFrame" src="{default_src}"></iframe>
    </main>
</div>

<script>
(function() {{
    const frame   = document.getElementById('reportFrame');
    const select  = document.getElementById('reportSelect');
    const items   = document.querySelectorAll('.nav-item');

    // ── Restore from URL hash ────────────────────
    const hash = window.location.hash.slice(1);
    if (hash) {{
        const target = document.querySelector(`.nav-item[data-key="${{hash}}"]`);
        if (target) {{ selectReport(target, true); }}
    }} else {{
        // Highlight default
        const first = document.querySelector('.nav-item');
        if (first) first.classList.add('active');
    }}
}})();

function selectReport(el, skipHash) {{
    const src = el.getAttribute('data-src');
    const key = el.getAttribute('data-key');

    // Update iframe
    document.getElementById('reportFrame').src = src;

    // Update active state
    document.querySelectorAll('.nav-item').forEach(i => i.classList.remove('active'));
    el.classList.add('active');

    // Sync dropdown
    const select = document.getElementById('reportSelect');
    for (let i = 0; i < select.options.length; i++) {{
        if (select.options[i].value === src) {{ select.selectedIndex = i; break; }}
    }}

    // Update URL hash for deep linking
    if (!skipHash) {{ window.location.hash = key; }}
}}

function selectFromDropdown(sel) {{
    const src = sel.value;
    const key = sel.options[sel.selectedIndex].getAttribute('data-key');

    document.getElementById('reportFrame').src = src;

    document.querySelectorAll('.nav-item').forEach(i => {{
        i.classList.toggle('active', i.getAttribute('data-key') === key);
    }});

    window.location.hash = key;
}}

function openInNewTab() {{
    const src = document.getElementById('reportFrame').src;
    if (src) window.open(src, '_blank');
}}

function toggleSidebar() {{
    document.getElementById('sidebar').classList.toggle('collapsed');
}}
</script>
</body>
</html>"""
