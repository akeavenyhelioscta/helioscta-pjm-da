"""Markdown formatters for view model endpoints.

Each public function takes the dict returned by the corresponding
``build_view_model()`` and returns a compact markdown string suitable
for agent consumption via MCP.

Design goals:
  - Preserve every data point from the JSON view model
  - Pivot hourly data (dates as rows, HE1-24 as columns) for ~70-80% size reduction
  - Use ``tabulate`` with ``tablefmt="pipe"`` for standard markdown tables
"""
from __future__ import annotations

from collections import defaultdict

from tabulate import tabulate


# ── Shared helpers ──────────────────────────────────────────────────


def _fmt(val, decimals: int = 2) -> str:
    """Format a numeric value or return ``-`` for None/missing."""
    if val is None:
        return "-"
    try:
        return f"{float(val):,.{decimals}f}"
    except (TypeError, ValueError):
        return "-"


def _fmt0(val) -> str:
    return _fmt(val, decimals=0)


def _fmt2(val) -> str:
    return _fmt(val, decimals=2)


def _table(headers: list[str], rows: list[list], floatfmt: str = ".2f") -> str:
    """Render a markdown pipe table via tabulate."""
    return tabulate(rows, headers=headers, tablefmt="pipe", numalign="right")


def _pivot_hourly(
    hourly: list[dict],
    date_key: str = "date",
    hour_key: str = "hour_ending",
    value_key: str = "lmp_total",
    decimals: int = 2,
) -> tuple[list[str], list[list]]:
    """Pivot hourly records into (headers, rows) with dates as rows and HE1-24 as columns."""
    by_date: dict[str, dict[int, float | None]] = defaultdict(dict)
    for rec in hourly:
        by_date[rec[date_key]][rec[hour_key]] = rec.get(value_key)

    headers = ["Date"] + [f"HE{h}" for h in range(1, 25)]
    rows = []
    for d in sorted(by_date.keys()):
        row = [d]
        for h in range(1, 25):
            row.append(_fmt(by_date[d].get(h), decimals))
        rows.append(row)
    return headers, rows


# ── LMP 7-Day Lookback ─────────────────────────────────────────────


def format_lmp_7day(vm: dict) -> str:
    """Format the LMP 7-day lookback view model as markdown.

    Combines DA, RT, and DART into single side-by-side tables so the
    agent can compare markets at a glance.
    """
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    hub = vm.get("hub", "Unknown Hub")
    dr = vm.get("date_range", {})
    parts.append(f"# {hub} LMP — {dr.get('start', '?')} to {dr.get('end', '?')}")

    markets = [("da", "DA"), ("rt", "RT"), ("dart", "DART")]
    price_cols = [
        ("lmp_total", "Total"),
        ("lmp_system_energy_price", "Energy"),
        ("lmp_congestion_price", "Cong"),
    ]

    # Collect all dates across markets
    all_dates: set[str] = set()
    for mkt_key, _ in markets:
        section = vm.get(mkt_key)
        if section and section.get("daily_summary"):
            all_dates.update(section["daily_summary"].keys())
    dates_sorted = sorted(all_dates)

    # --- Daily Summary: one table per price component, DA/RT/DART as columns ---
    for pcol, plabel in price_cols:
        parts.append(f"\n## Daily Summary — {plabel} ($/MWh)")
        headers = ["Date"]
        for _, mlabel in markets:
            headers.extend([f"{mlabel} OnPk", f"{mlabel} OffPk", f"{mlabel} Flat"])

        rows = []
        for d in dates_sorted:
            row = [d]
            for mkt_key, _ in markets:
                section = vm.get(mkt_key)
                daily = section.get("daily_summary", {}) if section else {}
                p = daily.get(d, {}).get(pcol, {})
                row.append(_fmt2(p.get("on_peak")))
                row.append(_fmt2(p.get("off_peak")))
                row.append(_fmt2(p.get("flat")))
            rows.append(row)
        if rows:
            parts.append(_table(headers, rows))

    # --- Hourly Detail: one pivoted table per price component, DA/RT/DART rows per date ---
    for pcol, plabel in price_cols:
        parts.append(f"\n## Hourly Detail — {plabel} ($/MWh)")
        headers = ["Date", "Mkt"] + [f"HE{h}" for h in range(1, 25)]

        rows = []
        for d in dates_sorted:
            for mkt_key, mlabel in markets:
                section = vm.get(mkt_key)
                if not section:
                    continue
                hourly = section.get("hourly", [])
                # Build lookup for this date
                by_hour: dict[int, float | None] = {}
                for rec in hourly:
                    if rec["date"] == d:
                        by_hour[rec["hour_ending"]] = rec.get(pcol)
                row = [d, mlabel]
                for h in range(1, 25):
                    row.append(_fmt2(by_hour.get(h)))
                rows.append(row)
        if rows:
            parts.append(_table(headers, rows))

    return "\n".join(parts)


def format_lgbm_qr_forecast_results(vm: dict) -> str:
    """Format the LightGBM QR single-day forecast view model as markdown."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    mi = vm.get("model_info", {})
    best = mi.get("best_params", {})
    parts.append(
        f"# LightGBM QR Forecast - {vm.get('forecast_date', '?')}"
        f" (n_estimators={best.get('n_estimators', '?')},"
        f" max_depth={best.get('max_depth', '?')},"
        f" learning_rate={best.get('learning_rate', '?')},"
        f" {mi.get('n_features', '?')} features,"
        f" {mi.get('n_train_samples', '?')} samples)"
    )

    summary = vm.get("summary", {})
    bands_list = vm.get("bands", [])
    bands = {b["band"]: b for b in bands_list if "band" in b}
    parts.append("\n## Summary ($/MWh)")
    headers = ["Period", "Fcst", "Actual", "Error", "P10", "P25", "P50", "P75", "P90"]
    rows = []
    for pkey, label in [("on_peak", "OnPeak"), ("off_peak", "OffPeak"), ("flat", "Flat")]:
        s = summary.get(pkey, {})
        row = [
            label,
            _fmt2(s.get("forecast")),
            _fmt2(s.get("actual")),
            _fmt2(s.get("error")),
        ]
        for b in ["P10", "P25", "P50", "P75", "P90"]:
            row.append(_fmt2(bands.get(b, {}).get(label)))
        rows.append(row)
    parts.append(_table(headers, rows))

    hourly = vm.get("hourly", [])
    if hourly:
        parts.append("\n## Hourly Detail ($/MWh)")
        has_act = any(hr.get("actual") is not None for hr in hourly)
        headers = ["HE", "Period", "Fcst"]
        if has_act:
            headers.extend(["Actual", "Error"])
        headers.extend(["P10", "P90"])
        rows = []
        for hr in hourly:
            row = [
                hr["hour"],
                "on" if hr["period"] == "on_peak" else "off",
                _fmt2(hr.get("forecast")),
            ]
            if has_act:
                row.extend([_fmt2(hr.get("actual")), _fmt2(hr.get("error"))])
            q = hr.get("quantiles", {})
            row.extend([_fmt2(q.get("P10")), _fmt2(q.get("P90"))])
            rows.append(row)
        parts.append(_table(headers, rows))

    importances = mi.get("feature_importances", [])
    if importances:
        parts.append("\n## Top Features (by gain)")
        headers = ["Rank", "Feature", "Importance"]
        rows = [[i + 1, f["feature"], _fmt(f["importance"], 4)] for i, f in enumerate(importances)]
        parts.append(_table(headers, rows))

    shap_importances = mi.get("shap_importances", [])
    if shap_importances:
        parts.append("\n## Top SHAP Features (P50)")
        headers = ["Rank", "Feature", "Importance"]
        rows = [[i + 1, f["feature"], _fmt(f["importance"], 4)] for i, f in enumerate(shap_importances)]
        parts.append(_table(headers, rows))

    return "\n".join(parts)


def format_lgbm_qr_strip_forecast_results(vm: dict) -> str:
    """Format the LightGBM QR strip forecast view model as markdown."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    n_days = len(vm.get("forecast_dates", []))
    parts.append(
        f"# LightGBM QR Strip Forecast - ref: {vm.get('reference_date', '?')}"
        f" ({n_days} days)"
    )

    day_type_models = vm.get("model_info", {}).get("day_type_models", {})
    if day_type_models:
        parts.append("\n## Day-Type Models")
        headers = [
            "Day Type",
            "n_estimators",
            "max_depth",
            "learning_rate",
            "Features",
            "Train Samples",
            "Train End",
        ]
        rows = []
        for day_type in sorted(day_type_models.keys()):
            info = day_type_models[day_type]
            bp = info.get("best_params", {})
            rows.append([
                day_type,
                bp.get("n_estimators", "-"),
                bp.get("max_depth", "-"),
                _fmt(bp.get("learning_rate"), 3),
                info.get("n_features", "-"),
                info.get("n_train_samples", "-"),
                info.get("train_end", "-"),
            ])
        parts.append(_table(headers, rows))

    strip = vm.get("strip", [])
    if strip:
        parts.append("\n## Strip Summary ($/MWh)")
        headers = [
            "Date",
            "D+",
            "OnPk Fcst",
            "OnPk P10",
            "OnPk P90",
            "OffPk Fcst",
            "OffPk P10",
            "OffPk P90",
            "Flat Fcst",
        ]
        rows = []
        for d in strip:
            s = d.get("summary", {})
            b = d.get("bands", {})
            rows.append([
                d["date"],
                d.get("offset", "-"),
                _fmt2(s.get("on_peak", {}).get("forecast")),
                _fmt2(b.get("P10", {}).get("on_peak")),
                _fmt2(b.get("P90", {}).get("on_peak")),
                _fmt2(s.get("off_peak", {}).get("forecast")),
                _fmt2(b.get("P10", {}).get("off_peak")),
                _fmt2(b.get("P90", {}).get("off_peak")),
                _fmt2(s.get("flat", {}).get("forecast")),
            ])
        parts.append(_table(headers, rows))

    for d in strip:
        hourly = d.get("hourly", [])
        if not hourly:
            continue
        label = f"D+{d.get('offset', '?')}: {d['date']}"
        has_bands = "p10" in hourly[0]
        parts.append(f"\n## {label} - Hourly Detail ($/MWh)")
        headers = ["HE", "Period", "Fcst"]
        if has_bands:
            headers.extend(["P10", "P90"])
        rows = []
        for hr in hourly:
            row = [
                hr["hour"],
                "on" if hr["period"] == "on_peak" else "off",
                _fmt2(hr.get("forecast")),
            ]
            if has_bands:
                row.extend([_fmt2(hr.get("p10")), _fmt2(hr.get("p90"))])
            rows.append(row)
        parts.append(_table(headers, rows))

    return "\n".join(parts)


# ── Forecast Results ────────────────────────────────────────────────


def format_supply_stack_forecast_results(vm: dict) -> str:
    """Format the supply stack forecast results view model as markdown."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    cfg = vm.get("model_config", {})
    parts.append(
        f"# Supply Stack Forecast - {vm.get('forecast_date', '?')}"
        f" (region={cfg.get('region', '?')}, preset={cfg.get('region_preset') or 'none'})"
    )
    if not vm.get("has_actuals"):
        parts.append("\n*Actuals not yet available.*")

    summary = vm.get("summary", {})
    if summary:
        parts.append("\n## Period Summary ($/MWh)")
        headers = ["Period", "Forecast", "Actual", "Error"]
        rows = []
        for pkey, plabel in [("on_peak", "On-Peak"), ("off_peak", "Off-Peak"), ("flat", "Flat")]:
            s = summary.get(pkey, {})
            rows.append([
                plabel,
                _fmt2(s.get("forecast")),
                _fmt2(s.get("actual")),
                _fmt2(s.get("error")),
            ])
        parts.append(_table(headers, rows))

    hourly = vm.get("hourly", [])
    if hourly:
        parts.append("\n## Hourly Detail")
        has_act = any(hr.get("actual") is not None for hr in hourly)
        headers = [
            "HE",
            "Pd",
            "Fcst",
            "MargFuel",
            "MargHR",
            "ResvMW",
            "Stack%",
            "NetLoad",
            "Gas",
            "P10",
            "P90",
        ]
        if has_act:
            headers.insert(3, "Actual")
            headers.insert(4, "Error")

        rows = []
        for hr in hourly:
            q = hr.get("quantiles", {})
            row = [
                hr["hour"],
                "on" if hr["period"] == "on_peak" else "off",
                _fmt2(hr.get("forecast")),
            ]
            if has_act:
                row.extend([_fmt2(hr.get("actual")), _fmt2(hr.get("error"))])
            row.extend(
                [
                    hr.get("marginal_fuel", "-"),
                    _fmt(hr.get("marginal_heat_rate"), 3),
                    _fmt2(hr.get("reserve_margin_mw")),
                    _fmt2(hr.get("stack_position_pct")),
                    _fmt2(hr.get("net_load_mw")),
                    _fmt(hr.get("gas_price_usd_mmbtu"), 4),
                    _fmt2(q.get("P10")),
                    _fmt2(q.get("P90")),
                ]
            )
            rows.append(row)
        parts.append(_table(headers, rows))

    bands = vm.get("bands", [])
    if bands:
        parts.append("\n## Quantile Bands ($/MWh)")
        headers = ["Band"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
        rows = []
        for b in bands:
            row = [b.get("band")]
            for h in range(1, 25):
                row.append(_fmt2(b.get(f"HE{h}")))
            row.extend([_fmt2(b.get("OnPeak")), _fmt2(b.get("OffPeak")), _fmt2(b.get("Flat"))])
            rows.append(row)
        parts.append(_table(headers, rows))

    metrics = vm.get("metrics", {})
    if metrics:
        parts.append("\n## Error Metrics")
        parts.append(f"- MAE: {_fmt2(metrics.get('mae'))}")
        parts.append(f"- RMSE: {_fmt2(metrics.get('rmse'))}")
        parts.append(f"- MAPE: {_fmt2(metrics.get('mape'))}%")

    return "\n".join(parts)


# ── Validation Results ─────────────────────────────────────────────


def format_supply_stack_validation_results(vm: dict) -> str:
    """Format supply stack validation view model as markdown."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    status = vm.get("overall_status", "?").upper()
    parts.append(
        f"# Supply Stack Validation — {vm.get('forecast_date', '?')}"
        f" [{status}: {vm.get('checks_passed', 0)} pass, {vm.get('checks_failed', 0)} fail]"
    )

    # Input quality
    input_checks = vm.get("input_quality", [])
    if input_checks:
        parts.append("\n## Input Quality")
        headers = ["Check", "Status", "Detail"]
        rows = [[c["check"], c["status"], c.get("detail", "")] for c in input_checks]
        parts.append(_table(headers, rows))

    # Stack invariants
    stack_checks = vm.get("stack_invariants", [])
    if stack_checks:
        parts.append("\n## Stack Invariants")
        headers = ["Check", "Status", "Detail"]
        rows = [[c["check"], c["status"], c.get("detail", "")] for c in stack_checks]
        parts.append(_table(headers, rows))

    # Sensitivity
    sensitivity = vm.get("sensitivity", [])
    if sensitivity:
        parts.append("\n## Sensitivity Analysis ($/MWh delta from base)")
        headers = ["Scenario", "On-Peak", "Off-Peak", "Flat"]
        rows = []
        for s in sensitivity:
            rows.append([
                s.get("scenario", "?"),
                _fmt2(s.get("on_peak_delta")),
                _fmt2(s.get("off_peak_delta")),
                _fmt2(s.get("flat_delta")),
            ])
        parts.append(_table(headers, rows))

    # Forecast summary
    summary = vm.get("forecast_summary", {})
    if summary and "error" not in summary:
        parts.append("\n## Forecast Summary")
        parts.append(f"- Date: {summary.get('forecast_date', '?')}")
        parts.append(f"- Actuals available: {summary.get('has_actuals', False)}")
        parts.append(f"- On-Peak avg: {_fmt2(summary.get('on_peak_avg_price'))} $/MWh")
        parts.append(f"- Off-Peak avg: {_fmt2(summary.get('off_peak_avg_price'))} $/MWh")
        parts.append(f"- Flat avg: {_fmt2(summary.get('flat_avg_price'))} $/MWh")

        fuel_dist = summary.get("marginal_fuel_distribution", {})
        if fuel_dist:
            parts.append("\n### Marginal Fuel Distribution")
            headers = ["Fuel", "Hours (%)"]
            rows = [[fuel, f"{pct}%"] for fuel, pct in sorted(fuel_dist.items(), key=lambda x: -x[1])]
            parts.append(_table(headers, rows))

        metrics = summary.get("metrics")
        if metrics:
            parts.append("\n### Error Metrics")
            parts.append(f"- MAE: {_fmt2(metrics.get('mae'))}")
            parts.append(f"- RMSE: {_fmt2(metrics.get('rmse'))}")
            parts.append(f"- MAPE: {_fmt2(metrics.get('mape'))}%")

    return "\n".join(parts)


def format_like_day_forecast_results(vm: dict) -> str:
    """Format the like-day forecast results view model as markdown."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    parts.append(
        f"# Like-Day DA Forecast — {vm.get('forecast_date', '?')}"
        f" (ref: {vm.get('reference_date', '?')}, {vm.get('n_analogs_used', '?')} analogs)"
    )
    if not vm.get("has_actuals"):
        parts.append("\n*Actuals not yet available.*")

    # --- Analog Days ---
    analogs = vm.get("analogs")
    if analogs:
        parts.append("\n## Analog Days")
        headers = ["Rank", "Date", "Distance", "Similarity", "Weight"]
        rows = []
        for a in analogs:
            rows.append([
                a.get("rank", "-"),
                a.get("date", "-"),
                _fmt(a.get("distance"), 4),
                _fmt(a.get("similarity"), 4),
                _fmt(a.get("weight"), 4),
            ])
        parts.append(_table(headers, rows))

    # --- Period Summary ---
    summary = vm.get("summary", {})
    if summary:
        parts.append("\n## Period Summary ($/MWh)")
        headers = ["Period", "Forecast", "Actual", "Error"]
        rows = []
        for pkey, plabel in [("on_peak", "On-Peak"), ("off_peak", "Off-Peak"), ("flat", "Flat")]:
            s = summary.get(pkey, {})
            rows.append([plabel, _fmt2(s.get("forecast")), _fmt2(s.get("actual")), _fmt2(s.get("error"))])
        parts.append(_table(headers, rows))

    # --- Hourly Detail ---
    hourly = vm.get("hourly", [])
    if hourly:
        parts.append("\n## Hourly Detail ($/MWh)")
        headers = ["HE", "Period", "Fcst", "Actual", "Error", "Severity", "P25", "P75", "OOB"]
        rows = []
        for hr in hourly:
            rows.append([
                hr["hour"],
                "on" if hr["period"] == "on_peak" else "off",
                _fmt2(hr.get("forecast")),
                _fmt2(hr.get("actual")),
                _fmt2(hr.get("error")),
                hr.get("error_severity") or "-",
                _fmt2(hr.get("p25")),
                _fmt2(hr.get("p75")),
                "YES" if hr.get("outside_iqr") else "",
            ])
        parts.append(_table(headers, rows))

    # --- Quantile Bands ---
    bands = vm.get("bands", [])
    if bands:
        parts.append("\n## Quantile Bands ($/MWh)")
        headers = ["Band"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
        rows = []
        for b in bands:
            row = [b["band"]]
            for h in range(1, 25):
                row.append(_fmt2(b.get(f"HE{h}")))
            for sc in ["OnPeak", "OffPeak", "Flat"]:
                row.append(_fmt2(b.get(sc)))
            rows.append(row)
        parts.append(_table(headers, rows))

    # --- Diffs ---
    diffs = vm.get("diffs", {})
    if diffs:
        parts.append("\n## Forecast vs Actual Diff ($/MWh)")
        for dk, dv in diffs.items():
            headers = [""] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
            row = [dk.replace("_", " ").title()]
            for h in range(1, 25):
                row.append(_fmt2(dv.get(f"HE{h}")))
            for sc in ["OnPeak", "OffPeak", "Flat"]:
                row.append(_fmt2(dv.get(sc)))
            parts.append(_table(headers, [row]))

    # --- Quantile Coverage ---
    qc = vm.get("quantile_coverage", {})
    if qc:
        parts.append("\n## Quantile Coverage")
        oob = qc.get("hours_outside_iqr", [])
        if oob:
            parts.append(f"- Hours outside IQR: {', '.join(f'HE{h}' for h in oob)}")
        parts.append(f"- 80% coverage: {_fmt2(qc.get('coverage_80'))}")
        parts.append(f"- 90% coverage: {_fmt2(qc.get('coverage_90'))}")
        parts.append(f"- 98% coverage: {_fmt2(qc.get('coverage_98'))}")
        parts.append(f"- Sharpness (90%): {_fmt2(qc.get('sharpness_90'))} $/MWh")

    return "\n".join(parts)


# ── Meteologica DA Forecast ──────────────────────────────────────────


def format_meteologica_da_forecast(vm: dict) -> str:
    """Format Meteologica DA price forecast in the same style as like-day results."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    parts.append(
        f"# Meteologica DA Forecast — {vm.get('forecast_date', '?')}"
    )
    if vm.get("execution_timestamp"):
        parts.append(f"\n*Execution: {vm['execution_timestamp']}*")

    # Period Summary
    summary = vm.get("summary", {})
    if summary:
        parts.append("\n## Period Summary ($/MWh)")
        headers = ["Period", "Forecast"]
        rows = []
        for pkey, plabel in [("on_peak", "On-Peak"), ("off_peak", "Off-Peak"), ("flat", "Flat")]:
            s = summary.get(pkey, {})
            rows.append([plabel, _fmt2(s.get("forecast"))])
        parts.append(_table(headers, rows))

    # Hourly Detail
    hourly = vm.get("hourly", [])
    if hourly:
        parts.append("\n## Hourly Detail ($/MWh)")
        headers = ["HE", "Period", "Forecast"]
        rows = []
        for hr in hourly:
            rows.append([
                hr["hour"],
                "on" if hr["period"] == "on_peak" else "off",
                _fmt2(hr.get("forecast")),
            ])
        parts.append(_table(headers, rows))

    return "\n".join(parts)


# ── Load Forecast Vintages ──────────────────────────────────────────


def format_load_forecast_vintages(vm: dict) -> str:
    """Format load forecast vintages as markdown."""
    return _format_vintage_view(vm, value_label="Load (MW)", title="Load Forecast Vintages")


def format_generation_forecast_vintages(vm: dict) -> str:
    """Format solar/wind generation forecast vintages as markdown."""
    ftype = vm.get("forecast_type", "generation").title()
    return _format_vintage_view(vm, value_label=f"{ftype} (MW)", title=f"{ftype} Forecast Vintages")


def _format_vintage_view(vm: dict, value_label: str, title: str) -> str:
    """Shared formatter for load / solar / wind vintage views."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    regions = vm.get("regions", [])
    vintage_order = vm.get("vintage_order", [])
    parts.append(f"# {title}")
    parts.append(f"\nRegions: {', '.join(regions)}")
    parts.append(f"Vintages: {', '.join(vintage_order)}")

    by_region = vm.get("by_region", {})
    for region in regions:
        rdata = by_region.get(region)
        if not rdata:
            continue

        parts.append(f"\n## {region}")
        fdates = rdata.get("forecast_dates", [])
        if fdates:
            parts.append(f"Forecast dates: {', '.join(fdates)}")

        # --- Source summaries ---
        sources = rdata.get("sources", {})
        for src_key, src_label in [("pjm", "PJM"), ("meteologica", "Meteologica")]:
            src = sources.get(src_key)
            if not src:
                continue

            parts.append(f"\n### {src_label} — Period Averages ({value_label})")
            vintages = src.get("vintages_present", [])
            by_date = src.get("by_date", {})

            # One table per period type
            for period_key, period_label in [("on_peak", "OnPeak"), ("off_peak", "OffPeak"), ("flat", "Flat")]:
                headers = ["Date"] + vintages
                rows = []
                for d in sorted(by_date.keys()):
                    row = [d]
                    for v in vintages:
                        val = by_date[d].get(v, {}).get(period_key)
                        row.append(_fmt0(val))
                    rows.append(row)
                if rows:
                    parts.append(f"\n**{period_label}**")
                    parts.append(_table(headers, rows))

        # --- Vintage Deltas ---
        vdeltas = rdata.get("vintage_deltas", {})
        for src_key, src_label in [("pjm", "PJM"), ("meteologica", "Meteologica")]:
            src_deltas = vdeltas.get(src_key)
            if not src_deltas:
                continue

            parts.append(f"\n### {src_label} — Vintage Deltas (MW)")
            headers = ["Pair", "Date", "OnPk", "OffPk", "Flat", "Peak HE", "Peak MW"]
            rows = []
            for pair_key, pair_data in src_deltas.items():
                by_date_d = pair_data.get("by_date", {})
                for d in sorted(by_date_d.keys()):
                    dd = by_date_d[d]
                    ph = dd.get("peak_hour_change") or {}
                    rows.append([
                        pair_key, d,
                        _fmt0(dd.get("on_peak_mw")),
                        _fmt0(dd.get("off_peak_mw")),
                        _fmt0(dd.get("flat_mw")),
                        ph.get("hour_ending", "-"),
                        _fmt0(ph.get("delta_mw")),
                    ])
                # Overall row
                ov = pair_data.get("overall", {})
                oph = ov.get("peak_hour_change") or {}
                rows.append([
                    pair_key, "OVERALL",
                    _fmt0(ov.get("on_peak_mw")),
                    _fmt0(ov.get("off_peak_mw")),
                    _fmt0(ov.get("flat_mw")),
                    oph.get("hour_ending", "-"),
                    _fmt0(oph.get("delta_mw")),
                ])
            if rows:
                parts.append(_table(headers, rows))

        # --- PJM vs Meteologica Spread ---
        spread = rdata.get("pjm_vs_meteologica", {})
        if spread:
            parts.append(f"\n### PJM vs Meteologica Spread (MW, PJM - Meteo)")
            headers = ["Vintage", "Date", "OnPk", "OffPk", "Flat"]
            rows = []
            for v_label, v_data in spread.items():
                by_date_s = v_data.get("by_date", {})
                for d in sorted(by_date_s.keys()):
                    ds = by_date_s[d]
                    rows.append([
                        v_label, d,
                        _fmt0(ds.get("on_peak_mw")),
                        _fmt0(ds.get("off_peak_mw")),
                        _fmt0(ds.get("flat_mw")),
                    ])
                ov = v_data.get("overall", {})
                rows.append([
                    v_label, "OVERALL",
                    _fmt0(ov.get("on_peak_mw")),
                    _fmt0(ov.get("off_peak_mw")),
                    _fmt0(ov.get("flat_mw")),
                ])
            if rows:
                parts.append(_table(headers, rows))

    return "\n".join(parts)


# ── Outage Term Bible ───────────────────────────────────────────────


def format_outage_term_bible(vm: dict) -> str:
    """Format the outage term bible view model as markdown."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    parts.append(f"# Outage Term Bible — {vm.get('reference_date', '?')} ({vm.get('current_month', '?')})")

    # --- Current Levels & Context ---
    otypes = vm.get("outage_types", {})
    if otypes:
        parts.append("\n## Current Levels & Seasonal Context")
        headers = [
            "Type", "Current MW", "Month Avg", "Std", "Min", "Max",
            "Percentile", "Z-Score", "7d Trend", "7d Delta MW",
        ]
        rows = []
        for tkey, tdata in otypes.items():
            mc = tdata.get("month_context", {})
            tr = tdata.get("trend_7d", {})
            rows.append([
                tkey.replace("_", " ").title(),
                _fmt0(tdata.get("current_mw")),
                _fmt0(mc.get("avg")),
                _fmt0(mc.get("std")),
                _fmt0(mc.get("min")),
                _fmt0(mc.get("max")),
                _fmt2(mc.get("percentile")),
                _fmt2(mc.get("z_score")),
                tr.get("direction", "-"),
                _fmt0(tr.get("delta_mw")),
            ])
        parts.append(_table(headers, rows))

    # --- Year-over-Year ---
    if otypes:
        parts.append(f"\n## Year-over-Year — {vm.get('current_month', '?')} Average (MW)")
        # Gather all years across types
        all_years: set[int] = set()
        for tdata in otypes.values():
            all_years.update(tdata.get("year_over_year", {}).keys())
        years_sorted = sorted(all_years)

        if years_sorted:
            headers = ["Type"] + [str(y) for y in years_sorted] + ["YoY Delta"]
            rows = []
            for tkey, tdata in otypes.items():
                yoy = tdata.get("year_over_year", {})
                row = [tkey.replace("_", " ").title()]
                for y in years_sorted:
                    row.append(_fmt0(yoy.get(y) if isinstance(y, int) else yoy.get(int(y))))
                row.append(_fmt0(tdata.get("yoy_delta")))
                rows.append(row)
            parts.append(_table(headers, rows))

    # --- Heatmap (total_outages only) ---
    heatmap = vm.get("heatmap", {}).get("total_outages")
    if heatmap:
        parts.append("\n## Monthly Heatmap — Total Outages (MW avg)")
        years = heatmap.get("years", [])
        matrix = heatmap.get("matrix", {})
        month_abbrs = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        headers = ["Year"] + month_abbrs
        rows = []
        for y in years:
            row = [y]
            ydata = matrix.get(y) or matrix.get(str(y), {})
            for m in range(1, 13):
                row.append(_fmt0(ydata.get(m) or ydata.get(str(m))))
            rows.append(row)
        if rows:
            parts.append(_table(headers, rows))

    # --- Current Month Daily ---
    cmd = vm.get("current_month_daily", {}).get("total_outages")
    if cmd:
        years = cmd.get("years", [])
        daily = cmd.get("daily", {})
        if years and daily:
            parts.append(f"\n## {vm.get('current_month', '?')} Daily — Total Outages (MW)")
            headers = ["Day"] + [str(y) for y in years]
            rows = []
            for day in sorted(daily.keys(), key=int):
                row = [day]
                ddata = daily[day]
                for y in years:
                    row.append(_fmt0(ddata.get(y) or ddata.get(str(y))))
                rows.append(row)
            parts.append(_table(headers, rows))

    return "\n".join(parts)


# ── Fuel Mix 7-Day Lookback ────────────────────────────────────────


# Key fuels shown in the daily summary table (skip minor fuels to stay compact)
_SUMMARY_FUELS = ["gas", "coal", "nuclear", "solar", "wind", "total"]
# Fuels shown as individual hourly pivot tables
_HOURLY_FUELS = ["gas", "coal", "nuclear", "solar", "wind", "total"]
# Ramp fuels (dispatchable)
_RAMP_FUELS = ["gas", "coal"]


def format_fuel_mix_7day(vm: dict) -> str:
    """Format the fuel mix 7-day lookback view model as markdown.

    Produces a combined daily summary table for key fuels, pivoted hourly
    detail per fuel, and ramp tables for dispatchable fuels.
    """
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    dr = vm.get("date_range", {})
    parts.append(f"# PJM Fuel Mix — {dr.get('start', '?')} to {dr.get('end', '?')}")

    daily = vm.get("daily_summary", {})
    dates_sorted = sorted(daily.keys())

    # --- Daily Summary ---
    if daily:
        parts.append("\n## Daily Summary (MW avg)")
        headers = ["Date"]
        for fuel in _SUMMARY_FUELS:
            label = fuel.title() if fuel != "total" else "Total"
            headers.extend([f"{label} OnPk", f"{label} OffPk", f"{label} Flat"])

        rows = []
        for d in dates_sorted:
            row = [d]
            for fuel in _SUMMARY_FUELS:
                f = daily[d].get(fuel, {})
                row.append(_fmt0(f.get("on_peak")))
                row.append(_fmt0(f.get("off_peak")))
                row.append(_fmt0(f.get("flat")))
            rows.append(row)
        parts.append(_table(headers, rows))

    # --- Hourly Detail (pivoted per fuel) ---
    hourly = vm.get("hourly", [])
    if hourly:
        for fuel in _HOURLY_FUELS:
            label = fuel.title() if fuel != "total" else "Total"
            hdrs, rows = _pivot_hourly(hourly, value_key=fuel, decimals=0)
            parts.append(f"\n## Hourly Detail — {label} (MW)")
            parts.append(_table(hdrs, rows))

    # --- Ramps (hour-over-hour MW change) ---
    ramps = vm.get("ramps", [])
    if ramps:
        for fuel in _RAMP_FUELS:
            label = fuel.title()
            hdrs, rows = _pivot_hourly(ramps, value_key=fuel, decimals=0)
            parts.append(f"\n## {label} Ramps (MW/hr change)")
            parts.append(_table(hdrs, rows))

    return "\n".join(parts)


# ── Transmission Outages ───────────────────────────────────────────


def format_transmission_outages(vm: dict) -> str:
    """Format the transmission outages view model as markdown.

    Two sections: regional summary table and notable individual outages.
    """
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    parts.append(f"# Transmission Outages — {vm.get('reference_date', '?')}")
    parts.append(f"\nActive/Approved ≥230 kV: **{vm.get('total_active', '?')}** outages")

    # --- Regional Summary ---
    regional = vm.get("regional_summary", [])
    if regional:
        parts.append("\n## Regional Summary")
        headers = [
            "Region", "Total", "Lines", "Equip",
            "765kV", "500kV", "345kV", "230kV",
            "Risk", "Longest Out", "Soonest Return",
        ]
        rows = []
        for r in regional:
            rows.append([
                r["region"],
                r["total"],
                r.get("path_count") or "-",
                r.get("capacity_count") or "-",
                r["count_765kv"] or "-",
                r["count_500kv"] or "-",
                r["count_345kv"] or "-",
                r["count_230kv"] or "-",
                r["risk_flagged"] or "-",
                f"{r['longest_out_days']}d" if r.get("longest_out_days") else "-",
                f"{r['soonest_return_days']}d" if r.get("soonest_return_days") is not None else "-",
            ])
        parts.append(_table(headers, rows))

    # --- Notable Outages ---
    notable = vm.get("notable_outages", [])
    if notable:
        parts.append(f"\n## Notable Outages ({len(notable)})")
        headers = [
            "Tags", "Region", "Facility", "Type", "kV", "Route",
            "Started", "Est Return", "Days Out", "Days Left", "Cause",
        ]
        rows = []
        for n in notable:
            # Build route display: FROM→TO for lines, station for equipment
            if n.get("from_station") and n.get("to_station"):
                route = f"{n['from_station']}→{n['to_station']}"
            elif n.get("station"):
                route = n["station"]
            else:
                route = "-"

            rows.append([
                ", ".join(n["tags"]),
                n["region"],
                n.get("facility", "")[:40],  # truncate long facility names
                n.get("equip_category", n.get("equip", "")),
                n["kv"],
                route,
                n.get("started", "-"),
                n.get("est_return", "-"),
                n.get("days_out", "-"),
                n.get("days_to_return") if n.get("days_to_return") is not None else "overdue",
                n.get("cause", "")[:35],  # truncate long cause strings
            ])
        parts.append(_table(headers, rows))

    # --- Recently Cancelled ---
    cancelled = vm.get("recently_cancelled", [])
    if cancelled:
        parts.append(f"\n## Recently Cancelled ({len(cancelled)}, last 7 days)")
        headers = [
            "Region", "Facility", "Type", "kV", "Route",
            "Was Sched Start", "Was Sched End", "Cancelled", "Cause",
        ]
        rows = []
        for c in cancelled:
            # Build route display: FROM→TO for lines, station for equipment
            if c.get("from_station") and c.get("to_station"):
                route = f"{c['from_station']}→{c['to_station']}"
            elif c.get("station"):
                route = c["station"]
            else:
                route = "-"

            rows.append([
                c["region"],
                c.get("facility", "")[:40],
                c.get("equip_category", c.get("equip", "")),
                c["kv"],
                route,
                c.get("was_scheduled_start", "-"),
                c.get("was_scheduled_end", "-"),
                c.get("cancelled_date", "-"),
                c.get("cause", "")[:35],
            ])
        parts.append(_table(headers, rows))

    return "\n".join(parts)


# ── Outage Forecast Vintages ───────────────────────────────────────


def format_outages_forecast_vintages(vm: dict) -> str:
    """Format the outage forecast vintage view model as markdown.

    One vintage table per outage type. Rows = execution dates (vintages),
    columns = forecast dates. Matches the HTML heatmap layout.
    """
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    region = vm.get("region", "?")
    parts.append(f"# Generation Outage Forecast — {region}")

    vintage_dates = vm.get("vintage_dates", [])
    forecast_dates = vm.get("forecast_dates", [])

    if not vintage_dates or not forecast_dates:
        parts.append("\nNo vintage data available.")
        return "\n".join(parts)

    # Short date labels for forecast date columns
    fd_labels = []
    for fd in forecast_dates:
        try:
            fd_labels.append(pd.Timestamp(fd).strftime("%a %m/%d"))
        except Exception:
            fd_labels.append(fd)

    outage_types = vm.get("outage_types", {})
    for _col, ot_data in outage_types.items():
        label = ot_data.get("label", _col)
        delta = ot_data.get("delta_vs_prior")
        matrix = ot_data.get("matrix", {})

        delta_str = ""
        if delta is not None:
            sign = "+" if delta > 0 else ""
            delta_str = f" (vs 24h ago: {sign}{_fmt0(delta)} MW)"

        parts.append(f"\n## {label}{delta_str}")

        headers = ["Vintage", "Label"] + fd_labels
        rows = []
        for vd in vintage_dates:
            vd_data = matrix.get(vd, {})
            row_label = vd_data.get("label", vd)
            row = [vd, row_label]
            for fd in forecast_dates:
                row.append(_fmt0(vd_data.get(fd)))
            rows.append(row)

        parts.append(_table(headers, rows))

    return "\n".join(parts)


# ── Like-Day Strip Forecast Results ────────────────────────────────


# ── ICE Power Intraday ────────────────────────────────────────────


def format_ice_power_intraday(vm: dict) -> str:
    """Format the ICE power intraday view model as markdown.

    Sections: session summary, settlement history, and (optional) intraday tape.
    """
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    parts.append("# ICE PJM Power — Settlements & Intraday Tape")

    # --- Session Summary (cross-product comparison) ---
    summary = vm.get("session_summary")
    if summary:
        parts.append("\n## Session Summary")
        # Collect all product names across all delivery dates
        all_products: list[str] = []
        for entry in summary:
            for p in entry.get("products", {}):
                if p not in all_products:
                    all_products.append(p)
        all_products.sort()

        headers = ["Delivery"] + [f"{p} VWAP" for p in all_products] + [f"{p} Vol" for p in all_products]
        rows = []
        for entry in summary:
            prods = entry.get("products", {})
            row = [entry["delivery_date"]]
            for p in all_products:
                row.append(_fmt2(prods[p]["vwap"]) if p in prods else "-")
            for p in all_products:
                row.append(_fmt0(prods[p]["volume"]) if p in prods else "-")
            rows.append(row)
        parts.append(_table(headers, rows))

    # --- Settlement History ---
    settles = vm.get("settlements")
    if settles:
        dr = settles.get("date_range", {})
        products = settles.get("products", [])
        parts.append(f"\n## Settlement History ({dr.get('start', '?')} to {dr.get('end', '?')})")

        # Product metadata summary
        product_meta = settles.get("product_meta", {})
        if product_meta:
            meta_lines = []
            for p, meta in product_meta.items():
                peak = meta.get("peak_type", "unknown")
                meta_lines.append(f"- **{p}** ({meta.get('symbol', '?')}): {peak}")
            parts.append("\n" + "\n".join(meta_lines))

        # Cross-product daily matrix
        matrix = settles.get("daily_matrix", [])
        if matrix and products:
            headers = ["Trade Date", "Delivery"] + products
            rows = []
            for entry in matrix:
                # Use first product's delivery date as representative
                delivery = None
                for p in products:
                    dd = entry.get(f"{p}_delivery")
                    if dd:
                        delivery = dd
                        break
                row = [entry["trade_date"], delivery or "—"]
                for p in products:
                    row.append(_fmt2(entry.get(p)))
                rows.append(row)
            parts.append("\n### Daily Settle ($/MWh)")
            parts.append(_table(headers, rows))

        # Per-product detail
        by_product = settles.get("by_product", {})
        for product in products:
            prows = by_product.get(product, [])
            if not prows:
                continue
            meta = product_meta.get(product, {})
            peak_label = f" ({meta.get('peak_type', '')})" if meta.get("peak_type") else ""
            parts.append(f"\n### {product}{peak_label}")
            headers = ["Trade Date", "Delivery", "Settle", "Prior", "Chg", "VWAP", "High", "Low", "Volume"]
            rows = []
            for r in prows:
                rows.append([
                    r["trade_date"],
                    r.get("delivery_date") or "—",
                    _fmt2(r.get("settle")),
                    _fmt2(r.get("prior_settle")),
                    _fmt2(r.get("settle_vs_prior")),
                    _fmt2(r.get("vwap")),
                    _fmt2(r.get("high")),
                    _fmt2(r.get("low")),
                    _fmt0(r.get("volume")),
                ])
            parts.append(_table(headers, rows))

    # --- Intraday Tape ---
    intraday = vm.get("intraday")
    if intraday:
        dr = intraday.get("date_range", {})
        products = intraday.get("products", [])
        parts.append(f"\n## Intraday Tape ({dr.get('start', '?')} to {dr.get('end', '?')})")

        intraday_meta = intraday.get("product_meta", {})
        by_product = intraday.get("by_product", {})
        for product in products:
            pdata = by_product.get(product, {})
            meta = intraday_meta.get(product, {})
            peak_label = f" [{meta.get('peak_type', '')}]" if meta.get("peak_type") else ""
            for d in sorted(pdata.keys()):
                session = pdata[d]
                dd = session.get("delivery_date")
                delivery_label = f" → delivery {dd}" if dd else ""
                parts.append(
                    f"\n### {product}{peak_label} — {d}{delivery_label}"
                    f" (Open: {_fmt2(session.get('session_open'))}"
                    f"  High: {_fmt2(session.get('session_high'))}"
                    f"  Low: {_fmt2(session.get('session_low'))}"
                    f"  Last: {_fmt2(session.get('session_last'))}"
                    f"  VWAP: {_fmt2(session.get('session_vwap'))}"
                    f"  Vol: {_fmt0(session.get('session_volume'))})"
                )
                snaps = session.get("snapshots", [])
                if snaps:
                    # Handle compressed rows (time_start/time_end) and
                    # regular rows (time_et)
                    headers = ["Time ET", "Bid", "Ask", "Spread", "Last", "VWAP", "Vol", "Chg"]
                    rows = []
                    for s in snaps:
                        if "time_start" in s:
                            time_label = f"{s['time_start']}–{s['time_end']}"
                        else:
                            time_label = s["time_et"]
                        rows.append([
                            time_label,
                            _fmt2(s.get("bid")),
                            _fmt2(s.get("ask")),
                            _fmt2(s.get("spread")),
                            _fmt2(s.get("last_px")),
                            _fmt2(s.get("vwap")),
                            _fmt0(s.get("volume")),
                            _fmt2(s.get("last_chg")),
                        ])
                    parts.append(_table(headers, rows))

    return "\n".join(parts)


# ── Like-Day Strip Forecast Results ────────────────────────────────


def format_like_day_strip_forecast_results(vm: dict) -> str:
    """Format the like-day strip forecast view model as markdown."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    n_days = len(vm.get("forecast_dates", []))
    parts.append(
        f"# Like-Day Strip Forecast — ref: {vm.get('reference_date', '?')}"
        f" ({n_days} days, {vm.get('n_analogs_used', '?')} analogs)"
    )

    # --- Analog Days ---
    analogs = vm.get("analogs")
    if analogs:
        parts.append("\n## Analog Days")
        headers = ["Rank", "Date", "Distance", "Similarity", "Weight"]
        rows = []
        for a in analogs:
            rows.append([
                a.get("rank", "-"),
                a.get("date", "-"),
                _fmt(a.get("distance"), 4),
                _fmt(a.get("similarity"), 4),
                _fmt(a.get("weight"), 4),
            ])
        parts.append(_table(headers, rows))

    # --- Strip Summary ---
    strip = vm.get("strip", [])
    if strip:
        parts.append("\n## Strip Summary ($/MWh)")
        headers = [
            "Date", "D+",
            "OnPk Fcst", "OnPk P10", "OnPk P90",
            "OffPk Fcst", "OffPk P10", "OffPk P90",
            "Flat Fcst",
        ]
        has_any_actuals = any(d.get("has_actuals") for d in strip)
        if has_any_actuals:
            headers.extend(["OnPk Act", "OffPk Act", "Flat Act"])

        rows = []
        for d in strip:
            s = d.get("summary", {})
            b = d.get("bands", {})
            row = [
                d["date"],
                d.get("offset", "-"),
                _fmt2(s.get("on_peak", {}).get("forecast")),
                _fmt2(b.get("P10", {}).get("on_peak")),
                _fmt2(b.get("P90", {}).get("on_peak")),
                _fmt2(s.get("off_peak", {}).get("forecast")),
                _fmt2(b.get("P10", {}).get("off_peak")),
                _fmt2(b.get("P90", {}).get("off_peak")),
                _fmt2(s.get("flat", {}).get("forecast")),
            ]
            if has_any_actuals:
                row.extend([
                    _fmt2(s.get("on_peak", {}).get("actual")),
                    _fmt2(s.get("off_peak", {}).get("actual")),
                    _fmt2(s.get("flat", {}).get("actual")),
                ])
            rows.append(row)
        parts.append(_table(headers, rows))

    # --- Hourly Detail per day ---
    for d in strip:
        hourly = d.get("hourly", [])
        if not hourly:
            continue

        label = f"D+{d.get('offset', '?')}: {d['date']}"
        has_act = d.get("has_actuals", False)
        has_bands = "p10" in hourly[0]

        parts.append(f"\n## {label} — Hourly Detail ($/MWh)")
        headers = ["HE", "Period", "Fcst"]
        if has_act:
            headers.extend(["Actual", "Error"])
        if has_bands:
            headers.extend(["P10", "P90"])

        rows = []
        for hr in hourly:
            row = [
                hr["hour"],
                "on" if hr["period"] == "on_peak" else "off",
                _fmt2(hr.get("forecast")),
            ]
            if has_act:
                row.extend([_fmt2(hr.get("actual")), _fmt2(hr.get("error"))])
            if has_bands:
                row.extend([_fmt2(hr.get("p10")), _fmt2(hr.get("p90"))])
            rows.append(row)
        parts.append(_table(headers, rows))

    return "\n".join(parts)


# ── LASSO QR Forecast ────────────────────────────────────────────────


def format_lasso_qr_forecast_results(vm: dict) -> str:
    """Format the LASSO QR single-day forecast view model as markdown."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    mi = vm.get("model_info", {})
    parts.append(
        f"# LASSO QR Forecast — {vm.get('forecast_date', '?')}"
        f" (alpha={mi.get('alpha', '?')}, {mi.get('n_features', '?')} features,"
        f" {mi.get('n_train_samples', '?')} samples)"
    )

    # Summary
    summary = vm.get("summary", {})
    bands_list = vm.get("bands", [])
    bands = {b["band"]: b for b in bands_list if "band" in b}
    parts.append("\n## Summary ($/MWh)")
    headers = ["Period", "Fcst", "Actual", "Error", "P10", "P25", "P50", "P75", "P90"]
    rows = []
    for pkey, label in [("on_peak", "OnPeak"), ("off_peak", "OffPeak"), ("flat", "Flat")]:
        s = summary.get(pkey, {})
        row = [
            label,
            _fmt2(s.get("forecast")),
            _fmt2(s.get("actual")),
            _fmt2(s.get("error")),
        ]
        for b in ["P10", "P25", "P50", "P75", "P90"]:
            row.append(_fmt2(bands.get(b, {}).get(label)))
        rows.append(row)
    parts.append(_table(headers, rows))

    # Hourly
    hourly = vm.get("hourly", [])
    if hourly:
        parts.append("\n## Hourly Detail ($/MWh)")
        has_act = any(hr.get("actual") is not None for hr in hourly)
        headers = ["HE", "Period", "Fcst"]
        if has_act:
            headers.extend(["Actual", "Error"])
        headers.extend(["P10", "P90"])
        rows = []
        for hr in hourly:
            row = [
                hr["hour"],
                "on" if hr["period"] == "on_peak" else "off",
                _fmt2(hr.get("forecast")),
            ]
            if has_act:
                row.extend([_fmt2(hr.get("actual")), _fmt2(hr.get("error"))])
            q = hr.get("quantiles", {})
            row.extend([_fmt2(q.get("P10")), _fmt2(q.get("P90"))])
            rows.append(row)
        parts.append(_table(headers, rows))

    # Feature importances
    importances = mi.get("feature_importances", [])
    if importances:
        parts.append("\n## Top Features (by |coefficient|)")
        headers = ["Rank", "Feature", "Importance"]
        rows = [[i + 1, f["feature"], _fmt(f["importance"], 4)] for i, f in enumerate(importances)]
        parts.append(_table(headers, rows))

    return "\n".join(parts)


def format_lasso_qr_strip_forecast_results(vm: dict) -> str:
    """Format the LASSO QR strip forecast view model as markdown."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    mi = vm.get("model_info", {})
    n_days = len(vm.get("forecast_dates", []))
    parts.append(
        f"# LASSO QR Strip Forecast — ref: {vm.get('reference_date', '?')}"
        f" ({n_days} days, alpha={mi.get('alpha', '?')},"
        f" {mi.get('n_features', '?')} features)"
    )

    # Strip summary
    strip = vm.get("strip", [])
    if strip:
        parts.append("\n## Strip Summary ($/MWh)")
        headers = [
            "Date", "D+",
            "OnPk Fcst", "OnPk P10", "OnPk P90",
            "OffPk Fcst", "OffPk P10", "OffPk P90",
            "Flat Fcst",
        ]
        rows = []
        for d in strip:
            s = d.get("summary", {})
            b = d.get("bands", {})
            rows.append([
                d["date"],
                d.get("offset", "-"),
                _fmt2(s.get("on_peak", {}).get("forecast")),
                _fmt2(b.get("P10", {}).get("on_peak")),
                _fmt2(b.get("P90", {}).get("on_peak")),
                _fmt2(s.get("off_peak", {}).get("forecast")),
                _fmt2(b.get("P10", {}).get("off_peak")),
                _fmt2(b.get("P90", {}).get("off_peak")),
                _fmt2(s.get("flat", {}).get("forecast")),
            ])
        parts.append(_table(headers, rows))

    # Hourly detail per day
    for d in strip:
        hourly = d.get("hourly", [])
        if not hourly:
            continue
        label = f"D+{d.get('offset', '?')}: {d['date']}"
        has_bands = "p10" in hourly[0]
        parts.append(f"\n## {label} — Hourly Detail ($/MWh)")
        headers = ["HE", "Period", "Fcst"]
        if has_bands:
            headers.extend(["P10", "P90"])
        rows = []
        for hr in hourly:
            row = [
                hr["hour"],
                "on" if hr["period"] == "on_peak" else "off",
                _fmt2(hr.get("forecast")),
            ]
            if has_bands:
                row.extend([_fmt2(hr.get("p10")), _fmt2(hr.get("p90"))])
            rows.append(row)
        parts.append(_table(headers, rows))

    return "\n".join(parts)


# ── Regional Congestion ───────────────────────────────────────────────


def format_regional_congestion(vm: dict) -> str:
    """Format the regional congestion view model as markdown.

    Three sections:
      1. Daily congestion heatmap — all hubs side-by-side, DA and RT
      2. Cross-hub congestion spreads — directional constraint signals
      3. Per-hub hourly congestion — pivoted HE1-24 detail for each hub
    """
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    dr = vm.get("date_range", {})
    parts.append(f"# Regional Congestion — {dr.get('start', '?')} to {dr.get('end', '?')}")

    hub_shorts = {"WESTERN HUB": "West", "AEP GEN HUB": "AEP",
                  "DOMINION HUB": "Dom", "EASTERN HUB": "East"}
    hubs = vm.get("hubs", [])

    # --- Section 1: Daily DA congestion heatmap (on-peak) ---
    daily = vm.get("daily_congestion", [])
    if daily:
        parts.append("\n## DA Congestion by Region — On-Peak ($/MWh)")
        headers = ["Date"] + [hub_shorts.get(h, h) + " OnPk" for h in hubs] + \
                  [hub_shorts.get(h, h) + " Flat" for h in hubs]
        rows = []
        for d in daily:
            row = [d["date"]]
            for h in hubs:
                short = hub_shorts.get(h, h)
                row.append(_fmt2(d.get(f"{short}_da_onpk")))
            for h in hubs:
                short = hub_shorts.get(h, h)
                row.append(_fmt2(d.get(f"{short}_da_flat")))
            rows.append(row)
        parts.append(_table(headers, rows))

    # --- Section 2: Daily RT congestion heatmap (on-peak) ---
    if daily:
        parts.append("\n## RT Congestion by Region — On-Peak ($/MWh)")
        headers = ["Date"] + [hub_shorts.get(h, h) + " OnPk" for h in hubs] + \
                  [hub_shorts.get(h, h) + " Flat" for h in hubs]
        rows = []
        for d in daily:
            row = [d["date"]]
            for h in hubs:
                short = hub_shorts.get(h, h)
                row.append(_fmt2(d.get(f"{short}_rt_onpk")))
            for h in hubs:
                short = hub_shorts.get(h, h)
                row.append(_fmt2(d.get(f"{short}_rt_flat")))
            rows.append(row)
        parts.append(_table(headers, rows))

    # --- Section 3: Cross-hub congestion spreads (DA on-peak) ---
    da_spread = vm.get("da_congestion_spread", [])
    if da_spread:
        parts.append("\n## DA Congestion Spreads — On-Peak ($/MWh)")
        headers = ["Date", "East-West", "Dom-AEP", "East-Dom",
                   "West OnPk", "AEP OnPk", "Dom OnPk", "East OnPk"]
        rows = []
        for d in da_spread:
            rows.append([
                d["date"],
                _fmt2(d.get("east_west_onpk")),
                _fmt2(d.get("dom_aep_onpk")),
                _fmt2(d.get("east_dom_onpk")),
                _fmt2(d.get("West_onpk")),
                _fmt2(d.get("AEP_onpk")),
                _fmt2(d.get("Dom_onpk")),
                _fmt2(d.get("East_onpk")),
            ])
        parts.append(_table(headers, rows))

    # --- Section 4: RT congestion spreads ---
    rt_spread = vm.get("rt_congestion_spread", [])
    if rt_spread:
        parts.append("\n## RT Congestion Spreads — On-Peak ($/MWh)")
        headers = ["Date", "East-West", "Dom-AEP", "East-Dom",
                   "West OnPk", "AEP OnPk", "Dom OnPk", "East OnPk"]
        rows = []
        for d in rt_spread:
            rows.append([
                d["date"],
                _fmt2(d.get("east_west_onpk")),
                _fmt2(d.get("dom_aep_onpk")),
                _fmt2(d.get("east_dom_onpk")),
                _fmt2(d.get("West_onpk")),
                _fmt2(d.get("AEP_onpk")),
                _fmt2(d.get("Dom_onpk")),
                _fmt2(d.get("East_onpk")),
            ])
        parts.append(_table(headers, rows))

    # --- Section 5: Per-hub hourly congestion detail ---
    profiles = vm.get("hub_profiles", [])
    for profile in profiles:
        hub = profile.get("hub", "?")
        short = hub_shorts.get(hub, hub)

        # DA hourly pivot
        da_hourly = profile.get("da_hourly", [])
        if da_hourly:
            parts.append(f"\n## {short} — DA Congestion Hourly ($/MWh)")
            h, r = _pivot_hourly(da_hourly, value_key="cong", decimals=2)
            if r:
                parts.append(_table(h, r))

        # RT hourly pivot
        rt_hourly = profile.get("rt_hourly", [])
        if rt_hourly:
            parts.append(f"\n## {short} — RT Congestion Hourly ($/MWh)")
            h, r = _pivot_hourly(rt_hourly, value_key="cong", decimals=2)
            if r:
                parts.append(_table(h, r))

    return "\n".join(parts)


# ── Gas Prices ────────────────────────────────────────────────────


def format_gas_prices(vm: dict) -> str:
    """Format the gas prices view model as markdown.

    Compact table of daily next-day cash gas with DoD changes and basis.
    """
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    dr = vm.get("date_range", {})
    parts.append(f"# ICE Gas Prices — Next-Day Cash ({dr.get('start', '?')} to {dr.get('end', '?')})")

    hubs = vm.get("hubs", [])
    daily = vm.get("daily_prices", [])
    if not daily:
        parts.append("\nNo data.")
        return "\n".join(parts)

    # Build headers: Date | M3 | HH | Z5S | AGT | M3 DoD | HH DoD | M3-HH | Z5S-HH
    headers = ["Date"] + hubs
    headers += [f"{h} DoD" for h in hubs]
    headers += [f"{h}-HH" for h in hubs if h != "HH"]

    rows = []
    for entry in daily:
        row = [entry["date"]]
        for h in hubs:
            row.append(_fmt(entry.get(h), decimals=3))
        for h in hubs:
            row.append(_fmt(entry.get(f"{h}_dod"), decimals=3))
        for h in hubs:
            if h != "HH":
                row.append(_fmt(entry.get(f"{h}-HH"), decimals=3))
        rows.append(row)

    parts.append(_table(headers, rows))

    # Latest summary line
    latest = vm.get("latest", {})
    if latest:
        summary_parts = []
        for h in hubs:
            val = latest.get(h)
            dod = latest.get(f"{h}_dod")
            if val is not None:
                dod_str = f" ({'+' if dod and dod > 0 else ''}{_fmt(dod, 3)})" if dod is not None else ""
                summary_parts.append(f"**{h}** ${_fmt(val, 3)}{dod_str}")
        parts.append(f"\n**Latest ({latest.get('date', '?')}):** " + " | ".join(summary_parts))

    return "\n".join(parts)
