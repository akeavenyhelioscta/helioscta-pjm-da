"use client";

import { useEffect, useState, useMemo, useCallback } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceArea,
  Legend,
} from "recharts";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface ForecastRow {
  forecast_execution_datetime: string;
  forecast_execution_date: string;
  forecast_date: string;
  hour_ending: number;
  wind_forecast: number;
  forecast_rank: number;
}

interface ActualRow {
  date: string;
  hour_ending: number;
  wind: number;
}

interface ApiResponse {
  target_date: string;
  forecasts: ForecastRow[];
  actuals: ActualRow[];
  available_dates: string[];
}

interface PerformanceEntry {
  hour: number;
  forecast: number | null;
  actual: number | null;
  error: number | null;
  errorPct: number | null;
}

interface Metrics {
  mae: number | null;
  mape: number | null;
  bias: number | null;
  daWindowError: number | null;
  peakHourError: number | null;
}

interface VintageEntry {
  hour: number;
  [key: string]: number | null;
}

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const ONPEAK_HOURS = new Set([8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]);
const HOURS = Array.from({ length: 24 }, (_, i) => i + 1);
const DA_WINDOW_HOURS = new Set([10, 11]);
const VINTAGE_COLORS = [
  "#6b7280", "#9ca3af", "#d1d5db", "#a78bfa", "#818cf8",
  "#60a5fa", "#38bdf8", "#22d3ee", "#2dd4bf", "#34d399",
  "#4ade80", "#a3e635", "#facc15", "#fb923c", "#f87171",
];

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function fmt(v: number | null, decimals = 0): string {
  if (v == null) return "\u2014";
  return v.toFixed(decimals).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function cellBg(val: number | null, min: number, max: number): string {
  if (val == null) return "";
  const range = max - min;
  if (range === 0) return "";
  const t = (val - min) / range;
  if (t > 0.85) return "bg-red-900/30";
  if (t > 0.7) return "bg-orange-900/20";
  if (t < 0.15) return "bg-blue-900/30";
  return "";
}

function fmtVintageLabel(ts: string): string {
  const d = new Date(ts);
  const mon = d.toLocaleString("en-US", { month: "short", timeZone: "America/New_York" });
  const day = d.toLocaleDateString("en-US", { day: "numeric", timeZone: "America/New_York" });
  const hh = d.toLocaleString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "America/New_York" });
  return `${mon} ${day} ${hh} ET`;
}

function isDAWindowVintage(ts: string): boolean {
  const d = new Date(ts);
  const estHour = parseInt(
    d.toLocaleString("en-US", { hour: "numeric", hour12: false, timeZone: "America/New_York" })
  );
  return estHour >= 9 && estHour <= 10;
}

function computeMetrics(entries: PerformanceEntry[]): Metrics {
  const valid = entries.filter((e) => e.error != null);
  if (valid.length === 0) return { mae: null, mape: null, bias: null, daWindowError: null, peakHourError: null };

  const mae = valid.reduce((s, e) => s + Math.abs(e.error!), 0) / valid.length;
  const mape = valid.filter((e) => e.actual != null && e.actual !== 0).length > 0
    ? (valid.filter((e) => e.actual != null && e.actual !== 0).reduce((s, e) => s + Math.abs(e.errorPct!), 0) /
       valid.filter((e) => e.actual != null && e.actual !== 0).length)
    : null;
  const bias = valid.reduce((s, e) => s + e.error!, 0) / valid.length;

  const daEntries = valid.filter((e) => DA_WINDOW_HOURS.has(e.hour));
  const daWindowError = daEntries.length > 0
    ? daEntries.reduce((s, e) => s + Math.abs(e.error!), 0) / daEntries.length
    : null;

  let peakHour = 0;
  let peakVal = -Infinity;
  for (const e of entries) {
    if (e.actual != null && e.actual > peakVal) {
      peakVal = e.actual;
      peakHour = e.hour;
    }
  }
  const peakEntry = entries.find((e) => e.hour === peakHour);
  const peakHourError = peakEntry?.error ?? null;

  return { mae, mape, bias, daWindowError, peakHourError };
}

/* ------------------------------------------------------------------ */
/*  Tooltip Components                                                 */
/* ------------------------------------------------------------------ */

function PerformanceTooltip({
  active, payload, label,
}: {
  active?: boolean;
  payload?: { dataKey: string; value: number; color: string }[];
  label?: number;
}) {
  if (!active || !payload || payload.length === 0) return null;
  const isDA = DA_WINDOW_HOURS.has(label as number);
  return (
    <div className="rounded-lg border border-gray-700 bg-[#1f2937] px-3 py-2 text-xs shadow-xl">
      <p className="mb-1.5 font-semibold text-gray-200">
        HE {label} {isDA && <span className="text-amber-400 ml-1">(DA Window)</span>}
      </p>
      {payload.filter((p) => p.value != null).map((p) => (
        <p key={p.dataKey} className="flex items-center gap-1.5">
          <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: p.color }} />
          <span className="text-gray-400">{p.dataKey}:</span>
          <span className="text-gray-200">{fmt(p.value)} MW</span>
        </p>
      ))}
    </div>
  );
}

function VintageTooltip({
  active, payload, label,
}: {
  active?: boolean;
  payload?: { dataKey: string; value: number; color: string }[];
  label?: number;
}) {
  if (!active || !payload || payload.length === 0) return null;
  const isDA = DA_WINDOW_HOURS.has(label as number);
  const entries = payload.filter((p) => p.value != null).sort((a, b) => b.value - a.value);
  return (
    <div className="rounded-lg border border-gray-700 bg-[#1f2937] px-3 py-2 text-xs shadow-xl max-h-64 overflow-y-auto">
      <p className="mb-1.5 font-semibold text-gray-200">
        HE {label} {isDA && <span className="text-amber-400 ml-1">(DA Window)</span>}
      </p>
      {entries.slice(0, 10).map((e) => (
        <p key={e.dataKey} className="flex items-center gap-1.5">
          <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: e.color }} />
          <span className="text-gray-400 truncate max-w-[120px]">{e.dataKey}:</span>
          <span className="text-gray-200">{fmt(e.value)} MW</span>
        </p>
      ))}
      {entries.length > 10 && (
        <p className="text-gray-500 mt-1">+{entries.length - 10} more</p>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Metric Card                                                        */
/* ------------------------------------------------------------------ */

function MetricCard({ label, value, unit, highlight }: {
  label: string;
  value: number | null;
  unit: string;
  highlight?: boolean;
}) {
  return (
    <div className={`rounded-lg border px-3 py-2 ${highlight ? "border-amber-700/50 bg-amber-900/10" : "border-gray-800 bg-[#0f1117]"}`}>
      <p className={`text-[10px] font-semibold uppercase tracking-wider ${highlight ? "text-amber-400" : "text-gray-500"}`}>
        {label}
      </p>
      <p className="mt-1 text-lg font-bold text-gray-100">
        {value != null ? fmt(value) : "\u2014"}
        <span className="ml-1 text-xs font-normal text-gray-500">{unit}</span>
      </p>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Main Component                                                     */
/* ------------------------------------------------------------------ */

export default function WindForecastPerformance() {
  /* --- state --- */
  const [targetDate, setTargetDate] = useState(() => {
    const y = new Date();
    y.setDate(y.getDate() - 1);
    return y.toISOString().slice(0, 10);
  });
  const [dateInput, setDateInput] = useState(targetDate);
  const [data, setData] = useState<ApiResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<"performance" | "evolution">("performance");
  const [selectedVintages, setSelectedVintages] = useState<Set<string>>(new Set());
  const [vintagesInitialized, setVintagesInitialized] = useState(false);

  /* --- fetch --- */
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);

    const params = new URLSearchParams({ target_date: targetDate });
    fetch(`/api/pjm-wind-forecast-performance?${params}`, { signal: controller.signal })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((json: ApiResponse) => {
        setData(json);
        if (!vintagesInitialized || true) {
          const execs = new Set<string>();
          for (const r of json.forecasts) {
            execs.add(r.forecast_execution_datetime);
          }
          const sorted = [...execs].sort().reverse();
          const daVintages = sorted.filter(isDAWindowVintage);
          const latest = sorted.slice(0, 4);
          const combined = new Set([...latest, ...daVintages.slice(0, 2)]);
          setSelectedVintages(combined);
          setVintagesInitialized(true);
        }
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError("Failed to load data");
      })
      .finally(() => setLoading(false));

    return () => controller.abort();
  }, [targetDate]);

  /* --- apply handler --- */
  const applyDate = useCallback(() => {
    setTargetDate(dateInput);
    setVintagesInitialized(false);
  }, [dateInput]);

  /* --- derived: performance data --- */
  const performanceData = useMemo(() => {
    if (!data) return [];

    const forecastMap = new Map<number, number>();
    for (const f of data.forecasts) {
      if (f.forecast_rank === 1) {
        forecastMap.set(Number(f.hour_ending), Number(f.wind_forecast));
      }
    }

    const actualMap = new Map<number, number>();
    for (const a of data.actuals) {
      actualMap.set(Number(a.hour_ending), Number(a.wind));
    }

    const entries: PerformanceEntry[] = HOURS.map((h) => {
      const fc = forecastMap.get(h) ?? null;
      const act = actualMap.get(h) ?? null;
      const err = fc != null && act != null ? act - fc : null;
      const errPct = err != null && fc != null && fc !== 0 ? (err / fc) * 100 : null;
      return { hour: h, forecast: fc, actual: act, error: err, errorPct: errPct };
    });
    return entries;
  }, [data]);

  /* --- derived: metrics --- */
  const metrics = useMemo(() => computeMetrics(performanceData), [performanceData]);

  /* --- derived: vintage data --- */
  const vintageData = useMemo(() => {
    if (!data) return { vintages: [] as string[], chartData: [] as VintageEntry[] };

    const byExec = new Map<string, Map<number, number>>();
    for (const f of data.forecasts) {
      const exec = f.forecast_execution_datetime;
      if (!byExec.has(exec)) byExec.set(exec, new Map());
      byExec.get(exec)!.set(Number(f.hour_ending), Number(f.wind_forecast));
    }

    const vintages = [...byExec.keys()].sort();

    const chartData: VintageEntry[] = HOURS.map((h) => {
      const entry: VintageEntry = { hour: h };
      for (const v of vintages) {
        if (selectedVintages.has(v)) {
          const label = fmtVintageLabel(v);
          entry[label] = byExec.get(v)?.get(h) ?? null;
        }
      }
      const actual = data.actuals.find((a) => Number(a.hour_ending) === h);
      entry["Fuel Mix Actual"] = actual ? Number(actual.wind) : null;
      return entry;
    });

    return { vintages, chartData };
  }, [data, selectedVintages]);

  /* --- derived: all unique vintages --- */
  const allVintages = useMemo(() => {
    if (!data) return [];
    const set = new Set<string>();
    for (const f of data.forecasts) {
      set.add(f.forecast_execution_datetime);
    }
    return [...set].sort().reverse();
  }, [data]);

  /* --- vintage color map --- */
  const vintageColorMap = useMemo(() => {
    const map = new Map<string, string>();
    const selected = [...selectedVintages].sort();
    selected.forEach((v, i) => {
      if (isDAWindowVintage(v)) {
        map.set(fmtVintageLabel(v), "#f59e0b");
      } else {
        map.set(fmtVintageLabel(v), VINTAGE_COLORS[i % VINTAGE_COLORS.length]);
      }
    });
    return map;
  }, [selectedVintages]);

  /* --- Y domain helpers --- */
  function yDomain(entries: PerformanceEntry[]): [number, number] {
    let lo = Infinity, hi = -Infinity;
    for (const e of entries) {
      for (const v of [e.forecast, e.actual]) {
        if (v != null) {
          if (v < lo) lo = v;
          if (v > hi) hi = v;
        }
      }
    }
    if (lo === Infinity) return [0, 100];
    const pad = (hi - lo) * 0.05 || 5;
    return [Math.floor(Math.max(0, lo - pad)), Math.ceil(hi + pad)];
  }

  function yDomainVintage(chartData: VintageEntry[]): [number, number] {
    let lo = Infinity, hi = -Infinity;
    for (const entry of chartData) {
      for (const [k, v] of Object.entries(entry)) {
        if (k === "hour") continue;
        if (typeof v === "number") {
          if (v < lo) lo = v;
          if (v > hi) hi = v;
        }
      }
    }
    if (lo === Infinity) return [0, 100];
    const pad = (hi - lo) * 0.05 || 5;
    return [Math.floor(Math.max(0, lo - pad)), Math.ceil(hi + pad)];
  }

  /* ================================================================ */
  /*  Render                                                           */
  /* ================================================================ */

  return (
    <div className="space-y-6">
      {/* ---------- Header ---------- */}
      <div>
        <h2 className="text-lg font-semibold text-white">
          Wind Forecast Performance
        </h2>
        <p className="text-xs text-gray-500">
          Wind forecast vs Fuel Mix actuals for PJM wind generation
          {data && ` \u00b7 ${data.target_date}`}
        </p>
      </div>

      {/* ---------- Filters ---------- */}
      <div className="flex flex-wrap items-end gap-4">
        <div className="flex flex-col gap-1">
          <label className="text-[10px] font-bold uppercase tracking-wider text-gray-500">
            Target Date
          </label>
          <input
            type="date"
            value={dateInput}
            onChange={(e) => setDateInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && applyDate()}
            className="rounded-md border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-200 focus:border-gray-500 focus:outline-none"
          />
        </div>
        {data && data.available_dates.length > 0 && (
          <div className="flex flex-col gap-1">
            <label className="text-[10px] font-bold uppercase tracking-wider text-gray-500">
              Quick Select
            </label>
            <select
              value={dateInput}
              onChange={(e) => { setDateInput(e.target.value); setTargetDate(e.target.value); setVintagesInitialized(false); }}
              className="rounded-md border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-200 focus:border-gray-500 focus:outline-none"
            >
              {data.available_dates.map((d) => (
                <option key={d} value={d}>{d}</option>
              ))}
            </select>
          </div>
        )}
        <button
          onClick={applyDate}
          className="rounded-md bg-gray-700 px-4 py-1.5 text-sm font-medium text-gray-200 transition-colors hover:bg-gray-600"
        >
          Apply
        </button>
      </div>

      {/* ---------- View Tabs ---------- */}
      <div className="flex gap-1 rounded-lg bg-gray-800/40 p-1 w-fit">
        {(["performance", "evolution"] as const).map((v) => (
          <button
            key={v}
            onClick={() => setActiveView(v)}
            className={`rounded-md px-4 py-1.5 text-sm font-medium transition-colors ${
              activeView === v
                ? "bg-gray-700 text-white"
                : "text-gray-400 hover:text-gray-200"
            }`}
          >
            {v === "performance" ? "Forecast vs Actual" : "Forecast Evolution"}
          </button>
        ))}
      </div>

      {/* ---------- Loading / Error ---------- */}
      {loading && (
        <div className="h-80 animate-pulse rounded-xl border border-gray-800 bg-[#0f1117]" />
      )}
      {error && (
        <div className="flex items-center justify-center h-48">
          <div className="text-red-400">{error}</div>
        </div>
      )}

      {/* ============================================================ */}
      {/*  VIEW A: Performance (Forecast vs Actual)                     */}
      {/* ============================================================ */}
      {!loading && !error && data && activeView === "performance" && (
        <>
          {/* --- Chart + Metrics --- */}
          <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
            <div className="flex items-center justify-between mb-3">
              <p className="text-sm font-semibold text-gray-200">PJM Wind (RTO)</p>
              <span className="inline-block h-2 w-2 rounded-full bg-cyan-400" />
            </div>

            {/* Metrics row */}
            <div className="grid grid-cols-5 gap-2 mb-3">
              <MetricCard label="MAE" value={metrics.mae} unit="MW" />
              <MetricCard label="MAPE" value={metrics.mape} unit="%" />
              <MetricCard label="Bias" value={metrics.bias} unit="MW" />
              <MetricCard label="DA Window" value={metrics.daWindowError} unit="MW" highlight />
              <MetricCard label="Peak Hr" value={metrics.peakHourError} unit="MW" />
            </div>

            {/* Chart */}
            <ResponsiveContainer width="100%" height={350}>
              <LineChart data={performanceData.map((e) => ({
                hour: e.hour,
                "Wind Forecast": e.forecast,
                "Fuel Mix Actual": e.actual,
              }))}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <ReferenceArea x1={9.5} x2={11.5} fill="#f59e0b" fillOpacity={0.06} />
                <XAxis
                  dataKey="hour"
                  tick={{ fill: "#9ca3af", fontSize: 10 }}
                  label={{ value: "Hour Ending", position: "insideBottom", offset: -4, fill: "#6b7280", fontSize: 10 }}
                />
                <YAxis
                  domain={yDomain(performanceData)}
                  allowDataOverflow
                  tick={{ fill: "#9ca3af", fontSize: 10 }}
                  tickFormatter={(v: number) => fmt(v)}
                />
                <Tooltip content={<PerformanceTooltip />} />
                <Legend wrapperStyle={{ fontSize: "10px", color: "#9ca3af" }} />
                <Line type="monotone" dataKey="Wind Forecast" stroke="#22d3ee" strokeWidth={2} strokeDasharray="5 3" dot={false} isAnimationActive={false} />
                <Line type="monotone" dataKey="Fuel Mix Actual" stroke="#22c55e" strokeWidth={2.5} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* --- Performance Table --- */}
          <div className="overflow-x-auto rounded-xl border border-gray-800">
            <table className="text-sm border-collapse" style={{ minWidth: "1400px" }}>
              <thead>
                <tr>
                  <th className="sticky left-0 z-10 bg-[#0f1117] px-3 py-2 text-left text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
                    Metric
                  </th>
                  <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">Onpeak</th>
                  <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">Offpeak</th>
                  <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 border-r whitespace-nowrap">Flat</th>
                  {HOURS.map((h) => (
                    <th
                      key={h}
                      className={`px-1.5 py-2 text-center text-xs font-medium border-b border-gray-700 whitespace-nowrap ${
                        DA_WINDOW_HOURS.has(h) ? "text-amber-400 bg-amber-900/10" : ONPEAK_HOURS.has(h) ? "text-yellow-500" : "text-gray-500"
                      }`}
                    >
                      {h}
                      {DA_WINDOW_HOURS.has(h) && <span className="block text-[8px] text-amber-500">DA</span>}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(() => {
                  const rows = [
                    { label: "Wind Forecast", getValue: (e: PerformanceEntry) => e.forecast },
                    { label: "Fuel Mix Actual", getValue: (e: PerformanceEntry) => e.actual },
                    { label: "Error", getValue: (e: PerformanceEntry) => e.error },
                    { label: "Error %", getValue: (e: PerformanceEntry) => e.errorPct },
                  ];

                  const errorVals = performanceData.map((e) => e.error).filter((v) => v != null) as number[];
                  const errMin = errorVals.length > 0 ? Math.min(...errorVals) : 0;
                  const errMax = errorVals.length > 0 ? Math.max(...errorVals) : 1;

                  return rows.map((r, mi) => {
                    const isErrorRow = r.label === "Error" || r.label === "Error %";
                    const hourVals = performanceData.map((e) => r.getValue(e));

                    const onpeakVals = hourVals.filter((_, i) => ONPEAK_HOURS.has(i + 1)).filter((v) => v != null) as number[];
                    const offpeakVals = hourVals.filter((_, i) => !ONPEAK_HOURS.has(i + 1)).filter((v) => v != null) as number[];
                    const allVals = hourVals.filter((v) => v != null) as number[];
                    const onpeak = onpeakVals.length > 0 ? onpeakVals.reduce((a, b) => a + b, 0) / onpeakVals.length : null;
                    const offpeak = offpeakVals.length > 0 ? offpeakVals.reduce((a, b) => a + b, 0) / offpeakVals.length : null;
                    const flat = allVals.length > 0 ? allVals.reduce((a, b) => a + b, 0) / allVals.length : null;

                    return (
                      <tr
                        key={r.label}
                        className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${
                          mi % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                        }`}
                      >
                        <td className={`sticky left-0 z-10 bg-inherit px-3 py-1.5 text-xs font-medium whitespace-nowrap ${
                          isErrorRow ? "text-gray-400 italic" : "text-gray-300"
                        }`}>
                          {r.label}
                        </td>
                        <td className="px-2 py-1.5 text-center text-sm text-gray-300">
                          {fmt(onpeak, r.label === "Error %" ? 1 : 0)}{r.label === "Error %" && onpeak != null ? "%" : ""}
                        </td>
                        <td className="px-2 py-1.5 text-center text-sm text-gray-300">
                          {fmt(offpeak, r.label === "Error %" ? 1 : 0)}{r.label === "Error %" && offpeak != null ? "%" : ""}
                        </td>
                        <td className="px-2 py-1.5 text-center text-sm text-gray-300 border-r border-gray-700">
                          {fmt(flat, r.label === "Error %" ? 1 : 0)}{r.label === "Error %" && flat != null ? "%" : ""}
                        </td>
                        {hourVals.map((val, hi) => (
                          <td
                            key={hi}
                            className={`px-1.5 py-1.5 text-center text-sm whitespace-nowrap ${
                              DA_WINDOW_HOURS.has(hi + 1) ? "border-l border-amber-700/30" : ""
                            } ${
                              isErrorRow ? cellBg(val, errMin, errMax) + " text-gray-400" : "text-gray-300"
                            }`}
                          >
                            {fmt(val, r.label === "Error %" ? 1 : 0)}{r.label === "Error %" && val != null ? "%" : ""}
                          </td>
                        ))}
                      </tr>
                    );
                  });
                })()}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* ============================================================ */}
      {/*  VIEW B: Evolution (Vintage Convergence)                      */}
      {/* ============================================================ */}
      {!loading && !error && data && activeView === "evolution" && (
        <>
          {/* --- Vintage Selector --- */}
          <div className="space-y-2">
            <div className="flex items-center gap-3">
              <p className="text-xs font-semibold text-gray-400">Forecast Vintages</p>
              <button
                onClick={() => setSelectedVintages(new Set(allVintages))}
                className="text-[10px] text-gray-500 hover:text-gray-300"
              >
                All
              </button>
              <button
                onClick={() => setSelectedVintages(new Set())}
                className="text-[10px] text-gray-500 hover:text-gray-300"
              >
                None
              </button>
              <button
                onClick={() => setSelectedVintages(new Set(allVintages.filter(isDAWindowVintage)))}
                className="text-[10px] text-amber-500 hover:text-amber-300"
              >
                DA Window Only
              </button>
            </div>
            <div className="flex flex-wrap gap-1.5 max-h-24 overflow-y-auto">
              {allVintages.map((v) => {
                const isSelected = selectedVintages.has(v);
                const isDA = isDAWindowVintage(v);
                const label = fmtVintageLabel(v);
                const color = vintageColorMap.get(label) || "#6b7280";
                return (
                  <button
                    key={v}
                    onClick={() => {
                      setSelectedVintages((prev) => {
                        const next = new Set(prev);
                        if (next.has(v)) next.delete(v);
                        else next.add(v);
                        return next;
                      });
                    }}
                    className={`rounded px-2 py-0.5 text-[10px] font-medium transition-all ${
                      isSelected ? "text-white" : "text-gray-600 hover:text-gray-400"
                    }`}
                    style={
                      isSelected
                        ? { backgroundColor: color + "33", border: `1px solid ${color}` }
                        : { border: isDA ? "1px solid #92400e" : "1px solid transparent" }
                    }
                  >
                    {label} {isDA && "\u2605"}
                  </button>
                );
              })}
            </div>
          </div>

          {/* --- Single Chart --- */}
          <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
            <div className="flex items-center justify-between mb-3">
              <p className="text-sm font-semibold text-gray-200">PJM Wind (RTO) — Forecast Evolution</p>
              <span className="inline-block h-2 w-2 rounded-full bg-cyan-400" />
            </div>

            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={vintageData.chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <ReferenceArea x1={9.5} x2={11.5} fill="#f59e0b" fillOpacity={0.06} />
                <XAxis
                  dataKey="hour"
                  tick={{ fill: "#9ca3af", fontSize: 10 }}
                  label={{ value: "Hour Ending", position: "insideBottom", offset: -4, fill: "#6b7280", fontSize: 10 }}
                />
                <YAxis
                  domain={yDomainVintage(vintageData.chartData)}
                  allowDataOverflow
                  tick={{ fill: "#9ca3af", fontSize: 10 }}
                  tickFormatter={(v: number) => fmt(v)}
                />
                <Tooltip content={<VintageTooltip />} />
                {Object.keys(vintageData.chartData[0] || {})
                  .filter((k) => k !== "hour")
                  .map((k) => (
                    <Line
                      key={k}
                      type="monotone"
                      dataKey={k}
                      stroke={k === "Fuel Mix Actual" ? "#22c55e" : (vintageColorMap.get(k) || "#6b7280")}
                      strokeWidth={k === "Fuel Mix Actual" ? 2.5 : (vintageColorMap.get(k) === "#f59e0b" ? 2.5 : 1.5)}
                      strokeDasharray={k === "Fuel Mix Actual" ? undefined : (vintageColorMap.get(k) === "#f59e0b" ? undefined : "4 2")}
                      dot={false}
                      isAnimationActive={false}
                    />
                  ))}
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* --- Evolution Table --- */}
          <div className="overflow-x-auto rounded-xl border border-gray-800">
            <table className="text-sm border-collapse" style={{ minWidth: "1400px" }}>
              <thead>
                <tr>
                  <th className="sticky left-0 z-10 bg-[#0f1117] px-3 py-2 text-left text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
                    Vintage
                  </th>
                  <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">Onpeak</th>
                  <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">Offpeak</th>
                  <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 border-r whitespace-nowrap">Flat</th>
                  {HOURS.map((h) => (
                    <th
                      key={h}
                      className={`px-1.5 py-2 text-center text-xs font-medium border-b border-gray-700 whitespace-nowrap ${
                        DA_WINDOW_HOURS.has(h) ? "text-amber-400 bg-amber-900/10" : ONPEAK_HOURS.has(h) ? "text-yellow-500" : "text-gray-500"
                      }`}
                    >
                      {h}
                      {DA_WINDOW_HOURS.has(h) && <span className="block text-[8px] text-amber-500">DA</span>}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(() => {
                  const displayVintages = vintageData.vintages.filter((v) => selectedVintages.has(v));

                  const actualMap = new Map<number, number>();
                  for (const a of data.actuals) {
                    actualMap.set(Number(a.hour_ending), Number(a.wind));
                  }

                  let heatMin = Infinity, heatMax = -Infinity;
                  for (const f of data.forecasts) {
                    const v = Number(f.wind_forecast);
                    if (v < heatMin) heatMin = v;
                    if (v > heatMax) heatMax = v;
                  }
                  for (const [, v] of actualMap) {
                    if (v < heatMin) heatMin = v;
                    if (v > heatMax) heatMax = v;
                  }

                  const tableRows = [
                    ...displayVintages.map((v) => ({ type: "vintage" as const, key: v, label: fmtVintageLabel(v), isDA: isDAWindowVintage(v) })),
                    { type: "actual" as const, key: "actual", label: "Fuel Mix Actual", isDA: false },
                  ];

                  return tableRows.map((row, mi) => {
                    const hourVals: (number | null)[] = HOURS.map((h) => {
                      if (row.type === "actual") return actualMap.get(h) ?? null;
                      const match = data.forecasts.find(
                        (f) => f.forecast_execution_datetime === row.key && Number(f.hour_ending) === h
                      );
                      return match ? Number(match.wind_forecast) : null;
                    });

                    const onpeakVals = hourVals.filter((_, i) => ONPEAK_HOURS.has(i + 1)).filter((v) => v != null) as number[];
                    const offpeakVals = hourVals.filter((_, i) => !ONPEAK_HOURS.has(i + 1)).filter((v) => v != null) as number[];
                    const allValsArr = hourVals.filter((v) => v != null) as number[];
                    const onpeak = onpeakVals.length > 0 ? onpeakVals.reduce((a, b) => a + b, 0) / onpeakVals.length : null;
                    const offpeak = offpeakVals.length > 0 ? offpeakVals.reduce((a, b) => a + b, 0) / offpeakVals.length : null;
                    const flat = allValsArr.length > 0 ? allValsArr.reduce((a, b) => a + b, 0) / allValsArr.length : null;

                    return (
                      <tr
                        key={row.key}
                        className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${
                          row.type === "actual" ? "bg-green-900/10" : mi % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                        }`}
                      >
                        <td className={`sticky left-0 z-10 bg-inherit px-3 py-1.5 text-[10px] font-medium whitespace-nowrap ${
                          row.type === "actual" ? "text-green-400 font-semibold" : row.isDA ? "text-amber-400" : "text-gray-400"
                        }`}>
                          {row.label} {row.isDA && "\u2605"}
                        </td>
                        <td className="px-2 py-1.5 text-center text-sm text-gray-300">{fmt(onpeak)}</td>
                        <td className="px-2 py-1.5 text-center text-sm text-gray-300">{fmt(offpeak)}</td>
                        <td className="px-2 py-1.5 text-center text-sm text-gray-300 border-r border-gray-700">{fmt(flat)}</td>
                        {hourVals.map((val, hi) => (
                          <td
                            key={hi}
                            className={`px-1.5 py-1.5 text-center text-sm text-gray-300 whitespace-nowrap ${
                              cellBg(val, heatMin, heatMax)
                            } ${DA_WINDOW_HOURS.has(hi + 1) ? "border-l border-amber-700/30" : ""}`}
                          >
                            {fmt(val)}
                          </td>
                        ))}
                      </tr>
                    );
                  });
                })()}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
