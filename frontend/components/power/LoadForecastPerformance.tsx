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

interface RegionConfig {
  key: string;
  forecast: string;
  rtPrelim: string;
  display: string;
}

interface ForecastRow {
  forecast_execution_datetime: string;
  forecast_execution_date: string;
  forecast_date: string;
  hour_ending: number;
  region: string;
  forecast_load_mw: number;
  forecast_rank: number;
}

interface ActualRow {
  date: string;
  hour_ending: number;
  region: string;
  rt_load_mw: number;
}

interface ApiResponse {
  target_date: string;
  regions: RegionConfig[];
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

interface RegionMetrics {
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
// DA LMP prediction window: 9-10am EST = HE 10, 11
const DA_WINDOW_HOURS = new Set([10, 11]);
const VINTAGE_COLORS = [
  "#6b7280", "#9ca3af", "#d1d5db", "#a78bfa", "#818cf8",
  "#60a5fa", "#38bdf8", "#22d3ee", "#2dd4bf", "#34d399",
  "#4ade80", "#a3e635", "#facc15", "#fb923c", "#f87171",
];
const REGION_COLORS: Record<string, string> = {
  RTO: "#22c55e",
  MIDATL: "#3b82f6",
  SOUTH: "#f59e0b",
  WEST: "#a855f6",
};

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

function computeMetrics(entries: PerformanceEntry[]): RegionMetrics {
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
  let peakLoad = -Infinity;
  for (const e of entries) {
    if (e.actual != null && e.actual > peakLoad) {
      peakLoad = e.actual;
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

export default function LoadForecastPerformance() {
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
    fetch(`/api/pjm-load-forecast-performance?${params}`, { signal: controller.signal })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((json: ApiResponse) => {
        setData(json);
        // Auto-select up to 6 most recent vintages on first load
        if (!vintagesInitialized || true) {
          const execs = new Set<string>();
          for (const r of json.forecasts) {
            execs.add(r.forecast_execution_datetime);
          }
          const sorted = [...execs].sort().reverse();
          // Pick latest + DA-window vintages
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

  /* --- derived: performance data per region --- */
  const performanceByRegion = useMemo(() => {
    if (!data) return new Map<string, PerformanceEntry[]>();
    const map = new Map<string, PerformanceEntry[]>();

    for (const rc of data.regions) {
      // Get latest forecast (rank 1) for this region
      const forecastMap = new Map<number, number>();
      for (const f of data.forecasts) {
        if (f.region === rc.forecast && f.forecast_rank === 1) {
          forecastMap.set(Number(f.hour_ending), Number(f.forecast_load_mw));
        }
      }
      // Get actuals
      const actualMap = new Map<number, number>();
      for (const a of data.actuals) {
        if (a.region === rc.rtPrelim) {
          actualMap.set(Number(a.hour_ending), Number(a.rt_load_mw));
        }
      }
      // Build entries
      const entries: PerformanceEntry[] = HOURS.map((h) => {
        const fc = forecastMap.get(h) ?? null;
        const act = actualMap.get(h) ?? null;
        const err = fc != null && act != null ? act - fc : null;
        const errPct = err != null && fc != null && fc !== 0 ? (err / fc) * 100 : null;
        return { hour: h, forecast: fc, actual: act, error: err, errorPct: errPct };
      });
      map.set(rc.key, entries);
    }
    return map;
  }, [data]);

  /* --- derived: metrics per region --- */
  const metricsByRegion = useMemo(() => {
    const map = new Map<string, RegionMetrics>();
    for (const [key, entries] of performanceByRegion) {
      map.set(key, computeMetrics(entries));
    }
    return map;
  }, [performanceByRegion]);

  /* --- derived: vintages per region --- */
  const vintagesByRegion = useMemo(() => {
    if (!data) return new Map<string, { vintages: string[]; chartData: VintageEntry[] }>();
    const map = new Map<string, { vintages: string[]; chartData: VintageEntry[] }>();

    for (const rc of data.regions) {
      // Group by execution datetime
      const byExec = new Map<string, Map<number, number>>();
      for (const f of data.forecasts) {
        if (f.region !== rc.forecast) continue;
        const exec = f.forecast_execution_datetime;
        if (!byExec.has(exec)) byExec.set(exec, new Map());
        byExec.get(exec)!.set(Number(f.hour_ending), Number(f.forecast_load_mw));
      }

      const vintages = [...byExec.keys()].sort();

      // Build chart data
      const chartData: VintageEntry[] = HOURS.map((h) => {
        const entry: VintageEntry = { hour: h };
        for (const v of vintages) {
          if (selectedVintages.has(v)) {
            const label = fmtVintageLabel(v);
            entry[label] = byExec.get(v)?.get(h) ?? null;
          }
        }
        // Add actuals
        const actual = data.actuals.find(
          (a) => a.region === rc.rtPrelim && Number(a.hour_ending) === h
        );
        entry["RT Prelim"] = actual ? Number(actual.rt_load_mw) : null;
        return entry;
      });

      map.set(rc.key, { vintages, chartData });
    }
    return map;
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
        map.set(fmtVintageLabel(v), "#f59e0b"); // amber for DA window
      } else {
        map.set(fmtVintageLabel(v), VINTAGE_COLORS[i % VINTAGE_COLORS.length]);
      }
    });
    return map;
  }, [selectedVintages]);

  /* --- Y domain helper --- */
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
    return [Math.floor(lo - pad), Math.ceil(hi + pad)];
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
    return [Math.floor(lo - pad), Math.ceil(hi + pad)];
  }

  /* ================================================================ */
  /*  Render                                                           */
  /* ================================================================ */

  return (
    <div className="space-y-6">
      {/* ---------- Header ---------- */}
      <div>
        <h2 className="text-lg font-semibold text-white">
          Load Forecast Performance
        </h2>
        <p className="text-xs text-gray-500">
          Forecast vs RT Prelim actuals across PJM regions
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
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-80 animate-pulse rounded-xl border border-gray-800 bg-[#0f1117]" />
          ))}
        </div>
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
          {/* --- 2x2 Grid of Region Charts --- */}
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            {data.regions.map((rc) => {
              const entries = performanceByRegion.get(rc.key) || [];
              const metrics = metricsByRegion.get(rc.key);
              const domain = yDomain(entries);
              const chartData = entries.map((e) => ({
                hour: e.hour,
                Forecast: e.forecast,
                "RT Prelim": e.actual,
              }));
              const color = REGION_COLORS[rc.key] || "#6b7280";

              return (
                <div key={rc.key} className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
                  <div className="flex items-center justify-between mb-3">
                    <p className="text-sm font-semibold text-gray-200">{rc.display}</p>
                    <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: color }} />
                  </div>

                  {/* Mini metrics row */}
                  {metrics && (
                    <div className="grid grid-cols-5 gap-2 mb-3">
                      <MetricCard label="MAE" value={metrics.mae} unit="MW" />
                      <MetricCard label="MAPE" value={metrics.mape} unit="%" />
                      <MetricCard label="Bias" value={metrics.bias} unit="MW" />
                      <MetricCard label="DA Window" value={metrics.daWindowError} unit="MW" highlight />
                      <MetricCard label="Peak Hr" value={metrics.peakHourError} unit="MW" />
                    </div>
                  )}

                  {/* Chart */}
                  <ResponsiveContainer width="100%" height={280}>
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                      <ReferenceArea x1={9.5} x2={11.5} fill="#f59e0b" fillOpacity={0.06} />
                      <XAxis
                        dataKey="hour"
                        tick={{ fill: "#9ca3af", fontSize: 10 }}
                        label={{ value: "Hour Ending", position: "insideBottom", offset: -4, fill: "#6b7280", fontSize: 10 }}
                      />
                      <YAxis
                        domain={domain}
                        allowDataOverflow
                        tick={{ fill: "#9ca3af", fontSize: 10 }}
                        tickFormatter={(v: number) => fmt(v)}
                      />
                      <Tooltip content={<PerformanceTooltip />} />
                      <Legend wrapperStyle={{ fontSize: "10px", color: "#9ca3af" }} />
                      <Line type="monotone" dataKey="Forecast" stroke="#a78bfa" strokeWidth={2} strokeDasharray="5 3" dot={false} isAnimationActive={false} />
                      <Line type="monotone" dataKey="RT Prelim" stroke="#22c55e" strokeWidth={2.5} dot={false} isAnimationActive={false} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              );
            })}
          </div>

          {/* --- Performance Table --- */}
          <div className="overflow-x-auto rounded-xl border border-gray-800">
            <table className="text-sm border-collapse" style={{ minWidth: "1400px" }}>
              <thead>
                <tr>
                  <th className="sticky left-0 z-10 bg-[#0f1117] px-3 py-2 text-left text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
                    Region
                  </th>
                  <th className="sticky left-[100px] z-10 bg-[#0f1117] px-3 py-2 text-left text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
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
                {data.regions.flatMap((rc, ri) => {
                  const entries = performanceByRegion.get(rc.key) || [];
                  const rows = [
                    { label: "Forecast", getValue: (e: PerformanceEntry) => e.forecast },
                    { label: "RT Prelim", getValue: (e: PerformanceEntry) => e.actual },
                    { label: "Error", getValue: (e: PerformanceEntry) => e.error },
                    { label: "Error %", getValue: (e: PerformanceEntry) => e.errorPct },
                  ];

                  // Compute summary values
                  const errorVals = entries.map((e) => e.error).filter((v) => v != null) as number[];
                  const errMin = errorVals.length > 0 ? Math.min(...errorVals) : 0;
                  const errMax = errorVals.length > 0 ? Math.max(...errorVals) : 1;

                  return rows.map((r, mi) => {
                    const idx = ri * 4 + mi;
                    const isErrorRow = r.label === "Error" || r.label === "Error %";
                    const hourVals = entries.map((e) => r.getValue(e));

                    // Compute onpeak/offpeak/flat
                    const onpeakVals = hourVals.filter((_, i) => ONPEAK_HOURS.has(i + 1)).filter((v) => v != null) as number[];
                    const offpeakVals = hourVals.filter((_, i) => !ONPEAK_HOURS.has(i + 1)).filter((v) => v != null) as number[];
                    const allVals = hourVals.filter((v) => v != null) as number[];
                    const onpeak = onpeakVals.length > 0 ? onpeakVals.reduce((a, b) => a + b, 0) / onpeakVals.length : null;
                    const offpeak = offpeakVals.length > 0 ? offpeakVals.reduce((a, b) => a + b, 0) / offpeakVals.length : null;
                    const flat = allVals.length > 0 ? allVals.reduce((a, b) => a + b, 0) / allVals.length : null;

                    return (
                      <tr
                        key={`${rc.key}-${r.label}`}
                        className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${
                          idx % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                        }`}
                      >
                        {mi === 0 && (
                          <td
                            rowSpan={4}
                            className="sticky left-0 z-10 bg-inherit px-3 py-1.5 text-sm font-semibold text-white whitespace-nowrap align-top border-r border-gray-700"
                          >
                            {rc.display}
                          </td>
                        )}
                        <td className={`sticky left-[100px] z-10 bg-inherit px-3 py-1.5 text-xs font-medium whitespace-nowrap ${
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
                })}
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

          {/* --- 2x2 Grid of Region Charts --- */}
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            {data.regions.map((rc) => {
              const vData = vintagesByRegion.get(rc.key);
              if (!vData) return null;
              const { chartData } = vData;
              const domain = yDomainVintage(chartData);
              const color = REGION_COLORS[rc.key] || "#6b7280";

              // Build line keys (excluding "hour")
              const lineKeys = Object.keys(chartData[0] || {}).filter((k) => k !== "hour");

              return (
                <div key={rc.key} className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
                  <div className="flex items-center justify-between mb-3">
                    <p className="text-sm font-semibold text-gray-200">{rc.display}</p>
                    <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: color }} />
                  </div>

                  <ResponsiveContainer width="100%" height={300}>
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                      <ReferenceArea x1={9.5} x2={11.5} fill="#f59e0b" fillOpacity={0.06} />
                      <XAxis
                        dataKey="hour"
                        tick={{ fill: "#9ca3af", fontSize: 10 }}
                        label={{ value: "Hour Ending", position: "insideBottom", offset: -4, fill: "#6b7280", fontSize: 10 }}
                      />
                      <YAxis
                        domain={domain}
                        allowDataOverflow
                        tick={{ fill: "#9ca3af", fontSize: 10 }}
                        tickFormatter={(v: number) => fmt(v)}
                      />
                      <Tooltip content={<VintageTooltip />} />
                      {lineKeys.map((k) => (
                        <Line
                          key={k}
                          type="monotone"
                          dataKey={k}
                          stroke={k === "RT Prelim" ? "#22c55e" : (vintageColorMap.get(k) || "#6b7280")}
                          strokeWidth={k === "RT Prelim" ? 2.5 : (vintageColorMap.get(k) === "#f59e0b" ? 2.5 : 1.5)}
                          strokeDasharray={k === "RT Prelim" ? undefined : (vintageColorMap.get(k) === "#f59e0b" ? undefined : "4 2")}
                          dot={false}
                          isAnimationActive={false}
                        />
                      ))}
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              );
            })}
          </div>

          {/* --- Evolution Table --- */}
          <div className="overflow-x-auto rounded-xl border border-gray-800">
            <table className="text-sm border-collapse" style={{ minWidth: "1400px" }}>
              <thead>
                <tr>
                  <th className="sticky left-0 z-10 bg-[#0f1117] px-3 py-2 text-left text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
                    Region
                  </th>
                  <th className="sticky left-[100px] z-10 bg-[#0f1117] px-3 py-2 text-left text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
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
                {data.regions.flatMap((rc) => {
                  const vData = vintagesByRegion.get(rc.key);
                  if (!vData) return [];
                  const { vintages: allRegionVintages } = vData;
                  const displayVintages = allRegionVintages.filter((v) => selectedVintages.has(v));

                  // Also add actuals row
                  const actualMap = new Map<number, number>();
                  for (const a of data.actuals) {
                    if (a.region === rc.rtPrelim) {
                      actualMap.set(Number(a.hour_ending), Number(a.rt_load_mw));
                    }
                  }

                  // Get min/max for heatmap across all vintages in this region
                  let heatMin = Infinity, heatMax = -Infinity;
                  for (const f of data.forecasts) {
                    if (f.region === rc.forecast) {
                      const v = Number(f.forecast_load_mw);
                      if (v < heatMin) heatMin = v;
                      if (v > heatMax) heatMax = v;
                    }
                  }
                  for (const [, v] of actualMap) {
                    if (v < heatMin) heatMin = v;
                    if (v > heatMax) heatMax = v;
                  }

                  const tableRows = [
                    ...displayVintages.map((v) => ({ type: "vintage" as const, key: v, label: fmtVintageLabel(v), isDA: isDAWindowVintage(v) })),
                    { type: "actual" as const, key: "actual", label: "RT Prelim", isDA: false },
                  ];

                  return tableRows.map((row, mi) => {
                    const hourVals: (number | null)[] = HOURS.map((h) => {
                      if (row.type === "actual") return actualMap.get(h) ?? null;
                      const match = data.forecasts.find(
                        (f) => f.region === rc.forecast && f.forecast_execution_datetime === row.key && Number(f.hour_ending) === h
                      );
                      return match ? Number(match.forecast_load_mw) : null;
                    });

                    const onpeakVals = hourVals.filter((_, i) => ONPEAK_HOURS.has(i + 1)).filter((v) => v != null) as number[];
                    const offpeakVals = hourVals.filter((_, i) => !ONPEAK_HOURS.has(i + 1)).filter((v) => v != null) as number[];
                    const allValsArr = hourVals.filter((v) => v != null) as number[];
                    const onpeak = onpeakVals.length > 0 ? onpeakVals.reduce((a, b) => a + b, 0) / onpeakVals.length : null;
                    const offpeak = offpeakVals.length > 0 ? offpeakVals.reduce((a, b) => a + b, 0) / offpeakVals.length : null;
                    const flat = allValsArr.length > 0 ? allValsArr.reduce((a, b) => a + b, 0) / allValsArr.length : null;

                    return (
                      <tr
                        key={`${rc.key}-${row.key}`}
                        className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${
                          row.type === "actual" ? "bg-green-900/10" : mi % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                        }`}
                      >
                        {mi === 0 && (
                          <td
                            rowSpan={tableRows.length}
                            className="sticky left-0 z-10 bg-inherit px-3 py-1.5 text-sm font-semibold text-white whitespace-nowrap align-top border-r border-gray-700"
                          >
                            {rc.display}
                          </td>
                        )}
                        <td className={`sticky left-[100px] z-10 bg-inherit px-3 py-1.5 text-[10px] font-medium whitespace-nowrap ${
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
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
