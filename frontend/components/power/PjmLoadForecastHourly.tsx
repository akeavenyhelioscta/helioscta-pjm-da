"use client";

import { useEffect, useState, useMemo, useCallback, useRef } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface RawRow {
  forecast_datetime: string;
  forecast_execution_date: string;
  forecast_load_mw: number;
  date: string;
  hour_ending: number;
  region: string;
}

interface ChartEntry {
  timestamp: number;
  [key: string]: number | null;
}

interface PivotRow {
  date: string;
  onpeak: number | null;
  offpeak: number | null;
  flat: number | null;
  hours: (number | null)[];
}

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const ONPEAK_HOURS = new Set([8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]);
const OFFPEAK_HOURS = new Set([1, 2, 3, 4, 5, 6, 7, 24]);
const HOURS = Array.from({ length: 24 }, (_, i) => i + 1);
const COLORS = ["#dc2626", "#2563eb", "#16a34a", "#f59e0b", "#8b5cf6", "#ec4899", "#14b8a6", "#f97316"];

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function fmt(v: number | null): string {
  return v != null ? v.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",") : "—";
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

function fmtTs(ts: number): string {
  const d = new Date(ts);
  const mon = d.toLocaleString("en-US", { month: "short", timeZone: "UTC" });
  const day = d.getUTCDate();
  const hh = String(d.getUTCHours()).padStart(2, "0");
  return `${mon} ${day} ${hh}:00`;
}

/* ------------------------------------------------------------------ */
/*  Custom Tooltip                                                     */
/* ------------------------------------------------------------------ */

function CustomTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean;
  payload?: { dataKey: string; value: number; color: string }[];
  label?: number;
}) {
  if (!active || !payload || payload.length === 0) return null;

  const entries = payload.filter((p) => p.value != null).sort((a, b) => b.value - a.value);

  return (
    <div className="rounded-lg border border-gray-700 bg-[#1f2937] px-3 py-2 text-xs shadow-xl">
      <p className="mb-1.5 font-semibold text-gray-200">
        {typeof label === "number" ? fmtTs(label) : label}
      </p>
      {entries.map((e) => (
        <p key={e.dataKey} className="flex items-center gap-1.5">
          <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: e.color }} />
          <span className="text-gray-400">{e.dataKey}:</span>
          <span className="text-gray-200">{fmt(e.value)} MW</span>
        </p>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export default function PjmLoadForecastHourly() {
  /* --- state --- */
  const [region, setRegion] = useState("RTO_COMBINED");
  const [regionInput, setRegionInput] = useState("RTO_COMBINED");
  const [regions, setRegions] = useState<string[]>([]);
  const [execDates, setExecDates] = useState<string[]>([]);
  const [selectedExecDates, setSelectedExecDates] = useState<string[]>([]);
  const [tableExecDate, setTableExecDate] = useState("");
  const [data, setData] = useState<RawRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const initialized = useRef(false);

  /* --- fetch --- */
  const execDatesKey = useMemo(() => [...selectedExecDates].sort().join(","), [selectedExecDates]);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);

    const params = new URLSearchParams({ region });
    if (execDatesKey) {
      params.set("exec_dates", execDatesKey);
    }

    fetch(`/api/pjm-load-forecast-hourly?${params}`, { signal: controller.signal })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((json) => {
        setData(json.rows ?? []);
        if (json.regions) setRegions(json.regions);
        if (json.exec_dates) {
          setExecDates(json.exec_dates);
          if (!initialized.current) {
            initialized.current = true;
            const defaults = (json.exec_dates as string[]).slice(0, 3);
            setSelectedExecDates(defaults);
            setTableExecDate(defaults[0] ?? "");
          }
        }
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError("Failed to load data");
      })
      .finally(() => setLoading(false));

    return () => controller.abort();
  }, [region, execDatesKey]);

  /* --- chart data --- */
  const chartData = useMemo(() => {
    const byTs = new Map<number, Map<string, number>>();
    for (const row of data) {
      const ts = new Date(row.forecast_datetime).getTime();
      const ed = String(row.forecast_execution_date).slice(0, 10);
      if (!byTs.has(ts)) byTs.set(ts, new Map());
      byTs.get(ts)!.set(ed, Number(row.forecast_load_mw));
    }

    return [...byTs.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([ts, vals]): ChartEntry => {
        const entry: ChartEntry = { timestamp: ts };
        for (const ed of selectedExecDates) {
          entry[ed] = vals.get(ed) ?? null;
        }
        return entry;
      });
  }, [data, selectedExecDates]);

  /* --- exec date colors --- */
  const colorMap = useMemo(() => {
    const map = new Map<string, string>();
    selectedExecDates.forEach((d, i) => map.set(d, COLORS[i % COLORS.length]));
    return map;
  }, [selectedExecDates]);

  /* --- Y domain --- */
  const yDomain = useMemo((): [number, number] => {
    let lo = Infinity, hi = -Infinity;
    for (const entry of chartData) {
      for (const ed of selectedExecDates) {
        const val = entry[ed];
        if (typeof val === "number") {
          if (val < lo) lo = val;
          if (val > hi) hi = val;
        }
      }
    }
    if (lo === Infinity) return [0, 100];
    const pad = (hi - lo) * 0.05 || 5;
    return [Math.floor(lo - pad), Math.ceil(hi + pad)];
  }, [chartData, selectedExecDates]);

  /* --- X-axis ticks at midnight each day --- */
  const xTicks = useMemo(() => {
    if (chartData.length === 0) return [];
    const seen = new Set<string>();
    const ticks: number[] = [];
    for (const entry of chartData) {
      const ds = new Date(entry.timestamp).toISOString().slice(0, 10);
      if (!seen.has(ds)) {
        seen.add(ds);
        ticks.push(new Date(ds + "T00:00:00Z").getTime());
      }
    }
    return ticks;
  }, [chartData]);

  /* --- pivot table data for selected exec date --- */
  const { pivotRows, minVal, maxVal } = useMemo(() => {
    const filtered = data.filter((r) => String(r.forecast_execution_date).slice(0, 10) === tableExecDate);
    const byDate = new Map<string, Map<number, number>>();
    for (const row of filtered) {
      const ds = String(row.date).slice(0, 10);
      const he = Number(row.hour_ending);
      if (!byDate.has(ds)) byDate.set(ds, new Map());
      byDate.get(ds)!.set(he, Number(row.forecast_load_mw));
    }

    const dates = [...byDate.keys()].sort();
    let min = Infinity, max = -Infinity;

    const pivotRows: PivotRow[] = dates.map((date) => {
      const hourMap = byDate.get(date)!;
      const hours: (number | null)[] = [];
      let onS = 0, onN = 0, offS = 0, offN = 0, fS = 0, fN = 0;

      for (let h = 1; h <= 24; h++) {
        const val = hourMap.get(h) ?? null;
        hours.push(val);
        if (val != null && Number.isFinite(val)) {
          if (val < min) min = val;
          if (val > max) max = val;
          fS += val; fN++;
          if (ONPEAK_HOURS.has(h)) { onS += val; onN++; }
          if (OFFPEAK_HOURS.has(h)) { offS += val; offN++; }
        }
      }

      return {
        date,
        onpeak: onN > 0 ? onS / onN : null,
        offpeak: offN > 0 ? offS / offN : null,
        flat: fN > 0 ? fS / fN : null,
        hours,
      };
    });

    return { pivotRows, minVal: min === Infinity ? 0 : min, maxVal: max === -Infinity ? 1 : max };
  }, [data, tableExecDate]);

  /* --- handlers --- */
  const toggleExecDate = useCallback((date: string) => {
    setSelectedExecDates((prev) => {
      if (prev.includes(date)) return prev.filter((d) => d !== date);
      return [...prev, date];
    });
  }, []);

  const applyRegion = useCallback(() => {
    setRegion(regionInput.trim());
    setSelectedExecDates([]);
    setTableExecDate("");
    initialized.current = false;
  }, [regionInput]);

  /* ================================================================ */
  /*  Render                                                           */
  /* ================================================================ */

  return (
    <div className="space-y-6">
      {/* ---------- Title ---------- */}
      <div>
        <h2 className="text-lg font-semibold text-white">
          {region} — Forecast Load (MW)
        </h2>
        <p className="text-xs text-gray-500">
          {selectedExecDates.length} execution dates · {data.length} rows
        </p>
      </div>

      {/* ---------- Filters ---------- */}
      <div className="flex flex-wrap items-end gap-4">
        <div className="flex flex-col gap-1">
          <label className="text-[10px] font-bold uppercase tracking-wider text-gray-500">Region</label>
          {regions.length > 0 ? (
            <select
              value={regionInput}
              onChange={(e) => setRegionInput(e.target.value)}
              className="rounded-md border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-200 focus:border-gray-500 focus:outline-none"
            >
              {regions.map((r) => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
          ) : (
            <input
              type="text"
              value={regionInput}
              onChange={(e) => setRegionInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && applyRegion()}
              className="w-48 rounded-md border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-200 focus:border-gray-500 focus:outline-none"
            />
          )}
        </div>

        <button
          onClick={applyRegion}
          className="rounded-md bg-gray-700 px-4 py-1.5 text-sm font-medium text-gray-200 transition-colors hover:bg-gray-600"
        >
          Apply
        </button>
      </div>

      {/* ---------- Execution date chips ---------- */}
      <div className="space-y-1.5">
        <p className="text-xs font-semibold text-gray-400">Execution Dates</p>
        <div className="flex flex-wrap gap-1.5">
          {execDates.map((d) => {
            const isSelected = selectedExecDates.includes(d);
            const color = colorMap.get(d);
            return (
              <button
                key={d}
                onClick={() => toggleExecDate(d)}
                className={`rounded px-2.5 py-1 text-xs font-medium transition-all ${
                  isSelected ? "text-white" : "text-gray-600 hover:text-gray-400"
                }`}
                style={
                  isSelected && color
                    ? { backgroundColor: color + "33", border: `1px solid ${color}` }
                    : { border: "1px solid transparent" }
                }
              >
                {d}
              </button>
            );
          })}
        </div>
      </div>

      {/* ---------- Loading / Error ---------- */}
      {loading && (
        <div className="flex items-center justify-center h-48">
          <div className="text-gray-500">Loading…</div>
        </div>
      )}
      {error && (
        <div className="flex items-center justify-center h-48">
          <div className="text-red-400">{error}</div>
        </div>
      )}

      {/* ---------- Chart ---------- */}
      {!loading && !error && chartData.length > 0 && (
        <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
          <p className="mb-3 text-xs font-semibold text-gray-400">
            Forecast Load by Execution Date (MW)
          </p>

          <ResponsiveContainer width="100%" height={400}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis
                dataKey="timestamp"
                type="number"
                domain={["dataMin", "dataMax"]}
                ticks={xTicks}
                tickFormatter={(ts: number) => {
                  const d = new Date(ts);
                  return `${d.toLocaleString("en-US", { month: "short", timeZone: "UTC" })} ${d.getUTCDate()}`;
                }}
                tick={{ fill: "#9ca3af", fontSize: 10 }}
              />
              <YAxis
                domain={yDomain}
                allowDataOverflow
                tick={{ fill: "#9ca3af", fontSize: 11 }}
                tickFormatter={(v: number) => fmt(v)}
                label={{ value: "Load (MW)", angle: -90, position: "insideLeft", offset: 10, fill: "#6b7280", fontSize: 11 }}
              />
              <Tooltip content={<CustomTooltip />} />
              {selectedExecDates.map((ed) => (
                <Line
                  key={ed}
                  type="monotone"
                  dataKey={ed}
                  stroke={colorMap.get(ed) || "#4b5563"}
                  strokeWidth={2}
                  dot={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* ---------- Table ---------- */}
      {!loading && !error && pivotRows.length > 0 && (
        <div className="space-y-3">
          <div className="flex items-center gap-3">
            <p className="text-xs font-semibold text-gray-400">Table — Execution Date:</p>
            <select
              value={tableExecDate}
              onChange={(e) => setTableExecDate(e.target.value)}
              className="rounded-md border border-gray-700 bg-gray-800 px-2 py-1 text-xs text-gray-200 focus:border-gray-500 focus:outline-none"
            >
              {selectedExecDates.map((d) => (
                <option key={d} value={d}>{d}</option>
              ))}
            </select>
          </div>

          <div className="overflow-x-auto rounded-xl border border-gray-800">
            <table className="text-sm border-collapse" style={{ minWidth: "1400px" }}>
              <thead>
                <tr>
                  <th className="sticky left-0 z-10 bg-[#0f1117] px-3 py-2 text-left text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
                    Forecast Date
                  </th>
                  <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">Onpeak</th>
                  <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">Offpeak</th>
                  <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 border-r whitespace-nowrap">Flat</th>
                  {HOURS.map((h) => (
                    <th
                      key={h}
                      className={`px-1.5 py-2 text-center text-xs font-medium border-b border-gray-700 whitespace-nowrap ${
                        ONPEAK_HOURS.has(h) ? "text-yellow-500" : "text-gray-500"
                      }`}
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pivotRows.map((row, idx) => (
                  <tr
                    key={row.date}
                    className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${
                      idx % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                    }`}
                  >
                    <td className="sticky left-0 z-10 bg-inherit px-3 py-1.5 text-sm font-medium text-white whitespace-nowrap">
                      {row.date}
                    </td>
                    <td className="px-2 py-1.5 text-center text-sm text-gray-300">{fmt(row.onpeak)}</td>
                    <td className="px-2 py-1.5 text-center text-sm text-gray-300">{fmt(row.offpeak)}</td>
                    <td className="px-2 py-1.5 text-center text-sm text-gray-300 border-r border-gray-700">{fmt(row.flat)}</td>
                    {row.hours.map((val, hi) => (
                      <td
                        key={hi}
                        className={`px-1.5 py-1.5 text-center text-sm text-gray-300 whitespace-nowrap ${cellBg(val, minVal, maxVal)}`}
                      >
                        {fmt(val)}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
