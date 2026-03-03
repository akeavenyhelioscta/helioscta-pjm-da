"use client";

import React, { useEffect, useState, useMemo, useCallback } from "react";
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
  date: string;
  hour_ending: number;
  region: string;
  [key: string]: string | number;
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
const HIGHLIGHT_COLORS = ["#dc2626", "#2563eb", "#16a34a", "#f59e0b", "#8b5cf6", "#ec4899", "#14b8a6", "#f97316"];

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

/* ------------------------------------------------------------------ */
/*  Custom tooltip                                                     */
/* ------------------------------------------------------------------ */

interface ChartEntry {
  hour: number;
  Max: number | null;
  Min: number | null;
  maxDate: string | null;
  minDate: string | null;
  [date: string]: number | string | null;
}

function CustomTooltip({
  active,
  payload,
  label,
  visibleDates,
  dateColors,
}: {
  active?: boolean;
  payload?: { dataKey: string; value: number; color: string; payload?: ChartEntry }[];
  label?: number;
  visibleDates: Set<string>;
  dateColors: Map<string, string>;
}) {
  if (!active || !payload || payload.length === 0) return null;

  const entry = payload[0]?.payload as ChartEntry | undefined;
  if (!entry) return null;

  const dateEntries: { date: string; value: number; color: string }[] = [];
  for (const p of payload) {
    if (p.dataKey === "Max" || p.dataKey === "Min") continue;
    if (p.value == null) continue;
    dateEntries.push({ date: p.dataKey, value: p.value, color: p.color });
  }
  dateEntries.sort((a, b) => b.value - a.value);

  return (
    <div className="rounded-lg border border-gray-700 bg-[#1f2937] px-3 py-2 text-xs shadow-xl">
      <p className="mb-1.5 font-semibold text-gray-200">HE {label}</p>
      {entry.Max != null && (
        <p className="text-gray-400">
          <span className="text-gray-500">Max:</span>{" "}
          <span className="text-gray-200">{entry.Max.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")} MW</span>
          {entry.maxDate && <span className="ml-1 text-gray-500">({entry.maxDate})</span>}
        </p>
      )}
      {entry.Min != null && (
        <p className="mb-1 text-gray-400">
          <span className="text-gray-500">Min:</span>{" "}
          <span className="text-gray-200">{entry.Min.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")} MW</span>
          {entry.minDate && <span className="ml-1 text-gray-500">({entry.minDate})</span>}
        </p>
      )}
      {dateEntries.length > 0 && (
        <div className="mt-1 border-t border-gray-700 pt-1 space-y-0.5">
          {dateEntries.slice(0, 8).map((e) => (
            <p key={e.date} className="flex items-center gap-1.5">
              <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: e.color }} />
              <span className="text-gray-400">{e.date}:</span>
              <span className="text-gray-200">{e.value.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")} MW</span>
            </p>
          ))}
          {dateEntries.length > 8 && (
            <p className="text-gray-600">+{dateEntries.length - 8} more</p>
          )}
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

function todayStr(): string {
  return new Date().toISOString().slice(0, 10);
}
function tomorrowStr(): string {
  const d = new Date();
  d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
}
function offsetDate(base: string, days: number): string {
  const d = new Date(base);
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}
function daysBetween(a: string, b: string): number {
  const ms = new Date(b).getTime() - new Date(a).getTime();
  return Math.round(ms / 86_400_000);
}

export default function PjmLoadRtMeteredHourly() {
  return <PjmLoadHourlyTable apiPath="/api/pjm-load-rt-metered-hourly" loadTypeLabel="RT Metered Load" />;
}

export function PjmLoadHourlyTable({ apiPath, loadTypeLabel, valueKey = "rt_load_mw", defaultRegion = "RTO", defaultDays = 7, lookAhead = false, extraParams, extraFilters, onResponse }: {
  apiPath: string;
  loadTypeLabel: string;
  valueKey?: string;
  defaultRegion?: string;
  defaultDays?: number;
  lookAhead?: boolean;
  extraParams?: Record<string, string>;
  extraFilters?: React.ReactNode;
  onResponse?: (json: Record<string, unknown>) => void;
}) {
  const initStart = lookAhead ? todayStr() : offsetDate(todayStr(), -defaultDays);
  const initEnd = lookAhead ? offsetDate(todayStr(), defaultDays) : todayStr();

  /* --- filter state (applied values that trigger fetch) --- */
  const [region, setRegion] = useState(defaultRegion);
  const [startDate, setStartDate] = useState(initStart);
  const [endDate, setEndDate] = useState(initEnd);

  /* --- input state (controlled inputs before "Apply") --- */
  const [regionInput, setRegionInput] = useState(defaultRegion);
  const [startInput, setStartInput] = useState(initStart);
  const [endInput, setEndInput] = useState(initEnd);
  const [daysInput, setDaysInput] = useState(String(defaultDays));

  /* --- data state --- */
  const [data, setData] = useState<RawRow[]>([]);
  const [regions, setRegions] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  /* --- chart line visibility --- */
  const [visibleDates, setVisibleDates] = useState<Set<string>>(new Set());
  const [showMax, setShowMax] = useState(true);
  const [showMin, setShowMin] = useState(true);

  /* --- table date multi-select --- */
  const [tableDates, setTableDates] = useState<Set<string> | null>(null);

  /* --- keep inputs in sync --- */
  const onStartChange = useCallback((v: string) => {
    setStartInput(v);
    if (v && endInput) {
      const d = daysBetween(v, endInput);
      if (d >= 0) setDaysInput(String(d));
    }
  }, [endInput]);

  const onEndChange = useCallback((v: string) => {
    setEndInput(v);
    if (startInput && v) {
      const d = daysBetween(startInput, v);
      if (d >= 0) setDaysInput(String(d));
    }
  }, [startInput]);

  const onDaysChange = useCallback((v: string) => {
    setDaysInput(v);
    const n = parseInt(v, 10);
    if (Number.isFinite(n) && n > 0 && endInput) {
      const d = new Date(endInput);
      d.setDate(d.getDate() - n);
      setStartInput(d.toISOString().slice(0, 10));
    }
  }, [endInput]);

  /* --- fetch --- */
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);

    const params = new URLSearchParams({
      region,
      start: startDate,
      end: endDate,
      ...extraParams,
    });

    fetch(`${apiPath}?${params}`, { signal: controller.signal })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((json) => {
        setData(json.rows);
        if (json.regions) setRegions(json.regions);
        onResponse?.(json);
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError("Failed to load data");
      })
      .finally(() => setLoading(false));

    return () => controller.abort();
  }, [region, startDate, endDate, apiPath, extraParams]);

  const applyFilters = useCallback(() => {
    setRegion(regionInput.trim());
    setStartDate(startInput);
    setEndDate(endInput);
  }, [regionInput, startInput, endInput]);

  /* --- pivot data --- */
  const { pivotRows, dates, minVal, maxVal } = useMemo(() => {
    const byDate = new Map<string, Map<number, RawRow>>();
    for (const raw of data) {
      const ds = String(raw.date).slice(0, 10);
      const row: RawRow = {
        ...raw,
        hour_ending: Number(raw.hour_ending),
        [valueKey]: Number(raw[valueKey]),
      };
      if (!byDate.has(ds)) byDate.set(ds, new Map());
      byDate.get(ds)!.set(row.hour_ending, row);
    }

    const dates = [...byDate.keys()].sort().reverse();
    let min = Infinity;
    let max = -Infinity;

    const pivotRows: PivotRow[] = dates.map((date) => {
      const hourMap = byDate.get(date)!;
      const hours: (number | null)[] = [];
      let onS = 0, onN = 0, offS = 0, offN = 0, fS = 0, fN = 0;

      for (let h = 1; h <= 24; h++) {
        const row = hourMap.get(h);
        const val = row ? Number(row[valueKey]) : null;
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

    return {
      pivotRows,
      dates,
      minVal: min === Infinity ? 0 : min,
      maxVal: max === -Infinity ? 1 : max,
    };
  }, [data]);

  // Default visible dates: most recent 3
  useEffect(() => {
    if (dates.length > 0) {
      setVisibleDates(new Set(dates.slice(0, 3)));
    }
  }, [dates]);

  /* --- filtered table rows --- */
  const filteredPivotRows = useMemo(() => {
    if (tableDates === null) return pivotRows;
    return pivotRows.filter((row) => tableDates.has(row.date));
  }, [pivotRows, tableDates]);

  const toggleTableDate = useCallback((date: string) => {
    setTableDates((prev) => {
      if (prev === null) {
        return new Set([date]);
      }
      const next = new Set(prev);
      if (next.has(date)) next.delete(date);
      else next.add(date);
      return next.size === 0 ? null : next;
    });
  }, []);

  const tableSelectAll = useCallback(() => setTableDates(null), []);
  const tableSelectNone = useCallback(() => setTableDates(new Set()), []);

  /* --- date color map --- */
  const dateColors = useMemo(() => {
    const map = new Map<string, string>();
    const visible = [...visibleDates].sort().reverse();
    visible.forEach((d, i) => {
      map.set(d, HIGHLIGHT_COLORS[i % HIGHLIGHT_COLORS.length]);
    });
    return map;
  }, [visibleDates]);

  /* --- chart data --- */
  const chartData = useMemo(() => {
    if (pivotRows.length === 0) return [];
    return HOURS.map((h): ChartEntry => {
      const idx = h - 1;
      const entry: ChartEntry = { hour: h, Max: null, Min: null, maxDate: null, minDate: null };
      let mx = -Infinity, mn = Infinity;
      let mxDate = "", mnDate = "";

      for (const row of pivotRows) {
        const val = row.hours[idx];
        entry[row.date] = val;
        if (val != null) {
          if (val > mx) { mx = val; mxDate = row.date; }
          if (val < mn) { mn = val; mnDate = row.date; }
        }
      }

      entry.Max = mx !== -Infinity ? Math.round(mx * 100) / 100 : null;
      entry.Min = mn !== Infinity ? Math.round(mn * 100) / 100 : null;
      entry.maxDate = mxDate || null;
      entry.minDate = mnDate || null;
      return entry;
    });
  }, [pivotRows]);

  /* --- Y-axis domain --- */
  const yDomain = useMemo((): [number, number] => {
    let lo = Infinity, hi = -Infinity;
    for (const entry of chartData) {
      for (const date of visibleDates) {
        const val = entry[date];
        if (typeof val === "number" && val != null) {
          if (val < lo) lo = val;
          if (val > hi) hi = val;
        }
      }
      if (showMax && typeof entry.Max === "number") {
        if (entry.Max > hi) hi = entry.Max;
        if (entry.Max < lo) lo = entry.Max;
      }
      if (showMin && typeof entry.Min === "number") {
        if (entry.Min < lo) lo = entry.Min;
        if (entry.Min > hi) hi = entry.Min;
      }
    }
    if (lo === Infinity || hi === -Infinity) return [0, 100];
    const pad = (hi - lo) * 0.05 || 5;
    return [Math.floor(lo - pad), Math.ceil(hi + pad)];
  }, [chartData, visibleDates, showMax, showMin]);

  /* --- toggle date visibility --- */
  const toggleDate = useCallback((date: string) => {
    setVisibleDates((prev) => {
      const next = new Set(prev);
      if (next.has(date)) next.delete(date);
      else next.add(date);
      return next;
    });
  }, []);

  const selectOnly = useCallback((date: string) => {
    setVisibleDates(new Set([date]));
  }, []);

  const selectAll = useCallback(() => {
    setVisibleDates(new Set(dates));
  }, [dates]);

  const selectNone = useCallback(() => {
    setVisibleDates(new Set());
  }, []);

  /* ================================================================ */
  /*  Render                                                           */
  /* ================================================================ */

  return (
    <div className="space-y-6">
      {/* ---------- Title + subtitle ---------- */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-white">
            {region} — {loadTypeLabel} (MW)
          </h2>
          <p className="text-xs text-gray-500">
            {dates.length} days · {data.length} hourly observations
          </p>
        </div>
      </div>

      {/* ---------- Filters ---------- */}
      <div className="flex flex-wrap items-end gap-4">
        {/* Region */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] font-bold uppercase tracking-wider text-gray-500">
            Region
          </label>
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
              onKeyDown={(e) => e.key === "Enter" && applyFilters()}
              className="w-48 rounded-md border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-200 focus:border-gray-500 focus:outline-none"
            />
          )}
        </div>

        {extraFilters}

        {/* Start date */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] font-bold uppercase tracking-wider text-gray-500">
            Start Date
          </label>
          <input
            type="date"
            value={startInput}
            onChange={(e) => onStartChange(e.target.value)}
            className="rounded-md border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-200 focus:border-gray-500 focus:outline-none"
          />
        </div>

        {/* End date */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] font-bold uppercase tracking-wider text-gray-500">
            End Date
          </label>
          <input
            type="date"
            value={endInput}
            onChange={(e) => onEndChange(e.target.value)}
            className="rounded-md border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-200 focus:border-gray-500 focus:outline-none"
          />
        </div>

        {/* Days lookback */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] font-bold uppercase tracking-wider text-gray-500">
            Days
          </label>
          <input
            type="number"
            min={1}
            max={730}
            value={daysInput}
            onChange={(e) => onDaysChange(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && applyFilters()}
            className="w-16 rounded-md border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-200 focus:border-gray-500 focus:outline-none"
          />
        </div>

        <button
          onClick={applyFilters}
          className="rounded-md bg-gray-700 px-4 py-1.5 text-sm font-medium text-gray-200 transition-colors hover:bg-gray-600"
        >
          Apply
        </button>
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

      {/* ---------- Chart + date selector ---------- */}
      {!loading && !error && chartData.length > 0 && (
        <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
          <div className="mb-3 flex items-center justify-between">
            <p className="text-xs font-semibold text-gray-400">
              {loadTypeLabel} — Hourly Profile (MW)
            </p>
            <div className="flex gap-2">
              <button
                onClick={selectAll}
                className="rounded px-2 py-0.5 text-[10px] font-medium text-gray-400 transition-colors hover:bg-gray-800 hover:text-gray-200"
              >
                All
              </button>
              <button
                onClick={selectNone}
                className="rounded px-2 py-0.5 text-[10px] font-medium text-gray-400 transition-colors hover:bg-gray-800 hover:text-gray-200"
              >
                None
              </button>
            </div>
          </div>

          <ResponsiveContainer width="100%" height={360}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis
                dataKey="hour"
                tick={{ fill: "#9ca3af", fontSize: 11 }}
                label={{ value: "Hour Ending", position: "insideBottom", offset: -4, fill: "#6b7280", fontSize: 11 }}
              />
              <YAxis
                domain={yDomain}
                allowDataOverflow={true}
                tick={{ fill: "#9ca3af", fontSize: 11 }}
                tickFormatter={(v: number) => v.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")}
                label={{ value: "Load (MW)", angle: -90, position: "insideLeft", offset: 10, fill: "#6b7280", fontSize: 11 }}
              />
              <Tooltip
                content={<CustomTooltip visibleDates={visibleDates} dateColors={dateColors} />}
              />
              {showMax && <Line type="monotone" dataKey="Max" stroke="#9ca3af" strokeWidth={1.5} strokeDasharray="5 5" dot={false} />}
              {showMin && <Line type="monotone" dataKey="Min" stroke="#9ca3af" strokeWidth={1.5} strokeDasharray="5 5" dot={false} />}
              {dates.map((date) => {
                const isVisible = visibleDates.has(date);
                const color = dateColors.get(date) || "#4b5563";
                return (
                  <Line
                    key={date}
                    type="monotone"
                    dataKey={date}
                    stroke={isVisible ? color : "#4b5563"}
                    strokeWidth={isVisible ? 2.5 : 0.5}
                    strokeOpacity={isVisible ? 1 : 0.2}
                    dot={false}
                    legendType="none"
                  />
                );
              })}
            </LineChart>
          </ResponsiveContainer>

          {/* Max/Min toggles + Date selector chips */}
          <div className="mt-3 flex flex-wrap items-center gap-1.5">
            <button
              onClick={() => setShowMax((v) => !v)}
              className={`rounded px-2 py-0.5 text-[10px] font-medium transition-all ${
                showMax
                  ? "border border-gray-500 bg-gray-700/40 text-gray-200"
                  : "border border-transparent text-gray-600 hover:text-gray-400"
              }`}
            >
              - - Max
            </button>
            <button
              onClick={() => setShowMin((v) => !v)}
              className={`rounded px-2 py-0.5 text-[10px] font-medium transition-all ${
                showMin
                  ? "border border-gray-500 bg-gray-700/40 text-gray-200"
                  : "border border-transparent text-gray-600 hover:text-gray-400"
              }`}
            >
              - - Min
            </button>
            <span className="mx-1 h-3 border-l border-gray-700" />
            {dates.map((date) => {
              const isVisible = visibleDates.has(date);
              const color = dateColors.get(date);
              return (
                <button
                  key={date}
                  onClick={() => toggleDate(date)}
                  onDoubleClick={() => selectOnly(date)}
                  className={`rounded px-2 py-0.5 text-[10px] font-medium transition-all ${
                    isVisible
                      ? "text-white"
                      : "text-gray-600 hover:text-gray-400"
                  }`}
                  style={isVisible && color ? { backgroundColor: color + "33", borderColor: color, border: `1px solid ${color}` } : { border: "1px solid transparent" }}
                  title={`Click to toggle · Double-click to solo`}
                >
                  {date}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* ---------- Pivot table ---------- */}
      {!loading && !error && pivotRows.length > 0 && (
        <div className="space-y-3">
          {/* Table date multi-select */}
          <div className="space-y-1.5">
            <div className="flex items-center gap-3">
              <p className="text-xs font-semibold text-gray-400">Table Dates</p>
              <button
                onClick={tableSelectAll}
                className="rounded px-2 py-0.5 text-[10px] font-medium text-gray-400 transition-colors hover:bg-gray-800 hover:text-gray-200"
              >
                All
              </button>
              <button
                onClick={tableSelectNone}
                className="rounded px-2 py-0.5 text-[10px] font-medium text-gray-400 transition-colors hover:bg-gray-800 hover:text-gray-200"
              >
                None
              </button>
              <span className="text-[10px] text-gray-600">
                {filteredPivotRows.length} of {pivotRows.length} days
              </span>
            </div>
            <div className="flex flex-wrap gap-1">
              {dates.map((date) => {
                const selected = tableDates === null || tableDates.has(date);
                return (
                  <button
                    key={date}
                    onClick={() => toggleTableDate(date)}
                    className={`rounded px-2 py-0.5 text-[10px] font-medium transition-all ${
                      selected
                        ? "border border-gray-500 bg-gray-700/40 text-gray-200"
                        : "border border-transparent text-gray-600 hover:text-gray-400"
                    }`}
                  >
                    {date}
                  </button>
                );
              })}
            </div>
          </div>
        <div className="overflow-x-auto rounded-xl border border-gray-800">
          <table className="text-sm border-collapse" style={{ minWidth: "1400px" }}>
            <thead>
              <tr>
                <th className="sticky left-0 z-10 bg-[#0f1117] px-3 py-2 text-left text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
                  Date
                </th>
                <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
                  Onpeak
                </th>
                <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 whitespace-nowrap">
                  Offpeak
                </th>
                <th className="px-2 py-2 text-center text-xs font-medium text-gray-400 border-b border-gray-700 border-r whitespace-nowrap">
                  Flat
                </th>
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
              {filteredPivotRows.map((row, idx) => (
                <tr
                  key={row.date}
                  className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${
                    idx % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                  }`}
                >
                  <td className="sticky left-0 z-10 bg-inherit px-3 py-1.5 text-sm font-medium text-white whitespace-nowrap">
                    {row.date}
                  </td>
                  <td className="px-2 py-1.5 text-center text-sm text-gray-300">
                    {fmt(row.onpeak)}
                  </td>
                  <td className="px-2 py-1.5 text-center text-sm text-gray-300">
                    {fmt(row.offpeak)}
                  </td>
                  <td className="px-2 py-1.5 text-center text-sm text-gray-300 border-r border-gray-700">
                    {fmt(row.flat)}
                  </td>
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
