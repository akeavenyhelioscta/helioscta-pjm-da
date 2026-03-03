"use client";

import { useState, useCallback } from "react";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface OutputRow {
  Date: string;
  Type: string;
  [key: string]: string | number | null;
}

interface FanChartPoint {
  hour_ending: number;
  point_forecast: number;
  [key: string]: number | null;
}

interface AnalogDay {
  date: string;
  rank: number;
  distance: number;
  similarity: number;
  weight: number;
}

interface ForecastMetrics {
  mae?: number;
  rmse?: number;
  mape?: number;
  rmae?: number;
  crps?: number;
  coverage_80pct?: number;
  coverage_90pct?: number;
  coverage_98pct?: number;
  sharpness_90pct?: number;
  mean_pinball?: number;
}

interface ForecastApiResponse {
  forecast_date: string;
  reference_date: string;
  has_actuals: boolean;
  n_analogs_used: number;
  output_table: OutputRow[];
  quantiles_table: OutputRow[];
  fan_chart: FanChartPoint[];
  analogs: AnalogDay[];
  metrics: ForecastMetrics | null;
}

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const HE_COLS = Array.from({ length: 24 }, (_, i) => `HE${i + 1}`);
const SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"];

const QUANTILE_BANDS: { lower: string; upper: string; label: string; color: string; opacity: number }[] = [
  { lower: "q_0.01", upper: "q_0.99", label: "P01–P99", color: "#6366f1", opacity: 0.15 },
  { lower: "q_0.05", upper: "q_0.95", label: "P05–P95", color: "#6366f1", opacity: 0.20 },
  { lower: "q_0.10", upper: "q_0.90", label: "P10–P90", color: "#6366f1", opacity: 0.25 },
  { lower: "q_0.25", upper: "q_0.75", label: "P25–P75", color: "#6366f1", opacity: 0.35 },
];

const WEIGHT_METHODS = [
  { value: "inverse_distance", label: "Inverse Distance" },
  { value: "equal", label: "Equal" },
  { value: "rank", label: "Rank-Based" },
];

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function getTomorrowStr(): string {
  const d = new Date();
  d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
}

function heatmapBg(val: number | null | undefined, type: string): string {
  if (val == null) return "";
  if (type === "Error") {
    const abs = Math.abs(val);
    if (abs < 2) return "rgba(34,197,94,0.15)";
    if (abs < 5) return "rgba(234,179,8,0.15)";
    if (abs < 10) return "rgba(249,115,22,0.20)";
    return "rgba(239,68,68,0.20)";
  }
  if (val < 0) return "rgba(59,130,246,0.15)";
  if (val < 20) return "rgba(59,130,246,0.10)";
  if (val < 40) return "rgba(34,197,94,0.10)";
  if (val < 60) return "rgba(234,179,8,0.10)";
  if (val < 100) return "rgba(249,115,22,0.15)";
  return "rgba(239,68,68,0.20)";
}

function fmtVal(val: number | null | undefined): string {
  if (val == null) return "—";
  return val.toFixed(2);
}

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export default function LikeDayForecast() {
  // Controls
  const [forecastDate, setForecastDate] = useState(getTomorrowStr());
  const [nAnalogs, setNAnalogs] = useState(30);
  const [weightMethod, setWeightMethod] = useState("inverse_distance");

  // Data
  const [data, setData] = useState<ForecastApiResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingMsg, setLoadingMsg] = useState("");
  const [error, setError] = useState<string | null>(null);

  // UI toggles
  const [showAllAnalogs, setShowAllAnalogs] = useState(false);
  const [showQuantiles, setShowQuantiles] = useState(false);

  const runForecast = useCallback(async () => {
    setLoading(true);
    setLoadingMsg("Building feature matrix...");
    setError(null);
    setData(null);

    const params = new URLSearchParams({
      n_analogs: String(nAnalogs),
      weight_method: weightMethod,
    });
    if (forecastDate) params.set("forecast_date", forecastDate);

    try {
      const res = await fetch(`/api/pjm-like-day-forecast?${params}`);
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        throw new Error(body?.detail || body?.error || `HTTP ${res.status}`);
      }
      const json: ForecastApiResponse = await res.json();
      setData(json);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setLoading(false);
      setLoadingMsg("");
    }
  }, [forecastDate, nAnalogs, weightMethod]);

  /* ---- Fan chart data: compute band heights ---- */
  const fanChartData = data?.fan_chart?.map((pt) => {
    const entry: Record<string, number | null> = {
      hour_ending: pt.hour_ending,
      point_forecast: pt.point_forecast,
    };

    // For stacked area bands, compute the height of each band layer
    // We render from outermost (P01-P99) to innermost (P25-P75)
    // Each area band = upper - lower for that quantile pair
    for (const band of QUANTILE_BANDS) {
      const lower = pt[band.lower] as number | null;
      const upper = pt[band.upper] as number | null;
      if (lower != null && upper != null) {
        entry[`${band.label}_lower`] = lower;
        entry[`${band.label}_upper`] = upper;
      }
    }

    // Actuals from output_table
    if (data.has_actuals) {
      const actualRow = data.output_table.find((r) => r.Type === "Actual");
      if (actualRow) {
        const heVal = actualRow[`HE${pt.hour_ending}`];
        entry.actual = typeof heVal === "number" ? heVal : null;
      }
    }

    return entry;
  });

  const analogsToShow = data
    ? showAllAnalogs
      ? data.analogs
      : data.analogs.slice(0, 10)
    : [];

  return (
    <div className="space-y-6">
      {/* ── Controls Bar ── */}
      <div className="flex flex-wrap items-end gap-4">
        <div>
          <label className="mb-1 block text-xs font-medium text-gray-400">
            Forecast Date
          </label>
          <input
            type="date"
            value={forecastDate}
            onChange={(e) => setForecastDate(e.target.value)}
            className="rounded border border-gray-700 bg-[#0f1117] px-3 py-1.5 text-sm text-gray-200 focus:border-amber-500 focus:outline-none"
          />
        </div>

        <div>
          <label className="mb-1 block text-xs font-medium text-gray-400">
            # Analogs
          </label>
          <input
            type="number"
            min={5}
            max={100}
            value={nAnalogs}
            onChange={(e) => setNAnalogs(Number(e.target.value))}
            className="w-20 rounded border border-gray-700 bg-[#0f1117] px-3 py-1.5 text-sm text-gray-200 focus:border-amber-500 focus:outline-none"
          />
        </div>

        <div>
          <label className="mb-1 block text-xs font-medium text-gray-400">
            Weight Method
          </label>
          <select
            value={weightMethod}
            onChange={(e) => setWeightMethod(e.target.value)}
            className="rounded border border-gray-700 bg-[#0f1117] px-3 py-1.5 text-sm text-gray-200 focus:border-amber-500 focus:outline-none"
          >
            {WEIGHT_METHODS.map((m) => (
              <option key={m.value} value={m.value}>
                {m.label}
              </option>
            ))}
          </select>
        </div>

        <button
          onClick={runForecast}
          disabled={loading}
          className="rounded bg-amber-600 px-5 py-1.5 text-sm font-semibold text-white transition-colors hover:bg-amber-500 disabled:cursor-not-allowed disabled:opacity-50"
        >
          {loading ? "Running..." : "Run Forecast"}
        </button>
      </div>

      {/* ── Loading / Error ── */}
      {loading && (
        <div className="flex items-center gap-3 rounded-lg border border-gray-800 bg-[#12141d] px-5 py-4">
          <svg
            className="h-5 w-5 animate-spin text-amber-400"
            xmlns="http://www.w3.org/2000/svg"
            fill="none"
            viewBox="0 0 24 24"
          >
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
            />
          </svg>
          <span className="text-sm text-gray-300">{loadingMsg || "Running forecast pipeline..."}</span>
        </div>
      )}

      {error && (
        <div className="rounded-lg border border-red-800/50 bg-red-900/20 px-5 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {/* ── Results ── */}
      {data && !loading && (
        <>
          {/* Header summary */}
          <div className="flex flex-wrap items-center gap-4 text-xs text-gray-500">
            <span>Forecast: <span className="text-gray-300">{data.forecast_date}</span></span>
            <span>Reference: <span className="text-gray-300">{data.reference_date}</span></span>
            <span>Analogs used: <span className="text-gray-300">{data.n_analogs_used}</span></span>
            {data.has_actuals && (
              <span className="rounded bg-emerald-900/40 px-2 py-0.5 text-emerald-400">Actuals Available</span>
            )}
          </div>

          {/* ── Metrics Panel ── */}
          {data.metrics && (
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-7">
              {[
                { label: "MAE", value: `$${data.metrics.mae?.toFixed(2)}/MWh`, key: "mae" },
                { label: "RMSE", value: `$${data.metrics.rmse?.toFixed(2)}/MWh`, key: "rmse" },
                { label: "MAPE", value: `${data.metrics.mape?.toFixed(1)}%`, key: "mape" },
                { label: "rMAE", value: data.metrics.rmae?.toFixed(3), key: "rmae" },
                { label: "CRPS", value: data.metrics.crps?.toFixed(4), key: "crps" },
                { label: "Coverage 90%", value: data.metrics.coverage_90pct != null ? `${(data.metrics.coverage_90pct * 100).toFixed(0)}%` : undefined, key: "coverage_90pct" },
                { label: "Sharpness 90%", value: data.metrics.sharpness_90pct != null ? `$${data.metrics.sharpness_90pct.toFixed(2)}` : undefined, key: "sharpness_90pct" },
              ]
                .filter((m) => m.value != null && m.value !== "undefined" && m.value !== "$undefined/MWh" && m.value !== "undefined%")
                .map((m) => (
                  <div
                    key={m.key}
                    className="rounded-lg border border-gray-800 bg-[#12141d] px-4 py-3"
                  >
                    <p className="text-[10px] font-semibold uppercase tracking-widest text-gray-500">
                      {m.label}
                    </p>
                    <p className="mt-1 text-lg font-bold text-gray-100">{m.value}</p>
                  </div>
                ))}
            </div>
          )}

          {/* ── Output Table ── */}
          <div>
            <h3 className="mb-2 text-sm font-semibold text-gray-300">
              DA LMP Forecast — Western Hub ($/MWh)
            </h3>
            <div className="overflow-x-auto rounded-lg border border-gray-800">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-gray-800 bg-[#0b0d14]">
                    <th className="sticky left-0 z-10 bg-[#0b0d14] px-3 py-2 text-left font-medium text-gray-400">
                      Type
                    </th>
                    {HE_COLS.map((col) => (
                      <th key={col} className="px-2 py-2 text-right font-medium text-gray-400">
                        {col.replace("HE", "")}
                      </th>
                    ))}
                    {SUMMARY_COLS.map((col) => (
                      <th
                        key={col}
                        className="border-l border-gray-800 px-2 py-2 text-right font-medium text-gray-400"
                      >
                        {col}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.output_table.map((row, idx) => (
                    <tr
                      key={row.Type}
                      className={`border-b border-gray-800/50 ${
                        idx % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                      } ${row.Type === "Error" ? "text-gray-500 italic" : ""}`}
                    >
                      <td className="sticky left-0 z-10 bg-inherit px-3 py-1.5 font-medium text-gray-300">
                        {row.Type}
                      </td>
                      {HE_COLS.map((col) => {
                        const val = row[col] as number | null;
                        return (
                          <td
                            key={col}
                            className="px-2 py-1.5 text-right tabular-nums text-gray-200"
                            style={{ background: heatmapBg(val, row.Type) }}
                          >
                            {fmtVal(val)}
                          </td>
                        );
                      })}
                      {SUMMARY_COLS.map((col) => {
                        const val = row[col] as number | null;
                        return (
                          <td
                            key={col}
                            className="border-l border-gray-800 px-2 py-1.5 text-right tabular-nums font-medium text-gray-200"
                            style={{ background: heatmapBg(val, row.Type) }}
                          >
                            {fmtVal(val)}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* ── Fan Chart ── */}
          {fanChartData && fanChartData.length > 0 && (
            <div>
              <h3 className="mb-2 text-sm font-semibold text-gray-300">
                Probabilistic Forecast — Fan Chart
              </h3>
              <div className="rounded-lg border border-gray-800 bg-[#12141d] p-4">
                <ResponsiveContainer width="100%" height={400}>
                  <AreaChart data={fanChartData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                    <XAxis
                      dataKey="hour_ending"
                      stroke="#6b7280"
                      tick={{ fill: "#9ca3af", fontSize: 11 }}
                      label={{ value: "Hour Ending", position: "insideBottomRight", offset: -5, fill: "#6b7280", fontSize: 11 }}
                    />
                    <YAxis
                      stroke="#6b7280"
                      tick={{ fill: "#9ca3af", fontSize: 11 }}
                      label={{ value: "$/MWh", angle: -90, position: "insideLeft", fill: "#6b7280", fontSize: 11 }}
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: "#1f2937",
                        border: "1px solid #374151",
                        borderRadius: 8,
                        fontSize: 12,
                      }}
                      labelStyle={{ color: "#9ca3af" }}
                      labelFormatter={(label) => `HE ${label}`}
                      formatter={(value: unknown, name: string) => {
                        if (value == null || typeof value !== "number") return ["—", name];
                        return [`$${value.toFixed(2)}`, name];
                      }}
                    />
                    <Legend
                      wrapperStyle={{ fontSize: 11, paddingTop: 8 }}
                    />

                    {/* Quantile bands - rendered as pairs of areas between lower and upper */}
                    {QUANTILE_BANDS.map((band) => (
                      <Area
                        key={`${band.label}_upper`}
                        type="monotone"
                        dataKey={`${band.label}_upper`}
                        stroke="none"
                        fill={band.color}
                        fillOpacity={band.opacity}
                        name={band.label}
                        dot={false}
                        activeDot={false}
                        isAnimationActive={false}
                      />
                    ))}
                    {QUANTILE_BANDS.map((band) => (
                      <Area
                        key={`${band.label}_lower`}
                        type="monotone"
                        dataKey={`${band.label}_lower`}
                        stroke="none"
                        fill="#12141d"
                        fillOpacity={1}
                        name={`${band.label} (lower)`}
                        legendType="none"
                        dot={false}
                        activeDot={false}
                        isAnimationActive={false}
                      />
                    ))}

                    {/* Point forecast line */}
                    <Area
                      type="monotone"
                      dataKey="point_forecast"
                      stroke="#f59e0b"
                      strokeWidth={2.5}
                      fill="none"
                      name="Point Forecast"
                      dot={{ r: 3, fill: "#f59e0b" }}
                    />

                    {/* Actual line (if available) */}
                    {data.has_actuals && (
                      <Area
                        type="monotone"
                        dataKey="actual"
                        stroke="#10b981"
                        strokeWidth={2}
                        fill="none"
                        strokeDasharray="6 3"
                        name="Actual"
                        dot={{ r: 2.5, fill: "#10b981" }}
                      />
                    )}
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {/* ── Quantile Bands Table (Collapsible) ── */}
          <div>
            <button
              onClick={() => setShowQuantiles(!showQuantiles)}
              className="mb-2 flex items-center gap-2 text-sm font-semibold text-gray-300 transition-colors hover:text-gray-100"
            >
              <svg
                className={`h-4 w-4 transition-transform ${showQuantiles ? "rotate-90" : ""}`}
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth={2}
              >
                <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
              </svg>
              Quantile Bands ($/MWh)
            </button>
            {showQuantiles && (
              <div className="overflow-x-auto rounded-lg border border-gray-800">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-gray-800 bg-[#0b0d14]">
                      <th className="sticky left-0 z-10 bg-[#0b0d14] px-3 py-2 text-left font-medium text-gray-400">
                        Band
                      </th>
                      {HE_COLS.map((col) => (
                        <th key={col} className="px-2 py-2 text-right font-medium text-gray-400">
                          {col.replace("HE", "")}
                        </th>
                      ))}
                      {SUMMARY_COLS.map((col) => (
                        <th
                          key={col}
                          className="border-l border-gray-800 px-2 py-2 text-right font-medium text-gray-400"
                        >
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.quantiles_table.map((row, idx) => (
                      <tr
                        key={row.Type}
                        className={`border-b border-gray-800/50 ${
                          idx % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                        } ${row.Type === "P50" ? "font-semibold" : ""}`}
                      >
                        <td className="sticky left-0 z-10 bg-inherit px-3 py-1.5 font-medium text-gray-300">
                          {row.Type}
                        </td>
                        {HE_COLS.map((col) => {
                          const val = row[col] as number | null;
                          return (
                            <td key={col} className="px-2 py-1.5 text-right tabular-nums text-gray-200">
                              {fmtVal(val)}
                            </td>
                          );
                        })}
                        {SUMMARY_COLS.map((col) => {
                          const val = row[col] as number | null;
                          return (
                            <td
                              key={col}
                              className="border-l border-gray-800 px-2 py-1.5 text-right tabular-nums font-medium text-gray-200"
                            >
                              {fmtVal(val)}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* ── Analog Days Table ── */}
          <div>
            <h3 className="mb-2 text-sm font-semibold text-gray-300">
              Analog Days ({data.analogs.length} total)
            </h3>
            <div className="overflow-x-auto rounded-lg border border-gray-800">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-gray-800 bg-[#0b0d14]">
                    <th className="px-3 py-2 text-left font-medium text-gray-400">Rank</th>
                    <th className="px-3 py-2 text-left font-medium text-gray-400">Date</th>
                    <th className="px-3 py-2 text-left font-medium text-gray-400">Day</th>
                    <th className="px-3 py-2 text-right font-medium text-gray-400">Distance</th>
                    <th className="px-3 py-2 text-right font-medium text-gray-400">Similarity</th>
                    <th className="px-3 py-2 text-right font-medium text-gray-400">Weight</th>
                  </tr>
                </thead>
                <tbody>
                  {analogsToShow.map((a, idx) => {
                    const d = new Date(a.date + "T00:00:00");
                    const dayName = d.toLocaleDateString("en-US", { weekday: "short" });
                    return (
                      <tr
                        key={a.date}
                        className={`border-b border-gray-800/50 ${
                          idx % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                        }`}
                      >
                        <td className="px-3 py-1.5 text-gray-400">{a.rank}</td>
                        <td className="px-3 py-1.5 font-mono text-gray-200">{a.date}</td>
                        <td className="px-3 py-1.5 text-gray-400">{dayName}</td>
                        <td className="px-3 py-1.5 text-right tabular-nums text-gray-200">
                          {a.distance.toFixed(4)}
                        </td>
                        <td className="px-3 py-1.5 text-right tabular-nums text-gray-200">
                          {(a.similarity * 100).toFixed(1)}%
                        </td>
                        <td className="px-3 py-1.5 text-right tabular-nums text-gray-200">
                          {a.weight.toFixed(4)}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {data.analogs.length > 10 && (
              <button
                onClick={() => setShowAllAnalogs(!showAllAnalogs)}
                className="mt-2 text-xs text-amber-400 transition-colors hover:text-amber-300"
              >
                {showAllAnalogs
                  ? "Show top 10 only"
                  : `Show all ${data.analogs.length} analogs`}
              </button>
            )}
          </div>
        </>
      )}

      {/* ── Empty state ── */}
      {!data && !loading && !error && (
        <div className="flex flex-col items-center justify-center py-16 text-gray-500">
          <svg className="mb-3 h-12 w-12 text-gray-700" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
          <p className="text-sm">Set parameters above and click <span className="font-semibold text-amber-400">Run Forecast</span> to generate a probabilistic DA LMP forecast.</p>
          <p className="mt-1 text-xs text-gray-600">Pipeline typically takes 10–30 seconds (6+ database queries).</p>
        </div>
      )}
    </div>
  );
}
