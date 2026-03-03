"use client";

import { useEffect, useState, useMemo } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface KpiData {
  value: number | null;
  date: string | null;
  hour_ending: number | null;
  yesterday_value: number | null;
}

interface HourlyRow {
  hour_ending: number;
  [key: string]: number | string;
}

interface DashboardResponse {
  kpis: {
    rt_load: KpiData;
    da_lmp: KpiData;
    da_load: KpiData;
    forecast_load: KpiData;
  };
  rt_load_profile: HourlyRow[];
  da_lmp_profile: HourlyRow[];
  forecast_vs_actual: {
    forecast: HourlyRow[];
    actual: HourlyRow[];
  };
}

interface KpiCardConfig {
  key: keyof DashboardResponse["kpis"];
  label: string;
  unit: string;
  decimals: number;
  color: string;
  iconPath: string;
}

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const KPI_CARDS: KpiCardConfig[] = [
  {
    key: "rt_load",
    label: "RT Load",
    unit: "MW",
    decimals: 0,
    color: "text-green-400",
    iconPath: "M13 10V3L4 14h7v7l9-11h-7z",
  },
  {
    key: "da_lmp",
    label: "DA LMP",
    unit: "$/MWh",
    decimals: 2,
    color: "text-yellow-500",
    iconPath: "M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z",
  },
  {
    key: "da_load",
    label: "DA Load",
    unit: "MW",
    decimals: 0,
    color: "text-orange-400",
    iconPath: "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
  },
  {
    key: "forecast_load",
    label: "Forecast Load",
    unit: "MW",
    decimals: 0,
    color: "text-purple-400",
    iconPath: "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
  },
];

const HOURS = Array.from({ length: 24 }, (_, i) => i + 1);

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function fmtNumber(v: number | null, decimals: number): string {
  if (v == null) return "--";
  return v.toFixed(decimals).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function fmtTimestamp(date: string | null, he: number | null): string {
  if (!date || he == null) return "--";
  const d = new Date(date);
  const mon = d.toLocaleString("en-US", { month: "short", timeZone: "UTC" });
  const day = d.getUTCDate();
  return `${mon} ${day}, HE ${he}`;
}

/* ------------------------------------------------------------------ */
/*  Sub-components                                                     */
/* ------------------------------------------------------------------ */

function KpiCard({ config, data }: { config: KpiCardConfig; data: KpiData | undefined }) {
  if (!data || data.value == null) {
    return (
      <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
        <p className="text-xs font-semibold uppercase tracking-wider text-gray-500">{config.label}</p>
        <p className="mt-2 text-sm text-gray-600">No data</p>
      </div>
    );
  }

  const current = Number(data.value);
  const previous = data.yesterday_value != null ? Number(data.yesterday_value) : null;
  const pct = previous != null && previous !== 0
    ? ((current - previous) / previous) * 100
    : null;

  return (
    <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
      <div className="flex items-center justify-between">
        <p className="text-xs font-semibold uppercase tracking-wider text-gray-500">{config.label}</p>
        <svg className={`h-4 w-4 ${config.color}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d={config.iconPath} />
        </svg>
      </div>
      <p className={`mt-2 text-2xl font-bold ${config.color}`}>
        {fmtNumber(current, config.decimals)}
        <span className="ml-1 text-sm font-normal text-gray-500">{config.unit}</span>
      </p>
      <div className="mt-1 flex items-center gap-2">
        <p className="text-[10px] text-gray-600">{fmtTimestamp(data.date, data.hour_ending)}</p>
        {pct != null && (
          <span className={`text-[10px] font-medium ${pct > 0 ? "text-green-500" : pct < 0 ? "text-red-500" : "text-gray-500"}`}>
            {pct > 0 ? "+" : ""}{pct.toFixed(1)}% vs yesterday
          </span>
        )}
      </div>
    </div>
  );
}

function MiniChartTooltip({ active, payload, label, unit }: {
  active?: boolean;
  payload?: { value: number }[];
  label?: number;
  unit: string;
}) {
  if (!active || !payload || payload.length === 0) return null;
  return (
    <div className="rounded-lg border border-gray-700 bg-[#1f2937] px-3 py-2 text-xs shadow-xl">
      <p className="font-semibold text-gray-200">HE {label}</p>
      <p className="text-gray-300">{fmtNumber(payload[0].value, unit === "$/MWh" ? 2 : 0)} {unit}</p>
    </div>
  );
}

function ComparisonTooltip({ active, payload, label }: {
  active?: boolean;
  payload?: { dataKey: string; value: number; color: string }[];
  label?: number;
}) {
  if (!active || !payload || payload.length === 0) return null;
  return (
    <div className="rounded-lg border border-gray-700 bg-[#1f2937] px-3 py-2 text-xs shadow-xl">
      <p className="mb-1.5 font-semibold text-gray-200">HE {label}</p>
      {payload.filter((p) => p.value != null).map((p) => (
        <p key={p.dataKey} className="flex items-center gap-1.5">
          <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: p.color }} />
          <span className="text-gray-400">{p.dataKey}:</span>
          <span className="text-gray-200">{fmtNumber(p.value, 0)} MW</span>
        </p>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Main Component                                                     */
/* ------------------------------------------------------------------ */

export default function Dashboard() {
  const [data, setData] = useState<DashboardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);

    fetch("/api/dashboard", { signal: controller.signal })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((json) => setData(json))
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError("Failed to load dashboard data");
      })
      .finally(() => setLoading(false));

    return () => controller.abort();
  }, []);

  const comparisonData = useMemo(() => {
    if (!data) return [];
    const forecastMap = new Map<number, number>();
    const actualMap = new Map<number, number>();
    for (const row of data.forecast_vs_actual.forecast) {
      forecastMap.set(Number(row.hour_ending), Number(row.forecast_load_mw));
    }
    for (const row of data.forecast_vs_actual.actual) {
      actualMap.set(Number(row.hour_ending), Number(row.rt_load_mw));
    }
    return HOURS.map((h) => ({
      hour: h,
      Forecast: forecastMap.get(h) ?? null,
      Actual: actualMap.get(h) ?? null,
    }));
  }, [data]);

  const rtProfileData = useMemo(() => {
    if (!data) return [];
    return data.rt_load_profile.map((r) => ({
      hour: Number(r.hour_ending),
      value: Number(r.rt_load_mw),
    }));
  }, [data]);

  const daLmpProfileData = useMemo(() => {
    if (!data) return [];
    return data.da_lmp_profile.map((r) => ({
      hour: Number(r.hour_ending),
      value: Number(r.lmp_total),
    }));
  }, [data]);

  /* ================================================================ */
  /*  Render                                                           */
  /* ================================================================ */

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-28 animate-pulse rounded-xl border border-gray-800 bg-[#0f1117]" />
          ))}
        </div>
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {[1, 2].map((i) => (
            <div key={i} className="h-56 animate-pulse rounded-xl border border-gray-800 bg-[#0f1117]" />
          ))}
        </div>
        <div className="h-80 animate-pulse rounded-xl border border-gray-800 bg-[#0f1117]" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-64 items-center justify-center">
        <p className="text-red-400">{error}</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* -------- KPI Cards -------- */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        {KPI_CARDS.map((config) => (
          <KpiCard key={config.key} config={config} data={data?.kpis[config.key]} />
        ))}
      </div>

      {/* -------- Mini Hourly Charts -------- */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        {/* RT Load Profile */}
        <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
          <p className="mb-3 text-xs font-semibold text-gray-400">RT Load Profile — Latest Day (MW)</p>
          {rtProfileData.length > 0 ? (
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={rtProfileData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="hour" tick={{ fill: "#9ca3af", fontSize: 10 }} />
                <YAxis
                  tick={{ fill: "#9ca3af", fontSize: 10 }}
                  tickFormatter={(v: number) => fmtNumber(v, 0)}
                />
                <Tooltip content={<MiniChartTooltip unit="MW" />} />
                <Line type="monotone" dataKey="value" stroke="#22c55e" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex h-[200px] items-center justify-center">
              <p className="text-xs text-gray-600">No RT load data available</p>
            </div>
          )}
        </div>

        {/* DA LMP Profile */}
        <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
          <p className="mb-3 text-xs font-semibold text-gray-400">DA LMP Profile — Latest Day ($/MWh)</p>
          {daLmpProfileData.length > 0 ? (
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={daLmpProfileData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="hour" tick={{ fill: "#9ca3af", fontSize: 10 }} />
                <YAxis
                  tick={{ fill: "#9ca3af", fontSize: 10 }}
                  tickFormatter={(v: number) => `$${v.toFixed(0)}`}
                />
                <Tooltip content={<MiniChartTooltip unit="$/MWh" />} />
                <Line type="monotone" dataKey="value" stroke="#eab308" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex h-[200px] items-center justify-center">
              <p className="text-xs text-gray-600">No DA LMP data available</p>
            </div>
          )}
        </div>
      </div>

      {/* -------- Forecast vs Actual -------- */}
      <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
        <p className="mb-3 text-xs font-semibold text-gray-400">Forecast vs Actual RT Load — Today (MW)</p>
        {comparisonData.some((d) => d.Forecast != null || d.Actual != null) ? (
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={comparisonData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
              <XAxis
                dataKey="hour"
                tick={{ fill: "#9ca3af", fontSize: 11 }}
                label={{ value: "Hour Ending", position: "insideBottom", offset: -4, fill: "#6b7280", fontSize: 11 }}
              />
              <YAxis
                tick={{ fill: "#9ca3af", fontSize: 11 }}
                tickFormatter={(v: number) => fmtNumber(v, 0)}
                label={{ value: "Load (MW)", angle: -90, position: "insideLeft", offset: 10, fill: "#6b7280", fontSize: 11 }}
              />
              <Tooltip content={<ComparisonTooltip />} />
              <Legend wrapperStyle={{ fontSize: "11px", color: "#9ca3af" }} />
              <Line type="monotone" dataKey="Forecast" stroke="#a78bfa" strokeWidth={2} strokeDasharray="5 3" dot={false} name="Forecast" />
              <Line type="monotone" dataKey="Actual" stroke="#22c55e" strokeWidth={2.5} dot={false} name="Actual (RT Metered)" />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="flex h-[320px] items-center justify-center">
            <p className="text-xs text-gray-600">No forecast/actual data for today yet</p>
          </div>
        )}
      </div>
    </div>
  );
}
