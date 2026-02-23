"use client";

import { useState, useEffect, useMemo, useCallback } from "react";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from "recharts";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

interface LikeDayResult {
  date: string;
  rank: number;
  distance: number;
  similarity: number;
}

interface HourlyProfile {
  date: string;
  hour_ending: number;
  market: string;
  lmp_total: number;
  lmp_system_energy_price: number;
  lmp_congestion_price: number;
  lmp_marginal_loss_price: number;
}

interface LikeDayApiResponse {
  target_date: string;
  hub: string;
  metric: string;
  n_neighbors: number;
  like_days: LikeDayResult[];
  hourly_profiles: HourlyProfile[];
}

interface ChartEntry {
  hour: number;
  [dateKey: string]: number | null;
}

type LmpComponent =
  | "lmp_total"
  | "lmp_system_energy_price"
  | "lmp_congestion_price"
  | "lmp_marginal_loss_price";

type QuickDatePreset = "all" | "6m" | "1y" | "2y" | "5y";

interface RankingFeature {
  market: string;
  col: LmpComponent;
  weight: number;
}

interface ChartSelection {
  market: string;
  col: LmpComponent;
}

type SeriesCategory = "prices" | "load" | "generation" | "transmission";

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const HIGHLIGHT_COLORS = [
  "#dc2626", "#2563eb", "#16a34a", "#f59e0b",
  "#8b5cf6", "#ec4899", "#14b8a6", "#f97316",
];

const ALL_HOURS = Array.from({ length: 24 }, (_, i) => i + 1);

const COMPONENT_OPTIONS: { value: LmpComponent; label: string }[] = [
  { value: "lmp_total", label: "Total LMP" },
  { value: "lmp_system_energy_price", label: "System Energy" },
  { value: "lmp_congestion_price", label: "Congestion" },
  { value: "lmp_marginal_loss_price", label: "Marginal Loss" },
];

const MARKET_OPTIONS = [
  { value: "da", label: "Day-Ahead" },
  { value: "rt", label: "Real-Time" },
  { value: "dart", label: "DA-RT Spread" },
];

/** Full catalog of selectable features: 3 markets × 4 components = 12 */
const FEATURE_CATALOG: { key: string; label: string; market: string; col: LmpComponent }[] =
  MARKET_OPTIONS.flatMap((m) =>
    COMPONENT_OPTIONS.map((c) => ({
      key: `${m.value}.${c.value}`,
      label: `${m.value.toUpperCase()} ${c.label}`,
      market: m.value,
      col: c.value,
    }))
  );

const METRIC_OPTIONS = [
  { value: "mae", label: "MAE" },
  { value: "rmse", label: "RMSE" },
  { value: "euclidean", label: "Euclidean" },
  { value: "cosine", label: "Cosine" },
];

const SERIES_OPTIONS: { value: SeriesCategory; label: string; enabled: boolean }[] = [
  { value: "prices", label: "Prices", enabled: true },
  { value: "load", label: "Load", enabled: false },
  { value: "generation", label: "Generation", enabled: false },
  { value: "transmission", label: "Transmission", enabled: false },
];

const QUICK_DATE_PRESETS: { value: QuickDatePreset; label: string }[] = [
  { value: "all", label: "All" },
  { value: "6m", label: "6M" },
  { value: "1y", label: "1Y" },
  { value: "2y", label: "2Y" },
  { value: "5y", label: "5Y" },
];

const DAY_LABELS = ["S", "M", "T", "W", "T", "F", "S"]; // 0=Sun..6=Sat
const MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

const SELECT_CLS =
  "rounded-md border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-200 focus:border-gray-500 focus:outline-none";

const LABEL_CLS = "text-[10px] font-bold uppercase tracking-wider text-gray-500";

const CHIP_ACTIVE = "bg-cyan-700/60 text-cyan-100 border-cyan-600";
const CHIP_INACTIVE = "bg-gray-800 text-gray-500 border-gray-700 hover:text-gray-300";
const CHIP_BASE = "rounded border px-2 py-1 text-xs font-medium transition-colors cursor-pointer select-none";

const QUICK_BTN = "rounded px-2 py-0.5 text-[10px] font-medium transition-colors";
const QUICK_BTN_DEFAULT = `${QUICK_BTN} bg-gray-800 text-gray-400 hover:bg-gray-700 hover:text-gray-200`;
const QUICK_BTN_ACCENT = `${QUICK_BTN} bg-cyan-800/50 text-cyan-300 hover:bg-cyan-700/50`;

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

function tomorrowStr(): string {
  const d = new Date();
  d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
}

function dateOffsetStr(years: number = 0, months: number = 0): string {
  const d = new Date();
  d.setFullYear(d.getFullYear() - years);
  d.setMonth(d.getMonth() - months);
  return d.toISOString().slice(0, 10);
}

function presetToStartDate(preset: QuickDatePreset): string {
  switch (preset) {
    case "6m": return dateOffsetStr(0, 6);
    case "1y": return dateOffsetStr(1);
    case "2y": return dateOffsetStr(2);
    case "5y": return dateOffsetStr(5);
    default: return "";
  }
}

function dayOfWeek(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00");
  return d.toLocaleDateString("en-US", { weekday: "short" });
}

function featureKey(f: { market: string; col: string }): string {
  return `${f.market}.${f.col}`;
}

function featureLabel(market: string, col: LmpComponent): string {
  const mkt = MARKET_OPTIONS.find((m) => m.value === market);
  const comp = COMPONENT_OPTIONS.find((c) => c.value === col);
  return `${(mkt?.value ?? market).toUpperCase()} ${comp?.label ?? col}`;
}

/** Toggle a value in a Set, return new Set */
function toggleInSet<T>(set: Set<T>, val: T): Set<T> {
  const next = new Set(set);
  if (next.has(val)) next.delete(val);
  else next.add(val);
  return next;
}

/** Invert selection against a full list */
function invertSet<T>(set: Set<T>, all: T[]): Set<T> {
  return new Set(all.filter((v) => !set.has(v)));
}

/* ------------------------------------------------------------------ */
/*  Toggle Grid Component                                              */
/* ------------------------------------------------------------------ */

function ToggleGrid<T extends string | number>({
  label,
  items,
  labels,
  selected,
  onChange,
  quickButtons,
  columns,
}: {
  label: string;
  items: T[];
  labels: string[];
  selected: Set<T>;
  onChange: (next: Set<T>) => void;
  quickButtons: { label: string; action: () => void; accent?: boolean }[];
  columns: number;
}) {
  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900/40 p-3">
      <p className={`${LABEL_CLS} mb-2`}>{label}</p>
      {/* Quick buttons */}
      <div className="mb-2 flex flex-wrap gap-1">
        {quickButtons.map((btn) => (
          <button key={btn.label} onClick={btn.action} className={btn.accent ? QUICK_BTN_ACCENT : QUICK_BTN_DEFAULT}>
            {btn.label}
          </button>
        ))}
      </div>
      {/* Grid */}
      <div className="grid gap-1" style={{ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }}>
        {items.map((item, i) => {
          const isOn = selected.has(item);
          return (
            <button
              key={String(item)}
              onClick={() => onChange(toggleInSet(selected, item))}
              className={`${CHIP_BASE} ${isOn ? CHIP_ACTIVE : CHIP_INACTIVE}`}
            >
              {labels[i]}
            </button>
          );
        })}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Custom Tooltip                                                     */
/* ------------------------------------------------------------------ */

function LikeDayTooltip({
  active,
  payload,
  label,
  dateColors,
  targetDate,
}: {
  active?: boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  payload?: any[];
  label?: number;
  dateColors: Map<string, string>;
  targetDate: string;
}) {
  if (!active || !payload || payload.length === 0) return null;

  const entries: { date: string; value: number; color: string }[] = [];
  for (const p of payload) {
    if (p.value == null || p.dataKey === "hour") continue;
    entries.push({ date: p.dataKey as string, value: p.value, color: dateColors.get(p.dataKey) || "#4b5563" });
  }
  entries.sort((a, b) => b.value - a.value);

  return (
    <div className="rounded-lg border border-gray-700 bg-[#1f2937] px-3 py-2 text-xs shadow-xl">
      <p className="mb-1.5 font-semibold text-gray-200">HE {label}</p>
      {entries.map((e) => (
        <p key={e.date} className="flex items-center gap-1.5">
          <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: e.color }} />
          <span className="text-gray-400">
            {e.date}
            {e.date === targetDate && <span className="ml-1 text-yellow-500">(target)</span>}
            :
          </span>
          <span className="text-gray-200">{e.value.toFixed(2)}</span>
        </p>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export default function LikeDay() {
  /* ---------- Filter input state ---------- */
  const [seriesInput, setSeriesInput] = useState<SeriesCategory>("prices");
  const [targetDateInput, setTargetDateInput] = useState(tomorrowStr);
  const [hubInput] = useState("WESTERN HUB");
  const [nNeighborsInput, setNNeighborsInput] = useState(5);
  const [metricInput, setMetricInput] = useState("mae");
  const [rankingFeatures, setRankingFeatures] = useState<RankingFeature[]>([
    { market: "da", col: "lmp_total", weight: 1 },
  ]);
  const [chartSelection, setChartSelection] = useState<ChartSelection>({
    market: "da",
    col: "lmp_total",
  });

  /* ---------- Date Range ---------- */
  const [histStartInput, setHistStartInput] = useState(() => presetToStartDate("2y"));
  const [histEndInput, setHistEndInput] = useState("");
  const [activePreset, setActivePreset] = useState<QuickDatePreset>("2y");

  /* ---------- Hours / Days / Months ---------- */
  const [selectedHours, setSelectedHours] = useState<Set<number>>(new Set(ALL_HOURS));
  const [selectedDays, setSelectedDays] = useState<Set<number>>(new Set([0, 1, 2, 3, 4, 5, 6]));
  const [selectedMonths, setSelectedMonths] = useState<Set<number>>(new Set([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]));

  /* ---------- API response state ---------- */
  const [data, setData] = useState<LikeDayApiResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  /* ---------- Chart visibility ---------- */
  const [visibleDates, setVisibleDates] = useState<Set<string>>(new Set());

  /* ---------- Derived: set of already-selected feature keys ---------- */
  const selectedKeys = useMemo(
    () => new Set(rankingFeatures.map(featureKey)),
    [rankingFeatures]
  );

  /* ---------- Fetch like days ---------- */
  const fetchLikeDays = useCallback(() => {
    setLoading(true);
    setError(null);

    const params = new URLSearchParams({
      target_date: targetDateInput,
      hub: hubInput,
      n_neighbors: String(nNeighborsInput),
      metric: metricInput,
    });

    // Cross-market features param
    params.set(
      "features",
      rankingFeatures.map((f) => `${f.market}.${f.col}:${f.weight}`).join(",")
    );

    // Date range
    if (histStartInput) params.set("hist_start", histStartInput);
    if (histEndInput) params.set("hist_end", histEndInput);

    // Hours filter (only send if not all selected)
    if (selectedHours.size < 24 && selectedHours.size > 0) {
      params.set("hours", Array.from(selectedHours).sort((a, b) => a - b).join(","));
    }

    // Days filter (only send if not all selected)
    if (selectedDays.size < 7 && selectedDays.size > 0) {
      params.set("days_of_week", Array.from(selectedDays).sort((a, b) => a - b).join(","));
    }

    // Months filter (only send if not all selected)
    if (selectedMonths.size < 12 && selectedMonths.size > 0) {
      params.set("months", Array.from(selectedMonths).sort((a, b) => a - b).join(","));
    }

    fetch(`/api/pjm-like-day?${params}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((json: LikeDayApiResponse) => {
        setData(json);
        const allDates = [json.target_date, ...json.like_days.map((ld) => ld.date)];
        setVisibleDates(new Set(allDates));
      })
      .catch((err) => {
        setError(err.message || "Failed to load like-day data");
      })
      .finally(() => setLoading(false));
  }, [targetDateInput, hubInput, nNeighborsInput, metricInput, rankingFeatures, histStartInput, histEndInput, selectedHours, selectedDays, selectedMonths]);

  /* ---------- Chart data ---------- */
  const { chartData, allDates, dateColors, chartLabel } = useMemo(() => {
    const label = featureLabel(chartSelection.market, chartSelection.col);
    if (!data) return { chartData: [] as ChartEntry[], allDates: [] as string[], dateColors: new Map<string, string>(), chartLabel: label };

    const targetDate = data.target_date;
    const likeDates = data.like_days.map((ld) => ld.date);
    const allDates = [targetDate, ...likeDates];

    const dateColors = new Map<string, string>();
    allDates.forEach((d, i) => {
      dateColors.set(d, HIGHLIGHT_COLORS[i % HIGHLIGHT_COLORS.length]);
    });

    // Filter profiles by selected chart market, then index by component
    const profileMap = new Map<string, number>();
    for (const p of data.hourly_profiles) {
      if (p.market === chartSelection.market) {
        profileMap.set(`${p.date}|${p.hour_ending}`, p[chartSelection.col]);
      }
    }

    const chartData: ChartEntry[] = ALL_HOURS.map((h) => {
      const entry: ChartEntry = { hour: h };
      for (const dateStr of allDates) {
        entry[dateStr] = profileMap.get(`${dateStr}|${h}`) ?? null;
      }
      return entry;
    });

    return { chartData, allDates, dateColors, chartLabel: label };
  }, [data, chartSelection]);

  /* ---------- Y-axis domain ---------- */
  const yDomain = useMemo((): [number, number] => {
    let lo = Infinity;
    let hi = -Infinity;
    for (const entry of chartData) {
      for (const dateStr of visibleDates) {
        const val = entry[dateStr];
        if (typeof val === "number") {
          if (val < lo) lo = val;
          if (val > hi) hi = val;
        }
      }
    }
    if (lo === Infinity || hi === -Infinity) return [0, 100];
    const pad = (hi - lo) * 0.05 || 5;
    return [Math.floor(lo - pad), Math.ceil(hi + pad)];
  }, [chartData, visibleDates]);

  /* ---------- Toggle handlers ---------- */
  const toggleDate = useCallback((date: string) => {
    setVisibleDates((prev) => toggleInSet(prev, date));
  }, []);

  const selectOnly = useCallback((date: string) => {
    setVisibleDates(new Set([date]));
  }, []);

  const selectAllDates = useCallback(() => {
    setVisibleDates(new Set(allDates));
  }, [allDates]);

  const selectNoneDates = useCallback(() => {
    setVisibleDates(new Set());
  }, []);

  /* ---------- Render ---------- */
  return (
    <div className="space-y-4">
      {/* Title — full width */}
      <div>
        <h2 className="text-lg font-semibold text-white">
          {hubInput} — Like Day Analysis
        </h2>
        <p className="text-xs text-gray-500">
          Nearest-neighbor similarity on hourly feature vectors across markets.
        </p>
      </div>

      {/* ============================================================ */}
      {/*  Two-column layout: filter sidebar | main content            */}
      {/* ============================================================ */}
      <div className="flex gap-6">
        {/* ── Left: Filter Sidebar ── */}
        <aside className="w-72 flex-shrink-0 space-y-3">
          {/* Date Range */}
          <div className="rounded-lg border border-gray-800 bg-gray-900/40 p-3">
            <p className={`${LABEL_CLS} mb-2`}>Date Range</p>
            <div className="space-y-2">
              <div>
                <label className={`${LABEL_CLS} mb-1 block`}>Start</label>
                <input
                  type="date"
                  value={histStartInput}
                  onChange={(e) => { setHistStartInput(e.target.value); setActivePreset("all"); }}
                  className={`w-full ${SELECT_CLS}`}
                />
              </div>
              <div>
                <label className={`${LABEL_CLS} mb-1 block`}>End</label>
                <input
                  type="date"
                  value={histEndInput}
                  onChange={(e) => { setHistEndInput(e.target.value); setActivePreset("all"); }}
                  className={`w-full ${SELECT_CLS}`}
                  placeholder="Present"
                />
              </div>
            </div>
            {/* Quick presets */}
            <div className="mt-2 flex flex-wrap gap-1">
              {QUICK_DATE_PRESETS.map((p) => (
                <button
                  key={p.value}
                  onClick={() => {
                    setActivePreset(p.value);
                    setHistStartInput(presetToStartDate(p.value));
                    setHistEndInput("");
                  }}
                  className={activePreset === p.value ? QUICK_BTN_ACCENT : QUICK_BTN_DEFAULT}
                >
                  {p.label}
                </button>
              ))}
            </div>
          </div>

          {/* Hours */}
          <ToggleGrid
            label="Hours"
            items={ALL_HOURS}
            labels={ALL_HOURS.map(String)}
            selected={selectedHours}
            onChange={setSelectedHours}
            columns={8}
            quickButtons={[
              { label: "All", action: () => setSelectedHours(new Set(ALL_HOURS)) },
              { label: "None", action: () => setSelectedHours(new Set()) },
              { label: "Invert", action: () => setSelectedHours(invertSet(selectedHours, ALL_HOURS)) },
              { label: "7-22", action: () => setSelectedHours(new Set(ALL_HOURS.filter((h) => h >= 7 && h <= 22))), accent: true },
              { label: "8-23", action: () => setSelectedHours(new Set(ALL_HOURS.filter((h) => h >= 8 && h <= 23))), accent: true },
            ]}
          />

          {/* Days */}
          <ToggleGrid
            label="Days"
            items={[0, 1, 2, 3, 4, 5, 6]}
            labels={DAY_LABELS}
            selected={selectedDays}
            onChange={setSelectedDays}
            columns={7}
            quickButtons={[
              { label: "All", action: () => setSelectedDays(new Set([0, 1, 2, 3, 4, 5, 6])) },
              { label: "None", action: () => setSelectedDays(new Set()) },
              { label: "Invert", action: () => setSelectedDays(invertSet(selectedDays, [0, 1, 2, 3, 4, 5, 6])) },
            ]}
          />

          {/* Months */}
          <ToggleGrid
            label="Months"
            items={[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]}
            labels={MONTH_LABELS}
            selected={selectedMonths}
            onChange={setSelectedMonths}
            columns={6}
            quickButtons={[
              { label: "All", action: () => setSelectedMonths(new Set([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])) },
              { label: "None", action: () => setSelectedMonths(new Set()) },
              { label: "Invert", action: () => setSelectedMonths(invertSet(selectedMonths, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])) },
            ]}
          />
        </aside>

        {/* ── Right: Main Content ── */}
        <div className="min-w-0 flex-1 space-y-4">
          {/* Card: Data Series */}
          <div className="rounded-lg border border-gray-800 bg-gray-900/40 p-4">
            <p className={`${LABEL_CLS} mb-3`}>Data Series</p>

            {/* Series chips */}
            <div className="mb-4 flex flex-wrap gap-1.5">
              {SERIES_OPTIONS.map((s) => {
                const isActive = seriesInput === s.value;
                return (
                  <button
                    key={s.value}
                    onClick={() => s.enabled && setSeriesInput(s.value)}
                    disabled={!s.enabled}
                    title={s.enabled ? undefined : "Coming Soon"}
                    className={`${CHIP_BASE} ${
                      !s.enabled
                        ? "cursor-not-allowed border-gray-800 bg-gray-800/40 text-gray-600 opacity-50"
                        : isActive
                          ? CHIP_ACTIVE
                          : CHIP_INACTIVE
                    }`}
                  >
                    {s.label}
                  </button>
                );
              })}
            </div>

            {/* Sub-selectors for Prices */}
            {seriesInput === "prices" && (
              <div className="space-y-3">
                <div className="flex flex-col gap-1">
                  <label className={LABEL_CLS}>Location</label>
                  <span className="rounded-md border border-gray-700 bg-gray-800/60 px-3 py-1.5 text-sm text-gray-300">
                    WESTERN HUB
                  </span>
                </div>

                {/* Ranking Features */}
                <div>
                  <label className={`${LABEL_CLS} mb-1.5 block`}>Ranking Features</label>
                  <div className="space-y-1.5">
                    {rankingFeatures.map((feat, idx) => {
                      const label = featureLabel(feat.market, feat.col);
                      return (
                        <div key={featureKey(feat)} className="flex items-center gap-2 rounded border border-gray-700 bg-gray-800/40 px-2.5 py-1.5">
                          <span className="flex-1 text-sm text-gray-200">{label}</span>
                          <label className="text-[10px] text-gray-500">Weight</label>
                          <input
                            type="number"
                            min={0.1}
                            max={10}
                            step={0.1}
                            value={feat.weight}
                            onChange={(e) => {
                              const next = [...rankingFeatures];
                              next[idx] = { ...next[idx], weight: parseFloat(e.target.value) || 1 };
                              setRankingFeatures(next);
                            }}
                            className="w-16 rounded border border-gray-600 bg-gray-900 px-1.5 py-0.5 text-center text-xs text-gray-200 focus:border-gray-400 focus:outline-none"
                          />
                          {rankingFeatures.length > 1 && (
                            <button
                              onClick={() => setRankingFeatures(rankingFeatures.filter((_, i) => i !== idx))}
                              className="ml-1 text-gray-600 hover:text-red-400 transition-colors"
                              title="Remove feature"
                            >
                              ×
                            </button>
                          )}
                        </div>
                      );
                    })}
                  </div>
                  {/* Add feature — grouped by market */}
                  {rankingFeatures.length < FEATURE_CATALOG.length && (
                    <div className="mt-1.5">
                      <select
                        value=""
                        onChange={(e) => {
                          if (!e.target.value) return;
                          const entry = FEATURE_CATALOG.find((f) => f.key === e.target.value);
                          if (entry) {
                            setRankingFeatures([
                              ...rankingFeatures,
                              { market: entry.market, col: entry.col, weight: 1 },
                            ]);
                          }
                        }}
                        className={`${SELECT_CLS} text-gray-500`}
                      >
                        <option value="">+ Add Feature</option>
                        {MARKET_OPTIONS.map((m) => {
                          const available = FEATURE_CATALOG.filter(
                            (f) => f.market === m.value && !selectedKeys.has(f.key)
                          );
                          if (available.length === 0) return null;
                          return (
                            <optgroup key={m.value} label={m.label}>
                              {available.map((f) => (
                                <option key={f.key} value={f.key}>{f.label}</option>
                              ))}
                            </optgroup>
                          );
                        })}
                      </select>
                    </div>
                  )}
                </div>

                <div className="flex flex-col gap-1">
                  <label className={LABEL_CLS}>Target Date</label>
                  <input
                    type="date"
                    value={targetDateInput}
                    onChange={(e) => setTargetDateInput(e.target.value)}
                    className={`w-48 ${SELECT_CLS}`}
                  />
                </div>
              </div>
            )}
          </div>

          {/* Card: Like Day Settings */}
          <div className="rounded-lg border border-gray-800 bg-gray-900/40 p-4">
            <p className={`${LABEL_CLS} mb-3`}>Like Day Settings</p>
            <div className="flex items-end gap-4">
              <div className="flex flex-col gap-1">
                <label className={LABEL_CLS}>Neighbors</label>
                <input
                  type="number"
                  min={1}
                  max={20}
                  value={nNeighborsInput}
                  onChange={(e) => setNNeighborsInput(parseInt(e.target.value, 10) || 5)}
                  className={`w-20 ${SELECT_CLS}`}
                />
              </div>

              <div className="flex flex-col gap-1">
                <label className={LABEL_CLS}>Metric</label>
                <select value={metricInput} onChange={(e) => setMetricInput(e.target.value)} className={SELECT_CLS}>
                  {METRIC_OPTIONS.map((o) => (
                    <option key={o.value} value={o.value}>{o.label}</option>
                  ))}
                </select>
              </div>

              <button
                onClick={fetchLikeDays}
                disabled={loading}
                className="rounded-md bg-blue-600 px-5 py-1.5 text-sm font-medium text-white transition-colors hover:bg-blue-500 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {loading ? "Searching\u2026" : "Find Like Days"}
              </button>
            </div>
          </div>

          {/* Loading */}
          {loading && (
            <div className="flex h-48 items-center justify-center">
              <p className="text-gray-500">Searching for like days\u2026</p>
            </div>
          )}

          {/* Error */}
          {error && !loading && (
            <div className="flex h-48 items-center justify-center">
              <p className="text-red-400">{error}</p>
            </div>
          )}

          {/* Results */}
          {!loading && !error && data && (
            <>
              {/* Results table */}
              <div className="overflow-x-auto rounded-xl border border-gray-800">
                <table className="w-full border-collapse text-sm">
                  <thead>
                    <tr>
                      <th className="border-b border-gray-700 px-4 py-2.5 text-left text-xs font-medium text-gray-400">Rank</th>
                      <th className="border-b border-gray-700 px-4 py-2.5 text-left text-xs font-medium text-gray-400">Date</th>
                      <th className="border-b border-gray-700 px-4 py-2.5 text-left text-xs font-medium text-gray-400">Day</th>
                      <th className="border-b border-gray-700 px-4 py-2.5 text-right text-xs font-medium text-gray-400">Distance</th>
                      <th className="border-b border-gray-700 px-4 py-2.5 text-right text-xs font-medium text-gray-400">Similarity</th>
                    </tr>
                  </thead>
                  <tbody>
                    {/* Target date row */}
                    <tr className="border-b border-gray-800/50 bg-blue-900/20">
                      <td className="px-4 py-2 font-medium text-yellow-500">Target</td>
                      <td className="px-4 py-2 font-medium text-white">{data.target_date}</td>
                      <td className="px-4 py-2 text-gray-300">{dayOfWeek(data.target_date)}</td>
                      <td className="px-4 py-2 text-right text-gray-500">&mdash;</td>
                      <td className="px-4 py-2 text-right text-gray-500">&mdash;</td>
                    </tr>
                    {/* Like day rows */}
                    {data.like_days.map((ld, idx) => (
                      <tr
                        key={ld.date}
                        className={`border-b border-gray-800/50 hover:bg-gray-800/30 ${
                          idx % 2 === 0 ? "bg-[#0f1117]" : "bg-[#12141d]"
                        }`}
                      >
                        <td className="px-4 py-2 text-gray-300">{ld.rank}</td>
                        <td className="px-4 py-2 font-medium text-white">{ld.date}</td>
                        <td className="px-4 py-2 text-gray-300">{dayOfWeek(ld.date)}</td>
                        <td className="px-4 py-2 text-right text-gray-300">{ld.distance.toFixed(4)}</td>
                        <td className="px-4 py-2 text-right text-gray-300">{(ld.similarity * 100).toFixed(1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Chart */}
              {chartData.length > 0 && (
                <div className="rounded-xl border border-gray-800 bg-[#0f1117] p-4">
                  <div className="mb-3 flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <select
                        value={`${chartSelection.market}.${chartSelection.col}`}
                        onChange={(e) => {
                          const [mkt, ...colParts] = e.target.value.split(".");
                          const col = colParts.join(".") as LmpComponent;
                          setChartSelection({ market: mkt, col });
                        }}
                        className="rounded border border-gray-700 bg-gray-800 px-2 py-0.5 text-xs font-semibold text-gray-300 focus:border-gray-500 focus:outline-none"
                      >
                        {MARKET_OPTIONS.map((m) => (
                          <optgroup key={m.value} label={m.label}>
                            {COMPONENT_OPTIONS.map((c) => (
                              <option key={`${m.value}.${c.value}`} value={`${m.value}.${c.value}`}>
                                {m.value.toUpperCase()} {c.label}
                              </option>
                            ))}
                          </optgroup>
                        ))}
                      </select>
                      <span className="text-xs text-gray-500">&mdash; Hourly Profile Comparison</span>
                    </div>
                    <div className="flex gap-2">
                      <button
                        onClick={selectAllDates}
                        className="rounded px-2 py-0.5 text-[10px] font-medium text-gray-400 transition-colors hover:bg-gray-800 hover:text-gray-200"
                      >
                        All
                      </button>
                      <button
                        onClick={selectNoneDates}
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
                        label={{
                          value: "Hour Ending",
                          position: "insideBottom",
                          offset: -4,
                          fill: "#6b7280",
                          fontSize: 11,
                        }}
                      />
                      <YAxis
                        domain={yDomain}
                        allowDataOverflow
                        tick={{ fill: "#9ca3af", fontSize: 11 }}
                        label={{
                          value: chartLabel,
                          angle: -90,
                          position: "insideLeft",
                          offset: 10,
                          fill: "#6b7280",
                          fontSize: 11,
                        }}
                      />
                      <Tooltip
                        content={
                          <LikeDayTooltip
                            dateColors={dateColors}
                            targetDate={data.target_date}
                          />
                        }
                      />
                      {allDates.map((dateStr) => {
                        const isVisible = visibleDates.has(dateStr);
                        const color = dateColors.get(dateStr) || "#4b5563";
                        const isTarget = dateStr === data.target_date;
                        return (
                          <Line
                            key={dateStr}
                            type="monotone"
                            dataKey={dateStr}
                            stroke={isVisible ? color : "#4b5563"}
                            strokeWidth={isVisible ? (isTarget ? 3 : 2) : 0.5}
                            strokeOpacity={isVisible ? 1 : 0.15}
                            strokeDasharray={isTarget ? undefined : "5 3"}
                            dot={false}
                            legendType="none"
                          />
                        );
                      })}
                    </LineChart>
                  </ResponsiveContainer>

                  {/* Date chip toggles */}
                  <div className="mt-3 flex flex-wrap items-center gap-1.5">
                    {allDates.map((dateStr) => {
                      const isVisible = visibleDates.has(dateStr);
                      const color = dateColors.get(dateStr);
                      const isTarget = dateStr === data.target_date;
                      return (
                        <button
                          key={dateStr}
                          onClick={() => toggleDate(dateStr)}
                          onDoubleClick={() => selectOnly(dateStr)}
                          className={`rounded px-2 py-0.5 text-[10px] font-medium transition-all ${
                            isVisible ? "text-white" : "text-gray-600 hover:text-gray-400"
                          }`}
                          style={
                            isVisible && color
                              ? { backgroundColor: color + "33", border: `1px solid ${color}` }
                              : { border: "1px solid transparent" }
                          }
                          title="Click to toggle · Double-click to solo"
                        >
                          {dateStr}{isTarget ? " (target)" : ""}
                        </button>
                      );
                    })}
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
