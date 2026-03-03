"use client";

import { useState } from "react";
import Sidebar, { type ActiveSection } from "@/components/Sidebar";
import PjmLmpsHourlyTable from "@/components/power/PjmLmpsHourlyTable";
import PjmLoadRtMeteredHourly from "@/components/power/PjmLoadRtMeteredHourly";
import PjmLoadRtPrelimHourly from "@/components/power/PjmLoadRtPrelimHourly";
import PjmLoadRtInstantaneousHourly from "@/components/power/PjmLoadRtInstantaneousHourly";
import PjmLoadDaHourly from "@/components/power/PjmLoadDaHourly";
import PjmLoadForecastHourly from "@/components/power/PjmLoadForecastHourly";
import LikeDay from "@/components/power/LikeDay";
import LikeDayForecast from "@/components/power/LikeDayForecast";
import LoadForecastPerformance from "@/components/power/LoadForecastPerformance";
import SolarForecastPerformance from "@/components/power/SolarForecastPerformance";
import WindForecastPerformance from "@/components/power/WindForecastPerformance";
import Dashboard from "@/components/power/Dashboard";

const SECTION_META: Record<ActiveSection, { title: string; subtitle: string; footer: string }> = {
  "dashboard": {
    title: "Dashboard",
    subtitle: "Real-time overview of PJM power market indicators.",
    footer: "Dashboard | Source: Azure PostgreSQL",
  },
  "pjm-lmps-hourly": {
    title: "PJM LMPs Hourly",
    subtitle: "Hourly locational marginal prices from PJM.",
    footer: "PJM LMP hourly data | Source: Azure PostgreSQL",
  },
  "pjm-load-da-hourly": {
    title: "PJM Load DA Hourly",
    subtitle: "Hourly day-ahead load by region from PJM.",
    footer: "PJM DA load hourly data | Source: Azure PostgreSQL",
  },
  "pjm-load-rt-metered-hourly": {
    title: "PJM Load RT Metered Hourly",
    subtitle: "Hourly real-time metered load by region from PJM.",
    footer: "PJM RT metered load hourly data | Source: Azure PostgreSQL",
  },
  "pjm-load-rt-prelim-hourly": {
    title: "PJM Load RT Prelim Hourly",
    subtitle: "Hourly real-time preliminary load by region from PJM.",
    footer: "PJM RT prelim load hourly data | Source: Azure PostgreSQL",
  },
  "pjm-load-rt-instantaneous-hourly": {
    title: "PJM Load RT Instantaneous Hourly",
    subtitle: "Hourly real-time instantaneous load by region from PJM.",
    footer: "PJM RT instantaneous load hourly data | Source: Azure PostgreSQL",
  },
  "pjm-load-forecast-hourly": {
    title: "PJM Load Forecast Hourly",
    subtitle: "Hourly load forecast by region from PJM.",
    footer: "PJM load forecast hourly data | Source: Azure PostgreSQL",
  },
  "load-forecast-performance": {
    title: "Load Forecast Performance",
    subtitle: "Forecast accuracy vs RT Prelim actuals and forecast evolution across PJM RTO, Mid Atlantic, South, and Western regions.",
    footer: "Load Forecast Performance | Source: Azure PostgreSQL",
  },
  "solar-forecast-performance": {
    title: "Solar Forecast Performance",
    subtitle: "Forecast accuracy vs Fuel Mix actuals for PJM solar generation (Grid-Scale + BTM).",
    footer: "Solar Forecast Performance | Source: Azure PostgreSQL",
  },
  "wind-forecast-performance": {
    title: "Wind Forecast Performance",
    subtitle: "Forecast accuracy vs Fuel Mix actuals for PJM wind generation.",
    footer: "Wind Forecast Performance | Source: Azure PostgreSQL",
  },
  "like-day": {
    title: "Like Day Analysis",
    subtitle: "Find historically similar days based on LMP feature similarity.",
    footer: "Like Day analysis | Source: Python FastAPI + Azure PostgreSQL",
  },
  "like-day-forecast": {
    title: "Like-Day DA LMP Forecast",
    subtitle: "Probabilistic day-ahead LMP forecast using analog day matching across 17 feature groups.",
    footer: "Like-Day Forecast | Source: Python FastAPI + Azure PostgreSQL",
  },
};

export default function HomePageClient() {
  const [activeSection, setActiveSection] = useState<ActiveSection>("dashboard");
  const meta = SECTION_META[activeSection];

  return (
    <div className="flex min-h-screen">
      <Sidebar activeSection={activeSection} onSectionChange={setActiveSection} />

      <div className="flex-1 overflow-auto">
        <main className="px-4 py-8 sm:px-8">
          <div className="mb-8">
            <p className="mb-1 text-xs font-semibold uppercase tracking-widest text-gray-500">
              Helios CTA | Power Markets
            </p>
            <h1 className="text-2xl font-bold text-gray-100 sm:text-3xl">{meta.title}</h1>
            <p className="mt-2 text-sm text-gray-500">{meta.subtitle}</p>
          </div>
          {activeSection === "dashboard" ? (
            <Dashboard />
          ) : (
            <div className="rounded-xl border border-gray-800 bg-gray-900/60 p-6 shadow-2xl">
              {activeSection === "pjm-lmps-hourly" && <PjmLmpsHourlyTable />}
              {activeSection === "pjm-load-rt-metered-hourly" && <PjmLoadRtMeteredHourly />}
              {activeSection === "pjm-load-rt-prelim-hourly" && <PjmLoadRtPrelimHourly />}
              {activeSection === "pjm-load-da-hourly" && <PjmLoadDaHourly />}
              {activeSection === "pjm-load-rt-instantaneous-hourly" && <PjmLoadRtInstantaneousHourly />}
              {activeSection === "pjm-load-forecast-hourly" && <PjmLoadForecastHourly />}
              {activeSection === "load-forecast-performance" && <LoadForecastPerformance />}
              {activeSection === "solar-forecast-performance" && <SolarForecastPerformance />}
              {activeSection === "wind-forecast-performance" && <WindForecastPerformance />}
              {activeSection === "like-day" && <LikeDay />}
              {activeSection === "like-day-forecast" && <LikeDayForecast />}
            </div>
          )}
          <p className="mt-6 text-center text-xs text-gray-600">{meta.footer}</p>
        </main>
      </div>
    </div>
  );
}
