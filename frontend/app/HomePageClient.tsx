"use client";

import { useState } from "react";
import Sidebar, { type ActiveSection } from "@/components/Sidebar";
import PjmLmpsHourlyTable from "@/components/power/PjmLmpsHourlyTable";
import LikeDay from "@/components/power/LikeDay";

const SECTION_META: Record<ActiveSection, { title: string; subtitle: string; footer: string }> = {
  "pjm-lmps-hourly": {
    title: "PJM LMPs Hourly",
    subtitle: "Hourly locational marginal prices from PJM.",
    footer: "PJM LMP hourly data | Source: Azure PostgreSQL",
  },
  "like-day": {
    title: "Like Day Analysis",
    subtitle: "Find historically similar days based on LMP feature similarity.",
    footer: "Like Day analysis | Source: Python FastAPI + Azure PostgreSQL",
  },
};

export default function HomePageClient() {
  const [activeSection, setActiveSection] = useState<ActiveSection>("pjm-lmps-hourly");
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
          <div className="rounded-xl border border-gray-800 bg-gray-900/60 p-6 shadow-2xl">
            {activeSection === "pjm-lmps-hourly" && <PjmLmpsHourlyTable />}
            {activeSection === "like-day" && <LikeDay />}
          </div>
          <p className="mt-6 text-center text-xs text-gray-600">{meta.footer}</p>
        </main>
      </div>
    </div>
  );
}
