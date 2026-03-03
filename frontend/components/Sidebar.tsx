"use client";

import { useState } from "react";

export type ActiveSection = "dashboard" | "pjm-lmps-hourly" | "pjm-load-da-hourly" | "pjm-load-rt-metered-hourly" | "pjm-load-rt-prelim-hourly" | "pjm-load-rt-instantaneous-hourly" | "pjm-load-forecast-hourly" | "load-forecast-performance" | "solar-forecast-performance" | "wind-forecast-performance" | "like-day" | "like-day-forecast";

interface SidebarProps {
  activeSection: ActiveSection;
  onSectionChange: (section: ActiveSection) => void;
}

interface NavItem {
  id: ActiveSection;
  label: string;
  iconPath: string;
  iconColor: string;
}

interface NavSection {
  title: string;
  items: NavItem[];
}

const NAV_SECTIONS: NavSection[] = [
  {
    title: "Overview",
    items: [
      {
        id: "dashboard",
        label: "Dashboard",
        iconPath: "M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0a1 1 0 01-1-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 01-1 1m-4 0h4",
        iconColor: "text-emerald-400",
      },
    ],
  },
  {
    title: "Data Sources",
    items: [
      {
        id: "pjm-lmps-hourly",
        label: "PJM LMPs Hourly",
        iconPath: "M13 10V3L4 14h7v7l9-11h-7z",
        iconColor: "text-yellow-500",
      },
      {
        id: "pjm-load-da-hourly",
        label: "PJM Load DA",
        iconPath: "M3 13h2v-2H3v2zm0 4h2v-2H3v2zm0-8h2V7H3v2zm4 4h14v-2H7v2zm0 4h14v-2H7v2zM7 7v2h14V7H7z",
        iconColor: "text-orange-400",
      },
      {
        id: "pjm-load-rt-metered-hourly",
        label: "PJM Load RT Metered",
        iconPath: "M3 13h2v-2H3v2zm0 4h2v-2H3v2zm0-8h2V7H3v2zm4 4h14v-2H7v2zm0 4h14v-2H7v2zM7 7v2h14V7H7z",
        iconColor: "text-green-400",
      },
      {
        id: "pjm-load-rt-prelim-hourly",
        label: "PJM Load RT Prelim",
        iconPath: "M3 13h2v-2H3v2zm0 4h2v-2H3v2zm0-8h2V7H3v2zm4 4h14v-2H7v2zm0 4h14v-2H7v2zM7 7v2h14V7H7z",
        iconColor: "text-teal-400",
      },
      {
        id: "pjm-load-rt-instantaneous-hourly",
        label: "PJM Load RT Instant",
        iconPath: "M3 13h2v-2H3v2zm0 4h2v-2H3v2zm0-8h2V7H3v2zm4 4h14v-2H7v2zm0 4h14v-2H7v2zM7 7v2h14V7H7z",
        iconColor: "text-cyan-400",
      },
      {
        id: "pjm-load-forecast-hourly",
        label: "PJM Load Forecast",
        iconPath: "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
        iconColor: "text-purple-400",
      },
    ],
  },
  {
    title: "Analysis",
    items: [
      {
        id: "load-forecast-performance",
        label: "Load Forecast Perf",
        iconPath: "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
        iconColor: "text-rose-400",
      },
      {
        id: "solar-forecast-performance",
        label: "Solar Forecast Perf",
        iconPath: "M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z",
        iconColor: "text-yellow-400",
      },
      {
        id: "wind-forecast-performance",
        label: "Wind Forecast Perf",
        iconPath: "M14 10l-2 1m0 0l-2-1m2 1v2.5M20 7l-2 1m2-1l-2-1m2 1v2.5M14 4l-2-1-2 1M4 7l2-1M4 7l2 1M4 7v2.5M12 21l-2-1m2 1l2-1m-2 1v-2.5M6 18l-2-1v-2.5M18 18l2-1v-2.5",
        iconColor: "text-cyan-400",
      },
      {
        id: "like-day",
        label: "Like Day",
        iconPath: "M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z",
        iconColor: "text-blue-400",
      },
    ],
  },
  {
    title: "Forecasts",
    items: [
      {
        id: "like-day-forecast",
        label: "Like-Day Forecast",
        iconPath: "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
        iconColor: "text-amber-400",
      },
    ],
  },
];

export default function Sidebar({ activeSection, onSectionChange }: SidebarProps) {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside
      className={`flex flex-col border-r border-gray-800 bg-[#0b0d14] transition-all duration-200 ${
        collapsed ? "w-14" : "w-56"
      }`}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-4">
        {!collapsed && (
          <span className="text-xs font-bold uppercase tracking-widest text-gray-500">
            Helios CTA
          </span>
        )}
        <button
          onClick={() => setCollapsed((v) => !v)}
          className="rounded p-1 text-gray-500 transition-colors hover:bg-gray-800 hover:text-gray-300"
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className={`h-4 w-4 transition-transform ${collapsed ? "rotate-180" : ""}`}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M15 19l-7-7 7-7" />
          </svg>
        </button>
      </div>

      {/* Nav */}
      <nav className="flex-1 px-2 py-2">
        {NAV_SECTIONS.map((section) => (
          <div key={section.title} className="mb-4">
            {!collapsed && (
              <p className="mb-1 px-3 text-[10px] font-semibold uppercase tracking-widest text-gray-600">
                {section.title}
              </p>
            )}
            <div className="space-y-1">
              {section.items.map((item) => {
                const isActive = activeSection === item.id;
                return (
                  <button
                    key={item.id}
                    onClick={() => onSectionChange(item.id)}
                    className={`flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors ${
                      collapsed ? "justify-center px-0" : ""
                    } ${
                      isActive
                        ? "bg-gray-800/60 text-white"
                        : "text-gray-400 hover:bg-gray-800/40 hover:text-gray-200"
                    }`}
                  >
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className={`h-4 w-4 flex-shrink-0 ${item.iconColor}`}
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      strokeWidth={2}
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" d={item.iconPath} />
                    </svg>
                    {!collapsed && <span>{item.label}</span>}
                  </button>
                );
              })}
            </div>
          </div>
        ))}
      </nav>

      {/* Footer */}
      {!collapsed && (
        <div className="border-t border-gray-800 px-3 py-3">
          <p className="text-[10px] text-gray-600">Source: Azure PostgreSQL</p>
        </div>
      )}
    </aside>
  );
}
