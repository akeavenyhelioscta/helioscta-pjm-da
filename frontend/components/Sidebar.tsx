"use client";

import { useState } from "react";

export type ActiveSection = "pjm-lmps-hourly" | "like-day";

interface SidebarProps {
  activeSection: ActiveSection;
  onSectionChange: (section: ActiveSection) => void;
}

const NAV_ITEMS: { id: ActiveSection; label: string; iconPath: string; iconColor: string }[] = [
  {
    id: "pjm-lmps-hourly",
    label: "PJM LMPs Hourly",
    iconPath: "M13 10V3L4 14h7v7l9-11h-7z",
    iconColor: "text-yellow-500",
  },
  {
    id: "like-day",
    label: "Like Day",
    iconPath: "M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z",
    iconColor: "text-blue-400",
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
      <nav className="flex-1 space-y-1 px-2 py-2">
        {NAV_ITEMS.map((item) => {
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
