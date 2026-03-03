"use client";

import { PjmLoadHourlyTable } from "./PjmLoadRtMeteredHourly";

export default function PjmLoadRtPrelimHourly() {
  return <PjmLoadHourlyTable apiPath="/api/pjm-load-rt-prelim-hourly" loadTypeLabel="RT Prelim Load" />;
}
