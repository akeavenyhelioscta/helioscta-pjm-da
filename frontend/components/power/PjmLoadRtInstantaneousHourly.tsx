"use client";

import { PjmLoadHourlyTable } from "./PjmLoadRtMeteredHourly";

export default function PjmLoadRtInstantaneousHourly() {
  return <PjmLoadHourlyTable apiPath="/api/pjm-load-rt-instantaneous-hourly" loadTypeLabel="RT Instantaneous Load" />;
}
