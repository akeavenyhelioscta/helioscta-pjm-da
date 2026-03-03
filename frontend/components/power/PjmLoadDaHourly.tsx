"use client";

import { PjmLoadHourlyTable } from "./PjmLoadRtMeteredHourly";

export default function PjmLoadDaHourly() {
  return <PjmLoadHourlyTable apiPath="/api/pjm-load-da-hourly" loadTypeLabel="DA Load" valueKey="da_load_mw" />;
}
