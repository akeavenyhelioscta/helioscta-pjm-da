import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const PYTHON_API_URL =
  process.env.PYTHON_API_URL || "http://localhost:8000";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  const forecast_date = searchParams.get("forecast_date") || "";
  const n_analogs = searchParams.get("n_analogs") || "30";
  const weight_method = searchParams.get("weight_method") || "inverse_distance";

  const params = new URLSearchParams({ n_analogs, weight_method });
  if (forecast_date) params.set("forecast_date", forecast_date);

  try {
    const res = await fetch(`${PYTHON_API_URL}/like-day-forecast?${params}`, {
      method: "POST",
    });

    if (!res.ok) {
      const text = await res.text();
      console.error("[pjm-like-day-forecast] Python API error:", res.status, text);
      return NextResponse.json(
        { error: "Like-day forecast service error", detail: text },
        { status: res.status }
      );
    }

    const data = await res.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error("[pjm-like-day-forecast] Failed to reach Python API:", error);
    return NextResponse.json(
      { error: "Failed to reach like-day forecast service" },
      { status: 502 }
    );
  }
}
