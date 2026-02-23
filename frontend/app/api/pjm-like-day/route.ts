import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const PYTHON_API_URL =
  process.env.PYTHON_API_URL || "http://localhost:8000";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);

  const target_date = searchParams.get("target_date") || "";
  const hub = searchParams.get("hub") || "WESTERN HUB";
  const n_neighbors = searchParams.get("n_neighbors") || "5";
  const metric = searchParams.get("metric") || "cosine";

  // Optional filters
  const hist_start = searchParams.get("hist_start") || "";
  const hist_end = searchParams.get("hist_end") || "";
  const hours = searchParams.get("hours") || "";
  const days_of_week = searchParams.get("days_of_week") || "";
  const months = searchParams.get("months") || "";
  const features = searchParams.get("features") || "";

  const params = new URLSearchParams({
    hub,
    n_neighbors,
    metric,
  });
  if (target_date) params.set("target_date", target_date);
  if (hist_start) params.set("hist_start", hist_start);
  if (hist_end) params.set("hist_end", hist_end);
  if (hours) params.set("hours", hours);
  if (days_of_week) params.set("days_of_week", days_of_week);
  if (months) params.set("months", months);
  if (features) params.set("features", features);

  try {
    const res = await fetch(`${PYTHON_API_URL}/like-day?${params}`, {
      method: "POST",
    });

    if (!res.ok) {
      const text = await res.text();
      console.error("[pjm-like-day] Python API error:", res.status, text);
      return NextResponse.json(
        { error: "Like-day service error", detail: text },
        { status: res.status }
      );
    }

    const data = await res.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error("[pjm-like-day] Failed to reach Python API:", error);
    return NextResponse.json(
      { error: "Failed to reach like-day service" },
      { status: 502 }
    );
  }
}
