import { Suspense } from "react";
import HomePageClient from "./HomePageClient";

export default function Page() {
  return (
    <Suspense
      fallback={
        <main className="mx-auto max-w-6xl px-4 py-10 sm:px-8">
          <div className="h-[520px] animate-pulse rounded-xl border border-gray-800 bg-gray-900/60" />
        </main>
      }
    >
      <HomePageClient />
    </Suspense>
  );
}
