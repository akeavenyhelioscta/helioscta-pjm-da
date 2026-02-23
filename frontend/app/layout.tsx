import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "PJM LMPs | Helios CTA",
  description: "Hourly locational marginal prices from PJM",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-[#0f1117] text-gray-100 antialiased">
        {children}
      </body>
    </html>
  );
}
