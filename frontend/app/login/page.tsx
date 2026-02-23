import { signIn } from "@/auth";

export default function LoginPage({
  searchParams,
}: {
  searchParams: Promise<{ error?: string }>;
}) {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center bg-[#0f1117]">
      <div className="w-full max-w-sm rounded-2xl border border-gray-700 bg-[#1a1d27] p-8 shadow-xl">
        <h1 className="mb-2 text-center text-2xl font-bold text-white">Helios CTA</h1>
        <p className="mb-8 text-center text-sm text-gray-400">Sign in to access the dashboard</p>

        <AccessDeniedMessage searchParams={searchParams} />

        <form
          action={async () => {
            "use server";
            await signIn("microsoft-entra-id", { redirectTo: "/" });
          }}
        >
          <button
            type="submit"
            className="flex w-full items-center justify-center gap-3 rounded-lg bg-[#0078d4] px-4 py-3 text-sm font-semibold text-white transition-colors hover:bg-[#106ebe]"
          >
            <MicrosoftIcon />
            Sign in with Microsoft
          </button>
        </form>
      </div>
    </main>
  );
}

async function AccessDeniedMessage({ searchParams }: { searchParams: Promise<{ error?: string }> }) {
  const params = await searchParams;
  if (params.error !== "AccessDenied") return null;
  return (
    <div className="mb-6 rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-400">
      Your email is not authorised to access this app. Contact your administrator.
    </div>
  );
}

function MicrosoftIcon() {
  return (
    <svg width="20" height="20" viewBox="0 0 21 21" xmlns="http://www.w3.org/2000/svg">
      <rect x="1" y="1" width="9" height="9" fill="#f25022" />
      <rect x="11" y="1" width="9" height="9" fill="#7fba00" />
      <rect x="1" y="11" width="9" height="9" fill="#00a4ef" />
      <rect x="11" y="11" width="9" height="9" fill="#ffb900" />
    </svg>
  );
}
