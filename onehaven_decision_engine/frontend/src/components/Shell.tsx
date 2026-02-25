import React from "react";
import { NavLink, useLocation } from "react-router-dom";
import clsx from "clsx";
import AuroraBackground from "./AuroraBackground";
import AgentSlots from "./AgentSlots";
import { useAuth } from "../lib/auth";
import { getOrgSlug } from "../lib/api";

const nav = [
  { to: "/dashboard", label: "Dashboard" },
  { to: "/properties", label: "Properties" },
  { to: "/agents", label: "Agents" },
  { to: "/constitution", label: "Operating Truth" },
];

function TopRightStatus() {
  const { principal, loading, logout } = useAuth();
  const loc = useLocation();

  const inAuthScreen =
    loc.pathname.startsWith("/login") || loc.pathname.startsWith("/register");

  if (inAuthScreen) return null;

  const orgSlug = principal?.org_slug || getOrgSlug();

  return (
    <div className="flex items-center gap-2">
      <div className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2">
        <div className="text-[10px] text-zinc-400">Org</div>
        <div className="text-xs font-semibold text-zinc-100">{orgSlug}</div>
      </div>

      {principal ? (
        <>
          <div className="hidden md:block rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2">
            <div className="text-[10px] text-zinc-400">
              {principal.role} • {principal.plan_code || "free"}
            </div>
            <div className="text-xs font-semibold text-zinc-100">
              {principal.email}
            </div>
          </div>

          <button
            onClick={() => logout()}
            disabled={loading}
            className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm text-zinc-200 hover:bg-white/[0.06] hover:border-white/20 disabled:opacity-60"
          >
            Logout
          </button>
        </>
      ) : (
        <NavLink
          to="/login"
          className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm text-zinc-200 hover:bg-white/[0.06] hover:border-white/20"
        >
          Login
        </NavLink>
      )}
    </div>
  );
}

export default function Shell({ children }: { children: React.ReactNode }) {
  const { principal } = useAuth();
  const loc = useLocation();

  const inAuthScreen =
    loc.pathname.startsWith("/login") || loc.pathname.startsWith("/register");

  return (
    <div className="h-screen w-screen overflow-hidden bg-black text-zinc-100">
      <div className="relative h-full w-full">
        <AuroraBackground />

        {/* Top bar (subtle) */}
        <div className="absolute top-0 left-0 right-0 z-20 px-5 py-4">
          <div className="max-w-[1400px] mx-auto flex items-center justify-end">
            <TopRightStatus />
          </div>
        </div>

        <div className="relative h-full w-full flex overflow-hidden pt-16">
          {/* Sidebar */}
          <aside className="w-80 p-5 flex flex-col gap-5 border-r border-white/10 bg-black/45 backdrop-blur-xl overflow-y-auto">
            <div className="gradient-border rounded-2xl glass p-4">
              <div className="flex items-center gap-3">
                <div className="h-10 w-10 rounded-xl bg-white/[0.06] border border-white/[0.10]" />
                <div>
                  <div className="text-sm font-semibold tracking-tight">
                    OneHaven
                  </div>
                  <div className="text-xs text-zinc-400">
                    Centralized Real Estate Dashboard
                  </div>
                </div>
              </div>
            </div>

            <nav className="flex flex-col gap-1">
              {nav.map((n) => (
                <NavLink
                  key={n.to}
                  to={n.to}
                  className={({ isActive }) =>
                    clsx(
                      "px-3 py-2.5 rounded-xl text-sm transition border",
                      isActive
                        ? "bg-white/[0.06] text-zinc-100 border-white/[0.14]"
                        : "text-zinc-300 border-transparent hover:bg-white/[0.04] hover:border-white/[0.10]",
                    )
                  }
                >
                  {n.label}
                </NavLink>
              ))}
            </nav>

            {/* ✅ Only show “operational” widgets when authed and not on auth screens */}
            {principal && !inAuthScreen ? <AgentSlots /> : null}

            <div className="mt-auto text-xs text-zinc-500 leading-relaxed">
              Built for ruthless deal clarity.
              <div className="text-[11px] text-zinc-600 mt-2">
                Tip: keep the UI calm. Make the system loud.
              </div>
            </div>
          </aside>

          {/* Main */}
          <main className="flex-1 overflow-y-auto">
            <div className="min-h-full">{children}</div>
          </main>
        </div>
      </div>
    </div>
  );
}
