// frontend/src/components/Shell.tsx
import React from "react";
import { NavLink } from "react-router-dom";
import clsx from "clsx";
import AuroraBackground from "./AuroraBackground";

const nav = [
  { to: "/dashboard", label: "Dashboard" },
  { to: "/agents", label: "Agents" },
  { to: "/constitution", label: "Operating Truth" },
];

export default function Shell({ children }: { children: React.ReactNode }) {
  return (
    <div className="h-screen w-screen overflow-hidden bg-black text-zinc-100">
      <div className="relative h-full w-full">
        <AuroraBackground />

        <div className="relative h-full w-full flex overflow-hidden">
          {/* Sidebar */}
          <aside className="w-72 p-5 flex flex-col gap-5 border-r border-white/10 bg-black/45 backdrop-blur-xl">
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
