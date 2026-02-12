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
    <div className="h-screen w-screen overflow-hidden bg-zinc-950 text-zinc-100">
      {/* animated/art background */}
      <div className="relative h-full w-full">
        <AuroraBackground />

        <div className="relative h-full w-full flex overflow-hidden">
          {/* Sidebar */}
          <aside className="w-72 p-4 flex flex-col gap-4 border-r border-zinc-800/80 bg-zinc-950/55 backdrop-blur-xl">
            <div className="gradient-border rounded-2xl glass p-3">
              <div className="flex items-center gap-3">
                <div className="h-9 w-9 rounded-xl bg-zinc-800/70 border border-zinc-700/70" />
                <div>
                  <div className="text-sm font-semibold tracking-tight">
                    OneHaven
                  </div>
                  <div className="text-xs text-zinc-400">Decision Engine</div>
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
                      "px-3 py-2 rounded-xl text-sm transition border",
                      isActive
                        ? "bg-zinc-900/55 text-zinc-100 border-zinc-700/70"
                        : "text-zinc-300 border-transparent hover:bg-zinc-900/35 hover:border-zinc-800",
                    )
                  }
                >
                  {n.label}
                </NavLink>
              ))}
            </nav>

            <div className="mt-auto text-xs text-zinc-500">
              Built for ruthless deal clarity.
            </div>
          </aside>

          {/* Main */}
          <main className="flex-1 overflow-auto">
            <div className="max-w-6xl mx-auto p-6">
              {/* top bar */}
              <div className="mb-5 flex items-center justify-between">
                <div>
                  <div className="text-sm text-zinc-400">OneHaven</div>
                  <div className="text-xl font-semibold tracking-tight">
                    Portfolio Console
                  </div>
                </div>

                <div className="gradient-border rounded-2xl glass px-4 py-2">
                  <div className="text-xs text-zinc-400">Status</div>
                  <div className="text-sm">Live • Local API</div>
                </div>
              </div>

              {children}
            </div>
          </main>
        </div>
      </div>
    </div>
  );
}
