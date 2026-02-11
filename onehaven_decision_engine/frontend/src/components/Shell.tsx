import React from "react";
import { NavLink } from "react-router-dom";
import clsx from "clsx";

const nav = [
  { to: "/dashboard", label: "Dashboard" },
  { to: "/agents", label: "Agents" },
  { to: "/constitution", label: "Operating Truth" },
];

export default function Shell({ children }: { children: React.ReactNode }) {
  return (
    <div className="h-screen w-screen flex overflow-hidden">
      <aside className="w-72 bg-zinc-950 border-r border-zinc-800 p-4 flex flex-col gap-4">
        <div className="flex items-center gap-2">
          <div className="h-8 w-8 rounded-lg bg-zinc-800" />
          <div>
            <div className="text-sm font-semibold">OneHaven</div>
            <div className="text-xs text-zinc-400">Decision Engine</div>
          </div>
        </div>

        <nav className="flex flex-col gap-1">
          {nav.map((n) => (
            <NavLink
              key={n.to}
              to={n.to}
              className={({ isActive }) =>
                clsx(
                  "px-3 py-2 rounded-lg text-sm",
                  isActive
                    ? "bg-zinc-900 text-zinc-100"
                    : "text-zinc-300 hover:bg-zinc-900/60",
                )
              }
            >
              {n.label}
            </NavLink>
          ))}
        </nav>

        <div className="mt-auto text-xs text-zinc-500">
          Dark UI inspired by OpenClaw’s dashboard layout.
        </div>
      </aside>

      <main className="flex-1 bg-zinc-950 overflow-auto">
        <div className="max-w-6xl mx-auto p-6">{children}</div>
      </main>
    </div>
  );
}
