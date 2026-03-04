// frontend/src/components/AppHeader.tsx
import React from "react";
import { Link, useLocation } from "react-router-dom";
import clsx from "clsx";

const NavLink = ({ to, label }: { to: string; label: string }) => {
  const loc = useLocation();
  const active = loc.pathname === to || loc.pathname.startsWith(to + "/");
  return (
    <Link
      to={to}
      className={clsx(
        "px-3 py-2 rounded-xl text-sm border transition cursor-pointer select-none",
        active
          ? "bg-white/[0.10] border-white/[0.18] text-white"
          : "bg-white/[0.04] border-white/10 text-white/80 hover:bg-white/[0.08] hover:border-white/[0.14]",
      )}
    >
      {label}
    </Link>
  );
};

export default function AppHeader({
  right,
  children,
}: {
  right?: React.ReactNode;
  children?: React.ReactNode;
}) {
  return (
    <header className="sticky top-0 z-40 backdrop-blur-md bg-black/35 border-b border-white/10">
      <div className="mx-auto max-w-[1200px] px-4 md:px-6 h-14 flex items-center justify-between">
        <Link
          to="/"
          className="flex items-center gap-2 cursor-pointer select-none"
        >
          <div className="h-8 w-8 rounded-xl bg-gradient-to-br from-indigo-500/80 via-fuchsia-500/60 to-cyan-400/70 shadow-lg shadow-fuchsia-500/10" />
          <div className="leading-tight">
            <div className="text-white font-semibold text-sm">OneHaven</div>
            <div className="text-white/60 text-[11px] -mt-[2px]">
              Investment OS
            </div>
          </div>
        </Link>

        <nav className="hidden md:flex items-center gap-2">
          <NavLink to="/properties" label="Properties" />
          <NavLink to="/dashboard" label="Dashboard" />
          <NavLink to="/agents" label="Agents" />
          <NavLink to="/jurisdictions" label="Jurisdictions" />
          <NavLink to="/constitution" label="Constitution" />
        </nav>

        <div className="flex items-center gap-2">
          {/* Optional injected controls */}
          {right}
          {children}

          <Link
            to="/deal-intake"
            className="px-3 py-2 rounded-xl text-sm bg-indigo-500/20 border border-indigo-400/30 hover:bg-indigo-500/25 transition cursor-pointer text-white select-none"
          >
            Deal Intake
          </Link>
        </div>
      </div>
    </header>
  );
}
