import React from "react";
import { Link, useLocation } from "react-router-dom";
import clsx from "clsx";
import { Moon, Sun, Sparkles } from "lucide-react";

const NavLink = ({ to, label }: { to: string; label: string }) => {
  const loc = useLocation();
  const active = loc.pathname === to || loc.pathname.startsWith(to + "/");

  return (
    <Link
      to={to}
      className={clsx(
        "oh-nav-link cursor-pointer select-none",
        active && "oh-nav-link-active",
      )}
    >
      {label}
    </Link>
  );
};

export default function AppHeader({
  right,
  children,
  theme = "dark",
  onToggleTheme,
}: {
  right?: React.ReactNode;
  children?: React.ReactNode;
  theme?: "light" | "dark";
  onToggleTheme?: () => void;
}) {
  return (
    <header className="oh-app-header">
      <div className="mx-auto max-w-[1320px] px-4 md:px-6 h-16 flex items-center justify-between gap-3">
        <div className="flex items-center gap-3 min-w-0">
          <Link
            to="/"
            className="flex items-center gap-3 cursor-pointer select-none min-w-0"
          >
            <div className="oh-brand-mark">
              <Sparkles className="h-4 w-4" />
            </div>

            <div className="leading-tight min-w-0">
              <div className="text-app-0 font-semibold text-sm truncate">
                OneHaven
              </div>
              <div className="text-app-4 text-[11px] -mt-[2px] truncate">
                Investment OS
              </div>
            </div>
          </Link>

          <nav className="hidden lg:flex items-center gap-2 ml-3">
            <NavLink to="/dashboard" label="Dashboard" />
            <NavLink to="/properties" label="Properties" />
            <NavLink to="/agents" label="Agents" />
            <NavLink to="/jurisdictions" label="Jurisdictions" />
            <NavLink to="/constitution" label="Constitution" />
          </nav>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          {onToggleTheme ? (
            <button
              type="button"
              onClick={onToggleTheme}
              className="oh-icon-btn cursor-pointer"
              aria-label="Toggle theme"
              title={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
            >
              {theme === "dark" ? (
                <Sun className="h-4 w-4" />
              ) : (
                <Moon className="h-4 w-4" />
              )}
            </button>
          ) : null}

          {right}
          {children}

          <Link
            to="/deal-intake"
            className="oh-btn oh-btn-primary cursor-pointer"
          >
            Deal Intake
          </Link>
        </div>
      </div>
    </header>
  );
}
