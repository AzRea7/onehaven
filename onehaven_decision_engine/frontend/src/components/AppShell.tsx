import React from "react";
import { NavLink, useLocation } from "react-router-dom";
import {
  BarChart3,
  Building2,
  FileCheck2,
  Gavel,
  GitBranch,
  Hammer,
  Landmark,
  LayoutDashboard,
  MapPinned,
  ShieldCheck,
  Sparkles,
  Users2,
} from "lucide-react";

type ThemeMode = "light" | "dark";

const STORAGE_KEY = "onehaven-theme";

function getInitialTheme(): ThemeMode {
  if (typeof window === "undefined") return "light";

  const stored = window.localStorage.getItem(STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;

  const prefersDark =
    typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-color-scheme: dark)").matches;

  return prefersDark ? "dark" : "light";
}

function SideLink({
  to,
  icon,
  label,
  small = false,
}: {
  to: string;
  icon: React.ReactNode;
  label: string;
  small?: boolean;
}) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        [
          "oh-side-link",
          small ? "oh-side-link-small" : "",
          isActive ? "oh-side-link-active" : "",
        ].join(" ")
      }
    >
      <span className="oh-side-link-icon">{icon}</span>
      <span>{label}</span>
    </NavLink>
  );
}

export default function AppShell({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const [theme, setTheme] = React.useState<ThemeMode>(() => getInitialTheme());

  React.useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
    window.localStorage.setItem(STORAGE_KEY, theme);
  }, [theme]);

  const isAuthPage =
    location.pathname.startsWith("/login") ||
    location.pathname.startsWith("/register");

  return (
    <div className="oh-root">
      <div className="oh-bg">
        <div className="oh-blob oh-a" />
        <div className="oh-blob oh-b" />
        <div className="oh-blob oh-c" />
        <div className="oh-grid-mask" />
      </div>

      {isAuthPage ? (
        <main className="oh-main-auth">{children}</main>
      ) : (
        <div className="oh-shell-layout">
          <aside className="oh-sidebar">
            <div className="oh-sidebar-inner">
              <NavLink to="/dashboard" className="oh-sidebar-brand">
                <div className="oh-brand-mark">
                  <Sparkles className="h-4 w-4" />
                </div>
                <div className="min-w-0">
                  <div className="text-sm font-semibold text-app-0">
                    OneHaven
                  </div>
                  <div className="text-[11px] text-app-4">Investment OS</div>
                </div>
              </NavLink>

              <div className="oh-sidebar-section">
                <div className="oh-sidebar-heading">Core</div>
                <div className="oh-sidebar-links">
                  <SideLink
                    to="/dashboard"
                    icon={<LayoutDashboard className="h-4 w-4" />}
                    label="Dashboard"
                  />
                  <SideLink
                    to="/properties"
                    icon={<Building2 className="h-4 w-4" />}
                    label="Properties"
                  />
                  <SideLink
                    to="/agents"
                    icon={<Users2 className="h-4 w-4" />}
                    label="Agents"
                  />
                  <SideLink
                    to="/jurisdictions"
                    icon={<MapPinned className="h-4 w-4" />}
                    label="Jurisdictions"
                  />
                  <SideLink
                    to="/constitution"
                    icon={<Gavel className="h-4 w-4" />}
                    label="Constitution"
                  />
                </div>
              </div>

              <div className="oh-sidebar-section">
                <div className="oh-sidebar-heading">Drilldowns</div>
                <div className="oh-sidebar-links">
                  <SideLink
                    to="/pipeline"
                    icon={<GitBranch className="h-4 w-4" />}
                    label="Pipeline"
                    small
                  />
                  <SideLink
                    to="/drilldowns/trust"
                    icon={<ShieldCheck className="h-4 w-4" />}
                    label="Trust"
                    small
                  />
                  <SideLink
                    to="/drilldowns/compliance"
                    icon={<FileCheck2 className="h-4 w-4" />}
                    label="Compliance"
                    small
                  />
                  <SideLink
                    to="/drilldowns/rehab"
                    icon={<Hammer className="h-4 w-4" />}
                    label="Rehab"
                    small
                  />
                  <SideLink
                    to="/drilldowns/cashflow"
                    icon={<BarChart3 className="h-4 w-4" />}
                    label="Cashflow"
                    small
                  />
                  <SideLink
                    to="/drilldowns/equity"
                    icon={<Landmark className="h-4 w-4" />}
                    label="Equity"
                    small
                  />
                </div>
              </div>

              <div className="mt-auto pt-4">
                <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                    Appearance
                  </div>
                  <div className="mt-3 flex items-center gap-2">
                    <button
                      type="button"
                      onClick={() => setTheme("dark")}
                      className={[
                        "oh-btn cursor-pointer",
                        theme === "dark"
                          ? "oh-btn-primary"
                          : "oh-btn-secondary",
                      ].join(" ")}
                    >
                      Dark
                    </button>
                    <button
                      type="button"
                      onClick={() => setTheme("light")}
                      className={[
                        "oh-btn cursor-pointer",
                        theme === "light"
                          ? "oh-btn-primary"
                          : "oh-btn-secondary",
                      ].join(" ")}
                    >
                      Light
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </aside>

          <div className="oh-shell-main">
            <main className="oh-main">{children}</main>
          </div>
        </div>
      )}
    </div>
  );
}
