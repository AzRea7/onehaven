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
  LogOut,
  MapPinned,
  ShieldCheck,
  Sparkles,
  User2,
  Users2,
} from "lucide-react";
import { useAuth } from "../lib/auth";

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
  const { principal, logout, loading } = useAuth();
  const [theme, setTheme] = React.useState<ThemeMode>(() => getInitialTheme());

  React.useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
    window.localStorage.setItem(STORAGE_KEY, theme);
  }, [theme]);

  const isAuthPage =
    location.pathname.startsWith("/login") ||
    location.pathname.startsWith("/register");

  const displayOrg = (principal?.org_slug || "").trim() || "No org selected";
  const displayEmail = (principal?.email || "").trim() || "Not signed in";
  const displayRole = (principal?.role || "").trim() || "viewer";

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
          <aside className="oh-sidebar h-screen min-h-0 overflow-hidden">
            <div className="oh-sidebar-inner flex h-screen min-h-0 flex-col overflow-hidden px-4 py-4">
              <NavLink
                to="/dashboard"
                className="oh-sidebar-brand shrink-0 rounded-2xl border border-app bg-app-panel px-3 py-3 shadow-soft"
              >
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

              <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1">
                <div className="space-y-5 pb-4">
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
                </div>
              </div>

              <div className="mt-4 shrink-0 border-t border-app pt-4">
                <div className="space-y-3">
                  <div className="rounded-2xl border border-app bg-app-panel px-4 py-4 shadow-soft">
                    <div className="flex items-start gap-3">
                      <div className="mt-0.5 inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-app bg-app-muted text-app-2">
                        <User2 className="h-4 w-4" />
                      </div>

                      <div className="min-w-0 flex-1">
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Workspace
                        </div>
                        <div className="mt-1 truncate text-sm font-semibold text-app-0">
                          {displayOrg}
                        </div>
                        <div className="truncate text-xs text-app-4">
                          {displayEmail}
                        </div>
                      </div>
                    </div>

                    <div className="mt-4 grid gap-2 text-xs">
                      <div className="flex items-center justify-between gap-3 rounded-xl border border-app bg-app-muted px-3 py-2">
                        <span className="text-app-4">Role</span>
                        <span className="font-medium uppercase tracking-[0.12em] text-app-1">
                          {displayRole}
                        </span>
                      </div>
                    </div>
                  </div>

                  <div className="rounded-2xl border border-app bg-app-panel px-4 py-4 shadow-soft">
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

                  <button
                    type="button"
                    onClick={() => logout()}
                    disabled={loading}
                    className="inline-flex h-12 w-full items-center justify-center gap-2 rounded-2xl border border-red-700 bg-red-600 px-4 text-sm font-semibold text-white shadow-lg shadow-red-900/25 transition hover:bg-red-700 hover:border-red-800 active:bg-red-800 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    <LogOut className="h-4 w-4" />
                    {loading ? "Signing out…" : "Logout"}
                  </button>
                </div>
              </div>
            </div>
          </aside>

          <div className="oh-shell-main min-w-0">
            <main className="oh-main">{children}</main>
          </div>
        </div>
      )}
    </div>
  );
}
