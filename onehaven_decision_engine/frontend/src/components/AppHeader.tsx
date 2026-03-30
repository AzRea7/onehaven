import React from "react";
import { useLocation } from "react-router-dom";
import clsx from "clsx";
import { Moon, Sparkles, Sun } from "lucide-react";
import PaneSwitcher from "./PaneSwitcher";

function inferHeaderContext(pathname: string) {
  if (pathname.startsWith("/panes/acquisition")) {
    return {
      label: "Acquisition",
      activePane: "acquisition",
      stage: "Offer / Close",
      nextStage: "Compliance",
      blocker: "Document and close coordination",
    };
  }
  if (pathname.startsWith("/panes/compliance")) {
    return {
      label: "Compliance / S8",
      activePane: "compliance",
      stage: "Rehab / Inspection",
      nextStage: "Tenants",
      blocker: "Inspection and readiness blockers",
    };
  }
  if (pathname.startsWith("/panes/tenants")) {
    return {
      label: "Tenant placement",
      activePane: "tenants",
      stage: "Marketing / Screening",
      nextStage: "Management",
      blocker: "Matching and lease-up backlog",
    };
  }
  if (pathname.startsWith("/panes/management")) {
    return {
      label: "Management",
      activePane: "management",
      stage: "Occupied operations",
      nextStage: "Portfolio health",
      blocker: "Maintenance and turnover workload",
    };
  }
  if (pathname.startsWith("/properties/")) {
    return {
      label: "Property lifecycle",
      activePane: null,
      stage: "Single-property execution",
      nextStage: "Advance by pane gate",
      blocker: "See property hero for live blocker",
    };
  }
  if (
    pathname.startsWith("/panes/investor") ||
    pathname.startsWith("/properties")
  ) {
    return {
      label: "Investor",
      activePane: "investor",
      stage: "Discovery / Underwriting",
      nextStage: "Acquisition",
      blocker: "Move shortlisted assets forward",
    };
  }
  if (pathname.startsWith("/dashboard")) {
    return {
      label: "Portfolio",
      activePane: null,
      stage: "Cross-pane oversight",
      nextStage: "Route work into pane queues",
      blocker: "Resolve highest portfolio blockers",
    };
  }
  return {
    label: "OneHaven",
    activePane: null,
    stage: "Lifecycle navigation",
    nextStage: "Route by pane",
    blocker: "Focus the correct queue",
  };
}

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
  const location = useLocation();
  const context = inferHeaderContext(location.pathname);

  return (
    <header className="oh-app-header w-full border-b border-app bg-[color:var(--bg-elevated)]/95 backdrop-blur-xl">
      <div className="w-full px-4 py-4 md:px-6 xl:px-8">
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
            <div className="min-w-0">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Sparkles className="h-3.5 w-3.5" />
                {context.stage}
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2">

              {onToggleTheme ? (
                <button
                  type="button"
                  onClick={onToggleTheme}
                  className={clsx(
                    "inline-flex h-10 items-center justify-center gap-2 rounded-2xl border border-app bg-app-panel px-3 text-sm text-app-2 transition hover:bg-app-muted",
                  )}
                  aria-label="Toggle theme"
                >
                  {theme === "dark" ? (
                    <Sun className="h-4 w-4" />
                  ) : (
                    <Moon className="h-4 w-4" />
                  )}
                </button>
              ) : null}
            </div>
          </div>

          <div className="oh-header-block oh-header-nav">
            <PaneSwitcher activePane={context.activePane} />
          </div>

          {(right || children) && (
            <div className="flex items-center justify-end gap-2">
              {right}
              {children}
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
