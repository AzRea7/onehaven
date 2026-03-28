import React from "react";
import { Link } from "react-router-dom";
import {
  AlertTriangle,
  ArrowRight,
  BriefcaseBusiness,
  ClipboardCheck,
  Settings2,
  Sparkles,
  TrendingUp,
  Users,
  Wallet,
} from "lucide-react";
import type { PaneKey } from "./PaneSwitcher";
import { paneLabel } from "./PaneSwitcher";

type PaneSummary = {
  pane: PaneKey | string;
  pane_label?: string;
  count?: number;
  kpis?: Record<string, any>;
  blockers?: Array<{ blocker?: string; count?: number }>;
  next_actions?: Array<{ action?: string }>;
};

function iconForPane(pane?: string) {
  switch (pane) {
    case "investor":
      return <Wallet className="h-4 w-4" />;
    case "acquisition":
      return <BriefcaseBusiness className="h-4 w-4" />;
    case "compliance":
      return <ClipboardCheck className="h-4 w-4" />;
    case "tenants":
      return <Users className="h-4 w-4" />;
    case "management":
    case "admin":
      return <Settings2 className="h-4 w-4" />;
    default:
      return <Wallet className="h-4 w-4" />;
  }
}

function hrefForPane(pane?: string) {
  switch (pane) {
    case "investor":
      return "/panes/investor";
    case "acquisition":
      return "/panes/acquisition";
    case "compliance":
      return "/panes/compliance";
    case "tenants":
      return "/panes/tenants";
    case "management":
      return "/panes/management";
    case "admin":
      return "/dashboard?pane=admin";
    default:
      return "/dashboard";
  }
}

function money(v: any) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `$${Math.round(n).toLocaleString()}`;
}

function decimal(v: any, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function investorHighlights(kpis?: Record<string, any>) {
  if (!kpis) return [];

  const out: Array<{ label: string; value: string; icon: React.ReactNode }> =
    [];

  if (kpis.avg_projected_monthly_cashflow != null) {
    out.push({
      label: "Avg cashflow",
      value: money(kpis.avg_projected_monthly_cashflow),
      icon: <TrendingUp className="h-3.5 w-3.5" />,
    });
  }

  if (kpis.avg_dscr != null) {
    out.push({
      label: "Avg DSCR",
      value: decimal(kpis.avg_dscr, 2),
      icon: <Sparkles className="h-3.5 w-3.5" />,
    });
  }

  return out.slice(0, 2);
}

export default function PaneSummaryCards({
  panes,
}: {
  panes?: PaneSummary[] | null;
}) {
  const rows = Array.isArray(panes) ? panes : [];

  if (!rows.length) return null;

  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
      {rows.map((pane) => {
        const blocker = pane.blockers?.[0];
        const action = pane.next_actions?.[0]?.action;
        const highlights =
          String(pane.pane) === "investor" ? investorHighlights(pane.kpis) : [];

        return (
          <Link
            key={String(pane.pane)}
            to={hrefForPane(String(pane.pane))}
            className="rounded-3xl border border-app bg-app-panel p-5 transition hover:border-app-strong hover:bg-app-muted"
          >
            <div className="flex items-start justify-between gap-3">
              <div className="rounded-2xl border border-app bg-app-muted p-2 text-app-1">
                {iconForPane(String(pane.pane))}
              </div>
              <ArrowRight className="h-4 w-4 text-app-4" />
            </div>

            <div className="mt-4 text-sm font-semibold text-app-0">
              {pane.pane_label || paneLabel(String(pane.pane))}
            </div>

            <div className="mt-1 text-3xl font-semibold text-app-0">
              {Number(pane.count || 0)}
            </div>

            <div className="mt-1 text-xs text-app-4">
              visible properties in this pane
            </div>

            {highlights.length ? (
              <div className="mt-4 grid gap-2 sm:grid-cols-2">
                {highlights.map((item) => (
                  <div
                    key={item.label}
                    className="rounded-2xl border border-app bg-app-muted px-3 py-3"
                  >
                    <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                      {item.icon}
                      {item.label}
                    </div>
                    <div className="mt-2 text-sm font-semibold text-app-0">
                      {item.value}
                    </div>
                  </div>
                ))}
              </div>
            ) : null}

            {blocker?.blocker ? (
              <div className="mt-4 rounded-2xl border border-app bg-app-muted px-3 py-3">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <AlertTriangle className="h-3.5 w-3.5" />
                  Top blocker
                </div>
                <div className="mt-2 text-sm font-medium text-app-0">
                  {blocker.blocker.replace(/_/g, " ")}
                </div>
                <div className="mt-1 text-xs text-app-4">
                  affecting {Number(blocker.count || 0)} properties
                </div>
              </div>
            ) : null}

            {action ? (
              <div className="mt-3 text-xs text-app-3">
                Next action: <span className="text-app-1">{action}</span>
              </div>
            ) : null}
          </Link>
        );
      })}
    </div>
  );
}
