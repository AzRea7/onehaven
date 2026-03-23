import React from "react";
import { Link } from "react-router-dom";
import {
  AlertTriangle,
  ArrowRight,
  BriefcaseBusiness,
  ClipboardCheck,
  Settings2,
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
  top_blockers?: Array<{ blocker?: string; count?: number }>;
  top_actions?: Array<{ action?: string }>;
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
    case "compliance":
      return "/panes/compliance";
    case "tenants":
      return "/panes/tenants";
    case "management":
      return "/panes/management";
    case "acquisition":
      return "/dashboard?pane=acquisition";
    case "admin":
      return "/dashboard?pane=admin";
    default:
      return "/dashboard";
  }
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
        const blocker = pane.top_blockers?.[0];
        const action = pane.top_actions?.[0]?.action;

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
