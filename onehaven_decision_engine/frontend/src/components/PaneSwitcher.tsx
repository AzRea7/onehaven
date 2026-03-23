import React from "react";
import { Link, useLocation } from "react-router-dom";
import clsx from "clsx";
import {
  BriefcaseBusiness,
  ClipboardCheck,
  LayoutDashboard,
  Settings2,
  Users,
  Wallet,
} from "lucide-react";

export type PaneKey =
  | "investor"
  | "acquisition"
  | "compliance"
  | "tenants"
  | "management"
  | "admin";

export type PaneMeta = {
  key: PaneKey;
  label: string;
  shortLabel: string;
  description: string;
  to: string;
  icon: React.ReactNode;
  step: number;
};

export const PANE_META: PaneMeta[] = [
  {
    key: "investor",
    label: "Investor",
    shortLabel: "Investor",
    description: "Discovery, shortlist, underwriting, and property evaluation.",
    to: "/properties",
    icon: <Wallet className="h-4 w-4" />,
    step: 1,
  },
  {
    key: "acquisition",
    label: "Acquisition",
    shortLabel: "Acquire",
    description: "Offer execution and close-stage acquisition workflow.",
    to: "/dashboard?pane=acquisition",
    icon: <BriefcaseBusiness className="h-4 w-4" />,
    step: 2,
  },
  {
    key: "compliance",
    label: "Compliance / S8",
    shortLabel: "Compliance",
    description: "Rehab, inspections, jurisdiction, and readiness blockers.",
    to: "/panes/compliance",
    icon: <ClipboardCheck className="h-4 w-4" />,
    step: 3,
  },
  {
    key: "tenants",
    label: "Tenant Placement",
    shortLabel: "Tenants",
    description: "Marketing, screening, matching, and lease progression.",
    to: "/panes/tenants",
    icon: <Users className="h-4 w-4" />,
    step: 4,
  },
  {
    key: "management",
    label: "Management",
    shortLabel: "Manage",
    description: "Occupied operations, turnover, support, and maintenance.",
    to: "/panes/management",
    icon: <Settings2 className="h-4 w-4" />,
    step: 5,
  },
  {
    key: "admin",
    label: "Admin",
    shortLabel: "Admin",
    description: "Org-wide controls, oversight, and cross-pane operations.",
    to: "/dashboard?pane=admin",
    icon: <LayoutDashboard className="h-4 w-4" />,
    step: 6,
  },
];

export function paneLabel(key?: string | null) {
  return (
    PANE_META.find((pane) => pane.key === key)?.label ??
    (key ? key.replace(/_/g, " ") : "Pane")
  );
}

export function paneStep(key?: string | null) {
  return PANE_META.find((pane) => pane.key === key)?.step ?? null;
}

export default function PaneSwitcher({
  activePane,
  allowedPanes,
}: {
  activePane?: string | null;
  allowedPanes?: string[] | null;
}) {
  const location = useLocation();
  const allowed = new Set(
    (allowedPanes || []).map((x) => String(x).toLowerCase()),
  );

  const panes = PANE_META.filter((pane) => {
    if (pane.key === "admin") return true;
    if (!allowed.size) return true;
    return allowed.has(pane.key);
  });

  const activeMeta =
    panes.find((pane) => pane.key === activePane) ??
    panes.find((pane) => {
      if (pane.to.includes("?")) {
        return location.pathname + location.search === pane.to;
      }
      return (
        location.pathname === pane.to ||
        location.pathname.startsWith(`${pane.to}/`)
      );
    }) ??
    null;

  return (
    <div className="oh-pane-switcher">
      {panes.map((pane) => {
        const isActive =
          pane.key === activeMeta?.key ||
          (pane.to.includes("?")
            ? location.pathname + location.search === pane.to
            : location.pathname === pane.to ||
              location.pathname.startsWith(`${pane.to}/`));

        return (
          <Link
            key={pane.key}
            to={pane.to}
            title={pane.label}
            className={clsx("oh-pane-tab", isActive && "oh-pane-tab-active")}
          >
            <span className="mr-2 flex items-center justify-center">
              {pane.icon}
            </span>
            <span className="truncate">{pane.shortLabel}</span>
          </Link>
        );
      })}
    </div>
  );
}
