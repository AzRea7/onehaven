import React from "react";
import {
  ClipboardCheck,
  Landmark,
  LocateFixed,
  RefreshCcw,
  Wallet,
} from "lucide-react";
import { Link, useSearchParams } from "react-router-dom";
import PageHero from "onehaven_onehaven_platform/frontend/src/shell/PageHero";
import PageShell from "onehaven_onehaven_platform/frontend/src/shell/PageShell";
import Surface from "packages/ui/src/components/Surface";
import KpiCard from "packages/ui/src/components/KpiCard";
import { api } from "@/lib/api";
import Golem from "packages/ui/src/components/Golem";
import PaneSwitcher from "onehaven_onehaven_platform/frontend/src/shell/PaneSwitcher";
import PaneSummaryCards from "products/intelligence/frontend/src/components/PaneSummaryCards";

type PaneOverviewPayload = {
  ok?: boolean;
  panes?: Array<{
    pane: string;
    pane_label?: string;
    count?: number;
    kpis?: Record<string, any>;
    blockers?: Array<{ blocker?: string; count?: number }>;
    next_actions?: Array<{ action?: string }>;
  }>;
  allowed_panes?: string[];
};
type PaneDashboardPayload = {
  ok?: boolean;
  pane?: string;
  pane_label?: string;
  allowed_panes?: string[];
  kpis?: Record<string, any>;
  blockers?: Array<{ blocker?: string; count?: number }>;
  recent_actions?: Array<{
    property_id?: number;
    address?: string;
    city?: string;
    stage?: string;
    action?: string;
  }>;
  next_actions?: Array<{
    property_id?: number;
    address?: string;
    city?: string;
    stage?: string;
    action?: string;
  }>;
  stale_items?: Array<{
    property_id?: number;
    address?: string;
    city?: string;
    stage?: string;
    reasons?: string[];
  }>;
  count?: number;
};

export default function Dashboard() {
  const [searchParams] = useSearchParams();
  const pane = (searchParams.get("pane") || "").trim().toLowerCase();
  const [overview, setOverview] = React.useState<PaneOverviewPayload | null>(
    null,
  );
  const [paneData, setPaneData] = React.useState<PaneDashboardPayload | null>(
    null,
  );
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [lastSync, setLastSync] = React.useState<number | null>(null);

  const refresh = React.useCallback(async () => {
    try {
      setLoading(true);
      const [overviewOut, paneOut] = await Promise.all([
        api.get<PaneOverviewPayload>("/dashboard/panes"),
        pane
          ? api.get<PaneDashboardPayload>(`/dashboard/panes/${pane}`)
          : Promise.resolve(null),
      ]);
      setOverview(overviewOut);
      setPaneData(paneOut);
      setErr(null);
      setLastSync(Date.now());
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, [pane]);

  React.useEffect(() => {
    refresh();
  }, [refresh]);
  const activePaneLabel = paneData?.pane_label || (pane ? pane : "Portfolio");
  const acquisitionCount = Number(
    overview?.panes?.find((x) => x.pane === "acquisition")?.count || 0,
  );

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="OneHaven"
          title={pane ? `${activePaneLabel} dashboard` : "Portfolio dashboard"}
          subtitle={
            pane
              ? "This dashboard is filtered into one operating mode so the user sees the correct queue, blockers, stale items, and next actions for that pane."
              : "The dashboard is now organized around pane ownership, with a real investor-to-acquire handoff instead of a UI-only button."
          }
          right={
            <div className="pointer-events-auto absolute inset-0 flex items-center justify-center overflow-visible">
              <div className="h-[220px] w-[220px] translate-y-[-8px] opacity-95 md:h-[250px] md:w-[250px]">
                <Golem className="h-full w-full" />
              </div>
            </div>
          }
          actions={
            <>
              <button onClick={refresh} className="oh-btn oh-btn-secondary">
                <RefreshCcw className="h-4 w-4" />
                Sync dashboard
              </button>
              <Link to="/panes/investor" className="oh-btn oh-btn-secondary">
                Open investor
              </Link>
              <Link to="/panes/acquisition" className="oh-btn oh-btn-secondary">
                Open acquire
              </Link>
              <div className="px-2 py-2 text-[11px] text-app-4">
                {lastSync
                  ? `last sync: ${new Date(lastSync).toLocaleTimeString()}`
                  : ""}
              </div>
            </>
          }
        />

        <PaneSwitcher
          activePane={pane || undefined}
          allowedPanes={paneData?.allowed_panes || overview?.allowed_panes}
        />
        {err ? (
          <Surface tone="danger">
            <div className="text-sm text-red-300">{err}</div>
          </Surface>
        ) : null}

        {!pane ? (
          <>
            <Surface
              title="Pane summaries"
              subtitle="Portfolio view of every operating pane."
            >
              <PaneSummaryCards panes={overview?.panes} />
            </Surface>
            <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
              <KpiCard
                title="Visible panes"
                value={
                  Array.isArray(overview?.panes) ? overview!.panes!.length : 0
                }
                subtitle="workspace modes currently available"
                icon={Landmark}
                tone="accent"
              />
              <KpiCard
                title="Investor"
                value={Number(
                  overview?.panes?.find((x) => x.pane === "investor")?.count ||
                    0,
                )}
                subtitle="discovery and underwriting"
                icon={Wallet}
                tone="success"
              />
              <KpiCard
                title="Acquire"
                value={acquisitionCount}
                subtitle="pre-offer pursuit through close"
                icon={Wallet}
                tone="warning"
              />
              <KpiCard
                title="Compliance"
                value={Number(
                  overview?.panes?.find((x) => x.pane === "compliance")
                    ?.count || 0,
                )}
                subtitle="rehab and readiness workload"
                icon={ClipboardCheck}
                tone="warning"
              />
              <KpiCard
                title="Management"
                value={Number(
                  overview?.panes?.find((x) => x.pane === "management")
                    ?.count || 0,
                )}
                subtitle="occupied and turnover operations"
                icon={LocateFixed}
                tone="accent"
              />
            </div>
          </>
        ) : (
          <>
            <Surface
              title={`${activePaneLabel} overview`}
              subtitle={
                loading
                  ? "Refreshing..."
                  : "Live pane summary from the workflow engine."
              }
            >
              <pre className="overflow-x-auto text-xs text-app-3">
                {JSON.stringify(paneData, null, 2)}
              </pre>
            </Surface>
          </>
        )}
      </div>
    </PageShell>
  );
}
