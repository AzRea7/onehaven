import React from "react";
import {
  AlertTriangle,
  BadgeDollarSign,
  ClipboardCheck,
  Landmark,
  LocateFixed,
  RefreshCcw,
  Wallet,
} from "lucide-react";
import { Link, useSearchParams } from "react-router-dom";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import KpiCard from "../components/KpiCard";
import EmptyState from "../components/EmptyState";
import { api } from "../lib/api";
import Golem from "../components/Golem";
import PaneSwitcher from "../components/PaneSwitcher";
import PaneSummaryCards from "../components/PaneSummaryCards";

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

function money(v?: number | null) {
  const n = Number(v || 0);
  return n.toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function num(v?: number | null, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

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

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="OneHaven"
          title={pane ? `${activePaneLabel} dashboard` : "Portfolio dashboard"}
          subtitle={
            pane
              ? "This dashboard is filtered into one operating mode so the user sees the correct queue, blockers, stale items, and next actions for that pane."
              : "The dashboard now acts as the portfolio command surface: pane summaries, blockers, stale items, and normalized action queues all live here."
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
                Open lifecycle start
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
              subtitle="Every pane now consumes the same dashboard shape, so the portfolio can summarize them consistently."
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
            <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
              <Surface title="Properties" subtitle="Visible in this pane">
                <div className="text-3xl font-semibold text-app-0">
                  {Number(paneData?.kpis?.total_properties || 0)}
                </div>
              </Surface>
              <Surface title="Blockers" subtitle="Properties with blockers">
                <div className="text-3xl font-semibold text-app-0">
                  {Number(paneData?.kpis?.with_blockers || 0)}
                </div>
              </Surface>
              <Surface
                title="Stale items"
                subtitle="Needs attention or refresh"
              >
                <div className="text-3xl font-semibold text-app-0">
                  {Number(paneData?.kpis?.stale_items || 0)}
                </div>
              </Surface>
              <Surface
                title="Next actions"
                subtitle="Immediate actionable rows"
              >
                <div className="text-3xl font-semibold text-app-0">
                  {Number(paneData?.kpis?.with_next_actions || 0)}
                </div>
              </Surface>
            </div>

            <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
              <Surface
                title="Top blockers"
                subtitle="Most common issues slowing this pane down."
              >
                {loading ? (
                  <div className="oh-skeleton h-[220px] rounded-3xl" />
                ) : !(paneData?.blockers || []).length ? (
                  <EmptyState compact title="No blockers found." />
                ) : (
                  <div className="space-y-3">
                    {paneData?.blockers?.slice(0, 8).map((row, idx) => (
                      <div
                        key={`${row.blocker}-${idx}`}
                        className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                      >
                        <div className="flex items-center justify-between gap-4">
                          <div className="text-sm font-medium text-app-0">
                            {String(row.blocker || "unknown").replace(
                              /_/g,
                              " ",
                            )}
                          </div>
                          <div className="text-sm font-semibold text-app-1">
                            {Number(row.count || 0)}
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </Surface>

              <Surface
                title="Next actions"
                subtitle="Action list already normalized for this pane."
              >
                {loading ? (
                  <div className="oh-skeleton h-[220px] rounded-3xl" />
                ) : !(paneData?.next_actions || []).length ? (
                  <EmptyState compact title="No next actions yet." />
                ) : (
                  <div className="space-y-3">
                    {paneData?.next_actions?.slice(0, 8).map((row, idx) => (
                      <Link
                        key={`${row.property_id}-${idx}`}
                        to={
                          row.property_id
                            ? `/properties/${row.property_id}`
                            : "/panes/investor"
                        }
                        className="block rounded-2xl border border-app bg-app-panel px-4 py-3 transition hover:border-app-strong hover:bg-app-muted"
                      >
                        <div className="text-sm font-semibold text-app-0">
                          {row.address || `Property #${row.property_id ?? "—"}`}
                        </div>
                        <div className="mt-1 text-xs text-app-4">
                          {[row.city, row.stage].filter(Boolean).join(" · ")}
                        </div>
                        <div className="mt-2 text-sm text-app-2">
                          {row.action}
                        </div>
                      </Link>
                    ))}
                  </div>
                )}
              </Surface>

              <Surface
                title="Stale items"
                subtitle="Rows that need refresh, cleanup, or manual review."
              >
                {loading ? (
                  <div className="oh-skeleton h-[220px] rounded-3xl" />
                ) : !(paneData?.stale_items || []).length ? (
                  <EmptyState compact title="No stale rows." />
                ) : (
                  <div className="space-y-3">
                    {paneData?.stale_items?.slice(0, 8).map((row, idx) => (
                      <Link
                        key={`${row.property_id}-${idx}`}
                        to={
                          row.property_id
                            ? `/properties/${row.property_id}`
                            : "/panes/investor"
                        }
                        className="block rounded-2xl border border-app bg-app-panel px-4 py-3 transition hover:border-app-strong hover:bg-app-muted"
                      >
                        <div className="text-sm font-semibold text-app-0">
                          {row.address || `Property #${row.property_id ?? "—"}`}
                        </div>
                        <div className="mt-1 text-xs text-app-4">
                          {[row.city, row.stage].filter(Boolean).join(" · ")}
                        </div>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {(row.reasons || []).slice(0, 3).map((reason) => (
                            <span key={reason} className="oh-pill oh-pill-warn">
                              {reason.replace(/_/g, " ")}
                            </span>
                          ))}
                        </div>
                      </Link>
                    ))}
                  </div>
                )}
              </Surface>
            </div>

            <Surface
              title="Pane operating quality"
              subtitle="Shared metrics stay visible while the dashboard is scoped to one mode."
            >
              <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
                <div className="rounded-3xl border border-app bg-app-panel p-5">
                  <div className="flex items-center gap-2 text-[11px] uppercase tracking-widest text-app-4">
                    <BadgeDollarSign className="h-3.5 w-3.5" />
                    Avg DSCR
                  </div>
                  <div className="mt-2 text-3xl font-semibold text-app-0">
                    {num(paneData?.kpis?.avg_dscr)}
                  </div>
                </div>

                <div className="rounded-3xl border border-app bg-app-panel p-5">
                  <div className="flex items-center gap-2 text-[11px] uppercase tracking-widest text-app-4">
                    <Wallet className="h-3.5 w-3.5" />
                    Avg cashflow est.
                  </div>
                  <div className="mt-2 text-3xl font-semibold text-app-0">
                    {money(paneData?.kpis?.avg_projected_monthly_cashflow)}
                  </div>
                </div>

                <div className="rounded-3xl border border-app bg-app-panel p-5">
                  <div className="flex items-center gap-2 text-[11px] uppercase tracking-widest text-app-4">
                    <AlertTriangle className="h-3.5 w-3.5" />
                    With blockers
                  </div>
                  <div className="mt-2 text-3xl font-semibold text-app-0">
                    {Number(paneData?.kpis?.with_blockers || 0)}
                  </div>
                </div>
              </div>
            </Surface>
          </>
        )}
      </div>
    </PageShell>
  );
}
