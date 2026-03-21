import React from "react";
import {
  BriefcaseBusiness,
  ClipboardCheck,
  Hammer,
  Landmark,
  PieChart,
  ShieldCheck,
  Users,
  Wallet,
  Home,
  RefreshCcw,
} from "lucide-react";
import { Link } from "react-router-dom";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import KpiCard from "../components/KpiCard";
import EmptyState from "../components/EmptyState";
import { api } from "../lib/api";
import Golem from "../components/Golem";

type RollupPayload = {
  ok?: boolean;
  kpis?: {
    total_homes?: number;
    good_deals?: number;
    review_deals?: number;
    rejected_deals?: number;
    active_leases?: number;
    cashflow_positive_homes?: number;
    homes_with_valuation?: number;
    red_zone_count?: number;
    total_estimated_value?: number;
    total_loan_balance?: number;
    total_estimated_equity?: number;
    rehab_open_cost_estimate?: number;
    net_cash_window?: number;
    avg_crime_score?: number | null;
    avg_dscr?: number | null;
    avg_cashflow_estimate?: number | null;
  };
  counts?: {
    deals?: number;
    rehab_tasks_total?: number;
    rehab_tasks_open?: number;
    transactions_window?: number;
    valuations?: number;
    properties?: number;
  };
  series?: {
    cash_by_month?: Array<{
      label: string;
      income?: number;
      expense?: number;
      capex?: number;
      net?: number;
    }>;
    decision_mix?: Array<{ key: string; label: string; count: number }>;
    workflow_mix?: Array<{ key: string; label: string; count: number }>;
    county_mix?: Array<{ key: string; label: string; count: number }>;
  };
  leaderboards?: {
    good_deals?: any[];
    cashflow?: any[];
    equity?: any[];
    rehab_backlog?: any[];
    compliance_attention?: any[];
  };
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

function GraphBars({
  items,
  emptyLabel,
  valueKey = "count",
}: {
  items?: Array<{ label: string; count?: number; net?: number }>;
  emptyLabel: string;
  valueKey?: "count" | "net";
}) {
  const rows = Array.isArray(items) ? items : [];
  const max =
    rows.length > 0
      ? Math.max(
          ...rows.map((r) =>
            Math.abs(Number(valueKey === "count" ? r.count || 0 : r.net || 0)),
          ),
          1,
        )
      : 1;

  if (!rows.length) return <EmptyState compact title={emptyLabel} />;

  return (
    <div className="space-y-3">
      {rows.map((row) => {
        const raw = Number(
          valueKey === "count" ? row.count || 0 : row.net || 0,
        );
        const width = Math.max(8, (Math.abs(raw) / max) * 100);
        const isNegative = raw < 0;

        return (
          <div key={row.label} className="space-y-1">
            <div className="flex items-center justify-between gap-4 text-xs">
              <span className="truncate text-app-3">{row.label}</span>
              <span className="font-semibold text-app-1">
                {valueKey === "count" ? raw : money(raw)}
              </span>
            </div>
            <div className="h-2 overflow-hidden rounded-full bg-app-muted">
              <div
                className={`h-full rounded-full ${
                  isNegative ? "bg-red-400/70" : "bg-[var(--accent)]"
                }`}
                style={{ width: `${width}%` }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
function resolvePropertyId(row: any): number | null {
  const candidates = [
    row?.id,
    row?.property_id,
    row?.property?.id,
    row?.propertyId,
    row?.property?.property_id,
  ];

  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }

  return null;
}

function Leaderboard({
  rows,
  metricLabel,
  metricGetter,
  emptyLabel,
}: {
  rows?: any[];
  metricLabel: string;
  metricGetter: (row: any) => React.ReactNode;
  emptyLabel: string;
}) {
  const items = Array.isArray(rows) ? rows : [];
  if (!items.length) return <EmptyState compact title={emptyLabel} />;

  return (
    <div className="space-y-2">
      {items.slice(0, 6).map((row, idx) => {
        const propertyId = resolvePropertyId(row);
        const title =
          row?.address ||
          row?.property?.address ||
          `Property #${propertyId ?? "—"}`;
        const city = row?.city || row?.property?.city || "";
        const state = row?.state || row?.property?.state || "";
        const stageLabel = row?.stage_label || row?.property?.stage_label || "";

        const content = (
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <div className="truncate text-sm font-semibold text-app-0">
                {title}
              </div>
              <div className="mt-1 truncate text-xs text-app-4">
                {[
                  city && state ? `${city}, ${state}` : city || state,
                  stageLabel,
                ]
                  .filter(Boolean)
                  .join(" · ")}
              </div>
            </div>
            <div className="text-right">
              <div className="text-[11px] uppercase tracking-widest text-app-4">
                {metricLabel}
              </div>
              <div className="text-sm font-semibold text-app-0">
                {metricGetter(row)}
              </div>
            </div>
          </div>
        );

        if (!propertyId) {
          return (
            <div
              key={`leaderboard-${idx}`}
              className="block rounded-2xl border border-app bg-app-panel px-4 py-3 opacity-70"
            >
              {content}
            </div>
          );
        }

        return (
          <Link
            key={propertyId}
            to={`/properties/${propertyId}`}
            className="block rounded-2xl border border-app bg-app-panel px-4 py-3 transition hover:border-app-strong hover:bg-app-muted"
          >
            {content}
          </Link>
        );
      })}
    </div>
  );
}

function SectionCard({
  title,
  subtitle,
  icon,
  value,
  tone = "default",
}: {
  title: string;
  subtitle: string;
  icon: React.ReactNode;
  value: React.ReactNode;
  tone?: "default" | "good" | "warn" | "bad";
}) {
  const toneCls =
    tone === "good"
      ? "border-emerald-400/20"
      : tone === "warn"
        ? "border-yellow-300/20"
        : tone === "bad"
          ? "border-red-400/20"
          : "border-app";

  return (
    <div className={`rounded-3xl border ${toneCls} bg-app-panel p-5`}>
      <div className="flex items-start justify-between gap-4">
        <div className="rounded-2xl border border-app bg-app-muted p-2 text-app-1">
          {icon}
        </div>
        <div className="text-right">
          <div className="text-2xl font-semibold text-app-0">{value}</div>
        </div>
      </div>
      <div className="mt-4 text-sm font-semibold text-app-0">{title}</div>
      <div className="mt-1 text-xs text-app-4">{subtitle}</div>
    </div>
  );
}

export default function Dashboard() {
  const [data, setData] = React.useState<RollupPayload | null>(null);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [lastSync, setLastSync] = React.useState<number | null>(null);

  const refresh = React.useCallback(async () => {
    try {
      setLoading(true);
      const out = await api.get<RollupPayload>("/ops/control-plane");
      setData(out);
      setErr(null);
      setLastSync(Date.now());
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    refresh();
  }, [refresh]);

  const kpis = data?.kpis || {};
  const counts = data?.counts || {};
  const totalHomes = Number(kpis.total_homes || 0);

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="OneHaven"
          title="Investment dashboard"
          subtitle="A cleaner investor view focused on pipeline movement, portfolio performance, and the fastest path from acquisition to tenant-occupied cashflow."
          right={
            <div className="absolute inset-0 flex items-center justify-center overflow-visible pointer-events-auto">
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
              <Link to="/properties" className="oh-btn oh-btn-secondary">
                Open properties
              </Link>
              <div className="px-2 py-2 text-[11px] text-app-4">
                {lastSync
                  ? `last sync: ${new Date(lastSync).toLocaleTimeString()}`
                  : ""}
              </div>
            </>
          }
        />

        {err ? (
          <Surface tone="danger">
            <div className="text-sm text-red-300">{err}</div>
          </Surface>
        ) : null}

        <Surface
          title="Portfolio snapshot"
          subtitle="High-signal metrics first so the dashboard feels like a control plane instead of a pile of modules."
        >
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-5">
            <KpiCard
              title="Homes"
              value={totalHomes}
              subtitle={`${Number(counts.deals || 0)} tracked deals`}
              icon={Home}
              tone="accent"
            />
            <KpiCard
              title="Good deals"
              value={Number(kpis.good_deals || 0)}
              subtitle={`${Number(kpis.review_deals || 0)} review · ${Number(
                kpis.rejected_deals || 0,
              )} reject`}
              icon={ShieldCheck}
              tone="success"
            />
            <KpiCard
              title="Cashflow"
              value={money(kpis.net_cash_window)}
              subtitle={`${Number(kpis.cashflow_positive_homes || 0)} positive homes`}
              icon={Wallet}
              tone={
                Number(kpis.net_cash_window || 0) >= 0 ? "success" : "danger"
              }
            />
            <KpiCard
              title="Equity"
              value={money(kpis.total_estimated_equity)}
              subtitle={`${Number(kpis.homes_with_valuation || 0)} valued homes`}
              icon={Landmark}
              tone="accent"
            />
            <KpiCard
              title="Rehab backlog"
              value={money(kpis.rehab_open_cost_estimate)}
              subtitle={`${Number(counts.rehab_tasks_open || 0)} open rehab tasks`}
              icon={Hammer}
              tone="warning"
            />
          </div>
        </Surface>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1.2fr_0.8fr]">
          <Surface
            title="Decision mix"
            subtitle="The simplified three-way classification for investors."
            actions={
              <Link to="/properties" className="oh-btn oh-btn-secondary">
                Review inventory
              </Link>
            }
          >
            <GraphBars
              items={data?.series?.decision_mix}
              emptyLabel={
                loading ? "Loading decisions…" : "No decision data yet."
              }
            />
          </Surface>

          <Surface
            title="Workflow stages"
            subtitle="Properties grouped by the real business workflow."
          >
            <GraphBars
              items={data?.series?.workflow_mix}
              emptyLabel={
                loading ? "Loading workflow…" : "No workflow data yet."
              }
            />
          </Surface>
        </div>

        <Surface
          title="Section 8 workflow control"
          subtitle="These categories match the operating flow from potential property to occupied cashflow asset."
        >
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-6">
            <SectionCard
              title="Deal / Procurement"
              subtitle="Source, ingest, classify, and decide"
              icon={<BriefcaseBusiness className="h-5 w-5" />}
              value={Number(
                data?.series?.workflow_mix?.find((x) => x.key === "deal")
                  ?.count || 0,
              )}
            />
            <SectionCard
              title="Rehab"
              subtitle="Scope, tasks, budget, execution"
              icon={<Hammer className="h-5 w-5" />}
              value={Number(
                data?.series?.workflow_mix?.find((x) => x.key === "rehab")
                  ?.count || 0,
              )}
              tone="warn"
            />
            <SectionCard
              title="Compliance"
              subtitle="Inspection, licensing, readiness"
              icon={<ClipboardCheck className="h-5 w-5" />}
              value={Number(
                data?.series?.workflow_mix?.find((x) => x.key === "compliance")
                  ?.count || 0,
              )}
              tone="warn"
            />
            <SectionCard
              title="Tenant"
              subtitle="Placement and voucher readiness"
              icon={<Users className="h-5 w-5" />}
              value={Number(
                data?.series?.workflow_mix?.find((x) => x.key === "tenant")
                  ?.count || 0,
              )}
            />
            <SectionCard
              title="Lease / Management"
              subtitle="Occupancy activation and operations"
              icon={<PieChart className="h-5 w-5" />}
              value={
                Number(
                  data?.series?.workflow_mix?.find((x) => x.key === "lease")
                    ?.count || 0,
                ) +
                Number(
                  data?.series?.workflow_mix?.find(
                    (x) => x.key === "management",
                  )?.count || 0,
                )
              }
            />
            <SectionCard
              title="Cashflow / Equity"
              subtitle="Income and long-term value"
              icon={<Wallet className="h-5 w-5" />}
              value={Number(
                data?.series?.workflow_mix?.find((x) => x.key === "cash_equity")
                  ?.count || 0,
              )}
              tone="good"
            />
          </div>
        </Surface>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
          <Surface
            title="Best opportunities"
            subtitle="Highest-quality surviving deals right now."
          >
            <Leaderboard
              rows={data?.leaderboards?.good_deals}
              metricLabel="class"
              metricGetter={(row) =>
                `${row.classification || row.latest_decision || "—"}${
                  row.dscr != null ? ` · ${num(row.dscr)}` : ""
                }`
              }
              emptyLabel={
                loading ? "Loading opportunities…" : "No opportunities yet."
              }
            />
          </Surface>

          <Surface
            title="Cashflow leaders"
            subtitle="Highest estimated monthly contribution."
          >
            <Leaderboard
              rows={data?.leaderboards?.cashflow}
              metricLabel="cashflow"
              metricGetter={(row) =>
                money(row.cashflow_estimate ?? row.property_net_cash_window)
              }
              emptyLabel={
                loading ? "Loading cashflow…" : "No cashflow rows yet."
              }
            />
          </Surface>

          <Surface
            title="Equity leaders"
            subtitle="Properties carrying the strongest balance-sheet value."
          >
            <Leaderboard
              rows={data?.leaderboards?.equity}
              metricLabel="equity"
              metricGetter={(row) => money(row.estimated_equity)}
              emptyLabel={loading ? "Loading equity…" : "No equity rows yet."}
            />
          </Surface>
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <Surface
            title="Cash trend"
            subtitle="Monthly net trend for the current portfolio slice."
          >
            <GraphBars
              items={data?.series?.cash_by_month}
              valueKey="net"
              emptyLabel={
                loading ? "Loading trend…" : "No transaction trend yet."
              }
            />
          </Surface>

          <Surface
            title="County concentration"
            subtitle="Where current inventory is clustering."
          >
            <GraphBars
              items={data?.series?.county_mix}
              emptyLabel={
                loading ? "Loading county mix…" : "No county data yet."
              }
            />
          </Surface>
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
          <div className="rounded-3xl border border-app bg-app-panel p-5">
            <div className="text-[11px] uppercase tracking-widest text-app-4">
              Active leases
            </div>
            <div className="mt-2 text-3xl font-semibold text-app-0">
              {Number(kpis.active_leases || 0)}
            </div>
          </div>

          <div className="rounded-3xl border border-app bg-app-panel p-5">
            <div className="text-[11px] uppercase tracking-widest text-app-4">
              Avg DSCR
            </div>
            <div className="mt-2 text-3xl font-semibold text-app-0">
              {num(kpis.avg_dscr)}
            </div>
          </div>

          <div className="rounded-3xl border border-app bg-app-panel p-5">
            <div className="text-[11px] uppercase tracking-widest text-app-4">
              Avg cashflow est.
            </div>
            <div className="mt-2 text-3xl font-semibold text-app-0">
              {money(kpis.avg_cashflow_estimate)}
            </div>
          </div>

          <div className="rounded-3xl border border-app bg-app-panel p-5">
            <div className="text-[11px] uppercase tracking-widest text-app-4">
              Avg crime score
            </div>
            <div className="mt-2 text-3xl font-semibold text-app-0">
              {kpis.avg_crime_score != null
                ? num(kpis.avg_crime_score, 1)
                : "—"}
            </div>
          </div>
        </div>
      </div>
    </PageShell>
  );
}
