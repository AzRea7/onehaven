import React from "react";
import {
  ArrowRight,
  BadgeDollarSign,
  Building2,
  ClipboardCheck,
  GitBranch,
  Hammer,
  Landmark,
  ShieldCheck,
  Wallet,
} from "lucide-react";
import { Link, useLocation } from "react-router-dom";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import GlobalFilters from "../components/GlobalFilters";
import { api } from "../lib/api";
import { readFilters, toQueryString } from "../lib/filters";
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
  };
  counts?: {
    deals?: number;
    rehab_tasks_total?: number;
    rehab_tasks_open?: number;
    transactions_window?: number;
    valuations?: number;
  };
  stage_counts?: Record<string, number>;
  series?: {
    cash_by_month?: Array<{
      label: string;
      income?: number;
      expense?: number;
      capex?: number;
      net?: number;
    }>;
    decision_mix?: Array<{ key: string; label: string; count: number }>;
    stage_mix?: Array<{ key: string; label: string; count: number }>;
    county_mix?: Array<{ key: string; label: string; count: number }>;
  };
  leaderboards?: {
    good_deals?: any[];
    cashflow?: any[];
    equity?: any[];
    rehab_backlog?: any[];
    compliance_attention?: any[];
  };
  properties?: any[];
};

function money(v?: number | null) {
  const n = Number(v || 0);
  return n.toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function pct(n: number, total: number) {
  if (!total) return 0;
  return Math.max(0, Math.min(100, (n / total) * 100));
}

function TonePill({
  label,
  value,
  tone = "default",
}: {
  label: string;
  value: React.ReactNode;
  tone?: "default" | "good" | "warn" | "bad";
}) {
  const cls =
    tone === "good"
      ? "border-emerald-400/25 bg-emerald-400/10 text-emerald-200"
      : tone === "warn"
        ? "border-yellow-300/25 bg-yellow-300/10 text-yellow-100"
        : tone === "bad"
          ? "border-red-400/25 bg-red-400/10 text-red-200"
          : "border-white/10 bg-white/5 text-white/75";

  return (
    <span
      className={`inline-flex items-center rounded-full border px-3 py-1 text-xs ${cls}`}
    >
      <span className="mr-2 opacity-70">{label}</span>
      <span className="font-semibold">{value}</span>
    </span>
  );
}

function Panel({
  title,
  subtitle,
  right,
  children,
}: {
  title: string;
  subtitle?: string;
  right?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="oh-panel p-5">
      <div className="flex items-start justify-between gap-4 flex-wrap">
        <div>
          <div className="text-sm font-semibold text-white">{title}</div>
          {subtitle ? (
            <div className="text-xs text-white/55 mt-1">{subtitle}</div>
          ) : null}
        </div>
        {right}
      </div>
      <div className="mt-4">{children}</div>
    </div>
  );
}

function MetricCard({
  title,
  value,
  sub,
  icon,
  to,
  tone = "default",
}: {
  title: string;
  value: React.ReactNode;
  sub: string;
  icon: React.ReactNode;
  to: string;
  tone?: "default" | "good" | "warn" | "bad";
}) {
  const borderTone =
    tone === "good"
      ? "border-emerald-400/20 hover:border-emerald-400/35"
      : tone === "warn"
        ? "border-yellow-300/20 hover:border-yellow-300/35"
        : tone === "bad"
          ? "border-red-400/20 hover:border-red-400/35"
          : "border-white/10 hover:border-white/20";

  return (
    <Link
      to={to}
      className={`group block rounded-2xl border ${borderTone} bg-white/[0.03] hover:bg-white/[0.05] transition p-5`}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="rounded-xl border border-white/10 bg-black/30 p-2 text-white/85">
          {icon}
        </div>
        <ArrowRight className="h-4 w-4 text-white/35 group-hover:text-white/70 transition" />
      </div>

      <div className="mt-5 text-xs uppercase tracking-widest text-white/45">
        {title}
      </div>
      <div className="mt-2 text-3xl font-semibold tracking-tight text-white">
        {value}
      </div>
      <div className="mt-2 text-sm text-white/55">{sub}</div>
    </Link>
  );
}

function MiniBars({
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

  if (!rows.length) {
    return <div className="text-sm text-white/55">{emptyLabel}</div>;
  }

  return (
    <div className="space-y-3">
      {rows.map((row) => {
        const raw = Number(
          valueKey === "count" ? row.count || 0 : row.net || 0,
        );
        const width = Math.max(6, (Math.abs(raw) / max) * 100);
        const isNegative = raw < 0;

        return (
          <div key={row.label} className="space-y-1">
            <div className="flex items-center justify-between gap-4 text-xs">
              <span className="truncate text-white/70">{row.label}</span>
              <span className="text-white/85 font-semibold">
                {valueKey === "count" ? raw : money(raw)}
              </span>
            </div>
            <div className="h-2 rounded-full bg-white/5 overflow-hidden">
              <div
                className={`h-full rounded-full ${
                  isNegative ? "bg-red-400/70" : "bg-white/70"
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
  if (!items.length) {
    return <div className="text-sm text-white/55">{emptyLabel}</div>;
  }

  return (
    <div className="space-y-2">
      {items.slice(0, 6).map((row) => (
        <Link
          key={row.id}
          to={`/properties/${row.id}`}
          className="block rounded-xl border border-white/10 bg-white/[0.03] hover:bg-white/[0.05] hover:border-white/[0.16] transition p-3"
        >
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <div className="text-sm font-semibold text-white truncate">
                {row.address}
              </div>
              <div className="text-xs text-white/55 mt-1 truncate">
                {row.city}, {row.state}
                {row.county ? ` · ${row.county}` : ""}
                {row.stage ? ` · ${String(row.stage).replace(/_/g, " ")}` : ""}
              </div>
            </div>
            <div className="text-right">
              <div className="text-[11px] uppercase tracking-widest text-white/40">
                {metricLabel}
              </div>
              <div className="text-sm font-semibold text-white">
                {metricGetter(row)}
              </div>
            </div>
          </div>
        </Link>
      ))}
    </div>
  );
}

export default function Dashboard() {
  const location = useLocation();
  const filters = React.useMemo(
    () => readFilters(new URLSearchParams(location.search)),
    [location.search],
  );

  const qs = React.useMemo(() => toQueryString(filters), [filters]);

  const [data, setData] = React.useState<RollupPayload | null>(null);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [lastSync, setLastSync] = React.useState<number | null>(null);

  const refresh = React.useCallback(
    async (background = false) => {
      try {
        if (!background) setLoading(true);
        const out = await api.get<RollupPayload>(`/ops/control-plane${qs}`);
        setData(out);
        setErr(null);
        setLastSync(Date.now());
      } catch (e: any) {
        setErr(String(e?.message || e));
      } finally {
        if (!background) setLoading(false);
      }
    },
    [qs],
  );

  React.useEffect(() => {
    refresh(false);
    const id = window.setInterval(() => {
      if (document.visibilityState === "visible") refresh(true);
    }, 60000);
    return () => window.clearInterval(id);
  }, [refresh]);

  const kpis = data?.kpis || {};
  const counts = data?.counts || {};
  const stageCounts = data?.stage_counts || {};
  const stageEntries = Object.entries(stageCounts).sort((a, b) => b[1] - a[1]);
  const totalHomes = Number(kpis.total_homes || 0);

  const goodDeals = Number(kpis.good_deals || 0);
  const reviewDeals = Number(kpis.review_deals || 0);
  const rejectedDeals = Number(kpis.rejected_deals || 0);

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="OneHaven"
          title="Cockpit view"
          subtitle="Decisions first. Next actions second. Details live inside each property."
          right={
            <div className="absolute inset-0 flex items-center justify-center pointer-events-auto overflow-visible">
              <div className="h-[220px] w-[220px] md:h-[250px] md:w-[250px] translate-y-[-12px] opacity-95">
                <Golem className="h-full w-full" />
              </div>
            </div>
          }
          actions={
            <>
              <button
                onClick={() => refresh(false)}
                className="oh-btn cursor-pointer"
              >
                sync
              </button>
              <TonePill label="homes" value={totalHomes} />
              <TonePill label="good deals" value={goodDeals} tone="good" />
              <TonePill label="review" value={reviewDeals} tone="warn" />
              <TonePill label="rejected" value={rejectedDeals} tone="bad" />
              <div className="text-[11px] text-white/45 px-2 py-2">
                {lastSync
                  ? `last sync: ${new Date(lastSync).toLocaleTimeString()}`
                  : ""}
              </div>
            </>
          }
        />

        <div className="oh-panel p-4">
          <GlobalFilters />
        </div>

        {err ? (
          <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
            {err}
          </div>
        ) : null}

        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-4">
          <MetricCard
            title="Trust / pipeline quality"
            value={goodDeals}
            sub={`${reviewDeals} in review · ${rejectedDeals} rejected`}
            icon={<ShieldCheck className="h-5 w-5" />}
            to={`/drilldowns/trust${qs}`}
            tone="good"
          />

          <MetricCard
            title="Compliance exposure"
            value={Number(counts.rehab_tasks_open || 0)}
            sub={`${Number(kpis.red_zone_count || 0)} red-zone homes in filtered set`}
            icon={<ClipboardCheck className="h-5 w-5" />}
            to={`/drilldowns/compliance${qs}`}
            tone="warn"
          />

          <MetricCard
            title="Rehab backlog"
            value={money(kpis.rehab_open_cost_estimate)}
            sub={`${Number(counts.rehab_tasks_total || 0)} total rehab tasks`}
            icon={<Hammer className="h-5 w-5" />}
            to={`/drilldowns/rehab${qs}`}
            tone="warn"
          />

          <MetricCard
            title="Cashflow"
            value={money(kpis.net_cash_window)}
            sub={`${Number(kpis.cashflow_positive_homes || 0)} homes positive in current window`}
            icon={<Wallet className="h-5 w-5" />}
            to={`/drilldowns/cashflow${qs}`}
            tone={Number(kpis.net_cash_window || 0) >= 0 ? "good" : "bad"}
          />

          <MetricCard
            title="Equity"
            value={money(kpis.total_estimated_equity)}
            sub={`${Number(kpis.homes_with_valuation || 0)} homes with valuation`}
            icon={<Landmark className="h-5 w-5" />}
            to={`/drilldowns/equity${qs}`}
            tone="good"
          />
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-[1.4fr,1fr] gap-4">
          <Panel
            title="Pipeline distribution"
            subtitle="Every stage tile opens the filtered pipeline drilldown."
            right={
              <Link to={`/pipeline${qs}`} className="oh-btn">
                open pipeline
              </Link>
            }
          >
            {!stageEntries.length ? (
              <div className="text-sm text-white/55">
                {loading ? "Loading pipeline…" : "No stage data yet."}
              </div>
            ) : (
              <div className="space-y-3">
                {stageEntries.map(([stage, count]) => {
                  const width = pct(
                    Number(count || 0),
                    Math.max(totalHomes, 1),
                  );
                  const next = new URLSearchParams(location.search);
                  next.set("stage", stage);

                  return (
                    <Link
                      key={stage}
                      to={`/pipeline?${next.toString()}`}
                      className="block rounded-2xl border border-white/10 bg-white/[0.03] hover:bg-white/[0.05] hover:border-white/[0.16] transition p-4"
                    >
                      <div className="flex items-center justify-between gap-4">
                        <div>
                          <div className="text-[11px] uppercase tracking-widest text-white/45">
                            {stage.replace(/_/g, " ")}
                          </div>
                          <div className="mt-1 text-lg font-semibold text-white">
                            {count}
                          </div>
                        </div>
                        <div className="text-xs text-white/55">
                          {width.toFixed(0)}% of filtered homes
                        </div>
                      </div>
                      <div className="mt-3 h-2 rounded-full bg-white/5 overflow-hidden">
                        <div
                          className="h-full rounded-full bg-white/70"
                          style={{ width: `${width}%` }}
                        />
                      </div>
                    </Link>
                  );
                })}
              </div>
            )}
          </Panel>

          <Panel
            title="Decision mix"
            subtitle="Quick sanity check so the deal engine doesn’t become decorative pumpkin logic."
          >
            <MiniBars
              items={data?.series?.decision_mix}
              emptyLabel={
                loading ? "Loading decisions…" : "No decision data yet."
              }
            />
          </Panel>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
          <Panel
            title="Best current opportunities"
            subtitle="Highest quality survivors in the filtered slice."
          >
            <Leaderboard
              rows={data?.leaderboards?.good_deals}
              metricLabel="decision"
              metricGetter={(row) =>
                `${row.latest_decision || "—"}${row.score != null ? ` · ${row.score}` : ""}`
              }
              emptyLabel={
                loading ? "Loading opportunities…" : "No opportunities yet."
              }
            />
          </Panel>

          <Panel
            title="Cashflow leaders"
            subtitle="Top properties by current net cash in the selected window."
          >
            <Leaderboard
              rows={data?.leaderboards?.cashflow}
              metricLabel="net"
              metricGetter={(row) => money(row.property_net_cash_window)}
              emptyLabel={
                loading ? "Loading cashflow…" : "No cashflow rows yet."
              }
            />
          </Panel>

          <Panel
            title="Equity leaders"
            subtitle="Fast view of who is already carrying balance-sheet weight."
          >
            <Leaderboard
              rows={data?.leaderboards?.equity}
              metricLabel="equity"
              metricGetter={(row) => money(row.estimated_equity)}
              emptyLabel={loading ? "Loading equity…" : "No equity rows yet."}
            />
          </Panel>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <Panel
            title="Cash trend"
            subtitle="Monthly net view for the current filtered book."
            right={
              <Link to={`/drilldowns/cashflow${qs}`} className="oh-btn">
                cashflow detail
              </Link>
            }
          >
            <MiniBars
              items={data?.series?.cash_by_month}
              valueKey="net"
              emptyLabel={
                loading ? "Loading trend…" : "No transaction trend yet."
              }
            />
          </Panel>

          <Panel
            title="County concentration"
            subtitle="Where your current filtered exposure is piling up."
          >
            <MiniBars
              items={data?.series?.county_mix}
              emptyLabel={
                loading ? "Loading county mix…" : "No county data yet."
              }
            />
          </Panel>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <Panel
            title="Rehab pressure"
            subtitle="Highest open rehab drag first."
            right={
              <Link to={`/drilldowns/rehab${qs}`} className="oh-btn">
                rehab detail
              </Link>
            }
          >
            <Leaderboard
              rows={data?.leaderboards?.rehab_backlog}
              metricLabel="open cost"
              metricGetter={(row) => money(row.rehab_open_cost)}
              emptyLabel={
                loading ? "Loading rehab backlog…" : "No rehab backlog yet."
              }
            />
          </Panel>

          <Panel
            title="Compliance attention"
            subtitle="Properties likely to want human eyeballs before they misbehave."
            right={
              <Link to={`/drilldowns/compliance${qs}`} className="oh-btn">
                compliance detail
              </Link>
            }
          >
            <Leaderboard
              rows={data?.leaderboards?.compliance_attention}
              metricLabel="open tasks"
              metricGetter={(row) => row.rehab_open ?? 0}
              emptyLabel={
                loading
                  ? "Loading compliance attention…"
                  : "No compliance attention list yet."
              }
            />
          </Panel>
        </div>

        <div className="oh-panel p-5">
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <div>
              <div className="text-sm font-semibold text-white flex items-center gap-2">
                <Building2 className="h-4 w-4" />
                Portfolio snapshot
              </div>
              <div className="text-xs text-white/55 mt-1">
                Current filtered totals you can glance at without diving into
                every property card like a raccoon in a wiring closet.
              </div>
            </div>
            <Link to={`/properties${qs}`} className="oh-btn">
              open properties
            </Link>
          </div>

          <div className="mt-5 grid grid-cols-2 md:grid-cols-4 xl:grid-cols-8 gap-3">
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <div className="text-[11px] uppercase tracking-widest text-white/45">
                homes
              </div>
              <div className="mt-2 text-2xl font-semibold text-white">
                {totalHomes}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <div className="text-[11px] uppercase tracking-widest text-white/45">
                deals
              </div>
              <div className="mt-2 text-2xl font-semibold text-white">
                {Number(counts.deals || 0)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <div className="text-[11px] uppercase tracking-widest text-white/45">
                leases
              </div>
              <div className="mt-2 text-2xl font-semibold text-white">
                {Number(kpis.active_leases || 0)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <div className="text-[11px] uppercase tracking-widest text-white/45">
                valuations
              </div>
              <div className="mt-2 text-2xl font-semibold text-white">
                {Number(counts.valuations || 0)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <div className="text-[11px] uppercase tracking-widest text-white/45">
                value
              </div>
              <div className="mt-2 text-xl font-semibold text-white">
                {money(kpis.total_estimated_value)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <div className="text-[11px] uppercase tracking-widest text-white/45">
                debt
              </div>
              <div className="mt-2 text-xl font-semibold text-white">
                {money(kpis.total_loan_balance)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <div className="text-[11px] uppercase tracking-widest text-white/45">
                equity
              </div>
              <div className="mt-2 text-xl font-semibold text-white">
                {money(kpis.total_estimated_equity)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <div className="text-[11px] uppercase tracking-widest text-white/45">
                avg crime
              </div>
              <div className="mt-2 text-2xl font-semibold text-white">
                {kpis.avg_crime_score != null ? kpis.avg_crime_score : "—"}
              </div>
            </div>
          </div>
        </div>

        <div className="oh-panel p-5">
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <div>
              <div className="text-sm font-semibold text-white flex items-center gap-2">
                <GitBranch className="h-4 w-4" />
                Main navigation, now with fewer useless ornaments
              </div>
              <div className="text-xs text-white/55 mt-1">
                Every serious panel now drills into a page with
                investor-specific detail instead of just looking expensive.
              </div>
            </div>
          </div>

          <div className="mt-4 flex flex-wrap gap-3">
            <Link to={`/drilldowns/trust${qs}`} className="oh-btn">
              trust
            </Link>
            <Link to={`/drilldowns/compliance${qs}`} className="oh-btn">
              compliance
            </Link>
            <Link to={`/drilldowns/rehab${qs}`} className="oh-btn">
              rehab
            </Link>
            <Link to={`/drilldowns/cashflow${qs}`} className="oh-btn">
              cashflow
            </Link>
            <Link to={`/drilldowns/equity${qs}`} className="oh-btn">
              equity
            </Link>
            <Link to={`/pipeline${qs}`} className="oh-btn">
              pipeline
            </Link>
            <Link to={`/properties${qs}`} className="oh-btn">
              properties
            </Link>
            <Link to="/agents" className="oh-btn">
              agents
            </Link>
          </div>
        </div>
      </div>
    </PageShell>
  );
}
