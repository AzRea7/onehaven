import React from "react";
import { Link, useLocation } from "react-router-dom";
import PageHero from "../../components/PageHero";
import PageShell from "../../components/PageShell";
import GlobalFilters from "../../components/GlobalFilters";
import { api } from "../../lib/api";
import { readFilters, toQueryString } from "../../lib/filters";

function money(v?: number | null) {
  const n = Number(v || 0);
  return n.toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function Panel({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="oh-panel p-5">
      <div className="text-sm font-semibold text-white">{title}</div>
      {subtitle ? (
        <div className="text-xs text-white/55 mt-1">{subtitle}</div>
      ) : null}
      <div className="mt-4">{children}</div>
    </div>
  );
}

export default function CashflowDrilldown() {
  const loc = useLocation();
  const filters = React.useMemo(
    () => readFilters(new URLSearchParams(loc.search)),
    [loc.search],
  );
  const qs = React.useMemo(() => toQueryString(filters), [filters]);

  const [data, setData] = React.useState<any>(null);
  const [err, setErr] = React.useState<string | null>(null);

  React.useEffect(() => {
    let cancelled = false;
    api
      .get(`/ops/drilldown/cashflow${qs}`)
      .then((out) => {
        if (cancelled) return;
        setData(out);
        setErr(null);
      })
      .catch((e: any) => {
        if (cancelled) return;
        setErr(String(e?.message || e));
      });
    return () => {
      cancelled = true;
    };
  }, [qs]);

  const leaders = data?.leaderboards?.cashflow || [];
  const series = data?.series?.cash_by_month || [];

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Drilldown"
          title="Cashflow monitoring"
          subtitle="Portfolio net flow in the current window plus top contributors."
          actions={
            <Link to={`/dashboard${qs}`} className="oh-btn">
              back to dashboard
            </Link>
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

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-widest text-white/45">
              net window
            </div>
            <div className="mt-2 text-2xl font-semibold text-white">
              {money(data?.kpis?.net_cash_window)}
            </div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-widest text-white/45">
              positive homes
            </div>
            <div className="mt-2 text-2xl font-semibold text-white">
              {Number(data?.kpis?.cashflow_positive_homes || 0)}
            </div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-widest text-white/45">
              transactions
            </div>
            <div className="mt-2 text-2xl font-semibold text-white">
              {Number(data?.counts?.transactions_window || 0)}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <Panel
            title="Monthly net trend"
            subtitle="Simple bar view of current filtered net flow."
          >
            <div className="space-y-3">
              {series.length === 0 ? (
                <div className="text-sm text-white/55">
                  No monthly series yet.
                </div>
              ) : (
                series.map((row: any) => {
                  const val = Number(row.net || 0);
                  const max = Math.max(
                    ...series.map((r: any) => Math.abs(Number(r.net || 0))),
                    1,
                  );
                  const width = Math.max(6, (Math.abs(val) / max) * 100);

                  return (
                    <div key={row.label} className="space-y-1">
                      <div className="flex items-center justify-between gap-3 text-xs">
                        <span className="text-white/70">{row.label}</span>
                        <span className="text-white font-semibold">
                          {money(val)}
                        </span>
                      </div>
                      <div className="h-2 rounded-full bg-white/5 overflow-hidden">
                        <div
                          className={`h-full rounded-full ${val < 0 ? "bg-red-400/70" : "bg-white/70"}`}
                          style={{ width: `${width}%` }}
                        />
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </Panel>

          <Panel
            title="Cashflow leaders"
            subtitle="Top properties by current net window."
          >
            <div className="space-y-2">
              {leaders.length === 0 ? (
                <div className="text-sm text-white/55">
                  No cashflow rows yet.
                </div>
              ) : (
                leaders.map((row: any) => (
                  <Link
                    key={row.id}
                    to={`/properties/${row.id}`}
                    className="block rounded-xl border border-white/10 bg-white/[0.03] hover:bg-white/[0.05] hover:border-white/[0.16] transition p-3"
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div>
                        <div className="text-sm font-semibold text-white">
                          {row.address}
                        </div>
                        <div className="text-xs text-white/55 mt-1">
                          {row.city}, {row.state}
                          {row.county ? ` · ${row.county}` : ""}
                        </div>
                      </div>
                      <div className="text-right text-xs text-white/70">
                        <div>net: {money(row.property_net_cash_window)}</div>
                        <div>income: {money(row.property_income_window)}</div>
                        <div>expense: {money(row.property_expense_window)}</div>
                      </div>
                    </div>
                  </Link>
                ))
              )}
            </div>
          </Panel>
        </div>
      </div>
    </PageShell>
  );
}
