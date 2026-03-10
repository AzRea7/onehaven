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

export default function EquityDrilldown() {
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
      .get(`/ops/drilldown/equity${qs}`)
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

  const leaders = data?.leaderboards?.equity || [];

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Drilldown"
          title="Equity monitoring"
          subtitle="Balance-sheet view across the currently filtered portfolio slice."
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

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-widest text-white/45">
              portfolio value
            </div>
            <div className="mt-2 text-2xl font-semibold text-white">
              {money(data?.kpis?.total_estimated_value)}
            </div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-widest text-white/45">
              loan balance
            </div>
            <div className="mt-2 text-2xl font-semibold text-white">
              {money(data?.kpis?.total_loan_balance)}
            </div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-widest text-white/45">
              estimated equity
            </div>
            <div className="mt-2 text-2xl font-semibold text-white">
              {money(data?.kpis?.total_estimated_equity)}
            </div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-widest text-white/45">
              homes valued
            </div>
            <div className="mt-2 text-2xl font-semibold text-white">
              {Number(data?.kpis?.homes_with_valuation || 0)}
            </div>
          </div>
        </div>

        <Panel
          title="Top equity holders"
          subtitle="Useful for refinance / disposition thinking without manually spelunking every property page."
        >
          <div className="space-y-2">
            {leaders.length === 0 ? (
              <div className="text-sm text-white/55">
                No valuation-backed equity rows yet.
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
                        {row.latest_valuation_as_of
                          ? ` · as of ${row.latest_valuation_as_of.slice(0, 10)}`
                          : ""}
                      </div>
                    </div>
                    <div className="text-right text-xs text-white/70">
                      <div>equity: {money(row.estimated_equity)}</div>
                      <div>value: {money(row.latest_value)}</div>
                      <div>loan: {money(row.latest_loan_balance)}</div>
                    </div>
                  </div>
                </Link>
              ))
            )}
          </div>
        </Panel>
      </div>
    </PageShell>
  );
}
