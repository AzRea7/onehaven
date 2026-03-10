import React from "react";
import { Link, useLocation } from "react-router-dom";
import PageHero from "../../components/PageHero";
import PageShell from "../../components/PageShell";
import GlobalFilters from "../../components/GlobalFilters";
import { api } from "../../lib/api";
import { readFilters, toQueryString } from "../../lib/filters";

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

function Metric({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
      <div className="text-[11px] uppercase tracking-widest text-white/45">
        {label}
      </div>
      <div className="mt-2 text-2xl font-semibold text-white">{value}</div>
    </div>
  );
}

export default function TrustDrilldown() {
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
      .get(`/ops/drilldown/trust${qs}`)
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

  const kpis = data?.kpis || {};
  const deals = data?.leaderboards?.good_deals || [];
  const decisions = data?.series?.decision_mix || [];

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Drilldown"
          title="Trust / decision quality"
          subtitle="How strong the current filtered opportunity set looks before you commit time, capital, or your remaining patience."
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
          <Metric label="good deals" value={Number(kpis.good_deals || 0)} />
          <Metric label="review deals" value={Number(kpis.review_deals || 0)} />
          <Metric label="rejected" value={Number(kpis.rejected_deals || 0)} />
          <Metric label="homes" value={Number(kpis.total_homes || 0)} />
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <Panel
            title="Decision mix"
            subtitle="If this gets too reject-heavy, your sourcing or filters are probably feeding the beast junk."
          >
            <div className="space-y-3">
              {decisions.length === 0 ? (
                <div className="text-sm text-white/55">
                  No decision rows yet.
                </div>
              ) : (
                decisions.map((row: any) => {
                  const count = Number(row.count || 0);
                  const total = Math.max(1, Number(kpis.total_homes || 0));
                  const width = (count / total) * 100;

                  return (
                    <div key={row.key} className="space-y-1">
                      <div className="flex items-center justify-between gap-3 text-xs">
                        <span className="text-white/70">{row.label}</span>
                        <span className="text-white font-semibold">
                          {count}
                        </span>
                      </div>
                      <div className="h-2 rounded-full bg-white/5 overflow-hidden">
                        <div
                          className="h-full rounded-full bg-white/70"
                          style={{ width: `${Math.max(6, width)}%` }}
                        />
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </Panel>

          <Panel
            title="Top opportunities"
            subtitle="Best current survivors by decision quality / score."
          >
            <div className="space-y-2">
              {deals.length === 0 ? (
                <div className="text-sm text-white/55">
                  No strong survivors yet.
                </div>
              ) : (
                deals.map((row: any) => (
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
                          {row.stage
                            ? ` · ${String(row.stage).replace(/_/g, " ")}`
                            : ""}
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="text-sm font-semibold text-white">
                          {row.latest_decision || "—"}
                        </div>
                        <div className="text-xs text-white/55">
                          {row.score != null ? `score ${row.score}` : ""}
                          {row.dscr != null
                            ? ` · dscr ${Number(row.dscr).toFixed(2)}`
                            : ""}
                        </div>
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
