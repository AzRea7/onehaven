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

export default function ComplianceDrilldown() {
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
      .get(`/ops/drilldown/compliance${qs}`)
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
  const rows = data?.leaderboards?.compliance_attention || [];
  const stageMix = data?.series?.stage_mix || [];

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Drilldown"
          title="Compliance exposure"
          subtitle="Where inspection friction, red-zone exposure, and open work are likely to create drag."
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
          <Metric
            label="red zone homes"
            value={Number(kpis.red_zone_count || 0)}
          />
          <Metric
            label="open rehab tasks"
            value={Number(data?.counts?.rehab_tasks_open || 0)}
          />
          <Metric label="avg crime score" value={kpis.avg_crime_score ?? "—"} />
          <Metric
            label="active leases"
            value={Number(kpis.active_leases || 0)}
          />
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <Panel
            title="Stage pressure"
            subtitle="Useful for spotting where work is bunching up before the pipeline starts acting like molasses."
          >
            <div className="space-y-3">
              {stageMix.length === 0 ? (
                <div className="text-sm text-white/55">No stage data yet.</div>
              ) : (
                stageMix.map((row: any) => (
                  <div
                    key={row.key}
                    className="rounded-xl border border-white/10 bg-white/[0.03] p-3"
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div className="text-sm text-white">{row.label}</div>
                      <div className="text-sm font-semibold text-white">
                        {row.count}
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </Panel>

          <Panel
            title="Needs attention"
            subtitle="Properties with compliance-ish drag signals."
          >
            <div className="space-y-2">
              {rows.length === 0 ? (
                <div className="text-sm text-white/55">
                  No attention list yet.
                </div>
              ) : (
                rows.map((row: any) => (
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
                          {row.stage
                            ? ` · ${String(row.stage).replace(/_/g, " ")}`
                            : ""}
                        </div>
                      </div>
                      <div className="text-right text-xs text-white/70">
                        <div>open tasks: {row.rehab_open ?? 0}</div>
                        <div>crime: {row.crime_score ?? "—"}</div>
                        <div>red zone: {String(!!row.is_red_zone)}</div>
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
