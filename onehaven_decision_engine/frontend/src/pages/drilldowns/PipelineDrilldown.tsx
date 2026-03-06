import React from "react";
import { Link, useLocation } from "react-router-dom";
import PageHero from "../../components/PageHero";
import GlobalFilters from "../../components/GlobalFilters";
import PageShell from "../../components/PageShell";
import { api } from "../../lib/api";
import { readFilters, toQueryString } from "../../lib/filters";

export default function PipelineDrilldown() {
  const loc = useLocation();
  const params = React.useMemo(
    () => new URLSearchParams(loc.search),
    [loc.search],
  );
  const filters = React.useMemo(() => readFilters(params), [params]);

  const [rollups, setRollups] = React.useState<any>(null);
  const [props, setProps] = React.useState<any[]>([]);
  const [err, setErr] = React.useState<string | null>(null);

  React.useEffect(() => {
    let cancelled = false;
    const qs = toQueryString(filters);

    Promise.all([api.get(`/ops/rollups${qs}`), api.get(`/properties${qs}`)])
      .then(([r, p]) => {
        if (cancelled) return;
        setRollups(r);
        setProps(Array.isArray(p) ? p : p?.items || []);
        setErr(null);
      })
      .catch((e: any) => {
        if (cancelled) return;
        setErr(String(e?.message || e));
      });

    return () => {
      cancelled = true;
    };
  }, [loc.search, filters]);

  const stageCounts = rollups?.stage_counts || {};

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Pipeline"
          title="Stage distribution"
          subtitle="Stage counts plus the properties contributing to the current filtered view."
        />

        <div className="oh-panel p-4">
          <GlobalFilters />
        </div>

        {err && (
          <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
            {err}
          </div>
        )}

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <div className="oh-panel p-5">
            <div className="text-sm font-semibold text-white">Stage counts</div>
            <div className="mt-4 grid grid-cols-2 md:grid-cols-3 gap-3">
              {Object.keys(stageCounts).length === 0 ? (
                <div className="text-sm text-white/55">
                  No stage counts yet.
                </div>
              ) : (
                Object.entries(stageCounts).map(([stage, count]) => (
                  <div
                    key={stage}
                    className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
                  >
                    <div className="text-[11px] uppercase tracking-widest text-white/45">
                      {String(stage).replace(/_/g, " ")}
                    </div>
                    <div className="mt-2 text-2xl font-semibold text-white">
                      {String(count)}
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          <div className="oh-panel p-5">
            <div className="text-sm font-semibold text-white">
              Properties (filtered)
            </div>
            <div className="mt-4 space-y-2">
              {props.length === 0 ? (
                <div className="text-sm text-white/55">
                  No properties for these filters.
                </div>
              ) : (
                props.slice(0, 30).map((p) => (
                  <Link
                    key={p.id}
                    to={`/properties/${p.id}`}
                    className="block rounded-2xl border border-white/10 bg-white/[0.03] hover:bg-white/[0.05] hover:border-white/[0.16] transition p-4"
                  >
                    <div className="font-semibold text-white">{p.address}</div>
                    <div className="text-xs text-white/55 mt-1">
                      {p.city}, {p.state} • {p.county || "—"} • red_zone:{" "}
                      {String(!!p.is_red_zone)}
                    </div>
                  </Link>
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </PageShell>
  );
}
