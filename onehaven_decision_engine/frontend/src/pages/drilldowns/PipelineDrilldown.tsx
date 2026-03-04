import React from "react";
import { useLocation } from "react-router-dom";
import PageHero from "../../components/PageHero";
import GlobalFilters from "../../components/GlobalFilters";
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

  React.useEffect(() => {
    const qs = toQueryString(filters);
    Promise.all([
      api.get(`/ops/rollups${qs}`),
      api.get(`/properties${qs}`),
    ]).then(([r, p]) => {
      setRollups(r);
      setProps(Array.isArray(p) ? p : p?.items || []);
    });
  }, [loc.search]);

  return (
    <div className="page">
      <PageHero
        title="Pipeline"
        subtitle="Stage distribution + contributing properties."
      />
      <div className="panel">
        <GlobalFilters />
      </div>

      <div
        className="panel"
        style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14 }}
      >
        <div className="surface">
          <div style={{ fontSize: 16, fontWeight: 800 }}>Stage counts</div>
          <pre className="muted" style={{ whiteSpace: "pre-wrap" }}>
            {JSON.stringify(rollups?.stage_counts || {}, null, 2)}
          </pre>
        </div>

        <div className="surface">
          <div style={{ fontSize: 16, fontWeight: 800 }}>
            Properties (filtered)
          </div>
          <div style={{ display: "grid", gap: 8, marginTop: 8 }}>
            {props.slice(0, 30).map((p) => (
              <a
                key={p.id}
                href={`/properties/${p.id}`}
                className="rowHover"
                style={{ cursor: "pointer" }}
              >
                <div style={{ fontWeight: 700 }}>{p.address}</div>
                <div className="muted" style={{ fontSize: 12 }}>
                  {p.city}, {p.state} • {p.county || "—"} • red_zone:{" "}
                  {String(!!p.is_red_zone)}
                </div>
              </a>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
