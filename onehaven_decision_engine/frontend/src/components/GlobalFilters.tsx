// onehaven_decision_engine/frontend/src/components/GlobalFilters.tsx
import React from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { api } from "../lib/api";
import { Filters, readFilters, writeFilters } from "../lib/filters";

const STAGES = [
  "import",
  "deal",
  "decision",
  "acquisition",
  "rehab_plan",
  "rehab_exec",
  "compliance",
  "tenant",
  "lease",
  "cash",
  "equity",
];

export default function GlobalFilters({ className }: { className?: string }) {
  const loc = useLocation();
  const nav = useNavigate();
  const params = React.useMemo(
    () => new URLSearchParams(loc.search),
    [loc.search],
  );
  const filters = React.useMemo(() => readFilters(params), [params]);

  const [counties, setCounties] = React.useState<
    { county: string; count: number }[]
  >([]);

  React.useEffect(() => {
    let mounted = true;
    api.get(`/meta/counties?state=MI`).then((r) => {
      const items = (r?.items || []) as any[];
      if (!mounted) return;
      setCounties(items.map((x) => ({ county: x.county, count: x.count })));
    });
    return () => {
      mounted = false;
    };
  }, []);

  function set(next: Filters) {
    const p2 = writeFilters(params, next);
    nav(`${loc.pathname}?${p2.toString()}`, { replace: true });
  }

  return (
    <div className={className || ""} style={{ display: "grid", gap: 10 }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1.2fr 1fr 1fr 1fr",
          gap: 10,
        }}
      >
        <input
          value={filters.search || ""}
          onChange={(e) => set({ ...filters, search: e.target.value })}
          placeholder="Search address / city / zip"
          className="input"
        />

        <select
          value={filters.county || ""}
          onChange={(e) => set({ ...filters, county: e.target.value })}
          className="input"
        >
          <option value="">All counties</option>
          {counties.map((c) => (
            <option key={c.county} value={c.county}>
              {c.county} ({c.count})
            </option>
          ))}
        </select>

        <select
          value={filters.stage || ""}
          onChange={(e) => set({ ...filters, stage: e.target.value })}
          className="input"
        >
          <option value="">All stages</option>
          {STAGES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>

        <select
          value={filters.red_zone || ""}
          onChange={(e) => set({ ...filters, red_zone: e.target.value })}
          className="input"
        >
          <option value="">Red-zone: All</option>
          <option value="false">Exclude red zone</option>
          <option value="true">Only red zone</option>
        </select>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr 1fr 1fr",
          gap: 10,
        }}
      >
        <input
          value={filters.crime_max || ""}
          onChange={(e) => set({ ...filters, crime_max: e.target.value })}
          placeholder="Crime score max"
          className="input"
        />
        <input
          value={filters.offender_max || ""}
          onChange={(e) => set({ ...filters, offender_max: e.target.value })}
          placeholder="Offender count max"
          className="input"
        />

        <button
          className="btn"
          onClick={() => set({})}
          style={{ justifySelf: "start" }}
          title="Clear all filters"
        >
          Clear filters
        </button>

        <div className="muted" style={{ alignSelf: "center" }}>
          Filters persist in URL (drilldowns share state)
        </div>
      </div>
    </div>
  );
}
