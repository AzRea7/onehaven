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

const SORTS = [
  { value: "", label: "Newest" },
  { value: "oldest", label: "Oldest" },
  { value: "address_asc", label: "Address A → Z" },
  { value: "address_desc", label: "Address Z → A" },
  { value: "crime_desc", label: "Crime high → low" },
  { value: "crime_asc", label: "Crime low → high" },
  { value: "offenders_desc", label: "Offenders high → low" },
  { value: "offenders_asc", label: "Offenders low → high" },
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

    api.get(`/meta/counties?state=${filters.state || "MI"}`).then((r) => {
      const items = (r?.items || []) as any[];
      if (!mounted) return;
      setCounties(
        items.map((x) => ({
          county: x.county,
          count: x.count,
        })),
      );
    });

    return () => {
      mounted = false;
    };
  }, [filters.state]);

  function set(next: Filters) {
    const p2 = writeFilters(params, next);
    const s = p2.toString();
    nav(`${loc.pathname}${s ? `?${s}` : ""}`, { replace: true });
  }

  return (
    <div className={className || ""} style={{ display: "grid", gap: 10 }}>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1.2fr 0.8fr 0.8fr 0.9fr 0.9fr",
          gap: 10,
        }}
      >
        <input
          value={filters.search || ""}
          onChange={(e) => set({ ...filters, search: e.target.value })}
          placeholder="Search address / city / zip"
          className="input"
        />

        <input
          value={filters.state || "MI"}
          onChange={(e) => set({ ...filters, state: e.target.value })}
          placeholder="State"
          className="input"
        />

        <input
          value={filters.city || ""}
          onChange={(e) => set({ ...filters, city: e.target.value })}
          placeholder="City"
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
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr 1fr 1fr 1fr",
          gap: 10,
        }}
      >
        <select
          value={filters.red_zone || ""}
          onChange={(e) => set({ ...filters, red_zone: e.target.value })}
          className="input"
        >
          <option value="">Red-zone: All</option>
          <option value="false">Exclude red zone</option>
          <option value="true">Only red zone</option>
        </select>

        <input
          value={filters.crime_min || ""}
          onChange={(e) => set({ ...filters, crime_min: e.target.value })}
          placeholder="Crime score min"
          className="input"
        />

        <input
          value={filters.crime_max || ""}
          onChange={(e) => set({ ...filters, crime_max: e.target.value })}
          placeholder="Crime score max"
          className="input"
        />

        <input
          value={filters.offender_min || ""}
          onChange={(e) => set({ ...filters, offender_min: e.target.value })}
          placeholder="Offender count min"
          className="input"
        />

        <input
          value={filters.offender_max || ""}
          onChange={(e) => set({ ...filters, offender_max: e.target.value })}
          placeholder="Offender count max"
          className="input"
        />
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr auto 1fr",
          gap: 10,
          alignItems: "center",
        }}
      >
        <select
          value={filters.sort || ""}
          onChange={(e) => set({ ...filters, sort: e.target.value })}
          className="input"
        >
          {SORTS.map((s) => (
            <option key={s.value || "default"} value={s.value}>
              {s.label}
            </option>
          ))}
        </select>

        <button
          className="btn"
          onClick={() => set({ state: "MI" })}
          style={{ justifySelf: "start" }}
          title="Clear all filters"
        >
          Clear filters
        </button>

        <div className="muted" style={{ justifySelf: "end" }}>
          Filters persist in URL
        </div>
      </div>
    </div>
  );
}
