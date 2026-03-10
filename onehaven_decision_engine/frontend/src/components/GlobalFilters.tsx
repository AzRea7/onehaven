import React from "react";
import clsx from "clsx";
import { useLocation, useNavigate } from "react-router-dom";
import { api } from "../lib/api";
import { Filters, readFilters, writeFilters } from "../lib/filters";

const STAGES = [
  { value: "", label: "All stages" },
  { value: "import", label: "Import" },
  { value: "deal", label: "Deal" },
  { value: "decision", label: "Decision" },
  { value: "acquisition", label: "Acquisition" },
  { value: "rehab_plan", label: "Rehab Planning" },
  { value: "rehab_exec", label: "Rehab Execution" },
  { value: "compliance", label: "Compliance" },
  { value: "tenant", label: "Tenant Placement" },
  { value: "lease", label: "Lease Active" },
  { value: "cash", label: "Cashflow" },
  { value: "equity", label: "Equity" },
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

function fieldClassName() {
  return [
    "w-full rounded-xl border border-white/10 bg-white/[0.04]",
    "px-3 py-2.5 text-sm text-white/90 placeholder:text-white/40",
    "outline-none transition",
    "focus:border-white/20 focus:ring-2 focus:ring-white/10",
  ].join(" ");
}

function labelClassName() {
  return "mb-1.5 text-[11px] font-medium uppercase tracking-[0.16em] text-white/45";
}

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

    api
      .get(`/meta/counties?state=${filters.state || "MI"}`)
      .then((r) => {
        const items = (r?.items || []) as any[];
        if (!mounted) return;
        setCounties(
          items
            .filter((x) => x?.county)
            .map((x) => ({
              county: x.county,
              count: x.count,
            })),
        );
      })
      .catch(() => {
        if (!mounted) return;
        setCounties([]);
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

  function clearAll() {
    set({ state: "MI" } as Filters);
  }

  return (
    <div className={clsx("gradient-border rounded-3xl p-[1px]", className)}>
      <div className="glass rounded-3xl p-4 md:p-5">
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-12">
          <div className="lg:col-span-3">
            <div className={labelClassName()}>Search</div>
            <input
              value={filters.search || ""}
              onChange={(e) => set({ ...filters, search: e.target.value })}
              placeholder="Search address / city / zip"
              className={fieldClassName()}
            />
          </div>

          <div className="lg:col-span-1">
            <div className={labelClassName()}>State</div>
            <input
              value={filters.state || "MI"}
              onChange={(e) => set({ ...filters, state: e.target.value })}
              placeholder="State"
              className={fieldClassName()}
            />
          </div>

          <div className="lg:col-span-2">
            <div className={labelClassName()}>City</div>
            <input
              value={filters.city || ""}
              onChange={(e) => set({ ...filters, city: e.target.value })}
              placeholder="City"
              className={fieldClassName()}
            />
          </div>

          <div className="lg:col-span-2">
            <div className={labelClassName()}>County</div>
            <select
              value={filters.county || ""}
              onChange={(e) => set({ ...filters, county: e.target.value })}
              className={fieldClassName()}
            >
              <option value="">All counties</option>
              {counties.map((c) => (
                <option key={c.county} value={c.county}>
                  {c.county} ({c.count})
                </option>
              ))}
            </select>
          </div>

          <div className="lg:col-span-2">
            <div className={labelClassName()}>Stage</div>
            <select
              value={filters.stage || ""}
              onChange={(e) => set({ ...filters, stage: e.target.value })}
              className={fieldClassName()}
            >
              {STAGES.map((s) => (
                <option key={s.value || "all"} value={s.value}>
                  {s.label}
                </option>
              ))}
            </select>
          </div>

          <div className="lg:col-span-2">
            <div className={labelClassName()}>Red zone</div>
            <select
              value={filters.red_zone || ""}
              onChange={(e) => set({ ...filters, red_zone: e.target.value })}
              className={fieldClassName()}
            >
              <option value="">All areas</option>
              <option value="false">Exclude red zone</option>
              <option value="true">Only red zone</option>
            </select>
          </div>

          <div className="lg:col-span-2">
            <div className={labelClassName()}>Crime min</div>
            <input
              value={filters.crime_min || ""}
              onChange={(e) => set({ ...filters, crime_min: e.target.value })}
              placeholder="Crime score min"
              inputMode="decimal"
              className={fieldClassName()}
            />
          </div>

          <div className="lg:col-span-2">
            <div className={labelClassName()}>Crime max</div>
            <input
              value={filters.crime_max || ""}
              onChange={(e) => set({ ...filters, crime_max: e.target.value })}
              placeholder="Crime score max"
              inputMode="decimal"
              className={fieldClassName()}
            />
          </div>

          <div className="lg:col-span-2">
            <div className={labelClassName()}>Offender min</div>
            <input
              value={filters.offender_min || ""}
              onChange={(e) =>
                set({ ...filters, offender_min: e.target.value })
              }
              placeholder="Offender count min"
              inputMode="numeric"
              className={fieldClassName()}
            />
          </div>

          <div className="lg:col-span-2">
            <div className={labelClassName()}>Offender max</div>
            <input
              value={filters.offender_max || ""}
              onChange={(e) =>
                set({ ...filters, offender_max: e.target.value })
              }
              placeholder="Offender count max"
              inputMode="numeric"
              className={fieldClassName()}
            />
          </div>

          <div className="lg:col-span-3">
            <div className={labelClassName()}>Sort</div>
            <select
              value={filters.sort || ""}
              onChange={(e) => set({ ...filters, sort: e.target.value })}
              className={fieldClassName()}
            >
              {SORTS.map((s) => (
                <option key={s.value || "default"} value={s.value}>
                  {s.label}
                </option>
              ))}
            </select>
          </div>

          <div className="lg:col-span-3 flex items-end justify-between gap-3">
            <button
              className="oh-btn cursor-pointer"
              onClick={clearAll}
              title="Clear all filters"
            >
              Clear filters
            </button>

            <div className="text-xs text-white/45 whitespace-nowrap">
              Filters persist in URL
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
