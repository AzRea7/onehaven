import React from "react";
import { RotateCcw, SlidersHorizontal } from "lucide-react";
import { useLocation, useNavigate } from "react-router-dom";

import { api } from "../lib/api";
import { Filters, readFilters, writeFilters } from "../lib/filters";
import Surface from "./Surface";

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

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label className="field">
      <span className="field-label">{label}</span>
      {children}
    </label>
  );
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
  const [expanded, setExpanded] = React.useState(false);

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
              county: String(x.county),
              count: Number(x.count || 0),
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
    const nextParams = writeFilters(params, next);
    nav(
      { pathname: loc.pathname, search: nextParams.toString() },
      { replace: true },
    );
  }

  function clearAll() {
    nav({ pathname: loc.pathname, search: "" }, { replace: true });
  }

  const activeCount = Object.values(filters).filter(Boolean).length;

  return (
    <Surface
      className={className}
      title="Global filters"
      subtitle="URL-backed filters shared across dashboard, properties, and drilldowns."
      padding="md"
      actions={
        <div className="flex items-center gap-2">
          <button
            className="btn btn-secondary btn-sm"
            onClick={() => setExpanded((v) => !v)}
          >
            <SlidersHorizontal className="h-4 w-4" />
            {expanded ? "Collapse" : "Expand"}
          </button>

          <button className="btn btn-secondary btn-sm" onClick={clearAll}>
            <RotateCcw className="h-4 w-4" />
            Clear
          </button>

          <span className="badge">{activeCount} active</span>
        </div>
      }
    >
      <div className="grid gap-3 lg:grid-cols-12">
        <div className="lg:col-span-4">
          <Field label="Search">
            <input
              value={filters.search || ""}
              onChange={(e) => set({ ...filters, search: e.target.value })}
              placeholder="Search address / city / zip"
              className="field-input"
            />
          </Field>
        </div>

        <div className="lg:col-span-2">
          <Field label="State">
            <input
              value={filters.state || "MI"}
              onChange={(e) => set({ ...filters, state: e.target.value })}
              placeholder="State"
              className="field-input"
            />
          </Field>
        </div>

        <div className="lg:col-span-3">
          <Field label="City">
            <input
              value={filters.city || ""}
              onChange={(e) => set({ ...filters, city: e.target.value })}
              placeholder="City"
              className="field-input"
            />
          </Field>
        </div>

        <div className="lg:col-span-3">
          <Field label="County">
            <select
              value={filters.county || ""}
              onChange={(e) => set({ ...filters, county: e.target.value })}
              className="field-input"
            >
              <option value="">All counties</option>
              {counties.map((c) => (
                <option key={c.county} value={c.county}>
                  {c.county} ({c.count})
                </option>
              ))}
            </select>
          </Field>
        </div>

        <div className="lg:col-span-3">
          <Field label="Stage">
            <select
              value={filters.stage || ""}
              onChange={(e) => set({ ...filters, stage: e.target.value })}
              className="field-input"
            >
              {STAGES.map((s) => (
                <option key={s.value || "all"} value={s.value}>
                  {s.label}
                </option>
              ))}
            </select>
          </Field>
        </div>

        <div className="lg:col-span-3">
          <Field label="Decision">
            <select
              value={filters.decision || ""}
              onChange={(e) => set({ ...filters, decision: e.target.value })}
              className="field-input"
            >
              <option value="">All decisions</option>
              <option value="PASS">PASS</option>
              <option value="REVIEW">REVIEW</option>
              <option value="REJECT">REJECT</option>
            </select>
          </Field>
        </div>

        <div className="lg:col-span-3">
          <Field label="Red zone">
            <select
              value={filters.red_zone || ""}
              onChange={(e) => set({ ...filters, red_zone: e.target.value })}
              className="field-input"
            >
              <option value="">All areas</option>
              <option value="false">Exclude red zone</option>
              <option value="true">Only red zone</option>
            </select>
          </Field>
        </div>

        <div className="lg:col-span-3">
          <Field label="Sort">
            <select
              value={filters.sort || ""}
              onChange={(e) => set({ ...filters, sort: e.target.value })}
              className="field-input"
            >
              {SORTS.map((s) => (
                <option key={s.value || "default"} value={s.value}>
                  {s.label}
                </option>
              ))}
            </select>
          </Field>
        </div>

        {expanded ? (
          <>
            <div className="lg:col-span-3">
              <Field label="Crime min">
                <input
                  value={filters.crime_min || ""}
                  onChange={(e) =>
                    set({ ...filters, crime_min: e.target.value })
                  }
                  placeholder="Minimum"
                  inputMode="decimal"
                  className="field-input"
                />
              </Field>
            </div>

            <div className="lg:col-span-3">
              <Field label="Crime max">
                <input
                  value={filters.crime_max || ""}
                  onChange={(e) =>
                    set({ ...filters, crime_max: e.target.value })
                  }
                  placeholder="Maximum"
                  inputMode="decimal"
                  className="field-input"
                />
              </Field>
            </div>

            <div className="lg:col-span-3">
              <Field label="Offender min">
                <input
                  value={filters.offender_min || ""}
                  onChange={(e) =>
                    set({ ...filters, offender_min: e.target.value })
                  }
                  placeholder="Minimum"
                  inputMode="numeric"
                  className="field-input"
                />
              </Field>
            </div>

            <div className="lg:col-span-3">
              <Field label="Offender max">
                <input
                  value={filters.offender_max || ""}
                  onChange={(e) =>
                    set({ ...filters, offender_max: e.target.value })
                  }
                  placeholder="Maximum"
                  inputMode="numeric"
                  className="field-input"
                />
              </Field>
            </div>
          </>
        ) : null}
      </div>
    </Surface>
  );
}
