// frontend/src/components/FilterBar.tsx
import React from "react";
import clsx from "clsx";
import { useSearchParams } from "react-router-dom";

type Opt = { value: string; label: string };

// IMPORTANT: stage order should mirror your desired workflow.
// Deal → Rehab → Compliance → Tenant → Cash → Equity
const STAGES: Opt[] = [
  { value: "", label: "All stages" },

  // optional early pipeline steps (keep if backend uses them)
  { value: "import", label: "Import" },
  { value: "decision", label: "Decision" },
  { value: "acquisition", label: "Acquisition" },

  // core ops flow (your “real” order)
  { value: "deal", label: "Deal" },
  { value: "rehab_plan", label: "Rehab Planning" },
  { value: "rehab_exec", label: "Rehab Execution" },
  { value: "compliance", label: "Compliance" },
  { value: "tenant", label: "Tenant Placement" },
  { value: "lease", label: "Lease Active" },
  { value: "cash", label: "Cashflow" },
  { value: "equity", label: "Equity" },
];

const DECISIONS: Opt[] = [
  { value: "", label: "All decisions" },
  { value: "undecided", label: "Undecided" },
  { value: "buy", label: "Buy" },
  { value: "watch", label: "Watch" },
  { value: "pass", label: "Pass" },
];

const REDZONE: Opt[] = [
  { value: "all", label: "All areas" },
  { value: "exclude", label: "Exclude red zones" },
  { value: "only", label: "Only red zones" },
];

const FINANCING: Opt[] = [
  { value: "all", label: "All financing" },
  { value: "cash", label: "Cash" },
  { value: "dscr", label: "DSCR" },
];

export default function FilterBar({
  counties,
  className,
  children,
}: {
  counties?: string[];
  className?: string;
  children?: React.ReactNode;
}) {
  const [sp, setSp] = useSearchParams();

  const set = (k: string, v: string) => {
    const next = new URLSearchParams(sp);

    // keep URL clean:
    // - empty string deletes
    // - "all" deletes (for toggles like red_zone/financing)
    if (!v || v === "all") next.delete(k);
    else next.set(k, v);

    setSp(next, { replace: true });
  };

  const get = (k: string, fallback = "") => sp.get(k) ?? fallback;

  return (
    <div
      className={clsx("oh-panel p-3 md:p-4", className)}
      style={{ contain: "layout paint" }}
    >
      <div className="grid grid-cols-1 md:grid-cols-12 gap-3">
        <div className="md:col-span-4">
          <div className="text-[11px] text-white/55 mb-1">Search</div>
          <input
            value={get("q", "")}
            onChange={(e) => set("q", e.target.value)}
            placeholder="address, city, zip, county…"
            className="w-full rounded-xl bg-black/40 border border-white/10 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          />
        </div>

        <div className="md:col-span-2">
          <div className="text-[11px] text-white/55 mb-1">County</div>
          <select
            value={get("county", "")}
            onChange={(e) => set("county", e.target.value)}
            className="w-full rounded-xl bg-black/40 border border-white/10 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10 cursor-pointer"
          >
            <option value="">All counties</option>
            {(counties || []).map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-2">
          <div className="text-[11px] text-white/55 mb-1">Decision</div>
          <select
            value={get("decision", "")}
            onChange={(e) => set("decision", e.target.value)}
            className="w-full rounded-xl bg-black/40 border border-white/10 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10 cursor-pointer"
          >
            {DECISIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-2">
          <div className="text-[11px] text-white/55 mb-1">Stage</div>
          <select
            value={get("stage", "")}
            onChange={(e) => set("stage", e.target.value)}
            className="w-full rounded-xl bg-black/40 border border-white/10 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10 cursor-pointer"
          >
            {STAGES.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-2">
          <div className="text-[11px] text-white/55 mb-1">Red zone</div>
          <select
            value={get("red_zone", "all")}
            onChange={(e) => set("red_zone", e.target.value)}
            className="w-full rounded-xl bg-black/40 border border-white/10 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10 cursor-pointer"
          >
            {REDZONE.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-2">
          <div className="text-[11px] text-white/55 mb-1">Financing</div>
          <select
            value={get("financing", "all")}
            onChange={(e) => set("financing", e.target.value)}
            className="w-full rounded-xl bg-black/40 border border-white/10 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10 cursor-pointer"
          >
            {FINANCING.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Optional extra row for injected controls */}
      {children ? <div className="mt-3">{children}</div> : null}
    </div>
  );
}
