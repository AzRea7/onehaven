import React from "react";
import clsx from "clsx";
import { useSearchParams } from "react-router-dom";

type Opt = { value: string; label: string };

const STAGES: Opt[] = [
  { value: "", label: "All stages" },
  { value: "import", label: "Import" },
  { value: "decision", label: "Decision" },
  { value: "acquisition", label: "Acquisition" },
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
  { value: "PASS", label: "Good deal / PASS" },
  { value: "REVIEW", label: "Review" },
  { value: "REJECT", label: "Reject" },
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

const SORTS: Opt[] = [
  { value: "relevance", label: "Relevance" },
  { value: "best_cashflow", label: "Best cashflow" },
  { value: "best_dscr", label: "Best DSCR" },
  { value: "best_rent_gap", label: "Best rent gap" },
  { value: "lowest_risk", label: "Lowest risk" },
  { value: "newest", label: "Newest" },
  { value: "lowest_price", label: "Lowest price" },
  { value: "highest_price", label: "Highest price" },
];

const HIDDEN_REASONS: Opt[] = [
  { value: "all", label: "All" },
  { value: "inactive_listing", label: "Inactive listing" },
  { value: "low_score", label: "Low score" },
  { value: "bad_risk", label: "Bad risk" },
  { value: "weak_cashflow", label: "Weak cashflow" },
  { value: "weak_dscr", label: "Weak DSCR" },
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
      <div className="grid grid-cols-1 gap-3 md:grid-cols-12">
        <div className="md:col-span-4">
          <div className="mb-1 text-[11px] text-white/55">Search</div>
          <input
            value={get("q", "")}
            onChange={(e) => set("q", e.target.value)}
            placeholder="address, city, zip, county…"
            className="w-full rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          />
        </div>

        <div className="md:col-span-2">
          <div className="mb-1 text-[11px] text-white/55">County</div>
          <select
            value={get("county", "")}
            onChange={(e) => set("county", e.target.value)}
            className="w-full cursor-pointer rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
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
          <div className="mb-1 text-[11px] text-white/55">Decision</div>
          <select
            value={get("decision", "")}
            onChange={(e) => set("decision", e.target.value)}
            className="w-full cursor-pointer rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          >
            {DECISIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-2">
          <div className="mb-1 text-[11px] text-white/55">Stage</div>
          <select
            value={get("stage", "")}
            onChange={(e) => set("stage", e.target.value)}
            className="w-full cursor-pointer rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          >
            {STAGES.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-2">
          <div className="mb-1 text-[11px] text-white/55">Red zone</div>
          <select
            value={get("red_zone", "all")}
            onChange={(e) => set("red_zone", e.target.value)}
            className="w-full cursor-pointer rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          >
            {REDZONE.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-2">
          <div className="mb-1 text-[11px] text-white/55">Financing</div>
          <select
            value={get("financing", "all")}
            onChange={(e) => set("financing", e.target.value)}
            className="w-full cursor-pointer rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          >
            {FINANCING.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-3">
          <div className="mb-1 text-[11px] text-white/55">Sort</div>
          <select
            value={get("sort", "relevance")}
            onChange={(e) => set("sort", e.target.value)}
            className="w-full cursor-pointer rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          >
            {SORTS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-3">
          <div className="mb-1 text-[11px] text-white/55">Hidden reason</div>
          <select
            value={get("hidden_reason", "all")}
            onChange={(e) => set("hidden_reason", e.target.value)}
            className="w-full cursor-pointer rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          >
            {HIDDEN_REASONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-3">
          <div className="mb-1 text-[11px] text-white/55">Deals only</div>
          <select
            value={get("deals_only", "true")}
            onChange={(e) => set("deals_only", e.target.value)}
            className="w-full cursor-pointer rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          >
            <option value="true">Only deal candidates</option>
            <option value="false">Include everything</option>
          </select>
        </div>

        <div className="md:col-span-3">
          <div className="mb-1 text-[11px] text-white/55">Suppressed rows</div>
          <select
            value={get("include_suppressed", "false")}
            onChange={(e) => set("include_suppressed", e.target.value)}
            className="w-full cursor-pointer rounded-xl border border-white/10 bg-black/40 px-3 py-2 text-sm text-white/90 outline-none focus:border-white/25 focus:ring-2 focus:ring-white/10"
          >
            <option value="false">Hide suppressed</option>
            <option value="true">Include suppressed</option>
          </select>
        </div>
      </div>

      {children ? <div className="mt-3">{children}</div> : null}
    </div>
  );
}
