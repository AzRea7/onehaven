import React from "react";
import {
  AlertTriangle,
  Clock3,
  Search,
  SlidersHorizontal,
  RotateCcw,
  ShieldAlert,
  FileWarning,
  GitCompareArrows,
} from "lucide-react";

export type AcquisitionQueueFiltersValue = {
  search: string;
  waitingOn: string;
  urgency: string;
  status: string;
  missingDocsOnly: boolean;
  conflictsOnly: boolean;
  blockedOnly: boolean;
};

type Props = {
  value: AcquisitionQueueFiltersValue;
  onChange: (next: AcquisitionQueueFiltersValue) => void;
};

const DEFAULT_FILTERS: AcquisitionQueueFiltersValue = {
  search: "",
  waitingOn: "ALL",
  urgency: "ALL",
  status: "ALL",
  missingDocsOnly: false,
  conflictsOnly: false,
  blockedOnly: false,
};

export default function AcquisitionFilters({ value, onChange }: Props) {
  const patch = (partial: Partial<AcquisitionQueueFiltersValue>) => {
    onChange({ ...value, ...partial });
  };

  const activeCount = [
    value.search.trim() ? 1 : 0,
    value.waitingOn !== "ALL" ? 1 : 0,
    value.urgency !== "ALL" ? 1 : 0,
    value.status !== "ALL" ? 1 : 0,
    value.missingDocsOnly ? 1 : 0,
    value.conflictsOnly ? 1 : 0,
    value.blockedOnly ? 1 : 0,
  ].reduce((sum, n) => sum + n, 0);

  return (
    <div className="rounded-3xl border border-app bg-app-panel p-4">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Queue filters
          </div>
          <div className="mt-1 text-sm text-app-3">
            Filter by owner, urgency, blocking state, missing docs, and parser
            conflicts.
          </div>
        </div>

        <div className="flex items-center gap-2">
          <span className="oh-pill">
            {activeCount} active filter{activeCount === 1 ? "" : "s"}
          </span>
          <button
            type="button"
            onClick={() => onChange(DEFAULT_FILTERS)}
            className="oh-btn oh-btn-secondary"
          >
            <RotateCcw className="h-4 w-4" />
            Reset
          </button>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
        <label className="rounded-2xl border border-app bg-app px-4 py-3">
          <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <Search className="h-3.5 w-3.5" />
            Search
          </div>
          <input
            value={value.search}
            onChange={(e) => patch({ search: e.target.value })}
            placeholder="address, city, next step, waiting on…"
            className="w-full bg-transparent text-sm text-app-0 outline-none"
          />
        </label>

        <label className="rounded-2xl border border-app bg-app px-4 py-3">
          <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <SlidersHorizontal className="h-3.5 w-3.5" />
            Waiting on
          </div>
          <select
            value={value.waitingOn}
            onChange={(e) => patch({ waitingOn: e.target.value })}
            className="w-full bg-transparent text-sm text-app-0 outline-none"
          >
            <option value="ALL">All owners</option>
            <option value="lender">Lender</option>
            <option value="title">Title</option>
            <option value="seller">Seller</option>
            <option value="operator">Operator</option>
            <option value="document">Document</option>
            <option value="other">Other</option>
          </select>
        </label>

        <label className="rounded-2xl border border-app bg-app px-4 py-3">
          <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <Clock3 className="h-3.5 w-3.5" />
            Urgency
          </div>
          <select
            value={value.urgency}
            onChange={(e) => patch({ urgency: e.target.value })}
            className="w-full bg-transparent text-sm text-app-0 outline-none"
          >
            <option value="ALL">All urgency</option>
            <option value="OVERDUE">Overdue</option>
            <option value="DUE_SOON">Due soon</option>
            <option value="ON_TRACK">On track</option>
            <option value="BLOCKED">Blocked</option>
          </select>
        </label>

        <label className="rounded-2xl border border-app bg-app px-4 py-3">
          <div className="mb-2 flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
            <AlertTriangle className="h-3.5 w-3.5" />
            Status
          </div>
          <select
            value={value.status}
            onChange={(e) => patch({ status: e.target.value })}
            className="w-full bg-transparent text-sm text-app-0 outline-none"
          >
            <option value="ALL">All statuses</option>
            <option value="blocked">Blocked</option>
            <option value="under_contract">Under contract</option>
            <option value="closing">Closing</option>
            <option value="needs_review">Needs review</option>
            <option value="ready_to_close">Ready to close</option>
            <option value="waiting_on_docs">Waiting on docs</option>
            <option value="waiting_on_lender">Waiting on lender</option>
            <option value="waiting_on_title">Waiting on title</option>
          </select>
        </label>

        <div className="grid gap-3">
          <label className="flex items-center gap-3 rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-1">
            <input
              type="checkbox"
              checked={value.missingDocsOnly}
              onChange={(e) => patch({ missingDocsOnly: e.target.checked })}
            />
            <FileWarning className="h-4 w-4 text-app-4" />
            Missing docs only
          </label>

          <label className="flex items-center gap-3 rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-1">
            <input
              type="checkbox"
              checked={value.conflictsOnly}
              onChange={(e) => patch({ conflictsOnly: e.target.checked })}
            />
            <GitCompareArrows className="h-4 w-4 text-app-4" />
            Conflicts only
          </label>
        </div>

        <div className="grid gap-3">
          <label className="flex items-center gap-3 rounded-2xl border border-app bg-app px-4 py-3 text-sm text-app-1">
            <input
              type="checkbox"
              checked={value.blockedOnly}
              onChange={(e) => patch({ blockedOnly: e.target.checked })}
            />
            <ShieldAlert className="h-4 w-4 text-app-4" />
            Blocked only
          </label>

          <div className="rounded-2xl border border-dashed border-app bg-app px-4 py-3 text-xs text-app-4">
            Use these with queue scoring so the highest-risk files rise to the
            top first.
          </div>
        </div>
      </div>
    </div>
  );
}
