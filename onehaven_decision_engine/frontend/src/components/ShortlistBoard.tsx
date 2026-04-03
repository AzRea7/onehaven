import React from "react";
import { Link } from "react-router-dom";
import {
  ArrowRight,
  Bookmark,
  Clock3,
  FileX,
  Flag,
  Wallet,
} from "lucide-react";
import {
  acquisitionStageLabel,
  canStartAcquisition,
  startAcquisitionBlockers,
} from "../lib/dealRules";

type Row = any;
type Props = {
  rows: Row[];
  onStartAcquisition?: (row: Row) => void | Promise<void>;
};

const COLUMNS = [
  { key: "saved", label: "Saved", icon: Bookmark },
  { key: "shortlisted", label: "Shortlisted", icon: Flag },
  { key: "review_later", label: "Review later", icon: Clock3 },
  { key: "offer_candidate", label: "Offer candidate", icon: Wallet },
  { key: "rejected", label: "Rejected", icon: FileX },
] as const;

function propertyIdOf(row: any) {
  const value = Number(row?.id || row?.property_id || row?.property?.id);
  return Number.isFinite(value) ? value : 0;
}
function tagsOf(row: any): string[] {
  return Array.isArray(row?.acquisition_tags) ? row.acquisition_tags : [];
}
function firstTag(row: any) {
  const tags = tagsOf(row);
  for (const c of COLUMNS) if (tags.includes(c.key)) return c.key;
  return null;
}
function money(value: any) {
  const n = Number(value);
  return Number.isFinite(n)
    ? n.toLocaleString(undefined, {
        style: "currency",
        currency: "USD",
        maximumFractionDigits: 0,
      })
    : "—";
}

export default function ShortlistBoard({ rows, onStartAcquisition }: Props) {
  const grouped = new Map<string, Row[]>();
  for (const column of COLUMNS) grouped.set(column.key, []);
  for (const row of rows) {
    const tag = firstTag(row);
    if (tag && grouped.has(tag)) grouped.get(tag)!.push(row);
  }

  return (
    <div className="grid gap-4 xl:grid-cols-5">
      {COLUMNS.map(({ key, label, icon: Icon }) => {
        const items = grouped.get(key) || [];
        return (
          <div
            key={key}
            className="rounded-3xl border border-app bg-app-panel p-4"
          >
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <Icon className="h-4 w-4" />
                {label}
              </div>
              <span className="oh-pill">{items.length}</span>
            </div>
            <div className="mt-4 space-y-3">
              {!items.length ? (
                <div className="rounded-2xl border border-dashed border-app px-3 py-4 text-xs text-app-4">
                  No properties in this lane.
                </div>
              ) : (
                items.slice(0, 6).map((row) => {
                  const property = row?.property || row;
                  const propertyId = propertyIdOf(row);
                  const stage =
                    row?.workflow?.current_stage || row?.current_stage;
                  const startable = canStartAcquisition(row);
                  const blockers = startAcquisitionBlockers(row);
                  return (
                    <div
                      key={propertyId}
                      className="rounded-2xl border border-app bg-app-muted p-3"
                    >
                      <div className="text-sm font-medium text-app-0">
                        {property?.address || `Property #${propertyId}`}
                      </div>
                      <div className="mt-1 text-xs text-app-4">
                        {[property?.city, property?.state, property?.zip]
                          .filter(Boolean)
                          .join(", ")}
                      </div>
                      <div className="mt-2 text-xs text-app-3">
                        asking{" "}
                        {money(
                          row?.purchase_price ||
                            row?.asking_price ||
                            property?.asking_price ||
                            property?.price,
                        )}
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2 text-xs">
                        <span className="oh-pill">
                          {acquisitionStageLabel(stage)}
                        </span>
                        {startable ? (
                          <span className="oh-pill oh-pill-good">
                            ready for Acquire
                          </span>
                        ) : null}
                      </div>
                      {!startable && blockers.length ? (
                        <div className="mt-2 text-[11px] text-app-4">
                          {blockers.slice(0, 2).join(" • ")}
                        </div>
                      ) : null}
                      <div className="mt-3 flex items-center justify-between gap-2">
                        <Link
                          to={`/properties/${propertyId}`}
                          className="inline-flex items-center gap-1 text-xs font-medium text-app-1 hover:text-white"
                        >
                          Open
                          <ArrowRight className="h-3.5 w-3.5" />
                        </Link>
                        {startable ? (
                          <button
                            type="button"
                            onClick={() => onStartAcquisition?.(row)}
                            className="oh-btn oh-btn-secondary text-xs"
                          >
                            Start acquisition
                          </button>
                        ) : null}
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
