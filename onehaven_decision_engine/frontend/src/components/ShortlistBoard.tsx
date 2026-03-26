import React from "react";
import { Link } from "react-router-dom";
import {
  ArrowRight,
  Bookmark,
  Clock3,
  FileWarning,
  FileX,
  Flag,
  GitCompareArrows,
  ShieldAlert,
  Wallet,
} from "lucide-react";
import AcquisitionTagBar from "./AcquisitionTagBar";

type Row = any;

type Props = {
  rows: Row[];
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
  for (const column of COLUMNS) {
    if (tags.includes(column.key)) return column.key;
  }
  return null;
}

function money(value: any) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function urgency(row: any) {
  const days = Number(row?.days_to_close);
  const waiting = String(row?.waiting_on || "").toLowerCase();
  const status = String(row?.status || "").toLowerCase();
  const nextStep = String(row?.next_step || "").toLowerCase();

  if (
    waiting.includes("blocked") ||
    status.includes("blocked") ||
    nextStep.includes("blocked")
  ) {
    return "blocked";
  }

  if (!Number.isFinite(days)) return "active";
  if (days < 0) return "overdue";
  if (days <= 7) return "due_soon";
  return "on_track";
}

function urgencyClass(u: string) {
  if (u === "blocked" || u === "overdue") return "oh-pill oh-pill-bad";
  if (u === "due_soon") return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-good";
}

function urgencyLabel(u: string) {
  if (u === "due_soon") return "due soon";
  if (u === "on_track") return "on track";
  return u;
}

function missingDocCount(row: any) {
  if (Number.isFinite(Number(row?.missing_document_groups_count))) {
    return Number(row.missing_document_groups_count);
  }

  if (Array.isArray(row?.missing_document_groups)) {
    return row.missing_document_groups.length;
  }

  if (Array.isArray(row?.required_documents)) {
    return row.required_documents.filter((d: any) => !d?.present).length;
  }

  return 0;
}

function conflictCount(row: any) {
  if (Number.isFinite(Number(row?.conflict_count))) {
    return Number(row.conflict_count);
  }

  if (Array.isArray(row?.field_conflicts)) {
    return row.field_conflicts.length;
  }

  if (Array.isArray(row?.parsed_value_conflicts)) {
    return row.parsed_value_conflicts.length;
  }

  return 0;
}

function readinessScore(row: any) {
  const explicit = Number(row?.estimated_close_readiness);
  if (Number.isFinite(explicit)) {
    return Math.max(0, Math.min(100, Math.round(explicit)));
  }

  const docsMissing = missingDocCount(row);
  const conflicts = conflictCount(row);
  const days = Number(row?.days_to_close);
  const waiting = String(row?.waiting_on || "").toLowerCase();

  let score = 65;

  score -= docsMissing * 12;
  score -= conflicts * 10;

  if (waiting.includes("blocked")) score -= 20;
  else if (waiting.includes("document")) score -= 10;
  else if (waiting.includes("title")) score -= 6;
  else if (waiting.includes("lender")) score -= 6;
  else if (waiting.includes("seller")) score -= 4;

  if (Number.isFinite(days)) {
    if (days < 0) score -= 14;
    else if (days <= 7) score -= 6;
    else if (days > 21) score += 6;
  }

  return Math.max(0, Math.min(100, Math.round(score)));
}

function readinessClass(score: number) {
  if (score >= 75) return "oh-pill oh-pill-good";
  if (score >= 45) return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-bad";
}

function nextRequiredDocument(row: any) {
  if (row?.next_required_document) return String(row.next_required_document);

  if (
    Array.isArray(row?.missing_document_groups) &&
    row.missing_document_groups.length
  ) {
    const first = row.missing_document_groups[0];
    return String(first?.label || first?.kind || "Required document");
  }

  if (Array.isArray(row?.required_documents)) {
    const firstMissing = row.required_documents.find((d: any) => !d?.present);
    if (firstMissing) {
      return String(
        firstMissing?.label || firstMissing?.kind || "Required document",
      );
    }
  }

  return "No missing required documents";
}

export default function ShortlistBoard({ rows }: Props) {
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
                  const u = urgency(row);
                  const docsMissing = missingDocCount(row);
                  const conflicts = conflictCount(row);
                  const readiness = readinessScore(row);
                  const nextDoc = nextRequiredDocument(row);

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
                        <span className={urgencyClass(u)}>
                          {urgencyLabel(u)}
                        </span>

                        {row?.waiting_on ? (
                          <span className="oh-pill">
                            waiting on {row.waiting_on}
                          </span>
                        ) : null}

                        <span className={readinessClass(readiness)}>
                          readiness {readiness}%
                        </span>
                      </div>

                      <div className="mt-3 grid gap-2 text-xs">
                        <div className="flex flex-wrap gap-2">
                          <span
                            className={
                              docsMissing > 0
                                ? "oh-pill oh-pill-warn"
                                : "oh-pill"
                            }
                          >
                            <FileWarning className="mr-1 h-3.5 w-3.5" />
                            {docsMissing} missing doc group
                            {docsMissing === 1 ? "" : "s"}
                          </span>

                          <span
                            className={
                              conflicts > 0 ? "oh-pill oh-pill-warn" : "oh-pill"
                            }
                          >
                            <GitCompareArrows className="mr-1 h-3.5 w-3.5" />
                            {conflicts} conflict{conflicts === 1 ? "" : "s"}
                          </span>
                        </div>

                        <div className="rounded-2xl border border-app bg-app-panel px-3 py-2 text-app-3">
                          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.16em] text-app-4">
                            <ShieldAlert className="h-3.5 w-3.5" />
                            Next required document
                          </div>
                          <div className="mt-1 text-xs text-app-1">
                            {nextDoc}
                          </div>
                        </div>
                      </div>

                      <div className="mt-3">
                        <AcquisitionTagBar
                          propertyId={propertyId}
                          value={tagsOf(row)}
                          compact
                        />
                      </div>

                      <Link
                        to={`/properties/${propertyId}`}
                        className="mt-3 inline-flex items-center gap-2 text-xs text-app-2 hover:text-app-0"
                      >
                        Open property
                        <ArrowRight className="h-3.5 w-3.5" />
                      </Link>
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
