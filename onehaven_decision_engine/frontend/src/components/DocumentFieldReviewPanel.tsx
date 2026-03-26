import React from "react";
import {
  AlertTriangle,
  CheckCircle2,
  GitCompareArrows,
  Loader2,
  RotateCcw,
  XCircle,
} from "lucide-react";
import { api } from "../lib/api";

export type FieldValueRow = {
  id?: number;
  field_name?: string;
  value_text?: string | null;
  value_number?: number | null;
  review_state?: string | null;
  confidence?: number | null;
  extraction_version?: string | null;
  manually_overridden?: boolean | null;
  source_document_id?: number | null;
  source_document_name?: string | null;
};

type Props = {
  propertyId: number;
  values?: FieldValueRow[];
  onChanged?: () => void;
};

function displayValue(v: FieldValueRow) {
  if (v.value_text != null && v.value_text !== "") return String(v.value_text);
  if (v.value_number != null && Number.isFinite(Number(v.value_number))) {
    return String(v.value_number);
  }
  return "—";
}

function normalizedDisplayValue(v: FieldValueRow) {
  return displayValue(v).trim().toLowerCase();
}

function groupByField(values: FieldValueRow[]) {
  return values.reduce<Record<string, FieldValueRow[]>>((acc, row) => {
    const key = String(row.field_name || "unknown");
    acc[key] = acc[key] || [];
    acc[key].push(row);
    return acc;
  }, {});
}

function stateClass(state: string) {
  if (state === "accepted") return "oh-pill oh-pill-good";
  if (state === "rejected") return "oh-pill oh-pill-bad";
  if (state === "superseded") return "oh-pill";
  return "oh-pill oh-pill-warn";
}

function formatError(e: any) {
  const status = e?.response?.status;
  const detail =
    e?.response?.data?.detail || e?.response?.data?.message || e?.message;
  if (status && detail) return `(${status}) ${String(detail)}`;
  if (detail) return String(detail);
  return "Failed to update field review.";
}

function confidenceLabel(value?: number | null) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(2);
}

function sortRows(rows: FieldValueRow[]) {
  return [...rows].sort((a, b) => {
    const stateA = String(a.review_state || "suggested").toLowerCase();
    const stateB = String(b.review_state || "suggested").toLowerCase();

    const stateRank = (state: string) => {
      if (state === "accepted") return 0;
      if (state === "suggested") return 1;
      if (state === "superseded") return 2;
      if (state === "rejected") return 3;
      return 4;
    };

    const stateDiff = stateRank(stateA) - stateRank(stateB);
    if (stateDiff !== 0) return stateDiff;

    const confA = Number(a.confidence);
    const confB = Number(b.confidence);
    const validA = Number.isFinite(confA) ? confA : -1;
    const validB = Number.isFinite(confB) ? confB : -1;

    if (validA !== validB) return validB - validA;

    return displayValue(a).localeCompare(displayValue(b));
  });
}

export default function DocumentFieldReviewPanel({
  propertyId,
  values = [],
  onChanged,
}: Props) {
  const [pendingId, setPendingId] = React.useState<number | null>(null);
  const [error, setError] = React.useState<string | null>(null);

  const grouped = React.useMemo(() => groupByField(values), [values]);

  const mutate = async (
    valueId: number | undefined,
    action: "accept" | "reject" | "supersede",
  ) => {
    if (!valueId || pendingId != null) return;

    setPendingId(valueId);
    setError(null);

    try {
      await api.post(
        `/acquisition/properties/${propertyId}/field-values/${valueId}/${action}`,
        {},
      );
      onChanged?.();
    } catch (e: any) {
      setError(formatError(e));
    } finally {
      setPendingId(null);
    }
  };

  const groups = Object.entries(grouped)
    .map(([field, rows]) => [field, sortRows(rows)] as const)
    .sort((a, b) => a[0].localeCompare(b[0]));

  const conflictCount = groups.filter(
    ([, rows]) => new Set(rows.map(normalizedDisplayValue)).size > 1,
  ).length;

  const suggestedCount = values.filter(
    (row) =>
      String(row.review_state || "suggested").toLowerCase() === "suggested",
  ).length;

  const acceptedCount = values.filter(
    (row) => String(row.review_state || "").toLowerCase() === "accepted",
  ).length;

  return (
    <div className="rounded-3xl border border-app bg-app-panel p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Parsed field review
          </div>
          <div className="mt-1 text-sm text-app-3">
            Review extracted values, resolve document disagreements, and promote
            canonical values before trusting close readiness.
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <span
            className={
              conflictCount ? "oh-pill oh-pill-warn" : "oh-pill oh-pill-good"
            }
          >
            <GitCompareArrows className="mr-1 h-3.5 w-3.5" />
            {conflictCount} disagreement group{conflictCount === 1 ? "" : "s"}
          </span>
          <span className="oh-pill">{acceptedCount} accepted</span>
          <span className="oh-pill">{suggestedCount} pending</span>
        </div>
      </div>

      {error ? <div className="mt-3 text-xs text-red-300">{error}</div> : null}

      {!groups.length ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-4">
          No parser suggestions available yet.
        </div>
      ) : (
        <div className="mt-4 space-y-4">
          {groups.map(([field, rows]) => {
            const disagreement =
              new Set(rows.map(normalizedDisplayValue)).size > 1;

            const acceptedRow = rows.find(
              (row) =>
                String(row.review_state || "").toLowerCase() === "accepted",
            );

            return (
              <div
                key={field}
                className={`rounded-2xl border p-4 ${
                  disagreement
                    ? "border-amber-500/25 bg-amber-500/5"
                    : "border-app bg-app-muted"
                }`}
              >
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold text-app-0">
                      {field.replace(/_/g, " ")}
                    </div>
                    <div className="mt-1 text-xs text-app-4">
                      {rows.length} extracted value
                      {rows.length === 1 ? "" : "s"}
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-2">
                    {disagreement ? (
                      <span className="oh-pill oh-pill-warn">disagreement</span>
                    ) : (
                      <span className="oh-pill">single value</span>
                    )}
                    {acceptedRow ? (
                      <span className="oh-pill oh-pill-good">
                        canonical chosen
                      </span>
                    ) : null}
                  </div>
                </div>

                <div className="mt-3 space-y-3">
                  {rows.map((row, idx) => {
                    const state = String(
                      row.review_state || "suggested",
                    ).toLowerCase();
                    const isPending = pendingId === row.id;

                    return (
                      <div
                        key={row.id || `${field}-${idx}`}
                        className="rounded-2xl border border-app bg-app-panel p-4"
                      >
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <div className="text-sm font-medium text-app-0">
                              {displayValue(row)}
                            </div>
                            <div className="mt-1 text-xs text-app-4">
                              confidence {confidenceLabel(row.confidence)}
                              {row.extraction_version
                                ? ` · ${row.extraction_version}`
                                : ""}
                              {row.source_document_name
                                ? ` · ${row.source_document_name}`
                                : row.source_document_id
                                  ? ` · doc #${row.source_document_id}`
                                  : ""}
                            </div>
                          </div>

                          <div className="flex flex-wrap gap-2">
                            <span className={stateClass(state)}>{state}</span>
                            {row.manually_overridden ? (
                              <span className="oh-pill oh-pill-accent">
                                manual override
                              </span>
                            ) : null}
                          </div>
                        </div>

                        <div className="mt-3 flex flex-wrap gap-2">
                          <button
                            type="button"
                            disabled={isPending || pendingId != null || !row.id}
                            onClick={() => mutate(row.id, "accept")}
                            className="oh-btn oh-btn-secondary"
                          >
                            {isPending ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                              <CheckCircle2 className="h-4 w-4" />
                            )}
                            Accept
                          </button>

                          <button
                            type="button"
                            disabled={isPending || pendingId != null || !row.id}
                            onClick={() => mutate(row.id, "reject")}
                            className="oh-btn oh-btn-secondary"
                          >
                            <XCircle className="h-4 w-4" />
                            Reject
                          </button>

                          <button
                            type="button"
                            disabled={isPending || pendingId != null || !row.id}
                            onClick={() => mutate(row.id, "supersede")}
                            className="oh-btn oh-btn-secondary"
                          >
                            <RotateCcw className="h-4 w-4" />
                            Supersede
                          </button>
                        </div>
                      </div>
                    );
                  })}
                </div>

                {disagreement ? (
                  <div className="mt-3 inline-flex items-center gap-2 text-xs text-amber-200">
                    <AlertTriangle className="h-3.5 w-3.5" />
                    Multiple documents disagree on this field. Resolve this
                    before trusting estimated close readiness.
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
