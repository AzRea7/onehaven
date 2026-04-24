import React from "react";
import { AlertTriangle, CalendarClock, Clock3, Flag } from "lucide-react";
import EmptyState from "packages/ui/onehaven_onehaven_platform/frontend/src/components/EmptyState";
import Surface from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Surface";

export type AcquisitionDeadline = {
  id?: number | string;
  kind?: string | null;
  label?: string | null;
  due_at?: string | null;
  status?: string | null;
  waiting_on?: string | null;
  notes?: string | null;
};

type Props = {
  deadlines?: AcquisitionDeadline[];
  waitingOn?: string | null;
};

function fmtDate(value?: string | null) {
  if (!value) return "—";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return "—";
  return dt.toLocaleDateString();
}

function fmtDateTime(value?: string | null) {
  if (!value) return "—";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return "—";
  return dt.toLocaleString();
}

function dueTone(value?: string | null) {
  if (!value) return "oh-pill";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return "oh-pill";

  const now = Date.now();
  const diffMs = dt.getTime() - now;
  const diffDays = diffMs / (1000 * 60 * 60 * 24);

  if (diffDays < 0) return "oh-pill oh-pill-bad";
  if (diffDays <= 7) return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-good";
}

function statusTone(value?: string | null) {
  const v = String(value || "")
    .trim()
    .toLowerCase();

  if (v === "done" || v === "completed") return "oh-pill oh-pill-good";
  if (v === "blocked" || v === "failed" || v === "overdue") {
    return "oh-pill oh-pill-bad";
  }
  if (v === "active" || v === "pending" || v === "due_soon") {
    return "oh-pill oh-pill-warn";
  }
  return "oh-pill";
}

function labelText(row: AcquisitionDeadline) {
  return row.label || row.kind || "Deadline";
}

export default function AcquisitionDeadlinePanel({
  deadlines = [],
  waitingOn,
}: Props) {
  const rows = Array.isArray(deadlines) ? deadlines : [];

  return (
    <Surface
      title="Deadlines"
      subtitle="Critical dates and upcoming closing pressure."
      actions={
        waitingOn ? (
          <span className="oh-pill oh-pill-warn">
            <Flag className="h-3.5 w-3.5" />
            Waiting on {waitingOn}
          </span>
        ) : undefined
      }
    >
      {!rows.length ? (
        <EmptyState
          icon={CalendarClock}
          title="No deadlines yet"
          description="Add acquisition milestones or parsed contract dates to make closing pressure visible here."
        />
      ) : (
        <div className="space-y-3">
          {rows.map((row, idx) => {
            const key = String(row.id ?? `${row.kind || "deadline"}-${idx}`);
            return (
              <div
                key={key}
                className="rounded-2xl border border-app bg-app-panel px-4 py-4"
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                      <Clock3 className="h-4 w-4 text-app-4" />
                      {labelText(row)}
                    </div>

                    <div className="mt-2 text-sm text-app-3">
                      Due: {fmtDate(row.due_at)}
                    </div>

                    <div className="mt-1 text-xs text-app-4">
                      {fmtDateTime(row.due_at)}
                    </div>

                    {row.notes ? (
                      <div className="mt-2 text-sm text-app-3">{row.notes}</div>
                    ) : null}
                  </div>

                  <div className="flex flex-wrap gap-2">
                    <span className={dueTone(row.due_at)}>
                      {row.due_at ? "timed" : "unscheduled"}
                    </span>
                    <span className={statusTone(row.status)}>
                      {row.status || "active"}
                    </span>
                    {row.waiting_on ? (
                      <span className="oh-pill">
                        <AlertTriangle className="h-3.5 w-3.5" />
                        {row.waiting_on}
                      </span>
                    ) : null}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </Surface>
  );
}
