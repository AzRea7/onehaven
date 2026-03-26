import React from "react";
import { AlertTriangle, CalendarClock, CheckCircle2, Lock } from "lucide-react";

export type AcquisitionDeadline = {
  id?: number;
  kind?: string;
  label?: string;
  due_at?: string | null;
  status?: string | null;
  waiting_on?: string | null;
  notes?: string | null;
  days_remaining?: number | null;
};

type Props = {
  deadlines?: AcquisitionDeadline[];
};

function labelFor(kind?: string, label?: string) {
  if (label) return label;
  const raw = String(kind || "deadline").replace(/_/g, " ");
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

function computeDays(raw?: string | null) {
  if (!raw) return null;
  const now = new Date();
  const due = new Date(raw);
  if (Number.isNaN(due.getTime())) return null;
  return Math.ceil((due.getTime() - now.getTime()) / 86400000);
}

function tone(
  days: number | null,
  status?: string | null,
  waitingOn?: string | null,
) {
  const normalized = String(status || "").toLowerCase();
  const waiting = String(waitingOn || "").toLowerCase();

  if (
    normalized.includes("blocked") ||
    normalized.includes("hold") ||
    waiting.includes("blocked")
  ) {
    return "blocked";
  }

  if (normalized === "complete" || normalized === "completed") return "good";
  if (days != null && days < 0) return "bad";
  if (days != null && days <= 3) return "warn";
  return "neutral";
}

function pillClass(kind: string) {
  if (kind === "good") return "oh-pill oh-pill-good";
  if (kind === "warn") return "oh-pill oh-pill-warn";
  if (kind === "bad") return "oh-pill oh-pill-bad";
  if (kind === "blocked") return "oh-pill oh-pill-bad";
  return "oh-pill";
}

function toneLabel(kind: string) {
  if (kind === "blocked") return "Blocked";
  if (kind === "bad") return "Overdue";
  if (kind === "warn") return "Due soon";
  if (kind === "good") return "Complete";
  return "On track";
}

function itemBorderClass(kind: string) {
  if (kind === "blocked") return "border-red-500/30 bg-red-500/5";
  if (kind === "bad") return "border-red-500/20 bg-app-muted";
  if (kind === "warn") return "border-amber-500/20 bg-app-muted";
  if (kind === "good") return "border-emerald-500/20 bg-app-muted";
  return "border-app bg-app-muted";
}

function formatDueAt(raw?: string | null) {
  if (!raw) return "No due date";
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return raw;
  return d.toLocaleString();
}

export default function AcquisitionDeadlinePanel({ deadlines = [] }: Props) {
  const normalized = deadlines
    .map((item) => {
      const days = item.days_remaining ?? computeDays(item.due_at);
      const itemTone = tone(days, item.status, item.waiting_on);
      return { ...item, days_remaining: days, tone: itemTone };
    })
    .sort((a, b) => {
      const toneRank = (toneValue: string) => {
        if (toneValue === "blocked") return 0;
        if (toneValue === "bad") return 1;
        if (toneValue === "warn") return 2;
        if (toneValue === "neutral") return 3;
        if (toneValue === "good") return 4;
        return 5;
      };

      const rankDiff = toneRank(a.tone) - toneRank(b.tone);
      if (rankDiff !== 0) return rankDiff;

      return (a.days_remaining ?? 9999) - (b.days_remaining ?? 9999);
    });

  const blockedCount = normalized.filter((d) => d.tone === "blocked").length;
  const overdueCount = normalized.filter((d) => d.tone === "bad").length;
  const dueSoonCount = normalized.filter((d) => d.tone === "warn").length;

  return (
    <div className="rounded-3xl border border-app bg-app-panel p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Deadline intelligence
          </div>
          <div className="mt-1 text-sm text-app-3">
            Surface what is overdue, due soon, or blocked before close.
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <span className={blockedCount ? "oh-pill oh-pill-bad" : "oh-pill"}>
            blocked {blockedCount}
          </span>
          <span className={overdueCount ? "oh-pill oh-pill-bad" : "oh-pill"}>
            overdue {overdueCount}
          </span>
          <span className={dueSoonCount ? "oh-pill oh-pill-warn" : "oh-pill"}>
            due soon {dueSoonCount}
          </span>
        </div>
      </div>

      {!normalized.length ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-4">
          No acquisition deadlines tracked yet.
        </div>
      ) : (
        <div className="mt-4 space-y-3">
          {normalized.map((deadline, idx) => {
            const urgency = deadline.tone;

            return (
              <div
                key={deadline.id || `${deadline.kind}-${idx}`}
                className={`rounded-2xl border p-4 ${itemBorderClass(urgency)}`}
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold text-app-0">
                      {labelFor(deadline.kind, deadline.label)}
                    </div>
                    <div className="mt-1 text-xs text-app-4">
                      {formatDueAt(deadline.due_at)}
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-2">
                    <span className={pillClass(urgency)}>
                      {toneLabel(urgency)}
                    </span>
                    {deadline.waiting_on ? (
                      <span className="oh-pill">
                        waiting on {deadline.waiting_on}
                      </span>
                    ) : null}
                  </div>
                </div>

                <div className="mt-3 flex flex-wrap gap-3 text-sm text-app-2">
                  <span className="inline-flex items-center gap-2">
                    {urgency === "good" ? (
                      <CheckCircle2 className="h-4 w-4" />
                    ) : urgency === "blocked" ? (
                      <Lock className="h-4 w-4" />
                    ) : urgency === "bad" ? (
                      <AlertTriangle className="h-4 w-4" />
                    ) : (
                      <CalendarClock className="h-4 w-4" />
                    )}

                    {deadline.days_remaining == null
                      ? "No countdown"
                      : deadline.days_remaining < 0
                        ? `${Math.abs(deadline.days_remaining)} day(s) late`
                        : `${deadline.days_remaining} day(s) remaining`}
                  </span>

                  {deadline.notes ? <span>{deadline.notes}</span> : null}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
