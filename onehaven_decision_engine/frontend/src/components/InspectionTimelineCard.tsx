import React from "react";
import {
  CalendarCheck2,
  CalendarClock,
  CircleDashed,
  Clock3,
  XCircle,
} from "lucide-react";
import Surface from "./Surface";

type TimelineRow = {
  inspection_id?: number;
  event_type?: string;
  created_at?: string;
  scheduled_for?: string;
  status?: string;
  inspector_name?: string;
  inspector_company?: string;
  note?: string;
  payload?: Record<string, any>;
};

type InspectionTimelineCardProps = {
  rows?: TimelineRow[];
  loading?: boolean;
  title?: string;
  appointment?: any;
  property?: {
    id?: number;
    address?: string;
    city?: string;
    state?: string;
  } | null;
};

function labelize(value?: string | null) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function statusTone(value?: string | null) {
  const normalized = String(value || "").toLowerCase();
  if (["passed", "completed", "confirmed"].includes(normalized)) {
    return "oh-pill oh-pill-good";
  }
  if (["failed", "canceled", "cancelled", "blocked"].includes(normalized)) {
    return "oh-pill oh-pill-bad";
  }
  if (["draft", "scheduled", "pending", "queued"].includes(normalized)) {
    return "oh-pill oh-pill-warn";
  }
  return "oh-pill";
}

function iconForEvent(eventType?: string, status?: string) {
  const value = String(eventType || status || "").toLowerCase();
  if (value.includes("cancel")) return XCircle;
  if (value.includes("complete") || value.includes("pass"))
    return CalendarCheck2;
  if (value.includes("schedule") || value.includes("confirm"))
    return CalendarClock;
  return CircleDashed;
}

function normalizeRows(
  rows: TimelineRow[] = [],
  appointment?: any,
): TimelineRow[] {
  if (rows.length > 0) return rows;
  if (!appointment) return [];
  return [
    {
      inspection_id: appointment?.inspection_id,
      event_type: "appointment",
      scheduled_for: appointment?.scheduled_for || appointment?.inspection_date,
      status: appointment?.status || "scheduled",
      inspector_name: appointment?.inspector_name || appointment?.inspector,
      inspector_company: appointment?.inspector_company,
      note: appointment?.appointment_notes || appointment?.note,
      payload: appointment,
    },
  ];
}

export default function InspectionTimelineCard({
  rows = [],
  loading = false,
  title = "Inspection timeline",
  appointment,
  property,
}: InspectionTimelineCardProps) {
  const normalizedRows = normalizeRows(rows, appointment);

  return (
    <Surface
      title={title}
      subtitle={
        property?.address
          ? `Appointment lifecycle, reminders, and completion events for ${property.address}.`
          : "Appointment lifecycle, reminders, and completion events for the selected property."
      }
    >
      {loading ? (
        <div className="grid gap-3">
          <div className="oh-skeleton h-[88px] rounded-2xl" />
          <div className="oh-skeleton h-[88px] rounded-2xl" />
        </div>
      ) : normalizedRows.length === 0 ? (
        <div className="text-sm text-app-4">
          No inspection timeline events exist for this property yet.
        </div>
      ) : (
        <div className="grid gap-3">
          {normalizedRows.map((row, index) => {
            const Icon = iconForEvent(row.event_type, row.status);
            const when =
              row.scheduled_for ||
              row.created_at ||
              row.payload?.scheduled_for ||
              row.payload?.inspection_date ||
              null;
            const inspector =
              row.inspector_name ||
              row.payload?.inspector_name ||
              row.payload?.inspector ||
              null;

            return (
              <div
                key={`${row.inspection_id || "inspection"}-${row.event_type || row.status || index}-${index}`}
                className="rounded-2xl border border-app bg-app-muted px-4 py-4"
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <Icon className="h-4 w-4 text-app-4" />
                      <div className="text-sm font-semibold text-app-0">
                        {labelize(
                          row.event_type || row.status || "timeline_event",
                        )}
                      </div>
                      {row.status ? (
                        <span className={statusTone(row.status)}>
                          {labelize(row.status)}
                        </span>
                      ) : null}
                    </div>

                    <div className="mt-2 grid gap-2 text-sm text-app-3 md:grid-cols-2">
                      <div className="inline-flex items-center gap-2">
                        <Clock3 className="h-4 w-4 text-app-4" />
                        {when || "No timestamp"}
                      </div>
                      <div className="inline-flex items-center gap-2">
                        <CalendarClock className="h-4 w-4 text-app-4" />
                        Inspection #
                        {row.inspection_id || row.payload?.inspection_id || "—"}
                      </div>
                    </div>

                    {inspector ? (
                      <div className="mt-2 text-sm text-app-3">
                        Inspector: {inspector}
                        {row.inspector_company || row.payload?.inspector_company
                          ? ` · ${row.inspector_company || row.payload?.inspector_company}`
                          : ""}
                      </div>
                    ) : null}

                    {row.note ||
                    row.payload?.note ||
                    row.payload?.reason ||
                    row.payload?.appointment_notes ? (
                      <div className="mt-2 text-sm leading-6 text-app-3">
                        {row.note ||
                          row.payload?.note ||
                          row.payload?.reason ||
                          row.payload?.appointment_notes}
                      </div>
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
