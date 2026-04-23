import React from "react";
import { BellRing, Clock3, Send } from "lucide-react";
import { api } from "@/lib/api";
import Surface from "@/components/Surface";

type ReminderRow = {
  inspection_id?: number;
  property_id?: number;
  scheduled_for?: string;
  inspector_name?: string;
  inspector_email?: string;
  status?: string;
  next_offset_minutes?: number;
  due_offset_minutes?: number;
  reminder_offset_minutes?: number;
};

type ComplianceReminderPanelProps = {
  propertyId?: number | null;
  property?: {
    id?: number;
    address?: string;
  } | null;
  rows?: ReminderRow[];
  loading?: boolean;
  onSent?: () => void;
};

function formatOffset(minutes?: number | null) {
  const value = Number(minutes || 0);
  if (!value) return "Now";
  if (value % 1440 === 0)
    return `${value / 1440} day${value / 1440 === 1 ? "" : "s"} before`;
  if (value % 60 === 0)
    return `${value / 60} hour${value / 60 === 1 ? "" : "s"} before`;
  return `${value} minutes before`;
}

export default function ComplianceReminderPanel({
  propertyId,
  property,
  rows = [],
  loading = false,
  onSent,
}: ComplianceReminderPanelProps) {
  const effectivePropertyId = propertyId ?? property?.id ?? null;

  const filteredRows = React.useMemo(() => {
    if (!effectivePropertyId) return rows;
    return rows.filter(
      (row) => !row.property_id || row.property_id === effectivePropertyId,
    );
  }, [effectivePropertyId, rows]);

  const sendReminder = async (row: ReminderRow) => {
    if (!row.inspection_id) return;
    try {
      const reminderOffsetMinutes =
        row.reminder_offset_minutes ||
        row.due_offset_minutes ||
        row.next_offset_minutes ||
        0;
      await api.post(`/inspections/${row.inspection_id}/remind`, {
        reminder_offset_minutes: reminderOffsetMinutes,
      });
      onSent?.();
    } catch {
      // surface stays quiet here to avoid breaking panel flow
    }
  };

  return (
    <Surface
      title="Reminder panel"
      subtitle={
        property?.address
          ? `Upcoming inspection reminders for ${property.address}.`
          : "Upcoming inspection reminders available for manual trigger or automation preview."
      }
    >
      {loading ? (
        <div className="grid gap-3">
          <div className="oh-skeleton h-[86px] rounded-2xl" />
          <div className="oh-skeleton h-[86px] rounded-2xl" />
        </div>
      ) : filteredRows.length === 0 ? (
        <div className="text-sm text-app-4">
          No due reminders are queued for this property right now.
        </div>
      ) : (
        <div className="grid gap-3">
          {filteredRows.map((row, index) => (
            <div
              key={`${row.inspection_id || "reminder"}-${row.next_offset_minutes || row.due_offset_minutes || index}`}
              className="rounded-2xl border border-app bg-app-muted px-4 py-4"
            >
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <BellRing className="h-4 w-4 text-app-4" />
                    <div className="text-sm font-semibold text-app-0">
                      Inspection #{row.inspection_id || "—"}
                    </div>
                    {row.status ? (
                      <span className="oh-pill">{row.status}</span>
                    ) : null}
                  </div>

                  <div className="mt-2 grid gap-2 text-sm text-app-3 md:grid-cols-2">
                    <div className="inline-flex items-center gap-2">
                      <Clock3 className="h-4 w-4 text-app-4" />
                      {formatOffset(
                        row.next_offset_minutes ||
                          row.due_offset_minutes ||
                          row.reminder_offset_minutes,
                      )}
                    </div>
                    <div>{row.scheduled_for || "No scheduled time"}</div>
                  </div>

                  {row.inspector_name || row.inspector_email ? (
                    <div className="mt-2 text-sm text-app-3">
                      {row.inspector_name || "Inspector"}
                      {row.inspector_email ? ` · ${row.inspector_email}` : ""}
                    </div>
                  ) : null}
                </div>

                <button
                  type="button"
                  onClick={() => void sendReminder(row)}
                  disabled={!row.inspection_id}
                  className="oh-btn oh-btn-secondary"
                >
                  <Send className="h-4 w-4" />
                  Send reminder
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </Surface>
  );
}
