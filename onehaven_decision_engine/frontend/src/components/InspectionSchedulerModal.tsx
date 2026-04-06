import React from "react";
import { CalendarClock, Mail, Phone, UserRound, X } from "lucide-react";
import { api } from "../lib/api";
import AppSelect from "./AppSelect";

type ReminderOption = {
  value: number;
  label: string;
};

const REMINDER_OPTIONS: ReminderOption[] = [
  { value: 30, label: "30 minutes before" },
  { value: 60, label: "1 hour before" },
  { value: 120, label: "2 hours before" },
  { value: 1440, label: "1 day before" },
  { value: 2880, label: "2 days before" },
];

type SchedulerPayload = {
  scheduled_for: string;
  inspector_name?: string;
  inspector_company?: string;
  inspector_email?: string;
  inspector_phone?: string;
  reminder_offsets: number[];
  appointment_notes?: string;
  status?: string;
  calendar_provider?: string;
};

type InspectionSchedulerModalProps = {
  open: boolean;
  onClose: () => void;
  inspectionId: number | null;
  propertyLabel?: string;
  existing?: any;
  onSaved?: (payload: any) => void;
};

function normalizeDateTimeInput(value?: string | null) {
  if (!value) return "";
  const raw = String(value);
  if (raw.length >= 16) return raw.slice(0, 16);
  return raw;
}

function normalizeReminderOffsets(value: any): number[] {
  if (Array.isArray(value)) {
    return value.map((v) => Number(v)).filter((v) => Number.isFinite(v));
  }
  return [1440, 120];
}

export default function InspectionSchedulerModal({
  open,
  onClose,
  inspectionId,
  propertyLabel,
  existing,
  onSaved,
}: InspectionSchedulerModalProps) {
  const [scheduledFor, setScheduledFor] = React.useState("");
  const [inspectorName, setInspectorName] = React.useState("");
  const [inspectorCompany, setInspectorCompany] = React.useState("");
  const [inspectorEmail, setInspectorEmail] = React.useState("");
  const [inspectorPhone, setInspectorPhone] = React.useState("");
  const [appointmentNotes, setAppointmentNotes] = React.useState("");
  const [status, setStatus] = React.useState("scheduled");
  const [calendarProvider, setCalendarProvider] = React.useState("ics");
  const [reminderOffsets, setReminderOffsets] = React.useState<number[]>([1440, 120]);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (!open) return;
    setScheduledFor(normalizeDateTimeInput(existing?.scheduled_for));
    setInspectorName(existing?.inspector_name || "");
    setInspectorCompany(existing?.inspector_company || "");
    setInspectorEmail(existing?.inspector_email || "");
    setInspectorPhone(existing?.inspector_phone || "");
    setAppointmentNotes(existing?.appointment_notes || "");
    setStatus(existing?.status || "scheduled");
    setCalendarProvider(existing?.calendar_provider || "ics");
    setReminderOffsets(normalizeReminderOffsets(existing?.reminder_offsets));
    setError(null);
  }, [existing, open]);

  const toggleReminder = (minutes: number) => {
    setReminderOffsets((current) => {
      const exists = current.includes(minutes);
      const next = exists
        ? current.filter((value) => value !== minutes)
        : [...current, minutes];
      return [...next].sort((a, b) => b - a);
    });
  };

  const handleSave = async () => {
    if (!inspectionId) {
      setError("Missing inspection id.");
      return;
    }
    if (!scheduledFor) {
      setError("Scheduled date and time are required.");
      return;
    }

    const payload: SchedulerPayload = {
      scheduled_for: new Date(scheduledFor).toISOString(),
      inspector_name: inspectorName || undefined,
      inspector_company: inspectorCompany || undefined,
      inspector_email: inspectorEmail || undefined,
      inspector_phone: inspectorPhone || undefined,
      reminder_offsets: reminderOffsets,
      appointment_notes: appointmentNotes || undefined,
      status,
      calendar_provider: calendarProvider,
    };

    try {
      setSaving(true);
      setError(null);
      const result = await api.post(`/inspections/${inspectionId}/appointment`, payload);
      onSaved?.(result);
      onClose();
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  };

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[1200] flex items-center justify-center bg-black/50 px-4">
      <div className="w-full max-w-2xl rounded-3xl border border-app bg-app-panel shadow-2xl">
        <div className="flex items-start justify-between gap-4 border-b border-app px-6 py-5">
          <div>
            <div className="text-lg font-semibold text-app-0">
              Schedule inspection
            </div>
            <div className="mt-1 text-sm text-app-4">
              {propertyLabel || "Selected property"}
            </div>
          </div>
          <button type="button" onClick={onClose} className="oh-btn oh-btn-secondary">
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="grid gap-4 px-6 py-5">
          {error ? (
            <div className="rounded-2xl border border-red-500/20 bg-red-500/[0.06] px-4 py-3 text-sm text-red-200">
              {error}
            </div>
          ) : null}

          <div className="grid gap-4 md:grid-cols-2">
            <label className="grid gap-2">
              <span className="text-sm font-medium text-app-2">Scheduled for</span>
              <div className="relative">
                <CalendarClock className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
                <input
                  type="datetime-local"
                  value={scheduledFor}
                  onChange={(e) => setScheduledFor(e.target.value)}
                  className="w-full rounded-2xl border border-app bg-app-muted px-10 py-3 text-sm text-app-0 outline-none"
                />
              </div>
            </label>

            <label className="grid gap-2">
              <span className="text-sm font-medium text-app-2">Status</span>
              <AppSelect
                value={status}
                onChange={setStatus}
                options={[
                  { value: "draft", label: "Draft" },
                  { value: "scheduled", label: "Scheduled" },
                  { value: "confirmed", label: "Confirmed" },
                ]}
              />
            </label>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <label className="grid gap-2">
              <span className="text-sm font-medium text-app-2">Inspector name</span>
              <div className="relative">
                <UserRound className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
                <input
                  type="text"
                  value={inspectorName}
                  onChange={(e) => setInspectorName(e.target.value)}
                  placeholder="Jane Inspector"
                  className="w-full rounded-2xl border border-app bg-app-muted px-10 py-3 text-sm text-app-0 outline-none"
                />
              </div>
            </label>

            <label className="grid gap-2">
              <span className="text-sm font-medium text-app-2">Inspector company</span>
              <input
                type="text"
                value={inspectorCompany}
                onChange={(e) => setInspectorCompany(e.target.value)}
                placeholder="Metro Inspections"
                className="w-full rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-0 outline-none"
              />
            </label>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <label className="grid gap-2">
              <span className="text-sm font-medium text-app-2">Inspector email</span>
              <div className="relative">
                <Mail className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
                <input
                  type="email"
                  value={inspectorEmail}
                  onChange={(e) => setInspectorEmail(e.target.value)}
                  placeholder="jane@example.com"
                  className="w-full rounded-2xl border border-app bg-app-muted px-10 py-3 text-sm text-app-0 outline-none"
                />
              </div>
            </label>

            <label className="grid gap-2">
              <span className="text-sm font-medium text-app-2">Inspector phone</span>
              <div className="relative">
                <Phone className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
                <input
                  type="text"
                  value={inspectorPhone}
                  onChange={(e) => setInspectorPhone(e.target.value)}
                  placeholder="555-222-1111"
                  className="w-full rounded-2xl border border-app bg-app-muted px-10 py-3 text-sm text-app-0 outline-none"
                />
              </div>
            </label>
          </div>

          <div className="grid gap-2">
            <span className="text-sm font-medium text-app-2">Calendar provider</span>
            <AppSelect
              value={calendarProvider}
              onChange={setCalendarProvider}
              options={[
                { value: "ics", label: "ICS export" },
                { value: "google", label: "Google Calendar (adapter-ready)" },
              ]}
            />
          </div>

          <div className="grid gap-2">
            <span className="text-sm font-medium text-app-2">Reminder offsets</span>
            <div className="flex flex-wrap gap-2">
              {REMINDER_OPTIONS.map((item) => {
                const active = reminderOffsets.includes(item.value);
                return (
                  <button
                    key={item.value}
                    type="button"
                    onClick={() => toggleReminder(item.value)}
                    className={[
                      "rounded-full border px-3 py-2 text-sm transition",
                      active
                        ? "border-app-strong bg-app-muted text-app-0"
                        : "border-app bg-app-panel text-app-3 hover:border-app-strong",
                    ].join(" ")}
                  >
                    {item.label}
                  </button>
                );
              })}
            </div>
          </div>

          <label className="grid gap-2">
            <span className="text-sm font-medium text-app-2">Appointment notes</span>
            <textarea
              rows={4}
              value={appointmentNotes}
              onChange={(e) => setAppointmentNotes(e.target.value)}
              placeholder="Access instructions, tenant notes, lockbox code, etc."
              className="w-full rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-0 outline-none"
            />
          </label>
        </div>

        <div className="flex items-center justify-end gap-3 border-t border-app px-6 py-5">
          <button type="button" onClick={onClose} className="oh-btn oh-btn-secondary">
            Cancel
          </button>
          <button type="button" onClick={handleSave} disabled={saving} className="oh-btn">
            {saving ? "Saving..." : "Save inspection"}
          </button>
        </div>
      </div>
    </div>
  );
}
