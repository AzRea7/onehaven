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

type PropertyLike = {
  id?: number | null;
  address?: string | null;
  city?: string | null;
  state?: string | null;
};

type InspectionSchedulerModalProps = {
  open: boolean;
  onClose: () => void;
  inspectionId?: number | null;
  property?: PropertyLike | null;
  propertyLabel?: string;
  existing?: any;
  onSaved?: (payload?: any) => void | Promise<void>;
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

function buildPropertyLabel(
  property?: PropertyLike | null,
  propertyLabel?: string,
) {
  if (propertyLabel) return propertyLabel;
  if (!property) return "Selected property";
  return (
    [property.address, property.city, property.state]
      .filter(Boolean)
      .join(" · ") || "Selected property"
  );
}

export default function InspectionSchedulerModal({
  open,
  onClose,
  inspectionId,
  property,
  propertyLabel,
  existing,
  onSaved,
}: InspectionSchedulerModalProps) {
  const [resolvedInspectionId, setResolvedInspectionId] = React.useState<
    number | null
  >(inspectionId ?? null);
  const [resolvedExisting, setResolvedExisting] = React.useState<any>(
    existing || null,
  );
  const [scheduledFor, setScheduledFor] = React.useState("");
  const [inspectorName, setInspectorName] = React.useState("");
  const [inspectorCompany, setInspectorCompany] = React.useState("");
  const [inspectorEmail, setInspectorEmail] = React.useState("");
  const [inspectorPhone, setInspectorPhone] = React.useState("");
  const [appointmentNotes, setAppointmentNotes] = React.useState("");
  const [status, setStatus] = React.useState("scheduled");
  const [calendarProvider, setCalendarProvider] = React.useState("ics");
  const [reminderOffsets, setReminderOffsets] = React.useState<number[]>([
    1440, 120,
  ]);
  const [loadingContext, setLoadingContext] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (!open) return;

    let cancelled = false;

    async function loadContext() {
      setLoadingContext(true);
      setError(null);

      try {
        if (inspectionId) {
          if (!cancelled) {
            setResolvedInspectionId(inspectionId);
            setResolvedExisting(existing || null);
          }
          return;
        }

        if (!property?.id) {
          if (!cancelled) {
            setResolvedInspectionId(null);
            setResolvedExisting(existing || null);
          }
          return;
        }

        const summary = await api.get(
          `/inspections/property/${property.id}/schedule-summary`,
        );
        const appointment =
          summary?.appointment ||
          summary?.latest_appointment ||
          summary?.next_appointment ||
          null;

        if (!cancelled) {
          setResolvedInspectionId(
            appointment?.inspection_id != null
              ? Number(appointment.inspection_id)
              : null,
          );
          setResolvedExisting(existing || appointment || null);
        }
      } catch (e: any) {
        if (!cancelled) {
          setResolvedInspectionId(inspectionId ?? null);
          setResolvedExisting(existing || null);
          setError(String(e?.message || e));
        }
      } finally {
        if (!cancelled) setLoadingContext(false);
      }
    }

    void loadContext();

    return () => {
      cancelled = true;
    };
  }, [open, inspectionId, property?.id, existing]);

  React.useEffect(() => {
    if (!open) return;
    const source = resolvedExisting || existing || null;
    setScheduledFor(normalizeDateTimeInput(source?.scheduled_for));
    setInspectorName(source?.inspector_name || source?.inspector || "");
    setInspectorCompany(source?.inspector_company || "");
    setInspectorEmail(source?.inspector_email || "");
    setInspectorPhone(source?.inspector_phone || "");
    setAppointmentNotes(source?.appointment_notes || source?.note || "");
    setStatus(source?.status || "scheduled");
    setCalendarProvider(source?.calendar_provider || "ics");
    setReminderOffsets(normalizeReminderOffsets(source?.reminder_offsets));
    setError(null);
  }, [resolvedExisting, existing, open]);

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
    if (!resolvedInspectionId) {
      setError(
        "No inspection is available yet for this property. Schedule or create an inspection record first.",
      );
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
      const result = await api.post(
        `/inspections/${resolvedInspectionId}/appointment`,
        payload,
      );
      await onSaved?.(result);
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
              {buildPropertyLabel(property, propertyLabel)}
            </div>
            {resolvedInspectionId ? (
              <div className="mt-1 text-xs text-app-4">
                Inspection #{resolvedInspectionId}
              </div>
            ) : null}
          </div>
          <button
            type="button"
            onClick={onClose}
            className="oh-btn oh-btn-secondary"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="grid gap-4 px-6 py-5">
          {loadingContext ? (
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-3">
              Loading inspection scheduling context...
            </div>
          ) : null}

          {error ? (
            <div className="rounded-2xl border border-red-500/20 bg-red-500/[0.06] px-4 py-3 text-sm text-red-200">
              {error}
            </div>
          ) : null}

          <div className="grid gap-4 md:grid-cols-2">
            <label className="grid gap-2">
              <span className="text-sm font-medium text-app-2">
                Scheduled for
              </span>
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
              <span className="text-sm font-medium text-app-2">
                Inspector name
              </span>
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
              <span className="text-sm font-medium text-app-2">
                Inspector company
              </span>
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
              <span className="text-sm font-medium text-app-2">
                Inspector email
              </span>
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
              <span className="text-sm font-medium text-app-2">
                Inspector phone
              </span>
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
            <span className="text-sm font-medium text-app-2">
              Calendar provider
            </span>
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
            <span className="text-sm font-medium text-app-2">
              Reminder offsets
            </span>
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
            <span className="text-sm font-medium text-app-2">
              Appointment notes
            </span>
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
          <button
            type="button"
            onClick={onClose}
            className="oh-btn oh-btn-secondary"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={() => void handleSave()}
            disabled={saving || loadingContext}
            className="oh-btn"
          >
            {saving ? "Saving..." : "Save inspection"}
          </button>
        </div>
      </div>
    </div>
  );
}
