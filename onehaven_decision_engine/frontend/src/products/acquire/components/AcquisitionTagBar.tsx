import React from "react";
import {
  Bookmark,
  BriefcaseBusiness,
  Clock3,
  FileX,
  Flag,
  Save,
  Loader2,
  AlertTriangle,
} from "lucide-react";
import { api } from "@/lib/api";

const TAGS = [
  { key: "saved", label: "Saved", icon: Save },
  { key: "shortlisted", label: "Shortlisted", icon: Bookmark },
  { key: "review_later", label: "Review later", icon: Clock3 },
  { key: "offer_candidate", label: "Offer candidate", icon: BriefcaseBusiness },
  { key: "rejected", label: "Rejected", icon: FileX },
] as const;

type Props = {
  propertyId: number;
  value?: string[];
  onChange?: (next: string[]) => void | Promise<void>;
  compact?: boolean;
};

function normalizeTags(value?: string[]) {
  if (!Array.isArray(value)) return [];
  return Array.from(new Set(value.filter(Boolean).map(String)));
}

function formatError(e: any) {
  const status = e?.response?.status;
  const detail =
    e?.response?.data?.detail || e?.response?.data?.message || e?.message;
  if (status && detail) return `(${status}) ${String(detail)}`;
  if (detail) return String(detail);
  return "Failed to save acquisition tags.";
}

export default function AcquisitionTagBar({
  propertyId,
  value,
  onChange,
  compact = false,
}: Props) {
  const [tags, setTags] = React.useState<string[]>(normalizeTags(value));
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const previousStableRef = React.useRef<string[]>(normalizeTags(value));

  React.useEffect(() => {
    const next = normalizeTags(value);
    setTags(next);
    previousStableRef.current = next;
  }, [value]);

  const persist = React.useCallback(
    async (next: string[]) => {
      const normalizedNext = normalizeTags(next);
      const previous = previousStableRef.current;

      setTags(normalizedNext);
      setSaving(true);
      setError(null);

      try {
        if (onChange) {
          await onChange(normalizedNext);
        } else {
          await api.put(`/properties/${propertyId}/acquisition-tags`, {
            tags: normalizedNext,
          });
        }

        previousStableRef.current = normalizedNext;
      } catch (e: any) {
        setError(formatError(e));
        setTags(previous);
      } finally {
        setSaving(false);
      }
    },
    [onChange, propertyId],
  );

  const toggle = React.useCallback(
    async (tag: string) => {
      if (saving) return;

      const current = normalizeTags(tags);
      let next: string[];

      if (current.includes(tag)) {
        next = current.filter((t) => t !== tag);
      } else if (tag === "rejected") {
        next = ["rejected"];
      } else {
        next = [...current.filter((t) => t !== "rejected"), tag];
      }

      await persist(next);
    },
    [persist, saving, tags],
  );

  const activeCount = tags.length;
  const hasRejected = tags.includes("rejected");

  return (
    <div
      className={
        compact
          ? "space-y-2 rounded-2xl border border-app bg-app-panel p-3"
          : "space-y-3 rounded-3xl border border-app bg-app-panel p-5"
      }
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Acquisition tags
          </div>
          <div className="mt-1 text-sm text-app-3">
            Use tags to place this property into a real operating lane.
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {hasRejected ? (
            <span className="oh-pill oh-pill-bad">
              <AlertTriangle className="mr-1 h-3.5 w-3.5" />
              rejected lane
            </span>
          ) : null}

          <span className="oh-pill oh-pill-accent">
            {saving ? (
              <>
                <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                Saving…
              </>
            ) : (
              <>
                <Flag className="mr-1 h-3.5 w-3.5" />
                {activeCount} active
              </>
            )}
          </span>
        </div>
      </div>

      <div className="flex flex-wrap gap-2">
        {TAGS.map(({ key, label, icon: Icon }) => {
          const active = tags.includes(key);

          return (
            <button
              key={key}
              type="button"
              disabled={saving}
              onClick={() => toggle(key)}
              className={
                active
                  ? key === "rejected"
                    ? "oh-pill oh-pill-bad"
                    : "oh-pill oh-pill-accent"
                  : "oh-pill"
              }
              aria-pressed={active}
              title={
                key === "rejected"
                  ? "Rejected is exclusive and clears other operating tags."
                  : undefined
              }
            >
              <Icon className="mr-1 h-3.5 w-3.5" />
              {label}
            </button>
          );
        })}
      </div>

      <div className="text-xs text-app-4">
        {hasRejected
          ? "This property is marked rejected and removed from active acquisition lanes."
          : "Tagging helps drive shortlist board lanes, follow-up priority, and acquisition workflow routing."}
      </div>

      {error ? <div className="text-xs text-red-300">{error}</div> : null}
    </div>
  );
}
