import React from "react";
import { ShieldCheck, TriangleAlert, PlayCircle } from "lucide-react";
import Surface from "./Surface";
import EmptyState from "./EmptyState";

function toneClass(kind: "good" | "warn" | "bad" | "neutral" = "neutral") {
  if (kind === "good") return "oh-pill oh-pill-good";
  if (kind === "warn") return "oh-pill oh-pill-warn";
  if (kind === "bad") return "oh-pill oh-pill-bad";
  return "oh-pill";
}

function readinessPill(label: string, ok: boolean | undefined) {
  const tone = ok === true ? "good" : ok === false ? "bad" : "neutral";
  return (
    <span key={label} className={toneClass(tone)}>
      {label}: {ok === true ? "ready" : ok === false ? "blocked" : "—"}
    </span>
  );
}

export default function InspectionReadiness({
  readiness,
  brief,
  status,
  summary,
  onRunAutomation,
  busy,
}: {
  readiness?: any;
  brief?: any;
  status?: any;
  summary?: any;
  onRunAutomation?: () => void;
  busy?: boolean;
}) {
  const model = readiness || brief || null;
  if (!model) return null;

  const pills = [
    readinessPill("HQS", model?.hqs_ready),
    readinessPill("Local", model?.local_ready),
    readinessPill("Voucher", model?.voucher_ready),
    readinessPill("Lease-up", model?.lease_up_ready),
  ];

  return (
    <Surface
      title="Inspection readiness"
      subtitle="Fast read on whether the property is actually ready versus merely feeling optimistic."
      actions={
        onRunAutomation ? (
          <button
            onClick={onRunAutomation}
            disabled={busy}
            className="oh-btn oh-btn-secondary"
          >
            <PlayCircle className="h-4 w-4" />
            {busy ? "running…" : "run automation"}
          </button>
        ) : null
      }
    >
      <div className="flex flex-wrap gap-2">{pills}</div>

      <div className="mt-4 grid gap-3 md:grid-cols-3">
        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Readiness score
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {summary?.score_pct != null ? `${summary.score_pct}%` : "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Failures
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {summary?.failed != null ? summary.failed : "—"}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Status
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {status?.passed == null ? "—" : status.passed ? "Ready" : "Blocked"}
          </div>
        </div>
      </div>

      {brief?.explanation ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-panel px-4 py-4">
          <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
            {status?.passed ? (
              <ShieldCheck className="h-4 w-4 text-app-4" />
            ) : (
              <TriangleAlert className="h-4 w-4 text-app-4" />
            )}
            Readiness notes
          </div>
          <div className="mt-2 text-sm leading-6 text-app-3">
            {brief.explanation}
          </div>
        </div>
      ) : null}

      {!brief?.explanation &&
      summary?.failed == null &&
      status?.passed == null ? (
        <div className="mt-4">
          <EmptyState compact title="No detailed readiness notes yet" />
        </div>
      ) : null}
    </Surface>
  );
}
