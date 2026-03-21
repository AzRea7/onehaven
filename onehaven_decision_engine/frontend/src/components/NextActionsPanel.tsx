import React from "react";
import {
  ArrowRight,
  CheckCircle2,
  Clock3,
  ExternalLink,
  PlayCircle,
  ShieldCheck,
  AlertTriangle,
  Building2,
  Wrench,
  ClipboardX,
} from "lucide-react";
import Surface from "./Surface";
import EmptyState from "./EmptyState";

type ActionLike =
  | string
  | {
      title?: string;
      label?: string;
      detail?: string;
      description?: string;
      kind?: string;
      priority?: string;
      due_at?: string | null;
      href?: string | null;
      cta_label?: string | null;
      disabled?: boolean;
      onClick?: (() => void | Promise<void>) | null;
      category?: string | null;
      code?: string | null;
      severity?: string | null;
      source?: string | null;
      suggested_fix?: string | null;
      notes?: string | null;
      blocks_hqs?: boolean;
      blocks_local?: boolean;
      blocks_voucher?: boolean;
      blocks_lease_up?: boolean;
    };

function normalizeAction(action: ActionLike) {
  if (typeof action === "string") {
    return {
      title: action,
      detail: "",
      kind: "manual",
      priority: "normal",
      due_at: null as string | null,
      href: null as string | null,
      cta_label: null as string | null,
      disabled: false,
      onClick: null as (() => void | Promise<void>) | null,
      category: null as string | null,
      code: null as string | null,
      severity: null as string | null,
      source: null as string | null,
      blocks_hqs: false,
      blocks_local: false,
      blocks_voucher: false,
      blocks_lease_up: false,
    };
  }

  const derivedKind =
    action?.kind ||
    action?.category ||
    action?.source ||
    (action?.severity ? "compliance" : "manual");

  const derivedPriority =
    action?.priority ||
    (["blocking", "critical", "urgent", "high"].includes(
      String(action?.severity || "").toLowerCase(),
    )
      ? "high"
      : "normal");

  return {
    title: action?.title || action?.label || "Untitled action",
    detail:
      action?.detail ||
      action?.description ||
      action?.suggested_fix ||
      action?.notes ||
      "",
    kind: derivedKind,
    priority: derivedPriority,
    due_at: action?.due_at || null,
    href: action?.href || null,
    cta_label: action?.cta_label || null,
    disabled: Boolean(action?.disabled),
    onClick: action?.onClick || null,
    category: action?.category || null,
    code: action?.code || null,
    severity: action?.severity || null,
    source: action?.source || null,
    blocks_hqs: Boolean(action?.blocks_hqs),
    blocks_local: Boolean(action?.blocks_local),
    blocks_voucher: Boolean(action?.blocks_voucher),
    blocks_lease_up: Boolean(action?.blocks_lease_up),
  };
}

function priorityTone(priority?: string) {
  const p = String(priority || "").toLowerCase();
  if (p === "high" || p === "urgent" || p === "critical") {
    return "oh-pill oh-pill-bad";
  }
  if (p === "medium") return "oh-pill oh-pill-warn";
  if (p === "low") return "oh-pill oh-pill-good";
  return "oh-pill";
}

function kindTone(kind?: string) {
  const k = String(kind || "").toLowerCase();
  if (["advance", "workflow", "gate"].includes(k))
    return "oh-pill oh-pill-accent";
  if (
    [
      "compliance",
      "inspection",
      "municipal_registration",
      "municipal_inspection",
      "municipal_certificate",
      "compliance_repair",
    ].includes(k)
  ) {
    return "oh-pill oh-pill-warn";
  }
  if (
    ["deal", "cashflow", "equity", "valuation", "lease", "tenant"].includes(k)
  ) {
    return "oh-pill oh-pill-good";
  }
  if (
    ["jurisdiction", "policy", "coverage", "market", "pha_workflow"].includes(k)
  ) {
    return "oh-pill oh-pill-accent";
  }
  return "oh-pill";
}

function iconForKind(kind?: string) {
  const k = String(kind || "").toLowerCase();
  if (
    [
      "jurisdiction",
      "policy",
      "coverage",
      "market",
      "municipal_registration",
    ].includes(k)
  ) {
    return <Building2 className="h-4 w-4 text-app-4" />;
  }
  if (["compliance", "inspection", "municipal_certificate"].includes(k)) {
    return <AlertTriangle className="h-4 w-4 text-app-4" />;
  }
  if (["compliance_repair", "repair", "rehab"].includes(k)) {
    return <Wrench className="h-4 w-4 text-app-4" />;
  }
  return <ShieldCheck className="h-4 w-4 text-app-4" />;
}

export default function NextActionsPanel({
  actions,
}: {
  actions?: ActionLike[];
}) {
  const rows = Array.isArray(actions) ? actions.map(normalizeAction) : [];

  return (
    <Surface
      title="Next actions"
      subtitle="Real next moves from workflow gates, inspection failures, and compliance blockers."
    >
      {!rows.length ? (
        <EmptyState
          compact
          title="No next actions"
          description="Once workflow gates and inspection failures surface action items, they will show up here."
        />
      ) : (
        <div className="space-y-3">
          {rows.map((action, i) => {
            const interactiveButton =
              action.onClick || action.href ? (
                action.href ? (
                  <a
                    href={action.href}
                    target="_blank"
                    rel="noreferrer"
                    className="oh-btn oh-btn-secondary shrink-0"
                  >
                    {action.cta_label || "open"}
                    <ExternalLink className="h-4 w-4" />
                  </a>
                ) : (
                  <button
                    type="button"
                    onClick={() => action.onClick?.()}
                    disabled={action.disabled}
                    className="oh-btn oh-btn-primary shrink-0 disabled:opacity-60"
                  >
                    {action.cta_label || "run"}
                    <PlayCircle className="h-4 w-4" />
                  </button>
                )
              ) : (
                <div className="shrink-0 text-app-4">
                  <ArrowRight className="h-4 w-4" />
                </div>
              );

            return (
              <div
                key={`${action.title}-${i}`}
                className="rounded-2xl border border-app bg-app-panel px-4 py-4"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      {iconForKind(action.kind)}
                      <div className="text-sm font-semibold text-app-0">
                        {action.title}
                      </div>
                      <span className={priorityTone(action.priority)}>
                        {action.priority}
                      </span>
                      <span className={kindTone(action.kind)}>
                        {action.kind}
                      </span>
                      {action.severity ? (
                        <span className={priorityTone(action.severity)}>
                          {action.severity}
                        </span>
                      ) : null}
                    </div>

                    {action.detail ? (
                      <div className="mt-2 text-sm leading-6 text-app-3">
                        {action.detail}
                      </div>
                    ) : null}

                    <div className="mt-2 flex flex-wrap gap-2 text-xs text-app-4">
                      {action.category ? (
                        <span>category: {action.category}</span>
                      ) : null}
                      {action.code ? <span>code: {action.code}</span> : null}
                      {action.source ? (
                        <span>source: {action.source}</span>
                      ) : null}
                      {action.blocks_hqs ? <span>blocks HQS</span> : null}
                      {action.blocks_local ? <span>blocks local</span> : null}
                      {action.blocks_voucher ? (
                        <span>blocks voucher</span>
                      ) : null}
                      {action.blocks_lease_up ? (
                        <span>blocks lease-up</span>
                      ) : null}
                    </div>

                    {action.due_at ? (
                      <div className="mt-2 flex items-center gap-2 text-xs text-app-4">
                        <Clock3 className="h-3.5 w-3.5" />
                        due {new Date(action.due_at).toLocaleString()}
                      </div>
                    ) : null}
                  </div>

                  {interactiveButton}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {!!rows.length ? (
        <div className="mt-4 flex items-center gap-2 text-xs text-app-4">
          <ClipboardX className="h-3.5 w-3.5" />
          ordered for operator attention using real workflow and inspection
          blockers
        </div>
      ) : null}
    </Surface>
  );
}
