import React from "react";
import { ArrowRight, CheckCircle2, Clock3 } from "lucide-react";
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
    };
  }

  return {
    title: action?.title || action?.label || "Untitled action",
    detail: action?.detail || action?.description || "",
    kind: action?.kind || "manual",
    priority: action?.priority || "normal",
    due_at: action?.due_at || null,
    href: action?.href || null,
  };
}

function priorityTone(priority?: string) {
  const p = String(priority || "").toLowerCase();
  if (p === "high" || p === "urgent" || p === "critical")
    return "oh-pill oh-pill-bad";
  if (p === "medium") return "oh-pill oh-pill-warn";
  if (p === "low") return "oh-pill oh-pill-good";
  return "oh-pill";
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
      subtitle="The smallest useful list of things that actually move the property forward."
    >
      {!rows.length ? (
        <EmptyState
          compact
          title="No next actions"
          description="Once workflow gates and operating truth surface action items, they will show up here."
        />
      ) : (
        <div className="space-y-3">
          {rows.map((action, i) => {
            const content = (
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <div className="text-sm font-semibold text-app-0">
                      {action.title}
                    </div>
                    <span className={priorityTone(action.priority)}>
                      {action.priority}
                    </span>
                    <span className="oh-pill">{action.kind}</span>
                  </div>

                  {action.detail ? (
                    <div className="mt-2 text-sm leading-6 text-app-3">
                      {action.detail}
                    </div>
                  ) : null}

                  {action.due_at ? (
                    <div className="mt-2 flex items-center gap-2 text-xs text-app-4">
                      <Clock3 className="h-3.5 w-3.5" />
                      due {new Date(action.due_at).toLocaleString()}
                    </div>
                  ) : null}
                </div>

                <div className="shrink-0 text-app-4">
                  <ArrowRight className="h-4 w-4" />
                </div>
              </div>
            );

            if (action.href) {
              return (
                <a
                  key={`${action.title}-${i}`}
                  href={action.href}
                  target="_blank"
                  rel="noreferrer"
                  className="block rounded-2xl border border-app bg-app-panel px-4 py-4 hover:border-app-strong hover:bg-app-muted"
                >
                  {content}
                </a>
              );
            }

            return (
              <div
                key={`${action.title}-${i}`}
                className="rounded-2xl border border-app bg-app-panel px-4 py-4"
              >
                {content}
              </div>
            );
          })}
        </div>
      )}

      {!!rows.length ? (
        <div className="mt-4 flex items-center gap-2 text-xs text-app-4">
          <CheckCircle2 className="h-3.5 w-3.5" />
          ordered for operator attention, not decorative dashboard fluff
        </div>
      ) : null}
    </Surface>
  );
}
