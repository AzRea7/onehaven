import React from "react";

function normalizeActions(actions: any): string[] {
  if (!Array.isArray(actions)) return [];
  return actions
    .map((x) => {
      if (typeof x === "string") return x;
      if (x && typeof x.title === "string") return x.title;
      if (x && typeof x.label === "string") return x.label;
      return null;
    })
    .filter(Boolean) as string[];
}

export default function NextActionsPanel({
  actions,
  title = "Next Actions",
  emptyText = "No blockers detected.",
}: {
  actions?: any;
  title?: string;
  emptyText?: string;
}) {
  const list = normalizeActions(actions);

  return (
    <div className="oh-panel p-5">
      <div className="text-sm font-semibold text-white">{title}</div>

      <div className="mt-3 space-y-2">
        {list.length ? (
          list.slice(0, 8).map((a, i) => (
            <div
              key={`${a}-${i}`}
              className="rounded-xl border border-white/10 bg-white/[0.03] p-3 text-sm text-white/80"
            >
              {a}
            </div>
          ))
        ) : (
          <div className="text-sm text-white/55">{emptyText}</div>
        )}
      </div>
    </div>
  );
}
