import React from "react";
import type { LucideIcon } from "lucide-react";
import { Inbox } from "lucide-react";

export default function EmptyState({
  title,
  description,
  action,
  icon: Icon = Inbox,
  compact = false,
}: {
  title: string;
  description?: string;
  action?: React.ReactNode;
  icon?: LucideIcon;
  compact?: boolean;
}) {
  return (
    <div
      className={compact ? "empty-state empty-state-compact" : "empty-state"}
    >
      <div className="empty-state-icon">
        <Icon className="h-5 w-5" />
      </div>

      <div className="text-center">
        <div className="text-base font-semibold text-app-0">{title}</div>
        {description ? (
          <p className="mt-2 max-w-[42rem] text-sm leading-6 text-app-3">
            {description}
          </p>
        ) : null}
      </div>

      {action ? <div className="mt-2">{action}</div> : null}
    </div>
  );
}
