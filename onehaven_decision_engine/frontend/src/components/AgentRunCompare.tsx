import React from "react";

type CompareRow = {
  id: number;
  property_id?: number | null;
  agent_key: string;
  status: string;
  runtime_health?: string;
  approval_status?: string;
  attempts?: number;
  duration_ms?: number | null;
  trace_count?: number;
  message_count?: number;
  created_at?: string | null;
  finished_at?: string | null;
  has_output?: boolean;
  has_proposed_actions?: boolean;
  last_error?: string | null;
};

type Props = {
  rows: CompareRow[];
};

function fmtMs(v?: number | null) {
  if (v == null || !Number.isFinite(v)) return "—";
  if (v < 1000) return `${v}ms`;
  const sec = v / 1000;
  if (sec < 60) return `${sec.toFixed(1)}s`;
  const min = Math.floor(sec / 60);
  return `${min}m ${Math.round(sec % 60)}s`;
}

const fields: Array<{
  key: keyof CompareRow;
  label: string;
  render?: (v: any) => React.ReactNode;
}> = [
  { key: "id", label: "Run ID" },
  { key: "property_id", label: "Property" },
  { key: "agent_key", label: "Agent" },
  { key: "status", label: "Status" },
  { key: "runtime_health", label: "Health" },
  { key: "approval_status", label: "Approval" },
  { key: "attempts", label: "Attempts" },
  { key: "duration_ms", label: "Duration", render: (v) => fmtMs(v) },
  { key: "trace_count", label: "Trace events" },
  { key: "message_count", label: "Messages" },
  { key: "has_output", label: "Has output", render: (v) => (v ? "yes" : "no") },
  {
    key: "has_proposed_actions",
    label: "Has actions",
    render: (v) => (v ? "yes" : "no"),
  },
  { key: "last_error", label: "Last error" },
];

export default function AgentRunCompare({ rows }: Props) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-4 overflow-hidden">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-zinc-100">Run compare</div>
          <div className="text-xs text-zinc-400 mt-1">
            Side-by-side diff for debugging contracts, retries, and weird little
            gremlins.
          </div>
        </div>
        <div className="text-xs text-zinc-400">{rows.length}/4 selected</div>
      </div>

      {rows.length < 2 ? (
        <div className="mt-4 text-sm text-zinc-400">
          Select at least 2 runs from history to compare.
        </div>
      ) : (
        <div className="mt-4 overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="border-b border-white/10">
                <th className="px-3 py-2 text-left text-zinc-400 font-medium">
                  Field
                </th>
                {rows.map((row) => (
                  <th
                    key={row.id}
                    className="px-3 py-2 text-left text-zinc-200 font-medium"
                  >
                    #{row.id}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {fields.map((field) => (
                <tr
                  key={String(field.key)}
                  className="border-b border-white/5 align-top"
                >
                  <td className="px-3 py-2 text-zinc-400 w-[180px]">
                    {field.label}
                  </td>
                  {rows.map((row) => {
                    const raw = row[field.key];
                    const rendered = field.render
                      ? field.render(raw)
                      : (raw ?? "—");
                    return (
                      <td
                        key={`${row.id}-${String(field.key)}`}
                        className="px-3 py-2 text-zinc-100"
                      >
                        {typeof rendered === "string" &&
                        rendered.length > 120 ? (
                          <div className="max-w-[320px] whitespace-pre-wrap break-words text-xs text-red-200/90">
                            {rendered}
                          </div>
                        ) : (
                          (rendered as any)
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
