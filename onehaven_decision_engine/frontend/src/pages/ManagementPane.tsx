import React from "react";
import { Link } from "react-router-dom";
import { RefreshCcw, Settings2 } from "lucide-react";
import PageShell from "../components/PageShell";
import PageHero from "../components/PageHero";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import { api } from "../lib/api";

type ManagementRow = {
  property_id: number;
  address?: string;
  city?: string;
  state?: string;
  current_stage?: string;
  current_stage_label?: string;
  blockers?: string[];
  next_actions?: string[];
};

type PanePayload = {
  allowed_panes?: string[];
  kpis?: Record<string, any>;
  rows?: ManagementRow[];
};

export default function ManagementPane() {
  const [data, setData] = React.useState<PanePayload | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);

  const refresh = React.useCallback(async () => {
    try {
      setLoading(true);
      const out = await api.get<PanePayload>("/dashboard/panes/management");
      setData(out);
      setErr(null);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    refresh();
  }, [refresh]);

  const rows = Array.isArray(data?.rows) ? data!.rows! : [];

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Mode"
          title="Management pane"
          subtitle="Operate occupied properties, turnover, and maintenance from a dedicated management workspace."
          actions={
            <button onClick={refresh} className="oh-btn oh-btn-secondary">
              <RefreshCcw className="h-4 w-4" />
              Refresh pane
            </button>
          }
        />

        {err ? (
          <Surface tone="danger">
            <div className="text-sm text-red-300">{err}</div>
          </Surface>
        ) : null}

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
          <Surface title="Occupied" subtitle="Stable occupied operations">
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.occupied_count || 0)}
            </div>
          </Surface>
          <Surface title="Turnover" subtitle="Units between occupancies">
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.turnover_count || 0)}
            </div>
          </Surface>
          <Surface title="Maintenance" subtitle="Operational support workload">
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.maintenance_count || 0)}
            </div>
          </Surface>
        </div>

        <Surface
          title="Management queue"
          subtitle="Properties in occupied operations, turnover handling, or maintenance workflow."
        >
          {loading ? (
            <div className="grid gap-3">
              {Array.from({ length: 4 }).map((_, i) => (
                <div key={i} className="oh-skeleton h-[112px] rounded-3xl" />
              ))}
            </div>
          ) : !rows.length ? (
            <EmptyState
              icon={Settings2}
              title="No management properties"
              description="Nothing is currently routed into the management pane."
            />
          ) : (
            <div className="grid gap-4">
              {rows.map((row) => (
                <Link
                  key={row.property_id}
                  to={`/properties/${row.property_id}`}
                  className="rounded-3xl border border-app bg-app-panel px-5 py-4 transition hover:border-app-strong hover:bg-app-muted"
                >
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <div className="text-base font-semibold text-app-0">
                        {row.address || `Property #${row.property_id}`}
                      </div>
                      <div className="mt-1 text-sm text-app-4">
                        {[row.city, row.state].filter(Boolean).join(", ")}
                      </div>
                      <div className="mt-3 flex flex-wrap gap-2">
                        <span className="oh-pill oh-pill-good">
                          {row.current_stage_label ||
                            row.current_stage ||
                            "management"}
                        </span>
                        {row.blockers?.[0] ? (
                          <span className="oh-pill oh-pill-warn">
                            {row.blockers[0].replace(/_/g, " ")}
                          </span>
                        ) : null}
                      </div>
                      {row.next_actions?.[0] ? (
                        <div className="mt-3 text-sm text-app-2">
                          Next action:{" "}
                          <span className="text-app-1">
                            {row.next_actions[0]}
                          </span>
                        </div>
                      ) : null}
                    </div>
                  </div>
                </Link>
              ))}
            </div>
          )}
        </Surface>
      </div>
    </PageShell>
  );
}
