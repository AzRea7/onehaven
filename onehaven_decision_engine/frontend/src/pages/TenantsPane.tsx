import React from "react";
import { Link } from "react-router-dom";
import { RefreshCcw, Users } from "lucide-react";
import PageShell from "../components/PageShell";
import PageHero from "../components/PageHero";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import { api } from "../lib/api";

type TenantRow = {
  property_id: number;
  address?: string;
  city?: string;
  state?: string;
  current_stage?: string;
  current_stage_label?: string;
  next_actions?: string[];
};

type PanePayload = {
  allowed_panes?: string[];
  kpis?: Record<string, any>;
  rows?: TenantRow[];
};

export default function TenantsPane() {
  const [data, setData] = React.useState<PanePayload | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);

  const refresh = React.useCallback(async () => {
    try {
      setLoading(true);
      const out = await api.get<PanePayload>("/dashboard/panes/tenants");
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
          title="Tenant placement pane"
          subtitle="Track marketing, screening, matching, and lease-up from a dedicated tenant workflow view."
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
          <Surface title="Marketing" subtitle="Units ready for tenant outreach">
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.marketing_count || 0)}
            </div>
          </Surface>
          <Surface title="Screening" subtitle="Applicants in active review">
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.screening_count || 0)}
            </div>
          </Surface>
          <Surface title="Leased" subtitle="Leases moving toward occupancy">
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.leased_count || 0)}
            </div>
          </Surface>
        </div>

        <Surface
          title="Tenant pipeline queue"
          subtitle="Properties currently routed into tenant marketing, screening, and lease activation."
        >
          {loading ? (
            <div className="grid gap-3">
              {Array.from({ length: 4 }).map((_, i) => (
                <div key={i} className="oh-skeleton h-[112px] rounded-3xl" />
              ))}
            </div>
          ) : !rows.length ? (
            <EmptyState
              icon={Users}
              title="No tenant pipeline properties"
              description="Nothing is currently routed into the tenant placement pane."
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
                      <div className="mt-3">
                        <span className="oh-pill oh-pill-accent">
                          {row.current_stage_label ||
                            row.current_stage ||
                            "tenant stage"}
                        </span>
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
