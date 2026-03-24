import React from "react";
import { Link } from "react-router-dom";
import { AlertTriangle, ArrowRight, RefreshCcw, Users } from "lucide-react";
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
  county?: string;
  current_stage?: string;
  current_stage_label?: string;
  current_pane?: string;
  current_pane_label?: string;
  urgency?: string;
  blockers?: string[];
  next_actions?: string[];
};

type BlockerRow = {
  blocker?: string;
  count?: number;
  example_property_id?: number;
  example_address?: string;
  example_city?: string;
  urgency?: string;
};

type ActionRow = {
  property_id?: number;
  address?: string;
  city?: string;
  stage?: string;
  pane?: string;
  urgency?: string;
  blocker?: string;
  action?: string;
};

type QueueCounts = {
  total?: number;
  by_stage?: Record<string, number>;
  by_status?: Record<string, number>;
  by_urgency?: Record<string, number>;
};

type PanePayload = {
  allowed_panes?: string[];
  filters?: Record<string, any>;
  kpis?: Record<string, any>;
  blockers?: BlockerRow[];
  recent_actions?: ActionRow[];
  next_actions?: ActionRow[];
  stale_items?: Array<{
    property_id?: number;
    address?: string;
    city?: string;
    reasons?: string[];
    urgency?: string;
  }>;
  queue_counts?: QueueCounts;
  rows?: TenantRow[];
  count?: number;
};

function labelize(value?: string | null) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function urgencyTone(urgency?: string | null) {
  const v = String(urgency || "").toLowerCase();
  if (v === "critical") return "oh-pill oh-pill-bad";
  if (v === "high") return "oh-pill oh-pill-warn";
  if (v === "medium") return "oh-pill oh-pill-accent";
  return "oh-pill";
}

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
  const blockers = Array.isArray(data?.blockers) ? data!.blockers! : [];
  const nextActions = Array.isArray(data?.next_actions)
    ? data!.next_actions!
    : [];

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Lifecycle pane"
          title="Tenant placement pane"
          subtitle="Track marketing, screening, matching, and lease-up from the same shared pane dashboard shape."
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

        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
          <Surface
            title="Visible properties"
            subtitle="Properties currently routed here"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.total_properties || data?.count || 0)}
            </div>
          </Surface>

          <Surface
            title="With next actions"
            subtitle="Properties ready for operator work"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.with_next_actions || 0)}
            </div>
          </Surface>

          <Surface
            title="High priority"
            subtitle="Items that likely need fast attention"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.high_priority_items || 0)}
            </div>
          </Surface>

          <Surface
            title="Critical items"
            subtitle="Blocked or urgent tenant workflow"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.critical_items || 0)}
            </div>
          </Surface>
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
          <Surface
            title="Queue status"
            subtitle="Shared queue breakdown used across pane dashboards"
          >
            <div className="grid gap-3">
              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Total in queue
                </div>
                <div className="mt-1 text-2xl font-semibold text-app-0">
                  {Number(data?.queue_counts?.total || rows.length || 0)}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  By stage
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {Object.entries(data?.queue_counts?.by_stage || {}).length ? (
                    Object.entries(data?.queue_counts?.by_stage || {}).map(
                      ([key, value]) => (
                        <span key={key} className="oh-pill">
                          {labelize(key)} · {Number(value || 0)}
                        </span>
                      ),
                    )
                  ) : (
                    <span className="text-sm text-app-4">No stage data</span>
                  )}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  By urgency
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {Object.entries(data?.queue_counts?.by_urgency || {})
                    .length ? (
                    Object.entries(data?.queue_counts?.by_urgency || {}).map(
                      ([key, value]) => (
                        <span key={key} className={urgencyTone(key)}>
                          {labelize(key)} · {Number(value || 0)}
                        </span>
                      ),
                    )
                  ) : (
                    <span className="text-sm text-app-4">No urgency data</span>
                  )}
                </div>
              </div>
            </div>
          </Surface>

          <Surface
            title="Top blockers"
            subtitle="Most common reasons tenant progression is not moving forward"
          >
            {!blockers.length ? (
              <div className="text-sm text-app-4">No active blockers.</div>
            ) : (
              <div className="grid gap-3">
                {blockers.slice(0, 5).map((item, idx) => (
                  <div
                    key={`${item.blocker || "blocker"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-muted px-4 py-3"
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div className="min-w-0">
                        <div className="text-sm font-medium text-app-0">
                          {labelize(item.blocker)}
                        </div>
                        <div className="mt-1 text-xs text-app-4">
                          Example: {item.example_address || "Unknown property"}
                          {item.example_city ? ` · ${item.example_city}` : ""}
                        </div>
                      </div>
                      <div className="text-sm font-semibold text-app-1">
                        {Number(item.count || 0)}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Surface>

          <Surface
            title="Next actions"
            subtitle="Highest-priority actions the pane says to do next"
          >
            {!nextActions.length ? (
              <div className="text-sm text-app-4">
                No next actions right now.
              </div>
            ) : (
              <div className="grid gap-3">
                {nextActions.slice(0, 5).map((item, idx) => (
                  <div
                    key={`${item.property_id || "action"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-muted px-4 py-3"
                  >
                    <div className="text-sm font-medium text-app-0">
                      {item.action || "No action description"}
                    </div>
                    <div className="mt-1 text-xs text-app-4">
                      {item.address || `Property #${item.property_id || "—"}`}
                      {item.city ? ` · ${item.city}` : ""}
                    </div>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {item.urgency ? (
                        <span className={urgencyTone(item.urgency)}>
                          {labelize(item.urgency)}
                        </span>
                      ) : null}
                      {item.blocker ? (
                        <span className="oh-pill oh-pill-warn">
                          {labelize(item.blocker)}
                        </span>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Surface>
        </div>

        <Surface
          title="Tenant pipeline queue"
          subtitle="Properties currently routed into tenant marketing, screening, and lease activation."
        >
          {loading ? (
            <div className="grid gap-3">
              {Array.from({ length: 4 }).map((_, i) => (
                <div key={i} className="oh-skeleton h-[124px] rounded-3xl" />
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
                      <div className="flex flex-wrap items-center gap-2">
                        <div className="text-base font-semibold text-app-0">
                          {row.address || `Property #${row.property_id}`}
                        </div>
                        {row.urgency ? (
                          <span className={urgencyTone(row.urgency)}>
                            {labelize(row.urgency)}
                          </span>
                        ) : null}
                      </div>

                      <div className="mt-1 text-sm text-app-4">
                        {[row.city, row.state].filter(Boolean).join(", ")}
                        {row.county ? ` · ${row.county}` : ""}
                      </div>

                      <div className="mt-3 flex flex-wrap gap-2">
                        <span className="oh-pill oh-pill-accent">
                          {row.current_stage_label ||
                            row.current_stage ||
                            "tenant stage"}
                        </span>

                        {row.blockers?.[0] ? (
                          <span className="oh-pill oh-pill-warn">
                            {labelize(row.blockers[0])}
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

                    <ArrowRight className="h-4 w-4 text-app-4" />
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
