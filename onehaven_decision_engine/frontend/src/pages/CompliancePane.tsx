import React from "react";
import { Link } from "react-router-dom";
import { ClipboardCheck, RefreshCcw } from "lucide-react";
import PageShell from "../components/PageShell";
import PageHero from "../components/PageHero";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import { api } from "../lib/api";

type ComplianceRow = {
  property_id: number;
  address?: string;
  city?: string;
  state?: string;
  current_stage?: string;
  current_stage_label?: string;
  blockers?: string[];
  next_actions?: string[];
  compliance?: {
    completion_pct?: number;
    failed_count?: number;
    blocked_count?: number;
    open_failed_items?: number;
  };
  jurisdiction?: {
    completeness_status?: string;
    is_stale?: boolean;
  };
};

type PanePayload = {
  allowed_panes?: string[];
  kpis?: Record<string, any>;
  blockers?: Array<{ blocker?: string; count?: number }>;
  rows?: ComplianceRow[];
};

export default function CompliancePane() {
  const [data, setData] = React.useState<PanePayload | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);

  const refresh = React.useCallback(async () => {
    try {
      setLoading(true);
      const out = await api.get<PanePayload>("/dashboard/panes/compliance");
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
          title="Compliance / S8 pane"
          subtitle="Run rehab, jurisdiction, inspection, and compliance readiness from one operating surface."
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

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
          <Surface
            title="Compliance properties"
            subtitle="Visible in compliance pane"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.total_properties || 0)}
            </div>
          </Surface>
          <Surface
            title="Inspection pending"
            subtitle="Still waiting on inspection flow"
          >
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.inspection_pending_count || 0)}
            </div>
          </Surface>
          <Surface title="Failed items" subtitle="Open compliance failures">
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.failed_items_total || 0)}
            </div>
          </Surface>
          <Surface title="Jurisdiction stale" subtitle="Needs policy refresh">
            <div className="text-3xl font-semibold text-app-0">
              {Number(data?.kpis?.jurisdiction_stale_count || 0)}
            </div>
          </Surface>
        </div>

        <Surface
          title="Compliance queue"
          subtitle="Properties currently routed into rehab, readiness, and inspection work."
        >
          {loading ? (
            <div className="grid gap-3">
              {Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="oh-skeleton h-[120px] rounded-3xl" />
              ))}
            </div>
          ) : !rows.length ? (
            <EmptyState
              icon={ClipboardCheck}
              title="No compliance properties"
              description="Nothing is currently routed into the compliance pane."
            />
          ) : (
            <div className="grid gap-4">
              {rows.map((row) => (
                <Link
                  key={row.property_id}
                  to={`/properties/${row.property_id}`}
                  className="rounded-3xl border border-app bg-app-panel px-5 py-4 transition hover:border-app-strong hover:bg-app-muted"
                >
                  <div className="flex flex-wrap items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="text-base font-semibold text-app-0">
                        {row.address || `Property #${row.property_id}`}
                      </div>
                      <div className="mt-1 text-sm text-app-4">
                        {[row.city, row.state].filter(Boolean).join(", ")}
                      </div>

                      <div className="mt-3 flex flex-wrap gap-2">
                        <span className="oh-pill oh-pill-warn">
                          {row.current_stage_label ||
                            row.current_stage ||
                            "compliance"}
                        </span>

                        {row.jurisdiction?.is_stale ? (
                          <span className="oh-pill oh-pill-bad">
                            jurisdiction stale
                          </span>
                        ) : null}

                        {row.jurisdiction?.completeness_status &&
                        row.jurisdiction?.completeness_status !== "complete" ? (
                          <span className="oh-pill oh-pill-warn">
                            {row.jurisdiction.completeness_status}
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

                    <div className="grid gap-2 text-right">
                      <div>
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Completion
                        </div>
                        <div className="text-sm font-semibold text-app-0">
                          {row.compliance?.completion_pct != null
                            ? `${Math.round(Number(row.compliance.completion_pct) * 100)}%`
                            : "—"}
                        </div>
                      </div>
                      <div>
                        <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                          Failed / blocked
                        </div>
                        <div className="text-sm font-semibold text-app-0">
                          {Number(row.compliance?.failed_count || 0)} /{" "}
                          {Number(row.compliance?.blocked_count || 0)}
                        </div>
                      </div>
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
