import React from "react";
import { Link, useParams } from "react-router-dom";
import {
  AlertTriangle,
  ArrowRight,
  BadgeDollarSign,
  CheckCircle2,
  ClipboardCheck,
  FileWarning,
  GitBranch,
  Home,
  LocateFixed,
  MapPinned,
  RefreshCcw,
  ShieldAlert,
  Users,
  Wallet,
} from "lucide-react";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import Golem from "../components/Golem";
import { api } from "../lib/api";
import { nextPaneKey, paneLabel, paneStep } from "../components/PaneSwitcher";

type PropertyPayload = {
  id?: number;
  property_id?: number;
  address?: string;
  city?: string;
  state?: string;
  zip?: string;
  county?: string;
  current_stage?: string;
  current_stage_label?: string;
  current_pane?: string;
  current_pane_label?: string;
  suggested_pane?: string;
  route_reason?: string;
  normalized_decision?: string;
  gate_status?: string;
  asking_price?: number | null;
  projected_monthly_cashflow?: number | null;
  dscr?: number | null;
  blockers?: string[];
  next_actions?: string[];
  jurisdiction?: {
    completeness_status?: string;
    is_stale?: boolean;
  };
  compliance?: {
    completion_pct?: number;
    failed_count?: number;
    blocked_count?: number;
    open_failed_items?: number;
  };
};

function money(v?: number | null) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return Number(v).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
}

function num(v?: number | null, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function normalizeDecision(raw?: string) {
  const x = String(raw || "")
    .trim()
    .toUpperCase();
  if (["PASS", "GOOD_DEAL", "GOOD", "APPROVED", "APPROVE"].includes(x)) {
    return "GOOD_DEAL";
  }
  if (["REJECT", "FAIL", "FAILED", "NO_GO"].includes(x)) {
    return "REJECT";
  }
  return "REVIEW";
}

function decisionPillClass(raw?: string) {
  const d = normalizeDecision(raw);
  if (d === "GOOD_DEAL") return "oh-pill oh-pill-good";
  if (d === "REVIEW") return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-bad";
}

function panePillClass(raw?: string) {
  const x = String(raw || "")
    .trim()
    .toLowerCase();
  if (x === "management") return "oh-pill oh-pill-good";
  if (x === "tenants") return "oh-pill oh-pill-accent";
  if (x === "compliance") return "oh-pill oh-pill-warn";
  if (x === "acquisition") return "oh-pill oh-pill-accent";
  return "oh-pill";
}

export default function Property() {
  const { id } = useParams();
  const [data, setData] = React.useState<PropertyPayload | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);

  const refresh = React.useCallback(async () => {
    if (!id) return;

    try {
      setLoading(true);
      const out = await api.get<PropertyPayload>(`/dashboard/property/${id}`);
      setData(out);
      setErr(null);
    } catch (e: any) {
      try {
        const fallback = await api.get<PropertyPayload>(
          `/properties/${id}/view`,
        );
        setData(fallback);
        setErr(null);
      } catch (inner: any) {
        setErr(String(inner?.message || inner || e?.message || e));
      }
    } finally {
      setLoading(false);
    }
  }, [id]);

  React.useEffect(() => {
    refresh();
  }, [refresh]);

  if (loading) {
    return (
      <PageShell>
        <div className="space-y-6">
          <div className="oh-skeleton h-[220px] rounded-[32px]" />
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="oh-skeleton h-[140px] rounded-3xl" />
            ))}
          </div>
        </div>
      </PageShell>
    );
  }

  if (err || !data) {
    return (
      <PageShell>
        <EmptyState
          icon={FileWarning}
          title="Property failed to load"
          description={err || "Property data is unavailable."}
        />
      </PageShell>
    );
  }

  const currentPane = String(data.current_pane || "investor").toLowerCase();
  const suggestedPane = String(
    data.suggested_pane || data.current_pane || "investor",
  ).toLowerCase();
  const nextStagePane = nextPaneKey(currentPane);
  const paneChanged = suggestedPane && suggestedPane !== currentPane;
  const movedToCompliance =
    currentPane !== "compliance" && suggestedPane === "compliance";
  const movedToTenants =
    currentPane !== "tenants" && suggestedPane === "tenants";
  const movedToManagement =
    currentPane !== "management" && suggestedPane === "management";
  const topBlocker = data.blockers?.[0] || null;
  const nextAction = data.next_actions?.[0] || null;

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Property lifecycle"
          title={data.address || `Property #${id}`}
          subtitle={[
            data.city,
            data.state,
            data.zip,
            data.county ? `County: ${data.county}` : null,
          ]
            .filter(Boolean)
            .join(" · ")}
          right={
            <div className="pointer-events-auto absolute inset-0 flex items-center justify-center overflow-visible">
              <div className="h-[220px] w-[220px] translate-y-[-8px] opacity-95 md:h-[250px] md:w-[250px]">
                <Golem className="h-full w-full" />
              </div>
            </div>
          }
          actions={
            <>
              <button onClick={refresh} className="oh-btn oh-btn-secondary">
                <RefreshCcw className="h-4 w-4" />
                Refresh property
              </button>
              <Link
                to={`/panes/${currentPane}`}
                className="oh-btn oh-btn-secondary"
              >
                Open current pane
              </Link>
            </>
          }
        />

        <Surface
          title="Lifecycle routing"
          subtitle="This is the property-level lifecycle state that drives the pane shell."
        >
          <div className="grid gap-4 xl:grid-cols-[1.35fr_1fr]">
            <div className="rounded-3xl border border-app bg-app-panel p-5">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                    Current route
                  </div>
                  <div className="mt-2 text-lg font-semibold text-app-0">
                    {paneLabel(currentPane)}
                  </div>
                  <div className="mt-1 text-sm text-app-4">
                    stage{" "}
                    {data.current_stage_label || data.current_stage || "—"}
                  </div>
                </div>

                <div className="flex flex-wrap gap-2">
                  <span className={panePillClass(currentPane)}>
                    current pane {paneLabel(currentPane)}
                  </span>
                  <span className="oh-pill">
                    step {paneStep(currentPane) || "—"}
                  </span>
                  <span className="oh-pill oh-pill-accent">
                    next pane {paneLabel(suggestedPane)}
                  </span>
                </div>
              </div>

              <div className="mt-4 grid gap-3 md:grid-cols-3">
                <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                    Current stage
                  </div>
                  <div className="mt-2 text-sm font-semibold text-app-0">
                    {data.current_stage_label || data.current_stage || "—"}
                  </div>
                </div>

                <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                    Next stage
                  </div>
                  <div className="mt-2 text-sm font-semibold text-app-0">
                    {paneChanged
                      ? paneLabel(suggestedPane)
                      : nextStagePane
                        ? paneLabel(nextStagePane)
                        : "Hold in current pane"}
                  </div>
                </div>

                <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                    Top blocker
                  </div>
                  <div className="mt-2 text-sm font-semibold text-app-0">
                    {topBlocker ? topBlocker.replace(/_/g, " ") : "No blocker"}
                  </div>
                </div>
              </div>

              <div className="mt-4 flex flex-wrap gap-2">
                {movedToCompliance ? (
                  <span className="oh-pill oh-pill-warn">
                    moved to compliance
                  </span>
                ) : null}
                {movedToTenants ? (
                  <span className="oh-pill oh-pill-accent">
                    moved to tenants
                  </span>
                ) : null}
                {movedToManagement ? (
                  <span className="oh-pill oh-pill-good">
                    moved to management
                  </span>
                ) : null}
                {paneChanged ? (
                  <span className="oh-pill oh-pill-warn">advance ready</span>
                ) : (
                  <span className="oh-pill">still working current pane</span>
                )}
                <span className={decisionPillClass(data.normalized_decision)}>
                  {normalizeDecision(data.normalized_decision).replace(
                    "_",
                    " ",
                  )}
                </span>
                {data.gate_status ? (
                  <span className="oh-pill">{data.gate_status}</span>
                ) : null}
              </div>

              <div className="mt-4 text-sm text-app-3">
                {data.route_reason ||
                  "This property stays in or moves to the next pane based on stage completion, blockers, and workflow routing."}
              </div>

              {nextAction ? (
                <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3">
                  <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                    <ArrowRight className="h-3.5 w-3.5" />
                    Next action
                  </div>
                  <div className="mt-2 text-sm font-medium text-app-0">
                    {nextAction}
                  </div>
                </div>
              ) : null}
            </div>

            <div className="space-y-4">
              <div className="rounded-3xl border border-app bg-app-panel p-5">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <Wallet className="h-3.5 w-3.5" />
                  Underwriting
                </div>
                <div className="mt-4 grid gap-3">
                  <div className="flex items-center justify-between gap-3 rounded-2xl border border-app bg-app-muted px-4 py-3">
                    <span className="text-sm text-app-4">Asking price</span>
                    <span className="text-sm font-semibold text-app-0">
                      {money(data.asking_price)}
                    </span>
                  </div>
                  <div className="flex items-center justify-between gap-3 rounded-2xl border border-app bg-app-muted px-4 py-3">
                    <span className="text-sm text-app-4">Cashflow est.</span>
                    <span className="text-sm font-semibold text-app-0">
                      {money(data.projected_monthly_cashflow)}
                    </span>
                  </div>
                  <div className="flex items-center justify-between gap-3 rounded-2xl border border-app bg-app-muted px-4 py-3">
                    <span className="text-sm text-app-4">DSCR</span>
                    <span className="text-sm font-semibold text-app-0">
                      {num(data.dscr)}
                    </span>
                  </div>
                </div>
              </div>

              <div className="rounded-3xl border border-app bg-app-panel p-5">
                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  <MapPinned className="h-3.5 w-3.5" />
                  Compliance state
                </div>
                <div className="mt-4 flex flex-wrap gap-2">
                  {data.jurisdiction?.is_stale ? (
                    <span className="oh-pill oh-pill-bad">
                      jurisdiction stale
                    </span>
                  ) : (
                    <span className="oh-pill oh-pill-good">
                      jurisdiction current
                    </span>
                  )}
                  {data.jurisdiction?.completeness_status ? (
                    <span className="oh-pill">
                      {data.jurisdiction.completeness_status}
                    </span>
                  ) : null}
                  {Number(data.compliance?.failed_count || 0) > 0 ? (
                    <span className="oh-pill oh-pill-bad">
                      failed {Number(data.compliance?.failed_count || 0)}
                    </span>
                  ) : null}
                  {Number(data.compliance?.blocked_count || 0) > 0 ? (
                    <span className="oh-pill oh-pill-warn">
                      blocked {Number(data.compliance?.blocked_count || 0)}
                    </span>
                  ) : null}
                </div>
              </div>
            </div>
          </div>
        </Surface>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-4">
          <Surface title="Pane" subtitle="Current operating owner">
            <div className="flex items-center gap-2 text-2xl font-semibold text-app-0">
              <Home className="h-5 w-5" />
              {paneLabel(currentPane)}
            </div>
          </Surface>
          <Surface title="Stage" subtitle="Current workflow stage">
            <div className="flex items-center gap-2 text-2xl font-semibold text-app-0">
              <GitBranch className="h-5 w-5" />
              {data.current_stage_label || data.current_stage || "—"}
            </div>
          </Surface>
          <Surface title="Next stage" subtitle="Likely next lifecycle move">
            <div className="flex items-center gap-2 text-2xl font-semibold text-app-0">
              <ArrowRight className="h-5 w-5" />
              {paneChanged
                ? paneLabel(suggestedPane)
                : nextStagePane
                  ? paneLabel(nextStagePane)
                  : "Hold"}
            </div>
          </Surface>
          <Surface title="Top blocker" subtitle="What is holding movement">
            <div className="flex items-center gap-2 text-lg font-semibold text-app-0">
              <AlertTriangle className="h-5 w-5" />
              {topBlocker ? topBlocker.replace(/_/g, " ") : "No blocker"}
            </div>
          </Surface>
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
          <Surface
            title="Movement badges"
            subtitle="Automatic lifecycle movement cues"
          >
            <div className="flex flex-wrap gap-2">
              {movedToCompliance ? (
                <span className="oh-pill oh-pill-warn">
                  moved to compliance
                </span>
              ) : null}
              {movedToTenants ? (
                <span className="oh-pill oh-pill-accent">moved to tenants</span>
              ) : null}
              {movedToManagement ? (
                <span className="oh-pill oh-pill-good">
                  moved to management
                </span>
              ) : null}
              {!movedToCompliance && !movedToTenants && !movedToManagement ? (
                <span className="oh-pill">no pane move yet</span>
              ) : null}
            </div>
          </Surface>

          <Surface title="Blockers" subtitle="Normalized blocker set">
            {!(data.blockers || []).length ? (
              <EmptyState compact title="No blockers" />
            ) : (
              <div className="flex flex-wrap gap-2">
                {data.blockers?.map((b) => (
                  <span key={b} className="oh-pill oh-pill-warn">
                    {b.replace(/_/g, " ")}
                  </span>
                ))}
              </div>
            )}
          </Surface>

          <Surface title="Next actions" subtitle="What to do now">
            {!(data.next_actions || []).length ? (
              <EmptyState compact title="No next actions" />
            ) : (
              <div className="space-y-2">
                {data.next_actions?.map((action, idx) => (
                  <div
                    key={`${action}-${idx}`}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-3 text-sm text-app-2"
                  >
                    {action}
                  </div>
                ))}
              </div>
            )}
          </Surface>
        </div>

        <Surface
          title="Open pane workspace"
          subtitle="Jump directly into the owning queue"
        >
          <div className="flex flex-wrap gap-3">
            <Link
              to={`/panes/${currentPane}`}
              className="oh-btn oh-btn-secondary"
            >
              <LocateFixed className="h-4 w-4" />
              Current pane workspace
            </Link>
            {suggestedPane ? (
              <Link
                to={`/panes/${suggestedPane}`}
                className="oh-btn oh-btn-secondary"
              >
                <ClipboardCheck className="h-4 w-4" />
                Suggested pane workspace
              </Link>
            ) : null}
            <Link to="/dashboard" className="oh-btn oh-btn-secondary">
              <BadgeDollarSign className="h-4 w-4" />
              Portfolio dashboard
            </Link>
          </div>
        </Surface>
      </div>
    </PageShell>
  );
}
