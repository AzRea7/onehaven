import React from "react";
import { Link, useParams } from "react-router-dom";
import {
  AlertTriangle,
  ArrowRight,
  BadgeDollarSign,
  CheckCircle2,
  ClipboardCheck,
  Clock3,
  FileWarning,
  GitBranch,
  Home,
  LocateFixed,
  MapPinned,
  RefreshCcw,
  ShieldAlert,
  Users,
  Wallet,
  Building2,
  FileText,
  TriangleAlert,
  CheckCheck,
  GitCompareArrows,
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

type AcquisitionDetail = {
  property?: any;
  acquisition?: any;
  documents?: any[];
  required_documents?: Array<{
    kind?: string;
    label?: string;
    present?: boolean;
  }>;
  summary?: {
    days_to_close?: number | null;
    document_count?: number;
    required_documents_total?: number;
    required_documents_present?: number;
  };
};

type AcquisitionTagsPayload = {
  property_id?: number;
  tags?: string[];
  rows?: Array<{ tag?: string }>;
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

function waitingOnLabel(raw?: string) {
  const value = String(raw || "").trim();
  return value || "Nothing assigned";
}

function waitingOnCategory(
  raw?: string,
): "LENDER" | "TITLE" | "OPERATOR" | "SELLER" | "DOCUMENT" | "OTHER" {
  const text = String(raw || "")
    .trim()
    .toLowerCase();

  if (!text) return "OTHER";
  if (
    text.includes("lender") ||
    text.includes("loan") ||
    text.includes("finance")
  ) {
    return "LENDER";
  }
  if (text.includes("title") || text.includes("escrow")) {
    return "TITLE";
  }
  if (text.includes("seller")) {
    return "SELLER";
  }
  if (
    text.includes("document") ||
    text.includes("doc") ||
    text.includes("agreement") ||
    text.includes("inspection") ||
    text.includes("binder")
  ) {
    return "DOCUMENT";
  }
  if (
    text.includes("operator") ||
    text.includes("internal") ||
    text.includes("review") ||
    text.includes("team")
  ) {
    return "OPERATOR";
  }
  return "OTHER";
}

function daysToCloseTone(v?: number | null) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "text-app-0";
  if (n < 0) return "text-red-300";
  if (n <= 7) return "text-amber-300";
  return "text-emerald-300";
}

function urgencyLabel(days?: number | null, waitingOn?: string) {
  const n = Number(days);
  const text = String(waitingOn || "").toLowerCase();
  if (text.includes("blocked")) return "blocked";
  if (!Number.isFinite(n)) return "active";
  if (n < 0) return "overdue";
  if (n <= 7) return "due soon";
  return "on track";
}

function urgencyPillClass(label: string) {
  if (label === "overdue") return "oh-pill oh-pill-bad";
  if (label === "due soon") return "oh-pill oh-pill-warn";
  if (label === "blocked") return "oh-pill oh-pill-bad";
  return "oh-pill oh-pill-good";
}

function requiredDocsMissing(detail: AcquisitionDetail | null) {
  const rows = Array.isArray(detail?.required_documents)
    ? detail.required_documents
    : [];
  return rows.filter((x) => !x?.present);
}

function nextRequiredDocument(detail: AcquisitionDetail | null) {
  const missing = requiredDocsMissing(detail);
  if (missing.length) return missing[0]?.label || "Required document";
  return "No missing required documents";
}

function closeReadiness(detail: AcquisitionDetail | null) {
  const total = Number(detail?.summary?.required_documents_total || 0);
  const present = Number(detail?.summary?.required_documents_present || 0);
  const days = Number(detail?.summary?.days_to_close);
  const waiting = String(detail?.acquisition?.waiting_on || "").toLowerCase();

  let score = 0;

  if (total > 0) score += Math.round((present / total) * 55);
  if (detail?.summary?.document_count) {
    score += Math.min(Number(detail.summary.document_count) * 4, 20);
  }
  if (Number.isFinite(days)) {
    if (days > 14) score += 20;
    else if (days >= 7) score += 14;
    else if (days >= 0) score += 8;
    else score -= 12;
  }
  if (waiting.includes("document")) score -= 8;
  if (waiting.includes("blocked")) score -= 15;

  return Math.max(0, Math.min(100, score));
}

function readinessTone(score: number) {
  if (score >= 75) return "text-emerald-300";
  if (score >= 45) return "text-amber-300";
  return "text-red-300";
}

function collectConflicts(detail: AcquisitionDetail | null) {
  const documents = Array.isArray(detail?.documents) ? detail.documents : [];
  const fieldMap = new Map<
    string,
    Array<{ value: any; documentId: any; documentName: string }>
  >();

  for (const doc of documents) {
    const fields = doc?.extracted_fields || {};
    for (const [key, rawValue] of Object.entries(fields)) {
      const value = typeof rawValue === "string" ? rawValue.trim() : rawValue;
      if (value == null || value === "") continue;

      const arr = fieldMap.get(key) || [];
      arr.push({
        value,
        documentId: doc?.id,
        documentName:
          doc?.name || doc?.original_filename || `Document #${doc?.id ?? "?"}`,
      });
      fieldMap.set(key, arr);
    }
  }

  const conflicts: Array<{
    field: string;
    values: Array<{ value: any; documentId: any; documentName: string }>;
  }> = [];

  for (const [field, values] of fieldMap.entries()) {
    const normalized = new Set(
      values.map((x) => String(x.value).trim().toLowerCase()),
    );
    if (normalized.size > 1) {
      conflicts.push({ field, values });
    }
  }

  return conflicts;
}

function participantRows(detail: AcquisitionDetail | null) {
  const acquisition = detail?.acquisition || {};
  const contacts = Array.isArray(acquisition?.contacts)
    ? acquisition.contacts
    : [];
  return contacts;
}

function safeArray<T = any>(v: any): T[] {
  return Array.isArray(v) ? v : [];
}

export default function Property() {
  const { id } = useParams();
  const [data, setData] = React.useState<PropertyPayload | null>(null);
  const [acquisition, setAcquisition] =
    React.useState<AcquisitionDetail | null>(null);
  const [acquisitionTags, setAcquisitionTags] =
    React.useState<AcquisitionTagsPayload | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);

  const refresh = React.useCallback(async () => {
    if (!id) return;

    try {
      setLoading(true);

      let propertyPayload: PropertyPayload | null = null;

      try {
        propertyPayload = await api.get<PropertyPayload>(
          `/dashboard/property/${id}`,
        );
      } catch {
        propertyPayload = await api.get<PropertyPayload>(
          `/properties/${id}/view`,
        );
      }

      const [acquisitionDetailRes, tagsRes] = await Promise.allSettled([
        api.get<AcquisitionDetail>(`/acquisition/properties/${id}`),
        api.get<AcquisitionTagsPayload>(`/properties/${id}/acquisition-tags`),
      ]);

      setData(propertyPayload);

      if (acquisitionDetailRes.status === "fulfilled") {
        setAcquisition(acquisitionDetailRes.value);
      } else {
        setAcquisition(null);
      }

      if (tagsRes.status === "fulfilled") {
        setAcquisitionTags(tagsRes.value);
      } else {
        setAcquisitionTags(null);
      }

      setErr(null);
    } catch (e: any) {
      setErr(String(e?.message || e));
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

  const daysToClose = Number(acquisition?.summary?.days_to_close);
  const hasDaysToClose = Number.isFinite(daysToClose);
  const waitingOn = waitingOnLabel(acquisition?.acquisition?.waiting_on);
  const waitingCategory = waitingOnCategory(
    acquisition?.acquisition?.waiting_on,
  );
  const urgency = urgencyLabel(
    acquisition?.summary?.days_to_close,
    acquisition?.acquisition?.waiting_on,
  );
  const missingDocs = requiredDocsMissing(acquisition);
  const conflicts = collectConflicts(acquisition);
  const readiness = closeReadiness(acquisition);
  const participants = participantRows(acquisition);
  const tags =
    acquisitionTags?.tags ||
    safeArray(acquisitionTags?.rows)
      .map((row: any) => row?.tag)
      .filter(Boolean);

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
                    stage {data.current_stage_label || data.current_stage || "—"}
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

        <Surface
          title="What am I waiting on?"
          subtitle="This is the acquisition operator view of the deal: owner, urgency, missing document groups, parsed conflicts, and close readiness."
        >
          <div className="grid gap-4 xl:grid-cols-4">
            <div className="rounded-3xl border border-app bg-app-panel p-5">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Users className="h-3.5 w-3.5" />
                Waiting on
              </div>
              <div className="mt-3 text-lg font-semibold text-app-0">
                {waitingOn}
              </div>
              <div className="mt-2">
                <span className="oh-pill">{waitingCategory.toLowerCase()}</span>
              </div>
            </div>

            <div className="rounded-3xl border border-app bg-app-panel p-5">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <Clock3 className="h-3.5 w-3.5" />
                Close timing
              </div>
              <div
                className={`mt-3 text-lg font-semibold ${hasDaysToClose ? daysToCloseTone(daysToClose) : "text-app-0"}`}
              >
                {hasDaysToClose
                  ? daysToClose < 0
                    ? `${Math.abs(daysToClose)} days overdue`
                    : `${daysToClose} days remaining`
                  : "No target close date"}
              </div>
              <div className="mt-2">
                <span className={urgencyPillClass(urgency)}>{urgency}</span>
              </div>
            </div>

            <div className="rounded-3xl border border-app bg-app-panel p-5">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <FileText className="h-3.5 w-3.5" />
                Next required document
              </div>
              <div className="mt-3 text-lg font-semibold text-app-0">
                {nextRequiredDocument(acquisition)}
              </div>
              <div className="mt-2">
                <span className="oh-pill">
                  {missingDocs.length} missing groups
                </span>
              </div>
            </div>

            <div className="rounded-3xl border border-app bg-app-panel p-5">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                <CheckCheck className="h-3.5 w-3.5" />
                Estimated close readiness
              </div>
              <div
                className={`mt-3 text-lg font-semibold ${readinessTone(readiness)}`}
              >
                {readiness}%
              </div>
              <div className="mt-2">
                <span className="oh-pill">
                  {Number(
                    acquisition?.summary?.required_documents_present || 0,
                  )}
                  /{Number(acquisition?.summary?.required_documents_total || 0)}{" "}
                  required docs
                </span>
              </div>
            </div>
          </div>

          {tags?.length ? (
            <div className="mt-4 flex flex-wrap gap-2">
              {tags.map((tag) => (
                <span key={tag} className="oh-pill oh-pill-accent">
                  {tag}
                </span>
              ))}
            </div>
          ) : null}
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

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <Surface
            title="Missing document groups"
            subtitle="These are the required document groups still missing from the deal file."
          >
            {missingDocs.length === 0 ? (
              <EmptyState compact title="No missing document groups" />
            ) : (
              <div className="space-y-3">
                {missingDocs.map((doc, idx) => (
                  <div
                    key={`${doc.kind || doc.label || "missing"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                  >
                    <div className="flex items-center gap-2 text-sm font-medium text-app-0">
                      <FileWarning className="h-4 w-4" />
                      {doc.label || doc.kind || "Required document"}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Surface>

          <Surface
            title="Parsed values that disagree"
            subtitle="Conflicts across uploaded documents that need operator review."
          >
            {conflicts.length === 0 ? (
              <EmptyState compact title="No parsed conflicts detected" />
            ) : (
              <div className="space-y-4">
                {conflicts.map((conflict) => (
                  <div
                    key={conflict.field}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                  >
                    <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                      <GitCompareArrows className="h-4 w-4" />
                      {conflict.field.replace(/_/g, " ")}
                    </div>
                    <div className="mt-3 space-y-2">
                      {conflict.values.map((value, idx) => (
                        <div
                          key={`${conflict.field}-${idx}`}
                          className="rounded-2xl border border-app bg-app-muted px-3 py-3 text-sm"
                        >
                          <div className="font-medium text-app-0">
                            {String(value.value)}
                          </div>
                          <div className="mt-1 text-xs text-app-4">
                            {value.documentName}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Surface>
        </div>

        <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
          <Surface
            title="Participants"
            subtitle="Who is currently involved in the acquisition workflow."
          >
            {participants.length === 0 ? (
              <EmptyState compact title="No participants recorded" />
            ) : (
              <div className="space-y-3">
                {participants.map((person: any, idx: number) => (
                  <div
                    key={`${person?.email || person?.name || "participant"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                  >
                    <div className="flex items-center gap-2 text-sm font-medium text-app-0">
                      <Users className="h-4 w-4" />
                      {person?.name || person?.full_name || "Unnamed contact"}
                    </div>
                    <div className="mt-1 text-xs text-app-4">
                      [
                        person?.role,
                        person?.company,
                        person?.email,
                        person?.phone,
                      ]
                        .filter(Boolean)
                        .join(" · "){"}"}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Surface>

          <Surface
            title="Document stack"
            subtitle="Stored acquisition documents and parser/scanner state."
          >
            {!safeArray(acquisition?.documents).length ? (
              <EmptyState compact title="No documents uploaded" />
            ) : (
              <div className="space-y-3">
                {safeArray(acquisition?.documents)
                  .slice(0, 8)
                  .map((doc: any) => (
                    <div
                      key={doc?.id}
                      className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                    >
                      <div className="flex flex-wrap items-start justify-between gap-2">
                        <div>
                          <div className="text-sm font-medium text-app-0">
                            {doc?.name ||
                              doc?.original_filename ||
                              `Document #${doc?.id}`}
                          </div>
                          <div className="mt-1 text-xs text-app-4">
                            [
                              doc?.kind,
                              doc?.parse_status,
                              doc?.scan_status,
                              doc?.status,
                            ]
                              .filter(Boolean)
                              .join(" · "){"}"}
                          </div>
                        </div>
                        <div className="flex flex-wrap gap-2">
                          {doc?.parse_status ? (
                            <span className="oh-pill">{doc.parse_status}</span>
                          ) : null}
                          {doc?.scan_status === "clean" ? (
                            <span className="oh-pill oh-pill-good">clean</span>
                          ) : doc?.scan_status ? (
                            <span className="oh-pill oh-pill-warn">
                              {doc.scan_status}
                            </span>
                          ) : null}
                        </div>
                      </div>
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
            <Link to="/panes/acquisition" className="oh-btn oh-btn-secondary">
              <Building2 className="h-4 w-4" />
              Acquisition queue
            </Link>
          </div>
        </Surface>
      </div>
    </PageShell>
  );
}