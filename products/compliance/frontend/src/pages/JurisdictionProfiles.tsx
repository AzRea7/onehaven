import React from "react";
import PageHero from "onehaven_onehaven_platform/frontend/src/components/PageHero";
import PageShell from "onehaven_onehaven_platform/frontend/src/components/PageShell";
import GlassCard from "onehaven_onehaven_platform/frontend/src/components/GlassCard";
import JurisdictionCoverageBadge from "products/compliance/frontend/src/components/JurisdictionCoverageBadge";
import { api } from "../lib/api";

type ReviewQueuePayload = {
  ok?: boolean;
  count?: number;
  severity_counts?: Record<string, number>;
  entries?: any[];
};

type StaleReviewDashboardPayload = {
  ok?: boolean;
  filter?: string;
  count?: number;
  summary?: Record<string, number>;
  rows?: any[];
};

type JurisdictionVisibilityPayload = {
  ok?: boolean;
  jurisdiction_profile_id?: number;
  resolved_profile?: any | null;
  coverage_matrix?: any | null;
  health?: any | null;
  operational_status?: any | null;
};

type ProfileRow = {
  id: number;
  org_id?: number | null;
  scope?: string | null;
  state: string;
  county?: string | null;
  city?: string | null;
  friction_multiplier?: number | null;
  pha_name?: string | null;
  notes?: string | null;
  policy?: Record<string, any> | null;
  policy_json?: Record<string, any> | string | null;
  coverage_confidence?: string | null;
  confidence_label?: string | null;
  production_readiness?: string | null;
  resolved_rule_version?: string | null;
  rule_version?: string | null;
  last_refreshed?: string | null;
  last_refreshed_at?: string | null;
  source_evidence?: any[] | null;
  evidence?: any[] | null;
  resolved_layers?: any[] | null;
  layers?: any[] | null;
  completeness?: {
    completeness_status?: string | null;
    completeness_score?: number | null;
    is_stale?: boolean | null;
    stale_reason?: string | null;
    required_categories?: string[];
    covered_categories?: string[];
    missing_categories?: string[];
  } | null;
  tasks?: any[] | null;
  required_categories?: string[] | null;
  covered_categories?: string[] | null;
  missing_categories?: string[] | null;
  completeness_status?: string | null;
  completeness_score?: number | null;
  is_stale?: boolean | null;
  stale_reason?: string | null;
  operational_status?: any | null;
  health?: any | null;
  safe_to_rely_on?: boolean | null;
  lockout_causing_categories?: string[] | null;
  informational_gap_categories?: string[] | null;
  validation_pending_categories?: string[] | null;
  authority_gap_categories?: string[] | null;
  last_validation_at?: string | null;
  next_due_step?: string | null;
};

function pretty(v: any) {
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}

function norm(v: any) {
  return String(v ?? "")
    .trim()
    .toLowerCase();
}

function titleize(v: any) {
  return String(v ?? "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function inferScope(row: ProfileRow): "org" | "global" {
  if (row.scope === "org" || row.org_id) return "org";
  return "global";
}

function policyObject(row: ProfileRow): Record<string, any> {
  if (row.policy && typeof row.policy === "object") return row.policy;
  if (row.policy_json && typeof row.policy_json === "object") {
    return row.policy_json as Record<string, any>;
  }
  if (typeof row.policy_json === "string") {
    try {
      const parsed = JSON.parse(row.policy_json);
      if (parsed && typeof parsed === "object") return parsed;
    } catch {
      return {};
    }
  }
  return {};
}

function completenessFromRow(row: ProfileRow) {
  const c = row.completeness || {};
  return {
    completeness_status:
      c.completeness_status || row.completeness_status || "missing",
    completeness_score:
      typeof c.completeness_score === "number"
        ? c.completeness_score
        : (row.completeness_score ?? 0),
    is_stale:
      typeof c.is_stale === "boolean" ? c.is_stale : Boolean(row.is_stale),
    stale_reason: c.stale_reason || row.stale_reason || null,
    required_categories: c.required_categories || row.required_categories || [],
    covered_categories: c.covered_categories || row.covered_categories || [],
    missing_categories: c.missing_categories || row.missing_categories || [],
  };
}

function scorePct(v: any) {
  const n = Number(v ?? 0);
  if (!Number.isFinite(n)) return "0%";
  return `${Math.round(n * 100)}%`;
}

function formatDate(v: any) {
  if (!v) return "—";
  const d = new Date(String(v));
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString();
}

function Badge({
  children,
  tone = "neutral",
}: {
  children: React.ReactNode;
  tone?: "neutral" | "good" | "warn" | "bad";
}) {
  const cls =
    tone === "good"
      ? "border-emerald-400/25 bg-emerald-400/10 text-emerald-200"
      : tone === "warn"
        ? "border-amber-300/25 bg-amber-300/10 text-amber-100"
        : tone === "bad"
          ? "border-red-400/25 bg-red-400/10 text-red-200"
          : "border-white/10 bg-white/5 text-white/75";

  return (
    <span
      className={`inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] ${cls}`}
    >
      {children}
    </span>
  );
}

function SectionTitle({
  title,
  right,
}: {
  title: string;
  right?: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between gap-3">
      <div className="text-sm font-semibold text-white">{title}</div>
      {right ? <div>{right}</div> : null}
    </div>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-4 text-sm">
      <div className="text-white/55">{label}</div>
      <div className="text-right text-white/85">{value}</div>
    </div>
  );
}

function BoundaryPanel({
  title,
  body,
  tone = "warn",
}: {
  title: string;
  body: React.ReactNode;
  tone?: "warn" | "bad" | "good";
}) {
  const cls =
    tone === "bad"
      ? "border-red-400/25 bg-red-500/10 text-red-100"
      : tone === "good"
        ? "border-emerald-400/25 bg-emerald-500/10 text-emerald-100"
        : "border-amber-300/25 bg-amber-500/10 text-amber-100";
  return (
    <div className={`rounded-2xl border px-4 py-4 ${cls}`}>
      <div className="text-sm font-semibold">{title}</div>
      <div className="mt-2 text-sm leading-6">{body}</div>
    </div>
  );
}

function profileKey(
  row: Pick<ProfileRow, "state" | "county" | "city" | "pha_name" | "id">,
) {
  return `${row.id}:${norm(row.state)}|${norm(row.county)}|${norm(row.city)}|${norm(row.pha_name)}`;
}

function completenessTone(v?: string | null) {
  const s = norm(v);
  if (s === "complete") return "good";
  if (s === "partial") return "warn";
  return "bad";
}

function confidenceTone(v?: string | null) {
  const s = norm(v);
  if (s === "high" || s === "strong" || s === "verified") return "good";
  if (s === "medium" || s === "partial" || s === "unknown") return "warn";
  if (s === "low" || s === "weak") return "bad";
  return "neutral";
}

function toneForLayer(layer: any) {
  const s = norm(
    layer?.confidence || layer?.status || (layer?.applied ? "applied" : ""),
  );
  if (["applied", "verified", "high", "strong"].includes(s)) return "good";
  if (["partial", "medium", "unknown"].includes(s)) return "warn";
  if (["low", "missing", "stale"].includes(s)) return "bad";
  return "neutral";
}

export default function JurisdictionProfiles() {
  const [includeGlobal, setIncludeGlobal] = React.useState(true);
  const [state, setState] = React.useState("MI");
  const [rows, setRows] = React.useState<ProfileRow[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const [query, setQuery] = React.useState("");
  const [selectedId, setSelectedId] = React.useState<number | null>(null);
  const [selectedVisibility, setSelectedVisibility] =
    React.useState<JurisdictionVisibilityPayload | null>(null);

  const [testCity, setTestCity] = React.useState("Detroit");
  const [testCounty, setTestCounty] = React.useState("Wayne");
  const [resolved, setResolved] = React.useState<any | null>(null);
  const [reviewQueue, setReviewQueue] =
    React.useState<ReviewQueuePayload | null>(null);
  const [overrideRows, setOverrideRows] = React.useState<any[]>([]);
  const [overrideReason, setOverrideReason] = React.useState("");
  const [overrideRuleCategory, setOverrideRuleCategory] = React.useState("");
  const [overrideCritical, setOverrideCritical] = React.useState(false);
  const [resolveBusy, setResolveBusy] = React.useState(false);

  const [city, setCity] = React.useState("");
  const [county, setCounty] = React.useState("");
  const [friction, setFriction] = React.useState<number>(1.0);
  const [phaName, setPhaName] = React.useState("");
  const [policyJson, setPolicyJson] = React.useState(
    pretty({
      summary: "Org override profile.",
      compliance: {
        registration_required: "unknown",
        inspection_required: "unknown",
        certificate_required_before_occupancy: "unknown",
      },
      voucher: {
        landlord_packet_required: "unknown",
        hap_contract_and_tenancy_addendum_required: "unknown",
      },
      inspections: {
        reinspection_required_after_fail: "unknown",
      },
      notes: ["Replace this with verified operational reality."],
    }),
  );
  const [notes, setNotes] = React.useState("");
  const [saveBusy, setSaveBusy] = React.useState(false);
  const [recomputeBusyId, setRecomputeBusyId] = React.useState<number | null>(
    null,
  );
  const [staleFilter, setStaleFilter] = React.useState("all");
  const [staleDashboard, setStaleDashboard] =
    React.useState<StaleReviewDashboardPayload | null>(null);

  React.useEffect(() => {
    let cancelled = false;
    async function loadOverrides() {
      if (!selectedId) {
        setOverrideRows([]);
        return;
      }
      try {
        const out = await api.get(
          `/jurisdictions/${selectedId}/overrides?include_inactive=true`,
        );
        if (!cancelled)
          setOverrideRows(Array.isArray(out?.items) ? out.items : []);
      } catch {
        if (!cancelled) setOverrideRows([]);
      }
    }
    void loadOverrides();
    return () => {
      cancelled = true;
    };
  }, [selectedId]);

  async function createOverrideForSelected() {
    if (!selectedId || !overrideReason.trim()) return;
    await api.post(`/jurisdictions/${selectedId}/overrides`, {
      reason: overrideReason.trim(),
      rule_category: overrideRuleCategory || null,
      carrying_critical_rule: overrideCritical,
      trust_impact: overrideCritical ? "review_required" : "reduced_confidence",
    });
    setOverrideReason("");
    setOverrideRuleCategory("");
    setOverrideCritical(false);
    const out = await api.get(
      `/jurisdictions/${selectedId}/overrides?include_inactive=true`,
    );
    setOverrideRows(Array.isArray(out?.items) ? out.items : []);
  }

  const selected = React.useMemo(
    () => rows.find((r) => r.id === selectedId) ?? null,
    [rows, selectedId],
  );

  const filteredRows = React.useMemo(() => {
    const q = norm(query);
    return rows.filter((r) => {
      if (!q) return true;
      const completeness = completenessFromRow(r);
      const hay = [
        r.scope,
        inferScope(r),
        r.state,
        r.county,
        r.city,
        r.pha_name,
        r.notes,
        r.coverage_confidence,
        r.confidence_label,
        completeness.completeness_status,
        completeness.stale_reason,
        ...(completeness.missing_categories || []),
      ]
        .map(norm)
        .join(" ");
      return hay.includes(q);
    });
  }, [rows, query]);

  const stats = React.useMemo(() => {
    const total = rows.length;
    const org = rows.filter((r) => inferScope(r) === "org").length;
    const global = rows.filter((r) => inferScope(r) === "global").length;
    const city = rows.filter((r) => !!r.city).length;
    const county = rows.filter((r) => !r.city && !!r.county).length;
    const incomplete = rows.filter(
      (r) => completenessFromRow(r).completeness_status !== "complete",
    ).length;
    const stale = rows.filter((r) => completenessFromRow(r).is_stale).length;
    const weak = rows.filter((r) =>
      ["low", "partial", "medium", "unknown"].includes(
        norm(r.coverage_confidence || r.confidence_label),
      ),
    ).length;
    return { total, org, global, city, county, incomplete, stale, weak };
  }, [rows]);

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const [data, queue] = await Promise.all([
        api.listJurisdictionProfiles(includeGlobal, state),
        api.get(
          `/jurisdictions/review-queue?state=${encodeURIComponent(state)}`,
        ),
      ]);
      const list = Array.isArray(data) ? data : [];
      setReviewQueue(queue || null);
      setRows(list);
      if (list.length > 0) {
        const stillExists = selectedId && list.some((r) => r.id === selectedId);
        setSelectedId(stillExists ? selectedId : list[0].id);
      } else {
        setSelectedId(null);
      }
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function runResolve() {
    setResolved(null);
    setError(null);
    setResolveBusy(true);
    try {
      const out = await api.resolveJurisdictionProfile({
        city: testCity.trim() || null,
        county: testCounty.trim() || null,
        state,
      });
      setResolved(out);
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setResolveBusy(false);
    }
  }

  function clearForm() {
    setCity("");
    setCounty("");
    setFriction(1.0);
    setPhaName("");
    setNotes("");
    setPolicyJson(
      pretty({
        summary: "Org override profile.",
        compliance: {
          registration_required: "unknown",
          inspection_required: "unknown",
          certificate_required_before_occupancy: "unknown",
        },
        voucher: {
          landlord_packet_required: "unknown",
          hap_contract_and_tenancy_addendum_required: "unknown",
        },
        inspections: {
          reinspection_required_after_fail: "unknown",
        },
        notes: ["Replace this with verified operational reality."],
      }),
    );
  }

  function loadIntoForm(row: ProfileRow) {
    const policy = policyObject(row);
    setCity(row.city || "");
    setCounty(row.county || "");
    setFriction(Number(row.friction_multiplier ?? 1.0));
    setPhaName(row.pha_name || "");
    setNotes(row.notes || "");
    setPolicyJson(pretty(policy));
  }

  async function saveProfile() {
    setError(null);
    let policy: any = {};
    try {
      policy = policyJson ? JSON.parse(policyJson) : {};
    } catch {
      setError("policy_json is not valid JSON");
      return;
    }

    setSaveBusy(true);
    try {
      await api.upsertJurisdictionProfile({
        state,
        city: city.trim() || null,
        county: county.trim() || null,
        friction_multiplier: Number(friction || 1.0),
        pha_name: phaName.trim() || null,
        policy,
        notes: notes.trim() || null,
      });
      await refresh();
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setSaveBusy(false);
    }
  }

  async function deleteOne(r: ProfileRow) {
    setError(null);
    try {
      await api.deleteJurisdictionProfile({
        state: r.state || state,
        city: r.city || null,
        county: r.county || null,
      });
      await refresh();
    } catch (e: any) {
      setError(String(e?.message || e));
    }
  }

  async function recomputeOne(r: ProfileRow) {
    setError(null);
    setRecomputeBusyId(r.id);
    try {
      await api.recomputeJurisdictionProfile(r.id);
      await refresh();
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setRecomputeBusyId(null);
    }
  }

  React.useEffect(() => {
    refresh();
  }, [includeGlobal, state, staleFilter]);

  React.useEffect(() => {
    if (!selected?.id) {
      setSelectedVisibility(null);
      return;
    }

    let cancelled = false;
    api
      .get<JurisdictionVisibilityPayload>(
        `/jurisdictions/${selected.id}/visibility`,
      )
      .then((payload) => {
        if (!cancelled) setSelectedVisibility(payload || null);
      })
      .catch(() => {
        if (!cancelled) setSelectedVisibility(null);
      });

    return () => {
      cancelled = true;
    };
  }, [selected?.id]);

  const selectedCompleteness = selected ? completenessFromRow(selected) : null;
  const selectedTasks = Array.isArray(selected?.tasks) ? selected.tasks : [];
  const selectedLayers = Array.isArray(selected?.resolved_layers)
    ? selected?.resolved_layers
    : Array.isArray(selected?.layers)
      ? selected?.layers
      : [];
  const selectedEvidence = Array.isArray(selected?.source_evidence)
    ? selected?.source_evidence
    : Array.isArray(selected?.evidence)
      ? selected?.evidence
      : [];
  const selectedPolicy = selected ? policyObject(selected) : {};
  const selectedOperationalStatus =
    selectedVisibility?.operational_status ||
    selectedVisibility?.resolved_profile?.operational_status ||
    null;
  const selectedSourceSummary =
    selectedOperationalStatus?.source_summary || null;
  const selectedNextActions = selectedOperationalStatus?.next_actions || null;
  const selectedLockout = selectedOperationalStatus?.lockout || null;
  const selectedUnsafeReasons = Array.isArray(
    selectedOperationalStatus?.reasons,
  )
    ? selectedOperationalStatus?.reasons
    : [];

  const staleDashboardRows = Array.isArray(staleDashboard?.rows)
    ? staleDashboard.rows
    : [];
  const staleDashboardSummary = staleDashboard?.summary || {};
  const staleFilterOptions = [
    "all",
    "blocked",
    "degraded",
    "stale",
    "review-required",
    "missing-proof",
  ];

  return (
    <PageShell className="space-y-6">
      <PageHero
        eyebrow="Operational modeling"
        title="Jurisdiction Profiles"
        subtitle="Profiles are the modeled output layer for layered statewide, county, city, housing authority, and org override rules."
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <button
              onClick={refresh}
              className="rounded-2xl border border-white/15 bg-white/10 px-4 py-2 text-sm text-white hover:bg-white/15 disabled:opacity-50"
              disabled={loading}
            >
              {loading ? "Loading…" : "Refresh"}
            </button>
            <a
              href="/jurisdictions"
              className="rounded-2xl border border-cyan-400/25 bg-cyan-500/12 px-4 py-2 text-sm text-cyan-100 hover:bg-cyan-500/18"
            >
              Open pipeline workspace
            </a>
          </div>
        }
      />

      <GlassCard>
        <div className="flex flex-col gap-4">
          <SectionTitle
            title="Stale-review dashboard"
            right={
              <div className="flex flex-wrap items-center gap-2">
                {staleFilterOptions.map((item) => (
                  <button
                    key={item}
                    onClick={() => setStaleFilter(item)}
                    className={[
                      "rounded-full border px-3 py-1.5 text-xs",
                      staleFilter === item
                        ? "border-cyan-400/30 bg-cyan-500/15 text-cyan-100"
                        : "border-white/10 bg-white/[0.04] text-white/70",
                    ].join(" ")}
                  >
                    {titleize(item)}
                    {typeof staleDashboardSummary[item.replace("-", "_")] ===
                    "number"
                      ? ` · ${staleDashboardSummary[item.replace("-", "_")]}`
                      : ""}
                  </button>
                ))}
              </div>
            }
          />
          <div className="grid gap-3 md:grid-cols-6">
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-white/80">
              <div className="text-white/55">Blocked</div>
              <div className="mt-2 text-2xl font-semibold text-red-200">
                {Number(staleDashboardSummary.blocked || 0)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-white/80">
              <div className="text-white/55">Degraded</div>
              <div className="mt-2 text-2xl font-semibold text-amber-100">
                {Number(staleDashboardSummary.degraded || 0)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-white/80">
              <div className="text-white/55">Review required</div>
              <div className="mt-2 text-2xl font-semibold text-amber-100">
                {Number(staleDashboardSummary.review_required || 0)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-white/80">
              <div className="text-white/55">Stale</div>
              <div className="mt-2 text-2xl font-semibold text-amber-100">
                {Number(staleDashboardSummary.stale || 0)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-white/80">
              <div className="text-white/55">Missing proof</div>
              <div className="mt-2 text-2xl font-semibold text-red-100">
                {Number(staleDashboardSummary.missing_proof || 0)}
              </div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-white/80">
              <div className="text-white/55">Rows</div>
              <div className="mt-2 text-2xl font-semibold text-white">
                {Number(staleDashboard?.count || 0)}
              </div>
            </div>
          </div>

          {!staleDashboardRows.length ? (
            <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4 text-sm text-white/60">
              No jurisdictions currently match this stale-review filter.
            </div>
          ) : (
            <div className="space-y-3">
              {staleDashboardRows.slice(0, 20).map((row: any) => (
                <div
                  key={`stale-${row.jurisdiction_profile_id}`}
                  className="rounded-2xl border border-white/10 bg-black/30 p-4"
                >
                  <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                    <div>
                      <div className="text-sm font-semibold text-white">
                        {row.city || row.county || row.pha_name || row.state}
                      </div>
                      <div className="mt-1 text-xs text-white/50">
                        {[row.city, row.county, row.state, row.pha_name]
                          .filter(Boolean)
                          .join(" • ")}
                      </div>
                      <div className="mt-3 flex flex-wrap gap-2">
                        <Badge
                          tone={
                            row.health_status === "blocked"
                              ? "bad"
                              : row.health_status === "degraded"
                                ? "warn"
                                : "neutral"
                          }
                        >
                          {titleize(row.health_status || "unknown")}
                        </Badge>
                        <Badge tone={row.safe_to_rely_on ? "good" : "bad"}>
                          {row.safe_to_rely_on
                            ? "Safe to rely on"
                            : "Not safe now"}
                        </Badge>
                        {(row.tags || []).map((tag: string) => (
                          <Badge
                            key={tag}
                            tone={
                              tag === "blocked" || tag === "missing-proof"
                                ? "bad"
                                : "warn"
                            }
                          >
                            {titleize(tag)}
                          </Badge>
                        ))}
                      </div>
                    </div>
                    <div className="grid gap-2 text-sm text-white/75 xl:min-w-[360px]">
                      <Row
                        label="Why due now"
                        value={
                          (row.why_due_now || []).join(", ") ||
                          row.operational_reason ||
                          "—"
                        }
                      />
                      <Row
                        label="What to do next"
                        value={titleize(
                          row.what_to_do_next || "review_jurisdiction_state",
                        )}
                      />
                      <Row
                        label="Last successful refresh"
                        value={formatDate(row.last_refresh_success_at)}
                      />
                      <Row
                        label="Last validation"
                        value={formatDate(row.last_validation_at)}
                      />
                    </div>
                  </div>
                  <div className="mt-3 grid gap-3 md:grid-cols-4">
                    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
                      <div className="text-[11px] uppercase tracking-wider text-white/45">
                        Stale authoritative
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {(row.stale_authoritative_categories || []).length ? (
                          (row.stale_authoritative_categories || []).map(
                            (item: string) => (
                              <Badge key={item} tone="warn">
                                {titleize(item)}
                              </Badge>
                            ),
                          )
                        ) : (
                          <span className="text-sm text-white/55">None</span>
                        )}
                      </div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
                      <div className="text-[11px] uppercase tracking-wider text-white/45">
                        Conflicts
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {(row.conflicting_categories || []).length ? (
                          (row.conflicting_categories || []).map(
                            (item: string) => (
                              <Badge key={item} tone="bad">
                                {titleize(item)}
                              </Badge>
                            ),
                          )
                        ) : (
                          <span className="text-sm text-white/55">None</span>
                        )}
                      </div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
                      <div className="text-[11px] uppercase tracking-wider text-white/45">
                        Validation pending
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {(row.validation_pending_categories || []).length ? (
                          (row.validation_pending_categories || []).map(
                            (item: string) => (
                              <Badge key={item} tone="warn">
                                {titleize(item)}
                              </Badge>
                            ),
                          )
                        ) : (
                          <span className="text-sm text-white/55">None</span>
                        )}
                      </div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
                      <div className="text-[11px] uppercase tracking-wider text-white/45">
                        Blocking / missing proof
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {(row.lockout_causing_categories || [])
                          .concat(row.authority_gap_categories || [])
                          .concat(row.missing_categories || []).length ? (
                          (row.lockout_causing_categories || [])
                            .concat(row.authority_gap_categories || [])
                            .concat(row.missing_categories || [])
                            .slice(0, 8)
                            .map((item: string) => (
                              <Badge key={item} tone="bad">
                                {titleize(item)}
                              </Badge>
                            ))
                        ) : (
                          <span className="text-sm text-white/55">None</span>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </GlassCard>

      <GlassCard>
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="text-sm font-semibold text-white">
              Coverage and governance
            </div>
            <div className="mt-1 text-sm text-white/65">
              Profiles surface completeness, confidence, stale warnings, missing
              categories, rule versioning, and evidence so operators know where
              local coverage is strong versus partial.
            </div>
          </div>
          <div>
            <a
              href="/jurisdictions"
              className="inline-flex rounded-xl border border-cyan-400/25 bg-cyan-500/12 px-4 py-2 text-sm text-cyan-100 hover:bg-cyan-500/18"
            >
              Go to Jurisdictions
            </a>
          </div>
        </div>
      </GlassCard>

      {error ? (
        <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-200">
          {error}
        </div>
      ) : null}

      <BoundaryPanel
        title="Operator trust boundary"
        body={
          <>
            These jurisdiction profiles are operational trust views. They help
            you understand freshness, authority, proof, and review state, but
            they do <strong>not</strong> replace legal advice. A profile marked
            safe to rely on operationally still requires you to verify critical
            requirements with the authoritative jurisdiction source before
            treating it as legal clearance.
          </>
        }
      />

      <div className="grid grid-cols-1 gap-4 md:grid-cols-8">
        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Profiles
          </div>
          <div className="mt-2 text-2xl font-semibold text-white">
            {stats.total}
          </div>
        </GlassCard>
        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Org overrides
          </div>
          <div className="mt-2 text-2xl font-semibold text-amber-100">
            {stats.org}
          </div>
        </GlassCard>
        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Global defaults
          </div>
          <div className="mt-2 text-2xl font-semibold text-white">
            {stats.global}
          </div>
        </GlassCard>
        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            City scoped
          </div>
          <div className="mt-2 text-2xl font-semibold text-white">
            {stats.city}
          </div>
        </GlassCard>
        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            County scoped
          </div>
          <div className="mt-2 text-2xl font-semibold text-white">
            {stats.county}
          </div>
        </GlassCard>
        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Incomplete
          </div>
          <div className="mt-2 text-2xl font-semibold text-amber-100">
            {stats.incomplete}
          </div>
        </GlassCard>
        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Stale
          </div>
          <div className="mt-2 text-2xl font-semibold text-amber-100">
            {stats.stale}
          </div>
        </GlassCard>
        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Weak / partial
          </div>
          <div className="mt-2 text-2xl font-semibold text-amber-100">
            {stats.weak}
          </div>
        </GlassCard>
      </div>

      <GlassCard>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-[120px_180px_1fr]">
          <div>
            <div className="mb-1 text-xs uppercase tracking-wider text-white/45">
              State
            </div>
            <input
              value={state}
              onChange={(e) => setState(e.target.value.toUpperCase())}
              className="w-full rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm text-white"
            />
          </div>

          <div className="flex items-end">
            <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm text-white/75">
              <input
                type="checkbox"
                checked={includeGlobal}
                onChange={(e) => setIncludeGlobal(e.target.checked)}
                className="h-4 w-4"
              />
              include global defaults
            </label>
          </div>

          <div>
            <div className="mb-1 text-xs uppercase tracking-wider text-white/45">
              Search
            </div>
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search city, county, scope, confidence, notes, completeness, stale reason…"
              className="w-full rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm text-white placeholder:text-white/35"
            />
          </div>
        </div>
      </GlassCard>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[420px_1fr]">
        <div className="space-y-4">
          <GlassCard>
            <SectionTitle title="Resolve tester" />
            <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
              <input
                value={testCity}
                onChange={(e) => setTestCity(e.target.value)}
                placeholder="City (optional)"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
              <input
                value={testCounty}
                onChange={(e) => setTestCounty(e.target.value)}
                placeholder="County (optional)"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
            </div>

            <div className="mt-3 flex items-center gap-3">
              <button
                onClick={runResolve}
                className="rounded-xl border border-indigo-400/30 bg-indigo-500/20 px-3 py-2 text-sm text-white transition hover:bg-indigo-500/25"
                disabled={resolveBusy}
              >
                {resolveBusy ? "Resolving…" : "Resolve"}
              </button>
            </div>

            {resolved ? (
              <div className="mt-4 space-y-3">
                <div className="flex flex-wrap gap-2">
                  <Badge tone={resolved?.matched ? "good" : "bad"}>
                    {resolved?.matched ? "matched" : "not matched"}
                  </Badge>
                  <Badge>{resolved?.scope || "—"} scope</Badge>
                  <Badge>{resolved?.match_level || "—"} level</Badge>
                  {resolved?.coverage_confidence ||
                  resolved?.confidence_label ? (
                    <Badge
                      tone={confidenceTone(
                        resolved?.coverage_confidence ||
                          resolved?.confidence_label,
                      )}
                    >
                      {titleize(
                        resolved?.coverage_confidence ||
                          resolved?.confidence_label,
                      )}
                    </Badge>
                  ) : null}
                </div>

                <div className="rounded-xl border border-white/10 bg-black/30 p-3">
                  <div className="space-y-2">
                    <Row
                      label="Profile ID"
                      value={resolved?.profile_id ?? "—"}
                    />
                    <Row
                      label="Friction"
                      value={resolved?.friction_multiplier ?? 1.0}
                    />
                    <Row label="PHA" value={resolved?.pha_name || "—"} />
                    <Row
                      label="Rule version"
                      value={
                        resolved?.resolved_rule_version ||
                        resolved?.rule_version ||
                        "—"
                      }
                    />
                    <Row
                      label="Last refreshed"
                      value={formatDate(
                        resolved?.last_refreshed || resolved?.last_refreshed_at,
                      )}
                    />
                  </div>
                </div>

                <pre className="overflow-auto rounded-xl border border-white/10 bg-black/30 p-3 text-xs text-white/80 whitespace-pre-wrap">
                  {pretty(resolved)}
                </pre>
              </div>
            ) : null}
          </GlassCard>

          <GlassCard>
            <SectionTitle
              title="Profiles"
              right={
                <div className="text-xs text-white/60">
                  {filteredRows.length} rows
                </div>
              }
            />

            <div className="mt-3 space-y-2">
              {filteredRows.length === 0 ? (
                <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4 text-sm text-white/60">
                  No profiles found.
                </div>
              ) : (
                filteredRows.map((r) => {
                  const scope = inferScope(r);
                  const active = selectedId === r.id;
                  const completeness = completenessFromRow(r);
                  return (
                    <button
                      key={profileKey(r)}
                      onClick={() => setSelectedId(r.id)}
                      className={[
                        "w-full rounded-2xl border p-4 text-left transition",
                        active
                          ? "border-white/20 bg-white/[0.08]"
                          : "border-white/10 bg-white/[0.03] hover:bg-white/[0.05] hover:border-white/[0.14]",
                      ].join(" ")}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="text-sm font-semibold text-white">
                            {r.city || r.county || r.pha_name || r.state}
                          </div>
                          <div className="mt-1 text-xs text-white/50">
                            {r.city
                              ? `${r.county || "—"} • ${r.state}`
                              : r.county
                                ? `${r.state}`
                                : `${r.state} baseline`}
                          </div>
                        </div>

                        <div className="flex flex-col items-end gap-1">
                          <Badge tone={scope === "org" ? "warn" : "neutral"}>
                            {scope}
                          </Badge>
                          <Badge
                            tone={completenessTone(
                              completeness.completeness_status,
                            )}
                          >
                            {titleize(completeness.completeness_status)}
                          </Badge>
                        </div>
                      </div>

                      <div className="mt-3 flex flex-wrap gap-2">
                        <Badge
                          tone={confidenceTone(
                            r.coverage_confidence || r.confidence_label,
                          )}
                        >
                          {titleize(
                            r.coverage_confidence ||
                              r.confidence_label ||
                              "unknown",
                          )}
                        </Badge>
                        {completeness.is_stale ? (
                          <Badge tone="warn">stale</Badge>
                        ) : (
                          <Badge tone="good">fresh</Badge>
                        )}
                        {Array.isArray(completeness.missing_categories) &&
                        completeness.missing_categories.length > 0 ? (
                          <Badge tone="warn">
                            {completeness.missing_categories.length} gaps
                          </Badge>
                        ) : (
                          <Badge tone="good">no known gaps</Badge>
                        )}
                      </div>
                    </button>
                  );
                })
              )}
            </div>
          </GlassCard>
        </div>

        <div className="space-y-4">
          <GlassCard>
            <SectionTitle
              title="Create / update org override"
              right={
                <div className="flex items-center gap-2">
                  <button
                    onClick={clearForm}
                    className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm text-white/75 hover:bg-white/[0.08]"
                  >
                    Clear
                  </button>
                  <button
                    onClick={saveProfile}
                    disabled={saveBusy}
                    className="rounded-xl border border-emerald-400/30 bg-emerald-500/20 px-3 py-2 text-sm text-white hover:bg-emerald-500/25 disabled:opacity-60"
                  >
                    {saveBusy ? "Saving…" : "Save"}
                  </button>
                </div>
              }
            />

            <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
              <input
                value={city}
                onChange={(e) => setCity(e.target.value)}
                placeholder="City"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
              <input
                value={county}
                onChange={(e) => setCounty(e.target.value)}
                placeholder="County"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
              <input
                value={phaName}
                onChange={(e) => setPhaName(e.target.value)}
                placeholder="PHA / local program"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
              <input
                type="number"
                step="0.01"
                value={friction}
                onChange={(e) => setFriction(Number(e.target.value || 1))}
                placeholder="Friction multiplier"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
            </div>

            <textarea
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              placeholder="Notes / operational reality"
              className="mt-3 h-24 w-full rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
            />

            <textarea
              value={policyJson}
              onChange={(e) => setPolicyJson(e.target.value)}
              placeholder="policy json"
              className="mt-3 h-64 w-full rounded-xl border border-white/10 bg-black/30 px-3 py-2 font-mono text-xs text-white"
            />
          </GlassCard>

          {!selected ? (
            <GlassCard>
              <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4 text-sm text-white/60">
                Select a jurisdiction profile to inspect coverage, evidence,
                rules, and tasks.
              </div>
            </GlassCard>
          ) : (
            <>
              <GlassCard>
                <SectionTitle
                  title="Selected profile"
                  right={
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => loadIntoForm(selected)}
                        className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm text-white/75 hover:bg-white/[0.08]"
                      >
                        Load into form
                      </button>
                      <button
                        onClick={() => recomputeOne(selected)}
                        disabled={recomputeBusyId === selected.id}
                        className="rounded-xl border border-indigo-400/30 bg-indigo-500/20 px-3 py-2 text-sm text-white hover:bg-indigo-500/25 disabled:opacity-60"
                      >
                        {recomputeBusyId === selected.id
                          ? "Recomputing…"
                          : "Recompute"}
                      </button>
                      <button
                        onClick={() => deleteOne(selected)}
                        className="rounded-xl border border-red-400/30 bg-red-500/15 px-3 py-2 text-sm text-red-100 hover:bg-red-500/20"
                      >
                        Delete
                      </button>
                    </div>
                  }
                />

                <div className="mt-3">
                  <JurisdictionCoverageBadge
                    coverage={{
                      ...(selected || {}),
                      ...(selectedVisibility?.resolved_profile || {}),
                      operational_status: selectedOperationalStatus,
                    }}
                  />
                </div>

                <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
                  <div className="rounded-xl border border-white/10 bg-black/30 p-4">
                    <div className="space-y-2">
                      <Row label="ID" value={selected.id} />
                      <Row label="Scope" value={inferScope(selected)} />
                      <Row label="State" value={selected.state} />
                      <Row label="County" value={selected.county || "—"} />
                      <Row label="City" value={selected.city || "—"} />
                      <Row label="PHA" value={selected.pha_name || "—"} />
                      <Row
                        label="Friction"
                        value={selected.friction_multiplier ?? 1.0}
                      />
                      <Row
                        label="Coverage confidence"
                        value={
                          <Badge
                            tone={confidenceTone(
                              selected.coverage_confidence ||
                                selected.confidence_label,
                            )}
                          >
                            {titleize(
                              selected.coverage_confidence ||
                                selected.confidence_label ||
                                "unknown",
                            )}
                          </Badge>
                        }
                      />
                      <Row
                        label="Completeness"
                        value={
                          <Badge
                            tone={completenessTone(
                              selectedCompleteness?.completeness_status,
                            )}
                          >
                            {titleize(
                              selectedCompleteness?.completeness_status,
                            )}
                          </Badge>
                        }
                      />
                      <Row
                        label="Completeness score"
                        value={scorePct(
                          selectedCompleteness?.completeness_score,
                        )}
                      />
                      <Row
                        label="Rule version"
                        value={
                          selected.resolved_rule_version ||
                          selected.rule_version ||
                          "—"
                        }
                      />
                      <Row
                        label="Last refreshed"
                        value={formatDate(
                          selected.last_refreshed || selected.last_refreshed_at,
                        )}
                      />
                      <Row
                        label="Health state"
                        value={
                          <Badge
                            tone={confidenceTone(
                              selectedOperationalStatus?.health_state ||
                                selectedOperationalStatus?.refresh_state,
                            )}
                          >
                            {titleize(
                              selectedOperationalStatus?.health_state ||
                                selectedOperationalStatus?.refresh_state ||
                                "unknown",
                            )}
                          </Badge>
                        }
                      />
                      <Row
                        label="Safe to rely on"
                        value={
                          <Badge
                            tone={
                              selectedOperationalStatus?.safe_to_rely_on
                                ? "good"
                                : "bad"
                            }
                          >
                            {selectedOperationalStatus?.safe_to_rely_on
                              ? "yes"
                              : "no"}
                          </Badge>
                        }
                      />
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-black/30 p-4">
                    <div className="text-sm font-semibold text-white">
                      Coverage breakdown
                    </div>

                    <div className="mt-3">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Covered categories
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {selectedCompleteness?.covered_categories?.length ? (
                          selectedCompleteness.covered_categories.map(
                            (item: string) => (
                              <Badge key={item} tone="good">
                                {titleize(item)}
                              </Badge>
                            ),
                          )
                        ) : (
                          <span className="text-sm text-white/55">
                            None listed
                          </span>
                        )}
                      </div>
                    </div>

                    <div className="mt-4">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Missing categories
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {selectedCompleteness?.missing_categories?.length ? (
                          selectedCompleteness.missing_categories.map(
                            (item: string) => (
                              <Badge key={item} tone="warn">
                                {titleize(item)}
                              </Badge>
                            ),
                          )
                        ) : (
                          <Badge tone="good">No known gaps</Badge>
                        )}
                      </div>
                    </div>

                    {selectedCompleteness?.is_stale ? (
                      <div className="mt-4 rounded-xl border border-amber-400/20 bg-amber-500/10 p-3 text-sm text-amber-100">
                        {selectedCompleteness.stale_reason ||
                          "This profile is stale and should be refreshed."}
                      </div>
                    ) : null}

                    {selectedLockout?.lockout_active ||
                    selectedUnsafeReasons.length ||
                    selectedOperationalStatus?.refresh_status_reason ? (
                      <div className="mt-4 rounded-xl border border-red-400/20 bg-red-500/10 p-3 text-sm text-red-100">
                        {selectedLockout?.lockout_reason ||
                          selectedUnsafeReasons[0] ||
                          selectedOperationalStatus?.refresh_status_reason ||
                          "This jurisdiction still needs review before it is safe to rely on."}
                      </div>
                    ) : null}
                  </div>
                </div>

                {selectedOperationalStatus?.lockout?.lockout_active ||
                selectedOperationalStatus?.reasons?.length ? (
                  <div
                    className={`mt-3 oh-state-banner ${selectedOperationalStatus?.lockout?.lockout_active ? "oh-state-banner-bad" : selectedOperationalStatus?.safe_to_rely_on ? "oh-state-banner-good" : "oh-state-banner-warn"}`}
                  >
                    <div className="oh-state-banner-title">
                      {selectedOperationalStatus?.safe_to_rely_on
                        ? "Healthy and safe to rely on"
                        : selectedOperationalStatus?.lockout?.lockout_active
                          ? "Legally unsafe right now"
                          : "Review required before reliance"}
                    </div>
                    <div className="oh-state-banner-body">
                      {(selectedOperationalStatus?.reasons || [])[0] ||
                        "This jurisdiction still needs more review before it should be treated as cleared."}
                    </div>
                  </div>
                ) : null}

                <BoundaryPanel
                  title={
                    selectedOperationalStatus?.safe_to_rely_on
                      ? "Operationally usable, not legal advice"
                      : "Legal / human review still needed"
                  }
                  tone={
                    selectedOperationalStatus?.safe_to_rely_on
                      ? "good"
                      : selectedOperationalStatus?.lockout?.lockout_active
                        ? "bad"
                        : "warn"
                  }
                  body={
                    <>
                      {selectedOperationalStatus?.safe_to_rely_on
                        ? "This profile currently looks usable for operations, but it is still not a legal opinion or legal guarantee."
                        : "This profile should not be treated as legally cleared until the blocking or review-required items are resolved."}{" "}
                      Shared/exported summaries should keep this same boundary
                      language.
                    </>
                  }
                />

                {selected?.lockout_causing_categories?.length ||
                selected?.validation_pending_categories?.length ||
                selected?.authority_gap_categories?.length ||
                selected?.informational_gap_categories?.length ? (
                  <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Blocking categories
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {(selected?.lockout_causing_categories || []).map(
                          (item: string) => (
                            <Badge key={item} tone="bad">
                              {titleize(item)}
                            </Badge>
                          ),
                        )}
                        {!(selected?.lockout_causing_categories || [])
                          .length ? (
                          <span className="text-sm text-white/55">None</span>
                        ) : null}
                      </div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Validation pending
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {(selected?.validation_pending_categories || []).map(
                          (item: string) => (
                            <Badge key={item} tone="warn">
                              {titleize(item)}
                            </Badge>
                          ),
                        )}
                        {!(selected?.validation_pending_categories || [])
                          .length ? (
                          <span className="text-sm text-white/55">None</span>
                        ) : null}
                      </div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Authority gaps
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {(selected?.authority_gap_categories || []).map(
                          (item: string) => (
                            <Badge key={item} tone="bad">
                              {titleize(item)}
                            </Badge>
                          ),
                        )}
                        {!(selected?.authority_gap_categories || []).length ? (
                          <span className="text-sm text-white/55">None</span>
                        ) : null}
                      </div>
                    </div>
                    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Needed next
                      </div>
                      <div className="mt-2 text-sm font-semibold text-white">
                        {titleize(
                          selected?.next_due_step ||
                            selectedNextActions?.next_step ||
                            "monitor",
                        )}
                      </div>
                      <div className="mt-1 text-xs text-white/50">
                        Last validation{" "}
                        {formatDate(selected?.last_validation_at)}
                      </div>
                    </div>
                  </div>
                ) : null}

                {selectedOperationalStatus ? (
                  <div className="mt-3 grid gap-3 md:grid-cols-3">
                    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Next step
                      </div>
                      <div className="mt-2 text-sm font-semibold text-white">
                        {titleize(selectedNextActions?.next_step || "monitor")}
                      </div>
                      <div className="mt-1 text-xs text-white/50">
                        {selectedNextActions?.next_search_retry_due_at
                          ? `Due ${formatDate(selectedNextActions.next_search_retry_due_at)}`
                          : "No retry currently due"}
                      </div>
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Source freshness
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {Object.entries(
                          selectedSourceSummary?.freshness_counts || {},
                        ).map(([key, value]) => (
                          <Badge
                            key={key}
                            tone={norm(key) === "fresh" ? "good" : "warn"}
                          >
                            {titleize(key)} · {value as any}
                          </Badge>
                        ))}
                        {!Object.keys(
                          selectedSourceSummary?.freshness_counts || {},
                        ).length ? (
                          <span className="text-sm text-white/55">
                            No source freshness summary
                          </span>
                        ) : null}
                      </div>
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/30 p-4">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Validation state
                      </div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {Object.entries(
                          selectedSourceSummary?.validation_state_counts || {},
                        ).map(([key, value]) => (
                          <Badge
                            key={key}
                            tone={norm(key) === "validated" ? "good" : "warn"}
                          >
                            {titleize(key)} · {value as any}
                          </Badge>
                        ))}
                        {!Object.keys(
                          selectedSourceSummary?.validation_state_counts || {},
                        ).length ? (
                          <span className="text-sm text-white/55">
                            No validation summary
                          </span>
                        ) : null}
                      </div>
                    </div>
                  </div>
                ) : null}

                {selected.notes ? (
                  <div className="mt-3 rounded-xl border border-white/10 bg-white/[0.03] p-4 text-sm text-white/75">
                    {selected.notes}
                  </div>
                ) : null}
              </GlassCard>

              <GlassCard>
                <SectionTitle title="Resolved layers" />
                {!selectedLayers.length ? (
                  <div className="mt-3 rounded-xl border border-white/10 bg-white/[0.03] p-4 text-sm text-white/60">
                    No explicit layer rows returned for this profile.
                  </div>
                ) : (
                  <div className="mt-3 grid gap-3 md:grid-cols-2">
                    {selectedLayers.map((layer: any, idx: number) => (
                      <div
                        key={`${layer?.layer || layer?.scope || "layer"}-${idx}`}
                        className="rounded-xl border border-white/10 bg-black/30 p-4"
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <div className="text-sm font-semibold text-white">
                            {titleize(
                              layer?.layer ||
                                layer?.scope ||
                                layer?.label ||
                                "layer",
                            )}
                          </div>
                          <Badge tone={toneForLayer(layer)}>
                            {titleize(
                              layer?.confidence ||
                                layer?.status ||
                                (layer?.applied ? "applied" : "available"),
                            )}
                          </Badge>
                        </div>
                        <div className="mt-3 space-y-2 text-sm text-white/75">
                          <Row
                            label="Authority"
                            value={layer?.authority || layer?.source || "—"}
                          />
                          <Row label="Version" value={layer?.version || "—"} />
                          <Row
                            label="Applied"
                            value={layer?.applied ? "yes" : "no"}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </GlassCard>

              <GlassCard>
                <SectionTitle title="Source evidence" />
                {!selectedEvidence.length ? (
                  <div className="mt-3 rounded-xl border border-white/10 bg-white/[0.03] p-4 text-sm text-white/60">
                    No evidence rows returned.
                  </div>
                ) : (
                  <div className="mt-3 space-y-3">
                    {selectedEvidence.map((e: any, idx: number) => (
                      <div
                        key={`${e?.title || e?.label || e?.url || "evidence"}-${idx}`}
                        className="rounded-xl border border-white/10 bg-black/30 p-4"
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <div className="text-sm font-semibold text-white">
                            {e?.title ||
                              e?.label ||
                              e?.source_name ||
                              "Evidence"}
                          </div>
                          <Badge
                            tone={e?.is_authoritative ? "good" : "neutral"}
                          >
                            {e?.is_authoritative
                              ? "authoritative"
                              : "supporting"}
                          </Badge>
                        </div>
                        <div className="mt-2 text-sm text-white/70">
                          {e?.source_name || e?.source || "Unknown source"}
                        </div>
                        {e?.excerpt ? (
                          <div className="mt-3 rounded-xl border border-white/10 bg-white/[0.03] p-3 text-sm text-white/75">
                            {e.excerpt}
                          </div>
                        ) : null}
                        {e?.url ? (
                          <a
                            href={e.url}
                            target="_blank"
                            rel="noreferrer"
                            className="mt-3 inline-flex text-sm text-cyan-200 hover:text-cyan-100"
                          >
                            Open source
                          </a>
                        ) : null}
                      </div>
                    ))}
                  </div>
                )}
              </GlassCard>

              <GlassCard>
                <SectionTitle title="Tasks" />
                {!selectedTasks.length ? (
                  <div className="mt-3 rounded-xl border border-white/10 bg-white/[0.03] p-4 text-sm text-white/60">
                    No tasks linked to this profile.
                  </div>
                ) : (
                  <div className="mt-3 grid gap-3">
                    {selectedTasks.map((task: any, idx: number) => (
                      <div
                        key={`${task?.title || task?.code || idx}`}
                        className="rounded-xl border border-white/10 bg-black/30 p-4"
                      >
                        <div className="flex flex-wrap items-center gap-2">
                          <div className="text-sm font-semibold text-white">
                            {task?.title || task?.code || "Untitled task"}
                          </div>
                          {task?.priority ? (
                            <Badge tone="warn">{titleize(task.priority)}</Badge>
                          ) : null}
                          {task?.kind ? (
                            <Badge>{titleize(task.kind)}</Badge>
                          ) : null}
                        </div>
                        {task?.detail ? (
                          <div className="mt-2 text-sm text-white/75">
                            {task.detail}
                          </div>
                        ) : null}
                      </div>
                    ))}
                  </div>
                )}
              </GlassCard>

              <GlassCard>
                <SectionTitle
                  title="Override Ledger"
                  right={
                    <Badge tone={overrideRows.length ? "warn" : "neutral"}>
                      {overrideRows.length} records
                    </Badge>
                  }
                />
                <div className="mt-3 grid gap-3 md:grid-cols-4">
                  <input
                    className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
                    placeholder="Rule category"
                    value={overrideRuleCategory}
                    onChange={(e) => setOverrideRuleCategory(e.target.value)}
                  />
                  <input
                    className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white md:col-span-2"
                    placeholder="Why is the override needed?"
                    value={overrideReason}
                    onChange={(e) => setOverrideReason(e.target.value)}
                  />
                  <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white/80">
                    <input
                      type="checkbox"
                      checked={overrideCritical}
                      onChange={(e) => setOverrideCritical(e.target.checked)}
                    />{" "}
                    Critical rule
                  </label>
                </div>
                <div className="mt-3">
                  <button
                    className="rounded-xl border border-amber-400/30 bg-amber-500/15 px-3 py-2 text-sm text-amber-100"
                    onClick={() => void createOverrideForSelected()}
                  >
                    Add override
                  </button>
                </div>
                {!overrideRows.length ? (
                  <div className="mt-3 rounded-xl border border-white/10 bg-white/[0.03] p-4 text-sm text-white/60">
                    No override records for this profile.
                  </div>
                ) : (
                  <div className="mt-3 space-y-3">
                    {overrideRows.map((row: any) => (
                      <div
                        key={row.id}
                        className="rounded-xl border border-white/10 bg-black/30 p-4"
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <div className="text-sm font-semibold text-white">
                            {row.rule_category ||
                              row.rule_key ||
                              row.override_type ||
                              `Override ${row.id}`}
                          </div>
                          <div className="flex gap-2">
                            <Badge
                              tone={
                                row.is_currently_effective ? "warn" : "neutral"
                              }
                            >
                              {row.is_currently_effective
                                ? "Active"
                                : "Expired"}
                            </Badge>
                            {row.carrying_critical_rule ? (
                              <Badge tone="bad">Critical</Badge>
                            ) : null}
                          </div>
                        </div>
                        <div className="mt-2 text-sm text-white/75">
                          {row.reason}
                        </div>
                        <div className="mt-3 grid gap-2 text-xs text-white/60 md:grid-cols-3">
                          <div>Trust impact: {titleize(row.trust_impact)}</div>
                          <div>Expires: {formatDate(row.expires_at)}</div>
                          <div>Created: {formatDate(row.created_at)}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </GlassCard>

              <GlassCard>
                <SectionTitle title="Policy JSON" />
                <pre className="mt-3 overflow-auto rounded-xl border border-white/10 bg-black/30 p-3 text-xs text-white/80 whitespace-pre-wrap">
                  {pretty(selectedPolicy)}
                </pre>
              </GlassCard>
            </>
          )}
        </div>
      </div>
    </PageShell>
  );
}
