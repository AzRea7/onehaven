import React from "react";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import GlassCard from "../components/GlassCard";
import JurisdictionCoverageBadge from "../components/JurisdictionCoverageBadge";
import { api } from "../lib/api";

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
      const data = await api.listJurisdictionProfiles(includeGlobal, state);
      const list = Array.isArray(data) ? data : [];
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
  }, [includeGlobal, state]);

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
