import React from "react";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import GlassCard from "../components/GlassCard";
import { api } from "../lib/api";

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

function inferScope(row: ProfileRow): "org" | "global" {
  if (row.scope === "org" || row.org_id) return "org";
  return "global";
}

function policyObject(row: ProfileRow): Record<string, any> {
  if (row.policy && typeof row.policy === "object") return row.policy;
  if (row.policy_json && typeof row.policy_json === "object")
    return row.policy_json as Record<string, any>;
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

export default function JurisdictionProfiles() {
  const [includeGlobal, setIncludeGlobal] = React.useState(true);
  const [state, setState] = React.useState("MI");
  const [rows, setRows] = React.useState<ProfileRow[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const [query, setQuery] = React.useState("");
  const [selectedId, setSelectedId] = React.useState<number | null>(null);

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
    return { total, org, global, city, county, incomplete, stale };
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

  const selectedCompleteness = selected ? completenessFromRow(selected) : null;
  const selectedTasks = Array.isArray(selected?.tasks) ? selected?.tasks : [];

  return (
    <PageShell className="space-y-6">
      <PageHero
        eyebrow="Operational modeling"
        title="Jurisdiction Profiles"
        subtitle="Profiles are the modeled output layer. Pipeline actions like source refresh, run pipeline, cleanup stale, and repair market live on the Jurisdictions page."
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
              Looking for the pipeline buttons?
            </div>
            <div className="mt-1 text-sm text-white/65">
              Use the <span className="text-white">Jurisdictions</span> page
              for: Repair market, Run pipeline, Refresh sources, Refresh
              coverage, Resolve stale items, Refresh jurisdiction, and Notify
              stale.
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

      <div className="grid grid-cols-1 gap-4 md:grid-cols-7">
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
              placeholder="Search city, county, scope, PHA, notes, completeness, stale reason…"
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
                          <Badge>friction {r.friction_multiplier ?? 1.0}</Badge>
                          <Badge
                            tone={completenessTone(
                              completeness.completeness_status,
                            )}
                          >
                            {completeness.completeness_status}
                          </Badge>
                        </div>
                      </div>

                      <div className="mt-2 flex flex-wrap gap-2">
                        <Badge>
                          {scorePct(completeness.completeness_score)}
                        </Badge>
                        {completeness.is_stale ? (
                          <Badge tone="warn">stale</Badge>
                        ) : null}
                        {!!completeness.missing_categories.length && (
                          <Badge tone="bad">
                            missing {completeness.missing_categories.length}
                          </Badge>
                        )}
                      </div>

                      {r.notes ? (
                        <div className="mt-2 line-clamp-2 text-sm text-white/65">
                          {r.notes}
                        </div>
                      ) : null}
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
              title="Selected profile"
              right={
                selected ? (
                  <div className="flex flex-wrap gap-2">
                    <Badge
                      tone={inferScope(selected) === "org" ? "warn" : "neutral"}
                    >
                      {inferScope(selected)}
                    </Badge>
                    <Badge>id {selected.id}</Badge>
                    {selectedCompleteness ? (
                      <Badge
                        tone={completenessTone(
                          selectedCompleteness.completeness_status,
                        )}
                      >
                        {selectedCompleteness.completeness_status}
                      </Badge>
                    ) : null}
                  </div>
                ) : null
              }
            />

            {!selected ? (
              <div className="mt-3 text-sm text-white/60">
                Select a profile to inspect its current operational model.
              </div>
            ) : (
              <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="space-y-2">
                    <Row label="State" value={selected.state} />
                    <Row label="County" value={selected.county || "—"} />
                    <Row label="City" value={selected.city || "—"} />
                    <Row label="PHA" value={selected.pha_name || "—"} />
                    <Row
                      label="Friction multiplier"
                      value={selected.friction_multiplier ?? 1.0}
                    />
                    <Row
                      label="Completeness score"
                      value={scorePct(selectedCompleteness?.completeness_score)}
                    />
                    <Row
                      label="Freshness"
                      value={selectedCompleteness?.is_stale ? "stale" : "fresh"}
                    />
                  </div>

                  <div className="mt-4 flex flex-wrap gap-2">
                    <button
                      onClick={() => recomputeOne(selected)}
                      disabled={recomputeBusyId === selected.id}
                      className="rounded-xl border border-cyan-400/25 bg-cyan-500/15 px-3 py-2 text-xs text-white hover:bg-cyan-500/20 disabled:opacity-60"
                    >
                      {recomputeBusyId === selected.id
                        ? "Recomputing…"
                        : "Recompute completeness"}
                    </button>

                    {inferScope(selected) === "org" ? (
                      <button
                        onClick={() => loadIntoForm(selected)}
                        className="rounded-xl border border-emerald-400/25 bg-emerald-500/15 px-3 py-2 text-xs text-white hover:bg-emerald-500/20"
                      >
                        Load into editor
                      </button>
                    ) : null}

                    {inferScope(selected) === "org" ? (
                      <button
                        onClick={() => deleteOne(selected)}
                        className="rounded-xl border border-red-500/20 bg-red-500/10 px-3 py-2 text-xs text-red-100 hover:bg-red-500/15"
                      >
                        Delete override
                      </button>
                    ) : (
                      <span className="text-xs text-white/45">
                        Global rows cannot be deleted here.
                      </span>
                    )}
                  </div>
                </div>

                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="text-xs uppercase tracking-wider text-white/45">
                    Notes
                  </div>
                  <div className="mt-2 text-sm text-white/75">
                    {selected.notes || "No notes recorded."}
                  </div>

                  {selectedCompleteness?.stale_reason ? (
                    <>
                      <div className="mt-4 text-xs uppercase tracking-wider text-white/45">
                        Stale reason
                      </div>
                      <div className="mt-2 text-sm text-white/75">
                        {selectedCompleteness.stale_reason}
                      </div>
                    </>
                  ) : null}
                </div>

                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="text-xs uppercase tracking-wider text-white/45">
                    Covered categories
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {(selectedCompleteness?.covered_categories || []).length ? (
                      (selectedCompleteness?.covered_categories || []).map(
                        (c) => (
                          <Badge key={`covered-${c}`} tone="good">
                            {c}
                          </Badge>
                        ),
                      )
                    ) : (
                      <span className="text-sm text-white/55">
                        None recorded
                      </span>
                    )}
                  </div>
                </div>

                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="text-xs uppercase tracking-wider text-white/45">
                    Missing categories
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {(selectedCompleteness?.missing_categories || []).length ? (
                      (selectedCompleteness?.missing_categories || []).map(
                        (c) => (
                          <Badge key={`missing-${c}`} tone="bad">
                            {c}
                          </Badge>
                        ),
                      )
                    ) : (
                      <span className="text-sm text-white/55">
                        No missing categories
                      </span>
                    )}
                  </div>
                </div>

                <div className="lg:col-span-2 rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="text-xs uppercase tracking-wider text-white/45">
                    Jurisdiction tasks
                  </div>
                  <div className="mt-3 space-y-2">
                    {!selectedTasks.length ? (
                      <div className="text-sm text-white/55">
                        No jurisdiction tasks surfaced for this profile.
                      </div>
                    ) : (
                      selectedTasks.map((task: any, idx: number) => (
                        <div
                          key={`${task?.code || task?.key || task?.title || idx}`}
                          className="rounded-xl border border-white/10 bg-black/20 p-3"
                        >
                          <div className="text-sm font-medium text-white">
                            {task?.title || task?.label || "Untitled task"}
                          </div>
                          <div className="mt-1 text-xs text-white/50">
                            {(
                              task?.category ||
                              task?.kind ||
                              "jurisdiction"
                            ).toString()}{" "}
                            • {(task?.priority || "normal").toString()}
                          </div>
                          {task?.detail || task?.description ? (
                            <div className="mt-2 text-sm text-white/70">
                              {task?.detail || task?.description}
                            </div>
                          ) : null}
                        </div>
                      ))
                    )}
                  </div>
                </div>

                <div className="lg:col-span-2 rounded-2xl border border-white/10 bg-black/30 p-4">
                  <div className="text-xs uppercase tracking-wider text-white/45">
                    Policy JSON
                  </div>
                  <pre className="mt-3 overflow-auto whitespace-pre-wrap text-xs text-white/80">
                    {pretty(policyObject(selected))}
                  </pre>
                </div>
              </div>
            )}
          </GlassCard>

          <GlassCard>
            <SectionTitle
              title="Create / update org override"
              right={
                <div className="flex flex-wrap gap-2">
                  <button
                    onClick={clearForm}
                    className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-xs text-white/75 hover:bg-white/[0.08]"
                  >
                    Clear form
                  </button>
                </div>
              }
            />

            <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
              <input
                value={city}
                onChange={(e) => setCity(e.target.value)}
                placeholder="City (optional)"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
              <input
                value={county}
                onChange={(e) => setCounty(e.target.value)}
                placeholder="County (optional)"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
              <input
                value={String(friction)}
                onChange={(e) => setFriction(Number(e.target.value))}
                placeholder="Friction (e.g. 1.25)"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
              <input
                value={phaName}
                onChange={(e) => setPhaName(e.target.value)}
                placeholder="PHA name (optional)"
                className="rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
              />
            </div>

            <textarea
              value={policyJson}
              onChange={(e) => setPolicyJson(e.target.value)}
              rows={14}
              className="mt-3 w-full rounded-xl border border-white/10 bg-black/30 px-3 py-2 font-mono text-xs text-white"
            />

            <input
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              placeholder="Notes (optional)"
              className="mt-3 w-full rounded-xl border border-white/10 bg-black/30 px-3 py-2 text-sm text-white"
            />

            <div className="mt-3 flex flex-wrap items-center gap-3">
              <button
                onClick={saveProfile}
                disabled={saveBusy}
                className="rounded-xl border border-emerald-400/25 bg-emerald-500/15 px-3 py-2 text-sm text-white transition hover:bg-emerald-500/20 disabled:opacity-50"
              >
                {saveBusy ? "Saving…" : "Save override"}
              </button>

              <div className="text-xs text-white/60">
                Tip: leave city and county blank to override the state baseline
                for your org.
              </div>
            </div>
          </GlassCard>
        </div>
      </div>

      <div className="text-xs text-white/50">
        Docs:{" "}
        <span className="text-white/70">/meta/docs/michigan_jurisdictions</span>
      </div>
    </PageShell>
  );
}
