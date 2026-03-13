import React from "react";
import { api } from "../lib/api";
import PageHero from "../components/PageHero";
import GlassCard from "../components/GlassCard";
import PageShell from "../components/PageShell";

type CoverageRow = {
  state: string;
  county?: string | null;
  city?: string | null;
  pha_name?: string | null;
  coverage_status?: string | null;
  production_readiness?: string | null;
  confidence_label?: string | null;
  verified_rule_count?: number | null;
  source_count?: number | null;
  fetch_failure_count?: number | null;
  stale_warning_count?: number | null;
  has_sources?: boolean;
  has_extracted?: boolean;
  municipal_core_ok?: boolean;
  state_federal_core_ok?: boolean;
  pha_core_ok?: boolean;
  profile_id?: number | null;
};

type BriefOut = {
  state: string;
  county?: string | null;
  city?: string | null;
  pha_name?: string | null;
  coverage_status?: string | null;
  production_readiness?: string | null;
  confidence_label?: string | null;
  verified_rule_count?: number | null;
  source_count?: number | null;
  fetch_failure_count?: number | null;
  stale_warning_count?: number | null;
  has_sources?: boolean;
  has_extracted?: boolean;
  verified_rule_keys?: string[];
  municipal_core_ok?: boolean;
  state_federal_core_ok?: boolean;
  pha_core_ok?: boolean;
  compliance?: Record<string, any>;
  blocking_items?: any[];
  required_actions?: any[];
  evidence_links?: any[];
};

type ProfileRow = {
  id: number;
  org_id?: number | null;
  state: string;
  county?: string | null;
  city?: string | null;
  friction_multiplier?: number | null;
  pha_name?: string | null;
  notes?: string | null;
  policy?: Record<string, any> | null;
};

type LegacyRuleRow = {
  id: number;
  org_id?: number | null;
  city?: string | null;
  county?: string | null;
  state: string;
  rental_license_required?: boolean | null;
  inspection_authority?: string | null;
  typical_fail_points_json?: string | null;
  typical_fail_points?: string[] | string | null;
  registration_fee?: number | null;
  processing_days?: number | null;
  inspection_frequency?: string | null;
  tenant_waitlist_depth?: string | null;
  notes?: string | null;
};

function parseMaybeJsonArray(v: any): string[] {
  if (Array.isArray(v)) return v.map(String);

  if (typeof v === "string") {
    try {
      const parsed = JSON.parse(v);
      if (Array.isArray(parsed)) return parsed.map(String);
    } catch {
      return v
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
    }
  }

  return [];
}

function norm(s: any) {
  return String(s ?? "")
    .trim()
    .toLowerCase();
}

function marketKey(x: {
  state?: string | null;
  county?: string | null;
  city?: string | null;
  pha_name?: string | null;
}) {
  return [
    norm(x.state || "MI"),
    norm(x.county),
    norm(x.city),
    norm(x.pha_name),
  ].join("|");
}

function readinessTone(v?: string | null) {
  const s = norm(v);
  if (s === "ready") return "good";
  if (s === "needs_review" || s === "partial") return "warn";
  return "bad";
}

function confidenceTone(v?: string | null) {
  const s = norm(v);
  if (s === "high") return "good";
  if (s === "medium") return "warn";
  return "bad";
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

function MarketCard({
  row,
  active,
  onClick,
}: {
  row: CoverageRow;
  active: boolean;
  onClick: () => void;
}) {
  const title =
    row.city?.trim() ||
    row.county?.trim() ||
    row.pha_name?.trim() ||
    row.state ||
    "Unknown market";

  const subtitleParts = [
    row.city ? row.county : row.county || null,
    row.state || "MI",
    row.pha_name ? `PHA: ${row.pha_name}` : null,
  ].filter(Boolean);

  return (
    <button
      onClick={onClick}
      className={[
        "w-full text-left rounded-2xl border p-4 transition",
        active
          ? "border-white/20 bg-white/[0.08]"
          : "border-white/10 bg-white/[0.03] hover:bg-white/[0.05] hover:border-white/[0.14]",
      ].join(" ")}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-sm font-semibold text-white">{title}</div>
          <div className="mt-1 text-xs text-white/50">
            {subtitleParts.join(" • ") || "Michigan market"}
          </div>
        </div>

        <div className="flex flex-col items-end gap-1">
          <Badge tone={readinessTone(row.production_readiness)}>
            {row.production_readiness || "unknown"}
          </Badge>
          <Badge tone={confidenceTone(row.confidence_label)}>
            {row.confidence_label || "low"} confidence
          </Badge>
        </div>
      </div>

      <div className="mt-3 flex flex-wrap gap-2">
        <Badge>verified {row.verified_rule_count ?? 0}</Badge>
        <Badge>sources {row.source_count ?? 0}</Badge>
        {(row.fetch_failure_count ?? 0) > 0 ? (
          <Badge tone="bad">fetch fails {row.fetch_failure_count}</Badge>
        ) : null}
        {(row.stale_warning_count ?? 0) > 0 ? (
          <Badge tone="warn">stale {row.stale_warning_count}</Badge>
        ) : null}
      </div>
    </button>
  );
}

export default function Jurisdictions() {
  const [coverageRows, setCoverageRows] = React.useState<CoverageRow[]>([]);
  const [profiles, setProfiles] = React.useState<ProfileRow[]>([]);
  const [legacyRules, setLegacyRules] = React.useState<LegacyRuleRow[]>([]);
  const [selectedKey, setSelectedKey] = React.useState<string>("");
  const [brief, setBrief] = React.useState<BriefOut | null>(null);
  const [evidence, setEvidence] = React.useState<any | null>(null);

  const [query, setQuery] = React.useState("");
  const [onlyReady, setOnlyReady] = React.useState(false);
  const [onlyWeak, setOnlyWeak] = React.useState(false);
  const [busy, setBusy] = React.useState(false);
  const [detailBusy, setDetailBusy] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);

  const selectedRow = React.useMemo(
    () => coverageRows.find((r) => marketKey(r) === selectedKey) ?? null,
    [coverageRows, selectedKey],
  );

  const coverageByKey = React.useMemo(() => {
    const m = new Map<string, CoverageRow>();
    for (const row of coverageRows) {
      m.set(marketKey(row), row);
    }
    return m;
  }, [coverageRows]);

  const profilesByKey = React.useMemo(() => {
    const m = new Map<string, ProfileRow[]>();
    for (const p of profiles) {
      const key = marketKey(p);
      const prev = m.get(key) ?? [];
      prev.push(p);
      m.set(key, prev);
    }
    return m;
  }, [profiles]);

  const legacyRulesByKey = React.useMemo(() => {
    const m = new Map<string, LegacyRuleRow[]>();
    for (const r of legacyRules) {
      const key = marketKey(r);
      const prev = m.get(key) ?? [];
      prev.push(r);
      m.set(key, prev);
    }
    return m;
  }, [legacyRules]);

  const filteredRows = React.useMemo(() => {
    const q = norm(query);

    return coverageRows.filter((row) => {
      const hay = [
        row.city,
        row.county,
        row.state,
        row.pha_name,
        row.coverage_status,
        row.production_readiness,
        row.confidence_label,
      ]
        .map((x) => norm(x))
        .join(" ");

      if (q && !hay.includes(q)) return false;
      if (onlyReady && norm(row.production_readiness) !== "ready") return false;
      if (
        onlyWeak &&
        !["low", "unknown", ""].includes(norm(row.confidence_label))
      ) {
        return false;
      }

      return true;
    });
  }, [coverageRows, query, onlyReady, onlyWeak]);

  const selectedProfiles = React.useMemo(
    () =>
      selectedRow ? (profilesByKey.get(marketKey(selectedRow)) ?? []) : [],
    [selectedRow, profilesByKey],
  );

  const selectedLegacyRules = React.useMemo(
    () =>
      selectedRow ? (legacyRulesByKey.get(marketKey(selectedRow)) ?? []) : [],
    [selectedRow, legacyRulesByKey],
  );

  async function refresh() {
    setBusy(true);
    setErr(null);

    try {
      const [coverageOut, profileOut, legacyOut] = await Promise.all([
        api
          .policyCoverageAll({ focus: "se_mi_extended", org_scope: false })
          .catch(() => ({ rows: [] })),
        api.listJurisdictionProfiles(true, "MI").catch(() => []),
        api.listJurisdictionRules(true, "MI").catch(() => []),
      ]);

      const coverageList = Array.isArray(coverageOut)
        ? coverageOut
        : Array.isArray(coverageOut?.rows)
          ? coverageOut.rows
          : Array.isArray(coverageOut?.items)
            ? coverageOut.items
            : [];

      const sortedCoverage = [...coverageList].sort((a, b) => {
        const aReady = norm(a.production_readiness) === "ready" ? 1 : 0;
        const bReady = norm(b.production_readiness) === "ready" ? 1 : 0;
        if (aReady !== bReady) return bReady - aReady;

        const aVerified = Number(a.verified_rule_count ?? 0);
        const bVerified = Number(b.verified_rule_count ?? 0);
        if (aVerified !== bVerified) return bVerified - aVerified;

        return `${a.city || ""}${a.county || ""}${a.state || ""}`.localeCompare(
          `${b.city || ""}${b.county || ""}${b.state || ""}`,
        );
      });

      setCoverageRows(sortedCoverage);
      setProfiles(Array.isArray(profileOut) ? profileOut : []);
      setLegacyRules(Array.isArray(legacyOut) ? legacyOut : []);

      if (sortedCoverage.length > 0) {
        const nextKey = sortedCoverage.find((r) => marketKey(r) === selectedKey)
          ? selectedKey
          : marketKey(sortedCoverage[0]);
        setSelectedKey(nextKey);
      } else {
        setSelectedKey("");
        setBrief(null);
        setEvidence(null);
      }
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function loadDetail(row: CoverageRow) {
    setDetailBusy(true);
    setErr(null);

    try {
      const params = {
        state: row.state || "MI",
        county: row.county ?? null,
        city: row.city ?? null,
        pha_name: row.pha_name ?? null,
        org_scope: false,
      };

      const [briefOut, evidenceOut] = await Promise.all([
        api.policyBrief(params).catch(() => null),
        api
          .policyEvidenceMarket({
            state: row.state || "MI",
            county: row.county ?? null,
            city: row.city ?? null,
            include_global: true,
          })
          .catch(() => null),
      ]);

      setBrief(briefOut);
      setEvidence(evidenceOut);
    } catch (e: any) {
      setErr(String(e?.message || e));
      setBrief(null);
      setEvidence(null);
    } finally {
      setDetailBusy(false);
    }
  }

  React.useEffect(() => {
    refresh().catch((e) => setErr(String(e?.message || e)));
  }, []);

  React.useEffect(() => {
    if (!selectedRow) return;
    loadDetail(selectedRow).catch((e) => setErr(String(e?.message || e)));
  }, [selectedKey]);

  const marketStats = React.useMemo(() => {
    const total = coverageRows.length;
    const ready = coverageRows.filter(
      (r) => norm(r.production_readiness) === "ready",
    ).length;
    const weak = coverageRows.filter((r) =>
      ["low", "unknown", ""].includes(norm(r.confidence_label)),
    ).length;
    const verifiedRules = coverageRows.reduce(
      (sum, r) => sum + Number(r.verified_rule_count ?? 0),
      0,
    );
    return { total, ready, weak, verifiedRules };
  }, [coverageRows]);

  return (
    <PageShell className="space-y-6">
      <PageHero
        eyebrow="Compliance intelligence"
        title="Jurisdictions"
        subtitle="Real market coverage, confidence, and compliance readiness by city, county, and PHA. This page should answer whether automation is trustworthy, not just whether somebody typed in a seed row six months ago."
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <button
              onClick={() => refresh()}
              disabled={busy}
              className="rounded-2xl border border-white/15 bg-white/10 px-4 py-2 hover:bg-white/15 disabled:opacity-50"
            >
              {busy ? "Refreshing…" : "Refresh"}
            </button>
          </div>
        }
      />

      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Markets tracked
          </div>
          <div className="mt-2 text-2xl font-semibold text-white">
            {marketStats.total}
          </div>
        </GlassCard>

        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Automation-ready
          </div>
          <div className="mt-2 text-2xl font-semibold text-emerald-200">
            {marketStats.ready}
          </div>
        </GlassCard>

        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Weak confidence
          </div>
          <div className="mt-2 text-2xl font-semibold text-amber-100">
            {marketStats.weak}
          </div>
        </GlassCard>

        <GlassCard>
          <div className="text-xs uppercase tracking-wider text-white/45">
            Verified rules
          </div>
          <div className="mt-2 text-2xl font-semibold text-white">
            {marketStats.verifiedRules}
          </div>
        </GlassCard>
      </div>

      <GlassCard>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-[1fr_auto_auto]">
          <input
            className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-white placeholder:text-white/35"
            placeholder="Filter by city, county, state, readiness, or confidence…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />

          <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/75">
            <input
              type="checkbox"
              checked={onlyReady}
              onChange={(e) => setOnlyReady(e.target.checked)}
            />
            ready only
          </label>

          <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm text-white/75">
            <input
              type="checkbox"
              checked={onlyWeak}
              onChange={(e) => setOnlyWeak(e.target.checked)}
            />
            weak confidence only
          </label>
        </div>

        {err ? (
          <div className="mt-3 rounded-xl border border-red-500/25 bg-red-500/10 p-3 text-sm text-red-200">
            {err}
          </div>
        ) : null}
      </GlassCard>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[420px_1fr]">
        <div className="space-y-3">
          {filteredRows.length === 0 ? (
            <GlassCard>
              <div className="text-sm text-white/60">
                No jurisdictions matched the current filter.
              </div>
            </GlassCard>
          ) : (
            filteredRows.map((row) => (
              <MarketCard
                key={marketKey(row)}
                row={row}
                active={marketKey(row) === selectedKey}
                onClick={() => setSelectedKey(marketKey(row))}
              />
            ))
          )}
        </div>

        <div className="space-y-4">
          {!selectedRow ? (
            <GlassCard>
              <div className="text-sm text-white/60">
                Select a jurisdiction to inspect its real compliance posture.
              </div>
            </GlassCard>
          ) : (
            <>
              <GlassCard>
                <SectionTitle
                  title={`${selectedRow.city || selectedRow.county || selectedRow.pha_name || selectedRow.state} details`}
                  right={
                    detailBusy ? (
                      <span className="text-xs text-white/45">Loading…</span>
                    ) : (
                      <div className="flex flex-wrap gap-2">
                        <Badge
                          tone={readinessTone(selectedRow.production_readiness)}
                        >
                          {selectedRow.production_readiness || "unknown"}
                        </Badge>
                        <Badge
                          tone={confidenceTone(selectedRow.confidence_label)}
                        >
                          {selectedRow.confidence_label || "low"} confidence
                        </Badge>
                      </div>
                    )
                  }
                />

                <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
                  <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                    <div className="space-y-2">
                      <Row label="State" value={selectedRow.state || "MI"} />
                      <Row label="County" value={selectedRow.county || "—"} />
                      <Row label="City" value={selectedRow.city || "—"} />
                      <Row label="PHA" value={selectedRow.pha_name || "—"} />
                      <Row
                        label="Coverage status"
                        value={selectedRow.coverage_status || "—"}
                      />
                      <Row
                        label="Verified rules"
                        value={selectedRow.verified_rule_count ?? 0}
                      />
                    </div>
                  </div>

                  <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                    <div className="space-y-2">
                      <Row
                        label="Sources"
                        value={selectedRow.source_count ?? 0}
                      />
                      <Row
                        label="Fetch failures"
                        value={selectedRow.fetch_failure_count ?? 0}
                      />
                      <Row
                        label="Stale warnings"
                        value={selectedRow.stale_warning_count ?? 0}
                      />
                      <Row
                        label="Municipal core"
                        value={selectedRow.municipal_core_ok ? "ok" : "missing"}
                      />
                      <Row
                        label="State/Federal core"
                        value={
                          selectedRow.state_federal_core_ok ? "ok" : "missing"
                        }
                      />
                      <Row
                        label="PHA core"
                        value={selectedRow.pha_core_ok ? "ok" : "missing"}
                      />
                    </div>
                  </div>
                </div>
              </GlassCard>

              <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                <GlassCard>
                  <SectionTitle title="Required actions" />
                  <div className="mt-3 space-y-2">
                    {(brief?.required_actions ?? []).length === 0 ? (
                      <div className="text-sm text-white/55">
                        No required actions returned.
                      </div>
                    ) : (
                      (brief?.required_actions ?? []).map(
                        (item: any, idx: number) => (
                          <div
                            key={`${item?.code || item?.title || idx}`}
                            className="rounded-xl border border-white/10 bg-white/[0.03] p-3"
                          >
                            <div className="text-sm font-medium text-white">
                              {item?.title ||
                                item?.description ||
                                item?.code ||
                                "Untitled action"}
                            </div>
                            <div className="mt-1 text-xs text-white/50">
                              {(item?.category || "uncategorized").toString()} •
                              code: {item?.code || "—"}
                            </div>
                          </div>
                        ),
                      )
                    )}
                  </div>
                </GlassCard>

                <GlassCard>
                  <SectionTitle title="Blocking items" />
                  <div className="mt-3 space-y-2">
                    {(brief?.blocking_items ?? []).length === 0 ? (
                      <div className="text-sm text-white/55">
                        No blockers returned.
                      </div>
                    ) : (
                      (brief?.blocking_items ?? []).map(
                        (item: any, idx: number) => (
                          <div
                            key={`${item?.code || item?.title || idx}`}
                            className="rounded-xl border border-red-500/20 bg-red-500/[0.06] p-3"
                          >
                            <div className="text-sm font-medium text-red-100">
                              {item?.title ||
                                item?.description ||
                                item?.code ||
                                "Untitled blocker"}
                            </div>
                            <div className="mt-1 text-xs text-red-100/70">
                              {(item?.category || "uncategorized").toString()} •
                              code: {item?.code || "—"}
                            </div>
                          </div>
                        ),
                      )
                    )}
                  </div>
                </GlassCard>
              </div>

              <GlassCard>
                <SectionTitle title="Evidence and source posture" />
                <div className="mt-3 space-y-3">
                  {(brief?.evidence_links ?? []).length > 0 ? (
                    <div className="space-y-2">
                      {(brief?.evidence_links ?? []).map(
                        (item: any, idx: number) => (
                          <div
                            key={idx}
                            className="rounded-xl border border-white/10 bg-white/[0.03] p-3"
                          >
                            <div className="text-sm text-white/85">
                              {item?.title ||
                                item?.publisher ||
                                item?.url ||
                                "Evidence"}
                            </div>
                            <div className="mt-1 text-xs text-white/50">
                              {item?.url ||
                                item?.source_url ||
                                "No URL surfaced"}
                            </div>
                          </div>
                        ),
                      )}
                    </div>
                  ) : (
                    <div className="text-sm text-white/55">
                      No evidence links surfaced for this market yet.
                    </div>
                  )}

                  {evidence?.rows?.length ? (
                    <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                      <div className="text-xs uppercase tracking-wider text-white/45">
                        Evidence rows
                      </div>
                      <div className="mt-3 space-y-2">
                        {evidence.rows
                          .slice(0, 8)
                          .map((row: any, idx: number) => (
                            <div key={idx} className="text-sm text-white/75">
                              •{" "}
                              {row?.title ||
                                row?.publisher ||
                                row?.url ||
                                "Untitled source"}
                            </div>
                          ))}
                      </div>
                    </div>
                  ) : null}
                </div>
              </GlassCard>

              <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                <GlassCard>
                  <SectionTitle title="Resolved jurisdiction profiles" />
                  <div className="mt-3 space-y-2">
                    {selectedProfiles.length === 0 ? (
                      <div className="text-sm text-white/55">
                        No explicit jurisdiction profile rows matched this
                        market.
                      </div>
                    ) : (
                      selectedProfiles.map((p) => (
                        <div
                          key={p.id}
                          className="rounded-xl border border-white/10 bg-white/[0.03] p-3"
                        >
                          <div className="flex items-center justify-between gap-3">
                            <div className="text-sm font-medium text-white">
                              Profile #{p.id}
                            </div>
                            <Badge tone={p.org_id ? "warn" : "neutral"}>
                              {p.org_id ? "org override" : "global"}
                            </Badge>
                          </div>
                          <div className="mt-2 text-xs text-white/55">
                            friction: {p.friction_multiplier ?? "—"} • PHA:{" "}
                            {p.pha_name || "—"}
                          </div>
                          {p.notes ? (
                            <div className="mt-2 text-sm text-white/70">
                              {p.notes}
                            </div>
                          ) : null}
                        </div>
                      ))
                    )}
                  </div>
                </GlassCard>

                <GlassCard>
                  <SectionTitle title="Legacy jurisdiction rules (fallback only)" />
                  <div className="mt-3 space-y-2">
                    {selectedLegacyRules.length === 0 ? (
                      <div className="text-sm text-white/55">
                        No legacy rule rows matched this market.
                      </div>
                    ) : (
                      selectedLegacyRules.map((r) => {
                        const failPoints = parseMaybeJsonArray(
                          r.typical_fail_points_json ?? r.typical_fail_points,
                        );

                        return (
                          <div
                            key={r.id}
                            className="rounded-xl border border-white/10 bg-white/[0.03] p-3"
                          >
                            <div className="flex items-center justify-between gap-3">
                              <div className="text-sm font-medium text-white">
                                Legacy rule #{r.id}
                              </div>
                              <Badge tone={r.org_id ? "warn" : "neutral"}>
                                {r.org_id ? "org override" : "global"}
                              </Badge>
                            </div>
                            <div className="mt-2 text-xs text-white/55">
                              license: {String(!!r.rental_license_required)} •
                              authority: {r.inspection_authority || "—"} • freq:{" "}
                              {r.inspection_frequency || "—"}
                            </div>
                            <div className="mt-2 text-sm text-white/70">
                              fail points:{" "}
                              {failPoints.length ? failPoints.join(", ") : "—"}
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>
                </GlassCard>
              </div>
            </>
          )}
        </div>
      </div>
    </PageShell>
  );
}
