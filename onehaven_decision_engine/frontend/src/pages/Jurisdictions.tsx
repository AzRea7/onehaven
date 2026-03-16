import React from "react";
import { api } from "../lib/api";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import KpiCard from "../components/KpiCard";
import EmptyState from "../components/EmptyState";
import MarketSourcePackModal from "../components/MarketSourcePackModal";
import Golem from "../components/Golem";
import {
  AlertTriangle,
  CheckCircle2,
  MapPinned,
  Search,
  ShieldCheck,
} from "lucide-react";

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
  explanation?: string | null;
  verified_rules?: any[];
  local_rule_statuses?: Record<string, string>;
  verified_rule_count_local?: number | null;
  verified_rule_count_effective?: number | null;
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

type MarketAction =
  | "collect"
  | "build"
  | "coverage"
  | "cleanup"
  | "pipeline"
  | "repair";

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

function titleCase(v: any) {
  const s = String(v ?? "").trim();
  if (!s) return "—";
  return s
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
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
      ? "oh-pill oh-pill-good"
      : tone === "warn"
        ? "oh-pill oh-pill-warn"
        : tone === "bad"
          ? "oh-pill oh-pill-bad"
          : "oh-pill";

  return <span className={cls}>{children}</span>;
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
      <div className="text-sm font-semibold text-app-0">{title}</div>
      {right ? <div>{right}</div> : null}
    </div>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-4 text-sm">
      <div className="text-app-4">{label}</div>
      <div className="text-right text-app-1">{value}</div>
    </div>
  );
}

function ActionButton({
  label,
  busy,
  onClick,
  tone = "default",
}: {
  label: string;
  busy?: boolean;
  onClick: () => void;
  tone?: "default" | "primary" | "danger";
}) {
  const cls =
    tone === "primary" ? "oh-btn oh-btn-primary" : "oh-btn oh-btn-secondary";

  return (
    <button onClick={onClick} disabled={busy} className={cls}>
      {busy ? "Running…" : label}
    </button>
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
    row.city
      ? titleCase(row.county)
      : row.county
        ? titleCase(row.county)
        : null,
    row.state || "MI",
    row.pha_name ? `PHA: ${row.pha_name}` : null,
  ].filter(Boolean);

  return (
    <button
      onClick={onClick}
      className={[
        "w-full text-left rounded-2xl border p-4 transition",
        active
          ? "border-app-strong bg-app-panel"
          : "border-app bg-app-panel hover:bg-app-muted hover:border-app-strong",
      ].join(" ")}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-sm font-semibold text-app-0">
            {titleCase(title)}
          </div>
          <div className="mt-1 text-xs text-app-4">
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
  const [marketBusy, setMarketBusy] = React.useState<string>("");
  const [err, setErr] = React.useState<string | null>(null);
  const [message, setMessage] = React.useState<string | null>(null);

  const [showSourceList, setShowSourceList] = React.useState(true);
  const [showAssertionList, setShowAssertionList] = React.useState(true);
  const [sourcePackOpen, setSourcePackOpen] = React.useState(false);

  const selectedRow = React.useMemo(
    () => coverageRows.find((r) => marketKey(r) === selectedKey) ?? null,
    [coverageRows, selectedKey],
  );

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

  const selectedSources = React.useMemo(
    () => (Array.isArray(evidence?.sources) ? evidence.sources : []),
    [evidence],
  );

  const selectedAssertions = React.useMemo(
    () => (Array.isArray(evidence?.assertions) ? evidence.assertions : []),
    [evidence],
  );

  async function refresh() {
    setBusy(true);
    setErr(null);

    try {
      const [coverageOut, profileOut, legacyOut] = await Promise.all([
        api
          .policyCoverageAll({ focus: "se_mi_extended", org_scope: false })
          .catch(() => ({ items: [] })),
        api.listJurisdictionProfiles(true, "MI").catch(() => []),
        api.listJurisdictionRules(true, "MI").catch(() => []),
      ]);

      const coverageList = Array.isArray(coverageOut)
        ? coverageOut
        : Array.isArray((coverageOut as any)?.rows)
          ? (coverageOut as any).rows
          : Array.isArray((coverageOut as any)?.items)
            ? (coverageOut as any).items
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

  async function runMarketAction(action: MarketAction, row: CoverageRow) {
    const key = `${action}:${marketKey(row)}`;
    setMarketBusy(key);
    setErr(null);
    setMessage(null);

    const payload = {
      state: row.state || "MI",
      county: row.county ?? null,
      city: row.city ?? null,
      pha_name: row.pha_name ?? null,
      org_scope: false,
      focus: "se_mi_extended",
    };

    try {
      let result: any = null;

      if (action === "collect") {
        result = await api.policyCollectCatalogMarket(payload);
        setMessage(
          `Sources refreshed for ${titleCase(row.city || row.county || row.state)}. ${result?.ok_count ?? 0} ok, ${result?.failed_count ?? 0} failed.`,
        );
      } else if (action === "build") {
        result = await api.policyBuildMarket(payload);
        setMessage(
          `Profile rebuilt for ${titleCase(row.city || row.county || row.state)}.`,
        );
      } else if (action === "coverage") {
        result = await api.policyRefreshCoverage(payload);
        setMessage(
          `Coverage refreshed for ${titleCase(row.city || row.county || row.state)}.`,
        );
      } else if (action === "cleanup") {
        result = await api.policyCleanupStaleMarket(payload);
        setMessage(
          `Stale cleanup finished for ${titleCase(row.city || row.county || row.state)}. Remaining stale items: ${result?.cleanup?.stale_items_remaining ?? 0}.`,
        );
      } else if (action === "pipeline") {
        result = await api.policyRunMarketPipeline(payload);
        setMessage(
          `Pipeline finished for ${titleCase(row.city || row.county || row.state)}.`,
        );
      } else {
        result = await api.policyRepairMarket(payload);
        setMessage(
          `Repair market finished for ${titleCase(row.city || row.county || row.state)}. Unresolved rule gaps: ${(result?.unresolved_rule_gaps ?? []).length}.`,
        );
      }

      await refresh();
      await loadDetail({
        ...row,
        state: row.state || "MI",
        county: row.county ?? null,
        city: row.city ?? null,
        pha_name: row.pha_name ?? null,
      });
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setMarketBusy("");
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
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Compliance intelligence"
          title="Jurisdictions"
          subtitle="A few-click control plane for market repair, evidence inspection, and coverage verification."
          right={
            <div className="absolute inset-0 flex items-center justify-center pointer-events-auto overflow-visible">
              <div className="h-[180px] w-[180px] md:h-[200px] md:w-[200px] opacity-95">
                <Golem className="h-full w-full" />
              </div>
            </div>
          }
          actions={
            <>
              <button
                onClick={() => refresh()}
                disabled={busy}
                className="oh-btn oh-btn-secondary"
              >
                {busy ? "Refreshing…" : "Refresh page"}
              </button>
            </>
          }
        />

        <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
          <KpiCard
            title="Markets tracked"
            value={marketStats.total}
            subtitle="coverage rows"
            icon={MapPinned}
          />
          <KpiCard
            title="Automation-ready"
            value={marketStats.ready}
            subtitle="production ready"
            icon={CheckCircle2}
            tone="success"
          />
          <KpiCard
            title="Weak confidence"
            value={marketStats.weak}
            subtitle="needs operator attention"
            icon={AlertTriangle}
            tone="warning"
          />
          <KpiCard
            title="Verified rules"
            value={marketStats.verifiedRules}
            subtitle="across visible markets"
            icon={ShieldCheck}
            tone="accent"
          />
        </div>

        <Surface
          title="Search and focus"
          subtitle="Filter market rows by city, county, state, readiness, or confidence."
        >
          <div className="grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,1fr)_auto_auto]">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
              <input
                className="oh-input pl-10"
                placeholder="Filter by city, county, state, readiness, or confidence…"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
              />
            </div>

            <label className="flex items-center gap-2 rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-2">
              <input
                type="checkbox"
                checked={onlyReady}
                onChange={(e) => setOnlyReady(e.target.checked)}
              />
              ready only
            </label>

            <label className="flex items-center gap-2 rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-2">
              <input
                type="checkbox"
                checked={onlyWeak}
                onChange={(e) => setOnlyWeak(e.target.checked)}
              />
              weak confidence only
            </label>
          </div>

          {message ? (
            <div className="mt-3 rounded-2xl border border-emerald-500/25 bg-emerald-500/10 p-3 text-sm text-emerald-300">
              {message}
            </div>
          ) : null}

          {err ? (
            <div className="mt-3 rounded-2xl border border-red-500/25 bg-red-500/10 p-3 text-sm text-red-300">
              {err}
            </div>
          ) : null}
        </Surface>

        <div className="oh-jur-layout">
          <Surface
            title="Markets"
            subtitle="Pick a market to inspect and repair."
          >
            <div className="oh-jur-list space-y-3">
              {filteredRows.length === 0 ? (
                <EmptyState
                  compact
                  title="No jurisdictions matched"
                  description="Try a broader filter or disable one of the focus toggles."
                />
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
          </Surface>

          <div className="oh-jur-detail-stack">
            {!selectedRow ? (
              <Surface
                title="Market detail"
                subtitle="Select a jurisdiction to inspect its real compliance posture."
              >
                <EmptyState
                  compact
                  title="No jurisdiction selected"
                  description="Choose a market from the left column."
                />
              </Surface>
            ) : (
              <>
                <Surface
                  title={`${selectedRow.city || selectedRow.county || selectedRow.pha_name || selectedRow.state} details`}
                  subtitle="Pipeline workspace for the selected market."
                  actions={
                    detailBusy ? (
                      <span className="text-xs text-app-4">Loading…</span>
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
                >
                  <div className="rounded-2xl border border-cyan-400/15 bg-cyan-500/8 p-3 text-sm text-cyan-100">
                    Use the buttons below to refresh sources, run extraction,
                    rebuild the profile, clean stale items, or repair the whole
                    market.
                  </div>

                  <div className="mt-4 flex flex-wrap gap-2">
                    <ActionButton
                      label="Repair market"
                      busy={marketBusy === `repair:${marketKey(selectedRow)}`}
                      onClick={() => runMarketAction("repair", selectedRow)}
                      tone="primary"
                    />
                    <ActionButton
                      label="Run pipeline"
                      busy={marketBusy === `pipeline:${marketKey(selectedRow)}`}
                      onClick={() => runMarketAction("pipeline", selectedRow)}
                      tone="primary"
                    />
                    <ActionButton
                      label="Refresh sources"
                      busy={marketBusy === `collect:${marketKey(selectedRow)}`}
                      onClick={() => runMarketAction("collect", selectedRow)}
                    />
                    <ActionButton
                      label="Rebuild profile"
                      busy={marketBusy === `build:${marketKey(selectedRow)}`}
                      onClick={() => runMarketAction("build", selectedRow)}
                    />
                    <ActionButton
                      label="Refresh coverage"
                      busy={marketBusy === `coverage:${marketKey(selectedRow)}`}
                      onClick={() => runMarketAction("coverage", selectedRow)}
                    />
                    <ActionButton
                      label="Resolve stale items"
                      busy={marketBusy === `cleanup:${marketKey(selectedRow)}`}
                      onClick={() => runMarketAction("cleanup", selectedRow)}
                      tone="danger"
                    />
                    <ActionButton
                      label="Manage source pack"
                      onClick={() => setSourcePackOpen(true)}
                    />
                    <ActionButton
                      label={
                        showSourceList ? "Hide source list" : "View source list"
                      }
                      onClick={() => setShowSourceList((v) => !v)}
                    />
                    <ActionButton
                      label={
                        showAssertionList
                          ? "Hide assertions list"
                          : "View assertions list"
                      }
                      onClick={() => setShowAssertionList((v) => !v)}
                    />
                  </div>

                  <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
                    <div className="rounded-2xl border border-app bg-app-muted p-4">
                      <div className="space-y-2">
                        <Row label="State" value={selectedRow.state || "MI"} />
                        <Row
                          label="County"
                          value={titleCase(selectedRow.county)}
                        />
                        <Row label="City" value={titleCase(selectedRow.city)} />
                        <Row label="PHA" value={selectedRow.pha_name || "—"} />
                        <Row
                          label="Coverage"
                          value={selectedRow.coverage_status || "unknown"}
                        />
                        <Row
                          label="Verified rules"
                          value={selectedRow.verified_rule_count ?? 0}
                        />
                      </div>
                    </div>

                    <div className="rounded-2xl border border-app bg-app-muted p-4">
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
                          value={
                            selectedRow.municipal_core_ok ? "ok" : "missing"
                          }
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

                  {brief?.explanation ? (
                    <div className="mt-4 rounded-2xl border border-app bg-app-panel p-4 text-sm leading-6 text-app-2">
                      {brief.explanation}
                    </div>
                  ) : null}
                </Surface>

                <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                  {showSourceList ? (
                    <Surface
                      title={`Source list (${selectedSources.length})`}
                      subtitle="Tracked source records for this market."
                      actions={
                        <Badge>{selectedRow.source_count ?? 0} tracked</Badge>
                      }
                    >
                      <div className="oh-jur-scroll space-y-2">
                        {selectedSources.length === 0 ? (
                          <EmptyState compact title="No sources loaded yet" />
                        ) : (
                          selectedSources.map((s: any) => (
                            <div
                              key={s.id}
                              className="rounded-2xl border border-app bg-app-panel p-3"
                            >
                              <div className="flex items-start justify-between gap-3">
                                <div className="min-w-0">
                                  <div className="text-sm font-medium text-app-0">
                                    {s.title ||
                                      s.publisher ||
                                      s.url ||
                                      `Source ${s.id}`}
                                  </div>
                                  <div className="mt-1 text-xs text-app-4">
                                    {s.publisher || "Unknown publisher"} • HTTP{" "}
                                    {s.http_status ?? "—"}
                                  </div>
                                </div>
                                <Badge>{s.id}</Badge>
                              </div>
                              <div className="mt-2 break-all text-xs text-app-4">
                                {s.url || "No URL"}
                              </div>
                              {s.notes ? (
                                <div className="mt-2 text-sm text-app-2">
                                  {s.notes}
                                </div>
                              ) : null}
                            </div>
                          ))
                        )}
                      </div>
                    </Surface>
                  ) : null}

                  {showAssertionList ? (
                    <Surface
                      title={`Assertions list (${selectedAssertions.length})`}
                      subtitle="Extracted and reviewed rule assertions."
                      actions={
                        <Badge>
                          {selectedRow.verified_rule_count ?? 0} verified
                        </Badge>
                      }
                    >
                      <div className="oh-jur-scroll space-y-2">
                        {selectedAssertions.length === 0 ? (
                          <EmptyState
                            compact
                            title="No assertions loaded yet"
                          />
                        ) : (
                          selectedAssertions.map((a: any) => (
                            <div
                              key={a.id}
                              className="rounded-2xl border border-app bg-app-panel p-3"
                            >
                              <div className="flex items-start justify-between gap-3">
                                <div className="min-w-0">
                                  <div className="text-sm font-medium text-app-0">
                                    {a.rule_key}
                                  </div>
                                  <div className="mt-1 text-xs text-app-4">
                                    {a.rule_family || "unknown family"} •{" "}
                                    {a.assertion_type || "unknown type"} •
                                    source {a.source_id ?? "—"}
                                  </div>
                                </div>
                                <div className="flex flex-wrap gap-2">
                                  <Badge
                                    tone={
                                      a.review_status === "verified"
                                        ? "good"
                                        : a.review_status === "superseded"
                                          ? "warn"
                                          : "neutral"
                                    }
                                  >
                                    {a.review_status}
                                  </Badge>
                                  <Badge>
                                    {Number(a.confidence ?? 0).toFixed(2)}
                                  </Badge>
                                </div>
                              </div>
                              <pre className="oh-jur-code mt-3">
                                {JSON.stringify(a.value ?? {}, null, 2)}
                              </pre>
                              {a.review_notes ? (
                                <div className="mt-2 text-sm text-app-2">
                                  {a.review_notes}
                                </div>
                              ) : null}
                            </div>
                          ))
                        )}
                      </div>
                    </Surface>
                  ) : null}
                </div>

                <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                  <Surface
                    title="Required actions"
                    subtitle="Operator tasks surfaced by the brief."
                  >
                    <div className="oh-jur-scroll space-y-2">
                      {(brief?.required_actions ?? []).length === 0 ? (
                        <EmptyState
                          compact
                          title="No required actions returned"
                        />
                      ) : (
                        (brief?.required_actions ?? []).map(
                          (item: any, idx: number) => (
                            <div
                              key={`${item?.code || item?.key || item?.title || idx}`}
                              className="rounded-2xl border border-app bg-app-panel p-3"
                            >
                              <div className="text-sm font-medium text-app-0">
                                {item?.title ||
                                  item?.description ||
                                  item?.code ||
                                  item?.key ||
                                  "Untitled action"}
                              </div>
                              <div className="mt-1 text-xs text-app-4">
                                {(
                                  item?.category ||
                                  item?.severity ||
                                  "uncategorized"
                                ).toString()}{" "}
                                • code: {item?.code || item?.key || "—"}
                              </div>
                            </div>
                          ),
                        )
                      )}
                    </div>
                  </Surface>

                  <Surface
                    title="Blocking items"
                    subtitle="These are still in the way."
                  >
                    <div className="oh-jur-scroll space-y-2">
                      {(brief?.blocking_items ?? []).length === 0 ? (
                        <EmptyState compact title="No blockers returned" />
                      ) : (
                        (brief?.blocking_items ?? []).map(
                          (item: any, idx: number) => (
                            <div
                              key={`${item?.code || item?.key || item?.title || idx}`}
                              className="rounded-2xl border border-red-500/20 bg-red-500/[0.06] p-3"
                            >
                              <div className="text-sm font-medium text-red-200">
                                {item?.title ||
                                  item?.description ||
                                  item?.code ||
                                  item?.key ||
                                  "Untitled blocker"}
                              </div>
                              <div className="mt-1 text-xs text-red-200/70">
                                {(
                                  item?.category ||
                                  item?.severity ||
                                  "uncategorized"
                                ).toString()}{" "}
                                • code: {item?.code || item?.key || "—"}
                              </div>
                            </div>
                          ),
                        )
                      )}
                    </div>
                  </Surface>
                </div>

                <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                  <Surface
                    title="Resolved jurisdiction profiles"
                    subtitle="Explicit matched profile rows."
                  >
                    <div className="oh-jur-scroll space-y-2">
                      {selectedProfiles.length === 0 ? (
                        <EmptyState
                          compact
                          title="No explicit jurisdiction profile rows matched this market"
                        />
                      ) : (
                        selectedProfiles.map((p) => (
                          <div
                            key={p.id}
                            className="rounded-2xl border border-app bg-app-panel p-3"
                          >
                            <div className="flex items-center justify-between gap-3">
                              <div className="text-sm font-medium text-app-0">
                                Profile #{p.id}
                              </div>
                              <Badge tone={p.org_id ? "warn" : "neutral"}>
                                {p.org_id ? "org override" : "global"}
                              </Badge>
                            </div>
                            <div className="mt-2 text-xs text-app-4">
                              friction: {p.friction_multiplier ?? "—"} • PHA:{" "}
                              {p.pha_name || "—"}
                            </div>
                            {p.notes ? (
                              <div className="mt-2 text-sm text-app-2">
                                {p.notes}
                              </div>
                            ) : null}
                          </div>
                        ))
                      )}
                    </div>
                  </Surface>

                  <Surface
                    title="Legacy jurisdiction rules"
                    subtitle="Fallback-only legacy rule rows."
                  >
                    <div className="oh-jur-scroll space-y-2">
                      {selectedLegacyRules.length === 0 ? (
                        <EmptyState
                          compact
                          title="No legacy rule rows matched this market"
                        />
                      ) : (
                        selectedLegacyRules.map((r) => {
                          const failPoints = parseMaybeJsonArray(
                            r.typical_fail_points_json ?? r.typical_fail_points,
                          );

                          return (
                            <div
                              key={r.id}
                              className="rounded-2xl border border-app bg-app-panel p-3"
                            >
                              <div className="flex items-center justify-between gap-3">
                                <div className="text-sm font-medium text-app-0">
                                  Legacy rule #{r.id}
                                </div>
                                <Badge tone={r.org_id ? "warn" : "neutral"}>
                                  {r.org_id ? "org override" : "global"}
                                </Badge>
                              </div>
                              <div className="mt-2 text-xs text-app-4">
                                license: {String(!!r.rental_license_required)} •
                                authority: {r.inspection_authority || "—"} •
                                freq: {r.inspection_frequency || "—"}
                              </div>
                              <div className="mt-2 text-sm text-app-2">
                                fail points:{" "}
                                {failPoints.length
                                  ? failPoints.join(", ")
                                  : "—"}
                              </div>
                            </div>
                          );
                        })
                      )}
                    </div>
                  </Surface>
                </div>
              </>
            )}
          </div>
        </div>

        <MarketSourcePackModal
          open={sourcePackOpen}
          market={
            selectedRow
              ? {
                  state: selectedRow.state || "MI",
                  county: selectedRow.county ?? null,
                  city: selectedRow.city ?? null,
                  pha_name: selectedRow.pha_name ?? null,
                }
              : null
          }
          onClose={() => setSourcePackOpen(false)}
          onChanged={async () => {
            await refresh();
            if (selectedRow) {
              await loadDetail(selectedRow);
            }
          }}
        />
      </div>
    </PageShell>
  );
}
