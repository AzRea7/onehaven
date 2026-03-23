import React from "react";
import { Link } from "react-router-dom";
import {
  ArrowUpRight,
  Banknote,
  ClipboardList,
  Crosshair,
  Filter,
  GitBranch,
  Home,
  LocateFixed,
  MapPinned,
  RefreshCcw,
  Search,
  ShieldAlert,
  Wallet,
  AlertTriangle,
} from "lucide-react";

import { api } from "../lib/api";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import Golem from "../components/Golem";
import IngestionLaunchCard from "../components/IngestionLaunchCard";
import IngestionRunsPanel from "../components/IngestionRunsPanel";
import IngestionErrorsDrawer from "../components/IngestionErrorsDrawer";
import { paneLabel } from "../components/PaneSwitcher";

type Row = any;

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

function numberOrNull(v: any) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
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

function resolvePropertyId(r: any) {
  const candidates = [
    r?.id,
    r?.property_id,
    r?.property?.id,
    r?.propertyId,
    r?.property?.property_id,
  ];

  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }

  return null;
}

function decisionPillClass(raw?: string) {
  const d = normalizeDecision(raw);
  if (d === "GOOD_DEAL") return "oh-pill oh-pill-good";
  if (d === "REVIEW") return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-bad";
}

function normalizeStage(raw?: string) {
  const x = String(raw || "")
    .trim()
    .toLowerCase();

  if (
    [
      "deal",
      "intake",
      "sourcing",
      "procurement",
      "underwriting",
      "discovered",
      "shortlisted",
      "underwritten",
      "offer",
    ].includes(x)
  ) {
    return "deal";
  }
  if (["rehab", "renovation", "construction", "acquired"].includes(x))
    return "rehab";
  if (
    [
      "compliance",
      "inspection",
      "licensing",
      "compliance_readying",
      "inspection_pending",
    ].includes(x)
  ) {
    return "compliance";
  }
  if (["tenant", "voucher", "tenant_marketing", "tenant_screening"].includes(x))
    return "tenant";
  if (["lease", "leasing", "leased"].includes(x)) return "lease";
  if (["management", "ops", "maintenance", "occupied", "turnover"].includes(x))
    return "management";
  if (["cash", "cashflow", "equity", "portfolio"].includes(x)) {
    return "cash_equity";
  }

  return "deal";
}

function stageLabel(raw?: string) {
  const s = normalizeStage(raw);
  if (s === "deal") return "Deal / Procurement";
  if (s === "rehab") return "Rehab";
  if (s === "compliance") return "Compliance";
  if (s === "tenant") return "Tenant Placement";
  if (s === "lease") return "Lease Activation";
  if (s === "management") return "Management";
  return "Cashflow / Equity";
}

function stagePillClass(raw?: string) {
  const s = normalizeStage(raw);
  if (s === "cash_equity") return "oh-pill oh-pill-good";
  if (s === "lease" || s === "management") return "oh-pill oh-pill-accent";
  if (s === "tenant" || s === "compliance" || s === "rehab") {
    return "oh-pill oh-pill-warn";
  }
  return "oh-pill";
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

function getFinancingType(price?: number | null) {
  if (price == null || !Number.isFinite(Number(price))) return "Unknown";
  if (Number(price) < 75000) return "Cash";
  return "DSCR";
}

function inferCashflow(r: any) {
  const direct =
    r?.cashflow_estimate ??
    r?.projected_monthly_cashflow ??
    r?.last_underwriting_result?.cash_flow ??
    r?.last_underwriting_result?.cashflow ??
    r?.property_net_cash_window ??
    r?.metrics?.cashflow_estimate;

  const n = numberOrNull(direct);
  if (n != null) return n;

  const rent =
    numberOrNull(r?.market_rent_estimate) ??
    numberOrNull(r?.rent_assumption?.market_rent_estimate) ??
    0;
  const rehabOpen = numberOrNull(r?.rehab_open_cost) ?? 0;
  if (rent > 0) return rent - rehabOpen / 12;

  return null;
}

function inferAskingPrice(r: any) {
  return (
    numberOrNull(r?.asking_price) ??
    numberOrNull(r?.deal?.asking_price) ??
    numberOrNull(r?.deal?.price) ??
    numberOrNull(r?.property?.price) ??
    null
  );
}

function inferDscr(r: any) {
  return (
    numberOrNull(r?.dscr) ??
    numberOrNull(r?.last_underwriting_result?.dscr) ??
    null
  );
}

function inferCrime(r: any) {
  return (
    numberOrNull(r?.crime_score) ??
    numberOrNull(r?.property?.crime_score) ??
    null
  );
}

function inferStage(r: any) {
  return (
    r?.current_stage ||
    r?.stage ||
    r?.stage_label ||
    r?.workflow?.current_stage ||
    r?.property_state?.current_stage ||
    r?.property?.current_stage ||
    "deal"
  );
}

function inferStageLabel(r: any) {
  return (
    r?.current_stage_label ||
    r?.workflow?.current_stage_label ||
    r?.property_state?.current_stage_label ||
    stageLabel(inferStage(r))
  );
}

function inferDecision(r: any) {
  return normalizeDecision(
    r?.normalized_decision ||
      r?.classification ||
      r?.latest_decision ||
      r?.raw_decision ||
      r?.last_underwriting_result?.decision,
  );
}

function inferProperty(r: any) {
  return r?.property || r || {};
}

function inferLocationConfidence(r: any) {
  return (
    numberOrNull(r?.geocode_confidence) ??
    numberOrNull(r?.property?.geocode_confidence) ??
    null
  );
}

function inferLocationSource(r: any) {
  return r?.geocode_source || r?.property?.geocode_source || null;
}

function inferNormalizedAddress(r: any) {
  return r?.normalized_address || r?.property?.normalized_address || null;
}

function inferLat(r: any) {
  return numberOrNull(r?.lat) ?? numberOrNull(r?.property?.lat) ?? null;
}

function inferLng(r: any) {
  return numberOrNull(r?.lng) ?? numberOrNull(r?.property?.lng) ?? null;
}

function inferCounty(r: any) {
  return r?.county || r?.property?.county || null;
}

function inferCurrentPane(r: any) {
  return (
    r?.current_pane ||
    r?.workflow?.current_pane ||
    r?.property_state?.current_pane ||
    r?.pane?.current_pane ||
    "investor"
  );
}

function inferSuggestedPane(r: any) {
  return (
    r?.suggested_pane ||
    r?.workflow?.suggested_pane ||
    r?.property_state?.suggested_pane ||
    r?.pane?.suggested_pane ||
    inferCurrentPane(r)
  );
}

function inferRouteReason(r: any) {
  return (
    r?.route_reason ||
    r?.workflow?.route_reason ||
    r?.property_state?.route_reason ||
    r?.pane?.route_reason ||
    null
  );
}

function inferLocationStatus(r: any): {
  label: string;
  pillClass: string;
  detail: string;
} {
  const lat = inferLat(r);
  const lng = inferLng(r);
  const normalizedAddress = inferNormalizedAddress(r);
  const confidence = inferLocationConfidence(r);

  if (lat == null || lng == null) {
    return {
      label: "Location incomplete",
      pillClass: "oh-pill oh-pill-bad",
      detail: "Missing usable coordinates",
    };
  }

  if (!normalizedAddress) {
    return {
      label: "Location partial",
      pillClass: "oh-pill oh-pill-warn",
      detail: "Coordinates found but normalization missing",
    };
  }

  if (confidence != null && confidence < 0.7) {
    return {
      label: "Location approximate",
      pillClass: "oh-pill oh-pill-warn",
      detail: "Coordinates found with lower confidence",
    };
  }

  return {
    label: "Location verified",
    pillClass: "oh-pill oh-pill-good",
    detail: "Normalized and geocoded",
  };
}

type DecisionFilter = "ALL" | "GOOD_DEAL" | "REVIEW" | "REJECT";
type FinancingFilter = "ALL" | "CASH" | "DSCR";
type LocationFilter = "ALL" | "VERIFIED" | "PARTIAL" | "MISSING";
type PaneFilter =
  | "ALL"
  | "investor"
  | "acquisition"
  | "compliance"
  | "tenants"
  | "management";

export default function InvestorPane() {
  const [rows, setRows] = React.useState<Row[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [selectedRunId, setSelectedRunId] = React.useState<number | null>(null);
  const [ingestionRefreshKey, setIngestionRefreshKey] = React.useState(0);

  const [q, setQ] = React.useState("");
  const deferredQ = React.useDeferredValue(q);

  const [decision, setDecision] = React.useState<DecisionFilter>("ALL");
  const [financing, setFinancing] = React.useState<FinancingFilter>("ALL");
  const [locationFilter, setLocationFilter] =
    React.useState<LocationFilter>("ALL");
  const [paneFilter, setPaneFilter] = React.useState<PaneFilter>("ALL");

  const abortRef = React.useRef<AbortController | null>(null);

  const refresh = React.useCallback(async () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);
      setLoading(true);

      const out = await api.properties({}, ac.signal);

      const normalizedRows = Array.isArray(out)
        ? out
        : Array.isArray((out as any)?.items)
          ? (out as any).items
          : Array.isArray((out as any)?.rows)
            ? (out as any).rows
            : Array.isArray((out as any)?.properties)
              ? (out as any).properties
              : [];

      setRows(normalizedRows);
    } catch (e: any) {
      if (String(e?.name) === "AbortError") return;
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    refresh();
    return () => abortRef.current?.abort();
  }, [refresh]);

  const filtered = React.useMemo(() => {
    const needle = deferredQ.trim().toLowerCase();

    return (rows || []).filter((r) => {
      const p = inferProperty(r);
      const d = inferDecision(r);
      const price = inferAskingPrice(r);
      const financingType = getFinancingType(price);
      const locationStatus = inferLocationStatus(r);
      const currentPane = String(inferCurrentPane(r)).toLowerCase();

      const hay =
        `${p.address || ""} ${p.city || ""} ${p.state || ""} ${p.zip || ""} ${p.county || ""} ${
          inferNormalizedAddress(r) || ""
        } ${inferLocationSource(r) || ""} ${inferRouteReason(r) || ""}`.toLowerCase();

      if (needle && !hay.includes(needle)) return false;
      if (decision !== "ALL" && d !== decision) return false;
      if (financing === "CASH" && financingType !== "Cash") return false;
      if (financing === "DSCR" && financingType !== "DSCR") return false;
      if (paneFilter !== "ALL" && currentPane !== paneFilter) return false;

      if (
        locationFilter === "VERIFIED" &&
        locationStatus.label !== "Location verified"
      ) {
        return false;
      }
      if (
        locationFilter === "PARTIAL" &&
        locationStatus.label !== "Location partial" &&
        locationStatus.label !== "Location approximate"
      ) {
        return false;
      }
      if (
        locationFilter === "MISSING" &&
        locationStatus.label !== "Location incomplete"
      ) {
        return false;
      }

      return true;
    });
  }, [rows, deferredQ, decision, financing, locationFilter, paneFilter]);

  const counts = React.useMemo(() => {
    const c: Record<"GOOD_DEAL" | "REVIEW" | "REJECT", number> = {
      GOOD_DEAL: 0,
      REVIEW: 0,
      REJECT: 0,
    };
    for (const r of rows || []) {
      const d = inferDecision(r) as "GOOD_DEAL" | "REVIEW" | "REJECT";
      c[d] += 1;
    }
    return c;
  }, [rows]);

  const stageCounts = React.useMemo(() => {
    const out: Record<string, number> = {};
    for (const r of filtered) {
      const s = normalizeStage(inferStage(r));
      out[s] = (out[s] || 0) + 1;
    }
    return out;
  }, [filtered]);

  const locationCounts = React.useMemo(() => {
    const out = { verified: 0, partial: 0, missing: 0 };
    for (const r of rows || []) {
      const status = inferLocationStatus(r).label;
      if (status === "Location verified") out.verified += 1;
      else if (status === "Location incomplete") out.missing += 1;
      else out.partial += 1;
    }
    return out;
  }, [rows]);

  function refreshIngestion() {
    setIngestionRefreshKey((v) => v + 1);
    refresh().catch(() => undefined);
  }

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Investor pane"
          title="Investor inventory"
          subtitle="This is now the main investor workspace. Use it to search, shortlist mentally, inspect pane routing, and open the single-property workflow page."
          right={
            <div className="absolute inset-0 flex items-center justify-center overflow-visible pointer-events-auto">
              <div className="h-[220px] w-[220px] translate-y-[-8px] opacity-95 md:h-[250px] md:w-[250px]">
                <Golem className="h-full w-full" />
              </div>
            </div>
          }
          actions={
            <>
              <button onClick={refresh} className="oh-btn oh-btn-secondary">
                Refresh properties
              </button>
              <span className="oh-pill oh-pill-good">
                good deal {counts.GOOD_DEAL}
              </span>
              <span className="oh-pill oh-pill-warn">
                review {counts.REVIEW}
              </span>
              <span className="oh-pill oh-pill-bad">
                reject {counts.REJECT}
              </span>
            </>
          }
        />

        <div className="grid grid-cols-1 gap-6 xl:grid-cols-[1.28fr_0.92fr]">
          <div className="space-y-6">
            <Surface
              title="Acquisition intake"
              subtitle="Launch a focused intake run for southeast Michigan directly inside the investor workflow."
            >
              <IngestionLaunchCard
                refreshKey={ingestionRefreshKey}
                onQueued={refreshIngestion}
              />
            </Surface>

            <Surface
              title="Workflow progress"
              subtitle="A cleaner step path from possible deal to tenant-occupied cashflow."
            >
              <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
                {[
                  ["deal", "Deal / Procurement"],
                  ["rehab", "Rehab"],
                  ["compliance", "Compliance"],
                  ["tenant", "Tenant Placement"],
                  ["lease", "Lease Activation"],
                  ["cash_equity", "Cashflow / Equity"],
                ].map(([key, label]) => (
                  <div
                    key={key}
                    className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                  >
                    <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                      step
                    </div>
                    <div className="mt-2 text-sm font-semibold text-app-0">
                      {label}
                    </div>
                    <div className="mt-3 text-2xl font-semibold text-app-0">
                      {stageCounts[key] || 0}
                    </div>
                  </div>
                ))}
              </div>
            </Surface>

            <Surface
              title="Location quality"
              subtitle="Track which properties are ready for jurisdiction, risk, and rent workflows."
            >
              <div className="grid gap-3 md:grid-cols-3">
                <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                  <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                    <LocateFixed className="h-3.5 w-3.5" />
                    Verified
                  </div>
                  <div className="mt-3 text-2xl font-semibold text-app-0">
                    {locationCounts.verified}
                  </div>
                </div>

                <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                  <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                    <AlertTriangle className="h-3.5 w-3.5" />
                    Partial
                  </div>
                  <div className="mt-3 text-2xl font-semibold text-app-0">
                    {locationCounts.partial}
                  </div>
                </div>

                <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
                  <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                    <Crosshair className="h-3.5 w-3.5" />
                    Missing
                  </div>
                  <div className="mt-3 text-2xl font-semibold text-app-0">
                    {locationCounts.missing}
                  </div>
                </div>
              </div>
            </Surface>

            {err ? (
              <Surface tone="danger">
                <div className="text-sm text-red-300">{err}</div>
              </Surface>
            ) : null}

            <Surface
              title="Investor property list"
              subtitle={`${filtered.length} visible ${
                filtered.length === 1 ? "property" : "properties"
              }`}
            >
              <div className="mb-4 rounded-3xl border border-app bg-app-panel px-4 py-4">
                <div className="grid gap-3 lg:grid-cols-[1.15fr_0.7fr_0.7fr_0.8fr_0.8fr_auto]">
                  <label className="block">
                    <span className="oh-field-label">Search</span>
                    <div className="relative">
                      <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
                      <input
                        value={q}
                        onChange={(e) => setQ(e.target.value)}
                        placeholder="Search address, city, county, zip, normalized location"
                        className="oh-input pl-10"
                      />
                    </div>
                  </label>

                  <label className="block">
                    <span className="oh-field-label">Classification</span>
                    <select
                      value={decision}
                      onChange={(e) =>
                        setDecision(e.target.value as DecisionFilter)
                      }
                      className="oh-input"
                    >
                      <option value="ALL">All</option>
                      <option value="GOOD_DEAL">Good deal</option>
                      <option value="REVIEW">Review</option>
                      <option value="REJECT">Reject</option>
                    </select>
                  </label>

                  <label className="block">
                    <span className="oh-field-label">Financing</span>
                    <select
                      value={financing}
                      onChange={(e) =>
                        setFinancing(e.target.value as FinancingFilter)
                      }
                      className="oh-input"
                    >
                      <option value="ALL">All</option>
                      <option value="CASH">Cash</option>
                      <option value="DSCR">DSCR</option>
                    </select>
                  </label>

                  <label className="block">
                    <span className="oh-field-label">Location</span>
                    <select
                      value={locationFilter}
                      onChange={(e) =>
                        setLocationFilter(e.target.value as LocationFilter)
                      }
                      className="oh-input"
                    >
                      <option value="ALL">All</option>
                      <option value="VERIFIED">Verified</option>
                      <option value="PARTIAL">Partial / approximate</option>
                      <option value="MISSING">Missing</option>
                    </select>
                  </label>

                  <label className="block">
                    <span className="oh-field-label">Pane</span>
                    <select
                      value={paneFilter}
                      onChange={(e) =>
                        setPaneFilter(e.target.value as PaneFilter)
                      }
                      className="oh-input"
                    >
                      <option value="ALL">All panes</option>
                      <option value="investor">Investor</option>
                      <option value="acquisition">Acquisition</option>
                      <option value="compliance">Compliance</option>
                      <option value="tenants">Tenants</option>
                      <option value="management">Management</option>
                    </select>
                  </label>

                  <div className="flex items-end">
                    <button
                      onClick={refresh}
                      className="oh-btn oh-btn-secondary w-full lg:w-auto"
                    >
                      <RefreshCcw className="h-4 w-4" />
                      Refresh
                    </button>
                  </div>
                </div>
              </div>

              {loading ? (
                <div className="grid gap-3">
                  {Array.from({ length: 6 }).map((_, i) => (
                    <div
                      key={i}
                      className="oh-skeleton h-[190px] rounded-3xl"
                    />
                  ))}
                </div>
              ) : !filtered.length ? (
                <EmptyState
                  icon={Filter}
                  title="No properties matched"
                  description="Try a broader search or change the classification, financing, location, or pane filter."
                />
              ) : (
                <div className="max-h-[980px] overflow-y-auto pr-1">
                  <div className="grid gap-4">
                    {filtered.map((r: any) => {
                      const resolvedId = resolvePropertyId(r);
                      if (!resolvedId) return null;

                      const p = inferProperty(r);
                      const decisionTxt = inferDecision(r);
                      const stage = inferStage(r);
                      const stageTxt = inferStageLabel(r);

                      const askingPrice = inferAskingPrice(r);
                      const dscr = inferDscr(r);
                      const crime = inferCrime(r);
                      const cashflow = inferCashflow(r);

                      const financingType = getFinancingType(askingPrice);

                      const locationStatus = inferLocationStatus(r);
                      const locationSource = inferLocationSource(r);
                      const locationConfidence = inferLocationConfidence(r);
                      const normalizedAddress = inferNormalizedAddress(r);
                      const lat = inferLat(r);
                      const lng = inferLng(r);
                      const county = inferCounty(r);

                      const currentPane = inferCurrentPane(r);
                      const suggestedPane = inferSuggestedPane(r);
                      const routeReason = inferRouteReason(r);
                      const paneChanged =
                        String(currentPane).toLowerCase() !==
                        String(suggestedPane).toLowerCase();

                      return (
                        <Link
                          key={resolvedId}
                          to={`/properties/${resolvedId}`}
                          className="group block rounded-3xl border border-app bg-app-panel px-5 py-5 shadow-soft hover:-translate-y-[1px] hover:border-app-strong hover:shadow-soft-lg"
                        >
                          <div className="grid gap-5 xl:grid-cols-[1.35fr_0.95fr]">
                            <div className="min-w-0">
                              <div className="flex flex-wrap items-start justify-between gap-3">
                                <div className="min-w-0">
                                  <div className="truncate text-lg font-semibold text-app-0">
                                    {p.address || `Property #${resolvedId}`}
                                  </div>
                                  <div className="mt-1 truncate text-sm text-app-3">
                                    {p.city
                                      ? `${p.city}, ${p.state || ""} ${p.zip || ""}`
                                      : "—"}
                                    {county ? ` · ${county}` : ""}
                                    {p.bedrooms != null
                                      ? ` · ${p.bedrooms}bd`
                                      : ""}
                                    {p.bathrooms != null
                                      ? ` · ${Number(p.bathrooms).toFixed(1)}ba`
                                      : ""}
                                  </div>
                                </div>

                                <div className="flex items-center gap-2 text-app-4 group-hover:text-app-1">
                                  <ArrowUpRight className="h-4 w-4" />
                                </div>
                              </div>

                              <div className="mt-4 flex flex-wrap gap-2">
                                <span
                                  className={decisionPillClass(decisionTxt)}
                                >
                                  {decisionTxt.replace("_", " ")}
                                </span>
                                <span className={stagePillClass(stage)}>
                                  {stageTxt}
                                </span>
                                <span className={panePillClass(currentPane)}>
                                  {paneLabel(currentPane)}
                                </span>
                                <span className="oh-pill">{financingType}</span>
                                <span className={locationStatus.pillClass}>
                                  {locationStatus.label}
                                </span>
                              </div>

                              <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3">
                                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                                  <ClipboardList className="h-3.5 w-3.5" />
                                  Workflow gate
                                </div>
                                <div className="mt-2 text-sm text-app-2">
                                  Current stage:{" "}
                                  <span className="font-semibold text-app-0">
                                    {stageTxt}
                                  </span>
                                </div>
                                <div className="mt-2 flex flex-wrap gap-2">
                                  <span className={panePillClass(currentPane)}>
                                    current pane {paneLabel(currentPane)}
                                  </span>
                                  <span
                                    className={panePillClass(suggestedPane)}
                                  >
                                    target pane {paneLabel(suggestedPane)}
                                  </span>
                                  {paneChanged ? (
                                    <span className="oh-pill oh-pill-warn">
                                      move to next pane
                                    </span>
                                  ) : null}
                                </div>
                                <div className="mt-2 text-xs text-app-4">
                                  {routeReason ||
                                    "Open the property to continue the next action and move it toward the correct operating pane."}
                                </div>
                              </div>

                              <div className="mt-3 rounded-2xl border border-app px-4 py-3">
                                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                                  <MapPinned className="h-3.5 w-3.5" />
                                  Location automation
                                </div>

                                <div className="mt-2 flex flex-wrap gap-2">
                                  <span className={locationStatus.pillClass}>
                                    {locationStatus.label}
                                  </span>

                                  {locationSource ? (
                                    <span className="oh-pill">
                                      source {locationSource}
                                    </span>
                                  ) : null}

                                  {locationConfidence != null ? (
                                    <span className="oh-pill">
                                      confidence {locationConfidence.toFixed(2)}
                                    </span>
                                  ) : null}
                                </div>

                                <div className="mt-2 text-xs text-app-4">
                                  {locationStatus.detail}
                                </div>

                                {normalizedAddress ? (
                                  <div className="mt-2 text-sm text-app-2">
                                    {normalizedAddress}
                                  </div>
                                ) : (
                                  <div className="mt-2 text-sm text-app-4">
                                    Normalized address not available yet
                                  </div>
                                )}

                                {lat != null && lng != null ? (
                                  <div className="mt-2 text-xs text-app-4">
                                    {lat.toFixed(4)}, {lng.toFixed(4)}
                                  </div>
                                ) : null}
                              </div>
                            </div>

                            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-2">
                              <div className="rounded-2xl border border-app px-4 py-3">
                                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                                  <Banknote className="h-3.5 w-3.5" />
                                  Price
                                </div>
                                <div className="mt-2 text-base font-semibold text-app-0">
                                  {money(askingPrice)}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-app px-4 py-3">
                                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                                  <Home className="h-3.5 w-3.5" />
                                  DSCR
                                </div>
                                <div className="mt-2 text-base font-semibold text-app-0">
                                  {dscr != null ? dscr.toFixed(2) : "—"}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-app px-4 py-3">
                                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                                  <ShieldAlert className="h-3.5 w-3.5" />
                                  Crime
                                </div>
                                <div className="mt-2 text-base font-semibold text-app-0">
                                  {crime != null ? crime.toFixed(1) : "—"}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-app px-4 py-3">
                                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                                  <Wallet className="h-3.5 w-3.5" />
                                  Cash flow est.
                                </div>
                                <div className="mt-2 text-base font-semibold text-app-0">
                                  {money(cashflow)}
                                </div>
                              </div>

                              <div className="rounded-2xl border border-app px-4 py-3 sm:col-span-2">
                                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                                  <GitBranch className="h-3.5 w-3.5" />
                                  Pane routing
                                </div>
                                <div className="mt-2 flex flex-wrap gap-2">
                                  <span className={panePillClass(currentPane)}>
                                    now {paneLabel(currentPane)}
                                  </span>
                                  <span
                                    className={panePillClass(suggestedPane)}
                                  >
                                    next {paneLabel(suggestedPane)}
                                  </span>
                                </div>
                                <div className="mt-2 text-xs text-app-4">
                                  {routeReason ||
                                    "Routing detail will be shown when workflow state is available."}
                                </div>
                              </div>
                            </div>
                          </div>
                        </Link>
                      );
                    })}
                  </div>
                </div>
              )}
            </Surface>
          </div>

          <div className="space-y-6">
            <IngestionRunsPanel
              refreshKey={ingestionRefreshKey}
              onSelectRun={setSelectedRunId}
            />
          </div>
        </div>

        <IngestionErrorsDrawer
          runId={selectedRunId}
          onClose={() => setSelectedRunId(null)}
        />
      </div>
    </PageShell>
  );
}
