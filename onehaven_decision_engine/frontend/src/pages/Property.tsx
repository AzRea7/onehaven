import React from "react";
import { Link } from "react-router-dom";
import {
  ArrowUpRight,
  Banknote,
  Home,
  Search,
  ShieldAlert,
  Wallet,
  RefreshCcw,
  Filter,
  ClipboardList,
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

  if (["deal", "intake", "sourcing", "procurement", "underwriting"].includes(x))
    return "deal";
  if (["rehab", "renovation", "construction"].includes(x)) return "rehab";
  if (["compliance", "inspection", "licensing"].includes(x))
    return "compliance";
  if (["tenant", "voucher"].includes(x)) return "tenant";
  if (["lease", "leasing"].includes(x)) return "lease";
  if (["management", "ops"].includes(x)) return "management";
  if (["cash", "cashflow", "equity", "portfolio"].includes(x))
    return "cash_equity";

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
  if (s === "tenant" || s === "compliance" || s === "rehab")
    return "oh-pill oh-pill-warn";
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
    r?.stage ||
    r?.stage_label ||
    r?.workflow?.current_stage ||
    r?.property_state?.current_stage ||
    r?.property?.current_stage ||
    "deal"
  );
}

function inferDecision(r: any) {
  return normalizeDecision(
    r?.classification ||
      r?.latest_decision ||
      r?.raw_decision ||
      r?.last_underwriting_result?.decision,
  );
}

function inferProperty(r: any) {
  return r?.property || r || {};
}

type DecisionFilter = "ALL" | "GOOD_DEAL" | "REVIEW" | "REJECT";
type FinancingFilter = "ALL" | "CASH" | "DSCR";

export default function Properties() {
  const [rows, setRows] = React.useState<Row[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [selectedRunId, setSelectedRunId] = React.useState<number | null>(null);
  const [ingestionRefreshKey, setIngestionRefreshKey] = React.useState(0);

  const [q, setQ] = React.useState("");
  const deferredQ = React.useDeferredValue(q);

  const [decision, setDecision] = React.useState<DecisionFilter>("ALL");
  const [financing, setFinancing] = React.useState<FinancingFilter>("ALL");

  const abortRef = React.useRef<AbortController | null>(null);

  const refresh = React.useCallback(async () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);
      setLoading(true);
      const out = await api.properties({}, ac.signal);
      setRows(Array.isArray(out) ? out : []);
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
      const hay =
        `${p.address || ""} ${p.city || ""} ${p.state || ""} ${p.zip || ""} ${p.county || ""}`.toLowerCase();

      if (needle && !hay.includes(needle)) return false;
      if (decision !== "ALL" && d !== decision) return false;
      if (financing === "CASH" && financingType !== "Cash") return false;
      if (financing === "DSCR" && financingType !== "DSCR") return false;

      return true;
    });
  }, [rows, deferredQ, decision, financing]);

  const counts = React.useMemo(() => {
    const c = { GOOD_DEAL: 0, REVIEW: 0, REJECT: 0 };
    for (const r of rows || []) {
      const d = inferDecision(r);
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

  function refreshIngestion() {
    setIngestionRefreshKey((v) => v + 1);
    refresh().catch(() => undefined);
  }

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Portfolio inventory"
          title="Properties"
          subtitle="Ingest, review, and move properties through a cleaner workflow from deal to cash-generating occupancy."
          right={
            <div className="absolute inset-0 flex items-center justify-center pointer-events-auto overflow-visible">
              <div className="h-[220px] w-[220px] md:h-[250px] md:w-[250px] translate-y-[-8px] opacity-95">
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
              subtitle="Launch a focused intake run for southeast Michigan directly inside the property workflow."
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

            {err ? (
              <Surface tone="danger">
                <div className="text-sm text-red-300">{err}</div>
              </Surface>
            ) : null}

            <Surface
              title="Property list"
              subtitle={`${filtered.length} visible ${filtered.length === 1 ? "property" : "properties"}`}
            >
              <div className="mb-4 rounded-3xl border border-app bg-app-panel px-4 py-4">
                <div className="grid gap-3 lg:grid-cols-[1.3fr_0.7fr_0.7fr_auto]">
                  <label className="block">
                    <span className="oh-field-label">Search</span>
                    <div className="relative">
                      <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
                      <input
                        value={q}
                        onChange={(e) => setQ(e.target.value)}
                        placeholder="Search address, city, county, zip"
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
                      className="oh-skeleton h-[120px] rounded-3xl"
                    />
                  ))}
                </div>
              ) : !filtered.length ? (
                <EmptyState
                  icon={Filter}
                  title="No properties matched"
                  description="Try a broader search or change the classification / financing filter."
                />
              ) : (
                <div className="max-h-[980px] overflow-y-auto pr-1">
                  <div className="grid gap-4">
                    {filtered.map((r: any) => {
                      const p = inferProperty(r);
                      const decisionTxt = inferDecision(r);
                      const stage = inferStage(r);

                      const askingPrice = inferAskingPrice(r);
                      const dscr = inferDscr(r);
                      const crime = inferCrime(r);
                      const cashflow = inferCashflow(r);

                      const financingType = getFinancingType(askingPrice);

                      return (
                        <Link
                          key={p.id}
                          to={`/properties/${p.id}`}
                          className="group block rounded-3xl border border-app bg-app-panel px-5 py-5 shadow-soft hover:-translate-y-[1px] hover:border-app-strong hover:shadow-soft-lg"
                        >
                          <div className="grid gap-5 xl:grid-cols-[1.4fr_0.95fr]">
                            <div className="min-w-0">
                              <div className="flex flex-wrap items-start justify-between gap-3">
                                <div className="min-w-0">
                                  <div className="truncate text-lg font-semibold text-app-0">
                                    {p.address || `Property #${p.id}`}
                                  </div>
                                  <div className="mt-1 truncate text-sm text-app-3">
                                    {p.city
                                      ? `${p.city}, ${p.state || ""} ${p.zip || ""}`
                                      : "—"}
                                    {p.county ? ` · ${p.county}` : ""}
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
                                  {stageLabel(stage)}
                                </span>
                                <span className="oh-pill">{financingType}</span>
                              </div>

                              <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3">
                                <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                                  <ClipboardList className="h-3.5 w-3.5" />
                                  Workflow gate
                                </div>
                                <div className="mt-2 text-sm text-app-2">
                                  Current stage:{" "}
                                  <span className="font-semibold text-app-0">
                                    {stageLabel(stage)}
                                  </span>
                                </div>
                                <div className="mt-1 text-xs text-app-4">
                                  Open the property to continue the next action
                                  and move it toward compliant occupancy and
                                  cashflow.
                                </div>
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
