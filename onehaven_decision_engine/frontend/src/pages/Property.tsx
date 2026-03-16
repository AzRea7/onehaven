import React from "react";
import { Link, useLocation } from "react-router-dom";
import {
  ArrowUpRight,
  Banknote,
  Filter,
  Home,
  ShieldAlert,
  Wallet,
} from "lucide-react";

import { api } from "../lib/api";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import GlobalFilters from "../components/GlobalFilters";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import { filtersToApiParams, readFilters } from "../lib/filters";

type Row = any;

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

function clsDecision(d?: string) {
  const x = (d || "REJECT").toUpperCase();
  if (x === "PASS") return "badge badge-success";
  if (x === "REVIEW") return "badge badge-warning";
  return "badge badge-danger";
}

function clsStage(s?: string) {
  const x = (s || "").toLowerCase();
  if (x === "equity") return "badge badge-success";
  if (x === "cash" || x === "lease") return "badge badge-accent";
  if (x === "tenant" || x === "compliance") return "badge badge-warning";
  return "badge";
}

function getFinancingType(price?: number | null) {
  if (price == null || !Number.isFinite(Number(price))) return "Unknown";
  if (Number(price) < 75000) return "Cash";
  return "DSCR";
}

type FinancingFilter = "ALL" | "CASH" | "DSCR";

export default function Properties() {
  const [rows, setRows] = React.useState<Row[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);

  const [q, setQ] = React.useState("");
  const deferredQ = React.useDeferredValue(q);

  const [decision, setDecision] = React.useState<
    "ALL" | "PASS" | "REVIEW" | "REJECT"
  >("ALL");
  const [financing, setFinancing] = React.useState<FinancingFilter>("ALL");

  const location = useLocation();
  const abortRef = React.useRef<AbortController | null>(null);

  const filters = React.useMemo(() => {
    return readFilters(new URLSearchParams(location.search));
  }, [location.search]);

  const apiFilterParams = React.useMemo(() => {
    return filtersToApiParams(filters);
  }, [filters]);

  const refresh = React.useCallback(async () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);
      setLoading(true);
      const out = await api.properties(apiFilterParams, ac.signal);
      setRows(Array.isArray(out) ? out : []);
    } catch (e: any) {
      if (String(e?.name) === "AbortError") return;
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, [apiFilterParams]);

  React.useEffect(() => {
    refresh();
    return () => abortRef.current?.abort();
  }, [refresh]);

  const filtered = React.useMemo(() => {
    const needle = deferredQ.trim().toLowerCase();

    return (rows || []).filter((r) => {
      const p = r?.property || r || {};
      const deal = r?.deal || {};
      const u = r?.last_underwriting_result || {};
      const d = (u?.decision || "REJECT").toUpperCase();

      const hay =
        `${p.address || ""} ${p.city || ""} ${p.state || ""} ${p.zip || ""}`.toLowerCase();

      if (needle && !hay.includes(needle)) return false;
      if (decision !== "ALL" && d !== decision) return false;

      const priceRaw =
        deal?.asking_price ??
        deal?.price ??
        p?.price ??
        u?.asking_price ??
        u?.estimated_purchase_price ??
        null;

      const fin = getFinancingType(priceRaw == null ? null : Number(priceRaw));
      if (financing === "CASH") return fin === "Cash";
      if (financing === "DSCR") return fin === "DSCR";
      return true;
    });
  }, [rows, deferredQ, decision, financing]);

  const counts = React.useMemo(() => {
    const c = { PASS: 0, REVIEW: 0, REJECT: 0 };
    for (const r of rows || []) {
      const d = (
        r?.last_underwriting_result?.decision || "REJECT"
      ).toUpperCase();
      if (d === "PASS") c.PASS++;
      else if (d === "REVIEW") c.REVIEW++;
      else c.REJECT++;
    }
    return c;
  }, [rows]);

  return (
    <PageShell>
      <div className="app-stack">
        <PageHero
          eyebrow="Portfolio inventory"
          title="Properties"
          subtitle="A cleaner property pipeline: glanceable decisions, financing posture, stage, and the fastest click path into the cockpit."
          actions={
            <>
              <button onClick={refresh} className="btn btn-secondary">
                Refresh
              </button>
              <span className="badge badge-success">PASS {counts.PASS}</span>
              <span className="badge badge-warning">
                REVIEW {counts.REVIEW}
              </span>
              <span className="badge badge-danger">REJECT {counts.REJECT}</span>
            </>
          }
        />

        <GlobalFilters />

        <Surface
          title="Local property filters"
          subtitle="Fast client-side narrowing on top of your global URL filters."
          padding="md"
        >
          <div className="grid gap-3 md:grid-cols-3">
            <label className="field">
              <span className="field-label">Search</span>
              <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder="Search address / city / zip"
                className="field-input"
              />
            </label>

            <label className="field">
              <span className="field-label">Decision</span>
              <select
                value={decision}
                onChange={(e) => setDecision(e.target.value as any)}
                className="field-input"
              >
                <option value="ALL">All decisions</option>
                <option value="PASS">PASS</option>
                <option value="REVIEW">REVIEW</option>
                <option value="REJECT">REJECT</option>
              </select>
            </label>

            <label className="field">
              <span className="field-label">Financing</span>
              <select
                value={financing}
                onChange={(e) => setFinancing(e.target.value as any)}
                className="field-input"
              >
                <option value="ALL">All financing</option>
                <option value="CASH">Cash</option>
                <option value="DSCR">DSCR</option>
              </select>
            </label>
          </div>
        </Surface>

        {err ? (
          <Surface tone="danger" padding="md">
            <div className="text-sm text-red-300">{err}</div>
          </Surface>
        ) : null}

        <Surface
          title="Property list"
          subtitle={`${filtered.length} visible ${
            filtered.length === 1 ? "property" : "properties"
          }`}
          padding="md"
        >
          {loading ? (
            <div className="grid gap-3">
              {Array.from({ length: 6 }).map((_, i) => (
                <div key={i} className="skeleton h-[120px] rounded-3xl" />
              ))}
            </div>
          ) : !filtered.length ? (
            <EmptyState
              icon={Filter}
              title="No properties matched"
              description="Adjust the global or local filters. The list is working; it's just being ruthlessly obedient."
            />
          ) : (
            <div className="grid gap-4">
              {filtered.map((r: any) => {
                const p = r.property || r || {};
                const deal = r.deal || {};
                const u = r.last_underwriting_result || {};
                const decisionTxt = (u.decision || "REJECT").toUpperCase();

                const stage =
                  r?.workflow?.current_stage ||
                  r?.property_state?.current_stage ||
                  r?.stage ||
                  "deal";

                const financingType = getFinancingType(
                  deal?.asking_price ?? deal?.price ?? p?.price ?? null,
                );

                return (
                  <Link
                    key={p.id}
                    to={`/properties/${p.id}`}
                    className="group block rounded-3xl border border-app bg-app-panel px-5 py-5 shadow-soft hover:-translate-y-[1px] hover:border-app-strong hover:shadow-soft-lg"
                  >
                    <div className="grid gap-5 xl:grid-cols-[1.4fr_0.9fr]">
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
                              {p.bedrooms != null ? ` · ${p.bedrooms}bd` : ""}
                              {deal.strategy
                                ? ` · ${(deal.strategy as string).toUpperCase()}`
                                : ""}
                            </div>
                          </div>

                          <div className="flex items-center gap-2 text-app-4 group-hover:text-app-1">
                            <span className="text-sm">Open</span>
                            <ArrowUpRight className="h-4 w-4" />
                          </div>
                        </div>

                        <div className="mt-4 flex flex-wrap gap-2">
                          <span className={clsDecision(decisionTxt)}>
                            {decisionTxt}
                          </span>
                          <span className={clsStage(stage)}>
                            {stage.replaceAll("_", " ")}
                          </span>
                          <span className="badge">{financingType}</span>
                          {p.red_zone ? (
                            <span className="badge badge-danger">Red zone</span>
                          ) : null}
                        </div>
                      </div>

                      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-2">
                        <div className="rounded-2xl border border-app px-4 py-3">
                          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                            <Wallet className="h-3.5 w-3.5" />
                            Cash flow
                          </div>
                          <div className="mt-2 text-base font-semibold text-app-0">
                            {u.cash_flow != null ? money(u.cash_flow) : "—"}
                          </div>
                        </div>

                        <div className="rounded-2xl border border-app px-4 py-3">
                          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                            <Banknote className="h-3.5 w-3.5" />
                            Price
                          </div>
                          <div className="mt-2 text-base font-semibold text-app-0">
                            {money(
                              deal?.asking_price ?? deal?.price ?? p?.price,
                            )}
                          </div>
                        </div>

                        <div className="rounded-2xl border border-app px-4 py-3">
                          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                            <Home className="h-3.5 w-3.5" />
                            DSCR
                          </div>
                          <div className="mt-2 text-base font-semibold text-app-0">
                            {u.dscr != null ? Number(u.dscr).toFixed(2) : "—"}
                          </div>
                        </div>

                        <div className="rounded-2xl border border-app px-4 py-3">
                          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                            <ShieldAlert className="h-3.5 w-3.5" />
                            Crime
                          </div>
                          <div className="mt-2 text-base font-semibold text-app-0">
                            {p.crime_score != null
                              ? Number(p.crime_score).toFixed(1)
                              : "—"}
                          </div>
                        </div>
                      </div>
                    </div>
                  </Link>
                );
              })}
            </div>
          )}
        </Surface>
      </div>
    </PageShell>
  );
}
