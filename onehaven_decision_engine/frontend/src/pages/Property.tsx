import React from "react";
import { Link, useLocation } from "react-router-dom";
import { api } from "../lib/api";
import PageHero from "../components/PageHero";
import VirtualList from "../components/VirtualList";
import PageShell from "../components/PageShell";
import GlobalFilters from "../components/GlobalFilters";
import { filtersToApiParams, readFilters } from "../lib/filters";

type Row = any;

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

function clsDecision(d?: string) {
  const x = (d || "REJECT").toUpperCase();
  if (x === "PASS") return "border-green-400/25 bg-green-400/10 text-green-200";
  if (x === "REVIEW")
    return "border-yellow-300/25 bg-yellow-300/10 text-yellow-100";
  return "border-red-400/25 bg-red-400/10 text-red-200";
}

function getFinancingType(price?: number | null) {
  if (price == null || !Number.isFinite(Number(price))) return "unknown";
  if (Number(price) < 75000) return "CASH DEAL";
  return "DSCR LOAN";
}

type FinancingFilter = "ALL" | "CASH" | "DSCR";

function inputClass() {
  return [
    "w-full rounded-xl border border-white/10 bg-white/[0.04]",
    "px-3 py-2.5 text-sm text-white/90 placeholder:text-white/40",
    "outline-none transition",
    "focus:border-white/20 focus:ring-2 focus:ring-white/10",
  ].join(" ");
}

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

      if (financing === "CASH") return fin === "CASH DEAL";
      if (financing === "DSCR") return fin === "DSCR LOAN";
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

  const rowHeight = 86;

  const renderRow = React.useCallback((r: any) => {
    const p = r.property || r || {};
    const deal = r.deal || {};
    const u = r.last_underwriting_result || {};
    const decisionTxt = (u.decision || "REJECT").toUpperCase();

    return (
      <Link
        to={`/properties/${p.id}`}
        className="block rounded-xl border border-transparent hover:border-white/10 hover:bg-white/[0.03] transition px-3 py-3"
        style={{ contain: "layout paint" }}
      >
        <div className="grid grid-cols-[1fr_120px_120px_120px] gap-2 items-center">
          <div className="min-w-0">
            <div className="text-sm font-semibold text-white truncate">
              {p.address || `Property #${p.id}`}
            </div>
            <div className="text-xs text-white/50 truncate mt-0.5">
              {p.city ? `${p.city}, ${p.state || ""} ${p.zip || ""}` : "—"}
              {p.bedrooms != null ? ` · ${p.bedrooms}bd` : ""}
              {deal.strategy
                ? ` · ${(deal.strategy as string).toUpperCase()}`
                : ""}
            </div>
          </div>

          <div className="text-right">
            <span
              className={`inline-flex rounded-full border px-3 py-1 text-xs ${clsDecision(decisionTxt)}`}
            >
              {decisionTxt}
              {u.score != null ? ` · ${u.score}` : ""}
            </span>
          </div>

          <div className="text-right text-sm text-white/80 font-semibold">
            {u.dscr != null ? Number(u.dscr).toFixed(2) : "—"}
          </div>

          <div className="text-right text-sm text-white/80 font-semibold">
            {u.cash_flow != null ? money(u.cash_flow) : "—"}
          </div>
        </div>
      </Link>
    );
  }, []);

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Portfolio"
          title="Properties"
          subtitle="Scan and triage. Filter by decision, search by address, then click into the cockpit view."
          actions={
            <>
              <button
                onClick={refresh}
                className="oh-btn cursor-pointer"
                title="Refresh"
              >
                sync
              </button>
              <span className="oh-badge border-green-400/25 bg-green-400/10 text-green-200">
                PASS {counts.PASS}
              </span>
              <span className="oh-badge border-yellow-300/25 bg-yellow-300/10 text-yellow-100">
                REVIEW {counts.REVIEW}
              </span>
              <span className="oh-badge border-red-400/25 bg-red-400/10 text-red-200">
                REJECT {counts.REJECT}
              </span>
            </>
          }
        />

        <GlobalFilters />

        {err && (
          <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
            {err}
          </div>
        )}

        {loading ? (
          <div className="oh-panel p-5">
            <div className="text-sm text-white/70">Loading properties…</div>
          </div>
        ) : filtered.length === 0 ? (
          <div className="oh-panel p-5">
            <div className="text-sm text-white/70">No matching properties.</div>
            <div className="text-xs text-white/45 mt-2">
              Try clearing filters or import/create deals first.
            </div>
          </div>
        ) : (
          <div className="oh-panel p-2" style={{ contain: "layout paint" }}>
            <div className="grid grid-cols-[1fr_120px_120px_120px] gap-2 px-3 py-2 text-[11px] uppercase tracking-[0.22em] text-white/45">
              <div>Property</div>
              <div className="text-right">Decision</div>
              <div className="text-right">DSCR</div>
              <div className="text-right">Cash Flow</div>
            </div>

            <div className="hr my-1" />

            <VirtualList
              items={filtered}
              itemHeight={rowHeight}
              overscan={8}
              itemKey={(r) => String(r?.property?.id ?? r?.id ?? Math.random())}
              className="rounded-xl"
              style={{ height: "calc(100vh - 430px)" }}
              renderRow={(item) => renderRow(item)}
            />
          </div>
        )}
      </div>
    </PageShell>
  );
}
