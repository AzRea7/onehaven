// frontend/src/pages/Property.tsx
import React from "react";
import { Link } from "react-router-dom";
import { api } from "../lib/api";
import PageHero from "../components/PageHero";
import VirtualList from "../components/VirtualList";
import PageShell from "../components/PageShell";
import FilterBar from "../components/FilterBar";

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

  const abortRef = React.useRef<AbortController | null>(null);

  const refresh = React.useCallback(async () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);
      setLoading(true);
      const out = await api.dashboardProperties({
        limit: 400,
        signal: ac.signal,
      });
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
      const p = r?.property || {};
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
    const p = r.property || {};
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
              <button onClick={refresh} className="oh-btn" title="Refresh">
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

        {/* Use your new FilterBar as the consistent control surface */}
        <FilterBar>
          <div className="flex items-center gap-3 flex-wrap w-full">
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Search address, city, zip…"
              className="oh-input focus-ring max-w-xl"
            />

            <select
              value={decision}
              onChange={(e) => setDecision(e.target.value as any)}
              className="oh-input focus-ring max-w-[220px]"
            >
              <option value="ALL">All decisions</option>
              <option value="PASS">PASS</option>
              <option value="REVIEW">REVIEW</option>
              <option value="REJECT">REJECT</option>
            </select>

            <select
              value={financing}
              onChange={(e) => setFinancing(e.target.value as any)}
              className="oh-input focus-ring max-w-[240px]"
              title="Filter by financing type (<$75k cash, >=$75k DSCR)"
            >
              <option value="ALL">All financing</option>
              <option value="CASH">Cash deals (&lt; $75k)</option>
              <option value="DSCR">DSCR loans (≥ $75k)</option>
            </select>

            <div className="text-xs text-white/45 ml-auto">
              Showing{" "}
              <span className="text-white/80 font-semibold">
                {filtered.length}
              </span>{" "}
              of{" "}
              <span className="text-white/80 font-semibold">{rows.length}</span>
            </div>
          </div>
        </FilterBar>

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
              itemKey={(r) => String(r?.property?.id ?? Math.random())}
              className="rounded-xl"
              style={{ height: "calc(100vh - 380px)" }}
              renderRow={(item) => renderRow(item)}
            />
          </div>
        )}
      </div>
    </PageShell>
  );
}
