import React from "react";
import { useParams } from "react-router-dom";
import { api } from "../lib/api";

const tabs = [
  "Deal",
  "Rehab",
  "Compliance",
  "Tenant",
  "Cash",
  "Equity",
] as const;
type Tab = (typeof tabs)[number];

export default function PropertyView() {
  const { id } = useParams();
  const propertyId = Number(id);

  const [tab, setTab] = React.useState<Tab>("Deal");
  const [v, setV] = React.useState<any | null>(null);
  const [err, setErr] = React.useState<string | null>(null);

  React.useEffect(() => {
    api
      .propertyView(propertyId)
      .then(setV)
      .catch((e) => setErr(String(e.message || e)));
  }, [propertyId]);

  const p = v?.property;
  const d = v?.deal;
  const r = v?.last_underwriting_result;
  const rent = v?.rent_explain;
  const friction = v?.jurisdiction_friction;

  return (
    <div className="space-y-4">
      <div className="oh-panel p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-xl font-semibold tracking-tight">
              {p?.address ?? `Property ${propertyId}`}
            </div>
            <div className="text-sm text-zinc-400 mt-1">
              {p?.city}, {p?.state} {p?.zip} · {p?.bedrooms}bd · Strategy:{" "}
              <span className="oh-kbd">
                {(d?.strategy || "section8").toUpperCase()}
              </span>
            </div>
          </div>
          <div className="text-right">
            <div className="text-xs text-zinc-500">Latest decision</div>
            <div className="text-lg font-semibold">{r?.decision ?? "—"}</div>
            <div className="text-xs text-zinc-500 mt-1">
              Score: {r?.score ?? "—"} · DSCR: {r?.dscr?.toFixed?.(2) ?? "—"}
            </div>
          </div>
        </div>
      </div>

      {err && (
        <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
          {err}
        </div>
      )}

      <div className="flex gap-2 flex-wrap">
        {tabs.map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={[
              "px-3 py-2 rounded-xl border text-sm transition",
              tab === t
                ? "bg-white/[0.06] text-zinc-100 border-white/[0.16]"
                : "text-zinc-300 border-white/10 hover:bg-white/[0.04] hover:border-white/[0.14]",
            ].join(" ")}
          >
            {t}
          </button>
        ))}
      </div>

      {tab === "Deal" && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <Panel title="Underwriting">
            <Row
              k="Gross rent used"
              v={
                r?.gross_rent_used != null
                  ? `$${Math.round(r.gross_rent_used)}`
                  : "—"
              }
            />
            <Row
              k="Mortgage"
              v={
                r?.mortgage_payment != null
                  ? `$${Math.round(r.mortgage_payment)}`
                  : "—"
              }
            />
            <Row
              k="OpEx"
              v={
                r?.operating_expenses != null
                  ? `$${Math.round(r.operating_expenses)}`
                  : "—"
              }
            />
            <Row k="NOI" v={r?.noi != null ? `$${Math.round(r.noi)}` : "—"} />
            <Row
              k="Cash flow"
              v={r?.cash_flow != null ? `$${Math.round(r.cash_flow)}` : "—"}
            />
            <Row
              k="CoC"
              v={
                r?.cash_on_cash != null
                  ? `${(r.cash_on_cash * 100).toFixed(1)}%`
                  : "—"
              }
            />
            <Row
              k="Break-even rent"
              v={
                r?.break_even_rent != null
                  ? `$${Math.round(r.break_even_rent)}`
                  : "—"
              }
            />
          </Panel>

          <Panel title="Rent Explain">
            <Row
              k="Cap reason"
              v={rent?.cap_reason ?? r?.rent_cap_reason ?? "—"}
            />
            <Row
              k="Payment standard %"
              v={
                rent?.payment_standard_pct != null
                  ? `${(rent.payment_standard_pct * 100).toFixed(0)}%`
                  : "—"
              }
            />
            <Row
              k="FMR adjusted"
              v={
                rent?.fmr_adjusted != null
                  ? `$${Math.round(rent.fmr_adjusted)}`
                  : "—"
              }
            />
            <Row
              k="Rent reasonableness"
              v={
                rent?.rent_reasonableness_comp != null
                  ? `$${Math.round(rent.rent_reasonableness_comp)}`
                  : "—"
              }
            />
            <Row
              k="Override ceiling"
              v={
                rent?.approved_rent_ceiling != null
                  ? `$${Math.round(rent.approved_rent_ceiling)}`
                  : "—"
              }
            />
            <Row
              k="Rent used"
              v={
                rent?.rent_used != null ? `$${Math.round(rent.rent_used)}` : "—"
              }
            />
          </Panel>

          <Panel title="Jurisdiction Friction">
            <Row k="Multiplier" v={friction?.multiplier ?? "—"} />
            <div className="mt-2 text-xs text-zinc-500">Reasons</div>
            <ul className="mt-1 text-sm text-zinc-200 space-y-1 list-disc pl-5">
              {(friction?.reasons ?? []).map((x: string, i: number) => (
                <li key={i}>{x}</li>
              ))}
              {(friction?.reasons ?? []).length === 0 && (
                <li className="text-zinc-500">—</li>
              )}
            </ul>
          </Panel>

          <Panel title="Raw deal">
            <Row
              k="Asking"
              v={
                d?.asking_price != null ? `$${Math.round(d.asking_price)}` : "—"
              }
            />
            <Row
              k="Est purchase"
              v={
                d?.estimated_purchase_price != null
                  ? `$${Math.round(d.estimated_purchase_price)}`
                  : "—"
              }
            />
            <Row
              k="Rehab"
              v={
                d?.rehab_estimate != null
                  ? `$${Math.round(d.rehab_estimate)}`
                  : "—"
              }
            />
          </Panel>
        </div>
      )}

      {tab !== "Deal" && (
        <div className="oh-panel p-5">
          <div className="text-sm text-zinc-300">
            {tab} module UI comes next. Backend scaffolding exists in schemas
            for Rehab / Tenants / Cash. Your “Phase 4 completion” is wiring each
            tab to its endpoints and making the workflow slots drive next
            actions.
          </div>
        </div>
      )}
    </div>
  );
}

function Panel({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="oh-panel p-5">
      <div className="text-sm font-semibold">{title}</div>
      <div className="mt-3 space-y-2">{children}</div>
    </div>
  );
}

function Row({ k, v }: { k: string; v: any }) {
  return (
    <div className="flex items-center justify-between gap-4 text-sm">
      <div className="text-zinc-500">{k}</div>
      <div className="text-zinc-200 font-medium">{v}</div>
    </div>
  );
}
