import React from "react";
import { useParams } from "react-router-dom";
import { api } from "../lib/api";
import AgentSlots from "../components/AgentSlots";

const tabs = [
  "Deal",
  "Rehab",
  "Compliance",
  "Tenant",
  "Cash",
  "Equity",
] as const;
type Tab = (typeof tabs)[number];

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

export default function PropertyView() {
  const { id } = useParams();
  const propertyId = Number(id);

  const [tab, setTab] = React.useState<Tab>("Deal");

  const [v, setV] = React.useState<any | null>(null);
  const [err, setErr] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);

  // Tab data
  const [rehab, setRehab] = React.useState<any[] | null>(null);
  const [leases, setLeases] = React.useState<any[] | null>(null);
  const [txns, setTxns] = React.useState<any[] | null>(null);
  const [vals, setVals] = React.useState<any[] | null>(null);

  async function loadView() {
    try {
      setErr(null);
      const out = await api.propertyView(propertyId);
      setV(out);
    } catch (e: any) {
      // Keep error, but don't blank page
      setV(null);
      setErr(String(e.message || e));
    }
  }

  React.useEffect(() => {
    loadView();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [propertyId]);

  // Load tab-specific data when tab changes
  React.useEffect(() => {
    async function run() {
      try {
        if (tab === "Rehab") {
          setRehab(null);
          setRehab(await api.rehabTasks(propertyId));
        } else if (tab === "Tenant") {
          setLeases(null);
          setLeases(await api.leases(propertyId));
        } else if (tab === "Cash") {
          setTxns(null);
          setTxns(await api.txns(propertyId));
        } else if (tab === "Equity") {
          setVals(null);
          setVals(await api.valuations(propertyId));
        }
      } catch (e: any) {
        setErr(String(e.message || e));
      }
    }
    run();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, propertyId]);

  const p = v?.property;
  const d = v?.deal;
  const r = v?.last_underwriting_result;
  const rent = v?.rent_explain;
  const friction = v?.jurisdiction_friction;

  const noDeal = err?.toLowerCase().includes("no deal found for property");

  async function doAction(label: string, fn: () => Promise<any>) {
    try {
      setBusy(label);
      setErr(null);
      await fn();
      await loadView();
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(null);
    }
  }

  async function createDealQuick() {
    const askingStr = prompt("Asking price?", "120000");
    if (!askingStr) return;
    const asking = Number(askingStr);
    if (!Number.isFinite(asking) || asking <= 0) {
      setErr("Invalid asking price.");
      return;
    }

    await doAction("Creating deal…", async () => {
      // Your backend already has /deals in repo (Phase 1). If your route is different, adjust here.
      await api.createDeal({
        property_id: propertyId,
        asking_price: asking,
        rehab_estimate: 0,
        strategy: "section8",
      });
    });
  }

  async function enrich() {
    await doAction("Enriching rent…", () =>
      api.enrichProperty(propertyId, d?.strategy || "section8"),
    );
  }

  async function explain() {
    await doAction("Explaining rent…", () =>
      api.explainProperty(propertyId, d?.strategy || "section8", true),
    );
  }

  async function evaluate() {
    await doAction("Evaluating…", () =>
      api.evaluateProperty(propertyId, d?.strategy || "section8"),
    );
  }

  return (
    <div className="space-y-4">
      <div className="oh-panel p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-xl font-semibold tracking-tight">
              {p?.address ?? `Property ${propertyId}`}
            </div>
            <div className="text-sm text-zinc-400 mt-1">
              {p?.city ?? "—"}, {p?.state ?? "—"} {p?.zip ?? ""} ·{" "}
              {p?.bedrooms ?? "—"}bd · Strategy:{" "}
              <span className="oh-kbd">
                {((d?.strategy || "section8") as string).toUpperCase()}
              </span>
            </div>
          </div>

          <div className="text-right">
            <div className="text-xs text-zinc-500">Latest decision</div>
            <div className="text-lg font-semibold">{r?.decision ?? "—"}</div>
            <div className="text-xs text-zinc-500 mt-1">
              Score: {r?.score ?? "—"} · DSCR: {r?.dscr?.toFixed?.(2) ?? "—"}
            </div>

            <div className="mt-3 flex items-center justify-end gap-2">
              <button
                onClick={createDealQuick}
                className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/5 hover:bg-white/10"
                disabled={!!busy}
                title="Create a deal if missing"
              >
                {busy?.includes("Creating") ? "creating…" : "+ deal"}
              </button>

              <button
                onClick={enrich}
                className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/5 hover:bg-white/10"
                disabled={!!busy || !d}
                title="Rent enrich"
              >
                enrich
              </button>

              <button
                onClick={explain}
                className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/5 hover:bg-white/10"
                disabled={!!busy || !d}
                title="Rent explain (persisted)"
              >
                explain
              </button>

              <button
                onClick={evaluate}
                className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/10 hover:bg-white/15"
                disabled={!!busy || !d}
                title="Evaluate"
              >
                evaluate
              </button>
            </div>
          </div>
        </div>
      </div>

      {busy && (
        <div className="oh-panel-solid p-4 border-white/10 bg-white/5 text-zinc-200">
          {busy}
        </div>
      )}

      {err && (
        <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
          {noDeal
            ? "No deal exists for this property yet. Click “+ deal” to create one, then run enrich/explain/evaluate."
            : err}
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
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
          <div className="lg:col-span-2 grid grid-cols-1 md:grid-cols-2 gap-3">
            <Panel title="Underwriting">
              <Row
                k="Gross rent used"
                v={r?.gross_rent_used != null ? money(r.gross_rent_used) : "—"}
              />
              <Row
                k="Mortgage"
                v={
                  r?.mortgage_payment != null ? money(r.mortgage_payment) : "—"
                }
              />
              <Row
                k="OpEx"
                v={
                  r?.operating_expenses != null
                    ? money(r.operating_expenses)
                    : "—"
                }
              />
              <Row k="NOI" v={r?.noi != null ? money(r.noi) : "—"} />
              <Row
                k="Cash flow"
                v={r?.cash_flow != null ? money(r.cash_flow) : "—"}
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
                v={r?.break_even_rent != null ? money(r.break_even_rent) : "—"}
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
                v={rent?.fmr_adjusted != null ? money(rent.fmr_adjusted) : "—"}
              />
              <Row
                k="Rent reasonableness"
                v={
                  rent?.rent_reasonableness_comp != null
                    ? money(rent.rent_reasonableness_comp)
                    : "—"
                }
              />
              <Row
                k="Override ceiling"
                v={
                  rent?.approved_rent_ceiling != null
                    ? money(rent.approved_rent_ceiling)
                    : "—"
                }
              />
              <Row
                k="Rent used"
                v={rent?.rent_used != null ? money(rent.rent_used) : "—"}
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
                v={d?.asking_price != null ? money(d.asking_price) : "—"}
              />
              <Row
                k="Est purchase"
                v={
                  d?.estimated_purchase_price != null
                    ? money(d.estimated_purchase_price)
                    : "—"
                }
              />
              <Row
                k="Rehab"
                v={d?.rehab_estimate != null ? money(d.rehab_estimate) : "—"}
              />
            </Panel>
          </div>

          <div className="space-y-3">
            <AgentSlots propertyId={propertyId} />
            <Panel title="What’s next">
              <div className="text-sm text-zinc-300 leading-relaxed">
                Phase 4 completion = every tab loads real data, and agent slots
                drive workflow actions. This screen now runs the full pipeline
                (enrich → explain → evaluate) and shows the result.
              </div>
            </Panel>
          </div>
        </div>
      )}

      {tab === "Rehab" && (
        <Panel title="Rehab Tasks">
          {!rehab && (
            <div className="text-sm text-zinc-400">Loading rehab tasks…</div>
          )}
          {rehab && rehab.length === 0 && (
            <div className="text-sm text-zinc-400">No rehab tasks yet.</div>
          )}
          {rehab && rehab.length > 0 && (
            <div className="space-y-2">
              {rehab.map((t: any) => (
                <div
                  key={t.id}
                  className="rounded-xl border border-white/10 bg-white/[0.03] p-3"
                >
                  <div className="flex items-center justify-between">
                    <div className="font-semibold text-zinc-100">{t.title}</div>
                    <span className="text-[11px] px-2 py-1 rounded-full border border-white/10 bg-white/5 text-zinc-300">
                      {t.status}
                    </span>
                  </div>
                  <div className="text-xs text-zinc-400 mt-1">
                    {t.deadline
                      ? `Due: ${new Date(t.deadline).toLocaleDateString()}`
                      : "No deadline"}{" "}
                    ·{" "}
                    {t.cost_estimate != null
                      ? `Est: ${money(t.cost_estimate)}`
                      : "No estimate"}
                  </div>
                  {t.notes && (
                    <div className="text-sm text-zinc-300 mt-2">{t.notes}</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </Panel>
      )}

      {tab === "Compliance" && (
        <Panel title="Compliance / Checklist">
          <div className="text-sm text-zinc-300 leading-relaxed">
            Your backend is already generating/storing checklist JSON via{" "}
            <span className="oh-kbd">PropertyChecklist</span>. Next UI step is
            to expose checklist item updates (status/proof/notes) through a
            PATCH endpoint.
          </div>

          <div className="mt-4 rounded-xl border border-white/10 bg-white/[0.03] p-3">
            <div className="text-xs text-zinc-500">Checklist snapshot</div>
            <div className="text-sm text-zinc-200 mt-2">
              {v?.checklist?.items?.length != null
                ? `${v.checklist.items.length} items`
                : "—"}
            </div>
          </div>
        </Panel>
      )}

      {tab === "Tenant" && (
        <Panel title="Leases">
          {!leases && (
            <div className="text-sm text-zinc-400">Loading leases…</div>
          )}
          {leases && leases.length === 0 && (
            <div className="text-sm text-zinc-400">No leases yet.</div>
          )}
          {leases && leases.length > 0 && (
            <div className="space-y-2">
              {leases.map((l: any) => (
                <div
                  key={l.id}
                  className="rounded-xl border border-white/10 bg-white/[0.03] p-3"
                >
                  <div className="flex items-center justify-between">
                    <div className="font-semibold text-zinc-100">
                      Tenant #{l.tenant_id}
                    </div>
                    <div className="text-sm text-zinc-200 font-semibold">
                      {money(l.total_rent)}
                    </div>
                  </div>
                  <div className="text-xs text-zinc-400 mt-1">
                    Start: {new Date(l.start_date).toLocaleDateString()}{" "}
                    {l.end_date
                      ? `· End: ${new Date(l.end_date).toLocaleDateString()}`
                      : ""}
                  </div>
                  {l.notes && (
                    <div className="text-sm text-zinc-300 mt-2">{l.notes}</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </Panel>
      )}

      {tab === "Cash" && (
        <Panel title="Transactions">
          {!txns && (
            <div className="text-sm text-zinc-400">Loading transactions…</div>
          )}
          {txns && txns.length === 0 && (
            <div className="text-sm text-zinc-400">No transactions yet.</div>
          )}
          {txns && txns.length > 0 && (
            <div className="space-y-2">
              {txns.map((t: any) => (
                <div
                  key={t.id}
                  className="rounded-xl border border-white/10 bg-white/[0.03] p-3"
                >
                  <div className="flex items-center justify-between">
                    <div className="text-zinc-100 font-semibold">
                      {t.txn_type || t.type || "txn"}
                    </div>
                    <div className="text-zinc-200 font-semibold">
                      {money(t.amount)}
                    </div>
                  </div>
                  <div className="text-xs text-zinc-400 mt-1">
                    {t.txn_date
                      ? new Date(t.txn_date).toLocaleDateString()
                      : "—"}{" "}
                    {t.memo ? `· ${t.memo}` : ""}
                  </div>
                </div>
              ))}
            </div>
          )}
        </Panel>
      )}

      {tab === "Equity" && (
        <Panel title="Valuations">
          {!vals && (
            <div className="text-sm text-zinc-400">Loading valuations…</div>
          )}
          {vals && vals.length === 0 && (
            <div className="text-sm text-zinc-400">No valuations yet.</div>
          )}
          {vals && vals.length > 0 && (
            <div className="space-y-2">
              {vals.map((v: any) => (
                <div
                  key={v.id}
                  className="rounded-xl border border-white/10 bg-white/[0.03] p-3"
                >
                  <div className="flex items-center justify-between">
                    <div className="text-zinc-100 font-semibold">
                      {v.as_of ? new Date(v.as_of).toLocaleDateString() : "—"}
                    </div>
                    <div className="text-zinc-200 font-semibold">
                      {money(v.estimated_value)}
                    </div>
                  </div>
                  <div className="text-xs text-zinc-400 mt-1">
                    Loan: {v.loan_balance != null ? money(v.loan_balance) : "—"}{" "}
                    {v.notes ? `· ${v.notes}` : ""}
                  </div>
                </div>
              ))}
            </div>
          )}
        </Panel>
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
