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
  "Agents",
] as const;
type Tab = (typeof tabs)[number];

export default function Property() {
  const { id } = useParams();
  const pid = Number(id);

  const [tab, setTab] = React.useState<Tab>("Deal");
  const [view, setView] = React.useState<any | null>(null);
  const [err, setErr] = React.useState<string | null>(null);

  const [rehab, setRehab] = React.useState<any[]>([]);
  const [leases, setLeases] = React.useState<any[]>([]);
  const [txns, setTxns] = React.useState<any[]>([]);
  const [vals, setVals] = React.useState<any[]>([]);
  const [runs, setRuns] = React.useState<any[]>([]);
  const threadKey = `property:${pid}`;
  const [msgs, setMsgs] = React.useState<any[]>([]);
  const [msg, setMsg] = React.useState("");

  async function refresh() {
    try {
      setErr(null);
      const v = await api.propertyView(pid);
      setView(v);

      setRehab(await api.rehabTasks(pid));
      setLeases(await api.leases(pid));
      setTxns(await api.txns(pid));
      setVals(await api.valuations(pid));
      setRuns(await api.agentRuns(pid));
      setMsgs(await api.messages(threadKey));
    } catch (e: any) {
      setErr(String(e.message || e));
    }
  }

  React.useEffect(() => {
    refresh();
  }, [pid]);

  async function post() {
    await api.postMessage({
      thread_key: threadKey,
      sender: "user",
      recipient: "all",
      message: msg,
    });
    setMsg("");
    setMsgs(await api.messages(threadKey));
  }

  if (err)
    return (
      <div className="p-3 rounded-lg bg-red-950/40 border border-red-800 text-red-200">
        {err}
      </div>
    );
  if (!view) return <div className="text-zinc-400">Loading…</div>;

  const p = view.property;
  const r = view.last_underwriting_result;

  return (
    <div className="space-y-4">
      <div className="p-4 rounded-xl border border-zinc-800 bg-zinc-900/30">
        <div className="flex items-start justify-between">
          <div>
            <div className="text-lg font-semibold">{p.address}</div>
            <div className="text-sm text-zinc-400">
              {p.city}, {p.state} {p.zip} • {p.bedrooms}bd • year{" "}
              {p.year_built ?? "—"}
            </div>
          </div>
          <div className="text-right">
            <div className="text-xs text-zinc-400">Decision</div>
            <div className="text-lg font-semibold">{r?.decision ?? "—"}</div>
            <div className="text-xs text-zinc-400">
              Score: {r?.score ?? "—"}
            </div>
          </div>
        </div>

        <div className="mt-3 grid grid-cols-6 gap-2 text-xs text-zinc-300">
          <div>DSCR: {r?.dscr?.toFixed?.(2) ?? "—"}</div>
          <div>Cashflow: {r?.cash_flow?.toFixed?.(0) ?? "—"}</div>
          <div>Rent Used: {view.rent_explain?.rent_used ?? "—"}</div>
          <div>Cap: {view.rent_explain?.cap_reason ?? "—"}</div>
          <div>Friction: {view.jurisdiction_friction?.multiplier ?? "—"}</div>
          <div>
            Processing days: {view.jurisdiction_rule?.processing_days ?? "—"}
          </div>
        </div>
      </div>

      <div className="flex gap-2 flex-wrap">
        {tabs.map((t) => (
          <button
            key={t}
            className={`px-3 py-2 rounded-lg border text-sm ${
              tab === t
                ? "bg-zinc-900 border-zinc-700"
                : "bg-transparent border-zinc-800 text-zinc-300 hover:bg-zinc-900/40"
            }`}
            onClick={() => setTab(t)}
          >
            {t}
          </button>
        ))}
        <button
          className="ml-auto px-3 py-2 rounded-lg border border-zinc-800 text-sm hover:bg-zinc-900/40"
          onClick={refresh}
        >
          Refresh
        </button>
      </div>

      {tab === "Deal" && (
        <Card
          title="Underwriting Reasons"
          subtitle="Explainability: why PASS/REVIEW/REJECT"
        >
          <ul className="list-disc ml-5 text-sm text-zinc-300">
            {(r?.reasons || []).map((x: string, i: number) => (
              <li key={i}>{x}</li>
            ))}
          </ul>
          <div className="mt-3 text-xs text-zinc-400">
            Rent explain: cap_reason={view.rent_explain?.cap_reason},
            fmr_adjusted={view.rent_explain?.fmr_adjusted ?? "—"}
          </div>
        </Card>
      )}

      {tab === "Rehab" && (
        <Card
          title="Rehab Tasks"
          subtitle="Jira-lite tickets tied to inspection relevance"
        >
          <div className="space-y-2">
            {rehab.map((t, i) => (
              <div
                key={i}
                className="p-3 rounded-lg border border-zinc-800 bg-zinc-950/40"
              >
                <div className="flex justify-between">
                  <div className="font-medium">{t.title}</div>
                  <div className="text-xs text-zinc-400">{t.status}</div>
                </div>
                <div className="text-xs text-zinc-400">
                  {t.category} • inspection relevant:{" "}
                  {String(t.inspection_relevant)}
                </div>
              </div>
            ))}
            {rehab.length === 0 && (
              <div className="text-sm text-zinc-400">No tasks yet.</div>
            )}
          </div>
        </Card>
      )}

      {tab === "Compliance" && (
        <Card title="Checklist" subtitle="Pre-inspection autopilot">
          <div className="text-xs text-zinc-400 mb-2">
            Items: {view.checklist?.items?.length ?? 0}
          </div>
          <div className="grid grid-cols-1 gap-2">
            {(view.checklist?.items || [])
              .slice(0, 30)
              .map((it: any, i: number) => (
                <div
                  key={i}
                  className="p-3 rounded-lg border border-zinc-800 bg-zinc-950/40"
                >
                  <div className="font-medium">
                    {it.item_code} • {it.category}
                  </div>
                  <div className="text-sm text-zinc-300">{it.description}</div>
                  <div className="text-xs text-zinc-400">
                    severity {it.severity} • common_fail{" "}
                    {String(it.common_fail)}
                  </div>
                </div>
              ))}
          </div>
        </Card>
      )}

      {tab === "Tenant" && (
        <Card title="Leases" subtitle="Voucher status + HAP lifecycle">
          <div className="space-y-2">
            {leases.map((l, i) => (
              <div
                key={i}
                className="p-3 rounded-lg border border-zinc-800 bg-zinc-950/40"
              >
                <div className="flex justify-between">
                  <div className="font-medium">Lease #{l.id}</div>
                  <div className="text-xs text-zinc-400">
                    {l.hap_contract_status ?? "—"}
                  </div>
                </div>
                <div className="text-xs text-zinc-400">
                  total_rent {l.total_rent} • tenant {l.tenant_portion ?? "—"} •
                  HA {l.housing_authority_portion ?? "—"}
                </div>
              </div>
            ))}
            {leases.length === 0 && (
              <div className="text-sm text-zinc-400">No leases yet.</div>
            )}
          </div>
        </Card>
      )}

      {tab === "Cash" && (
        <Card
          title="Transactions"
          subtitle="Property-level P&L building blocks"
        >
          <div className="space-y-2">
            {txns.slice(0, 50).map((t, i) => (
              <div
                key={i}
                className="p-3 rounded-lg border border-zinc-800 bg-zinc-950/40 flex justify-between"
              >
                <div>
                  <div className="font-medium">{t.type}</div>
                  <div className="text-xs text-zinc-400">{t.memo ?? ""}</div>
                </div>
                <div className="font-semibold">{t.amount}</div>
              </div>
            ))}
            {txns.length === 0 && (
              <div className="text-sm text-zinc-400">No transactions yet.</div>
            )}
          </div>
        </Card>
      )}

      {tab === "Equity" && (
        <Card
          title="Valuations"
          subtitle="Equity snapshots for refinance/sell/hold decisions"
        >
          <div className="space-y-2">
            {vals.map((v, i) => (
              <div
                key={i}
                className="p-3 rounded-lg border border-zinc-800 bg-zinc-950/40"
              >
                <div className="flex justify-between">
                  <div className="font-medium">${v.estimated_value}</div>
                  <div className="text-xs text-zinc-400">
                    loan ${v.loan_balance ?? "—"}
                  </div>
                </div>
                <div className="text-xs text-zinc-400">{v.notes ?? ""}</div>
              </div>
            ))}
            {vals.length === 0 && (
              <div className="text-sm text-zinc-400">No valuations yet.</div>
            )}
          </div>
        </Card>
      )}

      {tab === "Agents" && (
        <div className="grid grid-cols-2 gap-3">
          <Card
            title="Agent Runs"
            subtitle="Human slots now; multi-agent swarm later"
          >
            <div className="space-y-2">
              {runs.map((r, i) => (
                <div
                  key={i}
                  className="p-3 rounded-lg border border-zinc-800 bg-zinc-950/40"
                >
                  <div className="flex justify-between">
                    <div className="font-medium">{r.agent_key}</div>
                    <div className="text-xs text-zinc-400">{r.status}</div>
                  </div>
                </div>
              ))}
              {runs.length === 0 && (
                <div className="text-sm text-zinc-400">No agent runs yet.</div>
              )}
            </div>
          </Card>

          <Card title="Property Thread" subtitle={`Thread key: ${threadKey}`}>
            <div className="h-64 overflow-auto border border-zinc-800 rounded-lg p-3 bg-zinc-950/40 space-y-2">
              {msgs.map((m, i) => (
                <div key={i} className="text-sm">
                  <span className="text-zinc-400">{m.sender}:</span>{" "}
                  <span className="text-zinc-200">{m.message}</span>
                </div>
              ))}
            </div>
            <div className="mt-2 flex gap-2">
              <input
                value={msg}
                onChange={(e) => setMsg(e.target.value)}
                className="flex-1 px-3 py-2 rounded-lg bg-zinc-900 border border-zinc-800 text-sm"
                placeholder="Send a message to agents…"
              />
              <button
                onClick={post}
                className="px-3 py-2 rounded-lg bg-zinc-100 text-zinc-900 text-sm font-medium"
              >
                Send
              </button>
            </div>
          </Card>
        </div>
      )}
    </div>
  );
}

function Card({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="p-4 rounded-xl border border-zinc-800 bg-zinc-900/30">
      <div className="flex items-end justify-between gap-3">
        <div>
          <div className="font-semibold">{title}</div>
          {subtitle && <div className="text-xs text-zinc-400">{subtitle}</div>}
        </div>
      </div>
      <div className="mt-3">{children}</div>
    </div>
  );
}
