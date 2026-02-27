// frontend/src/pages/PropertyView.tsx
import React from "react";
import { useParams } from "react-router-dom";
import { api } from "../lib/api";
import AgentSlots from "../components/AgentSlots";
import PageHero from "../components/PageHero";
import BrickBuilder from "../components/BrickBuilder";

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

function pct01(v: any) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${Math.round(n * 100)}%`;
}

function Badge({ children }: { children: React.ReactNode }) {
  return (
    <span className="text-[11px] px-2 py-1 rounded-full border border-white/10 bg-white/5 text-zinc-200">
      {children}
    </span>
  );
}

function Panel({
  title,
  right,
  children,
}: {
  title: string;
  right?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="oh-panel p-5">
      <div className="flex items-center justify-between gap-3">
        <div className="text-sm font-semibold">{title}</div>
        {right ? <div>{right}</div> : null}
      </div>
      <div className="mt-3 space-y-2">{children}</div>
    </div>
  );
}

function Row({ k, v }: { k: string; v: any }) {
  return (
    <div className="flex items-center justify-between gap-4 text-sm">
      <div className="text-zinc-500">{k}</div>
      <div className="text-zinc-200 font-medium text-right">{v}</div>
    </div>
  );
}

function ProgressBar({ value }: { value: number }) {
  const pct = Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
  return (
    <div className="h-2 rounded-full bg-white/10 overflow-hidden">
      <div className="h-2 bg-white/60" style={{ width: `${pct * 100}%` }} />
    </div>
  );
}

function ChecklistItemCard({
  item,
  onUpdate,
  busy,
}: {
  item: any;
  onUpdate: (patch: {
    status?: string | null;
    proof_url?: string | null;
    notes?: string | null;
  }) => Promise<void>;
  busy: boolean;
}) {
  const status = (item?.status || "todo").toLowerCase();
  const border =
    status === "done"
      ? "border-green-400/20 bg-green-400/5"
      : status === "failed"
        ? "border-red-400/20 bg-red-400/5"
        : status === "blocked"
          ? "border-yellow-300/20 bg-yellow-300/5"
          : status === "in_progress"
            ? "border-white/20 bg-white/5"
            : "border-white/10 bg-white/[0.03]";

  return (
    <div className={`rounded-2xl border ${border} p-4`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-zinc-100">
            {item?.description || item?.title || item?.item_code}
          </div>
          <div className="text-xs text-zinc-400 mt-1">
            {item?.category ? `${item.category} · ` : ""}
            <span className="text-zinc-200">{item?.item_code}</span>
            {" · "}
            status: <span className="text-zinc-200">{status}</span>
            {item?.marked_by ? ` · by ${item.marked_by}` : ""}
          </div>
        </div>

        <div className="flex flex-wrap items-center justify-end gap-2">
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "in_progress" })}
            className="oh-btn"
          >
            working
          </button>
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "done" })}
            className="oh-btn oh-btn-good"
          >
            done
          </button>
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "failed" })}
            className="oh-btn oh-btn-bad"
          >
            fail
          </button>
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "blocked" })}
            className="oh-btn oh-btn-warn"
          >
            blocked
          </button>
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-2">
        <div className="rounded-xl border border-white/10 bg-black/20 p-3">
          <div className="text-[11px] text-zinc-500">Proof URL</div>
          <div className="mt-2 flex items-center gap-2">
            <input
              defaultValue={item?.proof_url || ""}
              placeholder="https://..."
              className="oh-input"
              onBlur={(e) => {
                const val = e.target.value.trim();
                onUpdate({ proof_url: val ? val : null }).catch(() => {});
              }}
              disabled={busy}
            />
            {item?.proof_url ? (
              <a
                href={item.proof_url}
                target="_blank"
                rel="noreferrer"
                className="text-xs underline text-zinc-200"
              >
                open
              </a>
            ) : null}
          </div>
        </div>

        <div className="rounded-xl border border-white/10 bg-black/20 p-3">
          <div className="text-[11px] text-zinc-500">Notes</div>
          <textarea
            defaultValue={item?.notes || ""}
            placeholder="What changed? What remains?"
            className="oh-textarea"
            onBlur={(e) => {
              const val = e.target.value.trim();
              onUpdate({ notes: val ? val : null }).catch(() => {});
            }}
            disabled={busy}
          />
        </div>
      </div>
    </div>
  );
}

function TrustPill({ score }: { score: number | null }) {
  const s =
    score == null ? null : Math.max(0, Math.min(100, Math.round(score)));
  const cls =
    s == null
      ? "border-white/10 bg-white/[0.03] text-zinc-300"
      : s >= 80
        ? "border-green-400/20 bg-green-400/10 text-green-200"
        : s >= 55
          ? "border-yellow-300/20 bg-yellow-300/10 text-yellow-100"
          : "border-red-400/20 bg-red-400/10 text-red-200";

  return (
    <span className={`text-[11px] px-2 py-1 rounded-full border ${cls}`}>
      {s == null ? "—" : `Trust ${s}`}
    </span>
  );
}

export default function PropertyView() {
  const { id } = useParams();
  const propertyId = Number(id);

  const [tab, setTab] = React.useState<Tab>("Deal");
  const [bundle, setBundle] = React.useState<any | null>(null);
  const [ops, setOps] = React.useState<any | null>(null);

  const [trust, setTrust] = React.useState<any | null>(null);
  const [trustErr, setTrustErr] = React.useState<string | null>(null);

  const [err, setErr] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);

  const [checklist, setChecklist] = React.useState<any | null>(null);
  const [checkBusyCode, setCheckBusyCode] = React.useState<string | null>(null);

  const abortRef = React.useRef<AbortController | null>(null);

  const v = bundle?.view;
  const p = v?.property;
  const d = v?.deal;
  const r = v?.last_underwriting_result;
  const rent = v?.rent_explain;
  const friction = v?.jurisdiction_friction;

  const rehab = bundle?.rehab_tasks || [];
  const leases = bundle?.leases || [];
  const txns = bundle?.transactions || [];
  const vals = bundle?.valuations || [];

  const noDeal = err?.toLowerCase().includes("no deal found for property");

  async function loadAll() {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);

      // Load bundle + ops + trust in parallel
      const [out, opsOut, trustOut] = await Promise.all([
        api.propertyBundle(propertyId, ac.signal),
        api.opsPropertySummary(propertyId, 90, ac.signal).catch(() => null),
        api
          .trustGet("property", propertyId, ac.signal)
          .then((x) => {
            setTrustErr(null);
            return x;
          })
          .catch((e) => {
            setTrustErr(String(e?.message || e));
            return null;
          }),
      ]);

      setBundle(out);
      setOps(opsOut);
      setTrust(trustOut);

      try {
        const latest = await api.checklistLatest(propertyId, ac.signal);
        setChecklist(latest);
      } catch {
        setChecklist(null);
      }
    } catch (e: any) {
      if (String(e?.name) === "AbortError") return;
      setBundle(null);
      setOps(null);
      setTrust(null);
      setErr(String(e.message || e));
    }
  }

  React.useEffect(() => {
    if (!Number.isFinite(propertyId)) {
      setErr("Invalid property id.");
      return;
    }
    loadAll();
    return () => abortRef.current?.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [propertyId]);

  async function doAction(label: string, fn: () => Promise<any>) {
    try {
      setBusy(label);
      setErr(null);
      await fn();
      await loadAll();
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

  // Runtime fallback kept
  async function evaluate() {
    await doAction("Evaluating…", async () => {
      const strategy = d?.strategy || "section8";

      const maybeEvalProperty = (api as any).evaluateProperty;
      if (typeof maybeEvalProperty === "function") {
        return await maybeEvalProperty(propertyId, strategy);
      }

      const snapshotId =
        (bundle as any)?.snapshot_id ??
        (bundle as any)?.view?.snapshot_id ??
        (bundle as any)?.view?.latest_snapshot_id ??
        null;

      if (snapshotId != null) {
        return await api.evaluateRun(Number(snapshotId), strategy);
      }

      throw new Error(
        "No evaluation method available. api.evaluateProperty is missing and no snapshot_id was found in the bundle/view. Add evaluateProperty back to api.ts OR include snapshot_id in propertyBundle response.",
      );
    });
  }

  async function refreshChecklist() {
    const latest = await api.checklistLatest(propertyId);
    setChecklist(latest);
  }

  async function generateChecklist() {
    await doAction("Generating checklist…", async () => {
      await api.generateChecklist(propertyId, {
        strategy: d?.strategy || "section8",
        persist: true,
      });
    });
  }

  async function generateRehabFromGaps() {
    await doAction("Generating rehab tasks from gaps…", async () => {
      await api.opsGenerateRehabTasks(propertyId);
    });
  }

  const checklistItems = checklist?.items ?? v?.checklist?.items ?? [];

  const heroTitle = p?.address ? p.address : `Property ${propertyId}`;
  const heroSub = `${p?.city ?? "—"}, ${p?.state ?? "—"} ${p?.zip ?? ""} · ${
    p?.bedrooms ?? "—"
  }bd · Strategy: ${((d?.strategy || "section8") as string).toUpperCase()} · Decision: ${
    r?.decision ?? "—"
  } · Score: ${r?.score ?? "—"} · DSCR: ${r?.dscr?.toFixed?.(2) ?? "—"}`;

  // Ops-derived bits (safe defaults)
  const stage = ops?.stage || "deal";
  const cp = ops?.checklist_progress || {};
  const insp = ops?.inspection || {};
  const cash30 = ops?.cash?.last_30_days || {};
  const cash90 = ops?.cash?.last_90_days || ops?.cash?.last_90_days || {};
  const equity = ops?.equity || null;
  const nextActions: string[] = Array.isArray(ops?.next_actions)
    ? ops.next_actions
    : [];

  // Trust bits (defensive)
  const trustScore =
    trust?.score != null
      ? Number(trust.score)
      : trust?.trust_score != null
        ? Number(trust.trust_score)
        : null;
  const trustConfidence =
    trust?.confidence ?? trust?.confidence_label ?? trust?.band ?? null;
  const positives: any[] = Array.isArray(trust?.top_positive)
    ? trust.top_positive
    : Array.isArray(trust?.positives)
      ? trust.positives
      : [];
  const negatives: any[] = Array.isArray(trust?.top_negative)
    ? trust.top_negative
    : Array.isArray(trust?.negatives)
      ? trust.negatives
      : [];

  return (
    <div className="relative space-y-5">
      <PageHero
        eyebrow="Property"
        title={heroTitle}
        subtitle={heroSub}
        right={<BrickBuilder />}
        actions={
          <>
            <button
              onClick={loadAll}
              className="oh-btn"
              disabled={!!busy}
              title="Refresh"
            >
              sync
            </button>
            <button
              onClick={createDealQuick}
              className="oh-btn"
              disabled={!!busy}
              title="Create a deal if missing"
            >
              {busy?.includes("Creating") ? "creating…" : "+ deal"}
            </button>
            <button
              onClick={enrich}
              className="oh-btn"
              disabled={!!busy || !d}
              title="Rent enrich"
            >
              enrich
            </button>
            <button
              onClick={explain}
              className="oh-btn"
              disabled={!!busy || !d}
              title="Rent explain (persisted)"
            >
              explain
            </button>
            <button
              onClick={evaluate}
              className="oh-btn oh-btn-primary"
              disabled={!!busy || !d}
              title="Evaluate"
            >
              evaluate
            </button>
          </>
        }
      />

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

      {/* Reality + Trust + Slots */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
        <div className="lg:col-span-2">
          <Panel
            title="Reality Loop (auto-derived)"
            right={
              <div className="flex flex-wrap items-center gap-2 justify-end">
                <Badge>Stage: {String(stage).toUpperCase()}</Badge>
                {cp?.total != null ? (
                  <Badge>
                    Checklist: {cp.done ?? 0}/{cp.total ?? 0} (
                    {pct01(cp.pct_done)})
                  </Badge>
                ) : null}
                {insp?.latest ? (
                  <Badge>
                    Inspection: {insp.latest.passed ? "PASSED" : "NOT PASSED"} ·
                    fails {insp.open_failed_items ?? 0}
                  </Badge>
                ) : (
                  <Badge>Inspection: NONE</Badge>
                )}
              </div>
            }
          >
            <div className="space-y-3">
              <div>
                <Row
                  k="Checklist progress"
                  v={`${cp.done ?? 0}/${cp.total ?? 0} (${pct01(cp.pct_done)})`}
                />
                <div className="mt-2">
                  <ProgressBar value={Number(cp.pct_done || 0)} />
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="text-xs text-zinc-500">Cash rollup (30d)</div>
                  <div className="mt-2 space-y-1">
                    <Row k="Income" v={money(cash30.income)} />
                    <Row k="Expense" v={money(cash30.expense)} />
                    <Row k="Capex" v={money(cash30.capex)} />
                    <Row k="Net" v={money(cash30.net)} />
                  </div>
                </div>

                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="text-xs text-zinc-500">Equity snapshot</div>
                  <div className="mt-2 space-y-1">
                    <Row
                      k="As of"
                      v={
                        equity?.as_of ? String(equity.as_of).slice(0, 10) : "—"
                      }
                    />
                    <Row
                      k="Value"
                      v={equity ? money(equity.estimated_value) : "—"}
                    />
                    <Row
                      k="Loan"
                      v={equity ? money(equity.loan_balance) : "—"}
                    />
                    <Row
                      k="Equity"
                      v={equity ? money(equity.estimated_equity) : "—"}
                    />
                  </div>
                </div>
              </div>

              <div className="flex flex-wrap items-center gap-2">
                <button
                  onClick={generateChecklist}
                  className="oh-btn oh-btn-primary"
                  disabled={!!busy || !d}
                  title="Generate and persist checklist"
                >
                  checklist generate
                </button>
                <button
                  onClick={generateRehabFromGaps}
                  className="oh-btn"
                  disabled={!!busy}
                  title="Creates rehab tasks from checklist gaps + unresolved inspection fails"
                >
                  rehab from gaps
                </button>
              </div>

              <div className="pt-2">
                <div className="text-xs text-zinc-500 mb-2">Next actions</div>
                {nextActions.length ? (
                  <div className="space-y-2">
                    {nextActions.slice(0, 8).map((a, i) => (
                      <div
                        key={i}
                        className="rounded-xl border border-white/10 bg-white/[0.03] p-3 text-sm text-zinc-200"
                      >
                        {a}
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-sm text-zinc-400">
                    No blockers detected.
                  </div>
                )}
              </div>
            </div>
          </Panel>
        </div>

        <div className="space-y-3">
          {/* ✅ NEW: Trust Card (visible or it rots) */}
          <Panel title="Trust" right={<TrustPill score={trustScore} />}>
            {trust == null ? (
              <div className="text-sm text-zinc-400">
                Trust is not available yet.
                {trustErr ? (
                  <div className="mt-2 text-xs text-zinc-500">{trustErr}</div>
                ) : null}
              </div>
            ) : (
              <div className="space-y-3">
                <Row
                  k="Score"
                  v={trustScore != null ? `${Math.round(trustScore)}/100` : "—"}
                />
                <Row k="Confidence" v={trustConfidence ?? "—"} />
                <div className="grid grid-cols-1 gap-2">
                  <div className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
                    <div className="text-[11px] text-zinc-500 mb-2">
                      Top positives
                    </div>
                    {positives.length ? (
                      <div className="space-y-1">
                        {positives.slice(0, 3).map((x: any, i: number) => (
                          <div key={i} className="text-sm text-zinc-200">
                            •{" "}
                            {x.signal_key ||
                              x.key ||
                              x.name ||
                              JSON.stringify(x)}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-sm text-zinc-400">—</div>
                    )}
                  </div>

                  <div className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
                    <div className="text-[11px] text-zinc-500 mb-2">
                      Top negatives
                    </div>
                    {negatives.length ? (
                      <div className="space-y-1">
                        {negatives.slice(0, 3).map((x: any, i: number) => (
                          <div key={i} className="text-sm text-zinc-200">
                            •{" "}
                            {x.signal_key ||
                              x.key ||
                              x.name ||
                              JSON.stringify(x)}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-sm text-zinc-400">—</div>
                    )}
                  </div>
                </div>

                <div className="text-xs text-zinc-500">
                  Trust is computed from stored signals (providers, pipeline
                  completeness, overrides).
                </div>
              </div>
            )}
          </Panel>

          <AgentSlots propertyId={propertyId} />

          <Panel title="Ops intent">
            <div className="text-sm text-zinc-300 leading-relaxed">
              This panel is the loop-closer: it turns backend truth into UI
              truth (readiness + next actions), so agents and humans work the
              same queue.
            </div>
          </Panel>
        </div>
      </div>

      <div className="gradient-border rounded-2xl p-[1px]">
        <div className="glass rounded-2xl p-2 flex gap-2 flex-wrap">
          {tabs.map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={[
                "px-3 py-2 rounded-xl border text-sm transition",
                tab === t
                  ? "bg-white/[0.07] text-zinc-100 border-white/[0.18] shadow-[0_0_0_1px_rgba(255,255,255,0.04)]"
                  : "text-zinc-300 border-white/10 hover:bg-white/[0.04] hover:border-white/[0.14]",
              ].join(" ")}
            >
              {t}
            </button>
          ))}
        </div>
      </div>

      {/* Deal */}
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
            <Panel title="What’s next">
              <div className="text-sm text-zinc-300 leading-relaxed">
                Now that Ops Summary is present, the next step is to let a slot
                “accept” a Next Action and persist that as a WorkflowEvent +
                assignment.
              </div>
            </Panel>
          </div>
        </div>
      )}

      {/* Rehab */}
      {tab === "Rehab" && (
        <Panel
          title="Rehab Tasks"
          right={
            <button
              onClick={generateRehabFromGaps}
              className="oh-btn"
              disabled={!!busy}
              title="Creates rehab tasks from checklist gaps + unresolved inspection fails"
            >
              rehab from gaps
            </button>
          }
        >
          {rehab.length === 0 ? (
            <div className="text-sm text-zinc-400">No rehab tasks yet.</div>
          ) : (
            <div className="space-y-2">
              {rehab.map((t: any) => (
                <div
                  key={t.id}
                  className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
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

      {/* Compliance */}
      {tab === "Compliance" && (
        <Panel title="Compliance / Checklist">
          <div className="flex items-center justify-between gap-3 flex-wrap">
            <div className="text-sm text-zinc-300">
              Checklist items are editable (status/proof/notes) and write audit
              + workflow events.
            </div>
            <div className="flex gap-2">
              <button
                onClick={() =>
                  refreshChecklist().catch((e) =>
                    setErr(String(e?.message || e)),
                  )
                }
                className="oh-btn"
              >
                refresh
              </button>
              <button
                onClick={generateChecklist}
                className="oh-btn oh-btn-primary"
                disabled={!!busy || !d}
              >
                generate
              </button>
              <button
                onClick={generateRehabFromGaps}
                className="oh-btn"
                disabled={!!busy}
                title="Creates rehab tasks from checklist gaps + unresolved inspection fails"
              >
                rehab from gaps
              </button>
            </div>
          </div>

          <div className="mt-4 space-y-2">
            {checklistItems.length === 0 ? (
              <div className="text-sm text-zinc-400">
                No checklist found yet. Click{" "}
                <span className="text-zinc-200 font-semibold">generate</span> to
                create one.
              </div>
            ) : (
              checklistItems.map((it: any) => (
                <ChecklistItemCard
                  key={it.item_code}
                  item={it}
                  busy={checkBusyCode === it.item_code}
                  onUpdate={async (patch) => {
                    try {
                      setCheckBusyCode(it.item_code);
                      await api.updateChecklistItem(
                        propertyId,
                        it.item_code,
                        patch,
                      );
                      await refreshChecklist();

                      // refresh ops + trust too (since progress + overrides can affect trust/confidence)
                      const [opsOut, trustOut] = await Promise.all([
                        api
                          .opsPropertySummary(propertyId, 90)
                          .catch(() => null),
                        api.trustGet("property", propertyId).catch(() => null),
                      ]);
                      setOps(opsOut);
                      setTrust(trustOut);
                    } finally {
                      setCheckBusyCode(null);
                    }
                  }}
                />
              ))
            )}
          </div>
        </Panel>
      )}

      {/* Tenant */}
      {tab === "Tenant" && (
        <Panel title="Leases">
          {leases.length === 0 ? (
            <div className="text-sm text-zinc-400">No leases yet.</div>
          ) : (
            <div className="space-y-2">
              {leases.map((l: any) => (
                <div
                  key={l.id}
                  className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
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

      {/* Cash */}
      {tab === "Cash" && (
        <Panel title="Transactions">
          {txns.length === 0 ? (
            <div className="text-sm text-zinc-400">No transactions yet.</div>
          ) : (
            <div className="space-y-2">
              {txns.map((t: any) => (
                <div
                  key={t.id}
                  className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
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

      {/* Equity */}
      {tab === "Equity" && (
        <Panel title="Valuations">
          {vals.length === 0 ? (
            <div className="text-sm text-zinc-400">No valuations yet.</div>
          ) : (
            <div className="space-y-2">
              {vals.map((v2: any) => (
                <div
                  key={v2.id}
                  className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
                >
                  <div className="flex items-center justify-between">
                    <div className="text-zinc-100 font-semibold">
                      {v2.as_of ? new Date(v2.as_of).toLocaleDateString() : "—"}
                    </div>
                    <div className="text-zinc-200 font-semibold">
                      {money(v2.estimated_value)}
                    </div>
                  </div>
                  <div className="text-xs text-zinc-400 mt-1">
                    Loan:{" "}
                    {v2.loan_balance != null ? money(v2.loan_balance) : "—"}{" "}
                    {v2.notes ? `· ${v2.notes}` : ""}
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
