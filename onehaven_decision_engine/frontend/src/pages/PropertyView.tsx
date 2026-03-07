import React from "react";
import { useParams } from "react-router-dom";
import { api, buildZillowUrl } from "../lib/api";

import AgentSlots from "../components/AgentSlots";
import PageHero from "../components/PageHero";
import Golem from "../components/Golem";
import PropertyImage from "../components/PropertyImage";
import PropertyCompliancePanel from "../components/PropertyCompliancePanel";
import { getFinancingType } from "../lib/dealRules";
import PageShell from "../components/PageShell";

const tabs = [
  "Deal",
  "Rehab",
  "Compliance",
  "Tenant",
  "Cash",
  "Equity",
] as const;
type Tab = (typeof tabs)[number];

const TAB_TO_STAGE: Record<Tab, string> = {
  Deal: "deal",
  Rehab: "rehab_plan",
  Compliance: "compliance",
  Tenant: "tenant",
  Cash: "cash",
  Equity: "equity",
};

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

function pct01(v: any) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${Math.round(n * 100)}%`;
}

function stageRank(stage: string | null | undefined) {
  const order = [
    "import",
    "deal",
    "decision",
    "acquisition",
    "rehab_plan",
    "rehab_exec",
    "compliance",
    "tenant",
    "lease",
    "cash",
    "equity",
  ];
  const idx = order.indexOf(String(stage || "").toLowerCase());
  return idx >= 0 ? idx : 0;
}

function isTabUnlocked(tab: Tab, currentStage: string | null | undefined) {
  const needed = TAB_TO_STAGE[tab];
  return stageRank(currentStage) >= stageRank(needed);
}

function prettyStage(stage: string | null | undefined) {
  const s = String(stage || "").toLowerCase();
  return s.replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

const Badge = React.memo(function Badge({
  children,
  tone = "neutral",
}: {
  children: React.ReactNode;
  tone?: "neutral" | "good" | "warn" | "bad";
}) {
  const cls =
    tone === "good"
      ? "border-green-400/20 bg-green-400/10 text-green-200"
      : tone === "warn"
        ? "border-yellow-300/20 bg-yellow-300/10 text-yellow-100"
        : tone === "bad"
          ? "border-red-400/20 bg-red-400/10 text-red-200"
          : "border-white/10 bg-white/5 text-white/80";

  return (
    <span className={`text-[11px] px-2 py-1 rounded-full border ${cls}`}>
      {children}
    </span>
  );
});

const Panel = React.memo(function Panel({
  title,
  right,
  children,
}: {
  title: string;
  right?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="oh-panel p-5" style={{ contain: "layout paint" }}>
      <div className="flex items-center justify-between gap-3">
        <div className="text-sm font-semibold text-white">{title}</div>
        {right ? <div>{right}</div> : null}
      </div>
      <div className="mt-3 space-y-2">{children}</div>
    </div>
  );
});

const Row = React.memo(function Row({ k, v }: { k: string; v: any }) {
  return (
    <div className="flex items-center justify-between gap-4 text-sm">
      <div className="text-white/55">{k}</div>
      <div className="text-white/85 font-medium text-right">{v}</div>
    </div>
  );
});

const ProgressBar = React.memo(function ProgressBar({
  value,
}: {
  value: number;
}) {
  const pct = Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
  return (
    <div className="h-2 rounded-full bg-white/10 overflow-hidden">
      <div className="h-2 bg-white/60" style={{ width: `${pct * 100}%` }} />
    </div>
  );
});

const TrustPill = React.memo(function TrustPill({
  score,
}: {
  score: number | null;
}) {
  const s =
    score == null ? null : Math.max(0, Math.min(100, Math.round(score)));
  const cls =
    s == null
      ? "border-white/10 bg-white/[0.03] text-white/70"
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
});

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
    <div
      className={`rounded-2xl border ${border} p-4`}
      style={{ contain: "layout paint" }}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-sm font-semibold text-white">
            {item?.description || item?.title || item?.item_code}
          </div>
          <div className="text-xs text-white/55 mt-1">
            {item?.category ? `${item.category} · ` : ""}
            <span className="text-white/85">{item?.item_code}</span>
            {" · "}status: <span className="text-white/85">{status}</span>
            {item?.marked_by ? ` · by ${item.marked_by}` : ""}
          </div>
        </div>

        <div className="flex flex-wrap items-center justify-end gap-2">
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "in_progress" })}
            className="oh-btn cursor-pointer"
          >
            working
          </button>
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "done" })}
            className="oh-btn oh-btn-good cursor-pointer"
          >
            done
          </button>
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "failed" })}
            className="oh-btn oh-btn-bad cursor-pointer"
          >
            fail
          </button>
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "blocked" })}
            className="oh-btn oh-btn-warn cursor-pointer"
          >
            blocked
          </button>
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-2">
        <div className="rounded-xl border border-white/10 bg-black/20 p-3">
          <div className="text-[11px] text-white/45">Proof URL</div>
          <div className="mt-2 flex items-center gap-2">
            <input
              defaultValue={item?.proof_url || ""}
              placeholder="https://..."
              className="oh-input focus-ring"
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
                className="text-xs underline text-white/80 cursor-pointer"
              >
                open
              </a>
            ) : null}
          </div>
        </div>

        <div className="rounded-xl border border-white/10 bg-black/20 p-3">
          <div className="text-[11px] text-white/45">Notes</div>
          <textarea
            defaultValue={item?.notes || ""}
            placeholder="What changed? What remains?"
            className="oh-textarea focus-ring"
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

function AgentsDrawer({
  open,
  onClose,
  propertyId,
}: {
  open: boolean;
  onClose: () => void;
  propertyId: number;
}) {
  return (
    <div
      className={[
        "fixed inset-0 z-[60] transition",
        open ? "pointer-events-auto" : "pointer-events-none",
      ].join(" ")}
      aria-hidden={!open}
    >
      <div
        onClick={onClose}
        className={[
          "absolute inset-0 bg-black/60 transition-opacity cursor-pointer",
          open ? "opacity-100" : "opacity-0",
        ].join(" ")}
      />
      <div
        className={[
          "absolute right-0 top-0 h-full w-[420px] max-w-[92vw]",
          "border-l border-white/10 bg-black/70 backdrop-blur-xl",
          "transition-transform",
          open ? "translate-x-0" : "translate-x-full",
        ].join(" ")}
        style={{ contain: "layout paint" }}
      >
        <div className="p-4 border-b border-white/10 flex items-center justify-between">
          <div className="text-sm font-semibold text-white">Agent Slots</div>
          <button className="oh-btn cursor-pointer" onClick={onClose}>
            close
          </button>
        </div>
        <div className="p-4 overflow-auto h-[calc(100%-64px)]">
          <AgentSlots propertyId={propertyId} />
        </div>
      </div>
    </div>
  );
}

function WorkflowRail({
  workflow,
  onAdvance,
  busy,
}: {
  workflow: any;
  onAdvance: () => Promise<void>;
  busy: boolean;
}) {
  const stages = Array.isArray(workflow?.stages) ? workflow.stages : [];
  const primaryAction = workflow?.primary_action;
  const gate = workflow?.gate || {};

  return (
    <Panel
      title="Workflow"
      right={
        primaryAction?.kind === "advance" ? (
          <button
            onClick={() => onAdvance().catch(() => {})}
            disabled={busy}
            className="oh-btn oh-btn-primary cursor-pointer"
          >
            {busy ? "advancing…" : primaryAction?.title || "advance"}
          </button>
        ) : (
          <Badge>{workflow?.current_stage_label || "—"}</Badge>
        )
      }
    >
      <div className="grid grid-cols-1 md:grid-cols-3 xl:grid-cols-6 gap-2">
        {stages.map((s: any) => {
          const status =
            s.status === "completed"
              ? "border-green-400/20 bg-green-400/8"
              : s.status === "current"
                ? "border-white/25 bg-white/10"
                : s.status === "next"
                  ? "border-yellow-300/20 bg-yellow-300/8"
                  : "border-white/10 bg-white/[0.03]";

          return (
            <div
              key={s.key}
              className={`rounded-2xl border p-3 ${status}`}
              style={{ contain: "layout paint" }}
            >
              <div className="text-[11px] uppercase tracking-wider text-white/45">
                {s.status}
              </div>
              <div className="mt-1 text-sm font-semibold text-white">
                {s.label}
              </div>
              <div className="mt-1 text-xs text-white/55 leading-relaxed">
                {s.description}
              </div>
            </div>
          );
        })}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-[1.1fr_.9fr] gap-3 pt-2">
        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="text-xs text-white/45">Primary next move</div>
          <div className="mt-2 text-sm font-semibold text-white">
            {primaryAction?.title || "No action required"}
          </div>
          <div className="mt-2 text-xs text-white/55">
            Current stage: {workflow?.current_stage_label || "—"}
            {workflow?.next_stage_label
              ? ` · Next stage: ${workflow.next_stage_label}`
              : ""}
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
          <div className="text-xs text-white/45">Transition gate</div>
          <div className="mt-2 text-sm text-white/80">
            {gate?.ok
              ? `Ready to move into ${gate?.allowed_next_stage_label || gate?.allowed_next_stage || "next stage"}.`
              : gate?.blocked_reason || "Not ready yet."}
          </div>
        </div>
      </div>
    </Panel>
  );
}

export default function PropertyView() {
  const { id } = useParams();
  const propertyId = Number(id);

  const [tab, setTab] = React.useState<Tab>("Deal");
  const [bundle, setBundle] = React.useState<any | null>(null);
  const [ops, setOps] = React.useState<any | null>(null);
  const [workflow, setWorkflow] = React.useState<any | null>(null);

  const [trust, setTrust] = React.useState<any | null>(null);
  const [trustErr, setTrustErr] = React.useState<string | null>(null);

  const [err, setErr] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);

  const [checklist, setChecklist] = React.useState<any | null>(null);
  const [checkBusyCode, setCheckBusyCode] = React.useState<string | null>(null);

  const [agentsOpen, setAgentsOpen] = React.useState(false);

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

  const noDeal = (err || "").toLowerCase().includes("nodealfoundforproperty");

  const loadAll = React.useCallback(async () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);

      const [out, opsOut, workflowOut, trustOut] = await Promise.all([
        api.propertyBundle(propertyId, ac.signal),
        api.opsPropertySummary(propertyId, 90, ac.signal).catch(() => null),
        api.opsPropertyWorkflow(propertyId, ac.signal).catch(() => null),
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
      setWorkflow(workflowOut ?? opsOut?.workflow ?? null);
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
      setWorkflow(null);
      setTrust(null);
      setErr(String(e.message || e));
    }
  }, [propertyId]);

  React.useEffect(() => {
    if (!Number.isFinite(propertyId)) {
      setErr("Invalid property id.");
      return;
    }
    loadAll();
    return () => abortRef.current?.abort();
  }, [propertyId, loadAll]);

  React.useEffect(() => {
    const currentStage = workflow?.current_stage || ops?.stage || "deal";
    if (!isTabUnlocked(tab, currentStage)) {
      if (isTabUnlocked("Equity", currentStage)) setTab("Equity");
      else if (isTabUnlocked("Cash", currentStage)) setTab("Cash");
      else if (isTabUnlocked("Tenant", currentStage)) setTab("Tenant");
      else if (isTabUnlocked("Compliance", currentStage)) setTab("Compliance");
      else if (isTabUnlocked("Rehab", currentStage)) setTab("Rehab");
      else setTab("Deal");
    }
  }, [workflow, ops, tab]);

  const doAction = React.useCallback(
    async (label: string, fn: () => Promise<any>) => {
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
    },
    [loadAll],
  );

  const advanceWorkflow = React.useCallback(async () => {
    await doAction("Advancing workflow…", async () => {
      await api.workflowAdvance(propertyId);
    });
  }, [doAction, propertyId]);

  const createDealQuick = React.useCallback(async () => {
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
  }, [doAction, propertyId]);

  const enrich = React.useCallback(async () => {
    await doAction("Enriching rent…", () =>
      api.enrichProperty(propertyId, d?.strategy || "section8"),
    );
  }, [doAction, propertyId, d?.strategy]);

  const explain = React.useCallback(async () => {
    await doAction("Explaining rent…", () =>
      api.explainProperty(propertyId, d?.strategy || "section8", true),
    );
  }, [doAction, propertyId, d?.strategy]);

  const evaluate = React.useCallback(async () => {
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
        "No evaluation method available. api.evaluateProperty is missing and no snapshot_id was found in the bundle/view.",
      );
    });
  }, [doAction, propertyId, d?.strategy, bundle]);

  const refreshChecklist = React.useCallback(async () => {
    const latest = await api.checklistLatest(propertyId);
    setChecklist(latest);
  }, [propertyId]);

  const generateChecklist = React.useCallback(async () => {
    await doAction("Generating checklist…", async () => {
      await api.generateChecklist(propertyId, {
        strategy: d?.strategy || "section8",
        persist: true,
      });
    });
  }, [doAction, propertyId, d?.strategy]);

  const generateRehabFromGaps = React.useCallback(async () => {
    await doAction("Generating rehab tasks from gaps…", async () => {
      await api.opsGenerateRehabTasks(propertyId);
    });
  }, [doAction, propertyId]);

  const checklistItems = checklist?.items ?? v?.checklist?.items ?? [];

  const heroTitle = p?.address ? p.address : `Property ${propertyId}`;
  const zillowUrl = p ? buildZillowUrl(p) : null;

  const decision = (r?.decision ?? "—") as string;
  const decisionTone =
    String(decision).toLowerCase().includes("pass") ||
    String(decision).toLowerCase().includes("surviv")
      ? "good"
      : String(decision).toLowerCase().includes("fail") ||
          String(decision).toLowerCase().includes("reject")
        ? "bad"
        : "neutral";

  const financing = getFinancingType(d?.asking_price);
  const financingTone = financing === "CASH DEAL" ? "warn" : "neutral";

  const heroSub =
    `${p?.city ?? "—"}, ${p?.state ?? "—"} ${p?.zip ?? ""}`.trim();

  const stage = workflow?.current_stage || ops?.stage || "deal";
  const stageLabel =
    workflow?.current_stage_label || ops?.stage_label || prettyStage(stage);
  const cp = ops?.checklist_progress || {};
  const insp = ops?.inspection || {};
  const cash30 = ops?.cash?.last_30_days || {};
  const equity = ops?.equity || null;
  const nextActions: string[] = Array.isArray(ops?.next_actions)
    ? ops.next_actions
    : Array.isArray(workflow?.next_actions)
      ? workflow.next_actions
      : [];

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

  const primaryActionTitle =
    workflow?.primary_action?.title || nextActions[0] || "No immediate action";

  return (
    <PageShell className="relative space-y-5">
      <AgentsDrawer
        open={agentsOpen}
        onClose={() => setAgentsOpen(false)}
        propertyId={propertyId}
      />

      <PageHero
        eyebrow="Property"
        title={heroTitle}
        subtitle={heroSub}
        tilt={false}
        right={
          <div className="absolute inset-0 flex items-center justify-center pointer-events-auto overflow-visible">
            <div className="h-[210px] w-[210px] md:h-[230px] md:w-[230px] translate-y-[-12px] opacity-95">
              <Golem className="h-full w-full" />
            </div>
          </div>
        }
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <button
              onClick={loadAll}
              className="oh-btn cursor-pointer"
              disabled={!!busy}
              title="Refresh"
            >
              sync
            </button>

            <button
              onClick={evaluate}
              className="oh-btn oh-btn-primary cursor-pointer"
              disabled={!!busy || !d}
              title="Evaluate"
            >
              evaluate
            </button>

            <button
              onClick={enrich}
              className="oh-btn cursor-pointer"
              disabled={!!busy || !d}
              title="Rent enrich"
            >
              enrich
            </button>

            <button
              onClick={explain}
              className="oh-btn cursor-pointer"
              disabled={!!busy || !d}
              title="Rent explain (persisted)"
            >
              explain
            </button>

            <button
              onClick={createDealQuick}
              className="oh-btn cursor-pointer"
              disabled={!!busy}
              title="Create a deal if missing"
            >
              {busy?.includes("Creating") ? "creating…" : "+ deal"}
            </button>

            {workflow?.primary_action?.kind === "advance" && (
              <button
                onClick={advanceWorkflow}
                className="oh-btn oh-btn-primary cursor-pointer"
                disabled={!!busy}
                title="Advance to the next unlocked workflow stage"
              >
                {busy?.includes("Advancing") ? "advancing…" : "advance"}
              </button>
            )}

            <span className="hidden md:inline-block w-2" />

            {zillowUrl && (
              <a
                href={zillowUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="oh-btn cursor-pointer"
                title="Open Zillow (new tab)"
              >
                Zillow ↗
              </a>
            )}

            <button
              className="oh-btn cursor-pointer"
              onClick={() => setAgentsOpen(true)}
              title="Open agent slots drawer"
            >
              agents
            </button>
          </div>
        }
      />

      <div className="grid grid-cols-1 lg:grid-cols-[360px_1fr] gap-4">
        <div className="oh-panel p-4">
          <div className="text-xs uppercase tracking-widest text-white/45">
            House
          </div>

          <div className="mt-3">
            <PropertyImage
              address={p?.address}
              city={p?.city}
              state={p?.state}
              zip={p?.zip}
              className="h-[220px] w-full"
              roundedClassName="rounded-2xl"
            />
          </div>

          <div className="mt-3 flex flex-wrap gap-2">
            <Badge tone={decisionTone}>Decision: {decision}</Badge>
            <Badge>Score: {r?.score ?? "—"}</Badge>
            <Badge>DSCR: {r?.dscr?.toFixed?.(2) ?? "—"}</Badge>
            <Badge tone={financingTone}>{financing}</Badge>
          </div>

          <div className="mt-3 text-xs text-white/45">
            Strategy:{" "}
            <span className="text-white/80 font-semibold">
              {String(d?.strategy || "section8").toUpperCase()}
            </span>
            {" · "}Stage:{" "}
            <span className="text-white/80 font-semibold">
              {String(stageLabel).toUpperCase()}
            </span>
          </div>

          <div className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] p-3">
            <div className="text-[11px] uppercase tracking-wider text-white/45">
              Required next move
            </div>
            <div className="mt-2 text-sm font-semibold text-white">
              {primaryActionTitle}
            </div>
          </div>
        </div>

        <div className="space-y-4">
          <WorkflowRail
            workflow={workflow}
            onAdvance={advanceWorkflow}
            busy={!!busy}
          />

          <Panel
            title="Reality Loop"
            right={
              <div className="flex flex-wrap items-center gap-2 justify-end">
                {cp?.total != null ? (
                  <Badge>
                    Checklist {cp.done ?? 0}/{cp.total ?? 0} (
                    {pct01(cp.pct_done)})
                  </Badge>
                ) : (
                  <Badge>Checklist —</Badge>
                )}

                {insp?.latest ? (
                  <Badge tone={insp.latest.passed ? "good" : "warn"}>
                    Inspection {insp.latest.passed ? "PASSED" : "OPEN"} · fails{" "}
                    {insp.open_failed_items ?? 0}
                  </Badge>
                ) : (
                  <Badge>Inspection NONE</Badge>
                )}
              </div>
            }
          >
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="text-xs text-white/45">Checklist</div>
                <div className="mt-2">
                  <Row
                    k="Done"
                    v={`${cp.done ?? 0}/${cp.total ?? 0} (${pct01(cp.pct_done)})`}
                  />
                  <div className="mt-2">
                    <ProgressBar value={Number(cp.pct_done || 0)} />
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="text-xs text-white/45">Cash (30d)</div>
                <div className="mt-2 space-y-1">
                  <Row k="Income" v={money(cash30.income)} />
                  <Row k="Net" v={money(cash30.net)} />
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="text-xs text-white/45">Equity</div>
                <div className="mt-2 space-y-1">
                  <Row
                    k="Value"
                    v={equity ? money(equity.estimated_value) : "—"}
                  />
                  <Row
                    k="Equity"
                    v={equity ? money(equity.estimated_equity) : "—"}
                  />
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="text-xs text-white/45">One-click</div>
                <div className="mt-3 flex flex-wrap items-center gap-2">
                  <button
                    onClick={generateChecklist}
                    className="oh-btn oh-btn-primary cursor-pointer"
                    disabled={!!busy || !d}
                    title="Generate and persist checklist"
                  >
                    checklist
                  </button>
                  <button
                    onClick={generateRehabFromGaps}
                    className="oh-btn cursor-pointer"
                    disabled={!!busy}
                    title="Creates rehab tasks from checklist gaps + unresolved inspection fails"
                  >
                    rehab gaps
                  </button>
                </div>
              </div>
            </div>

            <div className="pt-2">
              <div className="text-xs text-white/45 mb-2">Next actions</div>
              {nextActions.length ? (
                <div className="space-y-2">
                  {nextActions.slice(0, 5).map((a, i) => (
                    <div
                      key={i}
                      className="rounded-xl border border-white/10 bg-white/[0.03] p-3 text-sm text-white/80"
                    >
                      {a}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-sm text-white/55">
                  No blockers detected.
                </div>
              )}
            </div>
          </Panel>

          <Panel title="Trust" right={<TrustPill score={trustScore} />}>
            {trust == null ? (
              <div className="text-sm text-white/55">
                Trust is not available yet.
                {trustErr ? (
                  <div className="mt-2 text-xs text-white/45">{trustErr}</div>
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
                    <div className="text-[11px] text-white/45 mb-2">
                      Top positives
                    </div>
                    {positives.length ? (
                      <div className="space-y-1">
                        {positives.slice(0, 2).map((x: any, i: number) => (
                          <div key={i} className="text-sm text-white/80">
                            •{" "}
                            {x.signal_key ||
                              x.key ||
                              x.name ||
                              JSON.stringify(x)}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-sm text-white/55">—</div>
                    )}
                  </div>

                  <div className="rounded-xl border border-white/10 bg-white/[0.03] p-3">
                    <div className="text-[11px] text-white/45 mb-2">
                      Top negatives
                    </div>
                    {negatives.length ? (
                      <div className="space-y-1">
                        {negatives.slice(0, 2).map((x: any, i: number) => (
                          <div key={i} className="text-sm text-white/80">
                            •{" "}
                            {x.signal_key ||
                              x.key ||
                              x.name ||
                              JSON.stringify(x)}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-sm text-white/55">—</div>
                    )}
                  </div>
                </div>

                <div className="text-xs text-white/45">
                  Trust reflects completeness + consistency of the pipeline.
                </div>
              </div>
            )}
          </Panel>
        </div>
      </div>

      {busy && (
        <div className="oh-panel-solid p-4 border-white/10 bg-white/5 text-white/80">
          {busy}
        </div>
      )}

      {err && (
        <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
          {noDeal
            ? 'No deal exists for this property yet. Click "+ deal" to create one, then run enrich/explain/evaluate.'
            : err}
        </div>
      )}

      <div className="gradient-border rounded-2xl p-[1px]">
        <div className="glass rounded-2xl p-2 flex gap-2 flex-wrap">
          {tabs.map((t) => {
            const unlocked = isTabUnlocked(t, stage);
            const active = tab === t;

            return (
              <button
                key={t}
                onClick={() => {
                  if (!unlocked) return;
                  setTab(t);
                }}
                disabled={!unlocked}
                title={
                  unlocked
                    ? `Open ${t}`
                    : `Locked until workflow reaches ${prettyStage(TAB_TO_STAGE[t])}`
                }
                className={[
                  "px-3 py-2 rounded-xl border text-sm transition focus-ring",
                  unlocked ? "cursor-pointer" : "cursor-not-allowed opacity-50",
                  active
                    ? "bg-white/[0.07] text-white border-white/[0.18]"
                    : unlocked
                      ? "text-white/70 border-white/10 hover:bg-white/[0.04] hover:border-white/[0.14]"
                      : "text-white/45 border-white/8 bg-white/[0.02]",
                ].join(" ")}
              >
                {t}
              </button>
            );
          })}
        </div>
      </div>

      {tab === "Deal" && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="lg:col-span-2 grid grid-cols-1 md:grid-cols-2 gap-4">
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
              <div className="mt-2 text-xs text-white/45">Reasons</div>
              <ul className="mt-1 text-sm text-white/80 space-y-1 list-disc pl-5">
                {(friction?.reasons ?? []).map((x: string, i: number) => (
                  <li key={i}>{x}</li>
                ))}
                {(friction?.reasons ?? []).length === 0 && (
                  <li className="text-white/55">—</li>
                )}
              </ul>
            </Panel>

            <Panel title="Deal inputs">
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

          <div className="space-y-4">
            <Panel title="Guidance">
              <div className="text-sm text-white/70 leading-relaxed">
                Start here. Deal is the first real decision gate in the
                pipeline. Run enrich, explain, and evaluate before trying to
                move the property deeper into the workflow.
              </div>
            </Panel>
          </div>
        </div>
      )}

      {tab === "Rehab" && (
        <Panel
          title="Rehab Tasks"
          right={
            <button
              onClick={generateRehabFromGaps}
              className="oh-btn cursor-pointer"
              disabled={!!busy}
              title="Creates rehab tasks from checklist gaps + unresolved inspection fails"
            >
              rehab from gaps
            </button>
          }
        >
          {rehab.length === 0 ? (
            <div className="text-sm text-white/55">No rehab tasks yet.</div>
          ) : (
            <div className="space-y-2">
              {rehab.map((t: any) => (
                <div
                  key={t.id}
                  className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
                  style={{ contain: "layout paint" }}
                >
                  <div className="flex items-center justify-between">
                    <div className="font-semibold text-white">{t.title}</div>
                    <span className="text-[11px] px-2 py-1 rounded-full border border-white/10 bg-white/5 text-white/70">
                      {t.status}
                    </span>
                  </div>
                  <div className="text-xs text-white/55 mt-1">
                    {t.deadline
                      ? `Due: ${new Date(t.deadline).toLocaleDateString()}`
                      : "No deadline"}{" "}
                    ·{" "}
                    {t.cost_estimate != null
                      ? `Est: ${money(t.cost_estimate)}`
                      : "No estimate"}
                  </div>
                  {t.notes && (
                    <div className="text-sm text-white/70 mt-2">{t.notes}</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </Panel>
      )}

      {tab === "Compliance" && (
        <div className="space-y-4">
          <PropertyCompliancePanel
            property={{
              id: p?.id ?? propertyId,
              state: p?.state,
              county: p?.county,
              city: p?.city,
              strategy: d?.strategy,
            }}
          />

          <Panel title="Compliance / Checklist">
            <div className="flex items-center justify-between gap-3 flex-wrap">
              <div className="text-sm text-white/70">
                Update status, proof, and notes. Compliance must be genuinely
                complete before tenant placement unlocks.
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() =>
                    refreshChecklist().catch((e) =>
                      setErr(String(e?.message || e)),
                    )
                  }
                  className="oh-btn cursor-pointer"
                >
                  refresh
                </button>
                <button
                  onClick={generateChecklist}
                  className="oh-btn oh-btn-primary cursor-pointer"
                  disabled={!!busy || !d}
                >
                  generate
                </button>
                <button
                  onClick={generateRehabFromGaps}
                  className="oh-btn cursor-pointer"
                  disabled={!!busy}
                  title="Creates rehab tasks from checklist gaps + unresolved inspection fails"
                >
                  rehab from gaps
                </button>
              </div>
            </div>

            <div className="mt-4 space-y-2">
              {checklistItems.length === 0 ? (
                <div className="text-sm text-white/55">
                  No checklist found yet. Click{" "}
                  <span className="text-white font-semibold">generate</span> to
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

                        const [opsOut, workflowOut, trustOut] =
                          await Promise.all([
                            api
                              .opsPropertySummary(propertyId, 90)
                              .catch(() => null),
                            api
                              .opsPropertyWorkflow(propertyId)
                              .catch(() => null),
                            api
                              .trustGet("property", propertyId)
                              .catch(() => null),
                          ]);
                        setOps(opsOut);
                        setWorkflow(workflowOut ?? opsOut?.workflow ?? null);
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
        </div>
      )}

      {tab === "Tenant" && (
        <Panel title="Leases">
          {leases.length === 0 ? (
            <div className="text-sm text-white/55">No leases yet.</div>
          ) : (
            <div className="space-y-2">
              {leases.map((l: any) => (
                <div
                  key={l.id}
                  className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
                  style={{ contain: "layout paint" }}
                >
                  <div className="flex items-center justify-between">
                    <div className="font-semibold text-white">
                      Tenant #{l.tenant_id}
                    </div>
                    <div className="text-sm text-white/85 font-semibold">
                      {money(l.total_rent)}
                    </div>
                  </div>
                  <div className="text-xs text-white/55 mt-1">
                    Start: {new Date(l.start_date).toLocaleDateString()}
                    {l.end_date
                      ? ` · End: ${new Date(l.end_date).toLocaleDateString()}`
                      : ""}
                  </div>
                  {l.notes && (
                    <div className="text-sm text-white/70 mt-2">{l.notes}</div>
                  )}
                </div>
              ))}
            </div>
          )}
        </Panel>
      )}

      {tab === "Cash" && (
        <Panel title="Transactions">
          {txns.length === 0 ? (
            <div className="text-sm text-white/55">No transactions yet.</div>
          ) : (
            <div className="space-y-2">
              {txns.map((t: any) => (
                <div
                  key={t.id}
                  className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
                  style={{ contain: "layout paint" }}
                >
                  <div className="flex items-center justify-between">
                    <div className="text-white font-semibold">
                      {t.txn_type || t.type || "txn"}
                    </div>
                    <div className="text-white/85 font-semibold">
                      {money(t.amount)}
                    </div>
                  </div>
                  <div className="text-xs text-white/55 mt-1">
                    {t.txn_date
                      ? new Date(t.txn_date).toLocaleDateString()
                      : "—"}
                    {t.memo ? ` · ${t.memo}` : ""}
                  </div>
                </div>
              ))}
            </div>
          )}
        </Panel>
      )}

      {tab === "Equity" && (
        <Panel title="Valuations">
          {vals.length === 0 ? (
            <div className="text-sm text-white/55">No valuations yet.</div>
          ) : (
            <div className="space-y-2">
              {vals.map((v2: any) => (
                <div
                  key={v2.id}
                  className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
                  style={{ contain: "layout paint" }}
                >
                  <div className="flex items-center justify-between">
                    <div className="text-white font-semibold">
                      {v2.as_of ? new Date(v2.as_of).toLocaleDateString() : "—"}
                    </div>
                    <div className="text-white/85 font-semibold">
                      {money(v2.estimated_value)}
                    </div>
                  </div>
                  <div className="text-xs text-white/55 mt-1">
                    Loan:{" "}
                    {v2.loan_balance != null ? money(v2.loan_balance) : "—"}
                    {v2.notes ? ` · ${v2.notes}` : ""}
                  </div>
                </div>
              ))}
            </div>
          )}
        </Panel>
      )}
    </PageShell>
  );
}
