import React from "react";
import { useParams } from "react-router-dom";
import { api, buildZillowUrl } from "../lib/api";

import AgentSlots from "../components/AgentSlots";
import NextActionsPanel from "../components/NextActionsPanel";
import PageHero from "../components/PageHero";
import Golem from "../components/Golem";
import PropertyImage from "../components/PropertyImage";
import PropertyCompliancePanel from "../components/PropertyCompliancePanel";
import RiskBadges from "../components/RiskBadges";
import StageProgress from "../components/StageProgress";
import PageShell from "../components/PageShell";
import PhotoUploader from "../components/PhotoUploader";
import PhotoGallery from "../components/PhotoGallery";
import RehabFromPhotosCTA from "../components/RehabFromPhotosCTA";
import InspectionReadiness from "../components/InspectionReadiness";
import TenantPipeline from "../components/TenantPipeline";
import Surface from "../components/Surface";
import KpiCard from "../components/KpiCard";
import EmptyState from "../components/EmptyState";

const tabs = [
  "Deal",
  "Rehab",
  "Compliance",
  "Tenant",
  "Lease",
  "Cash / Equity",
] as const;

type Tab = (typeof tabs)[number];

const TAB_TO_STAGE: Record<Tab, string> = {
  Deal: "deal",
  Rehab: "rehab",
  Compliance: "compliance",
  Tenant: "tenant",
  Lease: "lease",
  "Cash / Equity": "cash_equity",
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

function pct(v: any, digits = 1) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(digits)}%`;
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

function normalizeStage(raw?: string) {
  const x = String(raw || "")
    .trim()
    .toLowerCase();

  if (
    [
      "import",
      "intake",
      "deal",
      "decision",
      "acquisition",
      "procurement",
      "underwriting",
    ].includes(x)
  ) {
    return "deal";
  }
  if (
    [
      "rehab",
      "rehab_plan",
      "rehab_exec",
      "renovation",
      "construction",
    ].includes(x)
  ) {
    return "rehab";
  }
  if (["compliance", "inspection", "licensing"].includes(x)) {
    return "compliance";
  }
  if (["tenant", "voucher"].includes(x)) {
    return "tenant";
  }
  if (["lease", "leasing", "management", "ops"].includes(x)) {
    return "lease";
  }
  if (["cash", "equity", "cashflow", "portfolio", "cash_equity"].includes(x)) {
    return "cash_equity";
  }

  return "deal";
}

function stageRank(stage: string | null | undefined) {
  const order = [
    "deal",
    "rehab",
    "compliance",
    "tenant",
    "lease",
    "cash_equity",
  ];
  const idx = order.indexOf(normalizeStage(stage || ""));
  return idx >= 0 ? idx : 0;
}

function isTabUnlocked(tab: Tab, currentStage: string | null | undefined) {
  const needed = TAB_TO_STAGE[tab];
  return stageRank(currentStage) >= stageRank(needed);
}

function prettyStage(stage: string | null | undefined) {
  const s = normalizeStage(stage || "");
  if (s === "deal") return "Deal / Procurement";
  if (s === "rehab") return "Rehab";
  if (s === "compliance") return "Compliance";
  if (s === "tenant") return "Tenant Placement";
  if (s === "lease") return "Lease / Management";
  return "Cashflow / Equity";
}

function classificationTone(classification: string) {
  if (classification === "GOOD_DEAL") return "good";
  if (classification === "REVIEW") return "warn";
  return "bad";
}

const Badge = React.memo(function Badge({
  children,
  tone = "neutral",
}: {
  children: React.ReactNode;
  tone?: "neutral" | "good" | "warn" | "bad" | "accent";
}) {
  const cls =
    tone === "good"
      ? "oh-pill oh-pill-good"
      : tone === "warn"
        ? "oh-pill oh-pill-warn"
        : tone === "bad"
          ? "oh-pill oh-pill-bad"
          : tone === "accent"
            ? "oh-pill oh-pill-accent"
            : "oh-pill";

  return <span className={cls}>{children}</span>;
});

const Row = React.memo(function Row({ k, v }: { k: string; v: any }) {
  return (
    <div className="flex items-center justify-between gap-4 text-sm">
      <div className="text-app-4">{k}</div>
      <div className="text-app-1 font-medium text-right">{v}</div>
    </div>
  );
});

const ProgressBar = React.memo(function ProgressBar({
  value,
}: {
  value: number;
}) {
  const p = Math.max(0, Math.min(1, Number.isFinite(value) ? value : 0));
  return (
    <div className="h-2 rounded-full bg-app-muted overflow-hidden">
      <div
        className="h-2 rounded-full bg-[linear-gradient(90deg,var(--accent),var(--accent-2))]"
        style={{ width: `${p * 100}%` }}
      />
    </div>
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
  const tone =
    status === "done"
      ? "success"
      : status === "failed"
        ? "danger"
        : status === "blocked"
          ? "warning"
          : "default";

  return (
    <Surface tone={tone as any} padding="md">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-sm font-semibold text-app-0">
            {item?.description || item?.title || item?.item_code}
          </div>
          <div className="text-xs text-app-4 mt-1">
            {item?.category ? `${item.category} · ` : ""}
            <span className="text-app-2">{item?.item_code}</span>
            {" · "}status: <span className="text-app-2">{status}</span>
            {item?.marked_by ? ` · by ${item.marked_by}` : ""}
          </div>
        </div>

        <div className="flex flex-wrap items-center justify-end gap-2">
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "in_progress" })}
            className="oh-btn oh-btn-secondary cursor-pointer"
          >
            working
          </button>
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "done" })}
            className="oh-btn oh-btn-primary cursor-pointer"
          >
            done
          </button>
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "failed" })}
            className="oh-btn oh-btn-secondary cursor-pointer"
          >
            fail
          </button>
          <button
            disabled={busy}
            onClick={() => onUpdate({ status: "blocked" })}
            className="oh-btn oh-btn-secondary cursor-pointer"
          >
            blocked
          </button>
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3">
        <div className="rounded-2xl border border-app bg-app-muted p-3">
          <div className="text-[11px] text-app-4">Proof URL</div>
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
                className="text-xs underline text-app-2 cursor-pointer"
              >
                open
              </a>
            ) : null}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted p-3">
          <div className="text-[11px] text-app-4">Notes</div>
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
    </Surface>
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
          "border-l border-app bg-[color:var(--bg-elevated)] backdrop-blur-xl",
          "transition-transform",
          open ? "translate-x-0" : "translate-x-full",
        ].join(" ")}
      >
        <div className="p-4 border-b border-app flex items-center justify-between">
          <div className="text-sm font-semibold text-app-0">Agent Slots</div>
          <button
            className="oh-btn oh-btn-secondary cursor-pointer"
            onClick={onClose}
          >
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

export default function PropertyView() {
  const { id } = useParams();
  const propertyId = Number(id);

  const [tab, setTab] = React.useState<Tab>("Deal");
  const [bundle, setBundle] = React.useState<any | null>(null);
  const [ops, setOps] = React.useState<any | null>(null);
  const [workflow, setWorkflow] = React.useState<any | null>(null);

  const [err, setErr] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState<string | null>(null);

  const [checklist, setChecklist] = React.useState<any | null>(null);
  const [checkBusyCode, setCheckBusyCode] = React.useState<string | null>(null);

  const [agentsOpen, setAgentsOpen] = React.useState(false);

  const [photos, setPhotos] = React.useState<any[]>([]);
  const [photoAnalysis, setPhotoAnalysis] = React.useState<any | null>(null);
  const [photoBusy, setPhotoBusy] = React.useState(false);

  const [complianceBrief, setComplianceBrief] = React.useState<any | null>(
    null,
  );
  const [complianceStatus, setComplianceStatus] = React.useState<any | null>(
    null,
  );
  const [complianceRunSummary, setComplianceRunSummary] = React.useState<
    any | null
  >(null);
  const [inspectionReadiness, setInspectionReadiness] = React.useState<
    any | null
  >(null);
  const [complianceAutomationBusy, setComplianceAutomationBusy] =
    React.useState(false);

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
  const tenants = bundle?.tenants || [];

  const noDeal = (err || "").toLowerCase().includes("nodealfoundforproperty");

  const refreshPhotos = React.useCallback(
    async (signal?: AbortSignal) => {
      if (!Number.isFinite(propertyId)) return;
      try {
        const rows = await api.photos(propertyId, signal);
        setPhotos(Array.isArray(rows) ? rows : []);
      } catch {
        setPhotos([]);
      }
    },
    [propertyId],
  );

  const refreshChecklist = React.useCallback(
    async (signal?: AbortSignal) => {
      const latest = await api.checklistLatest(propertyId, signal);
      setChecklist(latest);
    },
    [propertyId],
  );

  const loadAll = React.useCallback(async () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);

      const [
        out,
        opsOut,
        workflowOut,
        complianceBriefOut,
        complianceStatusOut,
        complianceRunSummaryOut,
        inspectionReadinessOut,
      ] = await Promise.all([
        api.propertyBundle(propertyId, ac.signal),
        api.opsPropertySummary(propertyId, 90, ac.signal).catch(() => null),
        api.opsPropertyWorkflow(propertyId, ac.signal).catch(() => null),
        api.compliancePropertyBrief(propertyId, ac.signal).catch(() => null),
        api.complianceStatus(propertyId, ac.signal).catch(() => null),
        api.complianceRunSummary(propertyId, ac.signal).catch(() => null),
        api
          .complianceInspectionReadiness(propertyId, ac.signal)
          .catch(() => null),
      ]);

      setBundle(out);
      setOps(opsOut);
      setWorkflow(workflowOut ?? opsOut?.workflow ?? null);

      setComplianceBrief(complianceBriefOut);
      setComplianceStatus(complianceStatusOut);
      setComplianceRunSummary(complianceRunSummaryOut);
      setInspectionReadiness(inspectionReadinessOut);

      try {
        await refreshChecklist(ac.signal);
      } catch {
        setChecklist(null);
      }

      const galleryFromBundle = Array.isArray(out?.photo_gallery)
        ? out.photo_gallery
        : Array.isArray(out?.view?.photo_gallery)
          ? out.view.photo_gallery
          : Array.isArray(out?.view?.photo_gallery?.photos)
            ? out.view.photo_gallery.photos
            : [];

      if (galleryFromBundle.length) {
        setPhotos(galleryFromBundle);
      } else {
        await refreshPhotos(ac.signal);
      }
    } catch (e: any) {
      if (String(e?.name) === "AbortError") return;
      setBundle(null);
      setOps(null);
      setWorkflow(null);
      setComplianceBrief(null);
      setComplianceStatus(null);
      setComplianceRunSummary(null);
      setInspectionReadiness(null);
      setPhotos([]);
      setErr(String(e.message || e));
    }
  }, [propertyId, refreshChecklist, refreshPhotos]);

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
      if (isTabUnlocked("Cash / Equity", currentStage)) setTab("Cash / Equity");
      else if (isTabUnlocked("Lease", currentStage)) setTab("Lease");
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

  const runComplianceAutomation = React.useCallback(async () => {
    try {
      setComplianceAutomationBusy(true);
      setErr(null);

      const out = await api.runComplianceAutomation(propertyId, true);
      setInspectionReadiness(
        out && typeof out === "object" && "inspection_readiness" in out
          ? out.inspection_readiness
          : null,
      );
      setComplianceRunSummary(
        out && typeof out === "object" && "summary" in out ? out.summary : null,
      );

      await loadAll();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setComplianceAutomationBusy(false);
    }
  }, [propertyId, loadAll]);

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

  const handlePreviewRehabFromPhotos = React.useCallback(async () => {
    try {
      setPhotoBusy(true);
      setErr(null);
      const out = await api.previewRehabFromPhotos(propertyId);
      setPhotoAnalysis(out);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setPhotoBusy(false);
    }
  }, [propertyId]);

  const handleGenerateRehabFromPhotos = React.useCallback(async () => {
    try {
      setPhotoBusy(true);
      setErr(null);
      const out = await api.generateRehabFromPhotos(propertyId);
      setPhotoAnalysis(out);
      await loadAll();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setPhotoBusy(false);
    }
  }, [propertyId, loadAll]);

  const handleDeletePhoto = React.useCallback(
    async (photoId: number) => {
      try {
        setPhotoBusy(true);
        setErr(null);
        await api.deletePhoto(photoId);
        await refreshPhotos();
      } catch (e: any) {
        setErr(String(e?.message || e));
      } finally {
        setPhotoBusy(false);
      }
    },
    [refreshPhotos],
  );

  const handleUploadedPhoto = React.useCallback(async () => {
    await refreshPhotos();
  }, [refreshPhotos]);

  const checklistItems = checklist?.items ?? v?.checklist?.items ?? [];

  const heroTitle = p?.address ? p.address : `Property ${propertyId}`;
  const zillowUrl = p ? buildZillowUrl(p) : null;

  const classification = normalizeDecision(
    r?.decision ?? v?.classification ?? ops?.classification ?? "REVIEW",
  );
  const classificationToneValue = classificationTone(classification);

  const heroSub =
    `${p?.city ?? "—"}, ${p?.state ?? "—"} ${p?.zip ?? ""}`.trim();

  const stage = normalizeStage(workflow?.current_stage || ops?.stage || "deal");
  const stageLabel = prettyStage(
    workflow?.current_stage_label || ops?.stage_label || stage,
  );

  const cp = ops?.checklist_progress || {};
  const insp = ops?.inspection || {};
  const cash30 = ops?.cash?.last_30_days || {};
  const cash90 =
    ops?.cash?.last_90_days || ops?.cash?.last_90 || ops?.cash?.window_90 || {};
  const equity = ops?.equity || null;
  const tenantSummary = ops?.tenant || {};

  const nextActions = React.useMemo(() => {
    const rows: Array<
      | string
      | {
          title?: string;
          detail?: string;
          kind?: string;
          priority?: string;
          due_at?: string | null;
          href?: string | null;
        }
    > = [];

    if (!d) {
      rows.push({
        title: "Create initial deal record",
        detail:
          "This property cannot move cleanly through underwriting and workflow gates until the deal record exists.",
        kind: "deal",
        priority: "high",
      });
    }

    if (workflow?.primary_action?.title) {
      rows.push({
        title: workflow.primary_action.title,
        detail:
          workflow?.transition_gate?.blocked_reason ||
          "This is the next workflow action currently expected for this property.",
        kind: workflow?.primary_action?.kind || "workflow",
        priority: workflow?.transition_gate?.ok ? "high" : "medium",
      });
    }

    if (stage === "compliance" && checklistItems.length === 0) {
      rows.push({
        title: "Generate compliance checklist",
        detail:
          "Compliance should be tracked with checklist items before tenant placement is allowed.",
        kind: "compliance",
        priority: "high",
      });
    }

    if (stage === "rehab" && rehab.length === 0) {
      rows.push({
        title: "Create rehab tasks from current gaps",
        detail:
          "Convert open checklist gaps and unresolved inspection issues into rehab tasks.",
        kind: "rehab",
        priority: "medium",
      });
    }

    if (
      stage === "tenant" &&
      !tenantSummary?.occupancy_status &&
      leases.length === 0
    ) {
      rows.push({
        title: "Start tenant placement",
        detail:
          "This asset is in tenant placement stage but does not yet show an active placement or lease.",
        kind: "tenant",
        priority: "medium",
      });
    }

    if (
      stage === "cash_equity" &&
      txns.length === 0 &&
      vals.length === 0 &&
      equity == null
    ) {
      rows.push({
        title: "Begin operating history capture",
        detail:
          "Cashflow and equity stage is unlocked, but there is not enough operating history showing yet.",
        kind: "cashflow",
        priority: "low",
      });
    }

    if (Array.isArray(ops?.next_actions) && ops.next_actions.length > 0) {
      for (const item of ops.next_actions.slice(0, 3)) {
        if (typeof item === "string") {
          rows.push(item);
        } else if (item && typeof item === "object") {
          rows.push({
            title: item.title || item.label || "Next action",
            detail: item.detail || item.description || "",
            kind: item.kind || "manual",
            priority: item.priority || "normal",
            due_at: item.due_at || null,
            href: item.href || null,
          });
        }
      }
    } else if (
      Array.isArray(workflow?.next_actions) &&
      workflow.next_actions.length > 0
    ) {
      for (const item of workflow.next_actions.slice(0, 3)) {
        if (typeof item === "string") rows.push(item);
      }
    }

    if (rows.length === 0) {
      rows.push({
        title: "Refresh property state",
        detail:
          "No clear next action is currently surfaced. Refresh this property and verify stage blockers, checklist state, and underwriting outputs.",
        kind: "sync",
        priority: "low",
      });
    }

    return rows;
  }, [
    d,
    workflow,
    checklistItems.length,
    stage,
    rehab.length,
    tenantSummary,
    leases.length,
    txns.length,
    vals.length,
    equity,
    ops?.next_actions,
  ]);

  const primaryActionTitle =
    workflow?.primary_action?.title ||
    (typeof nextActions[0] === "string"
      ? nextActions[0]
      : nextActions[0]?.title) ||
    "No immediate action";

  const geo = bundle?.geo || {};
  const photoGallery = bundle?.photo_gallery || {};
  const fallbackPhotoUrls = Array.isArray(photoGallery?.photos)
    ? photoGallery.photos
    : [];
  const propertyImagePhotos =
    photos.length > 0
      ? photos.map((x) => x?.url).filter(Boolean)
      : fallbackPhotoUrls;

  const dscr = numberOrNull(r?.dscr ?? ops?.dscr);
  const askingPrice = numberOrNull(d?.asking_price ?? ops?.asking_price);
  const cashflowEstimate = numberOrNull(
    r?.cash_flow ?? ops?.cashflow_estimate ?? cash90?.net,
  );
  const crimeScore = numberOrNull(geo?.crime_score ?? p?.crime_score);

  return (
    <PageShell className="relative space-y-6">
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
              className="oh-btn oh-btn-secondary cursor-pointer"
              disabled={!!busy}
            >
              sync
            </button>

            <button
              onClick={createDealQuick}
              className="oh-btn oh-btn-secondary cursor-pointer"
              disabled={!!busy}
            >
              {busy?.includes("Creating") ? "creating…" : "+ deal"}
            </button>

            {workflow?.primary_action?.kind === "advance" && (
              <button
                onClick={advanceWorkflow}
                className="oh-btn oh-btn-primary cursor-pointer"
                disabled={!!busy}
              >
                {busy?.includes("Advancing") ? "advancing…" : "advance"}
              </button>
            )}

            {zillowUrl && (
              <a
                href={zillowUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="oh-btn oh-btn-secondary cursor-pointer"
              >
                Zillow ↗
              </a>
            )}

            <button
              className="oh-btn oh-btn-secondary cursor-pointer"
              onClick={() => setAgentsOpen(true)}
            >
              agents
            </button>
          </div>
        }
      />

      <div className="grid grid-cols-1 lg:grid-cols-[360px_1fr] gap-4">
        <Surface padding="md">
          <div className="text-xs uppercase tracking-widest text-app-4">
            House
          </div>

          <div className="mt-3">
            <PropertyImage
              photos={propertyImagePhotos}
              zillowUrl={zillowUrl}
              className="w-full"
              roundedClassName="rounded-2xl"
            />
          </div>

          <div className="mt-3 flex flex-wrap gap-2">
            <Badge tone={classificationToneValue as any}>
              {classification.replace("_", " ")}
            </Badge>
            <Badge>{stageLabel}</Badge>
            <Badge>Score: {r?.score ?? "—"}</Badge>
            <Badge>DSCR: {dscr != null ? dscr.toFixed(2) : "—"}</Badge>
            {tenantSummary?.occupancy_status ? (
              <Badge
                tone={
                  tenantSummary.occupancy_status === "occupied"
                    ? "good"
                    : tenantSummary.occupancy_status === "leased_not_started"
                      ? "warn"
                      : "bad"
                }
              >
                {tenantSummary.occupancy_status}
              </Badge>
            ) : null}
          </div>

          <div className="mt-3">
            <RiskBadges
              county={geo?.county ?? p?.county}
              isRedZone={geo?.is_red_zone}
              crimeScore={geo?.crime_score}
              offenderCount={geo?.offender_count}
              lat={geo?.lat}
              lng={geo?.lng}
            />
          </div>

          <div className="mt-4 rounded-2xl border border-app bg-app-muted p-3">
            <div className="text-[11px] uppercase tracking-wider text-app-4">
              Required next move
            </div>
            <div className="mt-2 text-sm font-semibold text-app-0">
              {primaryActionTitle}
            </div>
            <div className="mt-1 text-xs text-app-4">
              This property stays gated by workflow stage until the next step is
              completed.
            </div>
          </div>
        </Surface>

        <div className="space-y-4">
          <StageProgress
            workflow={workflow}
            currentStage={stage}
            currentStageLabel={stageLabel}
            onAdvance={advanceWorkflow}
            busy={!!busy}
          />

          <Surface
            title="Reality loop"
            actions={
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

                {cash30?.net != null ? (
                  <Badge tone={Number(cash30.net) >= 0 ? "good" : "bad"}>
                    30d net {money(cash30.net)}
                  </Badge>
                ) : null}

                {equity?.estimated_equity != null ? (
                  <Badge
                    tone={
                      Number(equity.estimated_equity) >= 0 ? "good" : "warn"
                    }
                  >
                    equity {money(equity.estimated_equity)}
                  </Badge>
                ) : null}
              </div>
            }
          >
            <div className="grid grid-cols-1 md:grid-cols-5 gap-3">
              <div className="rounded-2xl border border-app bg-app-muted p-4">
                <div className="text-xs text-app-4">Checklist</div>
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

              <div className="rounded-2xl border border-app bg-app-muted p-4">
                <div className="text-xs text-app-4">Cash (30d)</div>
                <div className="mt-2 space-y-1">
                  <Row k="Income" v={money(cash30.income)} />
                  <Row k="Net" v={money(cash30.net)} />
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-muted p-4">
                <div className="text-xs text-app-4">Cash (90d)</div>
                <div className="mt-2 space-y-1">
                  <Row k="Income" v={money(cash90.income)} />
                  <Row k="Net" v={money(cash90.net)} />
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-muted p-4">
                <div className="text-xs text-app-4">Tenant</div>
                <div className="mt-2 space-y-1">
                  <Row
                    k="Occupancy"
                    v={tenantSummary?.occupancy_status ?? "—"}
                  />
                  <Row
                    k="Active lease"
                    v={tenantSummary?.active_lease_count ?? leases.length ?? 0}
                  />
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app-muted p-4">
                <div className="text-xs text-app-4">Equity</div>
                <div className="mt-2 space-y-1">
                  <Row
                    k="Value"
                    v={equity ? money(equity.estimated_value) : "—"}
                  />
                  <Row
                    k="Equity"
                    v={equity ? money(equity.estimated_equity) : "—"}
                  />
                  <Row
                    k="LTV"
                    v={equity?.ltv_pct != null ? pct(equity.ltv_pct, 2) : "—"}
                  />
                </div>
              </div>
            </div>
          </Surface>

          <NextActionsPanel actions={nextActions} />
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <KpiCard
          title="Asking price"
          value={askingPrice != null ? money(askingPrice) : "—"}
          subtitle="Acquisition target"
        />
        <KpiCard
          title="Cash flow est."
          value={cashflowEstimate != null ? money(cashflowEstimate) : "—"}
          subtitle="Current projected view"
          tone="success"
        />
        <KpiCard
          title="DSCR"
          value={dscr != null ? dscr.toFixed(2) : "—"}
          subtitle="Debt coverage"
          tone="accent"
        />
        <KpiCard
          title="Crime"
          value={crimeScore != null ? crimeScore.toFixed(1) : "—"}
          subtitle="Area risk signal"
          tone="warning"
        />
      </div>

      {busy && (
        <Surface tone="accent">
          <div className="text-app-2">{busy}</div>
        </Surface>
      )}

      {err && (
        <Surface tone="danger">
          <div className="text-red-300">
            {noDeal
              ? 'No deal exists for this property yet. Click "+ deal" to create one.'
              : err}
          </div>
        </Surface>
      )}

      <Surface padding="sm">
        <div className="flex gap-2 flex-wrap">
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
                  "px-3 py-2 rounded-xl border text-sm transition",
                  unlocked ? "cursor-pointer" : "cursor-not-allowed opacity-50",
                  active
                    ? "bg-app-muted text-app-0 border-app-strong"
                    : unlocked
                      ? "text-app-3 border-app hover:bg-app-muted hover:border-app-strong"
                      : "text-app-4 border-app bg-app-muted/60",
                ].join(" ")}
              >
                {t}
              </button>
            );
          })}
        </div>
      </Surface>

      {tab === "Deal" && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="lg:col-span-2 grid grid-cols-1 md:grid-cols-2 gap-4">
            <Surface title="Underwriting">
              <div className="space-y-2">
                <Row k="Classification" v={classification.replace("_", " ")} />
                <Row
                  k="Gross rent used"
                  v={
                    r?.gross_rent_used != null ? money(r.gross_rent_used) : "—"
                  }
                />
                <Row
                  k="Mortgage"
                  v={
                    r?.mortgage_payment != null
                      ? money(r.mortgage_payment)
                      : "—"
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
                  v={
                    r?.break_even_rent != null ? money(r.break_even_rent) : "—"
                  }
                />
              </div>
            </Surface>

            <Surface title="Rent Explain">
              <div className="space-y-2">
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
                    rent?.fmr_adjusted != null ? money(rent.fmr_adjusted) : "—"
                  }
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
              </div>
            </Surface>

            <Surface title="Jurisdiction Friction">
              <div className="space-y-2">
                <Row k="Multiplier" v={friction?.multiplier ?? "—"} />
                <div className="mt-2 text-xs text-app-4">Reasons</div>
                <ul className="mt-1 text-sm text-app-2 space-y-1 list-disc pl-5">
                  {(friction?.reasons ?? []).map((x: string, i: number) => (
                    <li key={i}>{x}</li>
                  ))}
                  {(friction?.reasons ?? []).length === 0 && (
                    <li className="text-app-4">—</li>
                  )}
                </ul>
              </div>
            </Surface>

            <Surface title="Deal inputs">
              <div className="space-y-2">
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
              </div>
            </Surface>
          </div>

          <Surface title="Workflow gate">
            <div className="text-sm text-app-3 leading-relaxed">
              This property stays in the acquisition stage until the deal case
              is strong enough to move forward. Once the economics and readiness
              are acceptable, advance it into rehab.
            </div>
          </Surface>
        </div>
      )}

      {tab === "Rehab" && (
        <div className="space-y-4">
          <div className="grid grid-cols-1 xl:grid-cols-[1.2fr_0.8fr] gap-4">
            <PhotoGallery photos={photos} onDelete={handleDeletePhoto} />
            <PhotoUploader
              propertyId={propertyId}
              onUploaded={handleUploadedPhoto}
            />
          </div>

          <RehabFromPhotosCTA
            busy={photoBusy}
            analysis={photoAnalysis}
            onPreview={handlePreviewRehabFromPhotos}
            onGenerate={handleGenerateRehabFromPhotos}
          />

          <Surface
            title="Rehab Tasks"
            actions={
              <button
                onClick={generateRehabFromGaps}
                className="oh-btn oh-btn-secondary cursor-pointer"
                disabled={!!busy}
                title="Creates rehab tasks from checklist gaps + unresolved inspection fails"
              >
                rehab from gaps
              </button>
            }
          >
            {rehab.length === 0 ? (
              <EmptyState compact title="No rehab tasks yet." />
            ) : (
              <div className="space-y-2">
                {rehab.map((t: any) => (
                  <div
                    key={t.id}
                    className="rounded-2xl border border-app bg-app-panel p-4"
                  >
                    <div className="flex items-center justify-between">
                      <div className="font-semibold text-app-0">{t.title}</div>
                      <span className="oh-pill">{t.status}</span>
                    </div>
                    <div className="text-xs text-app-4 mt-1">
                      {t.deadline
                        ? `Due: ${new Date(t.deadline).toLocaleDateString()}`
                        : "No deadline"}{" "}
                      ·{" "}
                      {t.cost_estimate != null
                        ? `Est: ${money(t.cost_estimate)}`
                        : "No estimate"}
                    </div>
                    {t.notes && (
                      <div className="text-sm text-app-3 mt-2">{t.notes}</div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </Surface>
        </div>
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

          <InspectionReadiness
            readiness={inspectionReadiness}
            brief={complianceBrief}
            status={complianceStatus}
            summary={complianceRunSummary}
            onRunAutomation={runComplianceAutomation}
            busy={complianceAutomationBusy}
          />

          <Surface
            title="Compliance / Checklist"
            actions={
              <div className="flex flex-wrap items-center gap-2">
                {complianceStatus ? (
                  <Badge tone={complianceStatus?.passed ? "good" : "warn"}>
                    status: {complianceStatus?.passed ? "ready" : "not ready"}
                  </Badge>
                ) : null}
                {complianceRunSummary ? (
                  <Badge
                    tone={
                      (complianceRunSummary?.failed ?? 0) > 0
                        ? "bad"
                        : "neutral"
                    }
                  >
                    fails: {complianceRunSummary?.failed ?? 0}
                  </Badge>
                ) : null}
                {complianceRunSummary ? (
                  <Badge>
                    score: {complianceRunSummary?.score_pct ?? "—"}%
                  </Badge>
                ) : null}
              </div>
            }
          >
            <div className="flex items-center justify-between gap-3 flex-wrap">
              <div className="text-sm text-app-3">
                Compliance must be genuinely complete before tenant placement
                unlocks.
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() =>
                    refreshChecklist().catch((e) =>
                      setErr(String(e?.message || e)),
                    )
                  }
                  className="oh-btn oh-btn-secondary cursor-pointer"
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
                  className="oh-btn oh-btn-secondary cursor-pointer"
                  disabled={!!busy}
                >
                  rehab from gaps
                </button>
              </div>
            </div>

            <div className="mt-4 space-y-2">
              {checklistItems.length === 0 ? (
                <EmptyState
                  compact
                  title="No checklist found yet."
                  description='Click "generate" to create one.'
                />
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

                        const [
                          opsOut,
                          workflowOut,
                          complianceStatusOut,
                          complianceRunSummaryOut,
                          inspectionReadinessOut,
                        ] = await Promise.all([
                          api
                            .opsPropertySummary(propertyId, 90)
                            .catch(() => null),
                          api.opsPropertyWorkflow(propertyId).catch(() => null),
                          api.complianceStatus(propertyId).catch(() => null),
                          api
                            .complianceRunSummary(propertyId)
                            .catch(() => null),
                          api
                            .complianceInspectionReadiness(propertyId)
                            .catch(() => null),
                        ]);
                        setOps(opsOut);
                        setWorkflow(workflowOut ?? opsOut?.workflow ?? null);
                        setComplianceStatus(complianceStatusOut);
                        setComplianceRunSummary(complianceRunSummaryOut);
                        setInspectionReadiness(inspectionReadinessOut);
                      } finally {
                        setCheckBusyCode(null);
                      }
                    }}
                  />
                ))
              )}
            </div>
          </Surface>
        </div>
      )}

      {tab === "Tenant" && (
        <div className="space-y-4">
          <TenantPipeline
            tenants={tenants}
            leases={leases}
            opsTenant={tenantSummary}
          />

          <Surface title="Tenant workflow gate">
            <div className="text-sm text-app-3 leading-relaxed">
              Tenant placement only comes after compliance readiness. Use this
              stage to move from ready unit to approved occupant.
            </div>
          </Surface>
        </div>
      )}

      {tab === "Lease" && (
        <div className="space-y-4">
          <Surface title="Lease Ledger">
            {leases.length === 0 ? (
              <EmptyState compact title="No leases yet." />
            ) : (
              <div className="space-y-2">
                {leases.map((l: any) => (
                  <div
                    key={l.id}
                    className="rounded-2xl border border-app bg-app-panel p-4"
                  >
                    <div className="flex items-center justify-between gap-3 flex-wrap">
                      <div className="font-semibold text-app-0">
                        Tenant #{l.tenant_id}
                      </div>
                      <div className="text-sm text-app-1 font-semibold">
                        {money(l.total_rent)}
                      </div>
                    </div>
                    <div className="text-xs text-app-4 mt-1">
                      Start: {new Date(l.start_date).toLocaleDateString()}
                      {l.end_date
                        ? ` · End: ${new Date(l.end_date).toLocaleDateString()}`
                        : ""}
                      {l.hap_contract_status
                        ? ` · HAP: ${l.hap_contract_status}`
                        : ""}
                    </div>
                    {l.notes && (
                      <div className="text-sm text-app-3 mt-2">{l.notes}</div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </Surface>
        </div>
      )}

      {tab === "Cash / Equity" && (
        <div className="space-y-4">
          <Surface title="Cash Snapshot">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
              <KpiCard
                title="30d Income"
                value={money(cash30.income)}
                subtitle="Incoming"
              />
              <KpiCard
                title="30d Expense"
                value={money(cash30.expense)}
                subtitle="Outgoing"
              />
              <KpiCard
                title="30d Capex"
                value={money(cash30.capex)}
                subtitle="Projects"
              />
              <KpiCard
                title="30d Net"
                value={money(cash30.net)}
                subtitle="Net result"
                tone="accent"
              />
            </div>
          </Surface>

          <Surface title="Transactions">
            {txns.length === 0 ? (
              <EmptyState compact title="No transactions yet." />
            ) : (
              <div className="space-y-2">
                {txns.map((t: any) => (
                  <div
                    key={t.id}
                    className="rounded-2xl border border-app bg-app-panel p-4"
                  >
                    <div className="flex items-center justify-between gap-3 flex-wrap">
                      <div className="text-app-0 font-semibold">
                        {t.txn_type || t.type || "txn"}
                      </div>
                      <div className="text-app-1 font-semibold">
                        {money(t.amount)}
                      </div>
                    </div>
                    <div className="text-xs text-app-4 mt-1">
                      {t.txn_date
                        ? new Date(t.txn_date).toLocaleDateString()
                        : "—"}
                      {t.memo ? ` · ${t.memo}` : ""}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Surface>

          <Surface title="Equity Snapshot">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
              <KpiCard
                title="Estimated Value"
                value={equity ? money(equity.estimated_value) : "—"}
                subtitle="Market estimate"
              />
              <KpiCard
                title="Loan Balance"
                value={equity ? money(equity.loan_balance) : "—"}
                subtitle="Debt remaining"
              />
              <KpiCard
                title="Estimated Equity"
                value={equity ? money(equity.estimated_equity) : "—"}
                subtitle="Paper spread"
                tone="success"
              />
              <KpiCard
                title="LTV"
                value={equity?.ltv_pct != null ? pct(equity.ltv_pct, 2) : "—"}
                subtitle="Loan-to-value"
              />
            </div>
          </Surface>

          <Surface title="Valuations">
            {vals.length === 0 ? (
              <EmptyState compact title="No valuations yet." />
            ) : (
              <div className="space-y-2">
                {vals.map((v2: any) => (
                  <div
                    key={v2.id}
                    className="rounded-2xl border border-app bg-app-panel p-4"
                  >
                    <div className="flex items-center justify-between gap-3 flex-wrap">
                      <div className="text-app-0 font-semibold">
                        {v2.as_of
                          ? new Date(v2.as_of).toLocaleDateString()
                          : "—"}
                      </div>
                      <div className="text-app-1 font-semibold">
                        {money(v2.estimated_value)}
                      </div>
                    </div>
                    <div className="text-xs text-app-4 mt-1">
                      Loan:{" "}
                      {v2.loan_balance != null ? money(v2.loan_balance) : "—"}
                      {v2.notes ? ` · ${v2.notes}` : ""}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Surface>
        </div>
      )}
    </PageShell>
  );
}
