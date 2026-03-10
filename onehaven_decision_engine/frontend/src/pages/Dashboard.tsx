import React from "react";
import {
  TrendingUp,
  ShieldCheck,
  Bot,
  Sparkles,
  GitBranch,
} from "lucide-react";
import { Link, useLocation, useNavigate } from "react-router-dom";
import GlassCard from "../components/GlassCard";
import PageHero from "../components/PageHero";
import StatPill from "../components/StatPill";
import {
  OrbDealEngine,
  Section8Badge,
  AgentClaw,
  BuildStack,
  HoverTilt,
} from "../components/Artwork";
import { api } from "../lib/api";
import Golem from "../components/Golem";
import PageShell from "../components/PageShell";
import GlobalFilters from "../components/GlobalFilters";
import { filtersToApiParams, readFilters } from "../lib/filters";

type DashboardRow = {
  property?: {
    id: number;
    address: string;
    city: string;
    state: string;
    zip: string;
    bedrooms?: number;
  };
  deal?: { strategy?: string };
  last_underwriting_result?: {
    decision?: "REJECT" | "REVIEW" | "PASS";
    score?: number;
    dscr?: number;
  };
};

type StageRollups = {
  stage_counts?: Record<string, number>;
  counts?: {
    properties?: number;
    deals?: number;
    rehab_tasks_total?: number;
    rehab_tasks_open?: number;
    transactions_window?: number;
    valuations?: number;
  };
  filters?: Record<string, any>;
};

function toneForDecision(d?: string) {
  if (d === "PASS") return "good";
  if (d === "REVIEW") return "warn";
  return "bad";
}

function SkeletonLine() {
  return <div className="h-3 bg-white/10 rounded w-full" />;
}

export default function Dashboard() {
  const [rows, setRows] = React.useState<DashboardRow[]>([]);
  const [rollups, setRollups] = React.useState<StageRollups | null>(null);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [lastSync, setLastSync] = React.useState<number | null>(null);

  const navigate = useNavigate();
  const location = useLocation();
  const abortRef = React.useRef<AbortController | null>(null);

  const filters = React.useMemo(() => {
    return readFilters(new URLSearchParams(location.search));
  }, [location.search]);

  const apiFilterParams = React.useMemo(() => {
    return filtersToApiParams(filters);
  }, [filters]);

  const refresh = React.useCallback(
    async (background = false) => {
      abortRef.current?.abort();
      const ac = new AbortController();
      abortRef.current = ac;

      try {
        setErr(null);
        if (!background) setLoading(true);

        const [data, roll] = await Promise.all([
          api.dashboardProperties({
            limit: 80,
            signal: ac.signal,
            params: apiFilterParams,
          }),
          api.opsRollups(apiFilterParams, ac.signal).catch(() => null),
        ]);

        setRows(Array.isArray(data) ? data : []);
        setRollups(roll);
        setLastSync(Date.now());
      } catch (e: any) {
        if (String(e?.name) === "AbortError") return;
        setErr(String(e?.message || e));
      } finally {
        if (!background) setLoading(false);
      }
    },
    [apiFilterParams],
  );

  React.useEffect(() => {
    refresh(false);

    const interval = window.setInterval(() => {
      if (document.visibilityState === "visible") {
        refresh(true);
      }
    }, 60_000);

    return () => {
      window.clearInterval(interval);
      abortRef.current?.abort();
    };
  }, [refresh]);

  const { pass, review, reject, survivors, stageCounts } = React.useMemo(() => {
    let pass = 0;
    let review = 0;
    let reject = 0;

    for (const r of rows || []) {
      const d = r?.last_underwriting_result?.decision || "REJECT";
      if (d === "PASS") {
        pass++;
      } else if (d === "REVIEW") {
        review++;
      } else {
        reject++;
      }
    }

    const survivors = (rows || [])
      .filter((r) => r?.property?.id != null)
      .sort((a, b) => {
        const da = a?.last_underwriting_result?.decision || "REJECT";
        const db = b?.last_underwriting_result?.decision || "REJECT";

        const wa = da === "PASS" ? 2 : da === "REVIEW" ? 1 : 0;
        const wb = db === "PASS" ? 2 : db === "REVIEW" ? 1 : 0;

        return wb - wa;
      })
      .slice(0, 8);

    const stageCounts = rollups?.stage_counts || {};

    return { pass, review, reject, survivors, stageCounts };
  }, [rows, rollups]);

  const stageCards = React.useMemo(() => {
    const ordered = [
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

    return ordered
      .filter((s) => stageCounts[s] != null)
      .map((s) => ({
        key: s,
        label: s.replace(/_/g, " "),
        count: stageCounts[s] ?? 0,
      }));
  }, [stageCounts]);

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="OneHaven"
          title="Cockpit view"
          subtitle="Decisions first. Next actions second. Details live inside each property."
          right={
            <div className="absolute inset-0 flex items-center justify-center pointer-events-auto overflow-visible">
              <div className="h-[220px] w-[220px] md:h-[250px] md:w-[250px] translate-y-[-12px] opacity-95">
                <Golem className="h-full w-full" />
              </div>
            </div>
          }
          actions={
            <>
              <button
                onClick={() => refresh(false)}
                className="oh-btn cursor-pointer"
              >
                sync
              </button>
              <StatPill label="PASS" value={`${pass}`} tone="good" />
              <StatPill label="REVIEW" value={`${review}`} tone="warn" />
              <StatPill label="REJECT" value={`${reject}`} tone="bad" />
              <div className="text-[11px] text-white/45 px-2 py-2">
                {lastSync
                  ? `last sync: ${new Date(lastSync).toLocaleTimeString()}`
                  : ""}
              </div>
            </>
          }
        />

        <GlobalFilters className="oh-panel p-4" />

        {err && (
          <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
            {err}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
          <GlassCard>
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <div className="text-xs uppercase tracking-widest text-white/50">
                  Deal Engine
                </div>
                <div className="text-lg font-semibold tracking-tight text-white">
                  Ruthless filtering
                </div>
                <div className="text-sm text-white/55">
                  Auto-reject bad deals so you only touch survivors.
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/30 p-2">
                <TrendingUp className="h-4 w-4 text-white" />
              </div>
            </div>

            <div className="mt-5 h-[140px] relative overflow-visible">
              <HoverTilt className="absolute -right-10 top-14 h-[220px] w-[220px] opacity-95">
                <OrbDealEngine className="animate-[floatSoft_7.5s_ease-in-out_infinite]" />
              </HoverTilt>
            </div>
          </GlassCard>

          <GlassCard>
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <div className="text-xs uppercase tracking-widest text-white/50">
                  Compliance
                </div>
                <div className="text-lg font-semibold tracking-tight text-white">
                  Pass HQS first try
                </div>
                <div className="text-sm text-white/55">
                  Predict fail points, track fixes, compound accuracy.
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/30 p-2">
                <ShieldCheck className="h-4 w-4 text-white" />
              </div>
            </div>

            <div className="mt-5 h-[140px] relative overflow-visible">
              <HoverTilt className="absolute -right-10 top-14 h-[220px] w-[220px] opacity-95">
                <Section8Badge className="animate-[floatSoft_8s_ease-in-out_infinite]" />
              </HoverTilt>
            </div>
          </GlassCard>

          <GlassCard>
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <div className="text-xs uppercase tracking-widest text-white/50">
                  Agents + Humans
                </div>
                <div className="text-lg font-semibold tracking-tight text-white">
                  Playbook execution
                </div>
                <div className="text-sm text-white/55">
                  Agents assist; humans do the real-world moves.
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/30 p-2">
                <Bot className="h-4 w-4 text-white" />
              </div>
            </div>

            <div className="mt-5 h-[140px] relative overflow-visible">
              <HoverTilt className="absolute -right-10 top-14 h-[220px] w-[220px] opacity-95">
                <AgentClaw className="animate-[floatSoft_7.7s_ease-in-out_infinite]" />
              </HoverTilt>
            </div>
          </GlassCard>
        </div>

        <GlassCard hover={false}>
          <div className="flex items-center justify-between gap-6 flex-wrap">
            <div className="space-y-2">
              <div className="text-xs uppercase tracking-widest text-white/50">
                Build Pipeline
              </div>
              <div className="text-lg font-semibold tracking-tight flex items-center gap-2 text-white">
                Deterministic truth <Sparkles className="h-4 w-4" />
              </div>
              <div className="text-sm text-white/55 max-w-xl">
                Every action writes to one model. Audit trails everywhere. No
                silent overrides.
              </div>
            </div>

            <div className="relative h-[140px] w-[220px] overflow-visible">
              <HoverTilt className="absolute -right-8 top-[65px] h-[210px] w-[260px] opacity-95">
                <BuildStack />
              </HoverTilt>
            </div>
          </div>
        </GlassCard>

        <GlassCard>
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <div>
              <div className="text-sm font-semibold tracking-tight text-white flex items-center gap-2">
                <GitBranch className="h-4 w-4" />
                Pipeline drilldown
              </div>
              <div className="text-xs text-white/55">
                See stage distribution and the properties contributing to each
                stage.
              </div>
            </div>

            <Link
              to={`/pipeline${location.search || ""}`}
              className="oh-btn cursor-pointer"
            >
              Open pipeline
            </Link>
          </div>

          <div className="mt-4 grid grid-cols-2 md:grid-cols-5 gap-3">
            {stageCards.length === 0 ? (
              <div className="col-span-full text-sm text-white/55">
                No stage rollups yet.
              </div>
            ) : (
              stageCards.map((s) => (
                <button
                  key={s.key}
                  onClick={() => {
                    const next = new URLSearchParams(location.search);
                    next.set("stage", s.key);
                    navigate(`/pipeline?${next.toString()}`);
                  }}
                  className="rounded-2xl border border-white/10 bg-white/[0.03] hover:bg-white/[0.05] hover:border-white/[0.16] transition p-4 text-left cursor-pointer"
                >
                  <div className="text-[11px] uppercase tracking-widest text-white/45">
                    {s.label}
                  </div>
                  <div className="mt-2 text-2xl font-semibold text-white">
                    {s.count}
                  </div>
                </button>
              ))
            )}
          </div>
        </GlassCard>

        <GlassCard>
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <div>
              <div className="text-sm font-semibold tracking-tight text-white">
                Survivors (quick entry)
              </div>
              <div className="text-xs text-white/55">
                Open a property to run the full loop: underwriting → ops →
                checklist → cash.
              </div>
            </div>

            <Link
              to={`/properties${location.search || ""}`}
              className="oh-btn cursor-pointer"
            >
              View all properties
            </Link>
          </div>

          <div className="mt-5 space-y-2">
            {loading ? (
              <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4 space-y-2">
                <SkeletonLine />
                <SkeletonLine />
                <SkeletonLine />
              </div>
            ) : survivors.length === 0 ? (
              <div className="text-sm text-white/55">
                No rows yet. Run ingest / enrich / evaluate, then refresh.
              </div>
            ) : (
              survivors.map((row) => {
                const p = row.property!;
                const d = row.deal;
                const r = row.last_underwriting_result;
                const decision = r?.decision ?? "REJECT";
                const tone = toneForDecision(decision);
                const badge =
                  tone === "good"
                    ? "border-green-400/25 bg-green-400/10 text-green-200"
                    : tone === "warn"
                      ? "border-yellow-300/25 bg-yellow-300/10 text-yellow-100"
                      : "border-red-400/25 bg-red-400/10 text-red-200";

                return (
                  <Link
                    key={p.id}
                    to={`/properties/${p.id}`}
                    className="block rounded-xl border border-white/10 bg-white/[0.03] hover:bg-white/[0.05] hover:border-white/[0.16] transition p-3"
                    style={{ contain: "layout paint" }}
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div className="min-w-0">
                        <div className="text-sm font-semibold text-white truncate">
                          {p.address}
                        </div>
                        <div className="text-xs text-white/55 mt-1 truncate">
                          {p.city}, {p.state} {p.zip}
                          {p.bedrooms != null ? ` · ${p.bedrooms}bd` : ""}
                          {d?.strategy
                            ? ` · ${(d.strategy as string).toUpperCase()}`
                            : ""}
                        </div>
                      </div>

                      <div className="flex flex-col items-end gap-1">
                        <span
                          className={`inline-flex rounded-full border px-3 py-1 text-xs ${badge}`}
                        >
                          {decision}
                          {r?.score != null ? ` · ${r.score}` : ""}
                        </span>
                        <div className="text-[11px] text-white/45">
                          {r?.dscr != null ? `DSCR ${r.dscr.toFixed(2)}` : ""}
                        </div>
                      </div>
                    </div>
                  </Link>
                );
              })
            )}
          </div>
        </GlassCard>
      </div>
    </PageShell>
  );
}
