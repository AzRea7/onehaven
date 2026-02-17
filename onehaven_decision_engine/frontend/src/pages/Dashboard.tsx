// frontend/src/pages/Dashboard.tsx
import React from "react";
import { TrendingUp, ShieldCheck, Bot, Sparkles } from "lucide-react";
import { Link } from "react-router-dom";
import AnimatedBackdrop from "../components/AnimatedBackdrop";
import GlassCard from "../components/GlassCard";
import StatPill from "../components/StatPill";
import {
  OrbDealEngine,
  Section8Badge,
  AgentClaw,
  BuildStack,
  HoverTilt,
} from "../components/Artwork";
import { api } from "../lib/api";
import PageHero from "../components/PageHero";
import BrickBuilder from "../components/BrickBuilder";

type DashboardRow = {
  property?: {
    id: number;
    address: string;
    city: string;
    state: string;
    zip: string;
    bedrooms?: number;
    bathrooms?: number;
  };
  deal?: { strategy?: string; asking_price?: number };
  last_underwriting_result?: {
    decision?: "REJECT" | "REVIEW" | "PASS";
    score?: number;
    dscr?: number;
    cash_on_cash?: number;
    cash_flow?: number;
    reasons?: string[];
  };
};

function toneForDecision(d?: string) {
  if (d === "PASS") return "good";
  if (d === "REVIEW") return "warn";
  return "bad";
}

function SkeletonRow() {
  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.03] p-3 animate-pulse">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-2">
          <div className="h-4 w-56 bg-white/10 rounded" />
          <div className="h-3 w-40 bg-white/10 rounded" />
        </div>
        <div className="space-y-2 flex flex-col items-end">
          <div className="h-6 w-24 bg-white/10 rounded-full" />
          <div className="h-3 w-28 bg-white/10 rounded" />
        </div>
      </div>
    </div>
  );
}

export default function Dashboard() {
  const [rows, setRows] = React.useState<DashboardRow[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [lastSync, setLastSync] = React.useState<number | null>(null);

  const abortRef = React.useRef<AbortController | null>(null);

  const refresh = React.useCallback(async (background = false) => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);
      if (!background) setLoading(true);

      const data = await api.dashboardProperties({
        limit: 50,
        signal: ac.signal,
      });

      setRows(Array.isArray(data) ? data : []);
      setLastSync(Date.now());
    } catch (e: any) {
      if (String(e?.name) === "AbortError") return;
      setErr(String(e?.message || e));
    } finally {
      if (!background) setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    refresh(false);

    // Less frequent + background refresh avoids “UI pulse”
    const interval = setInterval(() => {
      if (document.visibilityState === "visible") refresh(true);
    }, 45_000);

    return () => {
      clearInterval(interval);
      abortRef.current?.abort();
    };
  }, [refresh]);

  const { pass, review, reject, top } = React.useMemo(() => {
    const decisions = (rows || [])
      .map((r) => r?.last_underwriting_result?.decision)
      .filter(Boolean) as string[];
    const pass = decisions.filter((d) => d === "PASS").length;
    const review = decisions.filter((d) => d === "REVIEW").length;
    const reject = decisions.filter((d) => d === "REJECT").length;

    const top = (rows || [])
      .filter((r) => r?.property?.id != null)
      .slice(0, 10);
    return { pass, review, reject, top };
  }, [rows]);

  return (
    <div className="relative min-h-screen text-white">
      <AnimatedBackdrop />

      <div className="mx-auto max-w-6xl px-6 py-8 space-y-8">
        <PageHero
          eyebrow="OneHaven"
          title="Build the wall. Filter the deals. Enforce the truth."
          subtitle="Your system is a machine: Deal → Underwrite → Rehab → Compliance → Tenant → Cash → Equity. The UI should feel like a cockpit — not a spreadsheet."
          right={
            <div className="absolute inset-0 flex items-center justify-center">
              <div className="h-[220px] w-[220px] md:h-[240px] md:w-[240px] opacity-95">
                <BrickBuilder className="h-full w-full" />
              </div>
            </div>
          }
          actions={
            <>
              <button
                onClick={() => refresh(false)}
                className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/5 hover:bg-white/10"
              >
                sync
              </button>

              <StatPill label="PASS" value={`${pass}`} tone="good" />
              <StatPill label="REVIEW" value={`${review}`} tone="warn" />
              <StatPill label="REJECT" value={`${reject}`} tone="bad" />

              <div className="text-[11px] text-zinc-500 px-2 py-2">
                {lastSync
                  ? `last sync: ${new Date(lastSync).toLocaleTimeString()}`
                  : " "}
              </div>
            </>
          }
        />

        {err && (
          <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
            {err}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
          <GlassCard className="relative">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <div className="text-xs uppercase tracking-widest text-zinc-400">
                  Deal Engine
                </div>
                <div className="text-lg font-semibold tracking-tight">
                  Ruthless filtering
                </div>
                <div className="text-sm text-zinc-400">
                  Auto-reject bad deals so you only touch survivors.
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/30 p-2">
                <TrendingUp className="h-4 w-4 text-zinc-100" />
              </div>
            </div>

            <div className="mt-5 h-[150px] relative overflow-visible">
              <HoverTilt className="absolute -right-10 -top-14 h-[220px] w-[220px] opacity-95">
                <OrbDealEngine className="animate-[floatSoft_6s_ease-in-out_infinite]" />
              </HoverTilt>
            </div>
          </GlassCard>

          <GlassCard className="relative">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <div className="text-xs uppercase tracking-widest text-zinc-400">
                  Compliance
                </div>
                <div className="text-lg font-semibold tracking-tight">
                  Pass HQS first try
                </div>
                <div className="text-sm text-zinc-400">
                  Predict fail points, track fixes, compound accuracy.
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/30 p-2">
                <ShieldCheck className="h-4 w-4 text-zinc-100" />
              </div>
            </div>

            <div className="mt-5 h-[150px] relative overflow-visible">
              <HoverTilt className="absolute -right-10 -top-14 h-[220px] w-[220px] opacity-95">
                <Section8Badge className="animate-[floatSoft_6.4s_ease-in-out_infinite]" />
              </HoverTilt>
            </div>
          </GlassCard>

          <GlassCard className="relative">
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <div className="text-xs uppercase tracking-widest text-zinc-400">
                  Agents + Humans
                </div>
                <div className="text-lg font-semibold tracking-tight">
                  Playbook execution
                </div>
                <div className="text-sm text-zinc-400">
                  Agents assist; humans do the real-world moves.
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/30 p-2">
                <Bot className="h-4 w-4 text-zinc-100" />
              </div>
            </div>

            <div className="mt-5 h-[150px] relative overflow-visible">
              <HoverTilt className="absolute -right-10 -top-14 h-[220px] w-[220px] opacity-95">
                <AgentClaw className="animate-[floatSoft_6.2s_ease-in-out_infinite]" />
              </HoverTilt>
            </div>
          </GlassCard>
        </div>

        <GlassCard hover={false} className="relative overflow-visible">
          <div className="flex items-center justify-between gap-6 flex-wrap">
            <div className="space-y-2">
              <div className="text-xs uppercase tracking-widest text-zinc-400">
                Build Pipeline
              </div>
              <div className="text-lg font-semibold tracking-tight flex items-center gap-2">
                Modern, fluid, deterministic <Sparkles className="h-4 w-4" />
              </div>
              <div className="text-sm text-zinc-400 max-w-xl">
                Everything writes to one model. No silent overrides. Audit
                trails everywhere.
              </div>
            </div>

            <div className="relative h-[140px] w-[220px] overflow-visible">
              <HoverTilt className="absolute -right-8 -top-10 h-[210px] w-[260px] opacity-95">
                <BuildStack />
              </HoverTilt>
            </div>
          </div>
        </GlassCard>

        <GlassCard>
          <div className="flex items-center justify-between">
            <div>
              <div className="text-sm font-semibold tracking-tight">
                Recent properties
              </div>
              <div className="text-xs text-zinc-400">
                Click a property → single-pane view (underwriting + rent +
                friction + checklist + ops).
              </div>
            </div>

            <Link
              to="/properties"
              className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/5 hover:bg-white/10"
            >
              View all
            </Link>
          </div>

          <div className="mt-5 space-y-2">
            {loading && (
              <>
                <SkeletonRow />
                <SkeletonRow />
                <SkeletonRow />
              </>
            )}

            {!loading && top.length === 0 && (
              <div className="text-sm text-zinc-400">
                No rows yet. Run ingest/enrich/evaluate, then refresh.
              </div>
            )}

            {!loading &&
              top.map((row) => {
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
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div>
                        <div className="text-sm font-semibold text-zinc-100">
                          {p.address}
                        </div>
                        <div className="text-xs text-zinc-400 mt-1">
                          {p.city}, {p.state} {p.zip}
                          {p.bedrooms != null ? ` · ${p.bedrooms}bd` : ""}
                        </div>
                      </div>

                      <div className="flex flex-col items-end gap-1">
                        <span
                          className={`inline-flex rounded-full border px-3 py-1 text-xs ${badge}`}
                        >
                          {decision}
                          {r?.score != null ? ` · ${r.score}` : ""}
                        </span>
                        <div className="text-[11px] text-zinc-400">
                          {(d?.strategy || "section8").toUpperCase()}
                          {r?.dscr != null
                            ? ` · DSCR ${r.dscr.toFixed(2)}`
                            : ""}
                        </div>
                      </div>
                    </div>
                  </Link>
                );
              })}
          </div>
        </GlassCard>
      </div>
    </div>
  );
}
