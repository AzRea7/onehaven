import React from "react";
import { motion } from "framer-motion";
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

type DashboardRow = {
  property: {
    id: number;
    address: string;
    city: string;
    state: string;
    zip: string;
    bedrooms?: number;
    bathrooms?: number;
  };
  deal?: {
    strategy?: string;
    asking_price?: number;
  };
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

export default function Dashboard() {
  const [rows, setRows] = React.useState<DashboardRow[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);

  async function refresh() {
    try {
      setErr(null);
      setLoading(true);
      const data = await api.dashboardProperties({ limit: 50 });
      setRows(Array.isArray(data) ? data : []);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    refresh();
  }, []);

  const decisions = rows
    .map((r) => r.last_underwriting_result?.decision)
    .filter(Boolean) as string[];
  const pass = decisions.filter((d) => d === "PASS").length;
  const review = decisions.filter((d) => d === "REVIEW").length;
  const reject = decisions.filter((d) => d === "REJECT").length;

  const top = rows.slice(0, 10);

  return (
    <div className="relative min-h-screen text-white">
      <AnimatedBackdrop />

      <div className="mx-auto max-w-6xl px-6 py-8 space-y-8">
        {/* header */}
        <div className="flex items-end justify-between gap-6">
          <div className="space-y-2">
            <div className="text-2xl md:text-3xl font-semibold tracking-tight">
              OneHaven Dashboard
            </div>
            <div className="text-sm text-zinc-400">
              Deal → Underwrite → Rehab → Compliance → Tenant → Cash → Equity
            </div>
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={refresh}
              className="text-[11px] px-3 py-2 rounded-xl border border-white/10 bg-white/5 hover:bg-white/10"
              title="Refresh"
            >
              sync
            </button>

            <StatPill label="PASS" value={`${pass}`} tone="good" />
            <StatPill label="REVIEW" value={`${review}`} tone="warn" />
            <StatPill label="REJECT" value={`${reject}`} tone="bad" />
          </div>
        </div>

        {err && (
          <div className="oh-panel-solid p-4 border-red-900/60 bg-red-950/30 text-red-200">
            {err}
          </div>
        )}

        {/* hero cards */}
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

        {/* “alive” artwork strip */}
        <GlassCard hover={false} className="relative overflow-visible">
          <div className="flex items-center justify-between gap-6">
            <div className="space-y-2">
              <div className="text-xs uppercase tracking-widest text-zinc-400">
                Build Pipeline
              </div>
              <div className="text-lg font-semibold tracking-tight flex items-center gap-2">
                Modern, fluid, and deterministic{" "}
                <Sparkles className="h-4 w-4" />
              </div>
              <div className="text-sm text-zinc-400 max-w-xl">
                This is your “one source of truth” interface. Every module
                writes to the same model.
              </div>
            </div>

            <div className="relative h-[140px] w-[220px] overflow-visible">
              <HoverTilt className="absolute -right-8 -top-10 h-[210px] w-[260px] opacity-95">
                <BuildStack />
              </HoverTilt>
            </div>
          </div>
        </GlassCard>

        {/* top properties */}
        <GlassCard>
          <div className="flex items-center justify-between">
            <div>
              <div className="text-sm font-semibold tracking-tight">
                Recent properties
              </div>
              <div className="text-xs text-zinc-400">
                Click a property → single-pane view (underwriting + rent +
                friction + checklist).
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
            {loading && <div className="text-sm text-zinc-400">Loading…</div>}

            {!loading && top.length === 0 && (
              <div className="text-sm text-zinc-400">
                No rows yet. Run ingest/enrich/evaluate, then refresh.
              </div>
            )}

            {top.map((row) => {
              const p = row.property;
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
                        {r?.dscr != null ? ` · DSCR ${r.dscr.toFixed(2)}` : ""}
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
