import { motion } from "framer-motion";
import { TrendingUp, ShieldCheck, Bot, Sparkles } from "lucide-react";
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

type DecisionRow = {
  id: number;
  deal_id: number;
  decision: "REJECT" | "REVIEW" | "PASS";
  score: number;
  reasons: string[];
  gross_rent_used: number;
  mortgage_payment: number;
  operating_expenses: number;
  noi: number;
  cash_flow: number;
  dscr: number;
  cash_on_cash: number;
};

function toneForDecision(d: DecisionRow["decision"]) {
  if (d === "PASS") return "good";
  if (d === "REVIEW") return "warn";
  return "bad";
}

export default function Dashboard({ rows = [] }: { rows?: DecisionRow[] }) {
  const pass = rows.filter((r) => r.decision === "PASS").length;
  const review = rows.filter((r) => r.decision === "REVIEW").length;
  const reject = rows.filter((r) => r.decision === "REJECT").length;

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
            <StatPill label="PASS" value={`${pass}`} tone="good" />
            <StatPill label="REVIEW" value={`${review}`} tone="warn" />
            <StatPill label="REJECT" value={`${reject}`} tone="bad" />
          </div>
        </div>

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

        {/* “alive” artwork strip (optional but looks sick) */}
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

        {/* top decisions */}
        <GlassCard>
          <div className="flex items-center justify-between">
            <div>
              <div className="text-sm font-semibold tracking-tight">
                Top decisions
              </div>
              <div className="text-xs text-zinc-400">
                PASS/REVIEW/REJECT with reasons (explainability is
                non-negotiable).
              </div>
            </div>
          </div>

          <div className="mt-5 overflow-auto rounded-xl border border-white/10 bg-black/20">
            <table className="min-w-full text-sm">
              <thead className="bg-white/[0.03] text-zinc-300">
                <tr>
                  <th className="text-left px-4 py-3 font-medium">Deal</th>
                  <th className="text-left px-4 py-3 font-medium">Decision</th>
                  <th className="text-left px-4 py-3 font-medium">Score</th>
                  <th className="text-left px-4 py-3 font-medium">DSCR</th>
                  <th className="text-left px-4 py-3 font-medium">CoC</th>
                  <th className="text-left px-4 py-3 font-medium">Reasons</th>
                </tr>
              </thead>

              <tbody className="divide-y divide-white/10">
                {top.length === 0 ? (
                  <tr>
                    <td className="px-4 py-5 text-zinc-400" colSpan={6}>
                      No rows yet. Run ingest/enrich/evaluate, then refresh.
                    </td>
                  </tr>
                ) : (
                  top.map((r) => {
                    const tone = toneForDecision(r.decision);
                    const badge =
                      tone === "good"
                        ? "border-green-400/25 bg-green-400/10 text-green-200"
                        : tone === "warn"
                          ? "border-yellow-300/25 bg-yellow-300/10 text-yellow-100"
                          : "border-red-400/25 bg-red-400/10 text-red-200";

                    return (
                      <tr
                        key={r.id}
                        className="hover:bg-white/[0.02] transition"
                      >
                        <td className="px-4 py-4 text-zinc-200">
                          #{r.deal_id}
                        </td>
                        <td className="px-4 py-4">
                          <span
                            className={`inline-flex rounded-full border px-3 py-1 text-xs ${badge}`}
                          >
                            {r.decision}
                          </span>
                        </td>
                        <td className="px-4 py-4 text-zinc-200">
                          {r.score?.toFixed?.(1) ?? r.score}
                        </td>
                        <td className="px-4 py-4 text-zinc-200">
                          {r.dscr?.toFixed?.(2) ?? r.dscr}
                        </td>
                        <td className="px-4 py-4 text-zinc-200">
                          {r.cash_on_cash?.toFixed?.(3) ?? r.cash_on_cash}
                        </td>
                        <td className="px-4 py-4 text-zinc-400">
                          {(r.reasons || []).slice(0, 3).join(" • ")}
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>
        </GlassCard>
      </div>
    </div>
  );
}
