import { motion } from "framer-motion";
import { TrendingUp, ShieldCheck, Bot, Sparkles } from "lucide-react";
import AnimatedBackdrop from "../components/AnimatedBackdrop";
import GlassCard from "../components/GlassCard";
import StatPill from "../components/StatPill";
import { OrbDealEngine, Section8Badge, AgentClaw } from "../components/Artwork";

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

      <div className="relative mx-auto max-w-6xl px-5 py-10">
        {/* Header */}
        <div className="flex items-start justify-between gap-6">
          <div>
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5 }}
              className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white/70"
            >
              <Sparkles className="h-4 w-4" />
              OneHaven Decision Engine
            </motion.div>

            <h1 className="mt-4 text-3xl font-semibold tracking-tight">
              Deals, scored like a machine… but readable like a human.
            </h1>
            <p className="mt-2 text-muted max-w-2xl">
              Section 8 + ops-aware underwriting with transparent reasons, crisp
              metrics, and zero mystery math.
            </p>

            <div className="mt-4 flex flex-wrap gap-2">
              <StatPill label="PASS" value={`${pass}`} tone="good" />
              <StatPill label="REVIEW" value={`${review}`} tone="warn" />
              <StatPill label="REJECT" value={`${reject}`} tone="bad" />
              <StatPill
                label="Loaded"
                value={`${rows.length} deals`}
                tone="neutral"
              />
            </div>
          </div>

          {/* Hero artwork */}
          <div className="hidden md:block w-[280px]">
            <div className="glass rounded-2xl p-4">
              <div className="h-[220px] w-full animate-floaty opacity-95">
                <OrbDealEngine />
              </div>
              <div className="mt-3 text-xs text-white/60">Deal Engine Core</div>
            </div>
          </div>
        </div>

        {/* Modules row */}
        <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-5">
          <GlassCard>
            <div className="flex items-center gap-3">
              <div className="h-10 w-10 rounded-xl bg-white/5 border border-white/10 flex items-center justify-center">
                <TrendingUp className="h-5 w-5 text-white/80" />
              </div>
              <div>
                <div className="font-semibold">Deal Engine</div>
                <div className="text-xs text-muted">
                  NOI, cash flow, CoC, DSCR, break-even
                </div>
              </div>
            </div>
            <div className="mt-4 h-[120px] opacity-90">
              <OrbDealEngine className="animate-floaty" />
            </div>
          </GlassCard>

          <GlassCard>
            <div className="flex items-center gap-3">
              <div className="h-10 w-10 rounded-xl bg-white/5 border border-white/10 flex items-center justify-center">
                <ShieldCheck className="h-5 w-5 text-white/80" />
              </div>
              <div>
                <div className="font-semibold">Section 8 Module</div>
                <div className="text-xs text-muted">
                  Ceilings, caps, standards, jurisdiction rules
                </div>
              </div>
            </div>
            <div className="mt-4 h-[120px] opacity-90">
              <Section8Badge className="animate-floaty" />
            </div>
          </GlassCard>

          <GlassCard>
            <div className="flex items-center gap-3">
              <div className="h-10 w-10 rounded-xl bg-white/5 border border-white/10 flex items-center justify-center">
                <Bot className="h-5 w-5 text-white/80" />
              </div>
              <div>
                <div className="font-semibold">Agents + Ops</div>
                <div className="text-xs text-muted">
                  Runs, messages, inspections, rehab tasks
                </div>
              </div>
            </div>
            <div className="mt-4 h-[120px] opacity-90">
              <AgentClaw className="animate-floaty" />
            </div>
          </GlassCard>
        </div>

        {/* Table */}
        <GlassCard className="mt-8" hover={false}>
          <div className="flex items-end justify-between gap-4">
            <div>
              <div className="text-lg font-semibold">Latest decisions</div>
              <div className="text-xs text-muted">
                Hover rows for motion. Click later for drill-down modals.
              </div>
            </div>
            <div className="flex gap-2">
              <StatPill label="Sorted" value="Newest" />
              <StatPill label="Mode" value="Section 8" />
            </div>
          </div>

          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-white/60">
                <tr className="border-b border-white/10">
                  <th className="text-left py-3 pr-3 font-medium">Deal</th>
                  <th className="text-left py-3 pr-3 font-medium">Decision</th>
                  <th className="text-right py-3 pr-3 font-medium">
                    Cash Flow
                  </th>
                  <th className="text-right py-3 pr-3 font-medium">DSCR</th>
                  <th className="text-right py-3 pr-3 font-medium">CoC</th>
                  <th className="text-left py-3 pr-3 font-medium">
                    Top reason
                  </th>
                </tr>
              </thead>
              <tbody>
                {top.map((r) => (
                  <motion.tr
                    key={r.id}
                    className="border-b border-white/5"
                    whileHover={{ backgroundColor: "rgba(255,255,255,0.04)" }}
                    transition={{ duration: 0.12 }}
                  >
                    <td className="py-3 pr-3">
                      <div className="font-medium">#{r.deal_id}</div>
                      <div className="text-xs text-muted">
                        rent_used ${r.gross_rent_used}
                      </div>
                    </td>
                    <td className="py-3 pr-3">
                      <span
                        className={[
                          "inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold",
                          r.decision === "PASS"
                            ? "border-green-400/20 bg-green-400/10 text-green-200"
                            : r.decision === "REVIEW"
                              ? "border-yellow-300/20 bg-yellow-300/10 text-yellow-100"
                              : "border-red-400/20 bg-red-400/10 text-red-200",
                        ].join(" ")}
                      >
                        {r.decision}
                        <span className="ml-2 text-white/60 font-medium">
                          ({r.score})
                        </span>
                      </span>
                    </td>
                    <td className="py-3 pr-3 text-right font-medium">
                      ${r.cash_flow.toFixed(2)}
                    </td>
                    <td className="py-3 pr-3 text-right text-white/80">
                      {r.dscr.toFixed(3)}
                    </td>
                    <td className="py-3 pr-3 text-right text-white/80">
                      {(r.cash_on_cash * 100).toFixed(1)}%
                    </td>
                    <td className="py-3 pr-3 text-white/70">
                      {r.reasons?.[0] ?? "—"}
                    </td>
                  </motion.tr>
                ))}
              </tbody>
            </table>
          </div>
        </GlassCard>
      </div>
    </div>
  );
}
