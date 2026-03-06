// src/components/PropertyCompliancePanel.tsx
import React from "react";
import { api } from "../lib/api";

type PropertyLike = {
  id?: number;
  state?: string | null;
  county?: string | null;
  city?: string | null;
  strategy?: string | null;
};

type Brief = {
  ok: boolean;
  market: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
  };
  compliance: {
    market_label: string;
    registration_required: boolean;
    inspection_required: boolean;
    certificate_required_before_occupancy: boolean;
    pha_specific_workflow: boolean;
    coverage_confidence: string;
    production_readiness: string;
  };
  explanation: string;
  required_actions: Array<{
    key: string;
    title: string;
    severity: string;
  }>;
  blocking_items: Array<{
    key: string;
    title: string;
    severity: string;
  }>;
  evidence_links: Array<{
    source_id: number;
    publisher?: string | null;
    title?: string | null;
    url: string;
    retrieved_at?: string | null;
  }>;
  coverage: {
    coverage_status: string;
    production_readiness: string;
    confidence_label: string;
    verified_rule_count: number;
    source_count: number;
    fetch_failure_count: number;
    stale_warning_count: number;
  };
  verified_rules: Array<{
    id: number;
    rule_key: string;
    rule_family?: string | null;
    assertion_type?: string | null;
    confidence: number;
    value: any;
  }>;
};

function statPill(label: string, value: string | number) {
  return (
    <div className="rounded-xl border border-white/10 bg-black/30 p-3">
      <div className="text-xs opacity-70">{label}</div>
      <div className="mt-1 font-semibold">{value}</div>
    </div>
  );
}

function boolText(v: boolean) {
  return v ? "Yes" : "No / not verified";
}

export default function PropertyCompliancePanel({
  property,
}: {
  property: PropertyLike;
}) {
  const [brief, setBrief] = React.useState<Brief | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [creatingTasks, setCreatingTasks] = React.useState(false);
  const [taskMsg, setTaskMsg] = React.useState<string | null>(null);
  const [err, setErr] = React.useState<string | null>(null);

  async function load() {
    if (!property?.state || !property?.city) return;
    setLoading(true);
    setErr(null);
    try {
      const res = await api.policyBrief({
        state: property.state || "MI",
        county: property.county || undefined,
        city: property.city || undefined,
        org_scope: false,
      });
      setBrief(res as Brief);
    } catch (e: any) {
      setErr(e?.message || "Failed to load compliance brief");
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [property?.state, property?.county, property?.city]);

  async function createTasksFromRules() {
    if (!property?.id || !brief) return;
    const actions = brief.required_actions || [];
    if (actions.length === 0) {
      setTaskMsg("No compliance actions are mapped yet for this market.");
      return;
    }

    setCreatingTasks(true);
    setTaskMsg(null);

    try {
      for (const a of actions) {
        await api.createRehabTask({
          property_id: property.id,
          title: `Compliance: ${a.title}`,
          description: `Auto-generated from jurisdiction compliance brief for ${brief.compliance.market_label}`,
          status: "todo",
          priority: a.severity === "required" ? "high" : "medium",
          category: "compliance",
        });
      }
      setTaskMsg(
        `Created ${actions.length} compliance task${actions.length === 1 ? "" : "s"}.`,
      );
    } catch (e: any) {
      setTaskMsg(e?.message || "Failed to create compliance tasks.");
    } finally {
      setCreatingTasks(false);
    }
  }

  if (!property?.state || !property?.city) {
    return (
      <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
        <div className="text-sm opacity-70">
          Compliance brief unavailable until property state and city are known.
        </div>
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-lg font-semibold">Property Compliance Brief</div>
          <div className="text-sm opacity-70">
            Auto-resolved jurisdiction rules, actions, blockers, and evidence.
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <button
            className="rounded-md border border-white/10 bg-white/10 px-3 py-2 text-sm hover:bg-white/15"
            onClick={load}
            style={{ cursor: "pointer" }}
            disabled={loading}
          >
            {loading ? "Refreshing…" : "Refresh Brief"}
          </button>

          <button
            className="rounded-md border border-cyan-400/30 bg-cyan-500/20 px-3 py-2 text-sm text-cyan-100 hover:bg-cyan-500/25 disabled:opacity-60"
            onClick={createTasksFromRules}
            style={{ cursor: "pointer" }}
            disabled={creatingTasks || !brief}
          >
            {creatingTasks ? "Creating…" : "Create Tasks From Rules"}
          </button>
        </div>
      </div>

      {err ? <div className="mt-3 text-sm text-red-300">{err}</div> : null}
      {taskMsg ? (
        <div className="mt-3 text-sm text-cyan-200">{taskMsg}</div>
      ) : null}
      {loading ? <div className="mt-3 text-sm opacity-70">Loading…</div> : null}

      {brief ? (
        <>
          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-3 xl:grid-cols-6">
            {statPill("Market", brief.compliance.market_label)}
            {statPill("Readiness", brief.compliance.production_readiness)}
            {statPill("Confidence", brief.compliance.coverage_confidence)}
            {statPill("Verified rules", brief.coverage.verified_rule_count)}
            {statPill("Sources", brief.coverage.source_count)}
            {statPill("Fetch failures", brief.coverage.fetch_failure_count)}
          </div>

          <div className="mt-4 rounded-xl border border-white/10 bg-black/30 p-4">
            <div className="text-sm font-medium">Summary</div>
            <div className="mt-2 text-sm opacity-90">{brief.explanation}</div>
          </div>

          <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2">
            <div className="rounded-xl border border-white/10 bg-black/30 p-4">
              <div className="text-sm font-medium">Compliance Status</div>
              <div className="mt-3 space-y-2 text-sm">
                <div>
                  Registration required:{" "}
                  <span className="font-semibold">
                    {boolText(brief.compliance.registration_required)}
                  </span>
                </div>
                <div>
                  Inspection required:{" "}
                  <span className="font-semibold">
                    {boolText(brief.compliance.inspection_required)}
                  </span>
                </div>
                <div>
                  Certificate before occupancy:{" "}
                  <span className="font-semibold">
                    {boolText(
                      brief.compliance.certificate_required_before_occupancy,
                    )}
                  </span>
                </div>
                <div>
                  PHA-specific workflow:{" "}
                  <span className="font-semibold">
                    {boolText(brief.compliance.pha_specific_workflow)}
                  </span>
                </div>
              </div>
            </div>

            <div className="rounded-xl border border-white/10 bg-black/30 p-4">
              <div className="text-sm font-medium">Blocking Items</div>
              <div className="mt-3 space-y-2">
                {brief.blocking_items.length === 0 ? (
                  <div className="text-sm opacity-70">
                    No active blockers detected from verified rules.
                  </div>
                ) : (
                  brief.blocking_items.map((b) => (
                    <div
                      key={b.key}
                      className="rounded-lg border border-red-400/20 bg-red-500/10 p-3 text-sm"
                    >
                      <div className="font-medium">{b.title}</div>
                      <div className="mt-1 text-xs opacity-70">
                        {b.severity}
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>

          <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2">
            <div className="rounded-xl border border-white/10 bg-black/30 p-4">
              <div className="text-sm font-medium">Required Next Steps</div>
              <div className="mt-3 space-y-2">
                {brief.required_actions.length === 0 ? (
                  <div className="text-sm opacity-70">
                    No verified actions mapped yet.
                  </div>
                ) : (
                  brief.required_actions.map((a) => (
                    <div
                      key={a.key}
                      className="rounded-lg border border-cyan-400/20 bg-cyan-500/10 p-3 text-sm"
                    >
                      <div className="font-medium">{a.title}</div>
                      <div className="mt-1 text-xs opacity-70">
                        {a.severity}
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>

            <div className="rounded-xl border border-white/10 bg-black/30 p-4">
              <div className="text-sm font-medium">Evidence Links</div>
              <div className="mt-3 space-y-2">
                {brief.evidence_links.length === 0 ? (
                  <div className="text-sm opacity-70">
                    No evidence links available yet.
                  </div>
                ) : (
                  brief.evidence_links.map((e) => (
                    <div
                      key={`${e.source_id}-${e.url}`}
                      className="rounded-lg border border-white/10 bg-white/5 p-3 text-sm"
                    >
                      <a
                        href={e.url}
                        target="_blank"
                        rel="noreferrer"
                        className="underline"
                      >
                        {e.publisher || "source"}
                        {e.title ? ` — ${e.title}` : ""}
                      </a>
                      {e.retrieved_at ? (
                        <div className="mt-1 text-xs opacity-70">
                          retrieved: {new Date(e.retrieved_at).toLocaleString()}
                        </div>
                      ) : null}
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>

          <div className="mt-4 rounded-xl border border-white/10 bg-black/30 p-4">
            <div className="text-sm font-medium">Winning Verified Rules</div>
            <div className="mt-3 space-y-2">
              {brief.verified_rules.length === 0 ? (
                <div className="text-sm opacity-70">
                  No verified rules available for this market yet.
                </div>
              ) : (
                brief.verified_rules.map((r) => (
                  <div
                    key={r.id}
                    className="rounded-lg border border-white/10 bg-white/5 p-3"
                  >
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-sm font-medium">{r.rule_key}</span>
                      {r.rule_family ? (
                        <span className="rounded-full border border-white/10 bg-white/10 px-2 py-0.5 text-[11px]">
                          {r.rule_family}
                        </span>
                      ) : null}
                      {r.assertion_type ? (
                        <span className="rounded-full border border-white/10 bg-white/10 px-2 py-0.5 text-[11px]">
                          {r.assertion_type}
                        </span>
                      ) : null}
                      <span className="text-xs opacity-70">
                        confidence {Number(r.confidence ?? 0).toFixed(2)}
                      </span>
                    </div>
                    <pre className="mt-2 overflow-auto rounded-lg border border-white/10 bg-black/40 p-3 text-xs">
                      {JSON.stringify(r.value, null, 2)}
                    </pre>
                  </div>
                ))
              )}
            </div>
          </div>
        </>
      ) : null}
    </div>
  );
}
