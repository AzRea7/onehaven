import React from "react";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import { api } from "../lib/api";

type Assertion = {
  id: number;
  org_id: number | null;
  state: string | null;
  county: string | null;
  city: string | null;
  pha_name: string | null;
  program_type: string | null;
  rule_key: string;
  rule_family?: string | null;
  assertion_type?: string | null;
  value: any;
  confidence: number;
  priority?: number;
  source_rank?: number;
  review_status: string;
  source_id: number | null;
  review_notes?: string | null;
  verification_reason?: string | null;
  reviewed_by_user_id?: number | null;
  stale_after?: string | null;
  superseded_by_assertion_id?: number | null;
};

type Source = {
  id: number;
  publisher?: string | null;
  title?: string | null;
  url?: string | null;
  http_status?: number | null;
  retrieved_at?: string | null;
  notes?: string | null;
};

type Coverage = {
  coverage_status: string;
  production_readiness: string;
  confidence_label: string;
  verified_rule_count: number;
  source_count: number;
  fetch_failure_count: number;
  stale_warning_count: number;
};

function scopeBadge(a: Assertion) {
  if (a.pha_name) {
    return {
      label: "PHA",
      cls: "bg-fuchsia-500/15 border-fuchsia-400/30 text-fuchsia-200",
    };
  }
  if (a.city) {
    return {
      label: "City",
      cls: "bg-cyan-500/15 border-cyan-400/30 text-cyan-200",
    };
  }
  if (a.county) {
    return {
      label: "County",
      cls: "bg-indigo-500/15 border-indigo-400/30 text-indigo-200",
    };
  }
  if (a.state) {
    return {
      label: "State",
      cls: "bg-emerald-500/15 border-emerald-400/30 text-emerald-200",
    };
  }
  return {
    label: "Global",
    cls: "bg-white/10 border-white/10 text-white/80",
  };
}

export default function PolicyReview() {
  const [state, setState] = React.useState("MI");
  const [county, setCounty] = React.useState("");
  const [city, setCity] = React.useState("");
  const [phaName, setPhaName] = React.useState("");
  const [reviewStatus, setReviewStatus] = React.useState("extracted");
  const [ruleKey, setRuleKey] = React.useState("");
  const [ruleFamily, setRuleFamily] = React.useState("");
  const [assertionType, setAssertionType] = React.useState("");

  const [assertions, setAssertions] = React.useState<Assertion[]>([]);
  const [sourcesById, setSourcesById] = React.useState<Record<number, Source>>(
    {},
  );
  const [coverage, setCoverage] = React.useState<Coverage | null>(null);

  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);

  async function refresh() {
    setLoading(true);
    setErr(null);
    try {
      const ares = await api.policyAssertions({
        review_status: reviewStatus || undefined,
        rule_key: ruleKey || undefined,
        rule_family: ruleFamily || undefined,
        assertion_type: assertionType || undefined,
        state,
        county: county || undefined,
        city: city || undefined,
        pha_name: phaName || undefined,
        include_global: true,
        limit: 300,
      });

      setAssertions((ares?.items ?? []) as Assertion[]);

      const sres = await api.policySources({
        state,
        county: county || undefined,
        city: city || undefined,
        pha_name: phaName || undefined,
        include_global: true,
        limit: 300,
      });

      const map: Record<number, Source> = {};
      for (const s of sres?.items ?? []) map[s.id] = s;
      setSourcesById(map);

      const cov = await api.policyCoverage({
        state,
        county: county || undefined,
        city: city || undefined,
        pha_name: phaName || undefined,
        org_scope: false,
      });
      setCoverage(cov as Coverage);
    } catch (e: any) {
      setErr(e?.message || "Failed to load policy review data");
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function mark(
    id: number,
    status:
      | "verified"
      | "rejected"
      | "reviewed"
      | "stale"
      | "needs_recheck"
      | "superseded",
    verificationReason?: string,
  ) {
    await api.policyReviewAssertion(id, {
      review_status: status,
      verification_reason: verificationReason ?? null,
    });
    await refresh();
  }

  async function verifyDuplicates(a: Assertion) {
    const dupes = assertions.filter(
      (x) =>
        x.id !== a.id &&
        x.rule_family === a.rule_family &&
        x.city === a.city &&
        x.county === a.county &&
        x.pha_name === a.pha_name &&
        x.review_status === "extracted",
    );

    for (const d of dupes) {
      await api.policyReviewAssertion(d.id, {
        review_status: "reviewed",
        verification_reason: "duplicate_same_market_family",
        review_notes: `Grouped with assertion ${a.id}`,
      });
    }

    await api.policyReviewAssertion(a.id, {
      review_status: "verified",
      verification_reason: "official_source_review",
      review_notes: "Verified as current winner for this market/rule family",
    });

    await refresh();
  }

  return (
    <PageShell>
      <PageHero
        title="Policy Review"
        subtitle="Review extracted jurisdiction assertions, promote trusted winners, track coverage readiness, and keep the compliance engine honest."
      />

      <div className="mt-4 rounded-2xl border border-white/10 bg-white/5 p-4">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-4 xl:grid-cols-8">
          <div>
            <label className="block text-xs opacity-70">State</label>
            <input
              className="mt-1 w-full rounded-md border border-white/10 bg-black/40 px-2 py-2"
              value={state}
              onChange={(e) => setState(e.target.value)}
            />
          </div>

          <div>
            <label className="block text-xs opacity-70">County</label>
            <input
              className="mt-1 w-full rounded-md border border-white/10 bg-black/40 px-2 py-2"
              placeholder="wayne"
              value={county}
              onChange={(e) => setCounty(e.target.value)}
            />
          </div>

          <div>
            <label className="block text-xs opacity-70">City</label>
            <input
              className="mt-1 w-full rounded-md border border-white/10 bg-black/40 px-2 py-2"
              placeholder="detroit"
              value={city}
              onChange={(e) => setCity(e.target.value)}
            />
          </div>

          <div>
            <label className="block text-xs opacity-70">PHA</label>
            <input
              className="mt-1 w-full rounded-md border border-white/10 bg-black/40 px-2 py-2"
              placeholder="Detroit Housing Commission"
              value={phaName}
              onChange={(e) => setPhaName(e.target.value)}
            />
          </div>

          <div>
            <label className="block text-xs opacity-70">Status</label>
            <select
              className="mt-1 w-full rounded-md border border-white/10 bg-black/40 px-2 py-2"
              value={reviewStatus}
              onChange={(e) => setReviewStatus(e.target.value)}
            >
              <option value="">all</option>
              <option value="extracted">extracted</option>
              <option value="reviewed">reviewed</option>
              <option value="verified">verified</option>
              <option value="rejected">rejected</option>
              <option value="stale">stale</option>
              <option value="needs_recheck">needs_recheck</option>
              <option value="superseded">superseded</option>
            </select>
          </div>

          <div>
            <label className="block text-xs opacity-70">Rule key</label>
            <input
              className="mt-1 w-full rounded-md border border-white/10 bg-black/40 px-2 py-2"
              placeholder="rental_registration_required"
              value={ruleKey}
              onChange={(e) => setRuleKey(e.target.value)}
            />
          </div>

          <div>
            <label className="block text-xs opacity-70">Rule family</label>
            <input
              className="mt-1 w-full rounded-md border border-white/10 bg-black/40 px-2 py-2"
              placeholder="rental_registration"
              value={ruleFamily}
              onChange={(e) => setRuleFamily(e.target.value)}
            />
          </div>

          <div>
            <label className="block text-xs opacity-70">Assertion type</label>
            <select
              className="mt-1 w-full rounded-md border border-white/10 bg-black/40 px-2 py-2"
              value={assertionType}
              onChange={(e) => setAssertionType(e.target.value)}
            >
              <option value="">all</option>
              <option value="document_reference">document_reference</option>
              <option value="anchor">anchor</option>
              <option value="operational">operational</option>
              <option value="superseding_notice">superseding_notice</option>
            </select>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          <button
            className="rounded-md border border-white/10 bg-white/10 px-3 py-2 text-sm hover:bg-white/15"
            onClick={refresh}
            style={{ cursor: "pointer" }}
          >
            Refresh
          </button>

          <button
            className="rounded-md border border-cyan-400/30 bg-cyan-500/15 px-3 py-2 text-sm text-cyan-100 hover:bg-cyan-500/20"
            onClick={async () => {
              await api.policyRefreshCoverage({
                state,
                county: county || undefined,
                city: city || undefined,
                pha_name: phaName || undefined,
                org_scope: false,
              });
              await refresh();
            }}
            style={{ cursor: "pointer" }}
          >
            Refresh Coverage
          </button>
        </div>

        {coverage ? (
          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-5">
            <div className="rounded-xl border border-white/10 bg-black/30 p-3">
              <div className="text-xs opacity-70">Coverage</div>
              <div className="mt-1 font-semibold">
                {coverage.coverage_status}
              </div>
            </div>
            <div className="rounded-xl border border-white/10 bg-black/30 p-3">
              <div className="text-xs opacity-70">Readiness</div>
              <div className="mt-1 font-semibold">
                {coverage.production_readiness}
              </div>
            </div>
            <div className="rounded-xl border border-white/10 bg-black/30 p-3">
              <div className="text-xs opacity-70">Confidence</div>
              <div className="mt-1 font-semibold">
                {coverage.confidence_label}
              </div>
            </div>
            <div className="rounded-xl border border-white/10 bg-black/30 p-3">
              <div className="text-xs opacity-70">Verified rules</div>
              <div className="mt-1 font-semibold">
                {coverage.verified_rule_count}
              </div>
            </div>
            <div className="rounded-xl border border-white/10 bg-black/30 p-3">
              <div className="text-xs opacity-70">Fetch failures</div>
              <div className="mt-1 font-semibold">
                {coverage.fetch_failure_count}
              </div>
            </div>
          </div>
        ) : null}

        {err && <div className="mt-3 text-sm text-red-300">{err}</div>}
        {loading && <div className="mt-3 text-sm opacity-70">Loading…</div>}
      </div>

      <div className="mt-4 space-y-3">
        {assertions.map((a) => {
          const src = a.source_id ? sourcesById[a.source_id] : undefined;
          const badge = scopeBadge(a);

          return (
            <div
              key={a.id}
              className="rounded-2xl border border-white/10 bg-white/5 p-4"
            >
              <div className="flex flex-wrap justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-sm font-semibold">{a.rule_key}</span>
                    {a.rule_family ? (
                      <span className="rounded-full border border-white/10 bg-white/10 px-2 py-0.5 text-[11px]">
                        {a.rule_family}
                      </span>
                    ) : null}
                    {a.assertion_type ? (
                      <span className="rounded-full border border-white/10 bg-white/10 px-2 py-0.5 text-[11px]">
                        {a.assertion_type}
                      </span>
                    ) : null}
                    <span
                      className={`rounded-full border px-2 py-0.5 text-[11px] ${badge.cls}`}
                    >
                      {badge.label}
                    </span>
                  </div>

                  <div className="mt-1 text-xs opacity-70">
                    scope: {a.state || "-"} / {a.county || "-"} /{" "}
                    {a.city || "-"} / {a.pha_name || "-"}{" "}
                    {a.org_id ? `(org ${a.org_id})` : "(global)"}
                  </div>

                  <div className="mt-2 text-xs opacity-80">
                    confidence: {(a.confidence ?? 0).toFixed(2)} • status:{" "}
                    {a.review_status}
                    {a.verification_reason
                      ? ` • reason: ${a.verification_reason}`
                      : ""}
                  </div>

                  {a.review_notes ? (
                    <div className="mt-1 text-xs opacity-70">
                      notes: {a.review_notes}
                    </div>
                  ) : null}

                  {a.stale_after ? (
                    <div className="mt-1 text-xs text-yellow-200">
                      stale after: {new Date(a.stale_after).toLocaleString()}
                    </div>
                  ) : null}
                </div>

                <div className="flex flex-wrap gap-2">
                  <button
                    className="rounded-md border border-emerald-400/30 bg-emerald-500/20 px-3 py-2 text-sm hover:bg-emerald-500/25"
                    onClick={() =>
                      mark(a.id, "verified", "official_source_review")
                    }
                    style={{ cursor: "pointer" }}
                  >
                    Verify
                  </button>
                  <button
                    className="rounded-md border border-cyan-400/30 bg-cyan-500/20 px-3 py-2 text-sm hover:bg-cyan-500/25"
                    onClick={() => verifyDuplicates(a)}
                    style={{ cursor: "pointer" }}
                  >
                    Verify Winner
                  </button>
                  <button
                    className="rounded-md border border-yellow-400/30 bg-yellow-500/20 px-3 py-2 text-sm hover:bg-yellow-500/25"
                    onClick={() => mark(a.id, "reviewed", "human_reviewed")}
                    style={{ cursor: "pointer" }}
                  >
                    Reviewed
                  </button>
                  <button
                    className="rounded-md border border-orange-400/30 bg-orange-500/20 px-3 py-2 text-sm hover:bg-orange-500/25"
                    onClick={() =>
                      mark(a.id, "needs_recheck", "source_changed")
                    }
                    style={{ cursor: "pointer" }}
                  >
                    Needs Recheck
                  </button>
                  <button
                    className="rounded-md border border-red-400/30 bg-red-500/20 px-3 py-2 text-sm hover:bg-red-500/25"
                    onClick={() =>
                      mark(a.id, "rejected", "unsupported_or_wrong")
                    }
                    style={{ cursor: "pointer" }}
                  >
                    Reject
                  </button>
                </div>
              </div>

              <pre className="mt-3 overflow-auto rounded-lg border border-white/10 bg-black/40 p-3 text-xs">
                {JSON.stringify(a.value, null, 2)}
              </pre>

              <div className="mt-3 rounded-xl border border-white/10 bg-black/20 p-3 text-xs">
                <div className="font-medium opacity-90">Source</div>
                <div className="mt-1 opacity-80">
                  {src?.url ? (
                    <a
                      href={src.url}
                      target="_blank"
                      rel="noreferrer"
                      className="underline"
                    >
                      {src.publisher || "source"}
                      {src.title ? ` — ${src.title}` : ""}
                    </a>
                  ) : (
                    <span>{src?.publisher || "none"}</span>
                  )}
                </div>
                {src?.retrieved_at ? (
                  <div className="mt-1 opacity-70">
                    retrieved: {new Date(src.retrieved_at).toLocaleString()}
                  </div>
                ) : null}
                {src?.http_status == null ? (
                  <div className="mt-1 text-yellow-200">
                    fetch warning / incomplete source fetch
                  </div>
                ) : null}
                {src?.notes ? (
                  <div className="mt-1 opacity-70">{src.notes}</div>
                ) : null}
              </div>
            </div>
          );
        })}

        {!loading && assertions.length === 0 && (
          <div className="mt-6 text-sm opacity-70">
            No assertions found for this filter.
          </div>
        )}
      </div>
    </PageShell>
  );
}
