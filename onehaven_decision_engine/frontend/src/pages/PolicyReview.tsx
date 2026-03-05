// frontend/src/pages/PolicyReview.tsx
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
  value: any;
  confidence: number;
  review_status: string;
  source_id: number | null;
  review_notes?: string | null;
};

type Source = {
  id: number;
  publisher?: string | null;
  title?: string | null;
  url?: string | null;
};

export default function PolicyReview() {
  const [state, setState] = React.useState("MI");
  const [county, setCounty] = React.useState("");
  const [city, setCity] = React.useState("");

  const [assertions, setAssertions] = React.useState<Assertion[]>([]);
  const [sourcesById, setSourcesById] = React.useState<Record<number, Source>>(
    {},
  );

  const [loading, setLoading] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);

  async function refresh() {
    setLoading(true);
    setErr(null);
    try {
      const items = await api.policyAssertions({
        review_status: "extracted",
        state,
        county: county || undefined,
        city: city || undefined,
        include_global: true,
        limit: 200,
      });

      setAssertions(items as Assertion[]);

      const srcs = await api.policySources({
        state,
        county: county || undefined,
        city: city || undefined,
        include_global: true,
        limit: 250,
      });

      const map: Record<number, Source> = {};
      for (const s of srcs as any[]) map[s.id] = s;
      setSourcesById(map);
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
    status: "verified" | "rejected" | "reviewed",
  ) {
    await api.policyReviewAssertion(id, { review_status: status });
    await refresh();
  }

  return (
    <PageShell>
      <PageHero
        title="Policy Review"
        subtitle="Review extracted jurisdiction assertions, verify only what is truly supported, and then build trustworthy jurisdiction profiles from verified rules."
      />

      <div className="mt-4 rounded-xl border border-white/10 bg-white/5 p-4">
        <div className="flex flex-wrap gap-3 items-end">
          <div>
            <label className="block text-xs opacity-70">State</label>
            <input
              className="mt-1 w-20 rounded-md bg-black/40 border border-white/10 px-2 py-1"
              value={state}
              onChange={(e) => setState(e.target.value)}
            />
          </div>

          <div>
            <label className="block text-xs opacity-70">County</label>
            <input
              className="mt-1 w-44 rounded-md bg-black/40 border border-white/10 px-2 py-1"
              placeholder="Wayne"
              value={county}
              onChange={(e) => setCounty(e.target.value)}
            />
          </div>

          <div>
            <label className="block text-xs opacity-70">City</label>
            <input
              className="mt-1 w-44 rounded-md bg-black/40 border border-white/10 px-2 py-1"
              placeholder="Detroit"
              value={city}
              onChange={(e) => setCity(e.target.value)}
            />
          </div>

          <button
            className="rounded-md bg-white/10 hover:bg-white/15 border border-white/10 px-3 py-2 text-sm"
            onClick={refresh}
            style={{ cursor: "pointer" }}
          >
            Refresh
          </button>
        </div>

        {err && <div className="mt-3 text-sm text-red-300">{err}</div>}
        {loading && <div className="mt-3 text-sm opacity-70">Loading…</div>}
      </div>

      <div className="mt-4 space-y-3">
        {assertions.map((a) => {
          const src = a.source_id ? sourcesById[a.source_id] : undefined;

          return (
            <div
              key={a.id}
              className="rounded-xl border border-white/10 bg-white/5 p-4"
            >
              <div className="flex flex-wrap justify-between gap-3">
                <div>
                  <div className="text-sm font-semibold">{a.rule_key}</div>
                  <div className="text-xs opacity-70">
                    scope: {a.state || "-"} / {a.county || "-"} /{" "}
                    {a.city || "-"}{" "}
                    {a.org_id ? `(org ${a.org_id})` : "(global)"}
                  </div>
                  <div className="mt-2 text-xs opacity-80">
                    confidence: {(a.confidence ?? 0).toFixed(2)} • status:{" "}
                    {a.review_status}
                  </div>
                  {a.review_notes ? (
                    <div className="mt-1 text-xs opacity-70">
                      notes: {a.review_notes}
                    </div>
                  ) : null}
                </div>

                <div className="flex gap-2">
                  <button
                    className="rounded-md bg-emerald-500/20 hover:bg-emerald-500/25 border border-emerald-400/30 px-3 py-2 text-sm"
                    onClick={() => mark(a.id, "verified")}
                    style={{ cursor: "pointer" }}
                  >
                    Verify
                  </button>
                  <button
                    className="rounded-md bg-yellow-500/20 hover:bg-yellow-500/25 border border-yellow-400/30 px-3 py-2 text-sm"
                    onClick={() => mark(a.id, "reviewed")}
                    style={{ cursor: "pointer" }}
                  >
                    Reviewed
                  </button>
                  <button
                    className="rounded-md bg-red-500/20 hover:bg-red-500/25 border border-red-400/30 px-3 py-2 text-sm"
                    onClick={() => mark(a.id, "rejected")}
                    style={{ cursor: "pointer" }}
                  >
                    Reject
                  </button>
                </div>
              </div>

              <pre className="mt-3 overflow-auto rounded-lg bg-black/40 border border-white/10 p-3 text-xs">
                {JSON.stringify(a.value, null, 2)}
              </pre>

              <div className="mt-3 text-xs opacity-80">
                source:{" "}
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
            </div>
          );
        })}

        {!loading && assertions.length === 0 && (
          <div className="text-sm opacity-70 mt-6">
            No extracted assertions found for this scope.
          </div>
        )}
      </div>
    </PageShell>
  );
}
