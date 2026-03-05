// onehaven_decision_engine/frontend/src/pages/JurisdictionProfiles.tsx
import React from "react";
import PageHero from "../components/PageHero";
import { api } from "../lib/api";

function pretty(v: any) {
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}

export default function JurisdictionProfiles() {
  const [includeGlobal, setIncludeGlobal] = React.useState(true);
  const [state, setState] = React.useState("MI");
  const [rows, setRows] = React.useState<any[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  // Resolve tester
  const [testCity, setTestCity] = React.useState("Detroit");
  const [testCounty, setTestCounty] = React.useState("Wayne");
  const [resolved, setResolved] = React.useState<any | null>(null);

  // Editor
  const [city, setCity] = React.useState<string>("");
  const [county, setCounty] = React.useState<string>("");
  const [friction, setFriction] = React.useState<number>(1.0);
  const [phaName, setPhaName] = React.useState<string>("");
  const [policyJson, setPolicyJson] = React.useState<string>(
    pretty({
      summary: "Org override profile.",
      licensing: {
        typical: "Fill with what you *actually* see in this jurisdiction.",
      },
      inspections: {
        typical: "Fill with your observed re-inspection patterns.",
      },
      notes: ["Keep it operational. You can get more formal later."],
    }),
  );
  const [notes, setNotes] = React.useState<string>("");

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const data = await api.listJurisdictionProfiles(includeGlobal, state);
      setRows(data || []);
    } catch (e: any) {
      setError(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function runResolve() {
    setResolved(null);
    setError(null);
    try {
      const out = await api.resolveJurisdictionProfile({
        city: testCity || null,
        county: testCounty || null,
        state,
      });
      setResolved(out);
    } catch (e: any) {
      setError(String(e?.message || e));
    }
  }

  async function saveProfile() {
    setError(null);
    let policy: any = {};
    try {
      policy = policyJson ? JSON.parse(policyJson) : {};
    } catch {
      setError("policy_json is not valid JSON");
      return;
    }

    try {
      await api.upsertJurisdictionProfile({
        state,
        city: city.trim() || null,
        county: county.trim() || null,
        friction_multiplier: Number(friction || 1.0),
        pha_name: phaName.trim() || null,
        policy,
        notes: notes.trim() || null,
      });
      await refresh();
    } catch (e: any) {
      setError(String(e?.message || e));
    }
  }

  async function deleteOne(r: any) {
    setError(null);
    try {
      await api.deleteJurisdictionProfile({
        state: r.state || state,
        city: r.city || null,
        county: r.county || null,
      });
      await refresh();
    } catch (e: any) {
      setError(String(e?.message || e));
    }
  }

  React.useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [includeGlobal, state]);

  return (
    <div className="px-4 md:px-6 py-6 max-w-[1200px] mx-auto space-y-6">
      <PageHero
        title="Jurisdiction Profiles"
        subtitle="Encode Michigan city/county/PHA reality as a reusable operational model (global defaults + org overrides)."
      />

      {error ? (
        <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-3 text-sm text-red-200">
          {error}
        </div>
      ) : null}

      <div className="flex flex-wrap items-center gap-3">
        <label className="text-sm text-white/70">State</label>
        <input
          value={state}
          onChange={(e) => setState(e.target.value.toUpperCase())}
          className="px-3 py-2 rounded-xl bg-white/[0.04] border border-white/10 text-white text-sm"
          style={{ width: 90 }}
        />

        <label className="text-sm text-white/70 ml-2">
          Include global defaults
        </label>
        <input
          type="checkbox"
          checked={includeGlobal}
          onChange={(e) => setIncludeGlobal(e.target.checked)}
          className="h-4 w-4"
        />

        <button
          onClick={refresh}
          className="ml-auto px-3 py-2 rounded-xl text-sm bg-white/[0.06] border border-white/10 hover:bg-white/[0.10] transition cursor-pointer text-white"
        >
          {loading ? "Loading…" : "Refresh"}
        </button>
      </div>

      {/* Resolve tester */}
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4 space-y-3">
        <div className="text-white font-semibold text-sm">Resolve Tester</div>
        <div className="flex flex-wrap gap-3 items-center">
          <input
            value={testCity}
            onChange={(e) => setTestCity(e.target.value)}
            placeholder="City (optional)"
            className="px-3 py-2 rounded-xl bg-black/30 border border-white/10 text-white text-sm"
          />
          <input
            value={testCounty}
            onChange={(e) => setTestCounty(e.target.value)}
            placeholder="County (optional)"
            className="px-3 py-2 rounded-xl bg-black/30 border border-white/10 text-white text-sm"
          />
          <button
            onClick={runResolve}
            className="px-3 py-2 rounded-xl text-sm bg-indigo-500/20 border border-indigo-400/30 hover:bg-indigo-500/25 transition cursor-pointer text-white"
          >
            Resolve
          </button>
        </div>
        {resolved ? (
          <pre className="text-xs text-white/80 whitespace-pre-wrap bg-black/30 border border-white/10 rounded-xl p-3 overflow-auto">
            {pretty(resolved)}
          </pre>
        ) : null}
      </div>

      {/* Editor */}
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4 space-y-3">
        <div className="text-white font-semibold text-sm">
          Create / Update Org Override
        </div>

        <div className="grid md:grid-cols-2 gap-3">
          <input
            value={city}
            onChange={(e) => setCity(e.target.value)}
            placeholder="City (optional)"
            className="px-3 py-2 rounded-xl bg-black/30 border border-white/10 text-white text-sm"
          />
          <input
            value={county}
            onChange={(e) => setCounty(e.target.value)}
            placeholder="County (optional)"
            className="px-3 py-2 rounded-xl bg-black/30 border border-white/10 text-white text-sm"
          />
          <input
            value={String(friction)}
            onChange={(e) => setFriction(Number(e.target.value))}
            placeholder="Friction (e.g. 1.25)"
            className="px-3 py-2 rounded-xl bg-black/30 border border-white/10 text-white text-sm"
          />
          <input
            value={phaName}
            onChange={(e) => setPhaName(e.target.value)}
            placeholder="PHA name (optional)"
            className="px-3 py-2 rounded-xl bg-black/30 border border-white/10 text-white text-sm"
          />
        </div>

        <textarea
          value={policyJson}
          onChange={(e) => setPolicyJson(e.target.value)}
          rows={10}
          className="w-full px-3 py-2 rounded-xl bg-black/30 border border-white/10 text-white text-xs font-mono"
        />

        <input
          value={notes}
          onChange={(e) => setNotes(e.target.value)}
          placeholder="Notes (optional)"
          className="w-full px-3 py-2 rounded-xl bg-black/30 border border-white/10 text-white text-sm"
        />

        <div className="flex items-center gap-3">
          <button
            onClick={saveProfile}
            className="px-3 py-2 rounded-xl text-sm bg-emerald-500/15 border border-emerald-400/25 hover:bg-emerald-500/20 transition cursor-pointer text-white"
          >
            Save override
          </button>
          <div className="text-xs text-white/60">
            Tip: leave city/county blank to override the state default for your
            org.
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] overflow-hidden">
        <div className="px-4 py-3 border-b border-white/10 flex items-center justify-between">
          <div className="text-white font-semibold text-sm">Profiles</div>
          <div className="text-white/60 text-xs">{rows.length} rows</div>
        </div>

        <div className="overflow-auto">
          <table className="w-full text-sm">
            <thead className="text-white/70">
              <tr className="border-b border-white/10">
                <th className="text-left px-4 py-2">Scope</th>
                <th className="text-left px-4 py-2">State</th>
                <th className="text-left px-4 py-2">County</th>
                <th className="text-left px-4 py-2">City</th>
                <th className="text-left px-4 py-2">Friction</th>
                <th className="text-left px-4 py-2">PHA</th>
                <th className="text-right px-4 py-2">Actions</th>
              </tr>
            </thead>
            <tbody className="text-white/85">
              {rows.map((r) => (
                <tr
                  key={r.id}
                  className="border-b border-white/5 hover:bg-white/[0.03]"
                >
                  <td className="px-4 py-2">{r.scope}</td>
                  <td className="px-4 py-2">{r.state}</td>
                  <td className="px-4 py-2">{r.county || "—"}</td>
                  <td className="px-4 py-2">{r.city || "—"}</td>
                  <td className="px-4 py-2">{r.friction_multiplier}</td>
                  <td className="px-4 py-2">{r.pha_name || "—"}</td>
                  <td className="px-4 py-2 text-right">
                    {r.scope === "org" ? (
                      <button
                        onClick={() => deleteOne(r)}
                        className="px-3 py-1.5 rounded-xl text-xs bg-red-500/10 border border-red-500/20 hover:bg-red-500/15 transition cursor-pointer text-red-100"
                      >
                        Delete
                      </button>
                    ) : (
                      <span className="text-xs text-white/40">Global</span>
                    )}
                  </td>
                </tr>
              ))}
              {!rows.length ? (
                <tr>
                  <td className="px-4 py-6 text-white/60" colSpan={7}>
                    No profiles found.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <div className="text-xs text-white/50">
        Docs:{" "}
        <span className="text-white/70">/meta/docs/michigan_jurisdictions</span>
      </div>
    </div>
  );
}
