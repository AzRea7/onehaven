// frontend/src/pages/Jurisdictions.tsx
import React from "react";
import { api } from "../lib/api";
import PageHero from "../components/PageHero";
import GlassCard from "../components/GlassCard";

type Rule = any;

export default function Jurisdictions() {
  const [rules, setRules] = React.useState<Rule[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [busy, setBusy] = React.useState(false);

  const [draft, setDraft] = React.useState({
    city: "",
    state: "MI",
    rental_license_required: false,
    inspection_authority: "",
    typical_fail_points: "GFCI missing, handrails, peeling paint",
    registration_fee: "",
    processing_days: "",
    inspection_frequency: "annual",
    tenant_waitlist_depth: "",
    notes: "",
  });

  async function refresh() {
    setErr(null);
    const out = await api.listJurisdictionRules(true);
    setRules(Array.isArray(out) ? out : []);
  }

  React.useEffect(() => {
    refresh().catch((e) => setErr(String(e.message || e)));
  }, []);

  async function seed() {
    setBusy(true);
    setErr(null);
    try {
      await api.seedJurisdictionDefaults();
      await refresh();
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function create() {
    setBusy(true);
    setErr(null);
    try {
      const payload = {
        ...draft,
        inspection_authority: draft.inspection_authority || null,
        typical_fail_points: draft.typical_fail_points
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
        registration_fee: draft.registration_fee
          ? Number(draft.registration_fee)
          : null,
        processing_days: draft.processing_days
          ? Number(draft.processing_days)
          : null,
      };
      await api.createJurisdictionRule(payload);
      setDraft((s) => ({ ...s, city: "" }));
      await refresh();
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  async function del(id: number) {
    setBusy(true);
    setErr(null);
    try {
      await api.deleteJurisdictionRule(id);
      await refresh();
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-6">
      <PageHero
        eyebrow="Phase 2"
        title="Jurisdiction Rules"
        subtitle="Global MI defaults + org overrides. This is where city friction becomes deterministic."
      />

      <GlassCard>
        <div className="flex items-center justify-between gap-3">
          <div className="text-white/80 text-sm">
            Tip: seed defaults once, then override cities you care about.
          </div>
          <button
            onClick={seed}
            disabled={busy}
            className="rounded-2xl px-4 py-2 bg-white/10 border border-white/15 hover:bg-white/15"
          >
            Seed MI Defaults
          </button>
        </div>

        <div className="mt-5 grid grid-cols-1 md:grid-cols-3 gap-3">
          <input
            className="rounded-xl bg-white/5 border border-white/10 px-3 py-2"
            placeholder="City"
            value={draft.city}
            onChange={(e) => setDraft((s) => ({ ...s, city: e.target.value }))}
          />
          <input
            className="rounded-xl bg-white/5 border border-white/10 px-3 py-2"
            placeholder="State"
            value={draft.state}
            onChange={(e) => setDraft((s) => ({ ...s, state: e.target.value }))}
          />
          <label className="flex items-center gap-2 text-sm text-white/70">
            <input
              type="checkbox"
              checked={draft.rental_license_required}
              onChange={(e) =>
                setDraft((s) => ({
                  ...s,
                  rental_license_required: e.target.checked,
                }))
              }
            />
            Rental license required
          </label>

          <input
            className="rounded-xl bg-white/5 border border-white/10 px-3 py-2 md:col-span-2"
            placeholder="Typical fail points (comma-separated)"
            value={draft.typical_fail_points}
            onChange={(e) =>
              setDraft((s) => ({ ...s, typical_fail_points: e.target.value }))
            }
          />

          <button
            onClick={create}
            disabled={busy || !draft.city.trim()}
            className="rounded-2xl px-4 py-2 bg-white text-black font-semibold disabled:opacity-50"
          >
            Add Org Override
          </button>
        </div>

        {err && (
          <div className="mt-3 text-sm text-red-300 break-all">{err}</div>
        )}
      </GlassCard>

      <div className="grid grid-cols-1 gap-3">
        {rules.map((r) => (
          <GlassCard key={r.id}>
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="text-lg font-semibold">
                  {r.city}, {r.state}{" "}
                  <span className="text-xs text-white/50">
                    {r.org_id ? "(Org override)" : "(Global default)"}
                  </span>
                </div>
                <div className="text-sm text-white/70">
                  License: {String(r.rental_license_required)} • Processing
                  days: {r.processing_days ?? "—"} • Frequency:{" "}
                  {r.inspection_frequency ?? "—"}
                </div>
              </div>

              {r.org_id && (
                <button
                  onClick={() => del(r.id)}
                  disabled={busy}
                  className="rounded-xl px-3 py-1 bg-red-500/15 border border-red-500/25 hover:bg-red-500/20 text-red-200"
                >
                  Delete
                </button>
              )}
            </div>

            <div className="mt-3 text-sm text-white/70">
              Fail points:{" "}
              {Array.isArray(r.typical_fail_points_json)
                ? r.typical_fail_points_json.join(", ")
                : r.typical_fail_points_json || r.typical_fail_points || "—"}
            </div>
          </GlassCard>
        ))}
      </div>
    </div>
  );
}
