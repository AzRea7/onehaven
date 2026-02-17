// frontend/src/pages/DealIntake.tsx
import React from "react";
import { api } from "../lib/api";
import PageHero from "../components/PageHero";
import GlassCard from "../components/GlassCard";

export default function DealIntake() {
  const [form, setForm] = React.useState({
    address: "",
    city: "",
    state: "MI",
    zip: "",
    bedrooms: 3,
    bathrooms: 1.0,
    square_feet: "",
    year_built: "",
    has_garage: false,
    property_type: "single_family",
    asking_price: 120000,
    rehab_estimate: 0,
    strategy: "section8",
    financing_type: "dscr",
    interest_rate: 0.07,
    term_years: 30,
    down_payment_pct: 0.2,
  });

  const [busy, setBusy] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);
  const [ok, setOk] = React.useState<any>(null);

  function set<K extends keyof typeof form>(k: K, v: any) {
    setForm((s) => ({ ...s, [k]: v }));
  }

  async function submit() {
    setBusy(true);
    setErr(null);
    setOk(null);
    try {
      const payload: any = {
        ...form,
        square_feet: form.square_feet ? Number(form.square_feet) : null,
        year_built: form.year_built ? Number(form.year_built) : null,
        bedrooms: Number(form.bedrooms),
        bathrooms: Number(form.bathrooms),
        asking_price: Number(form.asking_price),
        rehab_estimate: Number(form.rehab_estimate),
        interest_rate: Number(form.interest_rate),
        term_years: Number(form.term_years),
        down_payment_pct: Number(form.down_payment_pct),
      };
      const out = await api.intakeDeal(payload);
      setOk(out);
    } catch (e: any) {
      setErr(String(e.message || e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-6">
      <PageHero
        eyebrow="Phase 1"
        title="Deal Intake"
        subtitle="Clean manual intake. Constitution enforced. Rent assumption stub created automatically."
      />

      <GlassCard>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {[
            ["Address", "address", "text"],
            ["City", "city", "text"],
            ["State", "state", "text"],
            ["ZIP", "zip", "text"],
          ].map(([label, key, type]) => (
            <label key={key as string} className="space-y-1">
              <div className="text-sm text-white/70">{label}</div>
              <input
                className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2 outline-none focus:border-white/30"
                type={type as string}
                value={(form as any)[key as any]}
                onChange={(e) => set(key as any, e.target.value)}
              />
            </label>
          ))}

          <label className="space-y-1">
            <div className="text-sm text-white/70">Bedrooms</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              value={form.bedrooms}
              onChange={(e) => set("bedrooms", e.target.value)}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Bathrooms</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              step="0.5"
              value={form.bathrooms}
              onChange={(e) => set("bathrooms", e.target.value)}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Asking Price</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              value={form.asking_price}
              onChange={(e) => set("asking_price", e.target.value)}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Rehab Estimate</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              value={form.rehab_estimate}
              onChange={(e) => set("rehab_estimate", e.target.value)}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Square Feet (optional)</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              value={form.square_feet}
              onChange={(e) => set("square_feet", e.target.value)}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Year Built (optional)</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              value={form.year_built}
              onChange={(e) => set("year_built", e.target.value)}
            />
          </label>
        </div>

        <div className="mt-5 flex items-center gap-3">
          <button
            onClick={submit}
            disabled={busy}
            className="rounded-2xl px-4 py-2 bg-white text-black font-semibold hover:opacity-90 disabled:opacity-50"
          >
            {busy ? "Creating..." : "Create Deal + Property"}
          </button>

          {err && <div className="text-sm text-red-300 break-all">{err}</div>}
          {ok && (
            <div className="text-sm text-emerald-300">
              Created deal #{ok.id} for property #{ok.property_id}
            </div>
          )}
        </div>
      </GlassCard>
    </div>
  );
}
