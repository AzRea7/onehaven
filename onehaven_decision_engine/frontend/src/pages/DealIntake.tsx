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
    purchase_price: 120000,
    est_rehab: 0,
    strategy: "section8",
    financing_type: "dscr",
    interest_rate: 0.07,
    term_years: 30,
    down_payment_pct: 0.2,
  });

  const [busy, setBusy] = React.useState(false);
  const [err, setErr] = React.useState<string | null>(null);
  const [ok, setOk] = React.useState<any>(null);

  function set<K extends keyof typeof form>(k: K, v: (typeof form)[K]) {
    setForm((s) => ({ ...s, [k]: v }));
  }

  async function submit() {
    setBusy(true);
    setErr(null);
    setOk(null);

    try {
      const payload = {
        address: form.address.trim(),
        city: form.city.trim(),
        state: form.state.trim() || "MI",
        zip: form.zip.trim(),
        bedrooms: Number(form.bedrooms),
        bathrooms: Number(form.bathrooms),
        square_feet: form.square_feet === "" ? null : Number(form.square_feet),
        year_built: form.year_built === "" ? null : Number(form.year_built),
        has_garage: Boolean(form.has_garage),
        property_type: form.property_type,
        purchase_price: Number(form.purchase_price),
        est_rehab: Number(form.est_rehab),
        strategy: form.strategy,
        financing_type: form.financing_type,
        interest_rate: Number(form.interest_rate),
        term_years: Number(form.term_years),
        down_payment_pct: Number(form.down_payment_pct),
      };

      const out = await api.intakeDeal(payload);
      setOk(out);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setBusy(false);
    }
  }

  const createdDealId = ok?.deal?.id ?? ok?.deal_id ?? null;
  const createdPropertyId = ok?.property?.id ?? ok?.property_id ?? null;

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
            <label key={key} className="space-y-1">
              <div className="text-sm text-white/70">{label}</div>
              <input
                className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2 outline-none focus:border-white/30"
                type={type}
                value={(form as any)[key]}
                onChange={(e) =>
                  set(key as keyof typeof form, e.target.value as any)
                }
              />
            </label>
          ))}

          <label className="space-y-1">
            <div className="text-sm text-white/70">Bedrooms</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              value={form.bedrooms}
              onChange={(e) => set("bedrooms", Number(e.target.value))}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Bathrooms</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              step="0.5"
              value={form.bathrooms}
              onChange={(e) => set("bathrooms", Number(e.target.value))}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Purchase Price</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              value={form.purchase_price}
              onChange={(e) => set("purchase_price", Number(e.target.value))}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Estimated Rehab</div>
            <input
              className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2"
              type="number"
              value={form.est_rehab}
              onChange={(e) => set("est_rehab", Number(e.target.value))}
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

        <div className="mt-5 flex items-center gap-3 flex-wrap">
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
              Created deal #{createdDealId ?? "?"} for property #
              {createdPropertyId ?? "?"}
            </div>
          )}
        </div>
      </GlassCard>
    </div>
  );
}
