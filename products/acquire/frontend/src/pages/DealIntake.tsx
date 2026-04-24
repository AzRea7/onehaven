import React from "react";
import { Link } from "react-router-dom";
import {
  MapPinned,
  LocateFixed,
  AlertTriangle,
  ArrowUpRight,
} from "lucide-react";

import { api } from "@/lib/api";
import PageHero from "onehaven_onehaven_platform/frontend/src/shell/PageHero";
import GlassCard from "packages/ui/onehaven_onehaven_platform/frontend/src/components/GlassCard";

function numberOrNull(v: any) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function getLocationSummary(ok: any) {
  const property = ok?.property || {};
  const pipeline = ok?.pipeline || ok?.post_import_pipeline || {};
  const geo = pipeline?.geo || {};

  const normalizedAddress =
    property?.normalized_address ||
    ok?.normalized_address ||
    geo?.normalized_address ||
    null;

  const geocodeSource =
    property?.geocode_source ||
    ok?.geocode_source ||
    geo?.geocode_source ||
    null;

  const geocodeConfidence =
    numberOrNull(property?.geocode_confidence) ??
    numberOrNull(ok?.geocode_confidence) ??
    numberOrNull(geo?.geocode_confidence) ??
    null;

  const lat =
    numberOrNull(property?.lat) ??
    numberOrNull(ok?.lat) ??
    numberOrNull(geo?.lat) ??
    null;

  const lng =
    numberOrNull(property?.lng) ??
    numberOrNull(ok?.lng) ??
    numberOrNull(geo?.lng) ??
    null;

  const county = property?.county || ok?.county || geo?.county || null;

  if (lat == null || lng == null) {
    return {
      label: "Location incomplete",
      pillClass: "oh-pill oh-pill-bad",
      normalizedAddress,
      geocodeSource,
      geocodeConfidence,
      lat,
      lng,
      county,
    };
  }

  if (geocodeConfidence != null && geocodeConfidence < 0.7) {
    return {
      label: "Location approximate",
      pillClass: "oh-pill oh-pill-warn",
      normalizedAddress,
      geocodeSource,
      geocodeConfidence,
      lat,
      lng,
      county,
    };
  }

  return {
    label: "Location verified",
    pillClass: "oh-pill oh-pill-good",
    normalizedAddress,
    geocodeSource,
    geocodeConfidence,
    lat,
    lng,
    county,
  };
}

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
  const location = React.useMemo(() => getLocationSummary(ok), [ok]);

  return (
    <div className="space-y-6">
      <PageHero
        eyebrow="Phase 1"
        title="Deal Intake"
        subtitle="Clean manual intake with immediate property creation and downstream location, risk, and rent workflow support."
      />

      <GlassCard>
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {[
            ["Address", "address", "text"],
            ["City", "city", "text"],
            ["State", "state", "text"],
            ["ZIP", "zip", "text"],
          ].map(([label, key, type]) => (
            <label key={key} className="space-y-1">
              <div className="text-sm text-white/70">{label}</div>
              <input
                className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2 outline-none focus:border-white/30"
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
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2"
              type="number"
              value={form.bedrooms}
              onChange={(e) => set("bedrooms", Number(e.target.value))}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Bathrooms</div>
            <input
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2"
              type="number"
              step="0.5"
              value={form.bathrooms}
              onChange={(e) => set("bathrooms", Number(e.target.value))}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Purchase Price</div>
            <input
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2"
              type="number"
              value={form.purchase_price}
              onChange={(e) => set("purchase_price", Number(e.target.value))}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Estimated Rehab</div>
            <input
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2"
              type="number"
              value={form.est_rehab}
              onChange={(e) => set("est_rehab", Number(e.target.value))}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Square Feet (optional)</div>
            <input
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2"
              type="number"
              value={form.square_feet}
              onChange={(e) => set("square_feet", e.target.value)}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm text-white/70">Year Built (optional)</div>
            <input
              className="w-full rounded-xl border border-white/10 bg-white/5 px-3 py-2"
              type="number"
              value={form.year_built}
              onChange={(e) => set("year_built", e.target.value)}
            />
          </label>
        </div>

        <div className="mt-5 flex flex-wrap items-center gap-3">
          <button
            onClick={submit}
            disabled={busy}
            className="rounded-2xl bg-white px-4 py-2 font-semibold text-black hover:opacity-90 disabled:opacity-50"
          >
            {busy ? "Creating..." : "Create Deal + Property"}
          </button>

          {err && <div className="break-all text-sm text-red-300">{err}</div>}

          {ok && (
            <div className="text-sm text-emerald-300">
              Created deal #{createdDealId ?? "?"} for property #
              {createdPropertyId ?? "?"}
            </div>
          )}
        </div>

        {ok ? (
          <div className="mt-5 rounded-2xl border border-white/10 bg-white/5 px-4 py-4">
            <div className="flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-white/50">
              <LocateFixed className="h-3.5 w-3.5" />
              Location automation
            </div>

            <div className="mt-3 flex flex-wrap gap-2">
              <span className={location.pillClass}>{location.label}</span>

              {location.geocodeSource ? (
                <span className="oh-pill">source {location.geocodeSource}</span>
              ) : null}

              {location.geocodeConfidence != null ? (
                <span className="oh-pill">
                  confidence {location.geocodeConfidence.toFixed(2)}
                </span>
              ) : null}

              {location.county ? (
                <span className="oh-pill">
                  <MapPinned className="h-3.5 w-3.5" />
                  {location.county}
                </span>
              ) : null}
            </div>

            <div className="mt-3 text-sm text-white/80">
              {location.normalizedAddress ||
                "Normalized address not available yet"}
            </div>

            {location.lat != null && location.lng != null ? (
              <div className="mt-2 text-xs text-white/50">
                {location.lat.toFixed(4)}, {location.lng.toFixed(4)}
              </div>
            ) : (
              <div className="mt-2 flex items-center gap-2 text-xs text-amber-300">
                <AlertTriangle className="h-3.5 w-3.5" />
                Coordinates not available yet. Downstream risk and jurisdiction
                workflows may still be incomplete.
              </div>
            )}

            {createdPropertyId ? (
              <div className="mt-4">
                <Link
                  to={`/properties/${createdPropertyId}`}
                  className="inline-flex items-center gap-2 rounded-2xl border border-white/10 px-3 py-2 text-sm text-white/90 hover:bg-white/5"
                >
                  Open property
                  <ArrowUpRight className="h-4 w-4" />
                </Link>
              </div>
            ) : null}
          </div>
        ) : null}
      </GlassCard>
    </div>
  );
}
