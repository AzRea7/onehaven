import React from "react";
import { Link } from "react-router-dom";
import {
  Search,
  RefreshCcw,
  SlidersHorizontal,
  MapPin,
  BedDouble,
  Bath,
  Ruler,
  Wallet,
  Banknote,
  Landmark,
  ShieldAlert,
  Sparkles,
  ArrowUpRight,
  ImageOff,
  Building2,
} from "lucide-react";

import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import { api } from "../lib/api";

type Row = any;

type DecisionFilter = "ALL" | "GOOD_DEAL" | "REVIEW" | "REJECT";
type FinancingFilter = "ALL" | "CASH" | "DSCR" | "UNKNOWN";
type CompletenessFilter = "ALL" | "COMPLETE" | "PARTIAL" | "MISSING";
type SortKey =
  | "BEST_CASHFLOW"
  | "LOWEST_PRICE"
  | "HIGHEST_PRICE"
  | "BEST_DSCR"
  | "NEWEST";

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

function numberOrNull(v: any) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeDecision(raw?: string) {
  const x = String(raw || "")
    .trim()
    .toUpperCase();

  if (["PASS", "GOOD_DEAL", "GOOD", "APPROVED", "APPROVE"].includes(x)) {
    return "GOOD_DEAL";
  }
  if (["REJECT", "FAIL", "FAILED", "NO_GO"].includes(x)) {
    return "REJECT";
  }
  return "REVIEW";
}

function decisionPillClass(raw?: string) {
  const d = normalizeDecision(raw);
  if (d === "GOOD_DEAL") return "oh-pill oh-pill-good";
  if (d === "REVIEW") return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-bad";
}

function resolvePropertyId(r: any) {
  const candidates = [
    r?.id,
    r?.property_id,
    r?.property?.id,
    r?.propertyId,
    r?.property?.property_id,
  ];

  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }

  return null;
}

function inferProperty(r: any) {
  return r?.property || r || {};
}

function inferAskingPrice(r: any) {
  return (
    numberOrNull(r?.asking_price) ??
    numberOrNull(r?.deal?.asking_price) ??
    numberOrNull(r?.deal?.price) ??
    numberOrNull(r?.property?.price) ??
    null
  );
}

function inferMarketRent(r: any) {
  return (
    numberOrNull(r?.market_rent_estimate) ??
    numberOrNull(r?.rent_assumption?.market_rent_estimate) ??
    numberOrNull(r?.monthly_rent_estimate) ??
    numberOrNull(r?.estimated_rent) ??
    null
  );
}

function inferMortgage(r: any) {
  return (
    numberOrNull(r?.estimated_mortgage) ??
    numberOrNull(r?.mortgage_estimate) ??
    numberOrNull(r?.monthly_mortgage_payment) ??
    numberOrNull(r?.last_underwriting_result?.monthly_debt_service) ??
    numberOrNull(r?.last_underwriting_result?.mortgage_payment) ??
    null
  );
}

function inferCashflow(r: any) {
  const direct =
    r?.cashflow_estimate ??
    r?.projected_monthly_cashflow ??
    r?.last_underwriting_result?.cash_flow ??
    r?.last_underwriting_result?.cashflow ??
    r?.property_net_cash_window ??
    r?.metrics?.cashflow_estimate;

  const n = numberOrNull(direct);
  if (n != null) return n;

  const rent = inferMarketRent(r) ?? 0;
  const mortgage = inferMortgage(r) ?? 0;
  const taxes = numberOrNull(r?.monthly_tax_estimate) ?? 0;
  const insurance = numberOrNull(r?.monthly_insurance_estimate) ?? 0;

  if (rent > 0) return rent - mortgage - taxes - insurance;
  return null;
}

function inferDscr(r: any) {
  return (
    numberOrNull(r?.dscr) ??
    numberOrNull(r?.last_underwriting_result?.dscr) ??
    null
  );
}

function inferCrime(r: any) {
  return (
    numberOrNull(r?.crime_score) ??
    numberOrNull(r?.property?.crime_score) ??
    null
  );
}

function inferCounty(r: any) {
  return r?.county || r?.property?.county || null;
}

function inferLocationConfidence(r: any) {
  return (
    numberOrNull(r?.geocode_confidence) ??
    numberOrNull(r?.property?.geocode_confidence) ??
    null
  );
}

function inferNormalizedAddress(r: any) {
  return r?.normalized_address || r?.property?.normalized_address || null;
}

function inferLat(r: any) {
  return numberOrNull(r?.lat) ?? numberOrNull(r?.property?.lat) ?? null;
}

function inferLng(r: any) {
  return numberOrNull(r?.lng) ?? numberOrNull(r?.property?.lng) ?? null;
}

function getFinancingType(price?: number | null) {
  if (price == null || !Number.isFinite(Number(price))) return "Unknown";
  if (Number(price) < 75000) return "Cash";
  return "DSCR";
}

function inferCompleteness(r: any): "COMPLETE" | "PARTIAL" | "MISSING" {
  const price = inferAskingPrice(r);
  const rent = inferMarketRent(r);
  const cashflow = inferCashflow(r);
  const dscr = inferDscr(r);
  const normalizedAddress = inferNormalizedAddress(r);
  const lat = inferLat(r);
  const lng = inferLng(r);

  const strong =
    price != null &&
    rent != null &&
    cashflow != null &&
    dscr != null &&
    normalizedAddress &&
    lat != null &&
    lng != null;

  if (strong) return "COMPLETE";

  const partialSignals = [
    price != null,
    rent != null,
    cashflow != null,
    dscr != null,
    Boolean(normalizedAddress),
    lat != null && lng != null,
  ].filter(Boolean).length;

  if (partialSignals >= 3) return "PARTIAL";
  return "MISSING";
}

function completenessPillClass(v: "COMPLETE" | "PARTIAL" | "MISSING") {
  if (v === "COMPLETE") return "oh-pill oh-pill-good";
  if (v === "PARTIAL") return "oh-pill oh-pill-warn";
  return "oh-pill oh-pill-bad";
}

function completenessLabel(v: "COMPLETE" | "PARTIAL" | "MISSING") {
  if (v === "COMPLETE") return "Enriched";
  if (v === "PARTIAL") return "Partial";
  return "Missing data";
}

function inferPhotoUrl(r: any) {
  const candidates = [
    r?.photo_url,
    r?.thumbnail_url,
    r?.cover_photo_url,
    r?.hero_photo_url,
    r?.property?.photo_url,
    r?.property?.thumbnail_url,
    r?.property?.cover_photo_url,
    r?.property?.hero_photo_url,
    Array.isArray(r?.photo_urls) ? r.photo_urls[0] : null,
    Array.isArray(r?.property?.photo_urls) ? r.property.photo_urls[0] : null,
    Array.isArray(r?.photos) ? r.photos[0] : null,
    Array.isArray(r?.property?.photos) ? r.property.photos[0] : null,
  ];

  for (const value of candidates) {
    if (typeof value === "string" && value.trim()) return value;
    if (value && typeof value?.url === "string" && value.url.trim()) {
      return value.url;
    }
  }

  return null;
}

function metricTone(value: number | null | undefined) {
  if (value == null) return "text-app-0";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-red-300";
  return "text-app-0";
}

function safeDate(raw: any) {
  const d = raw ? new Date(raw) : null;
  return d && !Number.isNaN(d.getTime()) ? d : null;
}

function inferUpdatedAt(r: any) {
  return (
    r?.updated_at ||
    r?.last_synced_at ||
    r?.last_enriched_at ||
    r?.created_at ||
    r?.property?.updated_at ||
    null
  );
}

function relativeTime(raw: any) {
  const date = safeDate(raw);
  if (!date) return "Unknown";

  const diffMs = Date.now() - date.getTime();
  const mins = Math.floor(diffMs / 60000);
  const hours = Math.floor(diffMs / 3600000);
  const days = Math.floor(diffMs / 86400000);

  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  if (hours < 24) return `${hours}h ago`;
  return `${days}d ago`;
}

function inferTags(r: any): string[] {
  const tags = new Set<string>();

  const financing = getFinancingType(inferAskingPrice(r));
  const cashflow = inferCashflow(r);
  const dscr = inferDscr(r);
  const completeness = inferCompleteness(r);
  const crime = inferCrime(r);

  if (
    normalizeDecision(r?.normalized_decision || r?.classification) ===
    "GOOD_DEAL"
  ) {
    tags.add("Good deal");
  }
  if (financing === "Cash") tags.add("Cash");
  if (financing === "DSCR") tags.add("DSCR");
  if (cashflow != null && cashflow > 0) tags.add("Cash flow positive");
  if (dscr != null && dscr >= 1.2) tags.add("Strong DSCR");
  if (completeness === "COMPLETE") tags.add("Fully enriched");
  if (crime != null && crime <= 30) tags.add("Lower crime");

  const p = inferProperty(r);
  if (p?.bedrooms != null && Number(p.bedrooms) >= 3) tags.add("3+ beds");
  if (inferCounty(r)) tags.add(String(inferCounty(r)));

  return Array.from(tags).slice(0, 5);
}

function sortRows(rows: Row[], sort: SortKey) {
  const copy = [...rows];

  copy.sort((a, b) => {
    if (sort === "BEST_CASHFLOW") {
      return (inferCashflow(b) ?? -Infinity) - (inferCashflow(a) ?? -Infinity);
    }
    if (sort === "LOWEST_PRICE") {
      return (
        (inferAskingPrice(a) ?? Infinity) - (inferAskingPrice(b) ?? Infinity)
      );
    }
    if (sort === "HIGHEST_PRICE") {
      return (
        (inferAskingPrice(b) ?? -Infinity) - (inferAskingPrice(a) ?? -Infinity)
      );
    }
    if (sort === "BEST_DSCR") {
      return (inferDscr(b) ?? -Infinity) - (inferDscr(a) ?? -Infinity);
    }

    return (
      (safeDate(inferUpdatedAt(b))?.getTime() ?? 0) -
      (safeDate(inferUpdatedAt(a))?.getTime() ?? 0)
    );
  });

  return copy;
}

function Photo({ url, alt }: { url: string | null; alt: string }) {
  const [failed, setFailed] = React.useState(false);

  if (!url || failed) {
    return (
      <div className="flex h-full w-full items-center justify-center bg-app-muted text-app-4">
        <div className="flex flex-col items-center gap-2">
          <ImageOff className="h-6 w-6" />
          <span className="text-xs">No photo</span>
        </div>
      </div>
    );
  }

  return (
    <img
      src={url}
      alt={alt}
      onError={() => setFailed(true)}
      className="h-full w-full object-cover"
      loading="lazy"
    />
  );
}

function MetricCard({
  icon: Icon,
  label,
  value,
  valueClassName,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  value: React.ReactNode;
  valueClassName?: string;
}) {
  return (
    <div className="rounded-2xl border border-app bg-app-panel px-4 py-3">
      <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
        <Icon className="h-3.5 w-3.5" />
        {label}
      </div>
      <div
        className={`mt-2 text-base font-semibold ${valueClassName || "text-app-0"}`}
      >
        {value}
      </div>
    </div>
  );
}

export default function InvestorPane() {
  const [rows, setRows] = React.useState<Row[]>([]);
  const [err, setErr] = React.useState<string | null>(null);
  const [loading, setLoading] = React.useState(true);

  const [q, setQ] = React.useState("");
  const deferredQ = React.useDeferredValue(q);

  const [decision, setDecision] = React.useState<DecisionFilter>("ALL");
  const [financing, setFinancing] = React.useState<FinancingFilter>("ALL");
  const [completeness, setCompleteness] =
    React.useState<CompletenessFilter>("COMPLETE");
  const [sort, setSort] = React.useState<SortKey>("BEST_CASHFLOW");

  const abortRef = React.useRef<AbortController | null>(null);

  const refresh = React.useCallback(async () => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    try {
      setErr(null);
      setLoading(true);

      const out = await api.properties({}, ac.signal);

      const normalizedRows = Array.isArray(out)
        ? out
        : Array.isArray((out as any)?.items)
          ? (out as any).items
          : Array.isArray((out as any)?.rows)
            ? (out as any).rows
            : Array.isArray((out as any)?.properties)
              ? (out as any).properties
              : [];

      setRows(normalizedRows);
    } catch (e: any) {
      if (String(e?.name) === "AbortError") return;
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    refresh();
    return () => abortRef.current?.abort();
  }, [refresh]);

  const filtered = React.useMemo(() => {
    const needle = deferredQ.trim().toLowerCase();

    const output = (rows || []).filter((r) => {
      const resolvedId = resolvePropertyId(r);
      if (!resolvedId) return false;

      const p = inferProperty(r);
      const d = normalizeDecision(
        r?.normalized_decision ||
          r?.classification ||
          r?.latest_decision ||
          r?.raw_decision ||
          r?.last_underwriting_result?.decision,
      );

      const financingType = getFinancingType(inferAskingPrice(r));
      const completenessValue = inferCompleteness(r);

      const hay = [
        p?.address,
        p?.city,
        p?.state,
        p?.zip,
        inferCounty(r),
        inferNormalizedAddress(r),
        ...inferTags(r),
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

      if (needle && !hay.includes(needle)) return false;
      if (decision !== "ALL" && d !== decision) return false;

      if (financing === "CASH" && financingType !== "Cash") return false;
      if (financing === "DSCR" && financingType !== "DSCR") return false;
      if (financing === "UNKNOWN" && financingType !== "Unknown") return false;

      if (completeness !== "ALL" && completenessValue !== completeness) {
        return false;
      }

      return true;
    });

    return sortRows(output, sort);
  }, [rows, deferredQ, decision, financing, completeness, sort]);

  const counts = React.useMemo(() => {
    const output = {
      total: 0,
      complete: 0,
      good: 0,
      review: 0,
      reject: 0,
    };

    for (const r of rows || []) {
      if (!resolvePropertyId(r)) continue;
      output.total += 1;

      if (inferCompleteness(r) === "COMPLETE") output.complete += 1;

      const d = normalizeDecision(
        r?.normalized_decision ||
          r?.classification ||
          r?.latest_decision ||
          r?.raw_decision ||
          r?.last_underwriting_result?.decision,
      );

      if (d === "GOOD_DEAL") output.good += 1;
      else if (d === "REVIEW") output.review += 1;
      else output.reject += 1;
    }

    return output;
  }, [rows]);

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Investment inventory"
          title="Investor marketplace"
          subtitle="Search fully ingested properties, compare the investment numbers fast, and open a property only when it is worth a deeper review."
          actions={
            <>
              <button onClick={refresh} className="oh-btn oh-btn-secondary">
                <RefreshCcw className="h-4 w-4" />
                Refresh inventory
              </button>

              <Link to="/imports" className="oh-btn oh-btn-primary">
                <Sparkles className="h-4 w-4" />
                Sync inventory
              </Link>

              <span className="oh-pill oh-pill-good">
                enriched {counts.complete}
              </span>
              <span className="oh-pill oh-pill-accent">
                total {counts.total}
              </span>
            </>
          }
        />

        {err ? (
          <Surface tone="danger">
            <div className="text-sm text-red-300 break-all">{err}</div>
          </Surface>
        ) : null}

        <Surface
          title="Browse opportunities"
          subtitle={`${filtered.length} visible ${
            filtered.length === 1 ? "property" : "properties"
          }`}
        >
          <div className="mb-5 rounded-3xl border border-app bg-app-panel px-4 py-4">
            <div className="grid gap-3 xl:grid-cols-[1.3fr_0.75fr_0.75fr_0.8fr_0.8fr_auto]">
              <label className="block">
                <span className="oh-field-label">Search</span>
                <div className="relative">
                  <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-app-4" />
                  <input
                    value={q}
                    onChange={(e) => setQ(e.target.value)}
                    placeholder="Address, city, zip, county, tag"
                    className="oh-input pl-10"
                  />
                </div>
              </label>

              <label className="block">
                <span className="oh-field-label">Decision</span>
                <select
                  value={decision}
                  onChange={(e) =>
                    setDecision(e.target.value as DecisionFilter)
                  }
                  className="oh-input"
                >
                  <option value="ALL">All</option>
                  <option value="GOOD_DEAL">Good deal</option>
                  <option value="REVIEW">Review</option>
                  <option value="REJECT">Reject</option>
                </select>
              </label>

              <label className="block">
                <span className="oh-field-label">Financing</span>
                <select
                  value={financing}
                  onChange={(e) =>
                    setFinancing(e.target.value as FinancingFilter)
                  }
                  className="oh-input"
                >
                  <option value="ALL">All</option>
                  <option value="CASH">Cash</option>
                  <option value="DSCR">DSCR</option>
                  <option value="UNKNOWN">Unknown</option>
                </select>
              </label>

              <label className="block">
                <span className="oh-field-label">Data quality</span>
                <select
                  value={completeness}
                  onChange={(e) =>
                    setCompleteness(e.target.value as CompletenessFilter)
                  }
                  className="oh-input"
                >
                  <option value="ALL">All</option>
                  <option value="COMPLETE">Enriched only</option>
                  <option value="PARTIAL">Partial only</option>
                  <option value="MISSING">Missing only</option>
                </select>
              </label>

              <label className="block">
                <span className="oh-field-label">Sort</span>
                <select
                  value={sort}
                  onChange={(e) => setSort(e.target.value as SortKey)}
                  className="oh-input"
                >
                  <option value="BEST_CASHFLOW">Best cash flow</option>
                  <option value="BEST_DSCR">Best DSCR</option>
                  <option value="LOWEST_PRICE">Lowest price</option>
                  <option value="HIGHEST_PRICE">Highest price</option>
                  <option value="NEWEST">Recently updated</option>
                </select>
              </label>

              <div className="flex items-end">
                <button
                  onClick={refresh}
                  className="oh-btn oh-btn-secondary w-full lg:w-auto"
                >
                  <SlidersHorizontal className="h-4 w-4" />
                  Apply
                </button>
              </div>
            </div>
          </div>

          <div className="mb-5 grid gap-3 md:grid-cols-4">
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Good deal
              </div>
              <div className="mt-2 text-2xl font-semibold text-app-0">
                {counts.good}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Review
              </div>
              <div className="mt-2 text-2xl font-semibold text-app-0">
                {counts.review}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Rejected
              </div>
              <div className="mt-2 text-2xl font-semibold text-app-0">
                {counts.reject}
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Enriched
              </div>
              <div className="mt-2 text-2xl font-semibold text-app-0">
                {counts.complete}
              </div>
            </div>
          </div>

          {loading ? (
            <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-3">
              {Array.from({ length: 9 }).map((_, i) => (
                <div key={i} className="oh-skeleton h-[460px] rounded-3xl" />
              ))}
            </div>
          ) : !filtered.length ? (
            <EmptyState
              icon={Building2}
              title="No properties matched"
              description="Try a broader search, switch the data-quality filter, or run a sync to pull in more enriched inventory."
            />
          ) : (
            <div className="grid gap-4 md:grid-cols-2 2xl:grid-cols-3">
              {filtered.map((r) => {
                const resolvedId = resolvePropertyId(r);
                if (!resolvedId) return null;

                const p = inferProperty(r);
                const address = p?.address || `Property #${resolvedId}`;
                const city = p?.city || "—";
                const state = p?.state || "";
                const zip = p?.zip || "";
                const beds = p?.bedrooms;
                const baths = p?.bathrooms;
                const sqft = p?.sqft ?? p?.square_feet ?? null;

                const price = inferAskingPrice(r);
                const rent = inferMarketRent(r);
                const mortgage = inferMortgage(r);
                const cashflow = inferCashflow(r);
                const dscr = inferDscr(r);
                const crime = inferCrime(r);
                const county = inferCounty(r);
                const financingType = getFinancingType(price);
                const decisionTxt = normalizeDecision(
                  r?.normalized_decision ||
                    r?.classification ||
                    r?.latest_decision ||
                    r?.raw_decision ||
                    r?.last_underwriting_result?.decision,
                );
                const completenessValue = inferCompleteness(r);
                const photoUrl = inferPhotoUrl(r);
                const locationConfidence = inferLocationConfidence(r);
                const tags = inferTags(r);

                return (
                  <Link
                    key={resolvedId}
                    to={`/properties/${resolvedId}`}
                    className="group block overflow-hidden rounded-3xl border border-app bg-app-panel shadow-soft transition hover:-translate-y-[1px] hover:border-app-strong hover:shadow-soft-lg"
                  >
                    <div className="relative h-52 overflow-hidden border-b border-app bg-app-muted">
                      <Photo url={photoUrl} alt={address} />
                      <div className="absolute left-3 top-3 flex flex-wrap gap-2">
                        <span className={decisionPillClass(decisionTxt)}>
                          {decisionTxt.replace("_", " ")}
                        </span>
                        <span
                          className={completenessPillClass(completenessValue)}
                        >
                          {completenessLabel(completenessValue)}
                        </span>
                      </div>
                      <div className="absolute right-3 top-3 rounded-full border border-black/10 bg-black/40 p-2 text-white backdrop-blur-sm group-hover:bg-black/55">
                        <ArrowUpRight className="h-4 w-4" />
                      </div>
                    </div>

                    <div className="space-y-4 px-5 py-5">
                      <div>
                        <div className="line-clamp-1 text-lg font-semibold text-app-0">
                          {address}
                        </div>
                        <div className="mt-1 flex items-center gap-1 text-sm text-app-3">
                          <MapPin className="h-3.5 w-3.5" />
                          <span className="truncate">
                            {city}
                            {state ? `, ${state}` : ""} {zip}
                            {county ? ` · ${county}` : ""}
                          </span>
                        </div>
                      </div>

                      <div className="flex flex-wrap gap-2">
                        <span className="oh-pill">{financingType}</span>
                        {tags.map((tag) => (
                          <span key={tag} className="oh-pill">
                            {tag}
                          </span>
                        ))}
                      </div>

                      <div className="grid grid-cols-2 gap-3">
                        <MetricCard
                          icon={Banknote}
                          label="Price"
                          value={money(price)}
                        />
                        <MetricCard
                          icon={Wallet}
                          label="Cash flow"
                          value={money(cashflow)}
                          valueClassName={metricTone(cashflow)}
                        />
                        <MetricCard
                          icon={Landmark}
                          label="Mortgage"
                          value={money(mortgage)}
                        />
                        <MetricCard
                          icon={Sparkles}
                          label="DSCR"
                          value={dscr != null ? dscr.toFixed(2) : "—"}
                        />
                      </div>

                      <div className="grid grid-cols-2 gap-3">
                        <MetricCard
                          icon={Building2}
                          label="Est. rent"
                          value={money(rent)}
                        />
                        <MetricCard
                          icon={ShieldAlert}
                          label="Crime"
                          value={crime != null ? crime.toFixed(1) : "—"}
                        />
                      </div>

                      <div className="grid grid-cols-3 gap-3 rounded-2xl border border-app bg-app-muted px-4 py-3">
                        <div>
                          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                            <BedDouble className="h-3.5 w-3.5" />
                            Beds
                          </div>
                          <div className="mt-2 text-sm font-semibold text-app-0">
                            {beds ?? "—"}
                          </div>
                        </div>

                        <div>
                          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                            <Bath className="h-3.5 w-3.5" />
                            Baths
                          </div>
                          <div className="mt-2 text-sm font-semibold text-app-0">
                            {baths != null ? Number(baths).toFixed(1) : "—"}
                          </div>
                        </div>

                        <div>
                          <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                            <Ruler className="h-3.5 w-3.5" />
                            Sqft
                          </div>
                          <div className="mt-2 text-sm font-semibold text-app-0">
                            {sqft != null ? Number(sqft).toLocaleString() : "—"}
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center justify-between border-t border-app pt-3 text-xs text-app-4">
                        <div>updated {relativeTime(inferUpdatedAt(r))}</div>
                        <div>
                          confidence{" "}
                          {locationConfidence != null
                            ? locationConfidence.toFixed(2)
                            : "—"}
                        </div>
                      </div>
                    </div>
                  </Link>
                );
              })}
            </div>
          )}
        </Surface>
      </div>
    </PageShell>
  );
}
