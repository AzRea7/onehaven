export type Filters = {
  search?: string;
  state?: string;
  city?: string;
  county?: string;
  stage?: string;
  decision?: string;

  red_zone?: string; // "true" | "false" | undefined
  crime_max?: string;
  crime_min?: string;
  offender_max?: string;
  offender_min?: string;

  deals_only?: string; // "true" | "false"
  include_suppressed?: string; // "true" | "false"
  include_hidden?: string; // "true" | "false"
  hidden_reason?: string; // inactive_listing | low_score | bad_risk | weak_cashflow

  sort?: string;
  page?: string;
};

export const FILTER_KEYS: (keyof Filters)[] = [
  "search",
  "state",
  "city",
  "county",
  "stage",
  "decision",
  "red_zone",
  "crime_max",
  "crime_min",
  "offender_max",
  "offender_min",
  "deals_only",
  "include_suppressed",
  "include_hidden",
  "hidden_reason",
  "sort",
  "page",
];

export function readFilters(params: URLSearchParams): Filters {
  const f: Filters = {};
  for (const k of FILTER_KEYS) {
    const v = params.get(k);
    if (v != null && v !== "") {
      (f as Record<string, string>)[k] = v;
    }
  }
  return f;
}

export function writeFilters(
  params: URLSearchParams,
  next: Filters,
): URLSearchParams {
  const p = new URLSearchParams(params.toString());

  for (const k of FILTER_KEYS) {
    const v = (next as Record<string, string | undefined>)[k];
    if (v == null || v === "") p.delete(k);
    else p.set(k, v);
  }

  return p;
}

export function toQueryString(filters: Filters): string {
  const p = new URLSearchParams();
  for (const k of FILTER_KEYS) {
    const v = (filters as Record<string, string | undefined>)[k];
    if (v != null && v !== "") p.set(k, String(v));
  }
  const s = p.toString();
  return s ? `?${s}` : "";
}

export function filtersToApiParams(filters: Filters): Record<string, any> {
  const redZone = filters.red_zone;

  const out: Record<string, any> = {
    q: filters.search || undefined,
    state: filters.state || undefined,
    city: filters.city || undefined,
    county: filters.county || undefined,
    stage: filters.stage || undefined,
    decision: filters.decision || undefined,
    min_crime_score: filters.crime_min || undefined,
    max_crime_score: filters.crime_max || undefined,
    min_offender_count: filters.offender_min || undefined,
    max_offender_count: filters.offender_max || undefined,
    deals_only: filters.deals_only || undefined,
    include_suppressed: filters.include_suppressed || undefined,
    include_hidden: filters.include_hidden || undefined,
    hidden_reason: filters.hidden_reason || undefined,
    sort: filters.sort || undefined,
    page: filters.page || undefined,
  };

  if (redZone === "true") out.only_red_zone = "true";
  if (redZone === "false") out.exclude_red_zone = "true";

  return out;
}

export function isDealsOnly(filters: Filters): boolean {
  return filters.deals_only === "true";
}

export function includesSuppressed(filters: Filters): boolean {
  return filters.include_suppressed === "true";
}

export function includesHidden(filters: Filters): boolean {
  return filters.include_hidden === "true";
}
