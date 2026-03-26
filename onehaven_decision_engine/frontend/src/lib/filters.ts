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

  sort?: string;
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
  "sort",
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
    sort: filters.sort || undefined,
  };

  if (redZone === "true") out.only_red_zone = "true";
  if (redZone === "false") out.exclude_red_zone = "true";

  return out;
}
