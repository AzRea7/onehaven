// onehaven_decision_engine/frontend/src/lib/filters.ts
export type Filters = {
  search?: string;
  county?: string;
  stage?: string;
  decision?: string;
  red_zone?: string; // "true" | "false" | undefined
  crime_max?: string;
  offender_max?: string;
};

export const FILTER_KEYS: (keyof Filters)[] = [
  "search",
  "county",
  "stage",
  "decision",
  "red_zone",
  "crime_max",
  "offender_max",
];

export function readFilters(params: URLSearchParams): Filters {
  const f: Filters = {};
  for (const k of FILTER_KEYS) {
    const v = params.get(k);
    if (v != null && v !== "") (f as any)[k] = v;
  }
  return f;
}

export function writeFilters(
  params: URLSearchParams,
  next: Filters,
): URLSearchParams {
  const p = new URLSearchParams(params.toString());
  for (const k of FILTER_KEYS) {
    const v = (next as any)[k] as string | undefined;
    if (v == null || v === "") p.delete(k);
    else p.set(k, v);
  }
  return p;
}

export function toQueryString(filters: Filters): string {
  const p = new URLSearchParams();
  for (const k of FILTER_KEYS) {
    const v = (filters as any)[k];
    if (v != null && v !== "") p.set(k, String(v));
  }
  const s = p.toString();
  return s ? `?${s}` : "";
}
