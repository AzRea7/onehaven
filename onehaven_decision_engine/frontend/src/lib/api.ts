export const API_BASE = (import.meta as any).env?.VITE_API_BASE || "/api";

type AuthContext = {
  orgSlug: string;
  devEmail?: string;
  devRole?: string;
};

export type Principal = {
  org_id: number;
  org_slug: string;
  user_id: number;
  email: string;
  role: string;
  plan_code?: string | null;
};

export type PolicyMarketPayload = {
  state: string;
  county?: string | null;
  city?: string | null;
  focus?: string;
  org_scope?: boolean;
  async_mode?: boolean;
};

export type PolicyReadinessRow = {
  state: string;
  county?: string | null;
  city?: string | null;
  label?: string | null;
  coverage_status?: string | null;
  production_readiness?: string | null;
  confidence_label?: string | null;
  verified_rule_count?: number | null;
  source_count?: number | null;
  fetch_failure_count?: number | null;
  stale_warning_count?: number | null;
};

export type PolicyReviewQueueItem = {
  id: number;
  rule_key: string;
  rule_family?: string | null;
  review_status: string;
  confidence?: number | null;
  priority?: number | null;
  severity?: string | null;
  source_id?: number | null;
};

export type PropertyPhoto = {
  id: number;
  org_id?: number | null;
  property_id: number;
  source: string;
  kind: string;
  label?: string | null;
  url: string;
  storage_key?: string | null;
  content_type?: string | null;
  sort_order: number;
  created_at?: string | null;
  updated_at?: string | null;
};

export type RehabPhotoIssue = {
  title: string;
  category: string;
  severity: string;
  estimated_cost?: number | null;
  blocker: boolean;
  notes?: string | null;
  evidence_photo_ids: number[];
};

export type RehabPhotoAnalysis = {
  ok: boolean;
  property_id: number;
  photo_count: number;
  summary: Record<string, number>;
  issues: RehabPhotoIssue[];
  created?: number | null;
  created_task_ids: number[];
  code?: string | null;
};

export type IngestionSource = {
  id: number;
  org_id: number;
  provider: string;
  slug: string;
  display_name: string;
  source_type: string;
  status: string;
  is_enabled: boolean;
  base_url?: string | null;
  webhook_secret_hint?: string | null;
  schedule_cron?: string | null;
  sync_interval_minutes?: number | null;
  config_json: Record<string, any>;
  cursor_json: Record<string, any>;
  last_synced_at?: string | null;
  last_success_at?: string | null;
  last_failure_at?: string | null;
  next_scheduled_at?: string | null;
  last_error_summary?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type IngestionRun = {
  id: number;
  source_id: number;
  source_label: string;
  provider: string;
  trigger_type: string;
  status: string;
  started_at: string;
  finished_at?: string | null;
  records_seen: number;
  records_imported: number;
  properties_created?: number;
  properties_updated?: number;
  duplicates_skipped: number;
  invalid_rows: number;
  error_summary?: string | null;
};

export type IngestionRunDetail = {
  id: number;
  source_id: number;
  trigger_type: string;
  status: string;
  started_at: string;
  finished_at?: string | null;
  records_seen: number;
  records_imported: number;
  properties_created: number;
  properties_updated: number;
  deals_created: number;
  deals_updated: number;
  rent_rows_upserted: number;
  photos_upserted: number;
  duplicates_skipped: number;
  invalid_rows: number;
  retry_count: number;
  error_summary?: string | null;
  error_json?: Record<string, any> | null;
  summary_json?: Record<string, any> | null;
};

export type IngestionOverview = {
  sources_connected: number;
  sources_enabled: number;
  last_sync_at?: string | null;
  success_runs_24h: number;
  failed_runs_24h: number;
  records_imported_24h: number;
  duplicates_skipped_24h: number;
  total_sources?: number;
  properties_created_7d?: number;
  properties_updated_7d?: number;
};

export function getOrgSlug(): string {
  const env = (import.meta as any).env || {};
  const envOrg = (env.VITE_ORG_SLUG as string | undefined)?.trim();
  if (envOrg) return envOrg;
  return (localStorage.getItem("org_slug") || "").trim();
}

export function setOrgSlug(slug: string) {
  const s = (slug || "").trim();
  if (!s) return;
  localStorage.setItem("org_slug", s);
}

export function clearOrgSlug() {
  localStorage.removeItem("org_slug");
}

export function buildZillowUrl(property: {
  address?: string;
  city?: string;
  state?: string;
}) {
  if (!property?.address) return null;

  const slug =
    `${property.address} ${property.city ?? ""} ${property.state ?? ""}`
      .replace(/,/g, "")
      .replace(/\s+/g, "-");

  return `https://www.zillow.com/homes/${slug}_rb/`;
}

function getAuth(): AuthContext {
  const env = (import.meta as any).env || {};
  const devEmail = (env.VITE_DEV_EMAIL as string | undefined) || undefined;
  const devRole = (env.VITE_DEV_ROLE as string | undefined) || undefined;
  return { orgSlug: getOrgSlug(), devEmail, devRole };
}

function asArray<T = any>(x: any): T[] {
  if (Array.isArray(x)) return x;
  if (x && Array.isArray(x.items)) return x.items;
  if (x && Array.isArray(x.rows)) return x.rows;
  if (x && Array.isArray(x.data)) return x.data;
  return [];
}

type CacheEntry = { at: number; value: any };
const memCache = new Map<string, CacheEntry>();
const inflight = new Map<string, Promise<any>>();

export function clearApiCache() {
  memCache.clear();
  inflight.clear();
}

function cacheKey(method: string, path: string, body?: any) {
  return `${method}:${path}:${body ?? ""}`;
}

function qs(params: Record<string, any>) {
  const sp = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    sp.set(k, String(v));
  });
  const s = sp.toString();
  return s ? `?${s}` : "";
}

function makeEventSource(pathWithQuery: string): EventSource {
  const auth = getAuth();

  const base = API_BASE.startsWith("http")
    ? API_BASE
    : `${window.location.origin}${API_BASE}`;

  const url = new URL(`${base}${pathWithQuery}`);

  if (auth.orgSlug && !url.searchParams.get("org_slug")) {
    url.searchParams.set("org_slug", auth.orgSlug);
  }
  if (auth.devEmail && !url.searchParams.get("user_email")) {
    url.searchParams.set("user_email", auth.devEmail);
  }
  if (auth.devRole && !url.searchParams.get("user_role")) {
    url.searchParams.set("user_role", auth.devRole);
  }

  return new EventSource(url.toString(), { withCredentials: true } as any);
}

function authHeaders(): HeadersInit {
  const auth = getAuth();
  const headers: Record<string, string> = {};

  if (auth.orgSlug) headers["X-Org-Slug"] = auth.orgSlug;
  if (auth.devEmail) headers["X-User-Email"] = auth.devEmail;
  if (auth.devRole) headers["X-User-Role"] = auth.devRole;

  return headers;
}

async function request<T>(
  path: string,
  init?:
    | (RequestInit & {
        cacheTtlMs?: number;
        signal?: AbortSignal;
      })
    | undefined,
): Promise<T> {
  const auth = getAuth();
  const method = (init?.method || "GET").toUpperCase();
  const ttl = init?.cacheTtlMs ?? (method === "GET" ? 4_000 : 0);

  const bodyKey = typeof init?.body === "string" ? init.body : undefined;
  const key = cacheKey(method, path, bodyKey);

  if (method === "GET" && ttl > 0) {
    const hit = memCache.get(key);
    if (hit && Date.now() - hit.at < ttl) {
      return hit.value as T;
    }
  }

  if (method === "GET") {
    const pending = inflight.get(key);
    if (pending) return (await pending) as T;
  }

  const run = (async () => {
    const headers: Record<string, string> = {
      ...(init?.headers as any),
    };

    const hasBody =
      init?.body !== undefined &&
      init?.body !== null &&
      !(typeof init.body === "string" && init.body.length === 0);

    if (hasBody && !headers["Content-Type"]) {
      headers["Content-Type"] = "application/json";
    }

    const isAuthBootstrap =
      path.startsWith("/auth/login") ||
      path.startsWith("/auth/register") ||
      path.startsWith("/auth/logout") ||
      path.startsWith("/auth/orgs") ||
      path.startsWith("/auth/select-org");

    if (auth.orgSlug && !isAuthBootstrap) {
      headers["X-Org-Slug"] = auth.orgSlug;
    }
    if (auth.devEmail) headers["X-User-Email"] = auth.devEmail;
    if (auth.devRole) headers["X-User-Role"] = auth.devRole;

    const res = await fetch(`${API_BASE}${path}`, {
      ...init,
      credentials: "include",
      headers,
      signal: init?.signal,
    });

    if (!res.ok) {
      const text = await res.text();

      if (res.status === 401 && path.startsWith("/auth/me")) {
        return null as any as T;
      }

      throw new Error(`${res.status} ${res.statusText}: ${text}`);
    }

    const ct = res.headers.get("content-type") || "";
    const data = ct.includes("application/json")
      ? await res.json()
      : await res.text();

    if (method === "GET" && ttl > 0) {
      memCache.set(key, { at: Date.now(), value: data });
    } else if (method !== "GET") {
      clearApiCache();
    }

    return data as T;
  })();

  if (method === "GET") inflight.set(key, run);

  try {
    return await run;
  } finally {
    if (method === "GET") inflight.delete(key);
  }
}

async function requestArray<T = any>(
  path: string,
  init?:
    | (RequestInit & {
        cacheTtlMs?: number;
        signal?: AbortSignal;
      })
    | undefined,
): Promise<T[]> {
  const data = await request<any>(path, init);
  return asArray<T>(data);
}

export const api = {
  get: <T = any>(
    path: string,
    init?: { cacheTtlMs?: number; signal?: AbortSignal },
  ) =>
    request<T>(path, {
      method: "GET",
      cacheTtlMs: init?.cacheTtlMs ?? 0,
      signal: init?.signal,
    }),

  post: <T = any>(
    path: string,
    body?: any,
    init?: { cacheTtlMs?: number; signal?: AbortSignal },
  ) =>
    request<T>(path, {
      method: "POST",
      body: JSON.stringify(body ?? {}),
      cacheTtlMs: init?.cacheTtlMs,
      signal: init?.signal,
    }),

  put: <T = any>(
    path: string,
    body?: any,
    init?: { cacheTtlMs?: number; signal?: AbortSignal },
  ) =>
    request<T>(path, {
      method: "PUT",
      body: JSON.stringify(body ?? {}),
      cacheTtlMs: init?.cacheTtlMs,
      signal: init?.signal,
    }),

  patch: <T = any>(
    path: string,
    body?: any,
    init?: { cacheTtlMs?: number; signal?: AbortSignal },
  ) =>
    request<T>(path, {
      method: "PATCH",
      body: JSON.stringify(body ?? {}),
      cacheTtlMs: init?.cacheTtlMs,
      signal: init?.signal,
    }),

  delete: <T = any>(
    path: string,
    init?: { cacheTtlMs?: number; signal?: AbortSignal },
  ) =>
    request<T>(path, {
      method: "DELETE",
      cacheTtlMs: init?.cacheTtlMs,
      signal: init?.signal,
    }),

  authRegister: (payload: {
    email: string;
    password: string;
    org_slug?: string;
    org_name?: string;
  }) =>
    request<any>(`/auth/register`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  authLogin: (payload: { email: string; password: string; org_slug: string }) =>
    request<any>(`/auth/login`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  authLogout: () =>
    request<any>(`/auth/logout`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  authMe: () => request<any>(`/auth/me`, { method: "GET", cacheTtlMs: 0 }),

  authMyOrgs: () =>
    requestArray<any>(`/auth/orgs`, { method: "GET", cacheTtlMs: 2_000 }),

  authSelectOrg: (orgSlug: string) =>
    request<any>(`/auth/select-org${qs({ org_slug: orgSlug })}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  photos: (propertyId: number, signal?: AbortSignal) =>
    request<PropertyPhoto[]>(`/photos/${propertyId}`, {
      cacheTtlMs: 1_000,
      signal,
    }),

  uploadPhoto: async (args: {
    propertyId: number;
    file: File;
    kind?: string;
    label?: string;
  }) => {
    const form = new FormData();
    form.append("property_id", String(args.propertyId));
    form.append("file", args.file);
    form.append("kind", args.kind || "unknown");
    if (args.label) form.append("label", args.label);

    const res = await fetch(`${API_BASE}/photos/upload`, {
      method: "POST",
      credentials: "include",
      headers: authHeaders(),
      body: form,
    });

    if (!res.ok) {
      throw new Error(await res.text());
    }
    clearApiCache();
    return res.json() as Promise<PropertyPhoto>;
  },

  deletePhoto: (photoId: number) =>
    request<{ ok: boolean; id: number }>(`/photos/${photoId}`, {
      method: "DELETE",
    }),

  previewRehabFromPhotos: (propertyId: number, signal?: AbortSignal) =>
    request<RehabPhotoAnalysis>(`/rehab/from-photos/${propertyId}`, {
      cacheTtlMs: 500,
      signal,
    }),

  generateRehabFromPhotos: (propertyId: number) =>
    request<RehabPhotoAnalysis>(`/rehab/from-photos/${propertyId}`, {
      method: "POST",
    }),

  dashboardProperties: (p: {
    limit: number;
    signal?: AbortSignal;
    params?: Record<string, any>;
  }) =>
    requestArray<any>(
      `/dashboard/properties${qs({
        limit: p.limit ?? 100,
        ...(p.params || {}),
      })}`,
      {
        cacheTtlMs: 3_000,
        signal: p.signal,
      },
    ),

  properties: (params?: Record<string, any>, signal?: AbortSignal) =>
    request<any>(`/properties${qs(params || {})}`, {
      method: "GET",
      cacheTtlMs: 1_500,
      signal,
    }),

  propertyView: (id: number, signal?: AbortSignal) =>
    request<any>(`/properties/${id}/view`, { cacheTtlMs: 1_000, signal }),

  propertyBundle: (id: number, signal?: AbortSignal) =>
    request<any>(`/properties/${id}/bundle`, { cacheTtlMs: 1_000, signal }),

  propertyCockpit: (id: number, signal?: AbortSignal) =>
    request<any>(`/properties/${id}/cockpit`, { cacheTtlMs: 1_000, signal }),

  workflowCatalog: (signal?: AbortSignal) =>
    request<any>(`/workflow/catalog`, { cacheTtlMs: 10_000, signal }),

  workflowState: (
    propertyId: number,
    recompute: boolean = true,
    signal?: AbortSignal,
  ) =>
    request<any>(
      `/workflow/state/${propertyId}${qs({ recompute: recompute ? "true" : "false" })}`,
      {
        cacheTtlMs: 500,
        signal,
      },
    ),

  workflowTransition: (propertyId: number, signal?: AbortSignal) =>
    request<any>(`/workflow/transition/${propertyId}`, {
      cacheTtlMs: 500,
      signal,
    }),

  workflowAdvance: (propertyId: number) =>
    request<any>(`/workflow/advance/${propertyId}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  opsPropertySummary: (
    propertyId: number,
    cashDays: number = 90,
    signal?: AbortSignal,
  ) =>
    request<any>(
      `/ops/property/${propertyId}/summary${qs({ cash_days: cashDays })}`,
      { cacheTtlMs: 800, signal },
    ),

  opsPropertyWorkflow: (propertyId: number, signal?: AbortSignal) =>
    request<any>(`/ops/property/${propertyId}/workflow`, {
      cacheTtlMs: 500,
      signal,
    }),

  opsRollups: (params?: Record<string, any>, signal?: AbortSignal) =>
    request<any>(`/ops/rollups${qs(params || {})}`, {
      method: "GET",
      cacheTtlMs: 800,
      signal,
    }),

  opsControlPlane: (params?: Record<string, any>, signal?: AbortSignal) =>
    request<any>(`/ops/control-plane${qs(params || {})}`, {
      method: "GET",
      cacheTtlMs: 800,
      signal,
    }),

  opsGenerateRehabTasks: (propertyId: number) =>
    request<any>(`/ops/property/${propertyId}/generate_rehab_tasks`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  seedPolicyMarket: (payload: PolicyMarketPayload) =>
    request<any>(`/policy/market/seed`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        focus: payload.focus ?? "se_mi_extended",
        org_scope: payload.org_scope ?? false,
        async_mode: payload.async_mode ?? false,
      }),
    }),

  collectPolicyMarket: (payload: PolicyMarketPayload) =>
    request<any>(`/policy/market/collect`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        focus: payload.focus ?? "se_mi_extended",
        org_scope: payload.org_scope ?? false,
        async_mode: payload.async_mode ?? false,
      }),
    }),

  extractPolicyMarket: (payload: PolicyMarketPayload) =>
    request<any>(`/policy/market/extract`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        focus: payload.focus ?? "se_mi_extended",
        org_scope: payload.org_scope ?? false,
        async_mode: payload.async_mode ?? false,
      }),
    }),

  rebuildPolicyMarket: (
    payload: PolicyMarketPayload & { pha_name?: string | null },
  ) =>
    request<any>(`/policy/profiles/build`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
      }),
    }),

  getPolicyMarketStatus: (params: {
    state?: string;
    county?: string | null;
    city?: string | null;
    org_scope?: boolean;
  }) =>
    request<any>(
      `/policy/market/status${qs({
        state: params.state ?? "MI",
        county: params.county ?? undefined,
        city: params.city ?? undefined,
        org_scope: params.org_scope ?? false,
      })}`,
    ),

  getPolicyMarketReadiness: (params?: {
    focus?: string;
    org_scope?: boolean;
  }) =>
    request<{ ok: boolean; items: PolicyReadinessRow[] }>(
      `/policy/market/readiness${qs({
        focus: params?.focus ?? "se_mi_extended",
        org_scope: params?.org_scope ?? false,
      })}`,
    ),

  getPolicyReviewQueue: (params: {
    state?: string;
    county?: string | null;
    city?: string | null;
    org_scope?: boolean;
  }) =>
    request<{ ok: boolean; count: number; items: PolicyReviewQueueItem[] }>(
      `/policy/review-queue${qs({
        state: params.state ?? "MI",
        county: params.county ?? undefined,
        city: params.city ?? undefined,
        org_scope: params.org_scope ?? false,
      })}`,
    ),

  policyBuildMarket: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
    focus?: string;
    notes?: string | null;
  }) =>
    request<any>(`/policy/market/build`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        focus: payload.focus ?? "se_mi_extended",
        notes: payload.notes ?? null,
      }),
    }),

  policyRunMarketPipeline: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
    focus?: string;
  }) =>
    request<any>(`/policy/market/pipeline`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        focus: payload.focus ?? "se_mi_extended",
      }),
    }),

  policyRefreshCoverage: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
    notes?: string | null;
  }) =>
    request<any>(`/policy/coverage`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        notes: payload.notes ?? null,
      }),
    }),

  policyCleanupStaleMarket: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
    archive_extracted_duplicates?: boolean;
  }) =>
    request<any>(`/policy/market/cleanup-stale`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        archive_extracted_duplicates:
          payload.archive_extracted_duplicates ?? true,
      }),
    }),

  policyRepairMarket: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
    focus?: string;
    archive_extracted_duplicates?: boolean;
  }) =>
    request<any>(`/policy/market/repair`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        focus: payload.focus ?? "se_mi_extended",
        archive_extracted_duplicates:
          payload.archive_extracted_duplicates ?? true,
      }),
    }),

  createDeal: (payload: {
    property_id: number;
    asking_price: number;
    rehab_estimate?: number;
    strategy?: string;
    estimated_purchase_price?: number | null;
    financing_type?: string;
    interest_rate?: number;
    term_years?: number;
    down_payment_pct?: number;
  }) =>
    request<any>(`/deals`, {
      method: "POST",
      body: JSON.stringify({
        property_id: payload.property_id,
        asking_price: payload.asking_price,
        rehab_estimate: payload.rehab_estimate ?? 0,
        strategy: payload.strategy ?? "section8",
        estimated_purchase_price: payload.estimated_purchase_price ?? null,
        financing_type: payload.financing_type ?? "dscr",
        interest_rate: payload.interest_rate ?? 0.07,
        term_years: payload.term_years ?? 30,
        down_payment_pct: payload.down_payment_pct ?? 0.2,
      }),
    }),

  intakeDeal: (payload: any) =>
    request<any>(`/deals/intake`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  enrichProperty: (propertyId: number, strategy: string = "section8") =>
    request<any>(`/rent/enrich${qs({ property_id: propertyId, strategy })}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  geoEnrichProperty: (
    propertyId: number,
    force: boolean = false,
    signal?: AbortSignal,
  ) =>
    request<any>(
      `/properties/${propertyId}/geo/enrich${qs({
        force: force ? "true" : "false",
      })}`,
      {
        method: "POST",
        body: JSON.stringify({}),
        cacheTtlMs: 0,
        signal,
      },
    ),

  geoEnrichMissing: (params?: {
    state?: string;
    limit?: number;
    force?: boolean;
    signal?: AbortSignal;
  }) =>
    request<any>(
      `/geo/enrich_missing${qs({
        state: params?.state ?? "MI",
        limit: params?.limit ?? 50,
        force: params?.force ? "true" : "false",
      })}`,
      {
        method: "POST",
        body: JSON.stringify({}),
        cacheTtlMs: 0,
        signal: params?.signal,
      },
    ),

  geoRedZoneCheck: (lat: number, lng: number, signal?: AbortSignal) =>
    request<any>(`/geo/redzone_check${qs({ lat, lng })}`, {
      method: "GET",
      cacheTtlMs: 0,
      signal,
    }),

  explainProperty: (
    propertyId: number,
    strategy: string = "section8",
    persist: boolean = true,
    payment_standard_pct?: number,
  ) =>
    request<any>(
      `/rent/explain/${propertyId}${qs({
        strategy,
        persist: persist ? "true" : "false",
        payment_standard_pct,
      })}`,
      { method: "GET", cacheTtlMs: 0 },
    ),

  evaluateProperty: (propertyId: number, strategy: string = "section8") =>
    request<any>(`/evaluate/property/${propertyId}${qs({ strategy })}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  evaluateRun: (snapshotId: number, strategy: string = "section8") =>
    request<any>(`/evaluate/run${qs({ snapshot_id: snapshotId, strategy })}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  evaluateResults: (params: {
    snapshot_id?: number;
    decision?: string;
    limit?: number;
  }) =>
    requestArray<any>(
      `/evaluate/results${qs({
        snapshot_id: params.snapshot_id,
        decision: params.decision,
        limit: params.limit ?? 100,
      })}`,
      { cacheTtlMs: 1_000 },
    ),

  checklistLatest: (propertyId: number, signal?: AbortSignal) =>
    request(`/compliance/checklist/${propertyId}/latest`, {
      method: "GET",
      signal,
    }),

  generateChecklist: (
    propertyId: number,
    opts?: {
      strategy?: string;
      persist?: boolean;
      version?: string;
      include_policy?: boolean;
    },
    signal?: AbortSignal,
  ) => {
    const strategy = opts?.strategy || "section8";
    const persist = opts?.persist ?? true;
    const version = opts?.version || "v1";
    const includePolicy = opts?.include_policy ?? true;

    return request(
      `/compliance/checklist/${propertyId}?strategy=${encodeURIComponent(strategy)}&version=${encodeURIComponent(version)}&persist=${persist ? "true" : "false"}&include_policy=${includePolicy ? "true" : "false"}`,
      {
        method: "POST",
        signal,
      },
    );
  },

  updateChecklistItem: (
    propertyId: number,
    itemCode: string,
    patch: {
      status?: string | null;
      proof_url?: string | null;
      notes?: string | null;
    },
    signal?: AbortSignal,
  ) =>
    request(
      `/compliance/checklist/${propertyId}/items/${encodeURIComponent(itemCode)}`,
      {
        method: "PATCH",
        body: JSON.stringify(patch),
        signal,
      },
    ),

  rehabTasks: (propertyId: number, signal?: AbortSignal) =>
    requestArray<any>(
      `/rehab/tasks${qs({ property_id: propertyId, limit: 500 })}`,
      { cacheTtlMs: 2_000, signal },
    ),

  rehabSummary: (propertyId: number, signal?: AbortSignal) =>
    request<any>(`/rehab/tasks/summary/${propertyId}`, {
      cacheTtlMs: 800,
      signal,
    }),

  createRehabTask: (payload: any) =>
    request<any>(`/rehab/tasks`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  updateRehabTask: (taskId: number, payload: any) =>
    request<any>(`/rehab/tasks/${taskId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    }),

  deleteRehabTask: (taskId: number) =>
    request<any>(`/rehab/tasks/${taskId}`, { method: "DELETE" }),

  leases: (propertyId: number, signal?: AbortSignal) =>
    requestArray<any>(
      `/tenants/leases${qs({ property_id: propertyId, limit: 200 })}`,
      { cacheTtlMs: 2_000, signal },
    ),

  leaseWorkflow: (propertyId: number, signal?: AbortSignal) =>
    request<any>(`/tenants/leases/workflow/${propertyId}`, {
      cacheTtlMs: 800,
      signal,
    }),

  createLease: (payload: any) =>
    request<any>(`/tenants/leases`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  txns: (propertyId: number, signal?: AbortSignal) =>
    requestArray<any>(
      `/cash/transactions${qs({ property_id: propertyId, limit: 1000 })}`,
      { cacheTtlMs: 2_000, signal },
    ),

  createTxn: (payload: any) =>
    request<any>(`/cash/transactions`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  cashRollup: (propertyId: number, year: number, signal?: AbortSignal) =>
    request<any>(`/cash/rollup${qs({ property_id: propertyId, year })}`, {
      cacheTtlMs: 800,
      signal,
    }),

  valuations: (propertyId: number, signal?: AbortSignal) =>
    requestArray<any>(
      `/equity/valuations${qs({ property_id: propertyId, limit: 200 })}`,
      { cacheTtlMs: 2_000, signal },
    ),

  createValuation: (payload: any) =>
    request<any>(`/equity/valuations`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  valuationSuggestions: (
    propertyId: number,
    cadence: "quarterly" | "monthly" = "quarterly",
    count: number = 4,
    signal?: AbortSignal,
  ) =>
    request<any>(
      `/equity/valuation/suggestions${qs({
        property_id: propertyId,
        cadence,
        count,
      })}`,
      { cacheTtlMs: 800, signal },
    ),

  trustGet: (
    entity_type: string,
    entity_id: string | number,
    signal?: AbortSignal,
  ) =>
    request<any>(`/trust/${entity_type}/${entity_id}`, {
      cacheTtlMs: 800,
      signal,
    }),

  trustEmitSignal: (
    entity_type: string,
    entity_id: string | number,
    payload: { signal_key: string; value: number; meta?: any },
  ) =>
    request<any>(`/trust/${entity_type}/${entity_id}/signal`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  agents: () => requestArray<any>(`/agents`, { cacheTtlMs: 4_000 }),

  slotSpecs: (signal?: AbortSignal, cacheTtlMs: number = 10_000) =>
    requestArray<any>(`/agents/slots/specs`, { cacheTtlMs, signal }),

  slotAssignments: (
    a?: number | { property_id?: number; limit?: number; signal?: AbortSignal },
    b?: AbortSignal,
  ) => {
    let property_id: number | undefined;
    let limit: number | undefined;
    let signal: AbortSignal | undefined;

    if (typeof a === "number") {
      property_id = a;
      signal = b;
      limit = 200;
    } else if (a && typeof a === "object") {
      property_id = a.property_id;
      limit = a.limit ?? 200;
      signal = a.signal;
    } else {
      limit = 200;
      signal = b;
    }

    return requestArray<any>(
      `/agents/slots/assignments${qs({ property_id, limit })}`,
      { cacheTtlMs: 1_000, signal },
    );
  },

  upsertSlotAssignment: (payload: {
    slot_key: string;
    property_id?: number | null;
    owner_type?: string | null;
    assignee?: string | null;
    status?: string | null;
    notes?: string | null;
    agent_key?: string | null;
    payload_json?: any;
  }) =>
    request<any>(`/agents/slots/assignments`, {
      method: "POST",
      body: JSON.stringify({
        slot_key: payload.slot_key,
        property_id: payload.property_id ?? null,
        owner_type: payload.owner_type ?? null,
        assignee: payload.assignee ?? null,
        status: payload.status ?? null,
        notes: payload.notes ?? null,
        agent_key: payload.agent_key ?? null,
        payload_json: payload.payload_json ?? null,
      }),
    }),

  agentRunsList: (
    arg?:
      | number
      | {
          property_id?: number;
          agent_key?: string;
          status?: string;
          limit?: number;
        },
    signal?: AbortSignal,
  ) => {
    const params =
      typeof arg === "number"
        ? { property_id: arg, limit: 100 }
        : {
            property_id: arg?.property_id,
            agent_key: arg?.agent_key,
            status: arg?.status,
            limit: arg?.limit ?? 100,
          };
    return requestArray<any>(`/agent-runs/${qs(params)}`, {
      cacheTtlMs: 800,
      signal,
    });
  },

  agentRunsSummary: (params?: Record<string, any>, signal?: AbortSignal) =>
    request<any>(`/agent-runs/summary${qs(params || {})}`, {
      method: "GET",
      cacheTtlMs: 800,
      signal,
    }),

  agentRunsHistory: (params?: Record<string, any>, signal?: AbortSignal) =>
    request<any>(`/agent-runs/history${qs(params || {})}`, {
      method: "GET",
      cacheTtlMs: 800,
      signal,
    }),

  agentRunsCompare: (runIds: number[], signal?: AbortSignal) =>
    request<any>(`/agent-runs/compare${qs({ run_ids: runIds.join(",") })}`, {
      method: "GET",
      cacheTtlMs: 0,
      signal,
    }),

  agentRunsCockpit: (propertyId: number, signal?: AbortSignal) =>
    request<any>(`/agent-runs/property/${propertyId}/cockpit`, {
      method: "GET",
      cacheTtlMs: 500,
      signal,
    }),

  agentRunGet: (runId: number, signal?: AbortSignal) =>
    request<any>(`/agent-runs/${runId}`, {
      method: "GET",
      cacheTtlMs: 300,
      signal,
    }),

  agentRunTrace: (runId: number, signal?: AbortSignal) =>
    request<any>(`/agent-runs/${runId}/trace`, {
      method: "GET",
      cacheTtlMs: 0,
      signal,
    }),

  createAgentRun: (payload: {
    agent_key: string;
    property_id?: number | null;
    input_json?: any;
  }) =>
    request<any>(`/agents/runs`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  agentRunsPlan: (propertyId: number) =>
    request<any>(`/agent-runs/plan${qs({ property_id: propertyId })}`, {
      method: "POST",
      cacheTtlMs: 0,
    }),

  agentRunsEnqueue: (propertyId: number, dispatch: boolean = true) =>
    request<any>(
      `/agent-runs/enqueue${qs({
        property_id: propertyId,
        dispatch: dispatch ? "true" : "false",
      })}`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),

  agentRunsDispatchOne: (runId: number) =>
    request<any>(`/agent-runs/${runId}/dispatch`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  agentRunsApprove: (runId: number) =>
    request<any>(`/agent-runs/${runId}/approve`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  agentRunsReject: (runId: number, reason: string) =>
    request<any>(`/agent-runs/${runId}/reject${qs({ reason })}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  agentRunsApply: (runId: number) =>
    request<any>(`/agent-runs/${runId}/apply`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  agentRunsRetry: (runId: number, dispatch: boolean = true) =>
    request<any>(
      `/agent-runs/${runId}/retry${qs({ dispatch: dispatch ? "true" : "false" })}`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),

  agentRunsDeadletter: (params?: Record<string, any>, signal?: AbortSignal) =>
    requestArray<any>(`/agent-runs/deadletter${qs(params || {})}`, {
      cacheTtlMs: 800,
      signal,
    }),

  agentRunsAckDeadletter: (deadId: number) =>
    request<any>(`/agent-runs/deadletter/${deadId}/ack`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  agentRunsMessages: (
    runId: number,
    params?: Record<string, any>,
    signal?: AbortSignal,
  ) =>
    requestArray<any>(`/agent-runs/${runId}/messages${qs(params || {})}`, {
      cacheTtlMs: 0,
      signal,
    }),

  agentRunsStream: (runId: number) =>
    makeEventSource(`/agent-runs/${runId}/stream`),

  policyCatalogMunicipalities: (focus: string = "se_mi_extended") =>
    request<any>(`/policy/catalog/municipalities${qs({ focus })}`, {
      method: "GET",
      cacheTtlMs: 10_000,
    }),

  policyCollectCatalogMarket: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    org_scope?: boolean;
    focus?: string;
  }) =>
    request<any>(`/policy/catalog/collect/market`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        org_scope: payload.org_scope ?? false,
        focus: payload.focus ?? "se_mi_extended",
      }),
    }),

  policyCollectCatalogAll: (payload?: {
    focus?: string;
    org_scope?: boolean;
  }) =>
    request<any>(
      `/policy/catalog/collect/all${qs({
        focus: payload?.focus ?? "se_mi_extended",
        org_scope: payload?.org_scope ? "true" : "false",
      })}`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),

  policyExtractMarket: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    org_scope?: boolean;
    focus?: string;
  }) =>
    request<any>(`/policy/extract/market`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        org_scope: payload.org_scope ?? false,
        focus: payload.focus ?? "se_mi_extended",
      }),
    }),

  policyExtractAll: (payload?: { focus?: string; org_scope?: boolean }) =>
    request<any>(
      `/policy/extract/all${qs({
        focus: payload?.focus ?? "se_mi_extended",
        org_scope: payload?.org_scope ? "true" : "false",
      })}`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),

  policyReviewAssertionsBatch: (payload: {
    assertion_ids: number[];
    review_status: string;
    confidence?: number;
    review_notes?: string | null;
    verification_reason?: string | null;
  }) =>
    request<any>(`/policy/assertions/review/batch`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  policyBuildProfilesAll: (payload?: { focus?: string; org_scope?: boolean }) =>
    request<any>(
      `/policy/profiles/build/all${qs({
        focus: payload?.focus ?? "se_mi_extended",
        org_scope: payload?.org_scope ? "true" : "false",
      })}`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),

  policyCoverageAll: (params?: { focus?: string; org_scope?: boolean }) =>
    request<any>(
      `/policy/coverage/all${qs({
        focus: params?.focus ?? "se_mi_extended",
        org_scope: params?.org_scope ? "true" : "false",
      })}`,
      { method: "GET", cacheTtlMs: 0 },
    ),

  policyEvidenceMarket: (params: {
    state: string;
    county?: string | null;
    city?: string | null;
    include_global?: boolean;
  }) =>
    request<any>(
      `/policy-evidence/market${qs({
        state: params.state,
        county: params.county ?? undefined,
        city: params.city ?? undefined,
        include_global:
          params.include_global === undefined
            ? "true"
            : params.include_global
              ? "true"
              : "false",
      })}`,
      { method: "GET", cacheTtlMs: 0 },
    ),

  policyReviewQueue: (params?: { focus?: string; org_scope?: boolean }) =>
    request<any>(
      `/policy-evidence/review-queue${qs({
        focus: params?.focus ?? "se_mi_extended",
        org_scope: params?.org_scope ? "true" : "false",
      })}`,
      { method: "GET", cacheTtlMs: 0 },
    ),

  complianceCreateTasksFromPolicy: (propertyId: number) =>
    request<any>(`/compliance/property/${propertyId}/tasks/from-policy`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  policyCatalog: (focus: string = "se_mi_extended") =>
    request<any>(`/policy/catalog${qs({ focus })}`, {
      method: "GET",
      cacheTtlMs: 10_000,
    }),

  policyCatalogIngest: (payload?: { focus?: string; org_scope?: boolean }) =>
    request<any>(
      `/policy/catalog/ingest${qs({
        focus: payload?.focus ?? "se_mi_extended",
        org_scope: payload?.org_scope ? "true" : "false",
      })}`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),

  policyCollectSource: (payload: {
    url: string;
    state?: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    program_type?: string | null;
    publisher?: string | null;
    title?: string | null;
    notes?: string | null;
    org_scope?: boolean;
  }) =>
    request<any>(`/policy/sources/collect`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  policySources: (params?: {
    limit?: number;
    state?: string;
    county?: string;
    city?: string;
    pha_name?: string;
    program_type?: string;
    include_global?: boolean;
  }) =>
    request<any>(
      `/policy/sources${qs({
        limit: params?.limit ?? 100,
        state: params?.state,
        county: params?.county,
        city: params?.city,
        pha_name: params?.pha_name,
        program_type: params?.program_type,
        include_global:
          params?.include_global === undefined
            ? "true"
            : params.include_global
              ? "true"
              : "false",
      })}`,
      { cacheTtlMs: 1_000 },
    ),

  policyExtractAssertions: (payload: {
    source_id: number;
    org_scope?: boolean;
  }) =>
    request<any>(`/policy/assertions/extract`, {
      method: "POST",
      body: JSON.stringify({
        source_id: payload.source_id,
        org_scope: payload.org_scope ?? false,
      }),
    }),

  policyAssertions: (params?: {
    review_status?: string;
    rule_key?: string;
    rule_family?: string;
    assertion_type?: string;
    state?: string;
    county?: string;
    city?: string;
    pha_name?: string;
    program_type?: string;
    include_global?: boolean;
    limit?: number;
  }) =>
    request<any>(
      `/policy/assertions${qs({
        review_status: params?.review_status,
        rule_key: params?.rule_key,
        rule_family: params?.rule_family,
        assertion_type: params?.assertion_type,
        state: params?.state,
        county: params?.county,
        city: params?.city,
        pha_name: params?.pha_name,
        program_type: params?.program_type,
        include_global:
          params?.include_global === undefined
            ? "true"
            : params.include_global
              ? "true"
              : "false",
        limit: params?.limit ?? 200,
      })}`,
      { cacheTtlMs: 800 },
    ),

  policyReviewAssertion: (
    assertionId: number,
    payload: {
      review_status: string;
      confidence?: number;
      value?: any;
      review_notes?: string | null;
      verification_reason?: string | null;
      stale_after?: string | null;
      superseded_by_assertion_id?: number | null;
    },
  ) =>
    request<any>(`/policy/assertions/${assertionId}/review`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  policyBuildProfile: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
    notes?: string | null;
  }) =>
    request<any>(`/policy/profiles/build`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        notes: payload.notes ?? null,
      }),
    }),

  policyCoverage: (params: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
  }) =>
    request<any>(
      `/policy/coverage${qs({
        state: params.state,
        county: params.county ?? undefined,
        city: params.city ?? undefined,
        pha_name: params.pha_name ?? undefined,
        org_scope: params.org_scope ? "true" : "false",
      })}`,
      { method: "GET", cacheTtlMs: 0 },
    ),

  complianceInspectionReadiness: (propertyId: number, signal?: AbortSignal) =>
    request(`/compliance/property/${propertyId}/inspection-readiness`, {
      method: "GET",
      signal,
    }),

  runComplianceAutomation: (
    propertyId: number,
    createTasks = true,
    signal?: AbortSignal,
  ) =>
    request(
      `/compliance/property/${propertyId}/automation/run?create_tasks=${createTasks ? "true" : "false"}`,
      {
        method: "POST",
        signal,
      },
    ),

  compliancePropertyBrief: (propertyId: number, signal?: AbortSignal) =>
    request(`/compliance/property/${propertyId}/brief`, {
      method: "GET",
      signal,
    }),

  complianceStatus: (propertyId: number, signal?: AbortSignal) =>
    request(`/compliance/status/${propertyId}`, {
      method: "GET",
      signal,
    }),

  complianceRunSummary: (propertyId: number, signal?: AbortSignal) =>
    request(`/compliance/run_hqs/${propertyId}`, {
      method: "GET",
      signal,
    }),

  policyCatalogAdminMarket: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
    focus?: string;
  }) =>
    request<any>(`/policy/catalog-admin/market`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        focus: payload.focus ?? "se_mi_extended",
      }),
    }),

  policyCatalogAdminBootstrap: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
    focus?: string;
  }) =>
    request<any>(`/policy/catalog-admin/market/bootstrap`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        focus: payload.focus ?? "se_mi_extended",
      }),
    }),

  policyCatalogAdminReset: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
    focus?: string;
  }) =>
    request<any>(`/policy/catalog-admin/market/reset`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        focus: payload.focus ?? "se_mi_extended",
      }),
    }),

  policyCatalogAdminCreateItem: (payload: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    program_type?: string | null;
    org_scope?: boolean;
    url: string;
    publisher?: string | null;
    title?: string | null;
    notes?: string | null;
    source_kind?: string | null;
    is_authoritative?: boolean;
    priority?: number;
    baseline_url?: string | null;
  }) =>
    request<any>(`/policy/catalog-admin/market/items`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        program_type: payload.program_type ?? null,
        org_scope: payload.org_scope ?? false,
        url: payload.url,
        publisher: payload.publisher ?? null,
        title: payload.title ?? null,
        notes: payload.notes ?? null,
        source_kind: payload.source_kind ?? null,
        is_authoritative: payload.is_authoritative ?? true,
        priority: payload.priority ?? 100,
        baseline_url: payload.baseline_url ?? null,
      }),
    }),

  policyCatalogAdminUpdateItem: (
    itemId: number,
    payload: {
      org_scope?: boolean;
      title?: string | null;
      publisher?: string | null;
      notes?: string | null;
      source_kind?: string | null;
      is_authoritative?: boolean | null;
      priority?: number | null;
      url?: string | null;
      is_active?: boolean | null;
    },
  ) =>
    request<any>(`/policy/catalog-admin/market/items/${itemId}`, {
      method: "PATCH",
      body: JSON.stringify({
        org_scope: payload.org_scope ?? false,
        title: payload.title ?? undefined,
        publisher: payload.publisher ?? undefined,
        notes: payload.notes ?? undefined,
        source_kind: payload.source_kind ?? undefined,
        is_authoritative:
          payload.is_authoritative === null
            ? undefined
            : payload.is_authoritative,
        priority: payload.priority ?? undefined,
        url: payload.url ?? undefined,
        is_active: payload.is_active === null ? undefined : payload.is_active,
      }),
    }),

  policyCatalogAdminDisableItem: (
    itemId: number,
    payload: {
      state: string;
      county?: string | null;
      city?: string | null;
      pha_name?: string | null;
      org_scope?: boolean;
      focus?: string;
    },
  ) =>
    request<any>(`/policy/catalog-admin/market/items/${itemId}/disable`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        county: payload.county ?? null,
        city: payload.city ?? null,
        pha_name: payload.pha_name ?? null,
        org_scope: payload.org_scope ?? false,
        focus: payload.focus ?? "se_mi_extended",
      }),
    }),

  policyBrief: (params: {
    state: string;
    county?: string | null;
    city?: string | null;
    pha_name?: string | null;
    org_scope?: boolean;
  }) =>
    request<any>(
      `/policy/brief${qs({
        state: params.state,
        county: params.county ?? undefined,
        city: params.city ?? undefined,
        pha_name: params.pha_name ?? undefined,
        org_scope: params.org_scope ? "true" : "false",
      })}`,
      { method: "GET", cacheTtlMs: 0 },
    ),

  listJurisdictionRules: (includeGlobal: boolean, state: string = "MI") => {
    const scope = includeGlobal ? "all" : "org";
    return requestArray<any>(`/jurisdictions/rules${qs({ scope, state })}`, {
      cacheTtlMs: 2_000,
    });
  },

  seedJurisdictionDefaults: () =>
    request<any>(`/jurisdictions/seed`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  createJurisdictionRule: (payload: any) =>
    request<any>(`/jurisdictions/rule${qs({ scope: "org" })}`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  deleteJurisdictionRule: (idOrPayload: any) => {
    if (typeof idOrPayload === "number") {
      throw new Error("deleteJurisdictionRule requires a rule object.");
    }

    const city = idOrPayload.city;
    const state = idOrPayload.state || "MI";

    if (!city) throw new Error("deleteJurisdictionRule missing city");

    return request<any>(
      `/jurisdictions/rule${qs({ city, state, scope: "org" })}`,
      { method: "DELETE" },
    );
  },

  listJurisdictionProfiles: (includeGlobal: boolean, state: string = "MI") =>
    requestArray<any>(
      `/jurisdiction-profiles${qs({
        include_global: includeGlobal ? "true" : "false",
        state,
      })}`,
      { cacheTtlMs: 2_000 },
    ),

  resolveJurisdictionProfile: (payload: {
    city?: string | null;
    county?: string | null;
    state: string;
  }) =>
    request<any>(
      `/jurisdiction-profiles/resolve${qs({
        city: payload.city ?? undefined,
        county: payload.county ?? undefined,
        state: payload.state,
      })}`,
      { method: "GET", cacheTtlMs: 0 },
    ),

  upsertJurisdictionProfile: (payload: {
    state: string;
    city?: string | null;
    county?: string | null;
    friction_multiplier: number;
    pha_name?: string | null;
    policy?: any;
    notes?: string | null;
  }) =>
    request<any>(`/jurisdiction-profiles`, {
      method: "POST",
      body: JSON.stringify({
        state: payload.state,
        city: payload.city ?? null,
        county: payload.county ?? null,
        friction_multiplier: payload.friction_multiplier,
        pha_name: payload.pha_name ?? null,
        policy: payload.policy ?? {},
        notes: payload.notes ?? null,
      }),
    }),

  deleteJurisdictionProfile: (payload: {
    state: string;
    city?: string | null;
    county?: string | null;
  }) =>
    request<any>(
      `/jurisdiction-profiles${qs({
        state: payload.state,
        city: payload.city ?? undefined,
        county: payload.county ?? undefined,
      })}`,
      { method: "DELETE" },
    ),

  importsOverview: () =>
    request<IngestionOverview>(`/imports/overview`, {
      method: "GET",
      cacheTtlMs: 800,
    }),

  importsBootstrap: () =>
    request<any>(`/imports/bootstrap`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  ingestionOverview: (signal?: AbortSignal) =>
    request<IngestionOverview>(`/ingestion/overview`, {
      method: "GET",
      cacheTtlMs: 800,
      signal,
    }),

  listIngestionSources: (signal?: AbortSignal) =>
    requestArray<IngestionSource>(`/ingestion/sources`, {
      method: "GET",
      cacheTtlMs: 1_000,
      signal,
    }),

  createIngestionSource: (payload: {
    provider: string;
    slug: string;
    display_name: string;
    source_type?: string;
    is_enabled?: boolean;
    base_url?: string | null;
    schedule_cron?: string | null;
    sync_interval_minutes?: number | null;
    config_json?: Record<string, any>;
    credentials_json?: Record<string, any>;
  }) =>
    request<IngestionSource>(`/ingestion/sources`, {
      method: "POST",
      body: JSON.stringify({
        provider: payload.provider,
        slug: payload.slug,
        display_name: payload.display_name,
        source_type: payload.source_type ?? "api",
        is_enabled: payload.is_enabled ?? true,
        base_url: payload.base_url ?? null,
        schedule_cron: payload.schedule_cron ?? null,
        sync_interval_minutes: payload.sync_interval_minutes ?? 60,
        config_json: payload.config_json ?? {},
        credentials_json: payload.credentials_json ?? {},
      }),
    }),

  updateIngestionSource: (
    sourceId: number,
    payload: {
      display_name?: string;
      is_enabled?: boolean;
      status?: string;
      base_url?: string | null;
      schedule_cron?: string | null;
      sync_interval_minutes?: number | null;
      config_json?: Record<string, any>;
      credentials_json?: Record<string, any>;
    },
  ) =>
    request<IngestionSource>(`/ingestion/sources/${sourceId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    }),

  syncIngestionSource: (
    sourceId: number,
    payload?: {
      trigger_type?: "manual" | "scheduled" | "webhook" | "daily_refresh";
      state?: string;
      county?: string;
      city?: string;
      min_price?: number;
      max_price?: number;
      min_bedrooms?: number;
      min_bathrooms?: number;
      property_type?: string;
      limit?: number;
    },
  ) =>
    request<{
      ok: boolean;
      queued: boolean;
      task_id?: string;
      run_id?: number;
      status?: string;
      source_id?: number;
    }>(`/ingestion/sources/${sourceId}/sync`, {
      method: "POST",
      body: JSON.stringify({
        trigger_type: payload?.trigger_type ?? "manual",
        state: payload?.state ?? "MI",
        county: payload?.county ?? undefined,
        city: payload?.city ?? undefined,
        min_price: payload?.min_price ?? undefined,
        max_price: payload?.max_price ?? undefined,
        min_bedrooms: payload?.min_bedrooms ?? undefined,
        min_bathrooms: payload?.min_bathrooms ?? undefined,
        property_type: payload?.property_type ?? undefined,
        limit: payload?.limit ?? 100,
      }),
    }),

  syncIngestionDefaults: () =>
    request<{ ok: boolean; queued: number; source_ids: number[] }>(
      `/ingestion/sync-defaults`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),

  queueIngestionDailyRefresh: () =>
    request<{ ok: boolean; queued: boolean; task_id?: string }>(
      `/ingestion/daily-refresh`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),

  queueIngestionDueSources: () =>
    request<{ ok: boolean; queued: boolean; task_id?: string }>(
      `/ingestion/sync-due`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    ),

  listIngestionRuns: (params?: { limit?: number }, signal?: AbortSignal) =>
    requestArray<IngestionRun>(
      `/ingestion/runs${qs({ limit: params?.limit ?? 50 })}`,
      {
        method: "GET",
        cacheTtlMs: 800,
        signal,
      },
    ),

  getIngestionRunDetail: (runId: number, signal?: AbortSignal) =>
    request<IngestionRunDetail>(`/ingestion/runs/${runId}`, {
      method: "GET",
      cacheTtlMs: 200,
      signal,
    }),
};
