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
      "Content-Type": "application/json",
      ...(init?.headers as any),
    };

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

  dashboardProperties: (p: { limit: number; signal?: AbortSignal }) =>
    requestArray<any>(`/dashboard/properties${qs({ limit: p.limit ?? 100 })}`, {
      cacheTtlMs: 3_000,
      signal: p.signal,
    }),

  propertyView: (id: number, signal?: AbortSignal) =>
    request<any>(`/properties/${id}/view`, { cacheTtlMs: 2_000, signal }),

  propertyBundle: (id: number, signal?: AbortSignal) =>
    request<any>(`/properties/${id}/bundle`, { cacheTtlMs: 2_000, signal }),

  opsPropertySummary: (
    propertyId: number,
    cashDays: number = 90,
    signal?: AbortSignal,
  ) =>
    request<any>(
      `/ops/property/${propertyId}/summary${qs({ cash_days: cashDays })}`,
      { cacheTtlMs: 800, signal },
    ),

  opsGenerateRehabTasks: (propertyId: number) =>
    request<any>(`/ops/property/${propertyId}/generate_rehab_tasks`, {
      method: "POST",
      body: JSON.stringify({}),
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
    request<any>(`/intake/deal`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  enrichProperty: (propertyId: number, strategy: string = "section8") =>
    request<any>(`/rent/enrich${qs({ property_id: propertyId, strategy })}`, {
      method: "POST",
      body: JSON.stringify({}),
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
    request<any>(`/compliance/checklist/${propertyId}/latest`, {
      cacheTtlMs: 1_000,
      signal,
    }),

  generateChecklist: (
    propertyId: number,
    opts?: { strategy?: string; version?: string; persist?: boolean },
  ) => {
    const strategy = opts?.strategy ?? "section8";
    const version = opts?.version ?? "v1";
    const persist = opts?.persist ?? true;

    return request<any>(
      `/compliance/checklist/${propertyId}${qs({
        strategy,
        version,
        persist: persist ? "true" : "false",
      })}`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
    );
  },

  updateChecklistItem: (
    propertyId: number,
    itemCode: string,
    payload: {
      status?: string | null;
      proof_url?: string | null;
      notes?: string | null;
    },
  ) =>
    request<any>(
      `/compliance/checklist/${propertyId}/items/${encodeURIComponent(itemCode)}`,
      {
        method: "PATCH",
        body: JSON.stringify(payload),
      },
    ),

  rehabTasks: (propertyId: number, signal?: AbortSignal) =>
    requestArray<any>(
      `/rehab/tasks${qs({ property_id: propertyId, limit: 500 })}`,
      { cacheTtlMs: 2_000, signal },
    ),

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
    agent_key?: string | null;
    status?: string | null;
    payload_json?: any;
  }) =>
    request<any>(`/agents/slots/assignments`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  agentRunsList: (arg: number | { property_id: number }) => {
    const propertyId = typeof arg === "number" ? arg : arg.property_id;
    return requestArray<any>(`/agents/runs${qs({ property_id: propertyId })}`, {
      cacheTtlMs: 800,
    });
  },

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

  compliancePropertyBrief: (propertyId: number) =>
    request<any>(`/compliance/property/${propertyId}/brief`, {
      method: "GET",
      cacheTtlMs: 500,
    }),

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
      body: JSON.stringify(payload),
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
};
