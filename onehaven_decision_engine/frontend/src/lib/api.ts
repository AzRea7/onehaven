// frontend/src/lib/api.ts
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
  const envOrg = env.VITE_ORG_SLUG as string | undefined;
  if (envOrg) return envOrg;
  return localStorage.getItem("org_slug") || "demo";
}

export function setOrgSlug(slug: string) {
  localStorage.setItem("org_slug", slug);
}

function getAuth(): AuthContext {
  const env = (import.meta as any).env || {};
  const devEmail = (env.VITE_DEV_EMAIL as string | undefined) || undefined;
  const devRole = (env.VITE_DEV_ROLE as string | undefined) || undefined;
  return { orgSlug: getOrgSlug(), devEmail, devRole };
}

/**
 * Some endpoints may return:
 * - Array directly
 * - {items: [...]}
 * - {rows: [...]}
 * - {data: [...]}
 *
 * To prevent "n.map is not a function", always normalize.
 */
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

function cacheKey(method: string, path: string, body?: any) {
  return `${method}:${path}:${body ?? ""}`;
}

// Helpers for querystring
function qs(params: Record<string, any>) {
  const sp = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v === undefined || v === null) return;
    sp.set(k, String(v));
  });
  const s = sp.toString();
  return s ? `?${s}` : "";
}

/**
 * ✅ EventSource cannot set headers.
 * So for SSE we pass auth via querystring:
 *   ?org_slug=...&user_email=...&user_role=...
 * Backend must accept these as dev-auth fallbacks.
 */
function makeEventSource(pathWithQuery: string): EventSource {
  const auth = getAuth();

  const base = API_BASE.startsWith("http")
    ? API_BASE
    : `${window.location.origin}${API_BASE}`;
  const url = new URL(`${base}${pathWithQuery}`);

  if (!url.searchParams.get("org_slug"))
    url.searchParams.set("org_slug", auth.orgSlug);

  // ✅ dev auth fallbacks for SSE
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

  const bodyKey = typeof init?.body === "string" ? init?.body : undefined;
  const key = cacheKey(method, path, bodyKey);

  if (method === "GET" && ttl > 0) {
    const hit = memCache.get(key);
    if (hit && Date.now() - hit.at < ttl) return hit.value as T;
  }

  if (method === "GET") {
    const pending = inflight.get(key);
    if (pending) return (await pending) as T;
  }

  const run = (async () => {
    const res = await fetch(`${API_BASE}${path}`, {
      ...init,
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        "X-Org-Slug": auth.orgSlug,

        // ✅ browser parity with curl dev auth
        ...(auth.devEmail ? { "X-User-Email": auth.devEmail } : {}),
        ...(auth.devRole ? { "X-User-Role": auth.devRole } : {}),

        ...(init?.headers || {}),
      },
      signal: init?.signal,
    });

    if (!res.ok) {
      const text = await res.text();
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
  // -------------------------
  // ✅ AUTH
  // -------------------------
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
    request<any>(`/auth/logout`, { method: "POST", body: JSON.stringify({}) }),

  authMe: () => request<any>(`/auth/me`, { method: "GET", cacheTtlMs: 0 }),

  authMyOrgs: () =>
    requestArray<any>(`/auth/orgs`, { method: "GET", cacheTtlMs: 2_000 }),

  authSelectOrg: (orgSlug: string) =>
    request<any>(`/auth/select-org${qs({ org_slug: orgSlug })}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  // Dashboard / properties
  dashboardProperties: (p: { limit: number; signal?: AbortSignal }) =>
    requestArray<any>(`/dashboard/properties?limit=${p.limit ?? 100}`, {
      cacheTtlMs: 3_000,
      signal: p.signal,
    }),

  // Property “view” and “bundle”
  propertyView: (id: number, signal?: AbortSignal) =>
    request<any>(`/properties/${id}/view`, { cacheTtlMs: 2_000, signal }),
  propertyBundle: (id: number, signal?: AbortSignal) =>
    request<any>(`/properties/${id}/bundle`, { cacheTtlMs: 2_000, signal }),

  // ✅ Ops summary (closing loops)
  opsPropertySummary: (
    propertyId: number,
    cashDays: number = 90,
    signal?: AbortSignal,
  ) =>
    request<any>(
      `/ops/property/${propertyId}/summary${qs({ cash_days: cashDays })}`,
      {
        cacheTtlMs: 800,
        signal,
      },
    ),

  opsGenerateRehabTasks: (propertyId: number) =>
    request<any>(`/ops/property/${propertyId}/generate_rehab_tasks`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  // Deal creation (Phase 1)
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

  // Deal Intake
  intakeDeal: (payload: any) =>
    request<any>(`/intake/deal`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  // Rent
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

  // Evaluate
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

  // Checklist / Compliance
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
      { method: "POST", body: JSON.stringify({}) },
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
      { method: "PATCH", body: JSON.stringify(payload) },
    ),

  // Rehab
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

  // Tenants / leases
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

  // Cash
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

  // Equity
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

  // Agents
  agents: () => requestArray<any>(`/agents`, { cacheTtlMs: 4_000 }),

  // Agent Slots
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

  // Agent Runs
  agentRunsList: (arg: number | { property_id: number }) => {
    const propertyId = typeof arg === "number" ? arg : arg.property_id;
    return requestArray<any>(`/agent-runs${qs({ property_id: propertyId })}`, {
      cacheTtlMs: 800,
    });
  },

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
      { method: "POST", body: JSON.stringify({}) },
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

  // Jurisdictions
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
    if (typeof idOrPayload === "number")
      throw new Error("deleteJurisdictionRule requires a rule object.");
    const city = idOrPayload.city;
    const state = idOrPayload.state || "MI";
    if (!city) throw new Error("deleteJurisdictionRule missing city");
    return request<any>(
      `/jurisdictions/rule${qs({ city, state, scope: "org" })}`,
      { method: "DELETE" },
    );
  },
};
