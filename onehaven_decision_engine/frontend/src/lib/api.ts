// frontend/src/lib/api.ts
export const API_BASE = (import.meta as any).env?.VITE_API_BASE || "/api";

type AuthContext = {
  orgSlug: string;
  userEmail: string;
  userRole?: string;
};

function getAuth(): AuthContext {
  const env = (import.meta as any).env || {};
  const envOrg = env.VITE_ORG_SLUG as string | undefined;
  const envEmail = env.VITE_USER_EMAIL as string | undefined;
  const envRole = env.VITE_USER_ROLE as string | undefined;

  if (envOrg && envEmail) {
    return {
      orgSlug: envOrg,
      userEmail: envEmail,
      userRole: envRole || "owner",
    };
  }

  const orgSlug = localStorage.getItem("org_slug") || "demo";
  const userEmail = localStorage.getItem("user_email") || "austin@demo.local";
  const userRole = localStorage.getItem("user_role") || "owner";

  return { orgSlug, userEmail, userRole };
}

/**
 * Some endpoints may return:
 * - Array directly
 * - {items: [...]}
 * - {rows: [...]}
 * - {data: [...]}
 * - {detail: "..."} or other object
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

async function request<T>(
  path: string,
  init?:
    | (RequestInit & { cacheTtlMs?: number; signal?: AbortSignal })
    | undefined,
): Promise<T> {
  const auth = getAuth();
  const method = (init?.method || "GET").toUpperCase();
  const ttl = init?.cacheTtlMs ?? (method === "GET" ? 4_000 : 0);

  const bodyKey = typeof init?.body === "string" ? init?.body : undefined;
  const key = cacheKey(method, path, bodyKey);

  // GET cache
  if (method === "GET" && ttl > 0) {
    const hit = memCache.get(key);
    if (hit && Date.now() - hit.at < ttl) return hit.value as T;
  }

  // GET inflight dedupe
  if (method === "GET") {
    const pending = inflight.get(key);
    if (pending) return (await pending) as T;
  }

  const run = (async () => {
    const res = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: {
        "Content-Type": "application/json",
        "X-Org-Slug": auth.orgSlug,
        "X-User-Email": auth.userEmail,
        ...(auth.userRole ? { "X-User-Role": auth.userRole } : {}),
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

/**
 * Helper: request that MUST be an array (normalize always).
 * This is the main fix for "n.map is not a function".
 */
async function requestArray<T = any>(
  path: string,
  init?:
    | (RequestInit & { cacheTtlMs?: number; signal?: AbortSignal })
    | undefined,
): Promise<T[]> {
  const data = await request<any>(path, init);
  return asArray<T>(data);
}

export const api = {
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

  // Rent pipeline actions (Phase 3)
  enrichProperty: (propertyId: number, strategy: string = "section8") =>
    request<any>(
      `/rent/enrich?property_id=${propertyId}&strategy=${encodeURIComponent(strategy)}`,
      { method: "POST", body: JSON.stringify({}) },
    ),

  explainProperty: (
    propertyId: number,
    strategy: string = "section8",
    persist: boolean = true,
  ) =>
    request<any>(
      `/rent/explain?property_id=${propertyId}&strategy=${encodeURIComponent(
        strategy,
      )}&persist=${persist ? "true" : "false"}`,
      { method: "POST", body: JSON.stringify({}) },
    ),

  evaluateProperty: (propertyId: number, strategy: string = "section8") =>
    request<any>(
      `/evaluate/property/${propertyId}?strategy=${encodeURIComponent(strategy)}`,
      { method: "POST", body: JSON.stringify({}) },
    ),

  // Checklist / Compliance (matches your backend)
  checklistLatest: (propertyId: number, signal?: AbortSignal) =>
    request<any>(`/compliance/checklist/${propertyId}/latest`, {
      cacheTtlMs: 1_000,
      signal,
    }),

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
    requestArray<any>(`/rehab/tasks?property_id=${propertyId}&limit=500`, {
      cacheTtlMs: 2_000,
      signal,
    }),

  createRehabTask: (payload: any) =>
    request<any>(`/rehab/tasks`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  // Tenants / leases
  leases: (propertyId: number, signal?: AbortSignal) =>
    requestArray<any>(`/tenants/leases?property_id=${propertyId}&limit=200`, {
      cacheTtlMs: 2_000,
      signal,
    }),

  // Cash
  txns: (propertyId: number, signal?: AbortSignal) =>
    requestArray<any>(
      `/cash/transactions?property_id=${propertyId}&limit=1000`,
      {
        cacheTtlMs: 2_000,
        signal,
      },
    ),

  // Equity
  valuations: (propertyId: number, signal?: AbortSignal) =>
    requestArray<any>(
      `/equity/valuations?property_id=${propertyId}&limit=200`,
      {
        cacheTtlMs: 2_000,
        signal,
      },
    ),

  // Agents
  agents: () => requestArray<any>(`/agents`, { cacheTtlMs: 4_000 }),

  agentRuns: (propertyId: number) =>
    requestArray<any>(`/agents/runs?property_id=${propertyId}&limit=200`, {
      cacheTtlMs: 2_000,
    }),

  createAgentRun: (payload: any) =>
    request<any>(`/agents/runs`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  // Messages
  messages: (threadKey: string) =>
    requestArray<any>(
      `/agents/messages?thread_key=${encodeURIComponent(threadKey)}&limit=200`,
      { cacheTtlMs: 500 },
    ),

  postMessage: (payload: {
    thread_key: string;
    sender: string;
    message: string;
    recipient?: string;
  }) =>
    request<any>(`/agents/messages`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  // Slot Specs + Assignments
  slotSpecs: () =>
    requestArray<any>(`/agents/slots/specs`, { cacheTtlMs: 10_000 }),

  slotAssignments: (propertyId?: number, signal?: AbortSignal) => {
    const q =
      propertyId != null
        ? `?property_id=${propertyId}&limit=200`
        : `?limit=200`;
    return requestArray<any>(`/agents/slots/assignments${q}`, {
      cacheTtlMs: 2_000,
      signal,
    });
  },

  upsertSlotAssignment: (payload: {
    slot_key: string;
    property_id?: number | null;
    owner_type?: string | null;
    assignee?: string | null;
    status?: string | null;
    notes?: string | null;
  }) =>
    request<any>(`/agents/slots/assignments`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
};
