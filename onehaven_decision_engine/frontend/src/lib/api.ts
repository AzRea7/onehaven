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

async function requestArray<T = any>(
  path: string,
  init?:
    | (RequestInit & { cacheTtlMs?: number; signal?: AbortSignal })
    | undefined,
): Promise<T[]> {
  const data = await request<any>(path, init);
  return asArray<T>(data);
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

  // Deal creation (Phase 1) - existing
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

  // ✅ Deal Intake (Phase 1) - used by DealIntake.tsx
  // Your backend typically has an intake endpoint like /intake/deal or /intake.
  // This version tries /intake/deal first; if your backend uses a different path, adjust here.
  intakeDeal: (payload: any) =>
    request<any>(`/intake/deal`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  // Rent pipeline actions (Phase 3)
  enrichProperty: (propertyId: number, strategy: string = "section8") =>
    request<any>(`/rent/enrich${qs({ property_id: propertyId, strategy })}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  // ✅ FIXED: backend route is GET /rent/explain/{property_id}
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

  // ✅ Evaluate snapshot/run results already use POST /evaluate/run or /evaluate/snapshot
  // Your previous api.ts had /evaluate/property/{id}; your backend excerpt shows /evaluate/run and /evaluate/results.
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

  // ✅ Checklist / Compliance
  checklistLatest: (propertyId: number, signal?: AbortSignal) =>
    request<any>(`/compliance/checklist/${propertyId}/latest`, {
      cacheTtlMs: 1_000,
      signal,
    }),

  // ✅ Generate checklist (persisted by default)
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
      {
        cacheTtlMs: 2_000,
        signal,
      },
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
      {
        cacheTtlMs: 2_000,
        signal,
      },
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

  agentRuns: (propertyId: number) =>
    requestArray<any>(
      `/agents/runs${qs({ property_id: propertyId, limit: 200 })}`,
      {
        cacheTtlMs: 2_000,
      },
    ),

  createAgentRun: (payload: any) =>
    request<any>(`/agents/runs`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  // Messages
  messages: (threadKey: string) =>
    requestArray<any>(
      `/agents/messages${qs({ thread_key: threadKey, limit: 200 })}`,
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

  // --------------------------
  // ✅ Phase 2 - Jurisdictions
  // (matches your backend jurisdictions.py routes)
  // --------------------------

  // List rules; includeGlobal=true => scope=all
  listJurisdictionRules: (includeGlobal: boolean, state: string = "MI") => {
    const scope = includeGlobal ? "all" : "org";
    return requestArray<any>(`/jurisdictions/rules${qs({ scope, state })}`, {
      cacheTtlMs: 2_000,
    });
  },

  // Seed baseline defaults (your backend file didn’t show a seed route,
  // so we provide a compatible call to a common seed endpoint.
  // If you add /jurisdictions/seed, keep this.
  seedJurisdictionDefaults: () =>
    request<any>(`/jurisdictions/seed`, {
      method: "POST",
      body: JSON.stringify({}),
    }),

  // Create org override via the existing upsert endpoint
  createJurisdictionRule: (payload: any) =>
    request<any>(`/jurisdictions/rule${qs({ scope: "org" })}`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  // Delete org override via existing delete endpoint (needs city/state)
  deleteJurisdictionRule: (idOrPayload: any) => {
    // Your UI passes id, but backend delete uses city/state.
    // So: if caller gives a number, they must supply city/state by looking it up.
    // We keep it robust by requiring payload {city,state} OR passing rule object.
    if (typeof idOrPayload === "number") {
      throw new Error(
        "deleteJurisdictionRule requires {city, state} or a rule object; UI should pass the rule.",
      );
    }
    const city = idOrPayload.city;
    const state = idOrPayload.state || "MI";
    if (!city) throw new Error("deleteJurisdictionRule missing city");
    return request<any>(
      `/jurisdictions/rule${qs({ city, state, scope: "org" })}`,
      {
        method: "DELETE",
      },
    );
  },
};
