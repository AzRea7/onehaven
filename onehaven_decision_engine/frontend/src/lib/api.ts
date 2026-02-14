// frontend/src/lib/api.ts
export const API_BASE = (import.meta as any).env?.VITE_API_BASE || "/api";

type AuthContext = {
  orgSlug: string;
  userEmail: string;
  userRole?: string;
};

function getAuth(): AuthContext {
  // Prefer env vars (good for dev / single-tenant demo)
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

  // Fallback to localStorage (good when you add a login/settings UI)
  const orgSlug = localStorage.getItem("org_slug") || "demo";
  const userEmail = localStorage.getItem("user_email") || "austin@demo.local";
  const userRole = localStorage.getItem("user_role") || "owner";

  return { orgSlug, userEmail, userRole };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const auth = getAuth();

  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      "X-Org-Slug": auth.orgSlug,
      "X-User-Email": auth.userEmail,
      ...(auth.userRole ? { "X-User-Role": auth.userRole } : {}),
      ...(init?.headers || {}),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }

  // Some endpoints might return empty responses; guard just in case
  const ct = res.headers.get("content-type") || "";
  if (!ct.includes("application/json")) {
    return (await res.text()) as unknown as T;
  }

  return (await res.json()) as T;
}

export const api = {
  // Dashboard / properties
  dashboardProperties: (_p0: { limit: number }) =>
    request<any[]>(`/dashboard/properties?limit=100`),
  propertyView: (id: number) => request<any>(`/properties/${id}/view`),

  // Rehab
  rehabTasks: (propertyId: number) =>
    request<any[]>(`/rehab/tasks?property_id=${propertyId}&limit=500`),
  createRehabTask: (payload: any) =>
    request<any>(`/rehab/tasks`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  // Tenants / leases
  leases: (propertyId: number) =>
    request<any[]>(`/tenants/leases?property_id=${propertyId}&limit=200`),

  // Cash
  txns: (propertyId: number) =>
    request<any[]>(`/cash/transactions?property_id=${propertyId}&limit=1000`),

  // Equity
  valuations: (propertyId: number) =>
    request<any[]>(`/equity/valuations?property_id=${propertyId}&limit=200`),

  // Agents
  agents: () => request<any[]>(`/agents`),
  agentRuns: (propertyId: number) =>
    request<any[]>(`/agents/runs?property_id=${propertyId}&limit=200`),
  createAgentRun: (payload: any) =>
    request<any>(`/agents/runs`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  // Messages
  messages: (threadKey: string) =>
    request<any[]>(
      `/agents/messages?thread_key=${encodeURIComponent(threadKey)}&limit=200`,
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
  slotSpecs: () => request<any[]>(`/agents/slots/specs`),
  slotAssignments: (propertyId?: number) => {
    const q =
      propertyId != null
        ? `?property_id=${propertyId}&limit=200`
        : `?limit=200`;
    return request<any[]>(`/agents/slots/assignments${q}`);
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
