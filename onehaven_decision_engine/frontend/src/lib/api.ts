// frontend/src/lib/api.ts
export const API_BASE = (import.meta as any).env?.VITE_API_BASE || "/api";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }

  return (await res.json()) as T;
}

export const api = {
  // Dashboard / properties
  dashboard: () => request<any[]>(`/dashboard/properties?limit=100`),
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

  // Agent messages (this is what Property.tsx expects)
  messages: (threadKey: string) =>
    request<any[]>(
      `/agents/messages?thread_key=${encodeURIComponent(threadKey)}&limit=200`,
    ),

  postMessage: (payload: {
    thread_key: string;
    sender: string;
    message: string;
    recipient?: string; // <-- add this
  }) =>
    request<any>(`/agents/messages`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
};
