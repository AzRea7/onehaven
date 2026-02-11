export const API_BASE =
  (import.meta as any).env?.VITE_API_BASE || "http://localhost:8000";

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
  dashboard: () => request<any[]>(`/dashboard/properties?limit=100`),
  propertyView: (id: number) => request<any>(`/properties/${id}/view`),

  rehabTasks: (propertyId: number) =>
    request<any[]>(`/rehab/tasks?property_id=${propertyId}&limit=500`),
  createRehabTask: (payload: any) =>
    request<any>(`/rehab/tasks`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  leases: (propertyId: number) =>
    request<any[]>(`/tenants/leases?property_id=${propertyId}&limit=200`),
  txns: (propertyId: number) =>
    request<any[]>(`/cash/transactions?property_id=${propertyId}&limit=1000`),
  valuations: (propertyId: number) =>
    request<any[]>(`/equity/valuations?property_id=${propertyId}&limit=200`),

  agents: () => request<any[]>(`/agents`),
  agentRuns: (propertyId: number) =>
    request<any[]>(`/agents/runs?property_id=${propertyId}&limit=200`),
  createAgentRun: (payload: any) =>
    request<any>(`/agents/runs`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  messages: (threadKey: string) =>
    request<any[]>(
      `/agents/messages?thread_key=${encodeURIComponent(threadKey)}&limit=200`,
    ),
  postMessage: (payload: any) =>
    request<any>(`/agents/messages`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
};
