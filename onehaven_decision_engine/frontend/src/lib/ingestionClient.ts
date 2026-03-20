import { API_BASE } from "./api";

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
  daily_markets?: Array<Record<string, any>>;
  ui_mode?: string;
  normal_path?: string | boolean;
  legacy_snapshot_flow_enabled?: boolean;
};

export type IngestionSource = {
  id: number;
  provider: string;
  slug: string;
  display_name: string;
  source_type: string;
  status: string;
  is_enabled: boolean;
  sync_interval_minutes?: number | null;
  config_json?: Record<string, any> | null;
  credentials_json?: Record<string, any> | null;
  last_synced_at?: string | null;
  last_success_at?: string | null;
  last_failure_at?: string | null;
  next_scheduled_at?: string | null;
  last_error_summary?: string | null;
};

export type PipelineOutcome = {
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
  filtered_out: number;
  location_automation_enabled?: boolean;
  enrichments_completed?: {
    geo: number;
    risk: number;
    rent: number;
  };
  evaluations_completed?: number;
  workflow?: {
    state_synced: number;
    workflow_synced: number;
    next_actions_seeded: number;
  };
  failures?: number;
  partials?: number;
  errors?: Array<Record<string, any>>;
  filter_reason_counts?: Record<string, number>;
  normal_path?: boolean;
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
  summary_json?: Record<string, any> | null;
  pipeline_outcome?: PipelineOutcome;
};

export type IngestionLaunchPayload = {
  trigger_type?: "manual" | "scheduled" | "webhook" | "daily_refresh";
  state?: string;
  county?: string;
  city?: string;
  zip_code?: string;
  zip_codes?: string[];
  min_price?: number;
  max_price?: number;
  min_bedrooms?: number;
  min_bathrooms?: number;
  property_type?: string;
  price_buckets?: number[][];
  pages_per_shard?: number;
  limit?: number;
  execute_inline?: boolean;
};

export type SyncSourceResponse = {
  ok: boolean;
  queued: boolean;
  task_id?: string;
  run_id?: number;
  status?: string;
  source_id: number;
  summary_json?: Record<string, any>;
  pipeline_outcome?: PipelineOutcome;
  runtime_config?: Record<string, any>;
  normal_path?: boolean;
};

export type SyncDefaultSourcesResponse = {
  ok: boolean;
  queued?: number | boolean;
  source_ids?: number[];
  runs?: Array<{
    source_id: number;
    run_id?: number;
    status?: string;
    summary_json?: Record<string, any>;
    pipeline_outcome?: PipelineOutcome;
    normal_path?: boolean;
  }>;
  normal_path?: boolean;
};

function readOrgSlug(): string {
  const env = (import.meta as any).env || {};
  const envOrg = (env.VITE_ORG_SLUG as string | undefined)?.trim();
  if (envOrg) return envOrg;

  try {
    return (window.localStorage.getItem("org_slug") || "").trim();
  } catch {
    return "";
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers || {});
  headers.set("Content-Type", "application/json");

  const orgSlug = readOrgSlug();
  if (orgSlug) headers.set("X-Org-Slug", orgSlug);

  const res = await fetch(`${API_BASE}${path}`, {
    credentials: "include",
    ...init,
    headers,
  });

  if (!res.ok) {
    let detail = `Request failed (${res.status})`;
    try {
      const body = await res.json();
      detail = body?.detail || body?.message || detail;
    } catch {
      // noop
    }
    throw new Error(detail);
  }

  return (await res.json()) as T;
}

export const ingestionClient = {
  overview() {
    return request<IngestionOverview>("/ingestion/overview");
  },

  listSources() {
    return request<IngestionSource[]>("/ingestion/sources");
  },

  updateSource(id: number, payload: Partial<IngestionSource>) {
    return request<IngestionSource>(`/ingestion/sources/${id}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
  },

  listRuns(limit = 25) {
    return request<IngestionRun[]>(`/ingestion/runs?limit=${limit}`);
  },

  syncSource(sourceId: number, payload: IngestionLaunchPayload = {}) {
    return request<SyncSourceResponse>(`/ingestion/sources/${sourceId}/sync`, {
      method: "POST",
      body: JSON.stringify({
        trigger_type: payload.trigger_type ?? "manual",
        state: payload.state ?? "MI",
        county: payload.county ?? undefined,
        city: payload.city ?? undefined,
        zip_code: payload.zip_code ?? undefined,
        zip_codes: payload.zip_codes ?? undefined,
        min_price: payload.min_price ?? undefined,
        max_price: payload.max_price ?? undefined,
        min_bedrooms: payload.min_bedrooms ?? undefined,
        min_bathrooms: payload.min_bathrooms ?? undefined,
        property_type: payload.property_type ?? undefined,
        price_buckets: payload.price_buckets ?? undefined,
        pages_per_shard: payload.pages_per_shard ?? 1,
        limit: payload.limit ?? 100,
        execute_inline: payload.execute_inline ?? false,
      }),
    });
  },

  syncDefaultSources(payload: IngestionLaunchPayload = {}) {
    return request<SyncDefaultSourcesResponse>("/ingestion/sync-defaults", {
      method: "POST",
      body: JSON.stringify({
        trigger_type: payload.trigger_type ?? "manual",
        state: payload.state ?? "MI",
        county: payload.county ?? undefined,
        city: payload.city ?? undefined,
        zip_code: payload.zip_code ?? undefined,
        zip_codes: payload.zip_codes ?? undefined,
        min_price: payload.min_price ?? undefined,
        max_price: payload.max_price ?? undefined,
        min_bedrooms: payload.min_bedrooms ?? undefined,
        min_bathrooms: payload.min_bathrooms ?? undefined,
        property_type: payload.property_type ?? undefined,
        price_buckets: payload.price_buckets ?? undefined,
        pages_per_shard: payload.pages_per_shard ?? 1,
        limit: payload.limit ?? 100,
        execute_inline: payload.execute_inline ?? false,
      }),
    });
  },

  queueDailyRefresh() {
    return request<{
      ok: boolean;
      queued: boolean;
      task_id?: string;
      markets?: Array<Record<string, any>>;
    }>("/ingestion/daily-refresh", {
      method: "POST",
      body: JSON.stringify({}),
    });
  },

  runDetail(runId: number) {
    return request<{
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
      summary_json?: Record<string, any>;
      pipeline_outcome?: PipelineOutcome;
      normal_path?: boolean;
    }>(`/ingestion/runs/${runId}`);
  },
};
