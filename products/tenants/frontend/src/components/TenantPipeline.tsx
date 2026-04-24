import React from "react";
import { Building2, KeyRound, UserRound } from "lucide-react";
import Surface from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Surface";
import EmptyState from "packages/ui/onehaven_onehaven_platform/frontend/src/components/EmptyState";

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

export default function TenantPipeline({
  tenants,
  leases,
  opsTenant,
}: {
  tenants?: any[];
  leases?: any[];
  opsTenant?: any;
}) {
  const tenantRows = Array.isArray(tenants) ? tenants : [];
  const leaseRows = Array.isArray(leases) ? leases : [];

  return (
    <Surface
      title="Tenant pipeline"
      subtitle="Placement, occupancy, and lease posture in one view."
      actions={
        opsTenant?.occupancy_status ? (
          <span className="oh-pill">{opsTenant.occupancy_status}</span>
        ) : null
      }
    >
      <div className="grid gap-3 md:grid-cols-3">
        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Tenants
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {tenantRows.length}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Active leases
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {opsTenant?.active_lease_count ?? leaseRows.length}
          </div>
        </div>

        <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Occupancy
          </div>
          <div className="mt-2 text-xl font-semibold text-app-0">
            {opsTenant?.occupancy_status || "—"}
          </div>
        </div>
      </div>

      {!tenantRows.length && !leaseRows.length ? (
        <div className="mt-4">
          <EmptyState
            compact
            title="No tenant pipeline activity yet"
            description="Once screening, placement, or lease records exist, they show up here."
          />
        </div>
      ) : (
        <div className="mt-4 grid gap-4 lg:grid-cols-2">
          <div className="space-y-2">
            <div className="text-sm font-semibold text-app-0">Tenants</div>
            {!tenantRows.length ? (
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3 text-sm text-app-4">
                No tenant records yet.
              </div>
            ) : (
              tenantRows.map((tenant: any, i: number) => (
                <div
                  key={tenant?.id || i}
                  className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                >
                  <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                    <UserRound className="h-4 w-4 text-app-4" />
                    {tenant?.full_name ||
                      tenant?.name ||
                      `Tenant #${tenant?.id || i + 1}`}
                  </div>
                  <div className="mt-1 text-xs text-app-4">
                    {tenant?.email || "No email"}
                    {tenant?.phone ? ` · ${tenant.phone}` : ""}
                  </div>
                </div>
              ))
            )}
          </div>

          <div className="space-y-2">
            <div className="text-sm font-semibold text-app-0">Leases</div>
            {!leaseRows.length ? (
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3 text-sm text-app-4">
                No lease records yet.
              </div>
            ) : (
              leaseRows.map((lease: any, i: number) => (
                <div
                  key={lease?.id || i}
                  className="rounded-2xl border border-app bg-app-panel px-4 py-3"
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                      <KeyRound className="h-4 w-4 text-app-4" />
                      Lease #{lease?.id || i + 1}
                    </div>
                    <div className="text-sm font-semibold text-app-1">
                      {money(lease?.total_rent || lease?.rent)}
                    </div>
                  </div>
                  <div className="mt-1 text-xs text-app-4">
                    {lease?.start_date
                      ? new Date(lease.start_date).toLocaleDateString()
                      : "—"}
                    {lease?.end_date
                      ? ` → ${new Date(lease.end_date).toLocaleDateString()}`
                      : ""}
                    {lease?.hap_contract_status
                      ? ` · HAP ${lease.hap_contract_status}`
                      : ""}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      )}

      {opsTenant?.occupancy_status ? (
        <div className="mt-4 flex items-center gap-2 text-xs text-app-4">
          <Building2 className="h-3.5 w-3.5" />
          occupancy status is derived from the ops summary, not vibes
        </div>
      ) : null}
    </Surface>
  );
}
