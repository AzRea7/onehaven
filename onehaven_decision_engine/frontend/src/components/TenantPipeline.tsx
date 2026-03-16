import React from "react";

function money(v: any) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `$${Math.round(Number(v)).toLocaleString()}`;
}

function toneClass(kind: "good" | "warn" | "bad" | "neutral" = "neutral") {
  if (kind === "good")
    return "border-green-400/20 bg-green-400/10 text-green-200";
  if (kind === "warn")
    return "border-yellow-300/20 bg-yellow-300/10 text-yellow-100";
  if (kind === "bad") return "border-red-400/20 bg-red-400/10 text-red-200";
  return "border-white/10 bg-white/[0.03] text-white/80";
}

function statusTone(status?: string) {
  const s = (status || "").toLowerCase();
  if (s === "active" || s === "occupied") return "good";
  if (s === "upcoming" || s === "leased_not_started") return "warn";
  if (s === "ended" || s === "vacant") return "bad";
  return "neutral";
}

export default function TenantPipeline({
  tenants = [],
  leases = [],
  opsTenant = null,
}: {
  tenants?: any[];
  leases?: any[];
  opsTenant?: any;
}) {
  const tenantMap = React.useMemo(() => {
    const m = new Map<number, any>();
    for (const t of tenants || []) {
      if (t?.id != null) m.set(Number(t.id), t);
    }
    return m;
  }, [tenants]);

  const rows = React.useMemo(() => {
    return (leases || []).map((lease: any) => {
      const tenant = tenantMap.get(Number(lease?.tenant_id));
      const now = Date.now();
      const startTs = lease?.start_date
        ? new Date(lease.start_date).getTime()
        : null;
      const endTs = lease?.end_date ? new Date(lease.end_date).getTime() : null;

      let status = "active";
      if (startTs && startTs > now) status = "upcoming";
      else if (endTs && endTs < now) status = "ended";

      const totalRent = Number(lease?.total_rent || 0);
      const tenantPortion =
        lease?.tenant_portion != null ? Number(lease.tenant_portion) : null;
      const hapPortion =
        lease?.housing_authority_portion != null
          ? Number(lease.housing_authority_portion)
          : null;

      return {
        ...lease,
        status,
        tenant_name: tenant?.full_name || `Tenant #${lease?.tenant_id ?? "—"}`,
        tenant_email: tenant?.email || null,
        voucher_status: tenant?.voucher_status || null,
        total_rent: totalRent,
        tenant_portion: tenantPortion,
        housing_authority_portion: hapPortion,
      };
    });
  }, [leases, tenantMap]);

  const active = rows.filter((x) => x.status === "active");
  const upcoming = rows.filter((x) => x.status === "upcoming");
  const ended = rows.filter((x) => x.status === "ended");

  const occupancyStatus =
    opsTenant?.occupancy_status ||
    (active.length > 0
      ? "occupied"
      : upcoming.length > 0
        ? "leased_not_started"
        : "vacant");

  return (
    <div className="space-y-4">
      <div className="oh-panel p-5">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div>
            <div className="text-sm font-semibold text-white">
              Tenant / Lease Pipeline
            </div>
            <div className="text-xs text-white/50 mt-1">
              Occupancy, lease timing, and voucher mix in one place.
            </div>
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            <span
              className={`text-[11px] px-2 py-1 rounded-full border ${toneClass(statusTone(occupancyStatus) as any)}`}
            >
              {occupancyStatus}
            </span>
            <span className="text-[11px] px-2 py-1 rounded-full border border-white/10 bg-white/[0.03] text-white/80">
              active {active.length}
            </span>
            <span className="text-[11px] px-2 py-1 rounded-full border border-white/10 bg-white/[0.03] text-white/80">
              upcoming {upcoming.length}
            </span>
            <span className="text-[11px] px-2 py-1 rounded-full border border-white/10 bg-white/[0.03] text-white/80">
              ended {ended.length}
            </span>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-1 md:grid-cols-4 gap-3">
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-wider text-white/45">
              Occupancy
            </div>
            <div className="mt-2 text-sm text-white/90">{occupancyStatus}</div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-wider text-white/45">
              Tenants
            </div>
            <div className="mt-2 text-sm text-white/90">{tenants.length}</div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-wider text-white/45">
              Leases
            </div>
            <div className="mt-2 text-sm text-white/90">{leases.length}</div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="text-[11px] uppercase tracking-wider text-white/45">
              Active Rent
            </div>
            <div className="mt-2 text-sm text-white/90">
              {active.length > 0 ? money(active[0]?.total_rent) : "—"}
            </div>
          </div>
        </div>
      </div>

      <div className="oh-panel p-5">
        <div className="text-sm font-semibold text-white">Lease Timeline</div>
        <div className="mt-4 space-y-3">
          {rows.length === 0 ? (
            <div className="text-sm text-white/55">No leases yet.</div>
          ) : (
            rows.map((row: any) => (
              <div
                key={row.id}
                className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"
              >
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <div>
                    <div className="text-sm font-semibold text-white">
                      {row.tenant_name}
                    </div>
                    <div className="text-xs text-white/55 mt-1">
                      {row.tenant_email || "No email on file"}
                      {row.voucher_status
                        ? ` · voucher: ${row.voucher_status}`
                        : ""}
                    </div>
                  </div>

                  <div className="flex items-center gap-2 flex-wrap">
                    <span
                      className={`text-[11px] px-2 py-1 rounded-full border ${toneClass(
                        statusTone(row.status) as any,
                      )}`}
                    >
                      {row.status}
                    </span>
                    <span className="text-sm text-white/90 font-semibold">
                      {money(row.total_rent)}
                    </span>
                  </div>
                </div>

                <div className="mt-3 grid grid-cols-1 md:grid-cols-4 gap-3 text-sm">
                  <div>
                    <div className="text-white/45 text-[11px] uppercase tracking-wider">
                      Start
                    </div>
                    <div className="text-white/85 mt-1">
                      {row.start_date
                        ? new Date(row.start_date).toLocaleDateString()
                        : "—"}
                    </div>
                  </div>
                  <div>
                    <div className="text-white/45 text-[11px] uppercase tracking-wider">
                      End
                    </div>
                    <div className="text-white/85 mt-1">
                      {row.end_date
                        ? new Date(row.end_date).toLocaleDateString()
                        : "Open"}
                    </div>
                  </div>
                  <div>
                    <div className="text-white/45 text-[11px] uppercase tracking-wider">
                      Tenant Portion
                    </div>
                    <div className="text-white/85 mt-1">
                      {row.tenant_portion != null
                        ? money(row.tenant_portion)
                        : "—"}
                    </div>
                  </div>
                  <div>
                    <div className="text-white/45 text-[11px] uppercase tracking-wider">
                      HAP Portion
                    </div>
                    <div className="text-white/85 mt-1">
                      {row.housing_authority_portion != null
                        ? money(row.housing_authority_portion)
                        : "—"}
                    </div>
                  </div>
                </div>

                {row.notes ? (
                  <div className="text-sm text-white/65 mt-3">{row.notes}</div>
                ) : null}
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
