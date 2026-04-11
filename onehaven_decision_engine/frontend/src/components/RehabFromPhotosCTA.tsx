import React from "react";
import { Camera, ShieldAlert, Sparkles } from "lucide-react";
import Surface from "./Surface";
import EmptyState from "./EmptyState";

export default function RehabFromPhotosCTA({
  propertyId,
  selectedFindingCodes = [],
}: {
  propertyId?: number;
  selectedFindingCodes?: string[];
}) {
  return (
    <Surface
      title="Inspection findings from photos"
      subtitle="Use confirmed photo findings as inputs for remediation and follow-up task creation."
    >
      <div className="space-y-3">
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
              Property
            </div>
            <div className="mt-2 text-xl font-semibold text-app-0">
              {propertyId ?? "—"}
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
              Selected findings
            </div>
            <div className="mt-2 text-xl font-semibold text-app-0">
              {selectedFindingCodes.length}
            </div>
          </div>

          <div className="rounded-2xl border border-app bg-app-muted px-4 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
              Status
            </div>
            <div className="mt-2 text-xl font-semibold text-app-0">
              {selectedFindingCodes.length ? "Ready" : "Waiting"}
            </div>
          </div>
        </div>

        {!propertyId ? (
          <EmptyState
            compact
            icon={Sparkles}
            title="No property selected"
            description="Select a property first to connect photo findings to compliance work."
          />
        ) : !selectedFindingCodes.length ? (
          <EmptyState
            compact
            icon={Camera}
            title="No findings selected"
            description="Preview photo findings and select at least one confirmed finding to continue."
          />
        ) : (
          <div className="grid gap-2 md:grid-cols-2">
            {selectedFindingCodes.slice(0, 6).map((code, i) => (
              <div
                key={`${code}-${i}`}
                className="rounded-2xl border border-app bg-app-panel px-4 py-3"
              >
                <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                  <ShieldAlert className="h-4 w-4 text-app-4" />
                  {code}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </Surface>
  );
}
