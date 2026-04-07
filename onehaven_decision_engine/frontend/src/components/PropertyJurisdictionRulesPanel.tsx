import React from "react";
import {
  AlertTriangle,
  FileSearch,
  FolderTree,
  Layers3,
  Link2,
  MapPin,
  ShieldCheck,
} from "lucide-react";
import Surface from "./Surface";
import EmptyState from "./EmptyState";
import JurisdictionCoverageBadge from "./JurisdictionCoverageBadge";

type RuleLayer = {
  layer?: string | null;
  scope?: string | null;
  label?: string | null;
  source?: string | null;
  version?: string | null;
  matched?: boolean | null;
  applied?: boolean | null;
  authority?: string | null;
  confidence?: string | null;
};

type EvidenceRow = {
  label?: string | null;
  title?: string | null;
  url?: string | null;
  source?: string | null;
  source_name?: string | null;
  excerpt?: string | null;
  fetched_at?: string | null;
  is_authoritative?: boolean | null;
};

type ResolvedProfile = {
  scope?: string | null;
  state?: string | null;
  county?: string | null;
  city?: string | null;
  pha_name?: string | null;
  resolved_rule_version?: string | null;
  rule_version?: string | null;
  last_refreshed?: string | null;
  last_refreshed_at?: string | null;
  completeness_status?: string | null;
  completeness_score?: number | null;
  coverage_confidence?: string | null;
  confidence_label?: string | null;
  production_readiness?: string | null;
  is_stale?: boolean | null;
  stale_warning?: boolean | null;
  stale_reason?: string | null;
  required_categories?: string[] | null;
  covered_categories?: string[] | null;
  missing_categories?: string[] | null;
  source_evidence?: EvidenceRow[] | null;
  evidence?: EvidenceRow[] | null;
  evidence_rows?: EvidenceRow[] | null;
  layers?: RuleLayer[] | null;
  resolved_layers?: RuleLayer[] | null;
};

function toArray<T = any>(v: any): T[] {
  return Array.isArray(v) ? v : [];
}

function norm(v: any) {
  return String(v ?? "")
    .trim()
    .toLowerCase();
}

function titleize(v: any) {
  return String(v ?? "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function formatDate(value: unknown) {
  if (!value) return "—";
  const d = new Date(String(value));
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

function rowTone(v: any) {
  const s = norm(v);
  if (["applied", "matched", "yes", "true", "high", "verified"].includes(s)) {
    return "oh-pill oh-pill-good";
  }
  if (["partial", "medium", "unknown", "review"].includes(s)) {
    return "oh-pill oh-pill-warn";
  }
  if (["missing", "low", "false", "stale"].includes(s)) {
    return "oh-pill oh-pill-bad";
  }
  return "oh-pill";
}

function LayerCard({ row }: { row: RuleLayer }) {
  const layer = row.layer || row.scope || row.label || "layer";
  const applied = row.applied ?? row.matched ?? false;
  return (
    <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm font-semibold text-app-0">
          {titleize(layer)}
        </div>
        <span className={applied ? "oh-pill oh-pill-good" : "oh-pill"}>
          {applied ? "Applied" : "Available"}
        </span>
      </div>

      <div className="mt-3 grid gap-2 text-sm text-app-2">
        <div>
          <span className="text-app-4">Authority:</span>{" "}
          {row.authority || row.source || "—"}
        </div>
        <div>
          <span className="text-app-4">Version:</span> {row.version || "—"}
        </div>
        <div>
          <span className="text-app-4">Confidence:</span>{" "}
          <span className={rowTone(row.confidence)}>
            {titleize(row.confidence || "unknown")}
          </span>
        </div>
      </div>
    </div>
  );
}

export default function PropertyJurisdictionRulesPanel({
  profile,
}: {
  profile?: ResolvedProfile | null;
}) {
  const p = profile || {};
  const layers = toArray<RuleLayer>(p.resolved_layers || p.layers);
  const evidence = toArray<EvidenceRow>(
    p.source_evidence || p.evidence || p.evidence_rows,
  );
  const required = toArray<string>(p.required_categories);
  const covered = toArray<string>(p.covered_categories);
  const missing = toArray<string>(p.missing_categories);

  const locationBits = [p.city, p.county, p.state].filter(Boolean).join(", ");

  return (
    <Surface
      title="Jurisdiction rule overlays"
      subtitle="Resolved local compliance layers, coverage confidence, missing rule areas, and evidence."
    >
      {!profile ? (
        <EmptyState
          title="No jurisdiction profile loaded"
          description="Select a property or load its compliance brief to see the resolved layered rules."
        />
      ) : (
        <div className="space-y-4">
          <JurisdictionCoverageBadge coverage={p} />

          <div className="grid gap-3 md:grid-cols-2">
            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <MapPin className="h-4 w-4" />
                Resolved market
              </div>
              <div className="mt-3 space-y-2 text-sm text-app-2">
                <div>
                  <span className="text-app-4">Location:</span>{" "}
                  {locationBits || "—"}
                </div>
                <div>
                  <span className="text-app-4">PHA / overlay:</span>{" "}
                  {p.pha_name || "—"}
                </div>
                <div>
                  <span className="text-app-4">Resolved version:</span>{" "}
                  {p.resolved_rule_version || p.rule_version || "—"}
                </div>
                <div>
                  <span className="text-app-4">Last refreshed:</span>{" "}
                  {formatDate(p.last_refreshed || p.last_refreshed_at)}
                </div>
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-muted px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <ShieldCheck className="h-4 w-4" />
                Rule area coverage
              </div>

              <div className="mt-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Covered categories
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {covered.length ? (
                    covered.map((item) => (
                      <span key={item} className="oh-pill oh-pill-good">
                        {titleize(item)}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-app-4">None yet</span>
                  )}
                </div>
              </div>

              <div className="mt-4">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Missing local rule areas
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {missing.length ? (
                    missing.map((item) => (
                      <span key={item} className="oh-pill oh-pill-warn">
                        {titleize(item)}
                      </span>
                    ))
                  ) : (
                    <span className="oh-pill oh-pill-good">No known gaps</span>
                  )}
                </div>
              </div>

              {required.length ? (
                <div className="mt-4">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                    Required review areas
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {required.map((item) => (
                      <span key={item} className="oh-pill">
                        {titleize(item)}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          </div>

          {p.is_stale || p.stale_warning ? (
            <div className="rounded-2xl border border-amber-400/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-50/90">
              <div className="flex items-start gap-2">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                <div>
                  <div className="font-semibold text-amber-100">
                    Stale jurisdiction warning
                  </div>
                  <div className="mt-1">
                    {p.stale_reason || "This market needs a freshness review."}
                  </div>
                </div>
              </div>
            </div>
          ) : null}

          <div>
            <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-app-0">
              <FolderTree className="h-4 w-4" />
              Applied rule layers
            </div>
            {layers.length ? (
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {layers.map((row, idx) => (
                  <LayerCard
                    key={`${row.layer || row.scope || "layer"}-${idx}`}
                    row={row}
                  />
                ))}
              </div>
            ) : (
              <EmptyState
                title="No explicit layers returned"
                description="The backend did not provide a resolved layers list for this property yet."
              />
            )}
          </div>

          <div>
            <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-app-0">
              <FileSearch className="h-4 w-4" />
              Source evidence
            </div>

            {evidence.length ? (
              <div className="space-y-3">
                {evidence.map((row, idx) => (
                  <div
                    key={`${row.title || row.label || row.url || "evidence"}-${idx}`}
                    className="rounded-2xl border border-app bg-app-muted px-4 py-4"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="text-sm font-semibold text-app-0">
                        {row.title ||
                          row.label ||
                          row.source_name ||
                          "Evidence"}
                      </div>
                      {row.is_authoritative ? (
                        <span className="oh-pill oh-pill-good">
                          Authoritative
                        </span>
                      ) : (
                        <span className="oh-pill">Supporting</span>
                      )}
                    </div>

                    <div className="mt-3 space-y-2 text-sm text-app-2">
                      <div>
                        <span className="text-app-4">Source:</span>{" "}
                        {row.source_name || row.source || "—"}
                      </div>
                      <div>
                        <span className="text-app-4">Fetched:</span>{" "}
                        {formatDate(row.fetched_at)}
                      </div>
                      {row.excerpt ? (
                        <div className="rounded-xl border border-app bg-app-panel px-3 py-3 leading-6 text-app-1">
                          {row.excerpt}
                        </div>
                      ) : null}
                      {row.url ? (
                        <a
                          href={row.url}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-2 text-sm font-medium text-cyan-300 hover:text-cyan-200"
                        >
                          <Link2 className="h-4 w-4" />
                          Open source
                        </a>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState
                title="No source evidence returned"
                description="The UI is ready for evidence rows, but the backend still needs to supply them for this property."
              />
            )}
          </div>
        </div>
      )}
    </Surface>
  );
}
