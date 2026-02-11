import React from "react";

export default function Constitution() {
  return (
    <div className="space-y-3">
      <div className="text-xl font-semibold">Operating Truth</div>
      <div className="text-sm text-zinc-400">
        This page is intentionally simple. In production, render the markdown
        from /docs/operating_principles.md.
      </div>

      <div className="p-4 rounded-xl border border-zinc-800 bg-zinc-900/30 space-y-2 text-sm text-zinc-200">
        <div>
          • Rent is capped by payment standard (FMR) and rent reasonableness
          comps.
        </div>
        <div>
          • Inspection is deterministic; checklists + failure logs compound
          accuracy.
        </div>
        <div>
          • Time kills deals; processing delays must affect score and decision.
        </div>
        <div>
          • Compliance friction is a cost; it must be modeled and explained.
        </div>
        <div>• No silent overrides; explainability is non-negotiable.</div>
      </div>
    </div>
  );
}
