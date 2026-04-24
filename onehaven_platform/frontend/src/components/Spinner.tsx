import React from "react";

export default function Spinner({ label }: { label?: string }) {
  return (
    <div className="flex items-center gap-3 text-white/60">
      <div className="h-4 w-4 rounded-full border border-white/20 border-t-white/70 animate-spin" />
      {label ? <div className="text-sm">{label}</div> : null}
    </div>
  );
}
