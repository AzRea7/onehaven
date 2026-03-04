// frontend/src/components/AppFooter.tsx
import React from "react";

export default function AppFooter() {
  const version =
    (import.meta as any).env?.VITE_APP_VERSION ||
    (import.meta as any).env?.VITE_GIT_SHA ||
    "dev";
  const env = (import.meta as any).env?.MODE || "dev";

  return (
    <footer className="border-t border-white/10 bg-black/20">
      <div className="mx-auto max-w-[1200px] px-4 md:px-6 py-4 text-xs text-white/60 flex items-center justify-between">
        <div>© {new Date().getFullYear()} OneHaven</div>
        <div className="flex items-center gap-3">
          <span>env: {env}</span>
          <span className="hidden sm:inline">·</span>
          <span>version: {version}</span>
        </div>
      </div>
    </footer>
  );
}
