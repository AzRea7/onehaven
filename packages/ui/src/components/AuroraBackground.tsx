// frontend/onehaven_onehaven_platform/frontend/src/components/AuroraBackground.tsx
import React from "react";

/**
 * AuroraBackground (perf-safe)
 * - NO framer-motion
 * - CSS keyframes only (transform/opacity)
 * - Smaller layers + lower blur
 * - Can be disabled with VITE_DISABLE_BACKDROP=1
 */
export default function AuroraBackground() {
  const disabled =
    (import.meta as any).env?.VITE_DISABLE_BACKDROP === "1" ||
    (typeof window !== "undefined" &&
      window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches);

  if (disabled) {
    return (
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 -z-10"
        style={{
          background:
            "radial-gradient(900px 600px at 18% 12%, rgba(120,90,255,0.12), transparent 60%), radial-gradient(820px 540px at 84% 18%, rgba(255,88,122,0.08), transparent 62%), radial-gradient(900px 580px at 55% 92%, rgba(35,255,200,0.06), transparent 62%), linear-gradient(to bottom, rgba(0,0,0,0.20), rgba(0,0,0,0.82))",
        }}
      />
    );
  }

  return (
    <div
      aria-hidden
      className="pointer-events-none absolute inset-0 -z-10 overflow-hidden"
      style={{
        // isolate helps prevent accidental blending/paint invalidation from other layers
        isolation: "isolate",
      }}
    >
      {/* Base vignette */}
      <div className="absolute inset-0 bg-gradient-to-b from-black/10 via-black/45 to-black/80" />

      {/* Orbs (smaller + lower blur than before) */}
      <div className="oh-aurora orb-a" />
      <div className="oh-aurora orb-b" />
      <div className="oh-aurora orb-c" />

      {/* Optional noise (cheap) — you already have .noise in styles.css,
          but this keeps Aurora self-contained if you want it. */}
      <div className="noise" />

      <style>{`
        .oh-aurora{
          position:absolute;
          width: 560px;
          height: 560px;
          border-radius: 9999px;
          filter: blur(36px);
          opacity: 0.18;
          will-change: transform, opacity;
          transform: translate3d(0,0,0);
          contain: paint;
        }

        .orb-a{
          left:-220px; top:-220px;
          background: radial-gradient(circle at 30% 30%,
            rgba(120,90,255,0.75),
            rgba(120,90,255,0.06) 60%,
            transparent 72%
          );
          animation: aurA 20s ease-in-out infinite;
        }

        .orb-b{
          right:-260px; top:-160px;
          width: 620px;
          height: 620px;
          filter: blur(40px);
          opacity: 0.14;
          background: radial-gradient(circle at 70% 40%,
            rgba(255,88,122,0.70),
            rgba(255,88,122,0.05) 60%,
            transparent 72%
          );
          animation: aurB 26s ease-in-out infinite;
        }

        .orb-c{
          left: 28%;
          bottom:-340px;
          width: 680px;
          height: 680px;
          filter: blur(44px);
          opacity: 0.12;
          background: radial-gradient(circle at 45% 60%,
            rgba(35,255,200,0.55),
            rgba(35,255,200,0.05) 62%,
            transparent 74%
          );
          animation: aurC 30s ease-in-out infinite;
        }

        @keyframes aurA{
          0%   { transform: translate3d(0,0,0) scale(1.00); opacity: .14; }
          50%  { transform: translate3d(60px,38px,0) scale(1.05); opacity: .22; }
          100% { transform: translate3d(0,0,0) scale(1.00); opacity: .14; }
        }

        @keyframes aurB{
          0%   { transform: translate3d(0,0,0) scale(1.00); opacity: .12; }
          50%  { transform: translate3d(-70px,44px,0) scale(1.04); opacity: .18; }
          100% { transform: translate3d(0,0,0) scale(1.00); opacity: .12; }
        }

        @keyframes aurC{
          0%   { transform: translate3d(0,0,0) scale(1.02); opacity: .10; }
          50%  { transform: translate3d(54px,-40px,0) scale(1.06); opacity: .16; }
          100% { transform: translate3d(0,0,0) scale(1.02); opacity: .10; }
        }

        @media (prefers-reduced-motion: reduce){
          .orb-a,.orb-b,.orb-c{ animation: none !important; }
        }
      `}</style>
    </div>
  );
}
