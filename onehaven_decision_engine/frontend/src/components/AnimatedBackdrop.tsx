// frontend/src/components/AnimatedBackdrop.tsx
import React from "react";

/**
 * Ultra-perf backdrop:
 * - no pointer listeners
 * - transform/opacity only animations
 * - disable with VITE_DISABLE_BACKDROP=1
 */
export default function AnimatedBackdrop() {
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
            "radial-gradient(1200px 900px at 20% 20%, rgba(120,90,255,0.18), transparent 60%), radial-gradient(1100px 800px at 80% 30%, rgba(35,255,200,0.10), transparent 60%), radial-gradient(1100px 900px at 50% 90%, rgba(255,88,122,0.10), transparent 60%), linear-gradient(to bottom, rgba(0,0,0,0.70), rgba(0,0,0,0.92))",
        }}
      />
    );
  }

  return (
    <div
      aria-hidden
      className="pointer-events-none absolute inset-0 -z-10 overflow-hidden"
    >
      <div className="absolute inset-0 bg-gradient-to-b from-black/30 via-black/45 to-black/80" />

      <div className="oh-orb oh-a" />
      <div className="oh-orb oh-b" />
      <div className="oh-orb oh-c" />

      <div className="absolute inset-0 opacity-[0.06] mix-blend-overlay bg-[url('data:image/svg+xml,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 width=%22400%22 height=%22400%22%3E%3Cfilter id=%22n%22%3E%3CfeTurbulence type=%22fractalNoise%22 baseFrequency=%220.8%22 numOctaves=%222%22 stitchTiles=%22stitch%22/%3E%3C/filter%3E%3Crect width=%22400%22 height=%22400%22 filter=%22url(%23n)%22 opacity=%220.4%22/%3E%3C/svg%3E')] pointer-events-none" />

      <style>{`
        .oh-orb{
          position:absolute;
          width: 900px;
          height: 900px;
          border-radius: 9999px;
          filter: blur(42px);
          opacity: 0.20;
          will-change: transform, opacity;
          transform: translate3d(0,0,0);
        }
        .oh-a{
          left:-280px; top:-250px;
          background: radial-gradient(circle at 35% 35%, rgba(120,90,255,0.80), rgba(120,90,255,0.08) 60%, transparent 70%);
          animation: ohA 18s ease-in-out infinite;
        }
        .oh-b{
          right:-340px; top:-180px;
          background: radial-gradient(circle at 35% 35%, rgba(35,255,200,0.65), rgba(35,255,200,0.06) 60%, transparent 70%);
          animation: ohB 22s ease-in-out infinite;
        }
        .oh-c{
          left:10%; bottom:-500px;
          background: radial-gradient(circle at 40% 40%, rgba(255,88,122,0.60), rgba(255,88,122,0.06) 60%, transparent 70%);
          animation: ohC 26s ease-in-out infinite;
        }
        @keyframes ohA{
          0%{ transform: translate3d(0,0,0) scale(1); opacity:.18 }
          50%{ transform: translate3d(80px,40px,0) scale(1.06); opacity:.25 }
          100%{ transform: translate3d(0,0,0) scale(1); opacity:.18 }
        }
        @keyframes ohB{
          0%{ transform: translate3d(0,0,0) scale(1); opacity:.16 }
          50%{ transform: translate3d(-90px,60px,0) scale(1.05); opacity:.23 }
          100%{ transform: translate3d(0,0,0) scale(1); opacity:.16 }
        }
        @keyframes ohC{
          0%{ transform: translate3d(0,0,0) scale(1.02); opacity:.14 }
          50%{ transform: translate3d(70px,-60px,0) scale(1.08); opacity:.21 }
          100%{ transform: translate3d(0,0,0) scale(1.02); opacity:.14 }
        }
        @media (prefers-reduced-motion: reduce){
          .oh-a,.oh-b,.oh-c{ animation:none !important; }
        }
      `}</style>
    </div>
  );
}
