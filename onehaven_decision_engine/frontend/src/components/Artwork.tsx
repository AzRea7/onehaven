// frontend/src/components/Artwork.tsx
import clsx from "clsx";
import React from "react";

/**
 * HoverTilt (PERF-SAFE)
 * - NO React state updates on pointer move
 * - requestAnimationFrame throttled
 * - writes transform directly to element
 * - supports reduced motion + env disable
 */
export function HoverTilt({
  className,
  children,
  intensity = 10,
}: {
  className?: string;
  children: React.ReactNode;
  intensity?: number;
}) {
  const ref = React.useRef<HTMLDivElement | null>(null);
  const raf = React.useRef<number | null>(null);
  const last = React.useRef<{ rx: number; ry: number } | null>(null);

  const disabled =
    (import.meta as any).env?.VITE_DISABLE_TILT === "1" ||
    (typeof window !== "undefined" &&
      window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches);

  React.useEffect(() => {
    return () => {
      if (raf.current != null) cancelAnimationFrame(raf.current);
    };
  }, []);

  const apply = React.useCallback(() => {
    raf.current = null;
    const el = ref.current;
    const v = last.current;
    if (!el || !v) return;
    // Single transform write = compositor friendly
    el.style.transform = `perspective(900px) rotateX(${v.rx}deg) rotateY(${v.ry}deg) translate3d(0,0,0)`;
  }, []);

  const onMove = (e: React.PointerEvent) => {
    if (disabled) return;
    const el = ref.current;
    if (!el) return;

    const r = el.getBoundingClientRect();
    const px = (e.clientX - r.left) / Math.max(1, r.width); // 0..1
    const py = (e.clientY - r.top) / Math.max(1, r.height); // 0..1

    const ry = (px - 0.5) * intensity; // left/right
    const rx = (0.5 - py) * intensity; // up/down

    last.current = { rx, ry };

    if (raf.current == null) raf.current = requestAnimationFrame(apply);
  };

  const onLeave = () => {
    if (disabled) return;
    const el = ref.current;
    if (!el) return;
    el.style.transform =
      "perspective(900px) rotateX(0deg) rotateY(0deg) translate3d(0,0,0)";
  };

  return (
    <div
      ref={ref}
      className={clsx("hover-tilt", className)}
      onPointerMove={onMove}
      onPointerLeave={onLeave}
      style={{
        transform:
          "perspective(900px) rotateX(0deg) rotateY(0deg) translate3d(0,0,0)",
        transformStyle: "preserve-3d",
        willChange: disabled ? undefined : "transform",
      }}
    >
      {children}
    </div>
  );
}

/* ----------------------------- Art: Orb ----------------------------- */
export function OrbDealEngine({ className }: { className?: string }) {
  return (
    <svg
      className={clsx("w-full h-full", className)}
      viewBox="0 0 420 420"
      fill="none"
    >
      <defs>
        <radialGradient id="orbCore" cx="50%" cy="45%" r="60%">
          <stop offset="0%" stopColor="rgba(255,255,255,0.55)" />
          <stop offset="35%" stopColor="rgba(120,90,255,0.40)" />
          <stop offset="70%" stopColor="rgba(255,88,122,0.18)" />
          <stop offset="100%" stopColor="rgba(0,0,0,0)" />
        </radialGradient>
        <linearGradient id="orbRing" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="rgba(120,90,255,0.95)" />
          <stop offset="55%" stopColor="rgba(255,88,122,0.95)" />
          <stop offset="100%" stopColor="rgba(35,255,200,0.75)" />
        </linearGradient>
        <filter id="glow">
          <feGaussianBlur stdDeviation="7" result="blur" />
          <feColorMatrix
            in="blur"
            type="matrix"
            values="
              1 0 0 0 0
              0 1 0 0 0
              0 0 1 0 0
              0 0 0 0.85 0"
          />
          <feMerge>
            <feMergeNode />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      <circle cx="210" cy="210" r="150" fill="url(#orbCore)" />
      <circle
        cx="210"
        cy="210"
        r="156"
        stroke="rgba(255,255,255,0.10)"
        strokeWidth="2"
      />

      <g filter="url(#glow)" opacity="0.95">
        <path
          d="M70 230C90 120 170 70 250 80C330 90 370 160 350 240C330 320 250 360 170 345C90 330 50 290 70 230Z"
          stroke="url(#orbRing)"
          strokeWidth="4"
          fill="none"
        />
      </g>

      <g opacity="0.55">
        <path
          d="M140 120c18-10 44-12 65-6"
          stroke="rgba(255,255,255,0.25)"
          strokeWidth="2"
        />
        <path
          d="M250 300c-20 10-46 12-70 5"
          stroke="rgba(255,255,255,0.22)"
          strokeWidth="2"
        />
      </g>
    </svg>
  );
}

/* --------------------------- Art: HQS Badge -------------------------- */
export function Section8Badge({ className }: { className?: string }) {
  return (
    <svg
      className={clsx("w-full h-full", className)}
      viewBox="0 0 420 420"
      fill="none"
    >
      <defs>
        <linearGradient id="badge" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="rgba(255,88,122,0.55)" />
          <stop offset="45%" stopColor="rgba(120,90,255,0.45)" />
          <stop offset="100%" stopColor="rgba(35,255,200,0.25)" />
        </linearGradient>
        <filter id="softGlow">
          <feGaussianBlur stdDeviation="8" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      <g filter="url(#softGlow)">
        <path
          d="M210 55c55 35 105 25 140 55v120c0 90-70 140-140 160-70-20-140-70-140-160V110c35-30 85-20 140-55Z"
          fill="rgba(255,255,255,0.03)"
          stroke="url(#badge)"
          strokeWidth="3.5"
        />
      </g>

      <g opacity="0.85">
        <path
          d="M165 220c20-55 85-65 120-22 26 32 12 85-24 110-38 26-86 12-103-25"
          stroke="rgba(255,255,255,0.22)"
          strokeWidth="3"
        />
      </g>

      <text
        x="210"
        y="235"
        textAnchor="middle"
        fontSize="52"
        fontWeight="700"
        fill="rgba(255,255,255,0.85)"
        style={{ letterSpacing: "-1px" }}
      >
        HQS
      </text>
      <text
        x="210"
        y="272"
        textAnchor="middle"
        fontSize="13"
        fontWeight="600"
        fill="rgba(255,255,255,0.55)"
        style={{ letterSpacing: "2px" }}
      >
        SECTION 8
      </text>
    </svg>
  );
}

/* -------------------------- Art: Agent Claw --------------------------- */
export function AgentClaw({ className }: { className?: string }) {
  return (
    <svg
      className={clsx("w-full h-full", className)}
      viewBox="0 0 420 420"
      fill="none"
    >
      <defs>
        <linearGradient id="agentGrad" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="rgba(120,90,255,0.8)" />
          <stop offset="55%" stopColor="rgba(255,88,122,0.7)" />
          <stop offset="100%" stopColor="rgba(35,255,200,0.55)" />
        </linearGradient>
        <filter id="agentGlow">
          <feGaussianBlur stdDeviation="7" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      <g filter="url(#agentGlow)" opacity="0.95">
        <path
          d="M115 280c40-80 90-120 150-120 60 0 95 40 70 95-18 40-60 70-115 80-55 10-95-5-105-55Z"
          stroke="url(#agentGrad)"
          strokeWidth="5"
          fill="rgba(255,255,255,0.02)"
        />
        <path
          d="M150 275c30-55 62-80 100-80"
          stroke="rgba(255,255,255,0.18)"
          strokeWidth="3"
          strokeLinecap="round"
        />
      </g>

      <circle cx="290" cy="190" r="6" fill="rgba(255,255,255,0.65)" />
      <circle cx="305" cy="210" r="3.5" fill="rgba(255,255,255,0.35)" />
    </svg>
  );
}

/* -------------------- Art: BrickBuilder (Mascot) --------------------- */
/**
 * BrickBuilder: “mascot construction crew” energy
 * - SVG only (cheap)
 * - animateTransform + opacity (compositor-friendly)
 * - respects reduced motion via env flag: VITE_DISABLE_ART_ANIM=1
 */
export function BrickBuilder({ className }: { className?: string }) {
  const animDisabled =
    (import.meta as any).env?.VITE_DISABLE_ART_ANIM === "1" ||
    (typeof window !== "undefined" &&
      window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches);

  const Float = ({ children }: { children: React.ReactNode }) =>
    animDisabled ? <>{children}</> : <>{children}</>;

  return (
    <svg
      className={clsx("w-full h-full", className)}
      viewBox="0 0 520 420"
      fill="none"
    >
      <defs>
        <linearGradient id="bb_grad" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="rgba(120,90,255,0.95)" />
          <stop offset="55%" stopColor="rgba(255,88,122,0.85)" />
          <stop offset="100%" stopColor="rgba(35,255,200,0.70)" />
        </linearGradient>

        <linearGradient id="bb_brick" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="rgba(255,255,255,0.10)" />
          <stop offset="100%" stopColor="rgba(255,255,255,0.02)" />
        </linearGradient>

        <filter id="bb_glow">
          <feGaussianBlur stdDeviation="10" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>

        <filter id="bb_soft">
          <feGaussianBlur stdDeviation="4" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      {/* Ground shadow */}
      <g filter="url(#bb_glow)" opacity="0.55">
        <ellipse
          cx="270"
          cy="356"
          rx="155"
          ry="28"
          fill="rgba(255,255,255,0.05)"
        />
      </g>

      {/* House outline */}
      <path
        d="M170 220l100-78 100 78v120c0 22-18 40-40 40H210c-22 0-40-18-40-40V220Z"
        fill="rgba(255,255,255,0.02)"
        stroke="rgba(255,255,255,0.12)"
        strokeWidth="2.5"
      />
      <path
        d="M206 230l64-52 64 52"
        stroke="rgba(255,255,255,0.18)"
        strokeWidth="2.5"
        strokeLinejoin="round"
      />

      {/* “Mascot” head/helmet */}
      <g filter="url(#bb_soft)">
        <path
          d="M270 120c42 0 70 22 70 52 0 28-22 52-70 52s-70-24-70-52c0-30 28-52 70-52Z"
          fill="rgba(255,255,255,0.03)"
          stroke="url(#bb_grad)"
          strokeWidth="3.5"
        />
        {/* Helmet brim */}
        <path
          d="M202 160c18-18 44-28 68-28s50 10 68 28"
          stroke="rgba(255,255,255,0.20)"
          strokeWidth="4"
          strokeLinecap="round"
        />
      </g>

      {/* Eyes (OpenClaw-ish “character vibe”) */}
      <g opacity="0.95">
        <ellipse
          cx="248"
          cy="186"
          rx="13"
          ry="11"
          fill="rgba(255,255,255,0.85)"
        />
        <ellipse
          cx="292"
          cy="186"
          rx="13"
          ry="11"
          fill="rgba(255,255,255,0.85)"
        />
        <circle cx="252" cy="188" r="5" fill="rgba(20,20,24,0.85)" />
        <circle cx="296" cy="188" r="5" fill="rgba(20,20,24,0.85)" />
        {/* tiny highlights */}
        <circle cx="250" cy="186" r="1.6" fill="rgba(255,255,255,0.75)" />
        <circle cx="294" cy="186" r="1.6" fill="rgba(255,255,255,0.75)" />
      </g>

      {/* Smile / “crew confidence” */}
      <path
        d="M252 212c8 9 28 9 36 0"
        stroke="rgba(255,255,255,0.22)"
        strokeWidth="3"
        strokeLinecap="round"
      />

      {/* Brick wall base */}
      <g opacity="0.95">
        <rect
          x="175"
          y="280"
          width="100"
          height="36"
          rx="10"
          fill="url(#bb_brick)"
          stroke="url(#bb_grad)"
          strokeWidth="3"
        />
        <rect
          x="285"
          y="280"
          width="100"
          height="36"
          rx="10"
          fill="url(#bb_brick)"
          stroke="url(#bb_grad)"
          strokeWidth="3"
        />
        <rect
          x="228"
          y="320"
          width="120"
          height="40"
          rx="12"
          fill="rgba(255,255,255,0.035)"
          stroke="url(#bb_grad)"
          strokeWidth="3.5"
        />
      </g>

      {/* Floating bricks that “build” */}
      <g>
        <rect
          x="206"
          y="242"
          width="120"
          height="40"
          rx="12"
          fill="rgba(255,255,255,0.035)"
          stroke="url(#bb_grad)"
          strokeWidth="3.5"
        />
        {!animDisabled && (
          <animateTransform
            attributeName="transform"
            type="translate"
            values="0 0; 0 -8; 0 0"
            dur="3.6s"
            repeatCount="indefinite"
          />
        )}
      </g>

      <g>
        <rect
          x="246"
          y="135"
          width="76"
          height="32"
          rx="10"
          fill="rgba(255,255,255,0.03)"
          stroke="url(#bb_grad)"
          strokeWidth="3"
        />
        {!animDisabled && (
          <animateTransform
            attributeName="transform"
            type="translate"
            values="0 0; 0 -10; 0 0"
            dur="4.1s"
            repeatCount="indefinite"
          />
        )}
      </g>

      {/* Sparkles (tiny, cheap) */}
      <circle cx="400" cy="164" r="4" fill="rgba(255,255,255,0.65)">
        {!animDisabled && (
          <animate
            attributeName="opacity"
            values="0.15;0.9;0.15"
            dur="2.4s"
            repeatCount="indefinite"
          />
        )}
      </circle>
      <circle cx="415" cy="148" r="2.5" fill="rgba(255,255,255,0.35)">
        {!animDisabled && (
          <animate
            attributeName="opacity"
            values="0.10;0.65;0.10"
            dur="2.0s"
            repeatCount="indefinite"
          />
        )}
      </circle>
    </svg>
  );
}

/* ---------------------- Existing BuildStack kept --------------------- */
export function BuildStack({ className }: { className?: string }) {
  return (
    <svg
      className={clsx("w-full h-full", className)}
      viewBox="0 0 420 420"
      fill="none"
    >
      <defs>
        <linearGradient id="brick" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="rgba(255,88,122,0.75)" />
          <stop offset="45%" stopColor="rgba(120,90,255,0.75)" />
          <stop offset="100%" stopColor="rgba(35,255,200,0.55)" />
        </linearGradient>
        <filter id="bGlow">
          <feGaussianBlur stdDeviation="10" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      <g filter="url(#bGlow)" opacity="0.75">
        <ellipse
          cx="210"
          cy="330"
          rx="110"
          ry="26"
          fill="rgba(255,255,255,0.05)"
        />
      </g>

      <path
        d="M140 210l70-55 70 55v110c0 18-14 32-32 32H172c-18 0-32-14-32-32V210Z"
        fill="rgba(255,255,255,0.02)"
        stroke="rgba(255,255,255,0.12)"
        strokeWidth="2.5"
      />
      <path
        d="M165 220l45-36 45 36"
        stroke="rgba(255,255,255,0.18)"
        strokeWidth="2.5"
        strokeLinejoin="round"
      />

      <g opacity="0.95">
        <rect
          x="120"
          y="275"
          width="86"
          height="34"
          rx="10"
          fill="rgba(255,255,255,0.03)"
          stroke="url(#brick)"
          strokeWidth="3"
        />
        <rect
          x="214"
          y="275"
          width="86"
          height="34"
          rx="10"
          fill="rgba(255,255,255,0.03)"
          stroke="url(#brick)"
          strokeWidth="3"
        />
      </g>

      <g>
        <rect
          x="150"
          y="235"
          width="120"
          height="40"
          rx="12"
          fill="rgba(255,255,255,0.035)"
          stroke="url(#brick)"
          strokeWidth="3.5"
        />
        <animateTransform
          attributeName="transform"
          type="translate"
          values="0 0; 0 -6; 0 0"
          dur="3.8s"
          repeatCount="indefinite"
        />
      </g>

      <g>
        <rect
          x="172"
          y="120"
          width="76"
          height="34"
          rx="10"
          fill="rgba(255,255,255,0.03)"
          stroke="url(#brick)"
          strokeWidth="3"
        />
        <animateTransform
          attributeName="transform"
          type="translate"
          values="0 0; 0 -8; 0 0"
          dur="4.4s"
          repeatCount="indefinite"
        />
      </g>

      <circle cx="292" cy="165" r="4" fill="rgba(255,255,255,0.65)">
        <animate
          attributeName="opacity"
          values="0.2;0.8;0.2"
          dur="2.6s"
          repeatCount="indefinite"
        />
      </circle>
      <circle cx="305" cy="150" r="2.5" fill="rgba(255,255,255,0.35)">
        <animate
          attributeName="opacity"
          values="0.1;0.6;0.1"
          dur="2.1s"
          repeatCount="indefinite"
        />
      </circle>
    </svg>
  );
}
