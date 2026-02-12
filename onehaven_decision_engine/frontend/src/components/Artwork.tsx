import clsx from "clsx";
import React from "react";

/**
 * HoverTilt:
 * - sets --rx/--ry based on pointer position
 * - gives OpenClaw-ish “alive” hover feel without heavy JS libs
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

  const onMove = (e: React.PointerEvent) => {
    const el = ref.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const px = (e.clientX - r.left) / r.width; // 0..1
    const py = (e.clientY - r.top) / r.height; // 0..1
    const ry = (px - 0.5) * intensity; // left/right
    const rx = (0.5 - py) * intensity; // up/down
    el.style.setProperty("--rx", `${rx}deg`);
    el.style.setProperty("--ry", `${ry}deg`);
  };

  const onLeave = () => {
    const el = ref.current;
    if (!el) return;
    el.style.setProperty("--rx", `0deg`);
    el.style.setProperty("--ry", `0deg`);
  };

  return (
    <div
      ref={ref}
      className={clsx("hover-tilt", className)}
      onPointerMove={onMove}
      onPointerLeave={onLeave}
    >
      {children}
    </div>
  );
}

/* --- Existing art, recolored slightly warmer --- */
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

      {/* spark arcs */}
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

      {/* “claw” abstract */}
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

/**
 * NEW: BuildStack
 * “brick/house being stacked” with internal moving parts.
 * Use inside cards for that OpenClaw-like “alive artwork”.
 */
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

      {/* base glow */}
      <g filter="url(#bGlow)" opacity="0.75">
        <ellipse
          cx="210"
          cy="330"
          rx="110"
          ry="26"
          fill="rgba(255,255,255,0.05)"
        />
      </g>

      {/* house outline */}
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

      {/* bricks that “bob” independently */}
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

      {/* animated bricks using <animateTransform> (pure SVG, no JS) */}
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

      {/* little “spark” */}
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
