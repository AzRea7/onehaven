// frontend/src/components/Artwork.tsx
import clsx from "clsx";
import React from "react";

/**
 * PERF NOTE (why this file is faster now):
 * - All big CSS blobs are injected into <head> once (no repeated <style> tags per render).
 * - HoverTilt does zero React state updates during pointermove (still RAF throttled).
 * - SVG groups animate via CSS only (compositor-friendly transforms).
 */

function useGlobalStyle(id: string, cssText: string) {
  React.useEffect(() => {
    if (typeof document === "undefined") return;
    if (document.getElementById(id)) return;
    const tag = document.createElement("style");
    tag.id = id;
    tag.textContent = cssText;
    document.head.appendChild(tag);
  }, [id, cssText]);
}

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
    el.style.transform = `perspective(900px) rotateX(${v.rx}deg) rotateY(${v.ry}deg) translate3d(0,0,0)`;
  }, []);

  const onMove = (e: React.PointerEvent) => {
    if (disabled) return;
    const el = ref.current;
    if (!el) return;

    const r = el.getBoundingClientRect();
    const px = (e.clientX - r.left) / Math.max(1, r.width);
    const py = (e.clientY - r.top) / Math.max(1, r.height);

    const ry = (px - 0.5) * intensity;
    const rx = (0.5 - py) * intensity;

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

/* --------------------- Shared Neon Stone Frame Style --------------------- */
const neonArtCss = `
  .neon-art{
    --neon: #00D9FF;
    --neon-rgb: 0,217,255;
    --halo-1: rgba(0,217,255,0.22);
    --halo-2: rgba(0,217,255,0.10);
  }
  .neon-art.group:hover{
    --neon: #34FF6A;
    --neon-rgb: 52,255,106;
    --halo-1: rgba(52,255,106,0.24);
    --halo-2: rgba(52,255,106,0.11);
  }

  .na-halo{
    background: radial-gradient(closest-side, var(--halo-1), var(--halo-2), transparent 70%);
    filter: blur(12px);
    transform: translateZ(0);
    opacity: 0.9;
    transition: opacity 220ms ease, background 220ms ease;
    will-change: opacity;
  }
  .neon-art.group:hover .na-halo{ opacity: 1; }

  .na-svg{
    pointer-events:none;
    width:100%;
    height:auto;
    filter: drop-shadow(0 0 12px rgba(var(--neon-rgb),0.20));
    transition: filter 220ms ease;
  }
  .neon-art.group:hover .na-svg{
    filter: drop-shadow(0 0 12px rgba(var(--neon-rgb),0.22));
  }

  @keyframes naFloat {
    0%{transform:translate3d(0,0,0)}
    50%{transform:translate3d(0,-3px,0)}
    100%{transform:translate3d(0,0,0)}
  }
  @keyframes naPulse {
    0%{opacity:.55}
    50%{opacity:.92}
    100%{opacity:.55}
  }

  .na-float{
    transform-origin: 50% 60%;
    animation: naFloat 3.1s ease-in-out infinite;
    will-change: transform;
  }

  .na-neon-stroke{ stroke: var(--neon) !important; }
  .na-neon-fill{ fill: var(--neon) !important; }
  .na-neon-pulse{ animation: naPulse 2.7s ease-in-out infinite; will-change: opacity; }

  @media (prefers-reduced-motion: reduce) {
    .na-float,.na-neon-pulse{ animation:none !important; }
    .na-halo{ transition:none !important; }
    .na-svg{ transition:none !important; }
  }
`;

/* ----------------------- Brick Builder (NEW, animated) ----------------------- */
/**
 * Goal: “modern clean animated mascot” that feels like the openclaw vibe
 * but housing-themed:
 * - stone/neon outline style matches Orb/Badge/Claw/BuildStack
 * - on hover: bricks “stack” into a house silhouette, roof drops in, door lights up
 * - CSS-only transforms (GPU-friendly)
 */
const brickBuilderCss = `
  .bb{
    --bb-rise: 0px;
    --bb-spread: 0px;
    --bb-roof: 0px;
    --bb-door: 0.35;
    --bb-w: 0;
  }
  .bb.group:hover{
    --bb-rise: -10px;
    --bb-spread: 6px;
    --bb-roof: -8px;
    --bb-door: 0.75;
    --bb-w: 1;
  }

  @keyframes bbWiggle{
    0%{transform:translate3d(0,0,0) rotate(0deg)}
    50%{transform:translate3d(0,-2px,0) rotate(-0.7deg)}
    100%{transform:translate3d(0,0,0) rotate(0deg)}
  }
  .bb-float{ animation: bbWiggle 3.2s ease-in-out infinite; transform-origin: 50% 70%; will-change: transform; }

  .bb-brick{
    transition: transform 220ms ease, opacity 220ms ease;
    will-change: transform;
  }
  .bb-roof{
    transition: transform 240ms ease, opacity 240ms ease;
    will-change: transform, opacity;
  }
  .bb-doorGlow{
    transition: opacity 220ms ease;
    opacity: var(--bb-door);
  }
  .bb-spark{
    opacity: 0.6;
    transition: transform 220ms ease, opacity 220ms ease;
    transform: translate3d(0,0,0);
    will-change: transform, opacity;
  }
  .bb.group:hover .bb-spark{
    opacity: 0.85;
    transform: translate3d(0,-2px,0);
  }

  @media (prefers-reduced-motion: reduce) {
    .bb-float{ animation:none !important; }
    .bb-brick,.bb-roof,.bb-doorGlow,.bb-spark{ transition:none !important; }
  }
`;

export function BrickBuilder({ className }: { className?: string }) {
  useGlobalStyle("oh-neon-art-css", neonArtCss);
  useGlobalStyle("oh-brick-builder-css", brickBuilderCss);

  const uid = React.useId();
  const gStone = `${uid}-bbStone`;
  const gOutline = `${uid}-bbOutline`;
  const fNeon = `${uid}-bbNeon`;
  const fShadow = `${uid}-bbShadow`;

  return (
    <HoverTilt className={clsx("neon-art bb group", className)} intensity={9}>
      <div className="relative select-none" style={{ contain: "layout paint" }}>
        <div
          aria-hidden
          className="na-halo absolute inset-0 -z-10 rounded-[28px]"
        />

        <svg
          className="na-svg block h-full w-full"
          viewBox="0 0 420 420"
          fill="none"
        >
          <defs>
            <filter id={fNeon} x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur stdDeviation="7" result="b" />
              <feMerge>
                <feMergeNode in="b" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <filter id={fShadow} x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="12" result="s" />
              <feOffset dy="10" result="o" />
              <feColorMatrix
                in="o"
                type="matrix"
                values="
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0.35 0"
                result="shadow"
              />
              <feMerge>
                <feMergeNode in="shadow" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <radialGradient id={gStone} cx="42%" cy="28%" r="85%">
              <stop offset="0" stopColor="#1E3B5C" />
              <stop offset="0.55" stopColor="#0D1C2F" />
              <stop offset="1" stopColor="#070C14" />
            </radialGradient>

            <linearGradient id={gOutline} x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" stopColor="#050B12" />
              <stop offset="1" stopColor="#050B12" />
            </linearGradient>

            <radialGradient id={`${uid}-door`} cx="50%" cy="30%" r="70%">
              <stop offset="0" stopColor="#FFFFFF" stopOpacity="0.22" />
              <stop offset="0.45" stopColor="#00D9FF" stopOpacity="0.12" />
              <stop offset="1" stopColor="#00D9FF" stopOpacity="0.0" />
            </radialGradient>
          </defs>

          <g filter={`url(#${fShadow})`} className="bb-float">
            <ellipse
              cx="210"
              cy="350"
              rx="120"
              ry="20"
              fill="#000"
              opacity="0.22"
            />

            {/* main house block */}
            <path
              d="M122 214 L210 144 L298 214 V304 C298 326 280 344 258 344 H162 C140 344 122 326 122 304 V214 Z"
              fill={`url(#${gStone})`}
              stroke={`url(#${gOutline})`}
              strokeWidth="12"
              strokeLinejoin="round"
            />

            {/* roof "drops in" on hover */}
            <g
              className="bb-roof"
              style={{
                transform: `translate3d(0, var(--bb-roof), 0)`,
                opacity: `calc(0.75 + (var(--bb-w) * 0.25))`,
              }}
            >
              <path
                d="M146 210 L210 168 L274 210"
                stroke={`url(#${gOutline})`}
                strokeWidth="10"
                strokeLinejoin="round"
                opacity="0.9"
              />
              <path
                className="na-neon-stroke"
                d="M146 210 L210 168 L274 210"
                stroke="#00D9FF"
                strokeWidth="5"
                strokeLinejoin="round"
                opacity="0.55"
                filter={`url(#${fNeon})`}
              />
            </g>

            {/* Bricks that "stack" upward into a neat wall pattern */}
            <g
              className="bb-brick"
              style={{
                transform: `translate3d(0, var(--bb-rise), 0)`,
              }}
            >
              {/* dark cut lines + neon leaks (brick courses) */}
              <path
                d="M148 258 H272"
                stroke={`url(#${gOutline})`}
                strokeWidth="10"
                strokeLinecap="round"
                opacity="0.92"
              />
              <path
                className="na-neon-stroke na-neon-pulse"
                d="M148 258 H272"
                stroke="#00D9FF"
                strokeWidth="5"
                strokeLinecap="round"
                opacity="0.78"
                filter={`url(#${fNeon})`}
              />

              <path
                d="M148 294 H246"
                stroke={`url(#${gOutline})`}
                strokeWidth="10"
                strokeLinecap="round"
                opacity="0.92"
              />
              <path
                className="na-neon-stroke na-neon-pulse"
                d="M148 294 H246"
                stroke="#00D9FF"
                strokeWidth="5"
                strokeLinecap="round"
                opacity="0.72"
                filter={`url(#${fNeon})`}
              />

              {/* vertical brick seams that "spread" slightly on hover */}
              <path
                d="M186 246 V306"
                stroke={`url(#${gOutline})`}
                strokeWidth="9"
                strokeLinecap="round"
                opacity="0.85"
                style={{
                  transform: `translate3d(calc(var(--bb-spread) * -1),0,0)`,
                }}
              />
              <path
                className="na-neon-stroke"
                d="M186 246 V306"
                stroke="#00D9FF"
                strokeWidth="4.5"
                strokeLinecap="round"
                opacity="0.45"
                filter={`url(#${fNeon})`}
                style={{
                  transform: `translate3d(calc(var(--bb-spread) * -1),0,0)`,
                }}
              />

              <path
                d="M234 246 V306"
                stroke={`url(#${gOutline})`}
                strokeWidth="9"
                strokeLinecap="round"
                opacity="0.85"
                style={{ transform: `translate3d(var(--bb-spread),0,0)` }}
              />
              <path
                className="na-neon-stroke"
                d="M234 246 V306"
                stroke="#00D9FF"
                strokeWidth="4.5"
                strokeLinecap="round"
                opacity="0.45"
                filter={`url(#${fNeon})`}
                style={{ transform: `translate3d(var(--bb-spread),0,0)` }}
              />
            </g>

            {/* door + glow responds to hover */}
            <path
              d="M190 344 V286 C190 272 201 262 215 262 C229 262 240 272 240 286 V344"
              fill="rgba(255,255,255,0.02)"
              stroke={`url(#${gOutline})`}
              strokeWidth="10"
              strokeLinejoin="round"
              opacity="0.95"
            />
            <path
              className="na-neon-stroke"
              d="M205 300 H224"
              stroke="#00D9FF"
              strokeWidth="5"
              strokeLinecap="round"
              opacity="0.62"
              filter={`url(#${fNeon})`}
            />
            <ellipse
              className="bb-doorGlow"
              cx="215"
              cy="300"
              rx="42"
              ry="56"
              fill={`url(#${uid}-door)`}
            />

            {/* sparks */}
            <g className="bb-spark">
              <circle
                cx="288"
                cy="178"
                r="3.5"
                className="na-neon-fill"
                fill="#00D9FF"
              />
              <circle
                cx="300"
                cy="190"
                r="2.2"
                className="na-neon-fill"
                fill="#00D9FF"
                opacity="0.8"
              />
              <circle
                cx="134"
                cy="232"
                r="2.6"
                className="na-neon-fill"
                fill="#00D9FF"
                opacity="0.75"
              />
            </g>
          </g>
        </svg>
      </div>
    </HoverTilt>
  );
}

/* ----------------------------- Art: Orb ----------------------------- */
export function OrbDealEngine({ className }: { className?: string }) {
  useGlobalStyle("oh-neon-art-css", neonArtCss);

  const uid = React.useId();
  const gStone = `${uid}-orbStone`;
  const gOutline = `${uid}-orbOutline`;
  const fNeon = `${uid}-orbNeon`;
  const fShadow = `${uid}-orbShadow`;

  return (
    <HoverTilt className={clsx("neon-art group", className)} intensity={9}>
      <div className="relative select-none" style={{ contain: "layout paint" }}>
        <div
          aria-hidden
          className="na-halo absolute inset-0 -z-10 rounded-[28px]"
        />

        <svg
          className="na-svg block h-full w-full"
          viewBox="0 0 420 420"
          fill="none"
        >
          <defs>
            <filter id={fNeon} x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur stdDeviation="7" result="b" />
              <feMerge>
                <feMergeNode in="b" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <filter id={fShadow} x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="12" result="s" />
              <feOffset dy="10" result="o" />
              <feColorMatrix
                in="o"
                type="matrix"
                values="
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0.35 0"
                result="shadow"
              />
              <feMerge>
                <feMergeNode in="shadow" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <radialGradient id={gStone} cx="42%" cy="28%" r="85%">
              <stop offset="0" stopColor="#1E3B5C" />
              <stop offset="0.55" stopColor="#0D1C2F" />
              <stop offset="1" stopColor="#070C14" />
            </radialGradient>

            <linearGradient id={gOutline} x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" stopColor="#050B12" />
              <stop offset="1" stopColor="#050B12" />
            </linearGradient>
          </defs>

          <g filter={`url(#${fShadow})`} className="na-float">
            <ellipse
              cx="210"
              cy="332"
              rx="118"
              ry="22"
              fill="#000"
              opacity="0.24"
            />

            <circle
              cx="210"
              cy="200"
              r="128"
              fill={`url(#${gStone})`}
              stroke={`url(#${gOutline})`}
              strokeWidth="12"
            />

            <path
              d="M106 198 C128 128 178 96 210 96 C260 96 308 138 315 198 C322 258 276 304 210 304 C144 304 98 260 106 198Z"
              fill="none"
              stroke={`url(#${gOutline})`}
              strokeWidth="12"
              strokeLinecap="round"
              strokeLinejoin="round"
              opacity="0.95"
            />
            <path
              className="na-neon-stroke na-neon-pulse"
              d="M106 198 C128 128 178 96 210 96 C260 96 308 138 315 198 C322 258 276 304 210 304 C144 304 98 260 106 198Z"
              fill="none"
              stroke="#00D9FF"
              strokeWidth="6"
              strokeLinecap="round"
              strokeLinejoin="round"
              opacity="0.78"
              filter={`url(#${fNeon})`}
            />

            <circle cx="210" cy="200" r="58" fill="rgba(255,255,255,0.03)" />
            <circle
              cx="210"
              cy="200"
              r="46"
              className="na-neon-stroke"
              stroke="#00D9FF"
              strokeWidth="3"
              opacity="0.35"
              filter={`url(#${fNeon})`}
            />

            <circle
              cx="272"
              cy="156"
              r="4"
              className="na-neon-fill"
              fill="#00D9FF"
              opacity="0.75"
            />
            <circle
              cx="284"
              cy="172"
              r="2.5"
              className="na-neon-fill"
              fill="#00D9FF"
              opacity="0.55"
            />
          </g>
        </svg>
      </div>
    </HoverTilt>
  );
}

/* --------------------------- Art: HQS Badge -------------------------- */
export function Section8Badge({ className }: { className?: string }) {
  useGlobalStyle("oh-neon-art-css", neonArtCss);

  const uid = React.useId();
  const gStone = `${uid}-badgeStone`;
  const gOutline = `${uid}-badgeOutline`;
  const fNeon = `${uid}-badgeNeon`;
  const fShadow = `${uid}-badgeShadow`;

  return (
    <HoverTilt className={clsx("neon-art group", className)} intensity={9}>
      <div className="relative select-none" style={{ contain: "layout paint" }}>
        <div
          aria-hidden
          className="na-halo absolute inset-0 -z-10 rounded-[28px]"
        />

        <svg
          className="na-svg block h-full w-full"
          viewBox="0 0 420 420"
          fill="none"
        >
          <defs>
            <filter id={fNeon} x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur stdDeviation="7" result="b" />
              <feMerge>
                <feMergeNode in="b" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <filter id={fShadow} x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="12" result="s" />
              <feOffset dy="10" result="o" />
              <feColorMatrix
                in="o"
                type="matrix"
                values="
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0.35 0"
                result="shadow"
              />
              <feMerge>
                <feMergeNode in="shadow" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <radialGradient id={gStone} cx="42%" cy="28%" r="85%">
              <stop offset="0" stopColor="#1E3B5C" />
              <stop offset="0.55" stopColor="#0D1C2F" />
              <stop offset="1" stopColor="#070C14" />
            </radialGradient>

            <linearGradient id={gOutline} x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" stopColor="#050B12" />
              <stop offset="1" stopColor="#050B12" />
            </linearGradient>
          </defs>

          <g filter={`url(#${fShadow})`} className="na-float">
            <ellipse
              cx="210"
              cy="350"
              rx="110"
              ry="20"
              fill="#000"
              opacity="0.22"
            />

            <path
              d="M210 64
                 C262 94 318 84 350 112
                 V220
                 C350 312 280 356 210 374
                 C140 356 70 312 70 220
                 V112
                 C102 84 158 94 210 64 Z"
              fill={`url(#${gStone})`}
              stroke={`url(#${gOutline})`}
              strokeWidth="12"
              strokeLinejoin="round"
            />

            <path
              d="M210 88
                 C254 114 300 106 328 130
                 V220
                 C328 296 272 332 210 348
                 C148 332 92 296 92 220
                 V130
                 C120 106 166 114 210 88 Z"
              fill="none"
              stroke={`url(#${gOutline})`}
              strokeWidth="10"
              strokeLinejoin="round"
              opacity="0.92"
            />
            <path
              className="na-neon-stroke na-neon-pulse"
              d="M210 88
                 C254 114 300 106 328 130
                 V220
                 C328 296 272 332 210 348
                 C148 332 92 296 92 220
                 V130
                 C120 106 166 114 210 88 Z"
              fill="none"
              stroke="#00D9FF"
              strokeWidth="5"
              strokeLinejoin="round"
              opacity="0.78"
              filter={`url(#${fNeon})`}
            />

            <path
              className="na-neon-stroke"
              d="M150 220 L192 262 L286 166"
              fill="none"
              stroke="#00D9FF"
              strokeWidth="10"
              strokeLinecap="round"
              strokeLinejoin="round"
              opacity="0.85"
              filter={`url(#${fNeon})`}
            />

            <text
              x="210"
              y="150"
              textAnchor="middle"
              fontSize="44"
              fontWeight="800"
              fill="rgba(255,255,255,0.78)"
              style={{ letterSpacing: "-1px" }}
            >
              HQS
            </text>
            <text
              x="210"
              y="176"
              textAnchor="middle"
              fontSize="12"
              fontWeight="700"
              fill="rgba(255,255,255,0.55)"
              style={{ letterSpacing: "2px" }}
            >
              SECTION 8
            </text>
          </g>
        </svg>
      </div>
    </HoverTilt>
  );
}

/* -------------------------- Art: Agent Claw --------------------------- */
export function AgentClaw({ className }: { className?: string }) {
  useGlobalStyle("oh-neon-art-css", neonArtCss);

  const uid = React.useId();
  const gStone = `${uid}-clawStone`;
  const gOutline = `${uid}-clawOutline`;
  const fNeon = `${uid}-clawNeon`;
  const fShadow = `${uid}-clawShadow`;

  return (
    <HoverTilt className={clsx("neon-art group", className)} intensity={9}>
      <div className="relative select-none" style={{ contain: "layout paint" }}>
        <div
          aria-hidden
          className="na-halo absolute inset-0 -z-10 rounded-[28px]"
        />

        <svg
          className="na-svg block h-full w-full"
          viewBox="0 0 420 420"
          fill="none"
        >
          <defs>
            <filter id={fNeon} x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur stdDeviation="7" result="b" />
              <feMerge>
                <feMergeNode in="b" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <filter id={fShadow} x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="12" result="s" />
              <feOffset dy="10" result="o" />
              <feColorMatrix
                in="o"
                type="matrix"
                values="
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0.35 0"
                result="shadow"
              />
              <feMerge>
                <feMergeNode in="shadow" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <radialGradient id={gStone} cx="42%" cy="28%" r="85%">
              <stop offset="0" stopColor="#1E3B5C" />
              <stop offset="0.55" stopColor="#0D1C2F" />
              <stop offset="1" stopColor="#070C14" />
            </radialGradient>

            <linearGradient id={gOutline} x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" stopColor="#050B12" />
              <stop offset="1" stopColor="#050B12" />
            </linearGradient>
          </defs>

          <g filter={`url(#${fShadow})`} className="na-float">
            <ellipse
              cx="210"
              cy="338"
              rx="120"
              ry="20"
              fill="#000"
              opacity="0.22"
            />

            <path
              d="M120 290
                 C150 200 205 150 260 150
                 C320 150 350 200 330 246
                 C314 284 276 310 230 320
                 C178 332 132 328 120 290 Z"
              fill={`url(#${gStone})`}
              stroke={`url(#${gOutline})`}
              strokeWidth="12"
              strokeLinejoin="round"
            />

            <path
              d="M155 278 C180 230 210 210 248 206"
              fill="none"
              stroke={`url(#${gOutline})`}
              strokeWidth="10"
              strokeLinecap="round"
              opacity="0.92"
            />
            <path
              className="na-neon-stroke na-neon-pulse"
              d="M155 278 C180 230 210 210 248 206"
              fill="none"
              stroke="#00D9FF"
              strokeWidth="5"
              strokeLinecap="round"
              opacity="0.78"
              filter={`url(#${fNeon})`}
            />

            <path
              className="na-neon-stroke"
              d="M286 176 L304 154"
              stroke="#00D9FF"
              strokeWidth="6"
              strokeLinecap="round"
              opacity="0.7"
              filter={`url(#${fNeon})`}
            />
            <path
              className="na-neon-stroke"
              d="M314 206 L336 188"
              stroke="#00D9FF"
              strokeWidth="6"
              strokeLinecap="round"
              opacity="0.65"
              filter={`url(#${fNeon})`}
            />

            <circle
              cx="292"
              cy="190"
              r="4.5"
              className="na-neon-fill"
              fill="#00D9FF"
              opacity="0.7"
            />
            <circle
              cx="305"
              cy="210"
              r="2.7"
              className="na-neon-fill"
              fill="#00D9FF"
              opacity="0.55"
            />
          </g>
        </svg>
      </div>
    </HoverTilt>
  );
}

/* ---------------------- Art: BuildStack (kept) --------------------- */
export function BuildStack({ className }: { className?: string }) {
  useGlobalStyle("oh-neon-art-css", neonArtCss);

  const uid = React.useId();
  const gStone = `${uid}-bsStone`;
  const gOutline = `${uid}-bsOutline`;
  const fNeon = `${uid}-bsNeon`;
  const fShadow = `${uid}-bsShadow`;

  return (
    <HoverTilt className={clsx("neon-art group", className)} intensity={9}>
      <div className="relative select-none" style={{ contain: "layout paint" }}>
        <div
          aria-hidden
          className="na-halo absolute inset-0 -z-10 rounded-[28px]"
        />

        <svg
          className="na-svg block h-full w-full"
          viewBox="0 0 420 420"
          fill="none"
        >
          <defs>
            <filter id={fNeon} x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur stdDeviation="7" result="b" />
              <feMerge>
                <feMergeNode in="b" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <filter id={fShadow} x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="12" result="s" />
              <feOffset dy="10" result="o" />
              <feColorMatrix
                in="o"
                type="matrix"
                values="
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0.35 0"
                result="shadow"
              />
              <feMerge>
                <feMergeNode in="shadow" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <radialGradient id={gStone} cx="42%" cy="28%" r="85%">
              <stop offset="0" stopColor="#1E3B5C" />
              <stop offset="0.55" stopColor="#0D1C2F" />
              <stop offset="1" stopColor="#070C14" />
            </radialGradient>

            <linearGradient id={gOutline} x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" stopColor="#050B12" />
              <stop offset="1" stopColor="#050B12" />
            </linearGradient>
          </defs>

          <g filter={`url(#${fShadow})`} className="na-float">
            <ellipse
              cx="210"
              cy="350"
              rx="110"
              ry="18"
              fill="#000"
              opacity="0.22"
            />

            <path
              d="M128 210 L210 144 L292 210 V304 C292 324 276 340 256 340 H164 C144 340 128 324 128 304 V210 Z"
              fill={`url(#${gStone})`}
              stroke={`url(#${gOutline})`}
              strokeWidth="12"
              strokeLinejoin="round"
            />
            <path
              d="M160 210 L210 174 L260 210"
              stroke={`url(#${gOutline})`}
              strokeWidth="10"
              strokeLinejoin="round"
              opacity="0.9"
            />

            <path
              d="M150 260 H270"
              stroke={`url(#${gOutline})`}
              strokeWidth="10"
              strokeLinecap="round"
              opacity="0.92"
            />
            <path
              className="na-neon-stroke na-neon-pulse"
              d="M150 260 H270"
              stroke="#00D9FF"
              strokeWidth="5"
              strokeLinecap="round"
              opacity="0.78"
              filter={`url(#${fNeon})`}
            />

            <path
              d="M150 294 H240"
              stroke={`url(#${gOutline})`}
              strokeWidth="10"
              strokeLinecap="round"
              opacity="0.92"
            />
            <path
              className="na-neon-stroke na-neon-pulse"
              d="M150 294 H240"
              stroke="#00D9FF"
              strokeWidth="5"
              strokeLinecap="round"
              opacity="0.72"
              filter={`url(#${fNeon})`}
            />

            <path
              d="M192 340 V284 C192 272 202 262 214 262 C226 262 236 272 236 284 V340"
              fill="rgba(255,255,255,0.02)"
              stroke={`url(#${gOutline})`}
              strokeWidth="10"
              strokeLinejoin="round"
              opacity="0.95"
            />
            <path
              className="na-neon-stroke"
              d="M206 300 H222"
              stroke="#00D9FF"
              strokeWidth="5"
              strokeLinecap="round"
              opacity="0.65"
              filter={`url(#${fNeon})`}
            />

            <circle
              cx="284"
              cy="178"
              r="3.5"
              className="na-neon-fill"
              fill="#00D9FF"
              opacity="0.7"
            />
            <circle
              cx="296"
              cy="190"
              r="2.2"
              className="na-neon-fill"
              fill="#00D9FF"
              opacity="0.55"
            />
          </g>
        </svg>
      </div>
    </HoverTilt>
  );
}

/* ------------------------- Mascot CSS (blue -> green hover) ------------------------- */
/* DO NOT CHANGE ANYTHING ABOUT THE GOLEM BELOW (kept as you pasted). */
const golemCss = `
  .g-eye-bloom{
  opacity: 0.18;
  }
  
  .golem.group:hover .g-eye-bloom{
    opacity: 0.24;
  }

  .g-svg{
    pointer-events:none;
    width:100%;
    height:auto;
    filter: drop-shadow(0 0 12px rgba(0,217,255,0.20));
    transition: filter 220ms ease;
  }

  .g-halo{
    --halo-1: rgba(0,217,255,0.22);
    --halo-2: rgba(0,217,255,0.10);
    background: radial-gradient(closest-side, var(--halo-1), var(--halo-2), transparent 70%);
    filter: blur(12px);
    transform: translateZ(0);
    opacity: 0.9;
    transition: opacity 220ms ease, background 220ms ease;
  }

  .golem{
    --neon: #00D9FF;
    --neon-rgb: 0,217,255;
  }
  .golem.group:hover{
    --neon: #34FF6A;
    --neon-rgb: 52,255,106;
  }

  .golem.group:hover .g-halo{
    --halo-1: rgba(52,255,106,0.24);
    --halo-2: rgba(52,255,106,0.11);
    opacity: 1;
  }

  .golem.group:hover .g-svg{
    filter: drop-shadow(0 0 12px rgba(52,255,106,0.22));
  }

  .g-crack-glow{ stroke: var(--neon) !important; }
  .g-arm-rim{ stroke: var(--neon) !important; }
  .g-iris{ stroke: var(--neon); stroke-width: 1.5; }

  @keyframes gFloat {
    0%{transform:translate3d(0,0,0)}
    50%{transform:translate3d(0,-3px,0)}
    100%{transform:translate3d(0,0,0)}
  }
  @keyframes gArmFloat {
    0%{transform:translate3d(0,0,0) rotate(0deg)}
    50%{transform:translate3d(0,-4px,0) rotate(-1.2deg)}
    100%{transform:translate3d(0,0,0) rotate(0deg)}
  }
  @keyframes gCrackPulse {
    0%{opacity:.55}
    50%{opacity:.92}
    100%{opacity:.55}
  }

  .g-body{
    transform-origin: 50% 65%;
    animation: gFloat 2.9s ease-in-out infinite;
    will-change: transform;
  }

  .g-arm{
    transform-origin: center;
    animation: gArmFloat 3.2s ease-in-out infinite;
    will-change: transform;
  }
  .g-arm--r{ animation-duration: 3.55s; animation-delay: -0.2s; }
  .g-arm--l{ animation-duration: 3.35s; animation-delay: -0.45s; }

  .g-crack-glow{
    animation: gCrackPulse 2.7s ease-in-out infinite;
    will-change: opacity;
  }

  @keyframes gEyeDrift {
    0%{transform:translate3d(0,0,0)}
    50%{transform:translate3d(1px,-1px,0)}
    100%{transform:translate3d(0,0,0)}
  }
  .g-eyes{
    transform-origin: 210px 170px;
    animation: gEyeDrift 2.9s ease-in-out infinite;
    will-change: transform;
  }

  .group:hover .g-body{ animation-duration: 1.75s; }
  .group:hover .g-arm{ animation-duration: 1.55s; }
  .group:hover .g-crack-glow{ animation-duration: 1.05s; }

  .group:hover .g-eyes{
    transform: translate3d(0,-1px,0) scale(1.02);
    animation-duration: 1.15s;
  }

  .group:hover .g-iris{
    transform-origin: center;
    transform: scaleY(0.96);
    transition: transform 140ms ease;
  }

  @media (prefers-reduced-motion: reduce) {
    .g-body,.g-arm,.g-crack-glow,.g-eyes{ animation:none !important; }
    .g-halo{ transition:none !important; }
  }
`;

/* ------------------------- Art: Golem (Mascot) -------------------------- */
/* (Unchanged, kept exactly as you pasted) */
export function Golem({ className }: { className?: string }) {
  useGlobalStyle("oh-golem-css", golemCss);

  const animDisabled =
    (import.meta as any).env?.VITE_DISABLE_ART_ANIM === "1" ||
    (typeof window !== "undefined" &&
      window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches);

  return (
    <HoverTilt className={clsx("golem group", className)} intensity={10}>
      <div className="relative select-none" style={{ contain: "layout paint" }}>
        <div
          aria-hidden
          className="g-halo absolute inset-0 -z-10 rounded-[32px]"
        />

        <svg
          className="g-svg block h-full w-full"
          viewBox="0 0 420 420"
          fill="none"
        >
          <defs>
            <filter id="gNeon" x="-60%" y="-60%" width="220%" height="220%">
              <feGaussianBlur stdDeviation="7" result="b" />
              <feColorMatrix
                in="b"
                type="matrix"
                values="
                  0 0 0 0 0
                  0 0 0 0 0.85
                  0 0 0 0 1
                  0 0 0 0.95 0"
                result="blue"
              />
              <feMerge>
                <feMergeNode in="blue" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <filter id="gShadow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="12" result="s" />
              <feOffset dy="10" result="o" />
              <feColorMatrix
                in="o"
                type="matrix"
                values="
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0 0
                  0 0 0 0.35 0"
                result="shadow"
              />
              <feMerge>
                <feMergeNode in="shadow" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>

            <radialGradient id="gStone" cx="42%" cy="28%" r="85%">
              <stop offset="0" stopColor="#1E3B5C" />
              <stop offset="0.55" stopColor="#0D1C2F" />
              <stop offset="1" stopColor="#070C14" />
            </radialGradient>

            <linearGradient id="gIris" x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" stopColor="#7DF9FF" stopOpacity="1" />
              <stop offset="0.55" stopColor="#00D9FF" stopOpacity="0.95" />
              <stop offset="1" stopColor="#00A6FF" stopOpacity="0.75" />
            </linearGradient>

            <radialGradient id="gEyeBloom" cx="35%" cy="35%" r="70%">
              <stop offset="0" stopColor="#FFFFFF" stopOpacity="0.85" />
              <stop offset="0.35" stopColor="#CFF7FF" stopOpacity="0.25" />
              <stop offset="1" stopColor="#00D9FF" stopOpacity="0.0" />
            </radialGradient>

            <linearGradient id="gOutline" x1="0" y1="0" x2="1" y2="1">
              <stop offset="0" stopColor="#050B12" />
              <stop offset="1" stopColor="#050B12" />
            </linearGradient>
          </defs>

          <g filter="url(#gShadow)">
            <ellipse
              cx="210"
              cy="350"
              rx="122"
              ry="24"
              fill="#000"
              opacity="0.26"
            />

            <path
              className="g-body"
              d="
                M210 80
                C265 80 304 110 312 154
                C336 170 350 196 350 230
                C350 304 286 352 210 352
                C134 352 70 304 70 230
                C70 196 84 170 108 154
                C116 110 155 80 210 80
                Z
              "
              fill="url(#gStone)"
              stroke="url(#gOutline)"
              strokeWidth="12"
              strokeLinejoin="round"
            />

            <g className="g-cracks">
              <path
                className="g-crack-cut"
                d="M90 216 L128 230 L156 218 L186 236 L214 216 L246 238 L282 222 L314 238"
                stroke="url(#gOutline)"
                strokeWidth="12"
                strokeLinecap="round"
                strokeLinejoin="round"
                fill="none"
                opacity="0.95"
              />
              <path
                className="g-crack-glow"
                d="M90 216 L128 230 L156 218 L186 236 L214 216 L246 238 L282 222 L314 238"
                stroke="#00D9FF"
                strokeWidth="6"
                strokeLinecap="round"
                strokeLinejoin="round"
                fill="none"
                opacity="0.78"
                filter="url(#gNeon)"
              />
            </g>

            <g className="g-arms">
              <g className="g-arm g-arm--l">
                <path
                  d="M86 208
                     C66 218 58 240 68 258
                     C80 280 108 282 126 268
                     C144 254 146 228 132 214
                     C118 200 102 198 86 208 Z"
                  fill="url(#gStone)"
                  stroke="url(#gOutline)"
                  strokeWidth="12"
                  strokeLinejoin="round"
                />
                <path
                  className="g-arm-rim"
                  d="M86 208
                     C66 218 58 240 68 258"
                  fill="none"
                  stroke="#00D9FF"
                  strokeWidth="5"
                  strokeLinecap="round"
                  opacity="0.18"
                  filter="url(#gNeon)"
                />
              </g>

              <g className="g-arm g-arm--r">
                <path
                  d="M334 208
                     C354 218 362 240 352 258
                     C340 280 312 282 294 268
                     C276 254 274 228 288 214
                     C302 200 318 198 334 208 Z"
                  fill="url(#gStone)"
                  stroke="url(#gOutline)"
                  strokeWidth="12"
                  strokeLinejoin="round"
                />
                <path
                  className="g-arm-rim"
                  d="M334 208
                     C354 218 362 240 352 258"
                  fill="none"
                  stroke="#00D9FF"
                  strokeWidth="5"
                  strokeLinecap="round"
                  opacity="0.18"
                  filter="url(#gNeon)"
                />
              </g>
            </g>

            <g className="g-eyes">
              <g className="g-eye">
                <ellipse
                  className="g-socket"
                  cx="182"
                  cy="170"
                  rx="20"
                  ry="22"
                  fill="#06101A"
                />
                <ellipse
                  className="g-iris"
                  cx="182"
                  cy="170"
                  rx="11"
                  ry="12"
                  fill="var(--neon)"
                  filter="url(#gNeon)"
                />
                <ellipse
                  cx="182"
                  cy="170"
                  rx="18"
                  ry="18"
                  fill="var(--neon)"
                  opacity="0.7"
                />
                <circle
                  cx="176"
                  cy="164"
                  r="4.8"
                  fill="#EFFFFF"
                  opacity="0.9"
                />
                {!animDisabled && (
                  <animate
                    attributeName="ry"
                    values="22;22;22;2;22;22"
                    dur="4.6s"
                    repeatCount="indefinite"
                  />
                )}
              </g>

              <g className="g-eye">
                <ellipse
                  className="g-socket"
                  cx="240"
                  cy="170"
                  rx="20"
                  ry="22"
                  fill="#06101A"
                />
                <ellipse
                  className="g-iris"
                  cx="240"
                  cy="170"
                  rx="11"
                  ry="12"
                  fill="var(--neon)"
                  filter="url(#gNeon)"
                />
                <ellipse
                  cx="240"
                  cy="170"
                  rx="18"
                  ry="18"
                  fill="var(--neon)"
                  opacity="0.7"
                />
                <circle
                  cx="234"
                  cy="164"
                  r="4.8"
                  fill="#EFFFFF"
                  opacity="0.9"
                />
                {!animDisabled && (
                  <animate
                    attributeName="ry"
                    values="22;22;22;2;22;22"
                    dur="4.9s"
                    repeatCount="indefinite"
                  />
                )}
              </g>
            </g>
          </g>
        </svg>
      </div>
    </HoverTilt>
  );
}
