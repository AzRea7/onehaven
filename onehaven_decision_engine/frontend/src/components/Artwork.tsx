import clsx from "clsx";

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
          <stop offset="35%" stopColor="rgba(99,102,241,0.45)" />
          <stop offset="70%" stopColor="rgba(168,85,247,0.22)" />
          <stop offset="100%" stopColor="rgba(0,0,0,0)" />
        </radialGradient>
        <linearGradient id="orbRing" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="rgba(99,102,241,0.9)" />
          <stop offset="50%" stopColor="rgba(168,85,247,0.9)" />
          <stop offset="100%" stopColor="rgba(34,197,94,0.85)" />
        </linearGradient>
        <filter id="glow">
          <feGaussianBlur stdDeviation="6" result="blur" />
          <feColorMatrix
            in="blur"
            type="matrix"
            values="
              1 0 0 0 0
              0 1 0 0 0
              0 0 1 0 0
              0 0 0 0.8 0"
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

      {/* ring */}
      <g filter="url(#glow)" opacity="0.95">
        <path
          d="M70 230C90 120 170 70 250 80C330 90 370 160 350 240C330 320 250 360 170 345C90 330 50 290 70 230Z"
          stroke="url(#orbRing)"
          strokeWidth="4"
          fill="none"
        />
      </g>

      {/* ticks */}
      {Array.from({ length: 22 }).map((_, i) => {
        const a = (i / 22) * Math.PI * 2;
        const r1 = 178;
        const r2 = i % 2 === 0 ? 194 : 188;
        const x1 = 210 + Math.cos(a) * r1;
        const y1 = 210 + Math.sin(a) * r1;
        const x2 = 210 + Math.cos(a) * r2;
        const y2 = 210 + Math.sin(a) * r2;
        return (
          <line
            key={i}
            x1={x1}
            y1={y1}
            x2={x2}
            y2={y2}
            stroke="rgba(255,255,255,0.18)"
            strokeWidth={i % 2 === 0 ? 2 : 1}
          />
        );
      })}

      {/* “signal” arc */}
      <path
        d="M140 255C160 290 195 310 230 305C275 298 305 260 300 220"
        stroke="rgba(255,255,255,0.22)"
        strokeWidth="3"
        strokeLinecap="round"
      />
    </svg>
  );
}

export function Section8Badge({ className }: { className?: string }) {
  return (
    <svg
      className={clsx("w-full h-full", className)}
      viewBox="0 0 520 260"
      fill="none"
    >
      <defs>
        <linearGradient id="g" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="rgba(34,197,94,0.90)" />
          <stop offset="45%" stopColor="rgba(99,102,241,0.85)" />
          <stop offset="100%" stopColor="rgba(168,85,247,0.85)" />
        </linearGradient>
      </defs>

      <rect
        x="18"
        y="18"
        width="484"
        height="224"
        rx="28"
        fill="rgba(255,255,255,0.04)"
        stroke="rgba(255,255,255,0.10)"
      />
      <rect
        x="38"
        y="38"
        width="444"
        height="184"
        rx="22"
        fill="rgba(0,0,0,0)"
        stroke="url(#g)"
        strokeWidth="2"
        opacity="0.8"
      />

      {/* shield */}
      <path
        d="M260 55C305 72 345 74 380 70V128C380 170 338 205 260 220C182 205 140 170 140 128V70C175 74 215 72 260 55Z"
        fill="rgba(255,255,255,0.05)"
        stroke="rgba(255,255,255,0.16)"
      />

      {/* check */}
      <path
        d="M205 136L245 170L320 96"
        stroke="url(#g)"
        strokeWidth="10"
        strokeLinecap="round"
        strokeLinejoin="round"
      />

      {/* text-ish lines */}
      <path
        d="M78 92H190"
        stroke="rgba(255,255,255,0.16)"
        strokeWidth="3"
        strokeLinecap="round"
      />
      <path
        d="M78 126H170"
        stroke="rgba(255,255,255,0.12)"
        strokeWidth="3"
        strokeLinecap="round"
      />
      <path
        d="M78 160H200"
        stroke="rgba(255,255,255,0.10)"
        strokeWidth="3"
        strokeLinecap="round"
      />

      <path
        d="M330 120H440"
        stroke="rgba(255,255,255,0.14)"
        strokeWidth="3"
        strokeLinecap="round"
      />
      <path
        d="M330 154H420"
        stroke="rgba(255,255,255,0.10)"
        strokeWidth="3"
        strokeLinecap="round"
      />
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
        <linearGradient id="claw" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="rgba(99,102,241,0.9)" />
          <stop offset="50%" stopColor="rgba(168,85,247,0.9)" />
          <stop offset="100%" stopColor="rgba(34,197,94,0.85)" />
        </linearGradient>
        <radialGradient id="eye" cx="50%" cy="50%" r="50%">
          <stop offset="0%" stopColor="rgba(255,255,255,0.9)" />
          <stop offset="100%" stopColor="rgba(255,255,255,0.2)" />
        </radialGradient>
      </defs>

      {/* body */}
      <path
        d="M120 250C120 185 155 140 210 140C265 140 300 185 300 250C300 305 265 340 210 340C155 340 120 305 120 250Z"
        fill="rgba(255,255,255,0.04)"
        stroke="rgba(255,255,255,0.12)"
      />

      {/* eyes */}
      <circle cx="180" cy="230" r="18" fill="url(#eye)" opacity="0.85" />
      <circle cx="240" cy="230" r="18" fill="url(#eye)" opacity="0.85" />
      <circle cx="180" cy="230" r="6" fill="rgba(0,0,0,0.55)" />
      <circle cx="240" cy="230" r="6" fill="rgba(0,0,0,0.55)" />

      {/* left claw */}
      <path
        d="M105 240C70 225 60 190 85 165C110 140 150 160 155 195C160 230 135 255 105 240Z"
        fill="rgba(255,255,255,0.03)"
        stroke="url(#claw)"
        strokeWidth="3"
      />
      {/* right claw */}
      <path
        d="M315 240C350 225 360 190 335 165C310 140 270 160 265 195C260 230 285 255 315 240Z"
        fill="rgba(255,255,255,0.03)"
        stroke="url(#claw)"
        strokeWidth="3"
      />

      {/* antenna / signal */}
      <path
        d="M210 140V95"
        stroke="rgba(255,255,255,0.18)"
        strokeWidth="3"
        strokeLinecap="round"
      />
      <circle
        cx="210"
        cy="85"
        r="10"
        stroke="url(#claw)"
        strokeWidth="3"
        fill="rgba(255,255,255,0.04)"
      />
      <path
        d="M175 92C188 75 202 68 210 68C218 68 232 75 245 92"
        stroke="rgba(255,255,255,0.12)"
        strokeWidth="3"
        strokeLinecap="round"
      />
    </svg>
  );
}
