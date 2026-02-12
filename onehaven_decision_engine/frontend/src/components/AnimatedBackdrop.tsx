import { useEffect, useRef } from "react";

export default function AnimatedBackdrop() {
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const onMove = (e: MouseEvent) => {
      const r = el.getBoundingClientRect();
      const x = ((e.clientX - r.left) / r.width) * 100;
      const y = ((e.clientY - r.top) / r.height) * 100;
      el.style.setProperty("--x", `${x}%`);
      el.style.setProperty("--y", `${y}%`);
    };

    window.addEventListener("mousemove", onMove);
    return () => window.removeEventListener("mousemove", onMove);
  }, []);

  return (
    <div
      ref={ref}
      className="pointer-events-none absolute inset-0 overflow-hidden"
    >
      {/* base gradient */}
      <div className="absolute inset-0 bg-gradient-to-b from-[#060716] via-[#05060a] to-[#05060a]" />

      {/* interactive “neon ring” */}
      <div className="absolute inset-0 neon-ring" />

      {/* slow animated gradient haze */}
      <div
        className="absolute -inset-[40%] opacity-50 blur-3xl animate-shimmer"
        style={{
          backgroundImage:
            "linear-gradient(90deg, rgba(99,102,241,0.25), rgba(168,85,247,0.18), rgba(34,197,94,0.12), rgba(99,102,241,0.25))",
          backgroundSize: "200% 200%",
        }}
      />

      {/* faint grid */}
      <svg
        className="absolute inset-0 opacity-[0.10]"
        viewBox="0 0 1200 800"
        preserveAspectRatio="none"
      >
        <defs>
          <pattern
            id="grid"
            width="40"
            height="40"
            patternUnits="userSpaceOnUse"
          >
            <path
              d="M 40 0 L 0 0 0 40"
              fill="none"
              stroke="white"
              strokeWidth="1"
              opacity="0.35"
            />
          </pattern>
          <radialGradient id="fade" cx="50%" cy="30%" r="70%">
            <stop offset="0%" stopColor="white" stopOpacity="0.22" />
            <stop offset="100%" stopColor="white" stopOpacity="0" />
          </radialGradient>
        </defs>
        <rect width="1200" height="800" fill="url(#grid)" />
        <rect width="1200" height="800" fill="url(#fade)" />
      </svg>

      {/* vignette */}
      <div className="absolute inset-0 bg-gradient-to-t from-black/55 via-transparent to-black/40" />
    </div>
  );
}
