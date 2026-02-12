import { useEffect, useRef } from "react";

export default function AnimatedBackdrop() {
  const ref = useRef<HTMLDivElement | null>(null);
  const raf = useRef<number | null>(null);
  const last = useRef<{ x: number; y: number } | null>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const apply = () => {
      raf.current = null;
      const v = last.current;
      if (!v) return;
      el.style.setProperty("--x", `${v.x}%`);
      el.style.setProperty("--y", `${v.y}%`);
    };

    const onMove = (e: PointerEvent) => {
      const r = el.getBoundingClientRect();
      const x = ((e.clientX - r.left) / r.width) * 100;
      const y = ((e.clientY - r.top) / r.height) * 100;
      last.current = { x, y };

      // Only update once per animation frame (60fps max)
      if (raf.current == null) raf.current = requestAnimationFrame(apply);
    };

    window.addEventListener("pointermove", onMove, { passive: true });
    return () => {
      window.removeEventListener("pointermove", onMove);
      if (raf.current != null) cancelAnimationFrame(raf.current);
    };
  }, []);

  return (
    <div
      ref={ref}
      className="pointer-events-none absolute inset-0 neon-ring"
      aria-hidden="true"
      style={{
        // Helps compositor keep it on GPU
        willChange: "background",
      }}
    >
      <div className="absolute inset-0 bg-gradient-to-b from-black/25 via-black/35 to-black/70" />
      <div className="noise" />
    </div>
  );
}
