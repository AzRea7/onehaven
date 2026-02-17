// frontend/src/components/PageHero.tsx
import React from "react";
import clsx from "clsx";
import { HoverTilt } from "./Artwork";

/**
 * PageHero
 * - OpenClaw-ish “eyebrow + headline + subhead + right hero art”
 * - Right side supports absolute-positioned content (your BrickBuilder wrapper)
 * - Optional HoverTilt on the art container for a subtle “alive” feel
 * - Perf safe: no state loops, no heavy effects
 */
export default function PageHero({
  eyebrow,
  title,
  subtitle,
  right,
  actions,
  className,
  tilt = true,
}: {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  right?: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
  tilt?: boolean;
}) {
  const RightShell = ({ children }: { children: React.ReactNode }) => {
    if (!tilt) {
      return (
        <div className="relative h-[140px] w-[240px] md:h-[160px] md:w-[300px] overflow-visible">
          {children}
        </div>
      );
    }
    return (
      <HoverTilt className="relative h-[140px] w-[240px] md:h-[160px] md:w-[300px] overflow-visible">
        {children}
      </HoverTilt>
    );
  };

  return (
    <div className={clsx("gradient-border rounded-3xl p-[1px]", className)}>
      <div className="glass rounded-3xl px-6 py-6 md:px-7 md:py-7">
        <div className="flex items-start justify-between gap-6 flex-wrap">
          <div className="max-w-2xl">
            {eyebrow && (
              <div className="text-[11px] uppercase tracking-[0.22em] text-zinc-400">
                {eyebrow}
              </div>
            )}

            <div className="mt-1 text-2xl md:text-3xl font-semibold tracking-tight text-white">
              {title}
            </div>

            {subtitle && (
              <div className="mt-2 text-sm text-zinc-400 leading-relaxed">
                {subtitle}
              </div>
            )}

            {actions && (
              <div className="mt-4 flex items-center gap-2 flex-wrap">
                {actions}
              </div>
            )}
          </div>

          {right && (
            <RightShell>
              {/* This wrapper makes your `absolute inset-0 ...` layout work */}
              <div className="relative w-full h-full">{right}</div>
            </RightShell>
          )}
        </div>
      </div>
    </div>
  );
}
