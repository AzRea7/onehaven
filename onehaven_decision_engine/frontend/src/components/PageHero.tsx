import React from "react";
import clsx from "clsx";

export default function PageHero({
  eyebrow,
  title,
  subtitle,
  right,
  actions,
  className,
}: {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  right?: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
}) {
  return (
    <section className={clsx("hero-shell", className)}>
      <div className="hero-grid">
        <div className="min-w-0">
          {eyebrow ? <div className="hero-eyebrow">{eyebrow}</div> : null}

          <h1 className="hero-title">{title}</h1>

          {subtitle ? <p className="hero-subtitle">{subtitle}</p> : null}

          {actions ? (
            <div className="mt-5 flex flex-wrap items-center gap-2.5">
              {actions}
            </div>
          ) : null}
        </div>

        {right ? (
          <div className="hero-aside">
            <div className="hero-aside-surface">{right}</div>
          </div>
        ) : null}
      </div>

      <div className="hero-glow hero-glow-a" />
      <div className="hero-glow hero-glow-b" />
    </section>
  );
}
