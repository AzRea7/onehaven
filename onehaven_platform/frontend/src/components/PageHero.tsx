import React from "react";
import clsx from "clsx";

export default function PageHero({
  eyebrow,
  title,
  subtitle,
  right,
  actions,
  className,
  tilt = false,
}: {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  right?: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
  tilt?: boolean;
}) {
  return (
    <section
      className={clsx("oh-hero-shell", tilt && "oh-hero-tilt", className)}
    >
      <div className="oh-hero-grid">
        <div className="min-w-0">
          {eyebrow ? <div className="oh-hero-eyebrow">{eyebrow}</div> : null}

          <h1 className="oh-hero-title">{title}</h1>

          {subtitle ? <p className="oh-hero-subtitle">{subtitle}</p> : null}

          {actions ? (
            <div className="mt-4 flex flex-wrap items-center gap-2.5">
              {actions}
            </div>
          ) : null}
        </div>

        {right ? (
          <div className="oh-hero-aside">
            <div className="oh-hero-aside-surface">{right}</div>
          </div>
        ) : null}
      </div>
    </section>
  );
}
