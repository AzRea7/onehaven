import React from "react";
import clsx from "clsx";

type Tone = "default" | "accent" | "success" | "warning" | "danger";
type Padding = "sm" | "md" | "lg";

const toneClass: Record<Tone, string> = {
  default: "surface-default",
  accent: "surface-accent",
  success: "surface-success",
  warning: "surface-warning",
  danger: "surface-danger",
};

const paddingClass: Record<Padding, string> = {
  sm: "p-4",
  md: "p-5",
  lg: "p-6",
};

export default function Surface({
  title,
  subtitle,
  actions,
  children,
  className,
  tone = "default",
  padding = "md",
}: {
  title?: string;
  subtitle?: string;
  actions?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  tone?: Tone;
  padding?: Padding;
}) {
  return (
    <section
      className={clsx(
        "surface",
        toneClass[tone],
        paddingClass[padding],
        className,
      )}
    >
      {title || subtitle || actions ? (
        <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="min-w-0">
            {title ? <h2 className="surface-title">{title}</h2> : null}
            {subtitle ? <p className="surface-subtitle">{subtitle}</p> : null}
          </div>
          {actions ? (
            <div className="flex shrink-0 items-center gap-2">{actions}</div>
          ) : null}
        </div>
      ) : null}

      {children}
    </section>
  );
}
