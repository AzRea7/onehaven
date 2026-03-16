import React from "react";
import clsx from "clsx";

export default function PageShell({
  title,
  subtitle,
  right,
  children,
  className,
  contentClassName,
}: {
  title?: string;
  subtitle?: string;
  right?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  contentClassName?: string;
}) {
  return (
    <section
      className={clsx(
        "mx-auto w-full max-w-[1320px] px-4 py-5 sm:px-5 lg:px-8 lg:py-8",
        className,
      )}
    >
      {title || subtitle || right ? (
        <div className="mb-6 flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="min-w-0">
            {title ? (
              <h1 className="text-balance text-2xl font-semibold tracking-tight text-app-0 sm:text-3xl">
                {title}
              </h1>
            ) : null}
            {subtitle ? (
              <p className="mt-2 max-w-3xl text-sm leading-6 text-app-3 sm:text-[15px]">
                {subtitle}
              </p>
            ) : null}
          </div>

          {right ? (
            <div className="flex shrink-0 items-center gap-2">{right}</div>
          ) : null}
        </div>
      ) : null}

      <div className={clsx("min-w-0", contentClassName)}>{children}</div>
    </section>
  );
}
