import React from "react";
import clsx from "clsx";

export default function PageShell({
  title,
  subtitle,
  right,
  children,
  className,
}: {
  title?: string;
  subtitle?: string;
  right?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={clsx(
        "mx-auto w-full max-w-[1200px] px-4 md:px-6 py-6",
        className,
      )}
    >
      {title || subtitle || right ? (
        <div className="mb-5 flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            {title ? (
              <div className="text-xl font-semibold tracking-tight text-white">
                {title}
              </div>
            ) : null}
            {subtitle ? (
              <div className="text-sm text-white/55 mt-1">{subtitle}</div>
            ) : null}
          </div>
          {right ? (
            <div className="flex items-center gap-2">{right}</div>
          ) : null}
        </div>
      ) : null}

      {children}
    </div>
  );
}
