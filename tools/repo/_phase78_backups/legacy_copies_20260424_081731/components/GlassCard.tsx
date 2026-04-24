import type { ReactNode } from "react";
import clsx from "clsx";

export default function GlassCard({
  children,
  className,
  hover = true,
  padded = true,
}: {
  children: ReactNode;
  className?: string;
  hover?: boolean;
  padded?: boolean;
}) {
  return (
    <div
      className={clsx(
        "gradient-border rounded-2xl",
        "glass",
        hover ? "glass-hover" : "",
        padded ? "p-6 md:p-7" : "",
        "relative overflow-visible", // don’t clip artwork
        className,
      )}
    >
      {children}
    </div>
  );
}
