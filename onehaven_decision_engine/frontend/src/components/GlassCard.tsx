import { motion } from "framer-motion";
import clsx from "clsx";
import type { ReactNode } from "react";

export default function GlassCard({
  children,
  className,
  hover = true,
}: {
  children: ReactNode;
  className?: string;
  hover?: boolean;
}) {
  return (
    <motion.div
      className={clsx(
        "gradient-border rounded-2xl",
        "glass glass-hover",
        "p-6 md:p-7",
        "relative overflow-visible", // IMPORTANT: don't clip artwork
        className,
      )}
      initial={{ opacity: 0, y: 14 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.42, ease: "easeOut" }}
      whileHover={hover ? { y: -3, scale: 1.012 } : undefined}
    >
      {children}
    </motion.div>
  );
}
