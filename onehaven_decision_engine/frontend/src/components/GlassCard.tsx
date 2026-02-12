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
      className={clsx("glass rounded-2xl p-5", className)}
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.45, ease: "easeOut" }}
      whileHover={hover ? { y: -2, scale: 1.01 } : undefined}
    >
      {children}
    </motion.div>
  );
}
