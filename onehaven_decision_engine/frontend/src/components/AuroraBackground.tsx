import React from "react";
import { motion } from "framer-motion";

export default function AuroraBackground() {
  return (
    <div className="absolute inset-0 overflow-hidden">
      {/* subtle grid */}
      <div className="absolute inset-0 subtle-grid opacity-[0.35]" />

      {/* aurora blobs (original shapes, not copied assets) */}
      <motion.div
        className="absolute -top-40 -left-40 h-[520px] w-[520px] rounded-full blur-3xl opacity-35"
        style={{
          background:
            "radial-gradient(circle at 30% 30%, rgba(59,130,246,0.9), rgba(59,130,246,0) 60%)",
        }}
        animate={{ x: [0, 40, -10, 0], y: [0, -20, 10, 0] }}
        transition={{ duration: 14, repeat: Infinity, ease: "easeInOut" }}
      />
      <motion.div
        className="absolute top-10 -right-48 h-[560px] w-[560px] rounded-full blur-3xl opacity-30"
        style={{
          background:
            "radial-gradient(circle at 70% 40%, rgba(168,85,247,0.9), rgba(168,85,247,0) 60%)",
        }}
        animate={{ x: [0, -35, 15, 0], y: [0, 25, -10, 0] }}
        transition={{ duration: 16, repeat: Infinity, ease: "easeInOut" }}
      />
      <motion.div
        className="absolute -bottom-56 left-1/3 h-[640px] w-[640px] rounded-full blur-3xl opacity-25"
        style={{
          background:
            "radial-gradient(circle at 40% 60%, rgba(34,197,94,0.75), rgba(34,197,94,0) 62%)",
        }}
        animate={{ x: [0, 20, -25, 0], y: [0, -15, 20, 0] }}
        transition={{ duration: 18, repeat: Infinity, ease: "easeInOut" }}
      />

      {/* vignette */}
      <div className="absolute inset-0 bg-gradient-to-b from-zinc-950/10 via-zinc-950/55 to-zinc-950" />

      {/* grain */}
      <div className="noise" />
    </div>
  );
}
