import { motion } from "framer-motion";

export default function AuroraBackground() {
  // Smaller layers + slightly lower blur = dramatically faster.
  // Still feels like tinted black, just not a GPU furnace.
  return (
    <div className="absolute inset-0 overflow-hidden">
      <motion.div
        className="absolute -top-44 -left-44 h-[520px] w-[520px] rounded-full opacity-30"
        style={{
          background:
            "radial-gradient(circle at 30% 30%, rgba(120,90,255,0.80), rgba(120,90,255,0) 62%)",
          filter: "blur(42px)",
          willChange: "transform",
        }}
        animate={{ x: [0, 28, -8, 0], y: [0, -14, 10, 0] }}
        transition={{ duration: 18, repeat: Infinity, ease: "easeInOut" }}
      />

      <motion.div
        className="absolute top-12 -right-52 h-[560px] w-[560px] rounded-full opacity-22"
        style={{
          background:
            "radial-gradient(circle at 70% 40%, rgba(255,88,122,0.75), rgba(255,88,122,0) 62%)",
          filter: "blur(46px)",
          willChange: "transform",
        }}
        animate={{ x: [0, -34, 14, 0], y: [0, 18, -8, 0] }}
        transition={{ duration: 22, repeat: Infinity, ease: "easeInOut" }}
      />

      <motion.div
        className="absolute -bottom-64 left-1/3 h-[620px] w-[620px] rounded-full opacity-18"
        style={{
          background:
            "radial-gradient(circle at 45% 60%, rgba(35,255,200,0.52), rgba(35,255,200,0) 64%)",
          filter: "blur(52px)",
          willChange: "transform",
        }}
        animate={{ x: [0, 20, -18, 0], y: [0, -10, 14, 0] }}
        transition={{ duration: 26, repeat: Infinity, ease: "easeInOut" }}
      />

      <div className="absolute inset-0 bg-gradient-to-b from-black/10 via-black/45 to-black/80" />
      <div className="noise" />
    </div>
  );
}
