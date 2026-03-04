export function getFinancingType(price?: number | null) {
  const n = Number(price);
  if (!Number.isFinite(n) || n <= 0) return "unknown";
  return n < 75000 ? "CASH DEAL" : "DSCR LOAN";
}
