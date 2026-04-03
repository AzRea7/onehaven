export function getFinancingType(price?: number | null) {
  const n = Number(price);
  if (!Number.isFinite(n) || n <= 0) return "unknown";
  return n < 75000 ? "CASH DEAL" : "DSCR LOAN";
}

export function canStartAcquisition(row: any) {
  const gate =
    row?.workflow?.constraints?.acquisition?.start_gate ||
    row?.constraints?.acquisition?.start_gate;
  return Boolean(gate?.ok);
}

export function startAcquisitionBlockers(row: any): string[] {
  const gate =
    row?.workflow?.constraints?.acquisition?.start_gate ||
    row?.constraints?.acquisition?.start_gate;
  return Array.isArray(gate?.blockers) ? gate.blockers : [];
}

export function acquisitionStageLabel(stage?: string | null) {
  const value = String(stage || "")
    .trim()
    .toLowerCase();
  const map: Record<string, string> = {
    pursuing: "Pursuing",
    offer_prep: "Offer prep",
    offer_ready: "Offer ready",
    offer_submitted: "Offer submitted",
    negotiating: "Negotiating",
    under_contract: "Under contract",
    due_diligence: "Due diligence",
    closing: "Closing",
    owned: "Owned",
  };
  return map[value] || (value ? value.replace(/_/g, " ") : "Investor");
}
