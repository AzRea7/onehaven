import React from "react";
import { X, RefreshCcw, Wrench, RotateCcw, Database, Plus } from "lucide-react";
import { api } from "../lib/api";

type Market = {
  state: string;
  county?: string | null;
  city?: string | null;
  pha_name?: string | null;
};

type Props = {
  open: boolean;
  market: Market | null;
  onClose: () => void;
  onChanged?: () => Promise<void> | void;
};

const SOURCE_KIND_OPTIONS = [
  "municipal_registration",
  "municipal_inspection",
  "municipal_certificate",
  "municipal_enforcement",
  "municipal_ordinance",
  "municipal_building_anchor",
  "municipal_guidance",
  "pha_guidance",
  "pha_plan",
  "pha_notice",
  "federal_anchor",
  "state_anchor",
  "state_hcv_anchor",
];

function pretty(v: string | null | undefined) {
  return String(v || "—");
}

function locationLabel(market: Market | null) {
  if (!market) return "Source pack";
  return market.city || market.county || market.pha_name || market.state;
}

function locationSubLabel(market: Market | null) {
  if (!market) return "—";
  return [market.city, market.county, market.state].filter(Boolean).join(" • ");
}

export default function MarketSourcePackModal({
  open,
  market,
  onClose,
  onChanged,
}: Props) {
  const [loading, setLoading] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [data, setData] = React.useState<any | null>(null);
  const [err, setErr] = React.useState<string | null>(null);

  const [form, setForm] = React.useState({
    url: "",
    title: "",
    publisher: "",
    notes: "",
    source_kind: "municipal_guidance",
    is_authoritative: true,
    priority: 100,
  });

  const load = React.useCallback(async () => {
    if (!open || !market) return;

    setLoading(true);
    setErr(null);

    try {
      const out = await api.policyCatalogAdminMarket({
        state: market.state,
        county: market.county ?? null,
        city: market.city ?? null,
        pha_name: market.pha_name ?? null,
        org_scope: false,
        focus: "se_mi_extended",
      });
      setData(out);
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, [open, market]);

  React.useEffect(() => {
    load();
  }, [load]);

  React.useEffect(() => {
    if (!open) {
      setErr(null);
    }
  }, [open]);

  if (!open || !market) return null;

  const activeMarket = market;

  async function refresh() {
    await load();
    await onChanged?.();
  }

  async function bootstrap() {
    setSaving(true);
    setErr(null);
    try {
      await api.policyCatalogAdminBootstrap({
        state: activeMarket.state,
        county: activeMarket.county ?? null,
        city: activeMarket.city ?? null,
        pha_name: activeMarket.pha_name ?? null,
        org_scope: false,
        focus: "se_mi_extended",
      });
      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }

  async function resetToBaseline() {
    if (!window.confirm("Reset this market source pack to baseline?")) return;

    setSaving(true);
    setErr(null);
    try {
      await api.policyCatalogAdminReset({
        state: activeMarket.state,
        county: activeMarket.county ?? null,
        city: activeMarket.city ?? null,
        pha_name: activeMarket.pha_name ?? null,
        org_scope: false,
        focus: "se_mi_extended",
      });
      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }

  async function addSource() {
    if (!form.url.trim()) {
      setErr("URL is required.");
      return;
    }

    setSaving(true);
    setErr(null);

    try {
      await api.policyCatalogAdminCreateItem({
        state: activeMarket.state,
        county: activeMarket.county ?? null,
        city: activeMarket.city ?? null,
        pha_name: activeMarket.pha_name ?? null,
        org_scope: false,
        url: form.url.trim(),
        title: form.title.trim() || null,
        publisher: form.publisher.trim() || null,
        notes: form.notes.trim() || null,
        source_kind: form.source_kind,
        is_authoritative: form.is_authoritative,
        priority: Number(form.priority || 100),
      });

      setForm({
        url: "",
        title: "",
        publisher: "",
        notes: "",
        source_kind: "municipal_guidance",
        is_authoritative: true,
        priority: 100,
      });

      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }

  async function disableItem(item: any) {
    if (!window.confirm(`Disable "${item.title || item.url}"?`)) return;

    setSaving(true);
    setErr(null);
    try {
      await api.policyCatalogAdminDisableItem(item.id, {
        state: activeMarket.state,
        county: activeMarket.county ?? null,
        city: activeMarket.city ?? null,
        pha_name: activeMarket.pha_name ?? null,
        org_scope: false,
        focus: "se_mi_extended",
      });
      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }

  async function quickPriority(item: any, nextPriority: number) {
    setSaving(true);
    setErr(null);
    try {
      await api.policyCatalogAdminUpdateItem(item.id, {
        org_scope: false,
        priority: nextPriority,
      });
      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }

  async function runRepair() {
    setSaving(true);
    setErr(null);
    try {
      await api.policyRepairMarket({
        state: activeMarket.state,
        county: activeMarket.county ?? null,
        city: activeMarket.city ?? null,
        pha_name: activeMarket.pha_name ?? null,
        org_scope: false,
        focus: "se_mi_extended",
        archive_extracted_duplicates: true,
      });
      await refresh();
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  }

  const mergedItems = Array.isArray(data?.merged_items)
    ? data.merged_items
    : [];
  const editableItems = Array.isArray(data?.editable_items)
    ? data.editable_items
    : [];
  const coverage = data?.coverage || {};
  const missing = Array.isArray(coverage?.missing) ? coverage.missing : [];
  const counts = coverage?.counts || {};

  return (
    <>
      <div
        className="fixed inset-0 z-40 bg-black/40 backdrop-blur-[1px]"
        onClick={onClose}
      />

      <div
        className="fixed right-3 z-50 w-[760px] max-w-[calc(100vw-24px)]"
        style={{
          top: "calc(var(--oh-header-h, 0px) + 12px)",
          height: "calc(100vh - var(--oh-header-h, 0px) - 24px)",
        }}
      >
        <div
          className="flex h-full flex-col overflow-hidden rounded-3xl border border-app bg-app-panel shadow-2xl"
          onClick={(e) => e.stopPropagation()}
        >
          <div className="border-b border-app bg-app-panel px-5 py-4">
            <div className="flex items-start justify-between gap-4">
              <div className="min-w-0">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Source pack editor
                </div>
                <h2 className="mt-2 truncate text-xl font-semibold text-app-0">
                  {locationLabel(activeMarket)}
                </h2>
                <div className="mt-1 text-sm text-app-4">
                  {locationSubLabel(activeMarket)}
                </div>
              </div>

              <button
                type="button"
                onClick={onClose}
                className="inline-flex h-10 w-10 items-center justify-center rounded-2xl border border-app bg-app-muted text-app-2 transition hover:bg-app"
                aria-label="Close source pack"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>

          <div className="flex-1 overflow-y-auto px-5 py-5">
            {err ? (
              <div className="mb-4 rounded-2xl border border-red-500/30 bg-red-500/10 px-3 py-3 text-sm text-red-200">
                {err}
              </div>
            ) : null}

            <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
              <div className="rounded-2xl border border-app bg-app px-4 py-4">
                <div className="text-xs text-app-4">Merged items</div>
                <div className="mt-2 text-2xl font-semibold text-app-0">
                  {mergedItems.length}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app px-4 py-4">
                <div className="text-xs text-app-4">Editable DB items</div>
                <div className="mt-2 text-2xl font-semibold text-app-0">
                  {editableItems.length}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app px-4 py-4">
                <div className="text-xs text-app-4">Covered kinds</div>
                <div className="mt-2 text-2xl font-semibold text-app-0">
                  {Object.keys(counts).length}
                </div>
              </div>

              <div className="rounded-2xl border border-app bg-app px-4 py-4">
                <div className="text-xs text-app-4">Missing kinds</div>
                <div className="mt-2 text-2xl font-semibold text-amber-300">
                  {missing.length}
                </div>
              </div>
            </div>

            <div className="mt-4 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={bootstrap}
                disabled={saving}
                className="inline-flex items-center gap-2 rounded-2xl border border-cyan-500/30 bg-cyan-500/10 px-3 py-2 text-sm text-cyan-200 transition hover:bg-cyan-500/15 disabled:opacity-50"
              >
                <Database className="h-4 w-4" />
                Bootstrap baseline
              </button>

              <button
                type="button"
                onClick={resetToBaseline}
                disabled={saving}
                className="inline-flex items-center gap-2 rounded-2xl border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-sm text-amber-200 transition hover:bg-amber-500/15 disabled:opacity-50"
              >
                <RotateCcw className="h-4 w-4" />
                Reset to baseline
              </button>

              <button
                type="button"
                onClick={runRepair}
                disabled={saving}
                className="inline-flex items-center gap-2 rounded-2xl border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-sm text-emerald-200 transition hover:bg-emerald-500/15 disabled:opacity-50"
              >
                <Wrench className="h-4 w-4" />
                Repair market
              </button>

              <button
                type="button"
                onClick={refresh}
                disabled={loading || saving}
                className="inline-flex items-center gap-2 rounded-2xl border border-app bg-app-muted px-3 py-2 text-sm text-app-1 transition hover:bg-app disabled:opacity-50"
              >
                <RefreshCcw className="h-4 w-4" />
                Refresh
              </button>
            </div>

            <div className="mt-4 rounded-2xl border border-app bg-app px-4 py-4">
              <div className="text-sm font-semibold text-app-0">
                Coverage by source kind
              </div>

              <div className="mt-3 flex flex-wrap gap-2">
                {Object.entries(counts).map(([key, value]) => (
                  <span
                    key={key}
                    className="rounded-full border border-app bg-app-muted px-3 py-1 text-xs text-app-1"
                  >
                    {key}: {String(value)}
                  </span>
                ))}

                {missing.map((m: string) => (
                  <span
                    key={m}
                    className="rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-xs text-amber-200"
                  >
                    missing: {m}
                  </span>
                ))}
              </div>
            </div>

            <div className="mt-6 rounded-2xl border border-app bg-app px-4 py-4">
              <div className="text-sm font-semibold text-app-0">Add source</div>

              <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
                <input
                  value={form.url}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, url: e.target.value }))
                  }
                  placeholder="Official source URL"
                  className="rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-0 outline-none placeholder:text-app-4"
                />

                <input
                  value={form.title}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, title: e.target.value }))
                  }
                  placeholder="Title"
                  className="rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-0 outline-none placeholder:text-app-4"
                />

                <input
                  value={form.publisher}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, publisher: e.target.value }))
                  }
                  placeholder="Publisher"
                  className="rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-0 outline-none placeholder:text-app-4"
                />

                <select
                  value={form.source_kind}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, source_kind: e.target.value }))
                  }
                  className="rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-0 outline-none"
                >
                  {SOURCE_KIND_OPTIONS.map((opt) => (
                    <option key={opt} value={opt}>
                      {opt}
                    </option>
                  ))}
                </select>

                <input
                  type="number"
                  value={form.priority}
                  onChange={(e) =>
                    setForm((f) => ({
                      ...f,
                      priority: Number(e.target.value || 100),
                    }))
                  }
                  placeholder="Priority"
                  className="rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-0 outline-none placeholder:text-app-4"
                />

                <label className="flex items-center gap-2 rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-1">
                  <input
                    type="checkbox"
                    checked={form.is_authoritative}
                    onChange={(e) =>
                      setForm((f) => ({
                        ...f,
                        is_authoritative: e.target.checked,
                      }))
                    }
                  />
                  authoritative
                </label>

                <textarea
                  value={form.notes}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, notes: e.target.value }))
                  }
                  placeholder="Notes"
                  className="min-h-[90px] rounded-2xl border border-app bg-app-panel px-3 py-2 text-sm text-app-0 outline-none placeholder:text-app-4 md:col-span-2"
                />
              </div>

              <button
                type="button"
                onClick={addSource}
                disabled={saving}
                className="mt-4 inline-flex items-center gap-2 rounded-2xl border border-cyan-500/30 bg-cyan-500/10 px-4 py-2 text-sm text-cyan-200 transition hover:bg-cyan-500/15 disabled:opacity-50"
              >
                <Plus className="h-4 w-4" />
                Add source
              </button>
            </div>

            <div className="mt-6 rounded-2xl border border-app bg-app px-4 py-4">
              <div className="text-sm font-semibold text-app-0">
                Editable DB entries
              </div>

              <div className="mt-4 space-y-3">
                {loading ? (
                  <div className="text-sm text-app-4">Loading…</div>
                ) : editableItems.length === 0 ? (
                  <div className="text-sm text-app-4">
                    No editable items yet. Bootstrap baseline first or add a
                    custom source.
                  </div>
                ) : (
                  editableItems.map((item: any) => (
                    <div
                      key={item.id}
                      className="rounded-2xl border border-app bg-app-panel p-4"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="text-sm font-medium text-app-0">
                            {item.title || item.url}
                          </div>
                          <div className="mt-1 break-all text-xs text-app-4">
                            {item.url}
                          </div>
                        </div>

                        <div className="flex flex-wrap gap-2">
                          <span className="rounded-full border border-app bg-app-muted px-2 py-1 text-xs text-app-1">
                            {pretty(item.source_kind)}
                          </span>
                          <span className="rounded-full border border-app bg-app-muted px-2 py-1 text-xs text-app-1">
                            p{item.priority}
                          </span>
                          <span className="rounded-full border border-app bg-app-muted px-2 py-1 text-xs text-app-1">
                            {item.is_authoritative ? "auth" : "support"}
                          </span>
                          <span className="rounded-full border border-app bg-app-muted px-2 py-1 text-xs text-app-1">
                            {item.is_active ? "active" : "inactive"}
                          </span>
                        </div>
                      </div>

                      {item.notes ? (
                        <div className="mt-2 text-sm text-app-2">
                          {item.notes}
                        </div>
                      ) : null}

                      <div className="mt-3 flex flex-wrap gap-2">
                        <button
                          type="button"
                          onClick={() =>
                            quickPriority(
                              item,
                              Math.max(1, Number(item.priority || 100) - 5),
                            )
                          }
                          className="rounded-xl border border-app bg-app-muted px-2.5 py-1.5 text-xs text-app-1 transition hover:bg-app"
                        >
                          Priority ↑
                        </button>

                        <button
                          type="button"
                          onClick={() =>
                            quickPriority(
                              item,
                              Number(item.priority || 100) + 5,
                            )
                          }
                          className="rounded-xl border border-app bg-app-muted px-2.5 py-1.5 text-xs text-app-1 transition hover:bg-app"
                        >
                          Priority ↓
                        </button>

                        <button
                          type="button"
                          onClick={() => disableItem(item)}
                          className="rounded-xl border border-red-500/30 bg-red-500/10 px-2.5 py-1.5 text-xs text-red-200 transition hover:bg-red-500/15"
                        >
                          Disable
                        </button>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>

            <div className="mt-6 rounded-2xl border border-app bg-app px-4 py-4">
              <div className="text-sm font-semibold text-app-0">
                Merged source pack preview
              </div>

              <div className="mt-4 space-y-3">
                {mergedItems.length === 0 && !loading ? (
                  <div className="text-sm text-app-4">No merged items yet.</div>
                ) : null}

                {mergedItems.map((item: any, idx: number) => (
                  <div
                    key={`${item.url}-${idx}`}
                    className="rounded-2xl border border-app bg-app-panel p-4"
                  >
                    <div className="text-sm font-medium text-app-0">
                      {item.title || item.url}
                    </div>

                    <div className="mt-1 break-all text-xs text-app-4">
                      {item.url}
                    </div>

                    <div className="mt-2 flex flex-wrap gap-2">
                      <span className="rounded-full border border-app bg-app-muted px-2 py-1 text-xs text-app-1">
                        {pretty(item.source_kind)}
                      </span>
                      <span className="rounded-full border border-app bg-app-muted px-2 py-1 text-xs text-app-1">
                        p{item.priority}
                      </span>
                      <span className="rounded-full border border-app bg-app-muted px-2 py-1 text-xs text-app-1">
                        {item.is_authoritative ? "auth" : "support"}
                      </span>
                    </div>

                    {item.notes ? (
                      <div className="mt-2 text-sm text-app-2">
                        {item.notes}
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
