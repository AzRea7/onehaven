import React from "react";
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
        state: market!.state,
        county: market!.county ?? null,
        city: market!.city ?? null,
        pha_name: market!.pha_name ?? null,
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

  if (!open || !market) return null;

  async function refresh() {
    await load();
    await onChanged?.();
  }

  async function bootstrap() {
    setSaving(true);
    setErr(null);
    try {
      await api.policyCatalogAdminBootstrap({
        state: market!.state,
        county: market!.county ?? null,
        city: market!.city ?? null,
        pha_name: market!.pha_name ?? null,
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
    if (!confirm("Reset this market source pack to baseline?")) return;
    setSaving(true);
    setErr(null);
    try {
      await api.policyCatalogAdminReset({
        state: market!.state,
        county: market!.county ?? null,
        city: market!.city ?? null,
        pha_name: market!.pha_name ?? null,
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
        state: market!.state,
        county: market!.county ?? null,
        city: market!.city ?? null,
        pha_name: market!.pha_name ?? null,
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
    if (!confirm(`Disable "${item.title || item.url}"?`)) return;
    setSaving(true);
    setErr(null);
    try {
      await api.policyCatalogAdminDisableItem(item.id, {
        state: market!.state,
        county: market!.county ?? null,
        city: market!.city ?? null,
        pha_name: market!.pha_name ?? null,
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
        state: market!.state,
        county: market!.county ?? null,
        city: market!.city ?? null,
        pha_name: market!.pha_name ?? null,
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
    <div className="fixed inset-0 z-50 flex items-stretch justify-end bg-black/50">
      <div className="h-full w-full max-w-5xl overflow-y-auto border-l border-white/10 bg-[#09111f] p-6 text-white shadow-2xl">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-xs uppercase tracking-[0.2em] text-cyan-300/70">
              Source Pack Editor
            </div>
            <h2 className="mt-2 text-2xl font-semibold">
              {market.city || market.county || market.state}
            </h2>
            <div className="mt-1 text-sm text-white/55">
              {market.city || "—"} • {market.county || "—"} • {market.state}
            </div>
          </div>

          <button
            onClick={onClose}
            className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm hover:bg-white/10"
          >
            Close
          </button>
        </div>

        {err ? (
          <div className="mt-4 rounded-xl border border-red-500/20 bg-red-500/10 p-3 text-sm text-red-200">
            {err}
          </div>
        ) : null}

        <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-4">
          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="text-xs text-white/45">Merged items</div>
            <div className="mt-2 text-2xl font-semibold">
              {mergedItems.length}
            </div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="text-xs text-white/45">Editable DB items</div>
            <div className="mt-2 text-2xl font-semibold">
              {editableItems.length}
            </div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="text-xs text-white/45">Covered kinds</div>
            <div className="mt-2 text-2xl font-semibold">
              {Object.keys(counts).length}
            </div>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
            <div className="text-xs text-white/45">Missing kinds</div>
            <div className="mt-2 text-2xl font-semibold text-amber-200">
              {missing.length}
            </div>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          <button
            onClick={bootstrap}
            disabled={saving}
            className="rounded-xl border border-cyan-400/20 bg-cyan-500/10 px-3 py-2 text-sm text-cyan-100 hover:bg-cyan-500/15 disabled:opacity-50"
          >
            Bootstrap baseline
          </button>
          <button
            onClick={resetToBaseline}
            disabled={saving}
            className="rounded-xl border border-amber-400/20 bg-amber-500/10 px-3 py-2 text-sm text-amber-100 hover:bg-amber-500/15 disabled:opacity-50"
          >
            Reset to baseline
          </button>
          <button
            onClick={runRepair}
            disabled={saving}
            className="rounded-xl border border-emerald-400/20 bg-emerald-500/10 px-3 py-2 text-sm text-emerald-100 hover:bg-emerald-500/15 disabled:opacity-50"
          >
            Repair market
          </button>
          <button
            onClick={refresh}
            disabled={loading || saving}
            className="rounded-xl border border-white/10 bg-white/5 px-3 py-2 text-sm hover:bg-white/10 disabled:opacity-50"
          >
            Refresh
          </button>
        </div>

        <div className="mt-4 rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold">Coverage by source kind</div>
          <div className="mt-3 flex flex-wrap gap-2">
            {Object.entries(counts).map(([key, value]) => (
              <span
                key={key}
                className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white/80"
              >
                {key}: {String(value)}
              </span>
            ))}
            {missing.map((m: string) => (
              <span
                key={m}
                className="rounded-full border border-amber-400/20 bg-amber-500/10 px-3 py-1 text-xs text-amber-100"
              >
                missing: {m}
              </span>
            ))}
          </div>
        </div>

        <div className="mt-6 rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold">Add source</div>
          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
            <input
              value={form.url}
              onChange={(e) => setForm((f) => ({ ...f, url: e.target.value }))}
              placeholder="Official source URL"
              className="rounded-xl border border-white/10 bg-[#0b1628] px-3 py-2 text-sm"
            />
            <input
              value={form.title}
              onChange={(e) =>
                setForm((f) => ({ ...f, title: e.target.value }))
              }
              placeholder="Title"
              className="rounded-xl border border-white/10 bg-[#0b1628] px-3 py-2 text-sm"
            />
            <input
              value={form.publisher}
              onChange={(e) =>
                setForm((f) => ({ ...f, publisher: e.target.value }))
              }
              placeholder="Publisher"
              className="rounded-xl border border-white/10 bg-[#0b1628] px-3 py-2 text-sm"
            />
            <select
              value={form.source_kind}
              onChange={(e) =>
                setForm((f) => ({ ...f, source_kind: e.target.value }))
              }
              className="rounded-xl border border-white/10 bg-[#0b1628] px-3 py-2 text-sm"
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
              className="rounded-xl border border-white/10 bg-[#0b1628] px-3 py-2 text-sm"
            />
            <label className="flex items-center gap-2 rounded-xl border border-white/10 bg-[#0b1628] px-3 py-2 text-sm">
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
              className="md:col-span-2 min-h-[90px] rounded-xl border border-white/10 bg-[#0b1628] px-3 py-2 text-sm"
            />
          </div>

          <button
            onClick={addSource}
            disabled={saving}
            className="mt-4 rounded-xl border border-cyan-400/20 bg-cyan-500/10 px-4 py-2 text-sm text-cyan-100 hover:bg-cyan-500/15 disabled:opacity-50"
          >
            Add source
          </button>
        </div>

        <div className="mt-6 rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold">Editable DB entries</div>
          <div className="mt-4 space-y-3">
            {loading ? (
              <div className="text-sm text-white/55">Loading…</div>
            ) : editableItems.length === 0 ? (
              <div className="text-sm text-white/55">
                No editable items yet. Bootstrap baseline first or add a custom
                source.
              </div>
            ) : (
              editableItems.map((item: any) => (
                <div
                  key={item.id}
                  className="rounded-xl border border-white/10 bg-[#0b1628] p-4"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="text-sm font-medium">
                        {item.title || item.url}
                      </div>
                      <div className="mt-1 break-all text-xs text-white/55">
                        {item.url}
                      </div>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <span className="rounded-full border border-white/10 px-2 py-1 text-xs">
                        {pretty(item.source_kind)}
                      </span>
                      <span className="rounded-full border border-white/10 px-2 py-1 text-xs">
                        p{item.priority}
                      </span>
                      <span className="rounded-full border border-white/10 px-2 py-1 text-xs">
                        {item.is_authoritative ? "auth" : "support"}
                      </span>
                      <span className="rounded-full border border-white/10 px-2 py-1 text-xs">
                        {item.is_active ? "active" : "inactive"}
                      </span>
                    </div>
                  </div>

                  {item.notes ? (
                    <div className="mt-2 text-sm text-white/70">
                      {item.notes}
                    </div>
                  ) : null}

                  <div className="mt-3 flex flex-wrap gap-2">
                    <button
                      onClick={() =>
                        quickPriority(
                          item,
                          Math.max(1, Number(item.priority || 100) - 5),
                        )
                      }
                      className="rounded-lg border border-white/10 bg-white/5 px-2.5 py-1.5 text-xs hover:bg-white/10"
                    >
                      Priority ↑
                    </button>
                    <button
                      onClick={() =>
                        quickPriority(item, Number(item.priority || 100) + 5)
                      }
                      className="rounded-lg border border-white/10 bg-white/5 px-2.5 py-1.5 text-xs hover:bg-white/10"
                    >
                      Priority ↓
                    </button>
                    <button
                      onClick={() => disableItem(item)}
                      className="rounded-lg border border-red-400/20 bg-red-500/10 px-2.5 py-1.5 text-xs text-red-100 hover:bg-red-500/15"
                    >
                      Disable
                    </button>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        <div className="mt-6 rounded-2xl border border-white/10 bg-white/5 p-4">
          <div className="text-sm font-semibold">
            Merged source pack preview
          </div>
          <div className="mt-4 space-y-3">
            {mergedItems.map((item: any, idx: number) => (
              <div
                key={`${item.url}-${idx}`}
                className="rounded-xl border border-white/10 bg-[#0b1628] p-4"
              >
                <div className="text-sm font-medium">
                  {item.title || item.url}
                </div>
                <div className="mt-1 break-all text-xs text-white/55">
                  {item.url}
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  <span className="rounded-full border border-white/10 px-2 py-1 text-xs">
                    {pretty(item.source_kind)}
                  </span>
                  <span className="rounded-full border border-white/10 px-2 py-1 text-xs">
                    p{item.priority}
                  </span>
                  <span className="rounded-full border border-white/10 px-2 py-1 text-xs">
                    {item.is_authoritative ? "auth" : "support"}
                  </span>
                </div>
                {item.notes ? (
                  <div className="mt-2 text-sm text-white/70">{item.notes}</div>
                ) : null}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
