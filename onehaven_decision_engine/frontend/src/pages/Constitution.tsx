import React from "react";
import { Gavel, ShieldCheck, ScrollText } from "lucide-react";
import PageHero from "../components/PageHero";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";
import EmptyState from "../components/EmptyState";
import { api } from "../lib/api";
import Golem from "../components/Golem";

export default function Constitution() {
  const [rows, setRows] = React.useState<any[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [err, setErr] = React.useState<string | null>(null);

  const refresh = React.useCallback(async () => {
    try {
      setLoading(true);
      setErr(null);
      const out = await api.get("/audit/constitution").catch(() => []);
      setRows(
        Array.isArray(out) ? out : Array.isArray(out?.items) ? out.items : [],
      );
    } catch (e: any) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    refresh();
  }, [refresh]);

  return (
    <PageShell>
      <div className="space-y-6">
        <PageHero
          eyebrow="Operating truth"
          title="Constitution"
          subtitle="The rules that stop the app from becoming a creative writing exercise with database access."
          right={
            <div className="absolute inset-0 flex items-center justify-center pointer-events-auto overflow-visible">
              <div className="h-[240px] w-[240px] md:h-[270px] md:w-[270px] translate-y-[-6px] opacity-95">
                <Golem className="h-full w-full" />
              </div>
            </div>
          }
          actions={
            <>
              <button
                onClick={refresh}
                className="oh-btn oh-btn-secondary cursor-pointer"
              >
                refresh
              </button>
              <span className="oh-pill">{rows.length} entries</span>
            </>
          }
        />

        {err ? (
          <Surface tone="danger">
            <div className="text-sm text-red-300">{err}</div>
          </Surface>
        ) : null}

        <Surface
          title="Constitution entries"
          subtitle="Guardrails, violations, and enforced operating rules."
        >
          {loading ? (
            <div className="grid gap-3">
              {Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="oh-skeleton h-[92px] rounded-2xl" />
              ))}
            </div>
          ) : !rows.length ? (
            <EmptyState
              icon={Gavel}
              title="No constitution entries returned"
              description="This page is wired, but the API did not return operating-truth rows."
            />
          ) : (
            <div className="grid gap-3">
              {rows.map((row, i) => (
                <div
                  key={row?.id || i}
                  className="rounded-2xl border border-app bg-app-panel px-4 py-4"
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                        <ScrollText className="h-4 w-4 text-app-4" />
                        {row?.title || row?.rule_key || `Rule ${i + 1}`}
                      </div>
                      {row?.description ? (
                        <div className="mt-2 text-sm text-app-3 leading-6">
                          {row.description}
                        </div>
                      ) : null}
                    </div>

                    <div className="flex flex-wrap items-center gap-2">
                      <span className="oh-pill">{row?.severity || "rule"}</span>
                      <span
                        className={
                          row?.status === "passing" || row?.status === "ok"
                            ? "oh-pill oh-pill-good"
                            : row?.status === "warning"
                              ? "oh-pill oh-pill-warn"
                              : "oh-pill oh-pill-bad"
                        }
                      >
                        <ShieldCheck className="h-3.5 w-3.5" />
                        {row?.status || "unknown"}
                      </span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </Surface>
      </div>
    </PageShell>
  );
}
