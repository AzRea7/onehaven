import React from "react";
import GlassCard from "./GlassCard";
import { api } from "../lib/api";

type Props = {
  runId: number | null;
  onClose: () => void;
};

export default function IngestionErrorsDrawer({ runId, onClose }: Props) {
  const [detail, setDetail] = React.useState<any>(null);

  React.useEffect(() => {
    if (!runId) {
      setDetail(null);
      return;
    }
    api.getIngestionRunDetail(runId)
      .then(setDetail)
      .catch(() => setDetail(null));
  }, [runId]);

  if (!runId) return null;

  return (
    <div className="fixed inset-y-0 right-0 z-50 w-full max-w-xl bg-black/50 p-4 backdrop-blur md:w-[560px]">
      <GlassCard className="h-full overflow-auto p-4">
        <div className="mb-4 flex items-center justify-between">
          <h3 className="text-lg font-semibold">Run #{runId}</h3>
          <button
            onClick={onClose}
            className="rounded-xl border border-white/10 px-3 py-2"
          >
            Close
          </button>
        </div>

        {!detail ? (
          <div className="text-sm text-neutral-400">Loading run details…</div>
        ) : (
          <div className="space-y-4 text-sm">
            <div className="grid grid-cols-2 gap-3">
              <div>
                <div className="text-neutral-500">Status</div>
                <div>{detail.status}</div>
              </div>
              <div>
                <div className="text-neutral-500">Trigger</div>
                <div>{detail.trigger_type}</div>
              </div>
              <div>
                <div className="text-neutral-500">Imported</div>
                <div>{detail.records_imported}</div>
              </div>
              <div>
                <div className="text-neutral-500">Duplicates</div>
                <div>{detail.duplicates_skipped}</div>
              </div>
            </div>

            <div>
              <div className="mb-2 text-neutral-500">Error summary</div>
              <pre className="overflow-auto rounded-xl border border-white/10 bg-white/5 p-3 whitespace-pre-wrap">
                {detail.error_summary || "—"}
              </pre>
            </div>

            <div>
              <div className="mb-2 text-neutral-500">Error JSON</div>
              <pre className="overflow-auto rounded-xl border border-white/10 bg-white/5 p-3 whitespace-pre-wrap">
                {JSON.stringify(detail.error_json || {}, null, 2)}
              </pre>
            </div>

            <div>
              <div className="mb-2 text-neutral-500">Summary JSON</div>
              <pre className="overflow-auto rounded-xl border border-white/10 bg-white/5 p-3 whitespace-pre-wrap">
                {JSON.stringify(detail.summary_json || {}, null, 2)}
              </pre>
            </div>
          </div>
        )}
      </GlassCard>
    </div>
  );
}
