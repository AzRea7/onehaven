import React from "react";
import PageShell from "../components/PageShell";
import PageHero from "../components/PageHero";
import StatCard from "../components/StatCard";
import IngestionSourcesPanel from "../components/IngestionSourcesPanel";
import IngestionRunsPanel from "../components/IngestionRunsPanel";
import IngestionErrorsDrawer from "../components/IngestionErrorsDrawer";
import IngestionLaunchCard from "../components/IngestionLaunchCard";
import { ingestionClient, IngestionOverview } from "../lib/ingestionClient";

export default function ImportsPage() {
  const [overview, setOverview] = React.useState<IngestionOverview | null>(
    null,
  );
  const [selectedRunId, setSelectedRunId] = React.useState<number | null>(null);
  const [refreshKey, setRefreshKey] = React.useState(0);

  async function loadOverview() {
    try {
      setOverview(await ingestionClient.overview());
    } catch {
      setOverview(null);
    }
  }

  React.useEffect(() => {
    loadOverview();
  }, [refreshKey]);

  function refreshAll() {
    setRefreshKey((v) => v + 1);
  }

  return (
    <PageShell>
      <PageHero
        eyebrow="Operator control"
        title="Property intake"
        subtitle="Launch focused property ingestion runs by region, keep them capped and filterable, and monitor the resulting source health and run history from one clean workflow."
      />

      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <StatCard
          title="Connected Sources"
          value={overview?.sources_connected ?? 0}
        />
        <StatCard
          title="Enabled Sources"
          value={overview?.sources_enabled ?? 0}
        />
        <StatCard
          title="Imported (24h)"
          value={overview?.records_imported_24h ?? 0}
        />
        <StatCard
          title="Duplicates Skipped (24h)"
          value={overview?.duplicates_skipped_24h ?? 0}
        />
      </div>

      <div className="mt-6">
        <IngestionLaunchCard refreshKey={refreshKey} onQueued={refreshAll} />
      </div>

      <div className="mt-6 grid grid-cols-1 gap-6 xl:grid-cols-[1.1fr_1fr]">
        <IngestionSourcesPanel refreshKey={refreshKey} onChanged={refreshAll} />
        <IngestionRunsPanel
          refreshKey={refreshKey}
          onSelectRun={setSelectedRunId}
        />
      </div>

      <IngestionErrorsDrawer
        runId={selectedRunId}
        onClose={() => setSelectedRunId(null)}
      />
    </PageShell>
  );
}
