import React from "react";
import { Link } from "react-router-dom";
import PageShell from "../components/PageShell";
import PageHero from "../components/PageHero";
import StatCard from "../components/StatCard";
import IngestionRunsPanel from "../components/IngestionRunsPanel";
import IngestionErrorsDrawer from "../components/IngestionErrorsDrawer";
import IngestionSourcesPanel from "../components/IngestionSourcesPanel";
import Surface from "../components/Surface";
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
      <div className="space-y-6">
        <PageHero
          eyebrow="Operator control"
          title="Intake monitor"
          subtitle="Property intake lives on the Properties page. This page stays intentionally thin: monitor runs, verify market sync coverage, and open the main intake workflow."
          actions={
            <>
              <Link to="/properties" className="oh-btn oh-btn-secondary">
                Open property intake
              </Link>
              <button onClick={refreshAll} className="oh-btn oh-btn-secondary">
                Refresh monitor
              </button>
            </>
          }
        />

        <div className="grid grid-cols-1 gap-4 md:grid-cols-5">
          <StatCard
            title="Sources"
            value={overview?.total_sources ?? overview?.sources_enabled ?? 0}
          />
          <StatCard
            title="Imported (24h)"
            value={overview?.records_imported_24h ?? 0}
          />
          <StatCard
            title="Created (7d)"
            value={overview?.properties_created_7d ?? 0}
          />
          <StatCard
            title="Updated (7d)"
            value={overview?.properties_updated_7d ?? 0}
          />
          <StatCard
            title="Skipped dups (24h)"
            value={overview?.duplicates_skipped_24h ?? 0}
          />
        </div>

        <Surface
          title="Why this page is smaller now"
          subtitle="The noisy intake controls were removed from the standalone page so the normal operator flow starts from Properties."
        >
          <div className="text-sm text-app-3">
            Launch intake from the Properties page, review new inventory there,
            and use this monitor page only for sync health, recent run detail,
            and daily market coverage.
          </div>
        </Surface>

        <IngestionSourcesPanel refreshKey={refreshKey} onChanged={refreshAll} />

        <IngestionRunsPanel
          refreshKey={refreshKey}
          onSelectRun={setSelectedRunId}
        />

        <IngestionErrorsDrawer
          runId={selectedRunId}
          onClose={() => setSelectedRunId(null)}
        />
      </div>
    </PageShell>
  );
}
