import React from "react";
import { Link } from "react-router-dom";
import PageShell from "../components/PageShell";
import PageHero from "../components/PageHero";
import StatCard from "../components/StatCard";
import IngestionRunsPanel from "../components/IngestionRunsPanel";
import IngestionErrorsDrawer from "../components/IngestionErrorsDrawer";
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
          subtitle="Property intake now lives inside the Properties page. This screen is just a lightweight monitor for recent runs and sync health."
          actions={
            <>
              <Link to="/properties" className="oh-btn oh-btn-secondary">
                Open properties intake
              </Link>
              <button onClick={refreshAll} className="oh-btn oh-btn-secondary">
                Refresh
              </button>
            </>
          }
        />

        <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
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
        </div>

        <Surface
          title="Why this page is smaller now"
          subtitle="The intake experience was consolidated so the investor workflow starts from Properties instead of bouncing between duplicate control modules."
        >
          <div className="text-sm text-app-3">
            Use the Properties page to launch intake, review new properties, and
            move them through the workflow. This page remains as a slim
            monitoring view for operators.
          </div>
        </Surface>

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
