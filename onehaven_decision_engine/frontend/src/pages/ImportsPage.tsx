import React from "react";
import PageShell from "../components/PageShell";
import PageHero from "../components/PageHero";
import StatCard from "../components/StatCard";
import IngestionSourcesPanel from "../components/IngestionSourcesPanel";
import IngestionRunsPanel from "../components/IngestionRunsPanel";
import IngestionErrorsDrawer from "../components/IngestionErrorsDrawer";
import { api, IngestionOverview } from "../lib/api";

export default function ImportsPage() {
  const [overview, setOverview] = React.useState<IngestionOverview | null>(
    null,
  );
  const [selectedRunId, setSelectedRunId] = React.useState<number | null>(null);

  React.useEffect(() => {
    api
      .ingestionOverview()
      .then(setOverview)
      .catch(() => setOverview(null));
  }, []);

  return (
    <PageShell>
      <PageHero
        title="Automated Ingestion"
        subtitle="Deal funnel engine for Zillow, InvestorLift, partner feeds, webhook intake, and scheduled sync observability."
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

      <div className="mt-6 grid grid-cols-1 gap-6 xl:grid-cols-[1.2fr_1fr]">
        <IngestionSourcesPanel />
        <IngestionRunsPanel onSelectRun={setSelectedRunId} />
      </div>

      <IngestionErrorsDrawer
        runId={selectedRunId}
        onClose={() => setSelectedRunId(null)}
      />
    </PageShell>
  );
}
