import React from "react";
import { Navigate, Route, Routes, useParams } from "react-router-dom";
import AppShell from "./components/AppShell";

import Dashboard from "./pages/Dashboard";
import Property from "./pages/Property";
import PropertyView from "./pages/PropertyView";

import Agents from "./pages/Agents";
import Constitution from "./pages/Constitution";
import DealIntake from "./pages/DealIntake";
import Jurisdictions from "./pages/Jurisdictions";
import JurisdictionProfiles from "./pages/JurisdictionProfiles";
import PipelineDrilldown from "./pages/drilldowns/PipelineDrilldown";
import ComplianceDrilldown from "./pages/drilldowns/ComplianceDrilldown";
import RehabDrilldown from "./pages/drilldowns/RehabDrilldown";
import CashflowDrilldown from "./pages/drilldowns/CashflowDrilldown";
import EquityDrilldown from "./pages/drilldowns/EquityDrilldown";
import ImportsPage from "./pages/ImportsPage";
import Login from "./pages/Login";
import Register from "./pages/Register";
import PolicyReview from "./pages/PolicyReview";

import { AuthGate } from "./lib/auth";

function Protected({ children }: { children: React.ReactNode }) {
  return <AuthGate>{children}</AuthGate>;
}

function LegacyPropertyRedirect() {
  const { id } = useParams();
  return <Navigate to={id ? `/properties/${id}` : "/properties"} replace />;
}

export default function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />

        <Route path="/login" element={<Login />} />
        <Route path="/register" element={<Register />} />

        <Route
          path="/dashboard"
          element={
            <Protected>
              <Dashboard />
            </Protected>
          }
        />

        <Route
          path="/properties"
          element={
            <Protected>
              <Property />
            </Protected>
          }
        />

        <Route
          path="/properties/:id"
          element={
            <Protected>
              <PropertyView />
            </Protected>
          }
        />



        <Route
          path="/property/:id"
          element={
            <Protected>
              <LegacyPropertyRedirect />
            </Protected>
          }
        />

        <Route
          path="/imports"
          element={
            <Protected>
              <ImportsPage />
            </Protected>
          }
        />

        <Route
          path="/agents"
          element={
            <Protected>
              <Agents />
            </Protected>
          }
        />

        <Route
          path="/deal-intake"
          element={
            <Protected>
              <DealIntake />
            </Protected>
          }
        />

        <Route
          path="/jurisdictions"
          element={
            <Protected>
              <Jurisdictions />
            </Protected>
          }
        />

        <Route
          path="/jurisdiction-profiles"
          element={
            <Protected>
              <JurisdictionProfiles />
            </Protected>
          }
        />

        <Route
          path="/pipeline"
          element={
            <Protected>
              <PipelineDrilldown />
            </Protected>
          }
        />

        <Route
          path="/drilldowns/compliance"
          element={
            <Protected>
              <ComplianceDrilldown />
            </Protected>
          }
        />

        <Route
          path="/drilldowns/rehab"
          element={
            <Protected>
              <RehabDrilldown />
            </Protected>
          }
        />

        <Route
          path="/drilldowns/cashflow"
          element={
            <Protected>
              <CashflowDrilldown />
            </Protected>
          }
        />

        <Route
          path="/drilldowns/equity"
          element={
            <Protected>
              <EquityDrilldown />
            </Protected>
          }
        />

        <Route
          path="/constitution"
          element={
            <Protected>
              <Constitution />
            </Protected>
          }
        />

        <Route
          path="/policy-review"
          element={
            <Protected>
              <PolicyReview />
            </Protected>
          }
        />

        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </AppShell>
  );
}
