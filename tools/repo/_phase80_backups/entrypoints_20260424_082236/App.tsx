import React from "react";
import { Navigate, Route, Routes, useParams } from "react-router-dom";
import AppShell from "onehaven_onehaven_platform/frontend/src/components/AppShell";

import Dashboard from "products/ops/frontend/src/pages/Dashboard";
import Property from "products/ops/frontend/src/pages/Property";

import Agents from "onehaven_onehaven_platform/frontend/src/pages/Agents";
import Constitution from "onehaven_onehaven_platform/frontend/src/pages/Constitution";
import DealIntake from "products/acquire/frontend/src/pages/DealIntake";
import Jurisdictions from "products/compliance/frontend/src/pages/Jurisdictions";
import JurisdictionProfiles from "products/compliance/frontend/src/pages/JurisdictionProfiles";
import ImportsPage from "products/acquire/frontend/src/pages/ImportsPage";
import Login from "onehaven_onehaven_platform/frontend/src/pages/Login";
import Register from "onehaven_onehaven_platform/frontend/src/pages/Register";
import PolicyReview from "products/compliance/frontend/src/pages/PolicyReview";
import InvestorPane from "products/intelligence/frontend/src/pages/InvestorPane";
import AcquisitionPane from "products/acquire/frontend/src/pages/AcquisitionPane";
import CompliancePane from "products/compliance/frontend/src/pages/CompliancePane";
import TenantsPane from "products/tenants/frontend/src/pages/TenantsPane";
import ManagementPane from "products/ops/frontend/src/pages/ManagementPane";

import { AuthGate } from "./lib/auth";

function Protected({ children }: { children: React.ReactNode }) {
  return <AuthGate>{children}</AuthGate>;
}

function LegacyPropertyRedirect() {
  const { id } = useParams();
  return <Navigate to={id ? `/properties/${id}` : "/panes/investor"} replace />;
}

function LegacyPipelineRedirect() {
  return <Navigate to="/dashboard" replace />;
}

function LegacyComplianceRedirect() {
  return <Navigate to="/panes/compliance" replace />;
}

function LegacyRehabRedirect() {
  return <Navigate to="/panes/compliance" replace />;
}

function LegacyCashflowRedirect() {
  return <Navigate to="/panes/management" replace />;
}

function LegacyEquityRedirect() {
  return <Navigate to="/panes/management" replace />;
}

export default function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Navigate to="/panes/investor" replace />} />

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
              <Navigate to="/panes/investor" replace />
            </Protected>
          }
        />

        <Route
          path="/panes/investor"
          element={
            <Protected>
              <InvestorPane />
            </Protected>
          }
        />

        <Route
          path="/panes/acquisition"
          element={
            <Protected>
              <AcquisitionPane />
            </Protected>
          }
        />

        <Route
          path="/panes/compliance"
          element={
            <Protected>
              <CompliancePane />
            </Protected>
          }
        />

        <Route
          path="/panes/tenants"
          element={
            <Protected>
              <TenantsPane />
            </Protected>
          }
        />

        <Route
          path="/panes/management"
          element={
            <Protected>
              <ManagementPane />
            </Protected>
          }
        />

        <Route
          path="/properties/:id"
          element={
            <Protected>
              <Property />
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
              <LegacyPipelineRedirect />
            </Protected>
          }
        />

        <Route
          path="/drilldowns/compliance"
          element={
            <Protected>
              <LegacyComplianceRedirect />
            </Protected>
          }
        />

        <Route
          path="/drilldowns/rehab"
          element={
            <Protected>
              <LegacyRehabRedirect />
            </Protected>
          }
        />

        <Route
          path="/drilldowns/cashflow"
          element={
            <Protected>
              <LegacyCashflowRedirect />
            </Protected>
          }
        />

        <Route
          path="/drilldowns/equity"
          element={
            <Protected>
              <LegacyEquityRedirect />
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

        <Route path="*" element={<Navigate to="/panes/investor" replace />} />
      </Routes>
    </AppShell>
  );
}
