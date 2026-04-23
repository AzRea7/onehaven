import React from "react";
import { Navigate, Route, Routes, useParams } from "react-router-dom";
import AppShell from "./components/AppShell";

import Dashboard from "@/products/management/pages/Dashboard";
import Property from "./pages/Property";

import Agents from "./pages/Agents";
import Constitution from "./pages/Constitution";
import DealIntake from "@/products/acquire/pages/DealIntake";
import Jurisdictions from "./pages/Jurisdictions";
import JurisdictionProfiles from "./pages/JurisdictionProfiles";
import ImportsPage from "./pages/ImportsPage";
import Login from "./pages/Login";
import Register from "./pages/Register";
import PolicyReview from "./pages/PolicyReview";
import InvestorPane from "@/products/investor_intelligence/pages/InvestorPane";
import AcquisitionPane from "@/products/acquire/pages/AcquisitionPane";
import CompliancePane from "@/products/compliance/pages/CompliancePane";
import TenantsPane from "@/products/tenant/pages/TenantsPane";
import ManagementPane from "@/products/management/pages/ManagementPane";

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
