import React from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import Shell from "./components/Shell";

import Dashboard from "./pages/Dashboard";
import Property from "./pages/Property";
import Agents from "./pages/Agents";
import Constitution from "./pages/Constitution";
import PropertyView from "./pages/PropertyView";
import DealIntake from "./pages/DealIntake";
import Jurisdictions from "./pages/Jurisdictions";

import Login from "./pages/Login";
import Register from "./pages/Register";

import { AuthGate } from "./lib/auth";

function Protected({ children }: { children: React.ReactNode }) {
  return <AuthGate>{children}</AuthGate>;
}

export default function App() {
  return (
    <Shell>
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />

        {/* Public */}
        <Route path="/login" element={<Login />} />
        <Route path="/register" element={<Register />} />

        {/* Protected */}
        <Route
          path="/dashboard"
          element={
            <Protected>
              <Dashboard />
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
          path="/property/:id"
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
          path="/agents"
          element={
            <Protected>
              <Agents />
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

        {/* Fallback */}
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </Shell>
  );
}
