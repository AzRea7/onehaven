import React from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import Shell from "./components/Shell";
import Dashboard from "./pages/Dashboard";
import Properties from "./pages/Property";
import Agents from "./pages/Agents";
import Constitution from "./pages/Constitution";
import PropertyView from "./pages/PropertyView";

export default function App() {
  return (
    <Shell>
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />

        <Route path="/dashboard" element={<Dashboard />} />

        {/* ✅ NEW: properties list page */}
        <Route path="/properties" element={<Properties />} />

        {/* ✅ property view (single pane) */}
        <Route path="/properties/:id" element={<PropertyView />} />

        <Route path="/agents" element={<Agents />} />
        <Route path="/constitution" element={<Constitution />} />

        {/* ✅ catch-all */}
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </Shell>
  );
}
