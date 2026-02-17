import React from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import Shell from "./components/Shell";
import Dashboard from "./pages/Dashboard";
import Properties from "./pages/Property";
import PropertyView from "./pages/PropertyView";
import Agents from "./pages/Agents";
import Constitution from "./pages/Constitution";

export default function App() {
  return (
    <Shell>
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />

        <Route path="/dashboard" element={<Dashboard />} />

        {/* properties list */}
        <Route path="/properties" element={<Properties />} />

        {/* property single-pane */}
        <Route path="/properties/:id" element={<PropertyView />} />

        <Route path="/agents" element={<Agents />} />
        <Route path="/constitution" element={<Constitution />} />

        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </Shell>
  );
}
