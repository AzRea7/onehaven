import React from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import Shell from "./components/Shell";
import Dashboard from "./pages/Dashboard";
import Property from "./pages/Property";
import Agents from "./pages/Agents";
import Constitution from "./pages/Constitution";
import PropertyView from "./pages/PropertyView";

export default function App() {
  return (
    <Shell>
      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/property/:id" element={<Property />} />
        <Route path="/properties/:id" element={<PropertyView />} />
        <Route path="/agents" element={<Agents />} />
        <Route path="/constitution" element={<Constitution />} />
      </Routes>
    </Shell>
  );
}
