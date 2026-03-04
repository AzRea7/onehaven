// frontend/src/components/Golem.tsx
import React from "react";
import { Golem as GolemArt } from "./Artwork";

export default function Golem({ className }: { className?: string }) {
  return <GolemArt className={className} />;
}
