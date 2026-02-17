// frontend/src/components/BrickBuilder.tsx
import React from "react";
import { BrickBuilder as BrickBuilderArt } from "./Artwork";

export default function BrickBuilder({ className }: { className?: string }) {
  return <BrickBuilderArt className={className} />;
}
