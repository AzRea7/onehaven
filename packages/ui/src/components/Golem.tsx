// frontend/onehaven_onehaven_platform/frontend/src/components/Golem.tsx
import React from "react";
import clsx from "clsx";
import { Golem as GolemArt } from "packages/ui/onehaven_onehaven_platform/frontend/src/components/Artwork";

export default function Golem({ className }: { className?: string }) {
  return (
    <div
      className={clsx(
        "flex h-full w-full items-center justify-center overflow-visible p-1 md:p-2",
        className,
      )}
    >
      <div className="h-[88%] w-[88%] max-h-[220px] max-w-[220px] translate-y-1 md:max-h-[240px] md:max-w-[240px] md:translate-y-1.5">
        <GolemArt className="h-full w-full" />
      </div>
    </div>
  );
}
