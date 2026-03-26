import React from "react";
import {
  Building2,
  Mail,
  Phone,
  Users,
  Flag,
  AlertTriangle,
  Handshake,
} from "lucide-react";

export type AcquisitionParticipant = {
  id?: number;
  role?: string;
  name?: string;
  company?: string | null;
  email?: string | null;
  phone?: string | null;
  is_primary?: boolean | null;
  waiting_on?: boolean | null;
};

type Props = {
  participants?: AcquisitionParticipant[];
  waitingOn?: string | null;
};

function normalizeRole(role?: string | null) {
  const raw = String(role || "other")
    .trim()
    .toLowerCase();
  if (!raw) return "other";
  return raw;
}

function labelForRole(role?: string | null) {
  const normalized = normalizeRole(role);
  if (normalized === "loan_officer") return "loan officer";
  if (normalized === "title_company") return "title company";
  return normalized.replace(/_/g, " ");
}

function roleTone(role?: string | null, waitingOn?: string | null) {
  const normalizedRole = normalizeRole(role);
  const waiting = String(waitingOn || "")
    .trim()
    .toLowerCase();

  if (!waiting) return "neutral";

  if (
    waiting.includes("lender") &&
    (normalizedRole.includes("lender") ||
      normalizedRole.includes("loan") ||
      normalizedRole.includes("finance"))
  ) {
    return "warn";
  }

  if (
    waiting.includes("title") &&
    (normalizedRole.includes("title") || normalizedRole.includes("escrow"))
  ) {
    return "warn";
  }

  if (waiting.includes("seller") && normalizedRole.includes("seller")) {
    return "warn";
  }

  if (
    waiting.includes("operator") &&
    (normalizedRole.includes("operator") ||
      normalizedRole.includes("internal") ||
      normalizedRole.includes("team") ||
      normalizedRole.includes("analyst"))
  ) {
    return "warn";
  }

  if (
    waiting.includes("document") &&
    (normalizedRole.includes("document") ||
      normalizedRole.includes("coordinator") ||
      normalizedRole.includes("processor") ||
      normalizedRole.includes("closing"))
  ) {
    return "warn";
  }

  return "neutral";
}

function participantSortScore(
  person: AcquisitionParticipant,
  waitingOn?: string | null,
) {
  let score = 0;
  if (person.waiting_on) score += 100;
  if (person.is_primary) score += 50;
  if (roleTone(person.role, waitingOn) === "warn") score += 25;
  return score;
}

function ownerSummary(
  participants: AcquisitionParticipant[],
  waitingOn?: string | null,
) {
  const explicitOwner = participants.find((p) => p.waiting_on);
  if (explicitOwner) {
    return explicitOwner.name || labelForRole(explicitOwner.role) || "unknown";
  }

  const waiting = String(waitingOn || "").trim();
  if (waiting) return waiting;

  return null;
}

export default function AcquisitionParticipantsPanel({
  participants = [],
  waitingOn,
}: Props) {
  const grouped = participants.reduce<Record<string, AcquisitionParticipant[]>>(
    (acc, item) => {
      const key = normalizeRole(item.role);
      if (!acc[key]) acc[key] = [];
      acc[key].push(item);
      return acc;
    },
    {},
  );

  const groupedEntries = Object.entries(grouped).sort(([a], [b]) =>
    a.localeCompare(b),
  );

  for (const [, items] of groupedEntries) {
    items.sort(
      (a, b) =>
        participantSortScore(b, waitingOn) - participantSortScore(a, waitingOn),
    );
  }

  const currentOwner = ownerSummary(participants, waitingOn);
  const activeOwnerCount = participants.filter((p) => p.waiting_on).length;

  return (
    <div className="rounded-3xl border border-app bg-app-panel p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Participants
          </div>
          <div className="mt-1 text-sm text-app-3">
            See who owns the next handoff in the acquisition loop and who is
            currently blocking movement.
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          {currentOwner ? (
            <span className="oh-pill oh-pill-warn">
              waiting on {currentOwner}
            </span>
          ) : null}
          <span className="oh-pill">
            <Users className="mr-1 h-3.5 w-3.5" />
            {participants.length} participant
            {participants.length === 1 ? "" : "s"}
          </span>
        </div>
      </div>

      {!participants.length ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-4">
          No participants have been normalized for this file yet.
        </div>
      ) : (
        <>
          <div className="mt-4 flex flex-wrap gap-2 text-xs">
            <span
              className={activeOwnerCount ? "oh-pill oh-pill-warn" : "oh-pill"}
            >
              action owners {activeOwnerCount}
            </span>
            <span className="oh-pill">
              primary contacts {participants.filter((p) => p.is_primary).length}
            </span>
          </div>

          <div className="mt-4 grid gap-4 md:grid-cols-2">
            {groupedEntries.map(([role, items]) => (
              <div
                key={role}
                className="rounded-2xl border border-app bg-app-muted p-4"
              >
                <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                  <Handshake className="h-4 w-4" />
                  {labelForRole(role)}
                </div>

                <div className="mt-3 space-y-3">
                  {items.map((person, idx) => {
                    const highlighted =
                      !!person.waiting_on ||
                      roleTone(person.role, waitingOn) === "warn";

                    return (
                      <div
                        key={person.id || `${role}-${idx}`}
                        className={`rounded-2xl border px-4 py-3 ${
                          highlighted
                            ? "border-amber-500/30 bg-amber-500/5"
                            : "border-app bg-app-panel"
                        }`}
                      >
                        <div className="flex flex-wrap items-start justify-between gap-2">
                          <div>
                            <div className="text-sm font-medium text-app-0">
                              {person.name || "Unnamed contact"}
                            </div>
                            {person.role ? (
                              <div className="mt-1 text-xs text-app-4">
                                {labelForRole(person.role)}
                              </div>
                            ) : null}
                          </div>

                          <div className="flex flex-wrap gap-2">
                            {person.is_primary ? (
                              <span className="oh-pill oh-pill-accent">
                                <Flag className="mr-1 h-3.5 w-3.5" />
                                primary
                              </span>
                            ) : null}

                            {person.waiting_on ? (
                              <span className="oh-pill oh-pill-warn">
                                <AlertTriangle className="mr-1 h-3.5 w-3.5" />
                                action owner
                              </span>
                            ) : roleTone(person.role, waitingOn) === "warn" ? (
                              <span className="oh-pill">likely owner</span>
                            ) : null}
                          </div>
                        </div>

                        <div className="mt-2 space-y-1 text-xs text-app-4">
                          {person.company ? (
                            <div className="flex items-center gap-2">
                              <Building2 className="h-3.5 w-3.5" />
                              {person.company}
                            </div>
                          ) : null}

                          {person.email ? (
                            <div className="flex items-center gap-2">
                              <Mail className="h-3.5 w-3.5" />
                              <a
                                href={`mailto:${person.email}`}
                                className="hover:text-app-1"
                              >
                                {person.email}
                              </a>
                            </div>
                          ) : null}

                          {person.phone ? (
                            <div className="flex items-center gap-2">
                              <Phone className="h-3.5 w-3.5" />
                              <a
                                href={`tel:${person.phone}`}
                                className="hover:text-app-1"
                              >
                                {person.phone}
                              </a>
                            </div>
                          ) : null}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
