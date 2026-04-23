import React from "react";
import {
  AlertTriangle,
  Building2,
  Flag,
  Handshake,
  Mail,
  Phone,
  ShieldCheck,
  Star,
  Users,
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
  source_type?: string | null;
  notes?: string | null;
};

export type DocumentContact = {
  id?: number;
  role?: string;
  role_label?: string;
  name?: string;
  company?: string | null;
  email?: string | null;
  phone?: string | null;
  is_primary?: boolean | null;
  waiting_on?: boolean | null;
  source_type?: string | null;
  notes?: string | null;
  why_relevant?: string | null;
};

export type DocumentContactCard = {
  document_kind?: string;
  document_kind_label?: string;
  target_roles?: string[];
  primary_contact_for_document_kind?: DocumentContact | null;
  fallback_contacts_for_document_kind?: DocumentContact[];
  missing_contact_roles?: string[];
};

type Props = {
  participants?: AcquisitionParticipant[];
  waitingOn?: string | null;
  documentContactCard?: DocumentContactCard | null;
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
  const labels: Record<string, string> = {
    loan_officer: "Loan officer",
    lender: "Lender",
    title_company: "Title company",
    escrow_officer: "Escrow officer",
    listing_agent: "Listing agent",
    buyer_agent: "Buyer agent",
    seller_agent: "Seller agent",
    listing_office: "Listing office",
    insurance_agent: "Insurance agent",
    insurance_agency: "Insurance agency",
    inspector: "Inspector",
    inspection_company: "Inspection company",
  };
  return (
    labels[normalized] ||
    normalized.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
  );
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
  if (normalizeRole(person.role) === "listing_agent") score += 20;
  if (normalizeRole(person.role) === "listing_office") score += 10;
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

function sourceBadge(sourceType?: string | null) {
  const source = String(sourceType || "")
    .trim()
    .toLowerCase();
  if (!source) return null;
  if (source === "listing_import") {
    return (
      <span className="oh-pill oh-pill-accent">
        <Building2 className="mr-1 h-3.5 w-3.5" />
        listing import
      </span>
    );
  }
  return <span className="oh-pill">{source.replace(/_/g, " ")}</span>;
}

function buildEmailHref(contact: any, documentKindLabel?: string | null) {
  const rawEmail = String(contact?.email || "").trim();
  if (!rawEmail) return null;

  const emails = rawEmail
    .split(/[;,]/)
    .map((e) => e.trim())
    .filter(Boolean);

  if (!emails.length) return null;

  const contactName = String(contact?.name || "").trim() || "there";
  const contactRole = String(contact?.role || contact?.role_label || "")
    .trim()
    .toLowerCase();
  const subjectBase =
    String(documentKindLabel || "Document").trim() || "Document";

  const subject = encodeURIComponent(`${subjectBase} follow-up`);

  const roleSpecificLine = (() => {
    if (contactRole.includes("loan") || contactRole.includes("lender")) {
      return "I also want to confirm the financing timeline and any remaining lender-side conditions.";
    }
    if (contactRole.includes("title") || contactRole.includes("escrow")) {
      return "I also want to confirm closing coordination, funds timing, and any remaining title or escrow items.";
    }
    if (contactRole.includes("insurance")) {
      return "I also want to confirm coverage details, effective date, and any lender-required insurance items.";
    }
    if (contactRole.includes("inspect")) {
      return "I also want to confirm any material findings, severity, and recommended follow-up items.";
    }
    if (
      contactRole.includes("agent") ||
      contactRole.includes("broker") ||
      contactRole.includes("office")
    ) {
      return "I also want to confirm current deal status, open signatures, and any next steps needed from the parties.";
    }
    return "I also want to confirm any open items that could affect timing or execution.";
  })();

  const bodyText = [
    `Hi ${contactName},`,
    "",
    `I am following up regarding the ${subjectBase.toLowerCase()}.`,
    "",
    "Please send the latest update when you can.",
    "",
    roleSpecificLine,
    "",
    "Thanks,",
  ].join("\n");

  const body = encodeURIComponent(bodyText);
  return `mailto:${emails.join(",")}?subject=${subject}&body=${body}`;
}

function ContactRow({
  contact,
  documentKindLabel,
}: {
  contact: DocumentContact;
  documentKindLabel?: string | null;
}) {
  const emailHref = buildEmailHref(contact, documentKindLabel);

  return (
    <div className="rounded-2xl border border-app bg-app px-4 py-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-sm font-semibold text-app-0">
            {contact.name || "Unnamed contact"}
          </div>
          <div className="mt-1 flex flex-wrap gap-2 text-xs text-app-4">
            <span>{contact.role_label || labelForRole(contact.role)}</span>
            {contact.company ? <span>• {contact.company}</span> : null}
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          {contact.is_primary ? (
            <span className="oh-pill">
              <Star className="mr-1 h-3.5 w-3.5" />
              primary
            </span>
          ) : null}
          {contact.waiting_on ? (
            <span className="oh-pill oh-pill-warn">
              <Flag className="mr-1 h-3.5 w-3.5" />
              action owner
            </span>
          ) : null}
        </div>
      </div>

      <div className="mt-3 space-y-2 text-sm text-app-3">
        <div className="flex items-center gap-2">
          <Phone className="h-4 w-4 text-app-4" />
          <span>{contact.phone || "No phone"}</span>
        </div>
        <div className="flex items-center gap-2">
          <Mail className="h-4 w-4 text-app-4" />
          <span>{contact.email || "No email"}</span>
        </div>

        {emailHref ? (
          <a
            href={emailHref}
            className="mt-2 inline-flex items-center gap-2 rounded-xl border border-app bg-app-panel px-3 py-2 text-xs font-medium text-app-0 transition hover:bg-app"
          >
            <Mail className="h-3.5 w-3.5" />
            Email
          </a>
        ) : null}

        {contact.why_relevant ? (
          <div className="rounded-2xl border border-app bg-app-panel px-3 py-2 text-xs text-app-3">
            {contact.why_relevant}
          </div>
        ) : null}
      </div>
    </div>
  );
}

export default function AcquisitionParticipantsPanel({
  participants = [],
  waitingOn,
  documentContactCard,
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
  const listingImportCount = participants.filter(
    (p) => String(p.source_type || "").toLowerCase() === "listing_import",
  ).length;
  const primaryDocContact =
    documentContactCard?.primary_contact_for_document_kind || null;
  const fallbackDocContacts = Array.isArray(
    documentContactCard?.fallback_contacts_for_document_kind,
  )
    ? documentContactCard?.fallback_contacts_for_document_kind || []
    : [];
  const missingContactRoles = Array.isArray(
    documentContactCard?.missing_contact_roles,
  )
    ? documentContactCard?.missing_contact_roles || []
    : [];
  const documentKindLabel =
    documentContactCard?.document_kind_label || "Document";

  return (
    <div className="rounded-3xl border border-app bg-app-panel p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
            Participants
          </div>
          <div className="mt-1 text-sm text-app-3">
            Listing-agent and office contacts now flow directly into
            acquisition, so the follow-up owner is visible where the deal work
            actually happens.
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

      {documentContactCard ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-muted p-4">
          <div className="flex flex-wrap items-start justify-between gap-2">
            <div>
              <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                Who to call now
              </div>
              <div className="mt-1 text-sm font-semibold text-app-0">
                {documentKindLabel}
              </div>
            </div>
            <ShieldCheck className="h-4 w-4 text-app-4" />
          </div>

          <div className="mt-3 space-y-3">
            {primaryDocContact ? (
              <ContactRow
                contact={primaryDocContact}
                documentKindLabel={documentKindLabel}
              />
            ) : (
              <div className="rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                No primary contact is saved yet for this document kind.
              </div>
            )}

            {fallbackDocContacts.length ? (
              <div>
                <div className="mb-2 text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Fallback contacts
                </div>
                <div className="space-y-3">
                  {fallbackDocContacts.map((contact, idx) => (
                    <ContactRow
                      key={`${contact.id || contact.role || "contact"}-${idx}`}
                      contact={contact}
                      documentKindLabel={documentKindLabel}
                    />
                  ))}
                </div>
              </div>
            ) : null}

            {missingContactRoles.length ? (
              <div className="rounded-2xl border border-app bg-app-panel px-4 py-3 text-sm text-app-3">
                <div className="text-[11px] uppercase tracking-[0.18em] text-app-4">
                  Missing roles
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {missingContactRoles.map((role) => (
                    <span key={role} className="oh-pill oh-pill-warn">
                      {role}
                    </span>
                  ))}
                </div>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      {!participants.length ? (
        <div className="mt-4 rounded-2xl border border-app bg-app-muted px-4 py-3 text-sm text-app-4">
          No participants have been normalized for this property yet.
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
            <span
              className={
                listingImportCount ? "oh-pill oh-pill-accent" : "oh-pill"
              }
            >
              listing contacts {listingImportCount}
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
                            <div className="mt-1 flex flex-wrap gap-2">
                              <span className="text-xs text-app-4">
                                {labelForRole(person.role)}
                              </span>
                              {person.company ? (
                                <span className="text-xs text-app-4">
                                  · {person.company}
                                </span>
                              ) : null}
                            </div>
                          </div>

                          <div className="flex flex-wrap gap-2">
                            {person.is_primary ? (
                              <span className="oh-pill">
                                <Star className="mr-1 h-3.5 w-3.5" />
                                primary
                              </span>
                            ) : null}
                            {person.waiting_on ? (
                              <span className="oh-pill oh-pill-warn">
                                <Flag className="mr-1 h-3.5 w-3.5" />
                                action owner
                              </span>
                            ) : null}
                            {sourceBadge(person.source_type)}
                          </div>
                        </div>

                        <div className="mt-3 space-y-2 text-sm text-app-3">
                          <div className="flex items-center gap-2">
                            <Phone className="h-4 w-4 text-app-4" />
                            <span>{person.phone || "No phone"}</span>
                          </div>

                          <div className="flex items-center gap-2">
                            <Mail className="h-4 w-4 text-app-4" />
                            <span>{person.email || "No email"}</span>
                          </div>

                          {person.notes ? (
                            <div className="flex items-start gap-2">
                              <AlertTriangle className="mt-0.5 h-4 w-4 text-app-4" />
                              <span>{person.notes}</span>
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
