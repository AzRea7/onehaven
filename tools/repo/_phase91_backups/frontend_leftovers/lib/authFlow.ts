import { api, setOrgSlug, clearApiCache, clearOrgSlug } from "./api";

function getReadableError(e: any): string {
  const status = e?.response?.status;
  const detail =
    e?.response?.data?.detail ||
    e?.response?.data?.message ||
    e?.message ||
    "Unknown authentication error";

  if (status === 502) {
    return "Authentication service is unavailable right now (502 Bad Gateway). The frontend reached the proxy, but the backend auth service did not respond correctly.";
  }

  if (status === 401) {
    return "Authentication failed. Your session may be invalid or expired.";
  }

  if (status === 403) {
    return "Authentication succeeded, but access to this organization was denied.";
  }

  return status ? `${detail} (${status})` : String(detail);
}

export async function finalizeAuth(orgSlug: string) {
  const slug = (orgSlug || "").trim();
  if (!slug) throw new Error("Missing org slug. Cannot finalize auth.");

  try {
    // Persist org context first so protected endpoints send X-Org-Slug.
    setOrgSlug(slug);
    clearApiCache();

    // Ask backend to scope the session to the selected org.
    await api.authSelectOrg(slug);

    // Confirm final authenticated principal.
    const me = await api.authMe();

    if (!me) {
      throw new Error("Authenticated principal was empty after org selection.");
    }

    return me;
  } catch (e: any) {
    // Reset local context so we do not keep sending a broken/stale org state.
    clearApiCache();
    clearOrgSlug();

    throw new Error(getReadableError(e));
  }
}
