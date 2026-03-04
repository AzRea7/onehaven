// frontend/src/lib/authFlow.ts
import { api, setOrgSlug, clearApiCache } from "./api";

export async function finalizeAuth(orgSlug: string) {
  const slug = (orgSlug || "").trim();
  if (!slug) throw new Error("Missing org slug. Cannot finalize auth.");

  // Persist org context FIRST so all subsequent protected endpoints get X-Org-Slug.
  setOrgSlug(slug);
  clearApiCache();

  // Session org scoping (sets cookie org claim)
  await api.authSelectOrg(slug);

  // Final confirmation
  const me = await api.authMe();
  return me;
}
