import React from "react";
import { Link, useNavigate } from "react-router-dom";
import {
  Building2,
  LockKeyhole,
  Mail,
  ShieldCheck,
  UserPlus,
} from "lucide-react";
import { useAuth } from "../lib/auth";
import { getOrgSlug } from "../lib/api";
import { finalizeAuth } from "../lib/authFlow";
import PageShell from "../components/PageShell";
import Surface from "../components/Surface";

export default function Register() {
  const nav = useNavigate();
  const { register, error, loading, principal } = useAuth();

  const [email, setEmail] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [orgSlug, setOrgSlugState] = React.useState(getOrgSlug());
  const [orgName, setOrgName] = React.useState("Demo Org");
  const [localError, setLocalError] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (principal) nav("/dashboard");
  }, [principal, nav]);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLocalError(null);

    const slug = (orgSlug || "").trim();
    if (!slug) {
      setLocalError("Org slug is required.");
      return;
    }

    try {
      await register({ email, password, org_slug: slug, org_name: orgName });
      await finalizeAuth(slug);
      nav("/dashboard");
    } catch (err: any) {
      setLocalError(String(err?.message || err));
    }
  }

  return (
    <PageShell className="oh-auth-wrap">
      <div className="oh-auth-grid">
        <div className="oh-auth-side p-6 md:p-8">
          <div className="text-[11px] uppercase tracking-[0.2em] text-app-4">
            Create your workspace
          </div>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-app-0">
            Register
          </h1>
          <p className="mt-3 max-w-xl text-sm leading-7 text-app-3">
            This creates the user, the org, and the initial owner membership in
            one move. Much cleaner than manually summoning them from the
            database swamp.
          </p>

          <div className="mt-6 grid gap-3">
            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <UserPlus className="h-4 w-4 text-app-4" />
                Owner bootstrap
              </div>
              <div className="mt-2 text-sm text-app-3">
                Registration creates your org and your initial owner access in
                the same flow.
              </div>
            </div>

            <div className="rounded-2xl border border-app bg-app-panel px-4 py-4">
              <div className="flex items-center gap-2 text-sm font-semibold text-app-0">
                <ShieldCheck className="h-4 w-4 text-app-4" />
                Session finalized
              </div>
              <div className="mt-2 text-sm text-app-3">
                After registration, the app finalizes org selection so protected
                routes immediately work instead of wandering into auth limbo.
              </div>
            </div>
          </div>
        </div>

        <Surface
          className="oh-auth-card"
          title="Create account"
          subtitle="Choose the org slug you want to operate under, then create your owner account."
        >
          <form className="space-y-4" onSubmit={onSubmit}>
            <label className="block">
              <span className="oh-field-label">Org slug</span>
              <div className="relative">
                <Building2
                  aria-hidden="true"
                  className="pointer-events-none absolute left-4 top-1/2 z-10 h-4 w-4 -translate-y-1/2 text-app-4"
                />
                <input
                  value={orgSlug}
                  onChange={(e) => setOrgSlugState(e.target.value)}
                  placeholder="demo-org"
                  className="oh-input !h-12 !pl-14 !pr-4"
                />
              </div>
            </label>

            <label className="block">
              <span className="oh-field-label">Org name</span>
              <div className="relative">
                <Building2
                  aria-hidden="true"
                  className="pointer-events-none absolute left-4 top-1/2 z-10 h-4 w-4 -translate-y-1/2 text-app-4"
                />
                <input
                  value={orgName}
                  onChange={(e) => setOrgName(e.target.value)}
                  placeholder="Demo Org"
                  className="oh-input !h-12 !pl-14 !pr-4"
                />
              </div>
            </label>

            <label className="block">
              <span className="oh-field-label">Email</span>
              <div className="relative">
                <Mail
                  aria-hidden="true"
                  className="pointer-events-none absolute left-4 top-1/2 z-10 h-4 w-4 -translate-y-1/2 text-app-4"
                />
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="you@example.com"
                  className="oh-input !h-12 !pl-14 !pr-4"
                />
              </div>
            </label>

            <label className="block">
              <span className="oh-field-label">Password</span>
              <div className="relative">
                <LockKeyhole
                  aria-hidden="true"
                  className="pointer-events-none absolute left-4 top-1/2 z-10 h-4 w-4 -translate-y-1/2 text-app-4"
                />
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  className="oh-input !h-12 !pl-14 !pr-4"
                />
              </div>
            </label>

            {localError || error ? (
              <div className="rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-300">
                {localError || error}
              </div>
            ) : null}

            <div className="flex flex-wrap items-center gap-3 pt-2">
              <button
                type="submit"
                className="oh-btn oh-btn-primary"
                disabled={loading}
              >
                {loading ? "Creating…" : "Create account"}
              </button>

              <Link to="/login" className="oh-btn oh-btn-secondary">
                Back to login
              </Link>
            </div>
          </form>
        </Surface>
      </div>
    </PageShell>
  );
}
