import React from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../lib/auth";
import { getOrgSlug } from "../lib/api";
import GlassCard from "../components/GlassCard";
import PageHero from "../components/PageHero";

export default function Register() {
  const nav = useNavigate();
  const { register, error, loading, principal } = useAuth();

  const [email, setEmail] = React.useState("");
  const [password, setPassword] = React.useState("");

  // ✅ keep org context consistent with the rest of the app
  const [orgSlug, setOrgSlug] = React.useState(getOrgSlug());
  const [orgName, setOrgName] = React.useState("Demo Org");

  React.useEffect(() => {
    if (principal) nav("/dashboard");
  }, [principal, nav]);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    await register({ email, password, org_slug: orgSlug, org_name: orgName });
    nav("/dashboard");
  }

  return (
    <div className="space-y-6">
      <PageHero
        eyebrow="Create your org"
        title="Register"
        subtitle="Creates user + org + owner membership, then sets your auth cookie."
      />

      <GlassCard>
        <form onSubmit={onSubmit} className="space-y-4">
          <div className="grid md:grid-cols-4 gap-3">
            <label className="space-y-1 md:col-span-1">
              <div className="text-xs text-white/60">Org slug</div>
              <input
                className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2 outline-none focus:border-white/30"
                value={orgSlug}
                onChange={(e) => setOrgSlug(e.target.value)}
                placeholder="acme"
                autoComplete="organization"
              />
            </label>

            <label className="space-y-1 md:col-span-1">
              <div className="text-xs text-white/60">Org name</div>
              <input
                className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2 outline-none focus:border-white/30"
                value={orgName}
                onChange={(e) => setOrgName(e.target.value)}
                placeholder="Acme Holdings"
              />
            </label>

            <label className="space-y-1 md:col-span-1">
              <div className="text-xs text-white/60">Email</div>
              <input
                className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2 outline-none focus:border-white/30"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@domain.com"
                autoComplete="email"
              />
            </label>

            <label className="space-y-1 md:col-span-1">
              <div className="text-xs text-white/60">Password</div>
              <input
                type="password"
                className="w-full rounded-xl bg-white/5 border border-white/10 px-3 py-2 outline-none focus:border-white/30"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••"
                autoComplete="new-password"
              />
            </label>
          </div>

          {error ? (
            <div className="text-xs text-red-300 bg-red-500/10 border border-red-500/20 rounded-xl px-3 py-2">
              {error}
            </div>
          ) : null}

          <div className="flex items-center justify-between gap-3">
            <button
              disabled={loading}
              className="rounded-xl bg-white text-black px-4 py-2 text-sm font-semibold hover:opacity-90 disabled:opacity-60"
              type="submit"
            >
              {loading ? "Creating…" : "Create org + account"}
            </button>

            <Link
              className="text-sm text-white/70 hover:text-white"
              to="/login"
            >
              Already have an account? Log in →
            </Link>
          </div>
        </form>
      </GlassCard>
    </div>
  );
}
