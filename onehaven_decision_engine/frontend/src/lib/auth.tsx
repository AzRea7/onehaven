// frontend/src/lib/auth.tsx
import React from "react";
import {
  api,
  setOrgSlug,
  getOrgSlug,
  clearApiCache,
  clearOrgSlug,
} from "./api";

export type Principal = {
  org_id: number;
  org_slug: string;
  user_id: number;
  email: string;
  role: string;
  plan_code?: string | null;
};

type AuthState = {
  loading: boolean;
  principal: Principal | null;
  error: string | null;
};

type AuthContextValue = AuthState & {
  refresh: () => Promise<void>;
  login: (args: {
    email: string;
    password: string;
    org_slug: string;
  }) => Promise<void>;
  register: (args: {
    email: string;
    password: string;
    org_slug: string;
    org_name?: string;
  }) => Promise<void>;
  logout: () => Promise<void>;
  switchOrg: (org_slug: string) => Promise<void>;
};

const AuthContext = React.createContext<AuthContextValue | null>(null);

function must(v: string, name: string) {
  const s = (v || "").trim();
  if (!s) throw new Error(`${name} is required`);
  return s;
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = React.useState<AuthState>({
    loading: true,
    principal: null,
    error: null,
  });

  // ✅ must be inside component
  const logoutInFlight = React.useRef(false);

  async function refresh() {
    // If we're in the middle of logging out, don't spam /auth/me
    if (logoutInFlight.current) {
      setState({ loading: false, principal: null, error: null });
      return;
    }

    setState((s) => ({ ...s, loading: true, error: null }));

    try {
      const me = (await api.authMe()) as Principal | null;

      if (me?.org_slug) setOrgSlug(me.org_slug);
      setState({ loading: false, principal: me, error: null });
    } catch {
      // If auth fails, clear stale org context so we stop sending "demo"
      clearOrgSlug();
      clearApiCache();
      setState({ loading: false, principal: null, error: null });
    }
  }

  async function login(args: {
    email: string;
    password: string;
    org_slug: string;
  }) {
    logoutInFlight.current = false;
    setState((s) => ({ ...s, loading: true, error: null }));

    try {
      clearApiCache();

      const email = must(args.email, "email").toLowerCase();
      const password = must(args.password, "password");
      const org_slug = must(args.org_slug, "org_slug");

      // set local org first (so header is correct immediately)
      setOrgSlug(org_slug);

      await api.authLogin({ email, password, org_slug });

      const me = (await api.authMe()) as Principal | null;
      if (me?.org_slug) setOrgSlug(me.org_slug);

      setState({ loading: false, principal: me, error: null });
    } catch (e: any) {
      clearOrgSlug();
      clearApiCache();
      setState({
        loading: false,
        principal: null,
        error: String(e?.message || e),
      });
      throw e;
    }
  }

  async function register(args: {
    email: string;
    password: string;
    org_slug: string;
    org_name?: string;
  }) {
    logoutInFlight.current = false;
    setState((s) => ({ ...s, loading: true, error: null }));

    try {
      clearApiCache();

      const email = must(args.email, "email").toLowerCase();
      const password = must(args.password, "password");
      const org_slug = must(args.org_slug, "org_slug");
      const org_name = (args.org_name || "").trim() || undefined;

      setOrgSlug(org_slug);

      await api.authRegister({ email, password, org_slug, org_name });

      const me = (await api.authMe()) as Principal | null;
      if (me?.org_slug) setOrgSlug(me.org_slug);

      setState({ loading: false, principal: me, error: null });
    } catch (e: any) {
      clearOrgSlug();
      clearApiCache();
      setState({
        loading: false,
        principal: null,
        error: String(e?.message || e),
      });
      throw e;
    }
  }

  async function logout() {
    logoutInFlight.current = true;
    setState((s) => ({ ...s, loading: true, error: null }));

    try {
      clearApiCache();
      await api.authLogout();
    } catch {
      // ignore
    } finally {
      clearApiCache();
      clearOrgSlug();
      setState({ loading: false, principal: null, error: null });
      window.location.href = "/login";
    }
  }

  async function switchOrg(org_slug: string) {
    logoutInFlight.current = false;
    setState((s) => ({ ...s, loading: true, error: null }));

    try {
      clearApiCache();
      const slug = must(org_slug, "org_slug");

      await api.authSelectOrg(slug);

      // set the context immediately so refresh sends X-Org-Slug
      setOrgSlug(slug);

      await refresh();
    } catch (e: any) {
      setState((s) => ({
        ...s,
        loading: false,
        error: String(e?.message || e),
      }));
      throw e;
    }
  }

  React.useEffect(() => {
    const slug = getOrgSlug();
    if (slug) setOrgSlug(slug);
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const value: AuthContextValue = {
    ...state,
    refresh,
    login,
    register,
    logout,
    switchOrg,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = React.useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used inside AuthProvider");
  return ctx;
}

export function AuthGate({ children }: { children: React.ReactNode }) {
  const { loading, principal } = useAuth();

  if (loading) {
    return (
      <div className="min-h-[60vh] flex items-center justify-center">
        <div className="text-white/70 text-sm">Loading…</div>
      </div>
    );
  }

  if (!principal) {
    window.location.href = "/login";
    return null;
  }

  return <>{children}</>;
}
