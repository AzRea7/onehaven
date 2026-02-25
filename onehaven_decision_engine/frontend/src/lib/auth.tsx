import React from "react";
import { api, setOrgSlug, getOrgSlug } from "./api";

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

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = React.useState<AuthState>({
    loading: true,
    principal: null,
    error: null,
  });

  async function refresh() {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      const me = (await api.authMe()) as Principal;
      setState({ loading: false, principal: me, error: null });
    } catch {
      setState({ loading: false, principal: null, error: null });
    }
  }

  async function login(args: {
    email: string;
    password: string;
    org_slug: string;
  }) {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      setOrgSlug(args.org_slug);
      await api.authLogin(args);
      await refresh();
    } catch (e: any) {
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
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      setOrgSlug(args.org_slug);
      await api.authRegister({
        email: args.email,
        password: args.password,
        org_slug: args.org_slug,
        org_name: args.org_name,
      });
      await refresh();
    } catch (e: any) {
      setState({
        loading: false,
        principal: null,
        error: String(e?.message || e),
      });
      throw e;
    }
  }

  async function logout() {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      await api.authLogout();
    } finally {
      setState({ loading: false, principal: null, error: null });
    }
  }

  async function switchOrg(org_slug: string) {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      await api.authSelectOrg(org_slug);
      setOrgSlug(org_slug);
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
    if (!localStorage.getItem("org_slug")) setOrgSlug(getOrgSlug());
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
