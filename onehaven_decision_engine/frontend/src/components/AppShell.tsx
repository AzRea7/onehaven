import React from "react";
import { useLocation } from "react-router-dom";
import AppHeader from "./AppHeader";
import AppFooter from "./AppFooter";

type ThemeMode = "light" | "dark";

const STORAGE_KEY = "onehaven-theme";

function getInitialTheme(): ThemeMode {
  if (typeof window === "undefined") return "dark";

  const stored = window.localStorage.getItem(STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;

  const prefersDark =
    typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-color-scheme: dark)").matches;

  return prefersDark ? "dark" : "light";
}

export default function AppShell({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const [theme, setTheme] = React.useState<ThemeMode>(() => getInitialTheme());

  React.useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
    window.localStorage.setItem(STORAGE_KEY, theme);
  }, [theme]);

  const isAuthPage =
    location.pathname.startsWith("/login") ||
    location.pathname.startsWith("/register");

  return (
    <div className="oh-root">
      <div className="oh-bg">
        <div className="oh-blob oh-a" />
        <div className="oh-blob oh-b" />
        <div className="oh-blob oh-c" />
        <div className="oh-grid-mask" />
      </div>

      {!isAuthPage ? (
        <AppHeader
          theme={theme}
          onToggleTheme={() =>
            setTheme((prev) => (prev === "dark" ? "light" : "dark"))
          }
        />
      ) : null}

      <main className={isAuthPage ? "oh-main-auth" : "oh-main"}>
        {children}
      </main>

      {!isAuthPage ? <AppFooter /> : null}
    </div>
  );
}
