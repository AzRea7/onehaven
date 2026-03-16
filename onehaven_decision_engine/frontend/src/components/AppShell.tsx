import React from "react";
import { useLocation } from "react-router-dom";

import Header from "./Header";
import Footer from "./Footer";

type ThemeMode = "light" | "dark";

const STORAGE_KEY = "onehaven-theme";

function getInitialTheme(): ThemeMode {
  if (typeof window === "undefined") return "dark";
  const saved = window.localStorage.getItem(STORAGE_KEY);
  if (saved === "light" || saved === "dark") return saved;

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
    <div className="app-root">
      <div className="app-backdrop">
        <div className="app-blob app-blob-a" />
        <div className="app-blob app-blob-b" />
        <div className="app-blob app-blob-c" />
        <div className="app-grid-mask" />
      </div>

      {!isAuthPage ? (
        <Header
          theme={theme}
          onToggleTheme={() =>
            setTheme((prev) => (prev === "dark" ? "light" : "dark"))
          }
        />
      ) : null}

      <main className={isAuthPage ? "app-main-auth" : "app-main"}>
        {children}
      </main>

      {!isAuthPage ? <Footer /> : null}
    </div>
  );
}
