import React from "react";
import { Link, useLocation } from "react-router-dom";
import clsx from "clsx";
import { Moon, Sparkles, Sun, User2 } from "lucide-react";
import PaneSwitcher from "./PaneSwitcher";

const NavLink = ({ to, label }: { to: string; label: string }) => {
  const loc = useLocation();
  const active = loc.pathname === to || loc.pathname.startsWith(to + "/");

  return (
    <Link
      to={to}
      className={clsx(
        "oh-nav-link cursor-pointer select-none",
        active && "oh-nav-link-active",
      )}
    >
      {label}
    </Link>
  );
};

function inferHeaderContext(pathname: string) {
  if (pathname.startsWith("/panes/compliance")) {
    return {
      label: "Compliance / S8",
      activePane: "compliance",
    };
  }
  if (pathname.startsWith("/panes/tenants")) {
    return {
      label: "Tenant placement",
      activePane: "tenants",
    };
  }
  if (pathname.startsWith("/panes/management")) {
    return {
      label: "Management",
      activePane: "management",
    };
  }
  if (pathname.startsWith("/properties/")) {
    return {
      label: "Property view",
      activePane: null,
    };
  }
  if (pathname.startsWith("/properties")) {
    return {
      label: "Investor",
      activePane: "investor",
    };
  }
  if (pathname.startsWith("/dashboard")) {
    return {
      label: "Portfolio",
      activePane: null,
    };
  }
  return {
    label: "OneHaven",
    activePane: null,
  };
}

function inferActivePane(pathname: string) {
  if (pathname.startsWith("/panes/compliance")) return "compliance";
  if (pathname.startsWith("/panes/tenants")) return "tenants";
  if (pathname.startsWith("/panes/management")) return "management";
  if (pathname.startsWith("/properties")) return "investor";
  return null;
}

export default function AppHeader({
  right,
  children,
  theme = "dark",
  onToggleTheme,
}: {
  right?: React.ReactNode;
  children?: React.ReactNode;
  theme?: "light" | "dark";
  onToggleTheme?: () => void;
}) {
  const location = useLocation();
  const context = inferHeaderContext(location.pathname);
  const activePane = inferActivePane(location.pathname) ?? context.activePane;

  return (
    <header className="oh-app-header w-full border-b border-app bg-[color:var(--bg-elevated)]/95 backdrop-blur-xl">
      <div className="w-full px-4 md:px-6 xl:px-8 py-4">
        <div className="oh-app-header-row">
          <div className="oh-header-block oh-header-nav">
            <PaneSwitcher activePane={activePane} />
          </div>
        </div>

        {(right || children) && (
          <div className="mt-4 flex items-center justify-end gap-2">
            {right}
            {children}
          </div>
        )}
      </div>
    </header>
  );
}
