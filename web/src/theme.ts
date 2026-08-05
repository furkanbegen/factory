import { useCallback, useEffect, useState } from "react";

export type ThemePreference = "light" | "dark" | "system";
export type ResolvedTheme = "light" | "dark";

const STORAGE_KEY = "factory-theme";

function systemPrefersLight(): boolean {
  return window.matchMedia("(prefers-color-scheme: light)").matches;
}

function readStoredPreference(): ThemePreference {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === "light" || stored === "dark" || stored === "system") return stored;
  } catch {
    // ignore storage access errors (private mode, etc.)
  }
  return "system";
}

function applyResolvedTheme(resolved: ResolvedTheme) {
  document.documentElement.setAttribute("data-theme", resolved);
}

export function useTheme(): {
  theme: ThemePreference;
  resolvedTheme: ResolvedTheme;
  setTheme: (theme: ThemePreference) => void;
} {
  const [theme, setThemeState] = useState<ThemePreference>(readStoredPreference);
  const [systemLight, setSystemLight] = useState(systemPrefersLight);

  const resolvedTheme: ResolvedTheme = theme === "system" ? (systemLight ? "light" : "dark") : theme;

  const setTheme = useCallback((next: ThemePreference) => {
    setThemeState(next);
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // ignore storage access errors
    }
  }, []);

  useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: light)");
    const onChange = () => setSystemLight(media.matches);
    media.addEventListener("change", onChange);
    return () => media.removeEventListener("change", onChange);
  }, []);

  useEffect(() => {
    applyResolvedTheme(resolvedTheme);
  }, [resolvedTheme]);

  return { theme, resolvedTheme, setTheme };
}
