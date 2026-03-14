import { useEffect, useLayoutEffect, useState } from "react";

export type ThemePreference = "system" | "light" | "dark";
export type ResolvedTheme = "light" | "dark";

export const THEME_STORAGE_KEY = "rfx-theme-preference";

type LegacyMediaQueryList = MediaQueryList & {
  addListener: (listener: (event: MediaQueryListEvent) => void) => void;
  removeListener: (listener: (event: MediaQueryListEvent) => void) => void;
};

function isThemePreference(value: string | null): value is ThemePreference {
  return value === "system" || value === "light" || value === "dark";
}

function getThemeMediaQuery(): MediaQueryList | null {
  if (
    typeof window === "undefined" ||
    typeof window.matchMedia !== "function"
  ) {
    return null;
  }
  return window.matchMedia("(prefers-color-scheme: dark)");
}

export function resolveTheme(preference: ThemePreference): ResolvedTheme {
  if (preference === "dark") {
    return "dark";
  }
  if (preference === "light") {
    return "light";
  }
  return getThemeMediaQuery()?.matches ? "dark" : "light";
}

function readStoredThemePreference(): ThemePreference {
  if (typeof window === "undefined") {
    return "system";
  }
  const storedPreference = window.localStorage.getItem(THEME_STORAGE_KEY);
  return isThemePreference(storedPreference) ? storedPreference : "system";
}

function subscribeToMediaQuery(
  mediaQuery: MediaQueryList,
  listener: (event: MediaQueryListEvent) => void,
): () => void {
  if (typeof mediaQuery.addEventListener === "function") {
    mediaQuery.addEventListener("change", listener);
    return () => mediaQuery.removeEventListener("change", listener);
  }

  const legacyMediaQuery = mediaQuery as LegacyMediaQueryList;
  legacyMediaQuery.addListener(listener);
  return () => legacyMediaQuery.removeListener(listener);
}

export function useThemePreference() {
  const [preference, setPreference] = useState<ThemePreference>(() =>
    readStoredThemePreference(),
  );
  const [resolvedTheme, setResolvedTheme] = useState<ResolvedTheme>(() =>
    resolveTheme(readStoredThemePreference()),
  );

  useEffect(() => {
    setResolvedTheme(resolveTheme(preference));
  }, [preference]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(THEME_STORAGE_KEY, preference);
  }, [preference]);

  useLayoutEffect(() => {
    document.documentElement.dataset.theme = resolvedTheme;
  }, [resolvedTheme]);

  useEffect(() => {
    if (preference !== "system") {
      return;
    }

    const mediaQuery = getThemeMediaQuery();
    if (!mediaQuery) {
      return;
    }

    setResolvedTheme(mediaQuery.matches ? "dark" : "light");
    return subscribeToMediaQuery(mediaQuery, (event) => {
      setResolvedTheme(event.matches ? "dark" : "light");
    });
  }, [preference]);

  return {
    preference,
    resolvedTheme,
    setPreference,
  };
}
