import { Check, Monitor, Moon, Sun } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { ThemePreference } from "./theme";
import { useTheme } from "./theme";

const OPTIONS: { value: ThemePreference; label: string; icon: typeof Sun }[] = [
  { value: "light", label: "Light", icon: Sun },
  { value: "dark", label: "Dark", icon: Moon },
  { value: "system", label: "Match system", icon: Monitor },
];

export function ThemeToggle() {
  const { theme, resolvedTheme, setTheme } = useTheme();
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    const onDocClick = (event: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(event.target as Node)) setOpen(false);
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onDocClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const ActiveIcon = resolvedTheme === "light" ? Sun : Moon;

  return (
    <div className="theme-toggle" ref={rootRef}>
      <button
        className="icon-button"
        aria-label="Change theme"
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
      >
        <ActiveIcon size={17} />
      </button>
      {open && (
        <div className="theme-menu" role="menu">
          {OPTIONS.map(({ value, label, icon: Icon }) => (
            <button
              key={value}
              role="menuitemradio"
              aria-checked={theme === value}
              className={`theme-menu-item ${theme === value ? "active" : ""}`}
              onClick={() => {
                setTheme(value);
                setOpen(false);
              }}
            >
              <Icon size={15} />
              <span>{label}</span>
              {theme === value && <Check size={14} className="theme-menu-check" />}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
