import {
  AlertCircle,
  Check,
  ChevronDown,
  LoaderCircle,
  RefreshCw,
  XCircle,
} from "lucide-react";
import { type ReactNode, useEffect, useRef, useState } from "react";
import { APIError } from "./api";
import { stateLabel, timeAgo } from "./format";

export interface ViewStateProps {
  fetching: boolean;
  updatedAt: number;
  onRefresh: () => void;
}

export function ViewHeader({
  title,
  fetching,
  updatedAt,
  onRefresh,
}: ViewStateProps & { title: string }) {
  return (
    <div className="view-header">
      <h1>{title}</h1>
      <div className="refresh-state" aria-live="polite">
        <span>{updatedAt ? `Updated ${timeAgo(new Date(updatedAt).toISOString())}` : "Waiting for data"}</span>
        <button className="icon-button" aria-label="Refresh" onClick={onRefresh} disabled={fetching}>
          <RefreshCw size={16} className={fetching ? "spin" : ""} />
        </button>
      </div>
    </div>
  );
}

export function StatusBadge({ state }: { state: string }) {
  return <span className={`status-badge status-${state}`}><span className="status-dot" />{stateLabel(state)}</span>;
}

export function PanelHeading({ title, aside }: { title: string; aside?: string }) {
  return <div className="panel-heading"><h2>{title}</h2>{aside && <span>{aside}</span>}</div>;
}

export function EmptyState({
  icon,
  title,
  description,
  action,
}: {
  icon: ReactNode;
  title: string;
  description: string;
  action?: ReactNode;
}) {
  return <div className="empty-state"><span className="empty-icon">{icon}</span><h2>{title}</h2><p>{description}</p>{action}</div>;
}

export function LoadingState({ label }: { label: string }) {
  return <div className="page centered-state" role="status"><LoaderCircle size={24} className="spin" /><span>{label}</span></div>;
}

export function ErrorState({ error, onRetry }: { error: Error | null; onRetry: () => void }) {
  return (
    <div className="page centered-state error-state" role="alert">
      <span className="empty-icon danger"><XCircle size={22} /></span>
      <h1>Couldn’t load this view</h1>
      <p>{errorMessage(error)}</p>
      <button className="button button-secondary" onClick={onRetry}><RefreshCw size={15} /> Try again</button>
    </div>
  );
}

export function StaleBanner({ error }: { error: Error }) {
  return <div className="stale-banner" role="status"><AlertCircle size={16} /> Showing the last available data. Refresh failed: {errorMessage(error)}</div>;
}

export function InlineError({ error }: { error: Error | null }) {
  if (!error) return null;
  return <div className="inline-error" role="alert"><AlertCircle size={16} /> {errorMessage(error)}</div>;
}

export function MultiSelectField({
  id,
  values,
  selected,
  onChange,
  placeholder,
  emptyLabel,
  disabled,
}: {
  id: string;
  values: { id: string; label: string }[];
  selected: string[];
  onChange: (next: string[]) => void;
  placeholder: string;
  emptyLabel?: string;
  disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const selectedSet = new Set(selected);

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

  const toggle = (id: string, checked: boolean) => {
    onChange(checked ? [...selected, id] : selected.filter((value) => value !== id));
  };

  const summary = selected.length === 0
    ? placeholder
    : selected.length === 1
      ? values.find((value) => value.id === selected[0])?.label ?? "1 selected"
      : `${selected.length} selected`;

  return (
    <div className="multiselect" ref={rootRef}>
      <button
        id={id}
        type="button"
        className="multiselect-trigger"
        aria-haspopup="listbox"
        aria-expanded={open}
        disabled={disabled || values.length === 0}
        onClick={() => setOpen((value) => !value)}
      >
        <span className={selected.length === 0 ? "multiselect-placeholder" : ""}>{summary}</span>
        <ChevronDown size={16} />
      </button>
      {open && (
        <div className="multiselect-menu" role="listbox" aria-multiselectable="true">
          {values.length === 0
            ? <span className="field-hint">{emptyLabel ?? "Nothing to choose from."}</span>
            : values.map((value) => (
              <label className="multiselect-option" key={value.id}>
                <input
                  type="checkbox"
                  checked={selectedSet.has(value.id)}
                  onChange={(event) => toggle(value.id, event.target.checked)}
                />
                <span>{value.label}</span>
                {selectedSet.has(value.id) && <Check size={14} className="multiselect-check" />}
              </label>
            ))}
        </div>
      )}
    </div>
  );
}

function errorMessage(error: Error | null): string {
  if (!error) return "The server returned no data.";
  if (error instanceof APIError) return `${error.message} (${error.code})`;
  return error.message || "The request failed.";
}
