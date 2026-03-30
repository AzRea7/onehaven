import React from "react";
import { ChevronRight, Check } from "lucide-react";
import clsx from "clsx";

export type AppSelectOption = {
  value: string;
  label: string;
};

type AppSelectProps = {
  value: string;
  options: AppSelectOption[];
  onChange: (value: string) => void;
  placeholder?: string;
  className?: string;
  buttonClassName?: string;
  menuClassName?: string;
  disabled?: boolean;
};

export default function AppSelect({
  value,
  options,
  onChange,
  placeholder = "Select",
  className,
  buttonClassName,
  menuClassName,
  disabled = false,
}: AppSelectProps) {
  const [open, setOpen] = React.useState(false);
  const rootRef = React.useRef<HTMLDivElement | null>(null);

  const selected = options.find((opt) => opt.value === value);

  React.useEffect(() => {
    function onDocClick(event: MouseEvent) {
      if (!rootRef.current) return;
      if (!rootRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    }

    function onEscape(event: KeyboardEvent) {
      if (event.key === "Escape") setOpen(false);
    }

    document.addEventListener("mousedown", onDocClick);
    document.addEventListener("keydown", onEscape);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onEscape);
    };
  }, []);

  return (
    <div ref={rootRef} className={clsx("oh-select-popover", className)}>
      <button
        type="button"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => !disabled && setOpen((v) => !v)}
        className={clsx("oh-select-trigger", buttonClassName)}
      >
        <span className="truncate text-left">
          {selected?.label || placeholder}
        </span>
        <ChevronRight
          className={clsx(
            "h-4 w-4 shrink-0 transition-transform duration-200",
            open ? "rotate-90" : "rotate-0",
          )}
        />
      </button>

      {open ? (
        <div className={clsx("oh-select-menu", menuClassName)} role="listbox">
          {options.map((option) => {
            const isSelected = option.value === value;
            return (
              <button
                key={option.value}
                type="button"
                role="option"
                aria-selected={isSelected}
                onClick={() => {
                  onChange(option.value);
                  setOpen(false);
                }}
                className={clsx(
                  "oh-select-option",
                  isSelected && "oh-select-option-active",
                )}
              >
                <span>{option.label}</span>
                {isSelected ? <Check className="h-4 w-4 shrink-0" /> : null}
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
