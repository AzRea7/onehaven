import React from "react";
import { createPortal } from "react-dom";
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

type MenuPosition = {
  top: number;
  left: number;
  width: number;
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
  const [position, setPosition] = React.useState<MenuPosition | null>(null);
  const rootRef = React.useRef<HTMLDivElement | null>(null);
  const menuRef = React.useRef<HTMLDivElement | null>(null);

  const selected = options.find((opt) => opt.value === value);

  const updatePosition = React.useCallback(() => {
    if (!rootRef.current) return;
    const rect = rootRef.current.getBoundingClientRect();
    const menuHeight =
      menuRef.current?.offsetHeight ?? Math.min(options.length * 46 + 16, 320);
    const viewportHeight = window.innerHeight;
    const spaceBelow = viewportHeight - rect.bottom;
    const openUpward =
      spaceBelow < Math.min(menuHeight + 12, 220) && rect.top > spaceBelow;
    const top = openUpward
      ? Math.max(8, rect.top - menuHeight - 8)
      : Math.min(viewportHeight - 8, rect.bottom + 8);

    setPosition({
      top,
      left: Math.max(8, rect.left),
      width: rect.width,
    });
  }, [options.length]);

  React.useEffect(() => {
    if (!open) return;
    updatePosition();

    function onDocClick(event: MouseEvent) {
      const target = event.target as Node;
      if (rootRef.current?.contains(target)) return;
      if (menuRef.current?.contains(target)) return;
      setOpen(false);
    }

    function onEscape(event: KeyboardEvent) {
      if (event.key === "Escape") setOpen(false);
    }

    function onWindowChange() {
      updatePosition();
    }

    document.addEventListener("mousedown", onDocClick);
    document.addEventListener("keydown", onEscape);
    window.addEventListener("resize", onWindowChange);
    window.addEventListener("scroll", onWindowChange, true);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onEscape);
      window.removeEventListener("resize", onWindowChange);
      window.removeEventListener("scroll", onWindowChange, true);
    };
  }, [open, updatePosition]);

  return (
    <div ref={rootRef} className={clsx("oh-select-popover", className)}>
      <button
        type="button"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => {
          if (disabled) return;
          setOpen((v) => !v);
        }}
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

      {open && position
        ? createPortal(
            <div
              ref={menuRef}
              className={clsx("oh-select-menu", menuClassName)}
              role="listbox"
              style={{
                position: "fixed",
                top: position.top,
                left: position.left,
                width: position.width,
              }}
            >
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
            </div>,
            document.body,
          )
        : null}
    </div>
  );
}
