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
  maxHeight: number;
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
  const optionRefs = React.useRef<Array<HTMLButtonElement | null>>([]);
  const [highlightedIndex, setHighlightedIndex] = React.useState<number>(-1);

  const selectedIndex = options.findIndex((opt) => opt.value === value);
  const selected = selectedIndex >= 0 ? options[selectedIndex] : undefined;

  const updatePosition = React.useCallback(() => {
    if (!rootRef.current) return;
    const rect = rootRef.current.getBoundingClientRect();
    const estimatedHeight = Math.min(
      Math.max(options.length, 1) * 46 + 16,
      320,
    );
    const menuHeight = menuRef.current?.offsetHeight ?? estimatedHeight;
    const viewportHeight = window.innerHeight;
    const spaceBelow = viewportHeight - rect.bottom - 8;
    const spaceAbove = rect.top - 8;

    const openUpward =
      spaceBelow < Math.min(menuHeight, 220) && spaceAbove > spaceBelow;

    const maxHeight = Math.max(
      120,
      Math.min(320, openUpward ? spaceAbove : spaceBelow),
    );

    const top = openUpward
      ? Math.max(8, rect.top - Math.min(menuHeight, maxHeight) - 8)
      : Math.min(viewportHeight - 8, rect.bottom + 8);

    setPosition({
      top,
      left: Math.max(8, rect.left),
      width: Math.max(rect.width, 180),
      maxHeight,
    });
  }, [options.length]);

  React.useEffect(() => {
    if (!open) return;
    updatePosition();
    setHighlightedIndex(selectedIndex >= 0 ? selectedIndex : 0);

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
  }, [open, selectedIndex, updatePosition]);

  React.useEffect(() => {
    if (!open || highlightedIndex < 0) return;
    optionRefs.current[highlightedIndex]?.scrollIntoView({ block: "nearest" });
  }, [open, highlightedIndex]);

  const commitIndex = React.useCallback(
    (index: number) => {
      const option = options[index];
      if (!option) return;
      onChange(option.value);
      setOpen(false);
    },
    [onChange, options],
  );

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
        onKeyDown={(event) => {
          if (disabled) return;
          if (
            event.key === "ArrowDown" ||
            event.key === "Enter" ||
            event.key === " "
          ) {
            event.preventDefault();
            setOpen(true);
          }
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
              tabIndex={-1}
              style={{
                position: "fixed",
                top: position.top,
                left: position.left,
                width: position.width,
                maxHeight: position.maxHeight,
                overflowY: "auto",
                zIndex: 1000,
              }}
              onKeyDown={(event) => {
                if (!options.length) return;
                if (event.key === "ArrowDown") {
                  event.preventDefault();
                  setHighlightedIndex((prev) =>
                    prev < 0 ? 0 : Math.min(prev + 1, options.length - 1),
                  );
                } else if (event.key === "ArrowUp") {
                  event.preventDefault();
                  setHighlightedIndex((prev) =>
                    prev < 0 ? options.length - 1 : Math.max(prev - 1, 0),
                  );
                } else if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  if (highlightedIndex >= 0) commitIndex(highlightedIndex);
                } else if (event.key === "Escape") {
                  event.preventDefault();
                  setOpen(false);
                }
              }}
            >
              {options.map((option, index) => {
                const isSelected = option.value === value;
                const isHighlighted = index === highlightedIndex;
                return (
                  <button
                    key={option.value}
                    ref={(node) => {
                      optionRefs.current[index] = node;
                    }}
                    type="button"
                    role="option"
                    aria-selected={isSelected}
                    onMouseEnter={() => setHighlightedIndex(index)}
                    onClick={() => commitIndex(index)}
                    className={clsx(
                      "oh-select-option",
                      isSelected && "oh-select-option-active",
                      isHighlighted && "bg-app-muted",
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
