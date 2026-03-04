// frontend/src/components/VirtualList.tsx
import React from "react";

type VirtualListProps<T> = {
  items: T[];
  /** Fixed row height in px. Keep this accurate. */
  itemHeight: number;
  /** Render a single row */
  renderRow: (item: T, index: number) => React.ReactNode;
  /** Overscan rows above/below the viewport */
  overscan?: number;
  /** Optional className for the scroll container */
  className?: string;
  /** Optional style for the scroll container */
  style?: React.CSSProperties;
  /** Optional: stable key */
  itemKey?: (item: T, index: number) => string | number;
};

/**
 * VirtualList (windowed rendering)
 * - renders only visible rows + overscan
 * - scroll handler is requestAnimationFrame throttled
 * - avoids heavy reflow and DOM bloat
 */
export default function VirtualList<T>({
  items,
  itemHeight,
  renderRow,
  overscan = 6,
  className,
  style,
  itemKey,
}: VirtualListProps<T>) {
  const ref = React.useRef<HTMLDivElement | null>(null);
  const raf = React.useRef<number | null>(null);

  const [viewportH, setViewportH] = React.useState(600);
  const [scrollTop, setScrollTop] = React.useState(0);

  const totalH = items.length * itemHeight;

  const compute = React.useCallback(() => {
    const el = ref.current;
    if (!el) return;
    setViewportH(el.clientHeight || 600);
    setScrollTop(el.scrollTop || 0);
  }, []);

  React.useEffect(() => {
    compute();
    const el = ref.current;
    if (!el) return;

    const onScroll = () => {
      if (raf.current != null) return;
      raf.current = requestAnimationFrame(() => {
        raf.current = null;
        compute();
      });
    };

    el.addEventListener("scroll", onScroll, { passive: true });

    const ro = new ResizeObserver(() => compute());
    ro.observe(el);

    return () => {
      el.removeEventListener("scroll", onScroll);
      ro.disconnect();
      if (raf.current != null) cancelAnimationFrame(raf.current);
    };
  }, [compute]);

  const startIndex = Math.max(0, Math.floor(scrollTop / itemHeight) - overscan);
  const endIndex = Math.min(
    items.length,
    Math.ceil((scrollTop + viewportH) / itemHeight) + overscan,
  );

  const slice = items.slice(startIndex, endIndex);

  return (
    <div
      ref={ref}
      className={className}
      style={{
        overflowY: "auto",
        overflowX: "hidden",
        WebkitOverflowScrolling: "touch",
        contain: "content",
        ...style,
      }}
    >
      <div style={{ height: totalH, position: "relative" }}>
        <div
          style={{
            position: "absolute",
            top: startIndex * itemHeight,
            left: 0,
            right: 0,
          }}
        >
          {slice.map((item, i) => {
            const idx = startIndex + i;
            const key = itemKey ? itemKey(item, idx) : idx;
            return (
              <div
                key={key}
                style={{
                  height: itemHeight,
                  // Helps browsers skip painting offscreen rows
                  contentVisibility: "auto" as any,
                  containIntrinsicSize: `${itemHeight}px` as any,
                }}
              >
                {renderRow(item, idx)}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
