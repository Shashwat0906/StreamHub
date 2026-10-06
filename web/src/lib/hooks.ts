import { useEffect, useRef, useState } from 'react';

/** Width of an element, updated on resize (for responsive SVG charts). */
export function useWidth<T extends Element>(): [{ current: T | null }, number] {
  const ref = useRef<T | null>(null);
  const [width, setWidth] = useState(0);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) setWidth(Math.floor(e.contentRect.width));
    });
    ro.observe(el);
    setWidth(Math.floor(el.getBoundingClientRect().width));
    return () => ro.disconnect();
  }, []);
  return [ref, width];
}

/** Re-render every `ms` (for "x seconds ago" labels). */
export function useNow(ms = 1000): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(t);
  }, [ms]);
  return now;
}

/** Returns true for ~1.5s after `value` changes (to flash updated cells). */
export function useChanged(value: unknown): boolean {
  const prev = useRef(value);
  const [changed, setChanged] = useState(false);
  useEffect(() => {
    if (prev.current !== value) {
      prev.current = value;
      setChanged(true);
      const t = setTimeout(() => setChanged(false), 1500);
      return () => clearTimeout(t);
    }
  }, [value]);
  return changed;
}
