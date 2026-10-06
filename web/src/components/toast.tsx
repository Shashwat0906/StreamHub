import { createContext, useContext, useState } from 'react';
import type { ReactNode } from 'react';
import { cx } from '../lib/format';
import { Icon } from './icons';

type ToastTone = 'success' | 'error' | 'info' | 'warn';
interface Toast {
  id: number;
  tone: ToastTone;
  title: string;
  body?: string;
}

const ToastContext = createContext<(tone: ToastTone, title: string, body?: string) => void>(() => {});

let nextId = 1;

export function ToastProvider({ children }: { children?: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const push = (tone: ToastTone, title: string, body?: string) => {
    const id = nextId++;
    setToasts((t) => [...t.slice(-4), { id, tone, title, body }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), tone === 'error' ? 8000 : 4500);
  };
  const style: Record<ToastTone, [string, 'check' | 'alert' | 'info']> = {
    success: ['text-ok', 'check'],
    error: ['text-bad', 'alert'],
    warn: ['text-warn', 'alert'],
    info: ['text-info', 'info'],
  };
  return (
    <ToastContext.Provider value={push}>
      {children}
      <div className="pointer-events-none fixed bottom-4 right-4 z-[60] flex w-[min(380px,calc(100vw-2rem))] flex-col gap-2" aria-live="polite">
        {toasts.map((t) => (
          <div key={t.id} className="slide-up pointer-events-auto flex items-start gap-3 rounded-xl border border-line bg-surface-1 p-3 shadow-xl">
            <Icon name={style[t.tone][1]} className={cx('mt-0.5 h-4 w-4 shrink-0', style[t.tone][0])} />
            <div className="min-w-0 flex-1">
              <div className="text-sm font-medium text-ink">{t.title}</div>
              {t.body && <div className="mt-0.5 break-words text-xs text-ink-2">{t.body}</div>}
            </div>
            <button
              className="text-ink-3 hover:text-ink"
              aria-label="Dismiss"
              onClick={() => setToasts((x) => x.filter((y) => y.id !== t.id))}
            >
              <Icon name="x" className="h-3.5 w-3.5" />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast() {
  return useContext(ToastContext);
}
