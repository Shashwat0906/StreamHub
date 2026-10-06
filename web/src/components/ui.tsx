import { useEffect } from 'react';
import type { ReactNode } from 'react';
import { cx } from '../lib/format';
import { Icon } from './icons';
import type { IconName } from './icons';

// ---------------------------------------------------------------- layout

export function PageHeader({ title, subtitle, actions }: { title: string; subtitle?: ReactNode; actions?: ReactNode }) {
  return (
    <div className="mb-5 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
      <div>
        <h1 className="text-xl font-semibold tracking-tight text-ink">{title}</h1>
        {subtitle && <p className="mt-1 max-w-3xl text-sm text-ink-2">{subtitle}</p>}
      </div>
      {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
    </div>
  );
}

export function Card({
  title,
  subtitle,
  actions,
  children,
  className,
  bodyClassName,
}: {
  title?: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
  children?: ReactNode;
  className?: string;
  bodyClassName?: string;
}) {
  return (
    <section className={cx('rounded-xl border border-line bg-surface-1 shadow-sm', className)}>
      {(title || actions) && (
        <header className="flex items-start justify-between gap-3 border-b border-line px-4 py-3">
          <div className="min-w-0">
            {title && <h2 className="text-sm font-semibold text-ink">{title}</h2>}
            {subtitle && <p className="mt-0.5 text-xs text-ink-3">{subtitle}</p>}
          </div>
          {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
        </header>
      )}
      <div className={cx('p-4', bodyClassName)}>{children}</div>
    </section>
  );
}

// ---------------------------------------------------------------- stat card

const toneText: Record<string, string> = {
  default: 'text-ink',
  ok: 'text-ok',
  warn: 'text-warn',
  bad: 'text-bad',
  info: 'text-info',
  violet: 'text-violet',
};
const toneIconBg: Record<string, string> = {
  default: 'bg-surface-2 text-ink-2',
  ok: 'bg-ok-soft text-ok',
  warn: 'bg-warn-soft text-warn',
  bad: 'bg-bad-soft text-bad',
  info: 'bg-info-soft text-info',
  violet: 'bg-violet-soft text-violet',
};

export type Tone = 'default' | 'ok' | 'warn' | 'bad' | 'info' | 'violet';

export function StatCard({
  label,
  value,
  sub,
  icon,
  tone = 'default',
  footer,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  icon: IconName;
  tone?: Tone;
  footer?: ReactNode;
}) {
  return (
    <div className="flex flex-col rounded-xl border border-line bg-surface-1 p-4 shadow-sm">
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium uppercase tracking-wide text-ink-3">{label}</span>
        <span className={cx('grid h-7 w-7 place-items-center rounded-lg', toneIconBg[tone])}>
          <Icon name={icon} className="h-3.5 w-3.5" />
        </span>
      </div>
      <div className={cx('mt-2 text-2xl font-semibold tabular-nums tracking-tight', toneText[tone])}>{value}</div>
      {sub && <div className="mt-1 text-xs text-ink-2">{sub}</div>}
      {footer && <div className="mt-3">{footer}</div>}
    </div>
  );
}

// ---------------------------------------------------------------- badges

const badgeTone: Record<string, string> = {
  default: 'bg-surface-2 text-ink-2 ring-line',
  ok: 'bg-ok-soft text-ok ring-ok/25',
  warn: 'bg-warn-soft text-warn ring-warn/25',
  bad: 'bg-bad-soft text-bad ring-bad/25',
  info: 'bg-info-soft text-info ring-info/25',
  violet: 'bg-violet-soft text-violet ring-violet/25',
};

export function Badge({ tone = 'default', children, className, title }: { tone?: Tone; children?: ReactNode; className?: string; title?: string }) {
  return (
    <span
      title={title}
      className={cx(
        'inline-flex items-center gap-1 whitespace-nowrap rounded-md px-1.5 py-0.5 text-[11px] font-medium ring-1 ring-inset',
        badgeTone[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

const dotTone: Record<string, string> = {
  ok: 'bg-ok text-ok',
  warn: 'bg-warn text-warn',
  bad: 'bg-bad text-bad',
  info: 'bg-info text-info',
  default: 'bg-ink-3 text-ink-3',
  violet: 'bg-violet text-violet',
};

export function Dot({ tone = 'default', pulse }: { tone?: Tone; pulse?: boolean }) {
  return <span className={cx('inline-block h-2 w-2 shrink-0 rounded-full', dotTone[tone], pulse && 'pulse-dot')} />;
}

export function StatusBadge({ status }: { status: string }) {
  const map: Record<string, [Tone, string]> = {
    healthy: ['ok', 'Healthy'],
    fenced: ['warn', 'Fenced'],
    offline: ['bad', 'Offline'],
    running: ['ok', 'Running'],
    starting: ['info', 'Starting'],
    crashed: ['bad', 'Crashed'],
    stopped: ['default', 'Stopped'],
    killed: ['bad', 'Killed'],
    exited: ['bad', 'Exited'],
    Stable: ['ok', 'Stable'],
    PreparingRebalance: ['warn', 'Rebalancing'],
    Empty: ['default', 'Empty'],
  };
  const [tone, label] = map[status] ?? ['default', status];
  return (
    <Badge tone={tone}>
      <Dot tone={tone} pulse={tone === 'bad' || tone === 'warn'} />
      {label}
    </Badge>
  );
}

// ---------------------------------------------------------------- buttons & inputs

type ButtonVariant = 'primary' | 'secondary' | 'danger' | 'ghost';
const buttonVariant: Record<ButtonVariant, string> = {
  primary: 'bg-accent text-white dark:text-surface-0 hover:brightness-110 border-transparent',
  secondary: 'bg-surface-1 text-ink border-line hover:bg-surface-2',
  danger: 'bg-bad-soft text-bad border-bad/30 hover:bg-bad hover:text-white',
  ghost: 'bg-transparent text-ink-2 border-transparent hover:bg-surface-2 hover:text-ink',
};

export function Button({
  children,
  onClick,
  variant = 'secondary',
  icon,
  disabled,
  loading,
  type = 'button',
  size = 'md',
  title,
  className,
}: {
  children?: ReactNode;
  onClick?: () => void;
  variant?: ButtonVariant;
  icon?: IconName;
  disabled?: boolean;
  loading?: boolean;
  type?: 'button' | 'submit';
  size?: 'sm' | 'md';
  title?: string;
  className?: string;
}) {
  return (
    <button
      type={type}
      title={title}
      onClick={onClick}
      disabled={disabled || loading}
      className={cx(
        'inline-flex items-center justify-center gap-1.5 rounded-lg border font-medium transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent disabled:cursor-not-allowed disabled:opacity-50',
        size === 'sm' ? 'h-7 px-2 text-xs' : 'h-9 px-3 text-sm',
        buttonVariant[variant],
        className,
      )}
    >
      {loading ? <Spinner className="h-3.5 w-3.5" /> : icon && <Icon name={icon} className={size === 'sm' ? 'h-3.5 w-3.5' : 'h-4 w-4'} />}
      {children}
    </button>
  );
}

export function Field({ label, hint, children, error, className }: { label: string; hint?: ReactNode; children?: ReactNode; error?: string; className?: string }) {
  return (
    <label className={cx('block', className)}>
      <span className="mb-1 block text-xs font-medium text-ink-2">{label}</span>
      {children}
      {error ? <span className="mt-1 block text-xs text-bad">{error}</span> : hint && <span className="mt-1 block text-xs text-ink-3">{hint}</span>}
    </label>
  );
}

const inputCls =
  'w-full rounded-lg border border-line bg-surface-0 px-3 text-sm text-ink placeholder:text-ink-3 focus:border-accent focus:outline-none focus:ring-2 focus:ring-accent/20 disabled:opacity-60';

export function TextInput({
  value,
  onChange,
  placeholder,
  type = 'text',
  min,
  max,
  disabled,
  className,
  mono,
}: {
  value: string | number;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: string;
  min?: number;
  max?: number;
  disabled?: boolean;
  className?: string;
  mono?: boolean;
}) {
  return (
    <input
      type={type}
      value={value}
      min={min}
      max={max}
      disabled={disabled}
      placeholder={placeholder}
      onChange={(e: { target: { value: string } }) => onChange(e.target.value)}
      className={cx(inputCls, 'h-9', mono && 'font-mono', className)}
    />
  );
}

export function TextArea({ value, onChange, rows = 8, placeholder, invalid }: { value: string; onChange: (v: string) => void; rows?: number; placeholder?: string; invalid?: boolean }) {
  return (
    <textarea
      value={value}
      rows={rows}
      spellCheck={false}
      placeholder={placeholder}
      onChange={(e: { target: { value: string } }) => onChange(e.target.value)}
      className={cx(inputCls, 'py-2 font-mono text-[13px] leading-relaxed', invalid && 'border-warn focus:border-warn focus:ring-warn/20')}
    />
  );
}

export function Select<T extends string>({
  value,
  onChange,
  options,
  className,
  disabled,
}: {
  value: T;
  onChange: (v: T) => void;
  options: { value: T; label: string }[];
  className?: string;
  disabled?: boolean;
}) {
  return (
    <select
      value={value}
      disabled={disabled}
      onChange={(e: { target: { value: string } }) => onChange(e.target.value as T)}
      className={cx(inputCls, 'h-9 pr-8', className)}
    >
      {options.map((o) => (
        <option key={o.value} value={o.value}>
          {o.label}
        </option>
      ))}
    </select>
  );
}

export function Segmented<T extends string>({
  value,
  onChange,
  options,
}: {
  value: T;
  onChange: (v: T) => void;
  options: { value: T; label: string; hint?: string }[];
}) {
  return (
    <div className="inline-flex rounded-lg border border-line bg-surface-0 p-0.5" role="radiogroup">
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          role="radio"
          aria-checked={value === o.value}
          title={o.hint}
          onClick={() => onChange(o.value)}
          className={cx(
            'rounded-md px-3 py-1.5 text-xs font-medium transition-colors',
            value === o.value ? 'bg-surface-1 text-ink shadow-sm ring-1 ring-line' : 'text-ink-3 hover:text-ink',
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

export function Toggle({ checked, onChange, label }: { checked: boolean; onChange: (v: boolean) => void; label: string }) {
  return (
    <label className="inline-flex cursor-pointer select-none items-center gap-2 text-xs text-ink-2">
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        onClick={() => onChange(!checked)}
        className={cx('relative h-5 w-9 rounded-full transition-colors', checked ? 'bg-accent' : 'bg-surface-3')}
      >
        <span className={cx('absolute top-0.5 h-4 w-4 rounded-full bg-white shadow transition-all', checked ? 'left-[18px]' : 'left-0.5')} />
      </button>
      {label}
    </label>
  );
}

// ---------------------------------------------------------------- states

export function Spinner({ className = 'h-4 w-4' }: { className?: string }) {
  return (
    <svg className={cx('animate-spin', className)} viewBox="0 0 24 24" fill="none" aria-label="loading">
      <circle cx="12" cy="12" r="9" stroke="currentColor" strokeOpacity="0.2" strokeWidth="3" />
      <path d="M21 12a9 9 0 0 0-9-9" stroke="currentColor" strokeWidth="3" strokeLinecap="round" />
    </svg>
  );
}

export function Skeleton({ className }: { className?: string }) {
  return <div className={cx('animate-pulse rounded-md bg-surface-2', className)} />;
}

export function EmptyState({ icon = 'info', title, children, action }: { icon?: IconName; title: string; children?: ReactNode; action?: ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center px-6 py-10 text-center">
      <span className="grid h-10 w-10 place-items-center rounded-full bg-surface-2 text-ink-3">
        <Icon name={icon} className="h-5 w-5" />
      </span>
      <h3 className="mt-3 text-sm font-semibold text-ink">{title}</h3>
      {children && <p className="mt-1 max-w-md text-sm text-ink-2">{children}</p>}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}

export function ErrorState({ title, message, action }: { title: string; message: string; action?: ReactNode }) {
  return (
    <div className="flex items-start gap-3 rounded-xl border border-bad/30 bg-bad-soft p-4 text-sm">
      <Icon name="alert" className="mt-0.5 h-4 w-4 shrink-0 text-bad" />
      <div className="min-w-0 flex-1">
        <div className="font-semibold text-bad">{title}</div>
        <div className="mt-0.5 break-words text-ink-2">{message}</div>
        {action && <div className="mt-3">{action}</div>}
      </div>
    </div>
  );
}

export function Callout({ tone = 'info', children, icon = 'info' }: { tone?: Tone; children?: ReactNode; icon?: IconName }) {
  return (
    <div className={cx('flex items-start gap-2.5 rounded-lg border px-3 py-2.5 text-sm', badgeTone[tone], 'ring-0 border-current/20')}>
      <Icon name={icon} className="mt-0.5 h-4 w-4 shrink-0" />
      <div className="min-w-0 text-ink-2">{children}</div>
    </div>
  );
}

// ---------------------------------------------------------------- overlays

export function Modal({
  open,
  onClose,
  title,
  children,
  footer,
  wide,
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  children?: ReactNode;
  footer?: ReactNode;
  wide?: boolean;
}) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/50 p-4 backdrop-blur-sm sm:items-center" onClick={onClose}>
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(e: { stopPropagation: () => void }) => e.stopPropagation()}
        className={cx('slide-up w-full rounded-xl border border-line bg-surface-1 shadow-2xl', wide ? 'max-w-3xl' : 'max-w-lg')}
      >
        <div className="flex items-center justify-between border-b border-line px-5 py-3">
          <h2 className="text-sm font-semibold text-ink">{title}</h2>
          <button onClick={onClose} className="rounded-md p-1 text-ink-3 hover:bg-surface-2 hover:text-ink" aria-label="Close">
            <Icon name="x" />
          </button>
        </div>
        <div className="max-h-[70vh] overflow-y-auto px-5 py-4">{children}</div>
        {footer && <div className="flex justify-end gap-2 border-t border-line px-5 py-3">{footer}</div>}
      </div>
    </div>
  );
}

export function Drawer({ open, onClose, title, children }: { open: boolean; onClose: () => void; title: ReactNode; children?: ReactNode }) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-40 flex justify-end bg-black/40 backdrop-blur-[2px]" onClick={onClose}>
      <aside
        onClick={(e: { stopPropagation: () => void }) => e.stopPropagation()}
        className="slide-up flex h-full w-full max-w-2xl flex-col border-l border-line bg-surface-1 shadow-2xl"
      >
        <div className="flex items-center justify-between border-b border-line px-5 py-3">
          <div className="text-sm font-semibold text-ink">{title}</div>
          <button onClick={onClose} className="rounded-md p-1 text-ink-3 hover:bg-surface-2 hover:text-ink" aria-label="Close">
            <Icon name="x" />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-5">{children}</div>
      </aside>
    </div>
  );
}

// ---------------------------------------------------------------- table

export function Table({ head, children, className }: { head: ReactNode[]; children?: ReactNode; className?: string }) {
  return (
    <div className={cx('overflow-x-auto', className)}>
      <table className="w-full min-w-max text-left text-sm">
        <thead>
          <tr className="border-b border-line text-[11px] uppercase tracking-wide text-ink-3">
            {head.map((h, i) => (
              <th key={i} className="whitespace-nowrap px-3 py-2 font-medium first:pl-0 last:pr-0">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-line">{children}</tbody>
      </table>
    </div>
  );
}

export function Td({ children, className, mono, title }: { children?: ReactNode; className?: string; mono?: boolean; title?: string }) {
  return (
    <td title={title} className={cx('whitespace-nowrap px-3 py-2 align-middle first:pl-0 last:pr-0', mono && 'font-mono text-[13px]', className)}>
      {children}
    </td>
  );
}

export function Meter({ value, max, tone = 'ok', className }: { value: number; max: number; tone?: Tone; className?: string }) {
  const pct = max > 0 ? Math.min(100, (value / max) * 100) : 0;
  const bar: Record<string, string> = { ok: 'bg-ok', warn: 'bg-warn', bad: 'bg-bad', info: 'bg-info', default: 'bg-ink-3', violet: 'bg-violet' };
  return (
    <div className={cx('h-1.5 w-full overflow-hidden rounded-full bg-surface-3', className)}>
      <div className={cx('h-full rounded-full transition-all duration-500', bar[tone])} style={{ width: `${pct}%` }} />
    </div>
  );
}

export function KV({ k, v }: { k: ReactNode; v: ReactNode }) {
  return (
    <div className="flex items-baseline justify-between gap-4 py-1.5 text-sm">
      <span className="text-ink-3">{k}</span>
      <span className="min-w-0 truncate text-right font-medium text-ink">{v}</span>
    </div>
  );
}
