import { useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { useLive } from './api/live';
import { Icon } from './components/icons';
import type { IconName } from './components/icons';
import { useToast } from './components/toast';
import { Badge, Button, Dot, ErrorState, Skeleton } from './components/ui';
import { cx, fmtAgo } from './lib/format';
import { useNow } from './lib/hooks';
import { Link, useRoute } from './lib/router';
import { useTheme } from './lib/theme';
import { BrokersPage } from './pages/Brokers';
import { DLQPage } from './pages/DLQ';
import { FailuresPage } from './pages/Failures';
import { GroupsPage } from './pages/Groups';
import { OverviewPage } from './pages/Overview';
import { ProducePage } from './pages/Produce';
import { StreamPage } from './pages/Stream';
import { TopicsPage } from './pages/Topics';

interface NavItem {
  route: string;
  label: string;
  icon: IconName;
  badge?: (s: ReturnType<typeof useLive>) => ReactNode;
}

const nav: NavItem[] = [
  { route: 'overview', label: 'Overview', icon: 'overview' },
  {
    route: 'brokers',
    label: 'Broker Cluster',
    icon: 'server',
    badge: ({ snapshot }) =>
      snapshot && snapshot.totals.healthyBrokers < snapshot.totals.brokers ? (
        <Badge tone="bad">{snapshot.totals.brokers - snapshot.totals.healthyBrokers}</Badge>
      ) : null,
  },
  {
    route: 'topics',
    label: 'Topics & Partitions',
    icon: 'topics',
    badge: ({ snapshot }) => (snapshot && snapshot.totals.underReplicated > 0 ? <Badge tone="warn">{snapshot.totals.underReplicated}</Badge> : null),
  },
  { route: 'produce', label: 'Produce Message', icon: 'send' },
  { route: 'stream', label: 'Live Stream', icon: 'stream' },
  { route: 'groups', label: 'Consumer Groups', icon: 'groups' },
  {
    route: 'dlq',
    label: 'Dead Letter Queue',
    icon: 'dlq',
    badge: ({ snapshot }) => (snapshot && snapshot.totals.dlqMessages > 0 ? <Badge tone="bad">{snapshot.totals.dlqMessages}</Badge> : null),
  },
  { route: 'failures', label: 'Failure Simulation', icon: 'failure' },
];

export function App() {
  const route = useRoute();
  const live = useLive();
  const [theme, toggleTheme] = useTheme();
  const [menuOpen, setMenuOpen] = useState(false);
  const toast = useToast();
  const now = useNow(1000);

  useEffect(() => setMenuOpen(false), [route.name, route.params.join('/')]);

  // Surface important cluster events as toasts, wherever the user is.
  useEffect(
    () =>
      live.onEvent((e) => {
        const important = ['broker_unavailable', 'broker_fenced', 'controller_changed', 'partition_offline', 'broker_unfenced', 'consumer_crashed'];
        if (important.includes(e.kind)) {
          toast(e.severity === 'error' ? 'error' : e.severity === 'success' ? 'success' : 'warn', e.kind.replace(/_/g, ' '), e.message);
        }
      }),
    [],
  );

  const s = live.snapshot;
  const stale = live.lastUpdate > 0 && now - live.lastUpdate > 5000;

  return (
    <div className="flex min-h-screen">
      {/* Sidebar */}
      <aside
        className={cx(
          'fixed inset-y-0 left-0 z-30 flex w-60 flex-col border-r border-line bg-surface-1 transition-transform lg:translate-x-0',
          menuOpen ? 'translate-x-0' : '-translate-x-full',
        )}
      >
        <div className="flex h-14 items-center gap-2.5 border-b border-line px-4">
          <span className="grid h-8 w-8 place-items-center rounded-lg bg-accent text-white dark:text-surface-0">
            <Icon name="stream" className="h-4 w-4" strokeWidth={2.5} />
          </span>
          <div>
            <div className="text-sm font-semibold leading-tight text-ink">StreamHub</div>
            <div className="text-[11px] leading-tight text-ink-3">Cluster console</div>
          </div>
        </div>
        <nav className="flex-1 space-y-0.5 overflow-y-auto p-2">
          {nav.map((n) => {
            const active = route.name === n.route;
            return (
              <Link
                key={n.route}
                to={'/' + n.route}
                className={cx(
                  'flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-sm transition-colors',
                  active ? 'bg-surface-2 font-medium text-ink' : 'text-ink-2 hover:bg-surface-2 hover:text-ink',
                )}
              >
                <Icon name={n.icon} className={cx('h-4 w-4', active ? 'text-accent' : 'text-ink-3')} />
                <span className="flex-1">{n.label}</span>
                {n.badge?.(live)}
              </Link>
            );
          })}
        </nav>
        <div className="border-t border-line p-3 text-[11px] text-ink-3">
          {s ? (
            <div className="space-y-1">
              <div className="flex items-center justify-between">
                <span>Mode</span>
                <Badge tone={s.mode === 'managed' ? 'violet' : 'info'}>{s.mode}</Badge>
              </div>
              <div className="flex items-center justify-between">
                <span>Controller</span>
                <span className="font-mono text-ink-2">{s.controllerId >= 0 ? `broker ${s.controllerId}` : 'none'}</span>
              </div>
            </div>
          ) : (
            <Skeleton className="h-8" />
          )}
        </div>
      </aside>
      {menuOpen && <div className="fixed inset-0 z-20 bg-black/40 lg:hidden" onClick={() => setMenuOpen(false)} />}

      {/* Main column */}
      <div className="flex min-w-0 flex-1 flex-col lg:pl-60">
        <header className="sticky top-0 z-10 flex h-14 items-center gap-3 border-b border-line bg-surface-0/85 px-4 backdrop-blur lg:px-6">
          <button className="rounded-md p-1.5 text-ink-2 hover:bg-surface-2 lg:hidden" onClick={() => setMenuOpen(true)} aria-label="Open menu">
            <Icon name="menu" />
          </button>
          <ClusterHealthPill />
          <div className="ml-auto flex items-center gap-2">
            <ConnectionPill stale={stale} />
            <Button variant="ghost" icon={theme === 'dark' ? 'sun' : 'moon'} onClick={toggleTheme} title="Toggle theme" />
          </div>
        </header>

        <main className="mx-auto w-full max-w-[1500px] flex-1 px-4 py-6 lg:px-6">
          {s && !s.reachable && (
            <div className="mb-5">
              <ErrorState
                title="Cluster unreachable"
                message={`${s.error ?? 'No broker answered.'} The dashboard keeps retrying every second; pages below show the last known state.`}
              />
            </div>
          )}
          <Page name={route.name} params={route.params} />
        </main>
      </div>
    </div>
  );
}

function Page({ name, params }: { name: string; params: string[] }) {
  const { snapshot, conn } = useLive();
  if (!snapshot) return <LoadingPage conn={conn} />;
  switch (name) {
    case 'brokers':
      return <BrokersPage selected={params[0] ? Number(params[0]) : undefined} />;
    case 'topics':
      return <TopicsPage />;
    case 'produce':
      return <ProducePage />;
    case 'stream':
      return <StreamPage />;
    case 'groups':
      return <GroupsPage />;
    case 'dlq':
      return <DLQPage />;
    case 'failures':
      return <FailuresPage />;
    default:
      return <OverviewPage />;
  }
}

function LoadingPage({ conn }: { conn: string }) {
  return (
    <div>
      <div className="mb-6 flex items-center gap-2 text-sm text-ink-2">
        <span className="h-2 w-2 animate-ping rounded-full bg-accent" />
        {conn === 'reconnecting' ? 'Cannot reach the dashboard server, retrying…' : 'Connecting to the cluster…'}
      </div>
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        {Array.from({ length: 8 }).map((_, i) => (
          <Skeleton key={i} className="h-28" />
        ))}
      </div>
      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Skeleton className="h-64" />
        <Skeleton className="h-64" />
      </div>
    </div>
  );
}

function ConnectionPill({ stale }: { stale: boolean }) {
  const { conn, lastUpdate } = useLive();
  const now = useNow(1000);
  const tone = conn === 'live' && !stale ? 'ok' : conn === 'connecting' ? 'info' : 'bad';
  const label = conn === 'live' && !stale ? 'Live' : conn === 'connecting' ? 'Connecting' : 'Disconnected';
  return (
    <span
      className="hidden items-center gap-2 rounded-full border border-line bg-surface-1 px-2.5 py-1 text-xs text-ink-2 sm:inline-flex"
      title="Updates are pushed from the dashboard server over Server-Sent Events"
    >
      <Dot tone={tone} pulse={tone === 'ok'} />
      {label}
      {lastUpdate > 0 && <span className="text-ink-3">· {fmtAgo(lastUpdate, now)}</span>}
    </span>
  );
}

function ClusterHealthPill() {
  const { snapshot } = useLive();
  if (!snapshot) return <Skeleton className="h-6 w-48" />;
  const t = snapshot.totals;
  const degraded = t.healthyBrokers < t.brokers || t.offlinePartitions > 0 || t.underReplicated > 0;
  const down = !snapshot.reachable || t.offlinePartitions > 0;
  const tone = down ? 'bad' : degraded ? 'warn' : 'ok';
  const text = !snapshot.reachable
    ? 'Cluster unreachable'
    : t.offlinePartitions > 0
      ? `${t.offlinePartitions} partition(s) offline`
      : degraded
        ? `Degraded: ${t.healthyBrokers}/${t.brokers} brokers healthy, ${t.underReplicated} under-replicated`
        : `Healthy: ${t.brokers} brokers, ${t.partitions} partitions fully replicated`;
  return (
    <div className="flex min-w-0 items-center gap-2 text-sm">
      <Dot tone={tone} pulse={tone !== 'ok'} />
      <span className="truncate text-ink-2">{text}</span>
    </div>
  );
}
