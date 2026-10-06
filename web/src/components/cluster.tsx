import type { ReactNode } from 'react';
import type { BrokerView, ClusterEvent, Phase, PartitionView, Snapshot, TopicView } from '../api/types';
import { cx, fmtAgo, fmtTime } from '../lib/format';
import { Icon } from './icons';
import type { IconName } from './icons';
import { Badge, Dot } from './ui';
import type { Tone } from './ui';

export function brokerTone(status: string): Tone {
  return status === 'healthy' ? 'ok' : status === 'fenced' ? 'warn' : 'bad';
}

export function brokerMap(s: Snapshot): Map<number, BrokerView> {
  return new Map(s.brokers.map((b) => [b.id, b]));
}

/** One replica of a partition: leader / in-sync follower / out of sync / down. */
export function ReplicaChip({ id, partition, brokers }: { id: number; partition: PartitionView; brokers: Map<number, BrokerView> }) {
  const b = brokers.get(id);
  const down = !b || b.status === 'offline';
  const leader = partition.leader === id;
  const inSync = partition.isr.includes(id);
  let cls = 'bg-surface-2 text-ink-2 ring-line';
  let title = `Broker ${id}: follower, in sync`;
  if (leader && !down) {
    cls = 'bg-accent text-white dark:text-surface-0 ring-accent';
    title = `Broker ${id}: LEADER (epoch ${partition.leaderEpoch})`;
  } else if (down) {
    cls = 'bg-bad-soft text-bad ring-bad/40 line-through';
    title = `Broker ${id}: offline`;
  } else if (!inSync) {
    cls = 'bg-warn-soft text-warn ring-warn/40';
    title = `Broker ${id}: follower, OUT OF SYNC (not in ISR)`;
  } else {
    cls = 'bg-ok-soft text-ok ring-ok/30';
  }
  return (
    <span title={title} className={cx('inline-flex h-6 min-w-6 items-center justify-center gap-0.5 rounded-md px-1.5 font-mono text-[11px] font-semibold ring-1 ring-inset', cls)}>
      {leader && !down && <Icon name="crown" className="h-3 w-3" strokeWidth={2.5} />}
      {id}
    </span>
  );
}

export function ReplicaLegend() {
  const item = (cls: string, label: string, crown?: boolean) => (
    <span className="inline-flex items-center gap-1.5">
      <span className={cx('inline-flex h-4 min-w-4 items-center justify-center rounded px-1 ring-1 ring-inset', cls)}>
        {crown && <Icon name="crown" className="h-2.5 w-2.5" strokeWidth={2.5} />}
      </span>
      {label}
    </span>
  );
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-ink-3">
      {item('bg-accent text-white dark:text-surface-0 ring-accent', 'Leader', true)}
      {item('bg-ok-soft text-ok ring-ok/30', 'Follower (in ISR)')}
      {item('bg-warn-soft text-warn ring-warn/40', 'Follower (out of sync)')}
      {item('bg-bad-soft text-bad ring-bad/40', 'Broker offline')}
    </div>
  );
}

/** Partitions × brokers grid showing who leads and who follows. */
export function PartitionMatrix({ topic, snapshot, highlight }: { topic: TopicView; snapshot: Snapshot; highlight?: Set<string> }) {
  const brokers = snapshot.brokers;
  const bmap = brokerMap(snapshot);
  return (
    <div className="overflow-x-auto">
      <table className="text-xs">
        <thead>
          <tr>
            <th className="pb-2 pr-3 text-left font-medium text-ink-3">Partition</th>
            {brokers.map((b) => (
              <th key={b.id} className="px-1 pb-2 text-center font-medium text-ink-3">
                <span className="inline-flex items-center gap-1">
                  <Dot tone={brokerTone(b.status)} pulse={b.status !== 'healthy'} />B{b.id}
                </span>
              </th>
            ))}
            <th className="pb-2 pl-3 text-left font-medium text-ink-3">ISR</th>
          </tr>
        </thead>
        <tbody>
          {topic.partitions.map((p) => {
            const changed = highlight?.has(`${topic.name}-${p.id}`);
            return (
              <tr key={p.id} className={cx(changed && 'flash-in')}>
                <td className="py-1 pr-3 font-mono text-ink-2">
                  {topic.name}-{p.id}
                  {p.offline && <Badge tone="bad" className="ml-2">offline</Badge>}
                </td>
                {brokers.map((b) => {
                  const isReplica = p.replicas.includes(b.id);
                  return (
                    <td key={b.id} className="px-1 py-1 text-center">
                      {isReplica ? (
                        <ReplicaChip id={b.id} partition={p} brokers={bmap} />
                      ) : (
                        <span className="inline-block h-6 w-6 rounded-md border border-dashed border-line" title="not a replica" />
                      )}
                    </td>
                  );
                })}
                <td className="py-1 pl-3 font-mono text-ink-2">[{p.isr.join(', ')}]</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------- events

export const phaseMeta: Record<Phase, { label: string; icon: IconName; tone: Tone; step: number }> = {
  unavailable: { label: 'Broker unavailable', icon: 'power', tone: 'bad', step: 1 },
  detected: { label: 'Failure detected (fenced)', icon: 'alert', tone: 'bad', step: 1 },
  leader_failure: { label: 'Leader failure', icon: 'crown', tone: 'bad', step: 2 },
  election: { label: 'New leader elected', icon: 'crown', tone: 'warn', step: 3 },
  isr: { label: 'ISR change', icon: 'topics', tone: 'warn', step: 4 },
  reassignment: { label: 'Partition reassignment', icon: 'groups', tone: 'info', step: 5 },
  recovery: { label: 'Recovery', icon: 'check', tone: 'ok', step: 6 },
};

const sevTone: Record<ClusterEvent['severity'], Tone> = { info: 'info', warn: 'warn', error: 'bad', success: 'ok' };
const sevDot: Record<ClusterEvent['severity'], string> = {
  info: 'bg-info',
  warn: 'bg-warn',
  error: 'bg-bad',
  success: 'bg-ok',
};

export function EventTimeline({
  events,
  limit = 50,
  now,
  empty,
  compact,
}: {
  events: ClusterEvent[];
  limit?: number;
  now: number;
  empty?: ReactNode;
  compact?: boolean;
}) {
  const shown = events.slice(-limit).reverse();
  if (shown.length === 0) return <div className="py-6 text-center text-sm text-ink-3">{empty ?? 'No events yet.'}</div>;
  return (
    <ol className="relative">
      {shown.map((e, i) => (
        <li key={e.id} className={cx('relative flex gap-3 pb-3 pl-1', i === 0 && 'slide-up')}>
          {i < shown.length - 1 && <span className="absolute left-[8px] top-4 h-full w-px bg-line" />}
          <span className={cx('relative z-10 mt-1.5 h-2.5 w-2.5 shrink-0 rounded-full ring-4 ring-surface-1', sevDot[e.severity])} />
          <div className="min-w-0 flex-1">
            <div className={cx('text-ink', compact ? 'text-xs' : 'text-sm')}>{e.message}</div>
            <div className="mt-0.5 flex flex-wrap items-center gap-2 text-[11px] text-ink-3">
              <span className="font-mono" title={new Date(e.time).toISOString()}>
                {fmtTime(e.time)}
              </span>
              <span>· {fmtAgo(e.time, now)}</span>
              {e.phase && <Badge tone={phaseMeta[e.phase].tone}>{phaseMeta[e.phase].label}</Badge>}
              {!e.phase && <Badge tone={sevTone[e.severity]}>{e.kind.replace(/_/g, ' ')}</Badge>}
            </div>
          </div>
        </li>
      ))}
    </ol>
  );
}
