import { useState } from 'react';
import { api, errorMessage } from '../api/client';
import { useSnapshot } from '../api/live';
import type { ConsumerView, GroupView, Snapshot } from '../api/types';
import { Icon } from '../components/icons';
import { useToast } from '../components/toast';
import { Badge, Button, Callout, Card, EmptyState, Field, Meter, PageHeader, Select, StatusBadge, Table, Td, TextInput } from '../components/ui';
import { cx, fmtNum, shortMember, splitTP } from '../lib/format';

// Distinct colours for group members (assignment map).
const memberColors = [
  'bg-info text-white',
  'bg-violet text-white',
  'bg-accent text-white dark:text-surface-0',
  'bg-warn text-white',
  'bg-pink-500 text-white',
  'bg-cyan-600 text-white',
  'bg-lime-600 text-white',
  'bg-amber-600 text-white',
];
const memberDots = ['bg-info', 'bg-violet', 'bg-accent', 'bg-warn', 'bg-pink-500', 'bg-cyan-600', 'bg-lime-600', 'bg-amber-600'];

export function GroupsPage() {
  const s = useSnapshot();
  return (
    <div>
      <PageHeader
        title="Consumer Groups"
        subtitle="Each partition of a subscribed topic is owned by exactly one member of the group. Membership changes trigger a rebalance; committed offsets (stored in the replicated __consumer_offsets topic) say where each partition resumes."
      />
      <div className="grid gap-4 2xl:grid-cols-3">
        <div className="space-y-4 2xl:col-span-2">
          {s.groups.length === 0 ? (
            <Card>
              <EmptyState icon="groups" title="No consumer groups yet">
                Start a demo consumer on the right, or run <code className="font-mono">streamhub consume --group my-group --topic orders</code>.
              </EmptyState>
            </Card>
          ) : (
            s.groups.map((g) => <GroupCard key={g.id} g={g} s={s} />)
          )}
        </div>
        <DemoConsumers s={s} />
      </div>
    </div>
  );
}

function GroupCard({ g, s }: { g: GroupView; s: Snapshot }) {
  const colorOf = new Map(g.members.map((m, i) => [m.id, i % memberColors.length]));
  const demoByMember = new Map(s.demo.consumers.filter((c) => c.memberId).map((c) => [c.memberId, c]));
  // Topics this group touches: from assignments and committed offsets.
  const topics = Array.from(new Set([...g.members.flatMap((m) => m.partitions.map((p) => splitTP(p)[0])), ...g.offsets.map((o) => o.topic)])).sort();
  const owner = new Map<string, string>();
  g.members.forEach((m) => m.partitions.forEach((p) => owner.set(p, m.id)));
  const maxLag = Math.max(1, ...g.offsets.map((o) => o.lag));

  return (
    <Card
      title={
        <span className="flex flex-wrap items-center gap-2">
          <Icon name="groups" className="h-4 w-4 text-ink-3" />
          <span className="font-mono">{g.id}</span>
          <StatusBadge status={g.state} />
        </span>
      }
      subtitle={`${g.members.length} active consumer(s) · strategy ${g.strategy || '–'} · generation ${g.generation} · total lag ${fmtNum(g.totalLag)}`}
    >
      {g.error && (
        <Callout tone={g.stale ? 'warn' : 'bad'} icon="alert">
          {g.stale ? 'Showing last known state while the group coordinator fails over. ' : ''}
          {g.error}
        </Callout>
      )}
      {g.state === 'PreparingRebalance' && (
        <Callout tone="warn" icon="refresh">
          Rebalancing: members are committing and rejoining. Partitions are reassigned when everyone has rejoined (or the rebalance timeout expires).
        </Callout>
      )}

      {/* Members */}
      <div className="mt-1">
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-3">Active consumers</h3>
        {g.members.length === 0 ? (
          <div className="text-sm text-ink-3">No live members. Committed offsets are kept; a new member resumes from them.</div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {g.members.map((m) => {
              const demo = demoByMember.get(m.id);
              return (
                <div key={m.id} className="flex items-center gap-2 rounded-lg border border-line px-2.5 py-1.5 text-xs">
                  <span className={cx('h-2.5 w-2.5 rounded-full', memberDots[colorOf.get(m.id) ?? 0])} />
                  <span className="font-mono text-ink" title={m.id}>
                    {shortMember(m.id)}
                  </span>
                  <span className="text-ink-3">{m.partitions.length} partition(s)</span>
                  {demo && <Badge tone="violet">{demo.id}</Badge>}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Assignment map */}
      <div className="mt-4">
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-3">Partition → consumer assignment</h3>
        <div className="space-y-2">
          {topics.map((topic) => {
            const tv = s.topics.find((t) => t.name === topic);
            const n = tv?.partitions.length ?? 0;
            return (
              <div key={topic} className="flex flex-wrap items-center gap-1.5">
                <span className="w-32 truncate font-mono text-xs text-ink-2">{topic}</span>
                {Array.from({ length: n }).map((_, p) => {
                  const own = owner.get(`${topic}-${p}`);
                  const idx = own ? colorOf.get(own) ?? 0 : -1;
                  return (
                    <span
                      key={p}
                      title={own ? `${topic}-${p} → ${own}` : `${topic}-${p}: unassigned`}
                      className={cx(
                        'grid h-8 w-10 place-items-center rounded-md font-mono text-xs font-semibold transition-colors duration-500',
                        idx >= 0 ? memberColors[idx] : 'border border-dashed border-line text-ink-3',
                      )}
                    >
                      P{p}
                    </span>
                  );
                })}
              </div>
            );
          })}
        </div>
      </div>

      {/* Offsets */}
      <div className="mt-5">
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-3">Offsets and lag</h3>
        {g.offsets.length === 0 ? (
          <div className="text-sm text-ink-3">Nothing committed yet.</div>
        ) : (
          <Table head={['Partition', 'Consumer', 'Committed offset', 'Latest offset (HW)', 'Lag', '']}>
            {g.offsets.map((o) => (
              <tr key={`${o.topic}-${o.partition}`}>
                <Td mono>
                  {o.topic}-{o.partition}
                </Td>
                <Td>
                  {o.owner ? (
                    <span className="inline-flex items-center gap-1.5 font-mono text-xs">
                      <span className={cx('h-2 w-2 rounded-full', memberDots[colorOf.get(o.owner) ?? 0])} />
                      {shortMember(o.owner)}
                    </span>
                  ) : (
                    <span className="text-ink-3">unassigned</span>
                  )}
                </Td>
                <Td className="tabular-nums">{o.committed >= 0 ? fmtNum(o.committed) : <span className="text-ink-3">none</span>}</Td>
                <Td className="tabular-nums">{o.end >= 0 ? fmtNum(o.end) : '–'}</Td>
                <Td className={cx('tabular-nums font-medium', o.lag > 100 ? 'text-bad' : o.lag > 0 ? 'text-warn' : 'text-ok')}>{fmtNum(o.lag)}</Td>
                <Td className="w-40">
                  <Meter value={o.lag} max={maxLag} tone={o.lag > 100 ? 'bad' : o.lag > 0 ? 'warn' : 'ok'} />
                </Td>
              </tr>
            ))}
          </Table>
        )}
      </div>
    </Card>
  );
}

export function DemoConsumers({ s, compact }: { s: Snapshot; compact?: boolean }) {
  const toast = useToast();
  const topics = s.topics.filter((t) => !t.internal && !t.dlq);
  const [group, setGroup] = useState(s.groups[0]?.id ?? 'order-processor');
  const [topic, setTopic] = useState(topics[0]?.name ?? '');
  const [delay, setDelay] = useState('0');
  const [strategy, setStrategy] = useState('range');
  const [busy, setBusy] = useState('');

  const add = async () => {
    setBusy('add');
    try {
      const c = await api.addConsumer(group, topic, Number(delay) || 0, strategy);
      toast('success', `${c.id} joining ${group}`, 'The group rebalances to include it.');
    } catch (e) {
      toast('error', 'Could not start consumer', errorMessage(e));
    } finally {
      setBusy('');
    }
  };
  const act = async (c: ConsumerView, action: 'crash' | 'stop' | 'restart' | 'remove') => {
    setBusy(c.id + action);
    try {
      if (action === 'remove') await api.removeConsumer(c.id);
      else await api.consumerAction(c.id, action);
      const msg: Record<string, string> = {
        crash: 'Crashed without leaving: the coordinator notices after the 6 s session timeout, then rebalances.',
        stop: 'Left the group gracefully: rebalance starts immediately.',
        restart: 'Rejoining the group.',
        remove: 'Removed.',
      };
      toast(action === 'crash' ? 'warn' : 'success', `${c.id}: ${action}`, msg[action]);
    } catch (e) {
      toast('error', `${c.id}: ${action} failed`, errorMessage(e));
    } finally {
      setBusy('');
    }
  };

  return (
    <Card
      title="Demo consumers"
      subtitle="Real group members run by the dashboard server. Records with &quot;simulateFailure&quot;: true fail processing 3 times and go to <topic>.dlq."
    >
      {!compact && (
        <div className="mb-4 space-y-3 rounded-lg border border-line bg-surface-0 p-3">
          <div className="grid grid-cols-2 gap-3">
            <Field label="Group">
              <TextInput value={group} onChange={setGroup} mono />
            </Field>
            <Field label="Topic">
              <Select value={topic} onChange={setTopic} options={topics.map((t) => ({ value: t.name, label: t.name }))} />
            </Field>
            <Field label="Processing delay (ms/record)" hint="Slow consumers build lag">
              <TextInput type="number" min={0} max={10000} value={delay} onChange={setDelay} />
            </Field>
            <Field label="Assignment strategy">
              <Select
                value={strategy}
                onChange={setStrategy}
                options={[
                  { value: 'range', label: 'range' },
                  { value: 'roundrobin', label: 'round-robin' },
                ]}
              />
            </Field>
          </div>
          <Button variant="primary" icon="plus" onClick={add} loading={busy === 'add'} disabled={!group || !topic}>
            Add consumer
          </Button>
        </div>
      )}
      {s.demo.consumers.length === 0 ? (
        <div className="py-4 text-center text-sm text-ink-3">No demo consumers running.</div>
      ) : (
        <ul className="divide-y divide-line">
          {s.demo.consumers.map((c) => {
            const alive = c.status === 'running' || c.status === 'starting';
            return (
              <li key={c.id} className="py-2.5">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-mono text-sm font-medium text-ink">{c.id}</span>
                  <StatusBadge status={c.status} />
                  <span className="text-xs text-ink-3">
                    {c.group} · {c.topic}
                    {c.delayMs > 0 && ` · ${c.delayMs} ms/record`}
                  </span>
                </div>
                <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-ink-2">
                  <span>processed {fmtNum(c.processed)}</span>
                  <span className={c.failed > 0 ? 'text-bad' : ''}>failed {fmtNum(c.failed)}</span>
                  {alive && <span className="font-mono text-ink-3">gen {c.generation} · [{c.partitions.join(', ') || 'no partitions'}]</span>}
                </div>
                {c.lastError && alive && <div className="mt-1 truncate text-xs text-warn" title={c.lastError}>{c.lastError}</div>}
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {alive ? (
                    <>
                      <Button size="sm" variant="danger" icon="skull" loading={busy === c.id + 'crash'} onClick={() => act(c, 'crash')}>
                        Crash
                      </Button>
                      <Button size="sm" icon="stop" loading={busy === c.id + 'stop'} onClick={() => act(c, 'stop')}>
                        Stop gracefully
                      </Button>
                    </>
                  ) : (
                    <Button size="sm" variant="primary" icon="play" loading={busy === c.id + 'restart'} onClick={() => act(c, 'restart')}>
                      Restart
                    </Button>
                  )}
                  <Button size="sm" variant="ghost" icon="trash" loading={busy === c.id + 'remove'} onClick={() => act(c, 'remove')}>
                    Remove
                  </Button>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </Card>
  );
}
