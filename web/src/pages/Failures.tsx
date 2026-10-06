import { useMemo, useState } from 'react';
import { api, errorMessage } from '../api/client';
import { useLive, useSnapshot } from '../api/live';
import type { BrokerView, ClusterEvent, Phase, Snapshot } from '../api/types';
import { TimeSeriesChart } from '../components/charts';
import { EventTimeline, PartitionMatrix, phaseMeta, ReplicaLegend } from '../components/cluster';
import { Icon } from '../components/icons';
import type { IconName } from '../components/icons';
import { useToast } from '../components/toast';
import { Badge, Button, Callout, Card, Field, PageHeader, Select, StatusBadge, TextInput, Toggle } from '../components/ui';
import { cx, fmtNum, fmtRate } from '../lib/format';
import { useNow } from '../lib/hooks';
import { DemoConsumers } from './Groups';

const incidentStarts = ['broker_killed', 'broker_stopped', 'broker_unavailable', 'consumer_crashed'];

interface Step {
  n: number;
  title: string;
  icon: IconName;
  phases: Phase[];
  explain: string;
}

const steps: Step[] = [
  { n: 1, title: 'Broker unavailable', icon: 'power', phases: ['unavailable', 'detected'], explain: 'Process stops answering; after the session timeout the controller commits FenceBroker to the Raft log.' },
  { n: 2, title: 'Leader failure', icon: 'alert', phases: ['leader_failure'], explain: 'Partitions led by that broker have no working leader.' },
  { n: 3, title: 'New leader elected', icon: 'crown', phases: ['election'], explain: 'The metadata state machine picks the next live replica from the ISR and bumps the leader epoch.' },
  { n: 4, title: 'ISR changes', icon: 'topics', phases: ['isr'], explain: 'The dead replica is removed from the in-sync set so the high-watermark can keep advancing.' },
  { n: 5, title: 'Partition reassignment', icon: 'groups', phases: ['reassignment'], explain: 'Consumer groups rebalance partitions among live members; after recovery the controller moves leadership back to preferred replicas (checked every 30 s).' },
  { n: 6, title: 'Recovery', icon: 'check', phases: ['recovery'], explain: 'The broker restarts, truncates any divergent tail, catches up and rejoins the ISR.' },
];

export function FailuresPage() {
  const s = useSnapshot();
  const { events } = useLive();
  const now = useNow(1000);
  const userTopics = s.topics.filter((t) => !t.internal);
  const [topic, setTopic] = useState(userTopics.find((t) => !t.dlq)?.name ?? userTopics[0]?.name ?? '');
  const [phaseFilter, setPhaseFilter] = useState<string>('');

  // Current incident = everything since the latest failure-starting event.
  const incident = useMemo(() => {
    let start = -1;
    for (let i = events.length - 1; i >= 0; i--) {
      if (incidentStarts.includes(events[i].kind)) {
        start = i;
        break;
      }
    }
    if (start < 0) return null;
    // Include events from just before the start: the diff can notice a
    // broker going down before the kill event's timestamp.
    const t0 = events[start].time;
    return { start: events[start], events: events.filter((e) => e.time >= t0 - 1500) };
  }, [events]);

  const highlight = useMemo(() => {
    const set = new Set<string>();
    for (const e of events) if (now - e.time < 2500 && e.topic !== undefined && e.partition !== undefined) set.add(`${e.topic}-${e.partition}`);
    return set;
  }, [events, now]);

  const t = s.topics.find((x) => x.name === topic);
  const shownEvents = phaseFilter ? events.filter((e) => e.phase === phaseFilter) : events;

  return (
    <div>
      <PageHeader
        title="Cluster / Failure Simulation"
        subtitle="Break things on purpose and watch StreamHub recover. Every indicator below is real cluster state: broker /v1/state, controller metadata and consumer-group descriptions, diffed once per second."
      />
      {!s.capabilities.brokerControl && (
        <div className="mb-4">
          <Callout tone="warn" icon="alert">
            {s.capabilities.note}
          </Callout>
        </div>
      )}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {s.brokers.map((b) => (
          <BrokerControl key={b.id} b={b} s={s} />
        ))}
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-5">
        <Card
          className="xl:col-span-2"
          title="Incident tracker"
          subtitle={
            incident
              ? `Since: ${incident.start.message}`
              : 'Kill or stop a broker (or crash a consumer) to start an incident. Steps light up as the cluster reacts.'
          }
        >
          <IncidentSteps incident={incident} />
        </Card>
        <Card
          className="xl:col-span-3"
          title="Partition leadership"
          subtitle="Live replica roles per broker. Cells flash when a leader or ISR changes."
          actions={
            <div className="w-44">
              <Select value={topic} onChange={setTopic} options={userTopics.map((x) => ({ value: x.name, label: x.name }))} />
            </div>
          }
        >
          {t ? (
            <>
              <PartitionMatrix topic={t} snapshot={s} highlight={highlight} />
              <div className="mt-3">
                <ReplicaLegend />
              </div>
            </>
          ) : (
            <div className="py-6 text-center text-sm text-ink-3">No topic selected.</div>
          )}
          <div className="mt-4 border-t border-line pt-3">
            <div className="mb-1 text-xs font-medium text-ink-3">Throughput and healthy brokers (last 3 min)</div>
            <TimeSeriesChart
              height={150}
              times={s.history.map((h) => h.t)}
              series={[
                { key: 'p', label: 'Produced/s', color: 'var(--info)', values: s.history.map((h) => h.producedPerSec), area: true },
                { key: 'c', label: 'Consumed/s', color: 'var(--ok)', values: s.history.map((h) => h.consumedPerSec) },
              ]}
            />
          </div>
        </Card>
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-3">
        <Card
          className="xl:col-span-2"
          title="Cluster event timeline"
          subtitle="Events are derived by diffing consecutive snapshots (plus the actions you trigger here)."
          actions={
            <div className="w-48">
              <Select
                value={phaseFilter}
                onChange={setPhaseFilter}
                options={[{ value: '', label: 'All events' }, ...(Object.keys(phaseMeta) as Phase[]).map((p) => ({ value: p, label: phaseMeta[p].label }))]}
              />
            </div>
          }
        >
          <div className="max-h-[520px] overflow-y-auto pr-1">
            <EventTimeline events={shownEvents} limit={150} now={now} />
          </div>
        </Card>
        <div className="space-y-4">
          <TrafficControl s={s} />
          <DemoConsumers s={s} compact />
        </div>
      </div>
    </div>
  );
}

function BrokerControl({ b, s }: { b: BrokerView; s: Snapshot }) {
  const toast = useToast();
  const [busy, setBusy] = useState('');
  const running = !b.process || b.process.state === 'running' || b.process.state === 'starting';
  const act = async (action: 'kill' | 'stop' | 'start') => {
    setBusy(action);
    try {
      await api.brokerAction(b.id, action);
      const msg: Record<string, string> = {
        kill: 'SIGKILL sent. Watch the controller fence it and the ISR elect new leaders.',
        stop: 'SIGTERM sent: controlled shutdown moves its leaders away first.',
        start: 'Process started. It will rejoin and catch up.',
      };
      toast(action === 'kill' ? 'warn' : 'success', `Broker ${b.id}: ${action}`, msg[action]);
    } catch (e) {
      toast('error', `Broker ${b.id}: ${action} failed`, errorMessage(e));
    } finally {
      setBusy('');
    }
  };
  const ring = b.status === 'healthy' ? 'ring-ok/40 bg-ok-soft text-ok' : b.status === 'fenced' ? 'ring-warn/40 bg-warn-soft text-warn' : 'ring-bad/40 bg-bad-soft text-bad';
  return (
    <div className={cx('rounded-xl border bg-surface-1 p-4 shadow-sm transition-colors', b.status === 'healthy' ? 'border-line' : 'border-bad/50')}>
      <div className="flex items-center gap-3">
        <span className={cx('grid h-11 w-11 place-items-center rounded-xl ring-1 ring-inset', ring)}>
          <Icon name={b.status === 'offline' ? 'power' : 'server'} className="h-5 w-5" />
        </span>
        <div className="min-w-0">
          <div className="flex items-center gap-2 font-semibold text-ink">
            Broker {b.id}
            {b.isController && <Badge tone="violet">controller</Badge>}
          </div>
          <div className="truncate font-mono text-xs text-ink-3">{b.addr}</div>
        </div>
        <div className="ml-auto">
          <StatusBadge status={b.status} />
        </div>
      </div>
      <div className="mt-3 grid grid-cols-3 gap-2 text-center text-xs">
        <div className="rounded-lg bg-surface-2 py-1.5">
          <div className="text-base font-semibold tabular-nums text-accent">{b.leaderPartitions.length}</div>
          <div className="text-ink-3">leaders</div>
        </div>
        <div className="rounded-lg bg-surface-2 py-1.5">
          <div className="text-base font-semibold tabular-nums text-ink">{b.replicaPartitions.length}</div>
          <div className="text-ink-3">follower</div>
        </div>
        <div className="rounded-lg bg-surface-2 py-1.5">
          <div className="text-base font-semibold tabular-nums text-ink">{fmtRate(b.inMsgPerSec)}</div>
          <div className="text-ink-3">msg/s</div>
        </div>
      </div>
      <div className="mt-2 text-xs text-ink-3">
        {b.process ? `process: ${b.process.state}${b.process.pid ? ` (pid ${b.process.pid})` : ''}` : 'process not managed by the dashboard'}
        {b.raftRole && ` · raft ${b.raftRole}`}
      </div>
      {s.capabilities.brokerControl && (
        <div className="mt-3 flex flex-wrap gap-1.5">
          <Button size="sm" variant="danger" icon="skull" disabled={!running} loading={busy === 'kill'} onClick={() => act('kill')} title="SIGKILL: simulated crash">
            Kill
          </Button>
          <Button size="sm" icon="stop" disabled={!running} loading={busy === 'stop'} onClick={() => act('stop')} title="SIGTERM: controlled shutdown">
            Graceful stop
          </Button>
          <Button size="sm" variant="primary" icon="play" disabled={running} loading={busy === 'start'} onClick={() => act('start')}>
            Restart
          </Button>
        </div>
      )}
    </div>
  );
}

function IncidentSteps({ incident }: { incident: { start: ClusterEvent; events: ClusterEvent[] } | null }) {
  return (
    <ol className="space-y-1">
      {steps.map((st) => {
        const hits = incident ? incident.events.filter((e) => e.phase && st.phases.includes(e.phase)) : [];
        const done = hits.length > 0;
        const first = hits[0];
        const dt = first && incident ? Math.max(0, (first.time - incident.start.time) / 1000) : 0;
        return (
          <li key={st.n} className={cx('flex gap-3 rounded-lg p-2.5 transition-colors', done ? 'bg-surface-2' : 'opacity-60')}>
            <span
              className={cx(
                'grid h-8 w-8 shrink-0 place-items-center rounded-full text-xs font-bold ring-1 ring-inset',
                done ? (st.n <= 2 ? 'bg-bad-soft text-bad ring-bad/40' : st.n === 6 ? 'bg-ok-soft text-ok ring-ok/40' : 'bg-warn-soft text-warn ring-warn/40') : 'bg-surface-2 text-ink-3 ring-line',
              )}
            >
              {done ? <Icon name={st.icon} className="h-4 w-4" /> : st.n}
            </span>
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <span className="text-sm font-medium text-ink">{st.title}</span>
                {done && (
                  <span className="font-mono text-[11px] text-ink-3" title="seconds after the incident started">
                    +{dt.toFixed(1)}s
                  </span>
                )}
                {hits.length > 1 && <Badge>{fmtNum(hits.length)} events</Badge>}
              </div>
              <div className="mt-0.5 text-xs text-ink-2">{done ? first.message : st.explain}</div>
            </div>
          </li>
        );
      })}
    </ol>
  );
}

function TrafficControl({ s }: { s: Snapshot }) {
  const toast = useToast();
  const tr = s.demo.traffic;
  const topics = s.topics.filter((t) => !t.internal && !t.dlq);
  const [topic, setTopic] = useState(tr.topic || topics[0]?.name || '');
  const [rate, setRate] = useState(String(tr.rate || 20));
  const [fail, setFail] = useState(String(Math.round((tr.failureRate ?? 0.05) * 100)));
  const [busy, setBusy] = useState(false);
  const apply = async (running: boolean) => {
    setBusy(true);
    try {
      await api.traffic({ running, topic, rate: Number(rate), failureRate: Number(fail) / 100 });
      toast('success', running ? `Traffic: ${rate} msg/s to ${topic}` : 'Traffic stopped');
    } catch (e) {
      toast('error', 'Traffic change failed', errorMessage(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Card
      title="Traffic generator"
      subtitle="Idempotent acks=all producer run by the dashboard server"
      actions={<Toggle checked={tr.running} onChange={(v) => apply(v)} label={tr.running ? 'on' : 'off'} />}
    >
      <div className="grid grid-cols-2 gap-3">
        <Field label="Topic" className="col-span-2">
          <Select value={topic} onChange={setTopic} options={topics.map((t) => ({ value: t.name, label: t.name }))} />
        </Field>
        <Field label="msg/s">
          <TextInput type="number" min={1} max={5000} value={rate} onChange={setRate} />
        </Field>
        <Field label="fail %">
          <TextInput type="number" min={0} max={100} value={fail} onChange={setFail} />
        </Field>
      </div>
      <div className="mt-3 flex items-center justify-between">
        <span className="text-xs text-ink-3">
          sent {fmtNum(tr.sent)} · errors {fmtNum(tr.errors)}
          {tr.lastError && <span className="text-warn"> · last: {tr.lastError}</span>}
        </span>
        <Button size="sm" variant="primary" icon="play" loading={busy} onClick={() => apply(true)} disabled={!topic}>
          Apply
        </Button>
      </div>
    </Card>
  );
}
