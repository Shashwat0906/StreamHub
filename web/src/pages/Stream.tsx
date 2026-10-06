import { useEffect, useMemo, useRef, useState } from 'react';
import { useSnapshot } from '../api/live';
import type { StreamMessage } from '../api/types';
import { Icon } from '../components/icons';
import { Badge, Button, Card, Dot, EmptyState, Field, Modal, PageHeader, Select } from '../components/ui';
import { cx, fmtTime, prettyJSON } from '../lib/format';
import { useRoute } from '../lib/router';

const MAX_ROWS = 500;

type Conn = 'connecting' | 'live' | 'error' | 'paused';

export function StreamPage() {
  const s = useSnapshot();
  const route = useRoute();
  const userTopics = s.topics.filter((t) => !t.internal);
  const [topic, setTopic] = useState(route.query.get('topic') ?? '');
  const [partition, setPartition] = useState(route.query.get('partition') ?? '-1');
  const [paused, setPaused] = useState(false);
  const [rows, setRows] = useState<StreamMessage[]>([]);
  const [conn, setConn] = useState<Conn>('connecting');
  const [rate, setRate] = useState(0);
  const [open, setOpen] = useState<StreamMessage | null>(null);
  const counter = useRef(0);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;

  // Groups that track this topic (for the consumer-status column).
  const groupsForTopic = useMemo(
    () => s.groups.filter((g) => g.offsets.some((o) => !topic || o.topic === topic) || g.members.some((m) => m.partitions.some((p) => !topic || p.startsWith(topic + '-')))),
    [s.groups, topic],
  );
  const [group, setGroup] = useState('');
  const activeGroup = group || groupsForTopic[0]?.id || '';

  // One SSE connection per filter; the server tails the partitions.
  useEffect(() => {
    setRows([]);
    setConn('connecting');
    const qs = new URLSearchParams({ backfill: '20' });
    if (topic) qs.set('topic', topic);
    if (partition !== '-1') qs.set('partition', partition);
    const es = new EventSource('/api/stream?' + qs.toString());
    es.addEventListener('hello', () => setConn(pausedRef.current ? 'paused' : 'live'));
    es.addEventListener('error', () => setConn('error'));
    es.addEventListener('message', (m: MessageEvent) => {
      counter.current++;
      if (pausedRef.current) return;
      const msg = JSON.parse(m.data) as StreamMessage;
      setRows((r) => [msg, ...r].slice(0, MAX_ROWS));
    });
    return () => es.close();
  }, [topic, partition]);

  useEffect(() => setConn((c) => (paused ? 'paused' : c === 'paused' ? 'live' : c)), [paused]);

  useEffect(() => {
    const t = setInterval(() => {
      setRate(counter.current);
      counter.current = 0;
    }, 1000);
    return () => clearInterval(t);
  }, []);

  const committed = useMemo(() => {
    const m = new Map<string, number>();
    s.groups.find((g) => g.id === activeGroup)?.offsets.forEach((o) => m.set(`${o.topic}-${o.partition}`, o.committed));
    return m;
  }, [s.groups, activeGroup]);
  const dlq = useMemo(() => new Set(s.dlqIndex), [s.dlqIndex]);

  const status = (m: StreamMessage): { label: string; tone: 'ok' | 'warn' | 'bad' | 'default' } => {
    if (dlq.has(m.id)) return { label: 'dead-lettered', tone: 'bad' };
    if (!activeGroup) return { label: 'no group', tone: 'default' };
    const c = committed.get(`${m.topic}-${m.partition}`);
    if (c === undefined || c < 0) return { label: 'pending', tone: 'warn' };
    return c > m.offset ? { label: 'processed', tone: 'ok' } : { label: 'pending', tone: 'warn' };
  };

  const t = s.topics.find((x) => x.name === topic);
  const connBadge: Record<Conn, [string, 'ok' | 'warn' | 'bad' | 'info']> = {
    connecting: ['Connecting', 'info'],
    live: ['Streaming', 'ok'],
    error: ['Reconnecting', 'bad'],
    paused: ['Paused', 'warn'],
  };

  return (
    <div>
      <PageHeader
        title="Live Message Stream"
        subtitle="Records are tailed from partition leaders and pushed over Server-Sent Events. Only committed records (below the high-watermark) are visible to consumers, so everything shown here is replicated to the ISR."
        actions={
          <>
            <Button icon={paused ? 'play' : 'pause'} onClick={() => setPaused(!paused)}>
              {paused ? 'Resume' : 'Pause'}
            </Button>
            <Button icon="trash" variant="ghost" onClick={() => setRows([])}>
              Clear
            </Button>
          </>
        }
      />
      <Card bodyClassName="p-0">
        <div className="flex flex-wrap items-end gap-3 border-b border-line p-4">
          <div className="w-56">
            <Field label="Topic">
              <Select
                value={topic}
                onChange={(v) => {
                  setTopic(v);
                  setPartition('-1');
                  setGroup('');
                }}
                options={[{ value: '', label: 'All user topics' }, ...userTopics.map((x) => ({ value: x.name, label: x.name }))]}
              />
            </Field>
          </div>
          <div className="w-44">
            <Field label="Partition">
              <Select
                value={partition}
                onChange={setPartition}
                disabled={!topic}
                options={[{ value: '-1', label: 'All partitions' }, ...(t?.partitions ?? []).map((p) => ({ value: String(p.id), label: `Partition ${p.id}` }))]}
              />
            </Field>
          </div>
          <div className="w-56">
            <Field label="Consumer status for group">
              <Select
                value={activeGroup}
                onChange={setGroup}
                options={groupsForTopic.length ? groupsForTopic.map((g) => ({ value: g.id, label: g.id })) : [{ value: '', label: 'no group consumes this' }]}
              />
            </Field>
          </div>
          <div className="ml-auto flex items-center gap-3 pb-1.5 text-xs text-ink-2">
            <Badge tone={connBadge[conn][1]}>
              <Dot tone={connBadge[conn][1]} pulse={conn === 'live'} />
              {connBadge[conn][0]}
            </Badge>
            <span className="tabular-nums">{rate} msg/s</span>
            <span className="tabular-nums text-ink-3">{rows.length} shown</span>
          </div>
        </div>
        {rows.length === 0 ? (
          <EmptyState icon="stream" title={conn === 'error' ? 'Stream disconnected' : 'Waiting for messages…'}>
            {conn === 'error'
              ? 'The browser will reconnect automatically.'
              : 'New records appear here as soon as they are committed. Produce one from the Produce page, or start the traffic generator on the Failure Simulation page.'}
          </EmptyState>
        ) : (
          <div className="max-h-[68vh] overflow-auto">
            <table className="w-full min-w-[960px] text-left text-sm">
              <thead className="sticky top-0 z-10 bg-surface-1">
                <tr className="border-b border-line text-[11px] uppercase tracking-wide text-ink-3">
                  <th className="px-4 py-2 font-medium">Timestamp</th>
                  <th className="px-2 py-2 font-medium">Topic</th>
                  <th className="px-2 py-2 font-medium">Partition</th>
                  <th className="px-2 py-2 font-medium">Offset</th>
                  <th className="px-2 py-2 font-medium">Key</th>
                  <th className="px-2 py-2 font-medium">Payload</th>
                  <th className="px-2 py-2 font-medium">Producer ack</th>
                  <th className="px-4 py-2 font-medium">Consumer status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-line font-mono text-[12px]">
                {rows.map((m, i) => {
                  const st = status(m);
                  return (
                    <tr key={m.id} className={cx('cursor-pointer hover:bg-surface-2/60', i < 3 && 'flash-in')} onClick={() => setOpen(m)}>
                      <td className="whitespace-nowrap px-4 py-1.5 text-ink-3">{fmtTime(m.timestamp)}</td>
                      <td className="whitespace-nowrap px-2 py-1.5 text-ink">{m.topic}</td>
                      <td className="px-2 py-1.5 text-ink-2">{m.partition}</td>
                      <td className="px-2 py-1.5 text-ink-2">{m.offset}</td>
                      <td className="max-w-[120px] truncate px-2 py-1.5 text-info">{m.key || <span className="text-ink-3">null</span>}</td>
                      <td className="max-w-[360px] truncate px-2 py-1.5 text-ink-2" title={m.value}>
                        {m.value}
                      </td>
                      <td className="max-w-[220px] truncate px-2 py-1.5 font-sans text-xs text-ink-3" title={m.ack}>
                        <Icon name="check" className="mr-1 inline h-3 w-3 text-ok" />
                        {m.ack}
                      </td>
                      <td className="px-4 py-1.5 font-sans">
                        <Badge tone={st.tone}>{st.label}</Badge>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>
      <Modal open={open !== null} onClose={() => setOpen(null)} title={open ? `Message ${open.id}` : ''} wide>
        {open && (
          <div className="space-y-3 text-sm">
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
              {[
                ['Topic', open.topic],
                ['Partition', open.partition],
                ['Offset', open.offset],
                ['Size', `${open.size} B`],
              ].map(([k, v]) => (
                <div key={String(k)} className="rounded-lg bg-surface-2 px-3 py-2">
                  <div className="text-[10px] uppercase text-ink-3">{k}</div>
                  <div className="font-mono text-ink">{v}</div>
                </div>
              ))}
            </div>
            <div className="text-xs text-ink-3">
              Key <span className="font-mono text-info">{open.key || 'null'}</span> · {new Date(open.timestamp).toISOString()} · {open.ack}
            </div>
            <pre className="max-h-[50vh] overflow-auto rounded-lg bg-surface-0 p-3 font-mono text-[12px] text-ink">{prettyJSON(open.value)}</pre>
            {open.truncated && <div className="text-xs text-warn">Payload truncated to 4 KiB for display.</div>}
          </div>
        )}
      </Modal>
    </div>
  );
}
