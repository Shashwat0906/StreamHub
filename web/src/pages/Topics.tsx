import { useState } from 'react';
import { api, errorMessage } from '../api/client';
import { useSnapshot } from '../api/live';
import type { Snapshot, TopicView } from '../api/types';
import { brokerMap, ReplicaChip, ReplicaLegend } from '../components/cluster';
import { Icon } from '../components/icons';
import { useToast } from '../components/toast';
import { Badge, Button, Callout, Card, EmptyState, Field, Modal, PageHeader, Segmented, Table, Td, TextInput, Toggle } from '../components/ui';
import { cx, fmtBytes, fmtCompact, fmtNum } from '../lib/format';
import { useRoute } from '../lib/router';

export function TopicsPage() {
  const s = useSnapshot();
  const route = useRoute();
  const focus = route.query.get('topic') ?? '';
  const [showInternal, setShowInternal] = useState(false);
  const [view, setView] = useState<'tree' | 'table'>('tree');
  const [creating, setCreating] = useState(false);
  const [filter, setFilter] = useState('');
  const topics = s.topics.filter((t) => (showInternal || !t.internal) && t.name.toLowerCase().includes(filter.toLowerCase()));

  return (
    <div>
      <PageHeader
        title="Topics & Partitions"
        subtitle="Each partition is a replicated log. The leader serves reads and writes; followers in the ISR (in-sync replicas) hold every committed record. Latest offset and high-watermark come from the leader."
        actions={
          <Button variant="primary" icon="plus" onClick={() => setCreating(true)}>
            Create topic
          </Button>
        }
      />
      <div className="mb-4 flex flex-wrap items-center gap-3">
        <div className="w-64">
          <TextInput value={filter} onChange={setFilter} placeholder="Filter topics…" />
        </div>
        <Segmented
          value={view}
          onChange={setView}
          options={[
            { value: 'tree', label: 'Tree' },
            { value: 'table', label: 'Table' },
          ]}
        />
        <Toggle checked={showInternal} onChange={setShowInternal} label="Show internal topics (__consumer_offsets)" />
        <div className="ml-auto">
          <ReplicaLegend />
        </div>
      </div>

      {topics.length === 0 ? (
        <Card>
          <EmptyState
            icon="topics"
            title={filter ? 'No topic matches the filter' : 'No topics yet'}
            action={!filter && <Button variant="primary" icon="plus" onClick={() => setCreating(true)}>Create topic</Button>}
          >
            {!filter && 'Create a topic to start producing messages. Partitions are spread across brokers automatically.'}
          </EmptyState>
        </Card>
      ) : (
        <div className="space-y-4">
          {topics.map((t) => (
            <TopicCard key={t.name} t={t} s={s} view={view} focused={focus === t.name} />
          ))}
        </div>
      )}
      <CreateTopicModal open={creating} onClose={() => setCreating(false)} maxRF={Math.max(1, s.totals.healthyBrokers)} />
    </div>
  );
}

function TopicCard({ t, s, view, focused }: { t: TopicView; s: Snapshot; view: 'tree' | 'table'; focused: boolean }) {
  const bmap = brokerMap(s);
  const toast = useToast();
  const [confirm, setConfirm] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const lag = t.partitions.reduce((a, p) => a + p.lag, 0);
  const size = t.partitions.reduce((a, p) => a + p.sizeBytes, 0);
  const rf = t.partitions[0]?.replicas.length ?? 0;
  const del = async () => {
    setDeleting(true);
    try {
      await api.deleteTopic(t.name);
      toast('success', `Topic ${t.name} deleted`);
      setConfirm(false);
    } catch (e) {
      toast('error', 'Delete failed', errorMessage(e));
    } finally {
      setDeleting(false);
    }
  };
  return (
    <Card
      className={cx(focused && 'ring-2 ring-accent/50')}
      title={
        <span className="flex flex-wrap items-center gap-2">
          <Icon name={t.dlq ? 'dlq' : 'topics'} className="h-4 w-4 text-ink-3" />
          <span className="font-mono">{t.name}</span>
          {t.internal && <Badge>internal</Badge>}
          {t.dlq && <Badge tone="bad">dead letter queue</Badge>}
        </span>
      }
      subtitle={`${t.partitions.length} partitions · replication factor ${rf} · ${fmtCompact(t.messages)} records · ${fmtBytes(size)} per replica set · lag ${fmtCompact(lag)}`}
      actions={
        !t.internal && (
          <Button size="sm" variant="ghost" icon="trash" onClick={() => setConfirm(true)} title="Delete topic">
            Delete
          </Button>
        )
      }
    >
      {view === 'tree' ? (
        <div className="font-mono text-[13px]">
          <div className="mb-1 font-semibold text-ink">{t.name}</div>
          {t.partitions.map((p, i) => {
            const last = i === t.partitions.length - 1;
            return (
              <div key={p.id} className="flex flex-wrap items-center gap-x-3 gap-y-1 py-1">
                <span className="text-ink-3">{last ? '└──' : '├──'}</span>
                <span className="w-24 text-ink">Partition {p.id}</span>
                <span className="text-ink-3">→</span>
                {p.leader >= 0 ? (
                  <span className={cx('w-36', p.offline ? 'text-bad' : 'text-ink')}>
                    Broker {p.leader} <span className={p.offline ? 'text-bad' : 'text-accent'}>[Leader]</span>
                  </span>
                ) : (
                  <span className="w-36 text-bad">no leader [OFFLINE]</span>
                )}
                <span className="flex gap-1">
                  {p.replicas.map((r) => (
                    <ReplicaChip key={r} id={r} partition={p} brokers={bmap} />
                  ))}
                </span>
                <span className="font-sans text-xs text-ink-3">
                  latest {p.logEnd >= 0 ? fmtNum(p.logEnd - 1) : '–'} · HW {p.highWatermark >= 0 ? fmtNum(p.highWatermark) : '–'} · lag {fmtNum(p.lag)}
                </span>
                {p.underReplicated && !p.offline && <Badge tone="warn">under-replicated</Badge>}
              </div>
            );
          })}
        </div>
      ) : (
        <Table head={['Partition', 'Leader', 'Replicas', 'ISR', 'Latest offset', 'High-watermark', 'Log start', 'Consumer lag', 'Size', 'Epoch']}>
          {t.partitions.map((p) => (
            <tr key={p.id}>
              <Td mono>{p.id}</Td>
              <Td>{p.leader >= 0 ? <span className={p.offline ? 'text-bad' : ''}>Broker {p.leader}</span> : <Badge tone="bad">offline</Badge>}</Td>
              <Td>
                <span className="flex gap-1">
                  {p.replicas.map((r) => (
                    <ReplicaChip key={r} id={r} partition={p} brokers={bmap} />
                  ))}
                </span>
              </Td>
              <Td mono className={p.underReplicated ? 'text-warn' : 'text-ink-2'}>
                [{p.isr.join(', ')}]
              </Td>
              <Td className="tabular-nums" title="offset of the newest record (log end − 1)">
                {p.logEnd > 0 ? fmtNum(p.logEnd - 1) : p.logEnd === 0 ? 'empty' : '–'}
              </Td>
              <Td className="tabular-nums" title="records below the HW are on every ISR member and visible to consumers">
                {p.highWatermark >= 0 ? fmtNum(p.highWatermark) : '–'}
              </Td>
              <Td className="tabular-nums text-ink-2">{p.logStart >= 0 ? fmtNum(p.logStart) : '–'}</Td>
              <Td className="tabular-nums" title={p.groupLag ? Object.entries(p.groupLag).map(([g, l]) => `${g}: ${l}`).join('\n') : 'no group has committed here'}>
                {fmtNum(p.lag)}
              </Td>
              <Td className="tabular-nums text-ink-2">{fmtBytes(p.sizeBytes)}</Td>
              <Td className="tabular-nums text-ink-2">{p.leaderEpoch}</Td>
            </tr>
          ))}
        </Table>
      )}
      {t.configs && Object.keys(t.configs).length > 0 && (
        <div className="mt-3 flex flex-wrap gap-1.5">
          {Object.entries(t.configs).map(([k, v]) => (
            <Badge key={k}>
              {k}={v}
            </Badge>
          ))}
        </div>
      )}
      <Modal
        open={confirm}
        onClose={() => setConfirm(false)}
        title={`Delete topic ${t.name}?`}
        footer={
          <>
            <Button onClick={() => setConfirm(false)}>Cancel</Button>
            <Button variant="danger" icon="trash" onClick={del} loading={deleting}>
              Delete
            </Button>
          </>
        }
      >
        <p className="text-sm text-ink-2">All partitions and their data are removed from every broker. This cannot be undone.</p>
      </Modal>
    </Card>
  );
}

function CreateTopicModal({ open, onClose, maxRF }: { open: boolean; onClose: () => void; maxRF: number }) {
  const toast = useToast();
  const [name, setName] = useState('');
  const [partitions, setPartitions] = useState('3');
  const [rf, setRF] = useState(String(Math.min(3, maxRF)));
  const [retention, setRetention] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState('');
  const valid = /^[a-zA-Z0-9._-]{1,200}$/.test(name);
  const submit = async () => {
    setBusy(true);
    setErr('');
    try {
      const configs: Record<string, string> = {};
      if (retention) configs['retention.ms'] = String(Number(retention) * 60_000);
      await api.createTopic(name, Number(partitions), Number(rf), configs);
      toast('success', `Topic ${name} created`, `${partitions} partitions, replication factor ${rf}`);
      setName('');
      onClose();
    } catch (e) {
      setErr(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal
      open={open}
      onClose={onClose}
      title="Create topic"
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" icon="plus" onClick={submit} loading={busy} disabled={!valid}>
            Create
          </Button>
        </>
      }
    >
      <div className="space-y-4">
        <Field label="Name" error={name && !valid ? 'Use letters, digits, ".", "_" or "-" (max 200)' : undefined}>
          <TextInput value={name} onChange={setName} placeholder="payments" mono />
        </Field>
        <div className="grid grid-cols-2 gap-4">
          <Field label="Partitions" hint="Unit of parallelism and ordering">
            <TextInput type="number" min={1} max={100} value={partitions} onChange={setPartitions} />
          </Field>
          <Field label="Replication factor" hint={`Copies of each partition (≤ ${maxRF} healthy brokers)`}>
            <TextInput type="number" min={1} max={maxRF} value={rf} onChange={setRF} />
          </Field>
        </div>
        <Field label="Retention (minutes, optional)" hint="Old segments are deleted after this time">
          <TextInput type="number" min={1} value={retention} onChange={setRetention} placeholder="default (7 days)" />
        </Field>
        {Number(rf) < 2 && <Callout tone="warn" icon="alert">With replication factor 1 a single broker failure makes the partition unavailable.</Callout>}
        {err && <Callout tone="bad" icon="alert">{err}</Callout>}
      </div>
    </Modal>
  );
}
