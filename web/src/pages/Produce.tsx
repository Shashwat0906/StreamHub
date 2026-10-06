import { useState } from 'react';
import { api, errorMessage } from '../api/client';
import { useSnapshot } from '../api/live';
import type { AcksMode, ProduceResult } from '../api/types';
import { Icon } from '../components/icons';
import { useToast } from '../components/toast';
import { Badge, Button, Callout, Card, EmptyState, Field, KV, PageHeader, Segmented, Select, Table, Td, TextArea, TextInput } from '../components/ui';
import { fmtTime } from '../lib/format';
import { Link } from '../lib/router';

const templates: { label: string; key: string; value: unknown }[] = [
  { label: 'Payment', key: 'user-12', value: { userId: 12, amount: 500 } },
  { label: 'Order', key: 'user-7', value: { orderId: 1042, userId: 7, amount: 1299, currency: 'INR', items: ['keyboard', 'mouse'] } },
  { label: 'Will fail processing → DLQ', key: 'user-99', value: { userId: 99, amount: 50, simulateFailure: true } },
];

const acksHelp: Record<AcksMode, string> = {
  '0': 'Fire and forget: no response from the broker. Fastest; the record can be lost silently.',
  '1': 'Leader appended it. Lost if the leader dies before followers copy it.',
  all: 'Every in-sync replica has it (high-watermark passed). Survives a broker failure. Idempotent producer: retries never duplicate.',
};

export function ProducePage() {
  const s = useSnapshot();
  const toast = useToast();
  const userTopics = s.topics.filter((t) => !t.internal);
  const [topic, setTopic] = useState(userTopics.find((t) => !t.dlq)?.name ?? userTopics[0]?.name ?? '');
  const [key, setKey] = useState('user-12');
  const [value, setValue] = useState(JSON.stringify({ userId: 12, amount: 500 }, null, 2));
  const [acks, setAcks] = useState<AcksMode>('all');
  const [partition, setPartition] = useState('auto');
  const [busy, setBusy] = useState(false);
  const [last, setLast] = useState<ProduceResult | null>(null);
  const [history, setHistory] = useState<ProduceResult[]>([]);

  let jsonError = '';
  try {
    JSON.parse(value);
  } catch (e) {
    jsonError = errorMessage(e);
  }
  const t = s.topics.find((x) => x.name === topic);

  const send = async () => {
    setBusy(true);
    try {
      const res = await api.produce({
        topic,
        key,
        value,
        acks,
        partition: partition === 'auto' ? undefined : Number(partition),
      });
      setLast(res);
      setHistory((h) => [res, ...h].slice(0, 25));
      toast('success', res.status === 'acknowledged' ? `Acknowledged: ${res.messageId}` : 'Sent (no acknowledgement requested)', `${res.latencyMs.toFixed(1)} ms, acks=${res.acks}`);
    } catch (e) {
      const failed: ProduceResult = { messageId: '', topic, partition: -1, offset: -1, acks, status: 'failed', error: errorMessage(e), latencyMs: 0, timestamp: Date.now() };
      setLast(failed);
      setHistory((h) => [failed, ...h].slice(0, 25));
      toast('error', 'Produce failed', errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  if (userTopics.length === 0) {
    return (
      <div>
        <PageHeader title="Produce Message" />
        <Card>
          <EmptyState icon="topics" title="No topic to produce to" action={<Link to="/topics" className="text-sm text-accent hover:underline">Create a topic →</Link>} />
        </Card>
      </div>
    );
  }

  return (
    <div>
      <PageHeader
        title="Produce Message"
        subtitle="Publishes through the real StreamHub client: partitioner, leader routing, batching and retries are the same as any producer."
      />
      <div className="grid gap-4 xl:grid-cols-5">
        <Card className="xl:col-span-3" title="Message">
          <div className="space-y-4">
            <div className="grid gap-4 sm:grid-cols-2">
              <Field label="Topic">
                <Select
                  value={topic}
                  onChange={(v) => {
                    setTopic(v);
                    setPartition('auto');
                  }}
                  options={userTopics.map((x) => ({ value: x.name, label: `${x.name} (${x.partitions.length} partitions)` }))}
                />
              </Field>
              <Field label="Partition" hint={partition === 'auto' ? (key ? 'Chosen by hash(key) % partitions' : 'Round-robin (no key)') : 'Explicit partition'}>
                <Select
                  value={partition}
                  onChange={setPartition}
                  options={[{ value: 'auto', label: 'Automatic (partitioner)' }, ...(t?.partitions ?? []).map((p) => ({ value: String(p.id), label: `Partition ${p.id} (leader B${p.leader})` }))]}
                />
              </Field>
            </div>
            <Field label="Key" hint="Same key → same partition → ordered. Leave empty for round-robin.">
              <TextInput value={key} onChange={setKey} placeholder="optional" mono />
            </Field>
            <div>
              <div className="mb-1 flex flex-wrap items-center justify-between gap-2">
                <span className="text-xs font-medium text-ink-2">Payload</span>
                <div className="flex flex-wrap gap-1">
                  {templates.map((tp) => (
                    <Button
                      key={tp.label}
                      size="sm"
                      variant="ghost"
                      onClick={() => {
                        setKey(tp.key);
                        setValue(JSON.stringify(tp.value, null, 2));
                      }}
                    >
                      {tp.label}
                    </Button>
                  ))}
                  <Button size="sm" variant="ghost" icon="wand" disabled={!!jsonError} onClick={() => setValue(JSON.stringify(JSON.parse(value), null, 2))}>
                    Format
                  </Button>
                </div>
              </div>
              <TextArea value={value} onChange={setValue} rows={9} invalid={!!jsonError} />
              <div className="mt-1 text-xs">
                {jsonError ? (
                  <span className="text-warn">Not valid JSON ({jsonError}). It will be sent as plain text.</span>
                ) : (
                  <span className="text-ink-3">Valid JSON · {new Blob([value]).size} bytes</span>
                )}
              </div>
            </div>
            <Field label="Acknowledgement (acks)">
              <Segmented
                value={acks}
                onChange={setAcks}
                options={[
                  { value: '0', label: 'acks=0' },
                  { value: '1', label: 'acks=1' },
                  { value: 'all', label: 'acks=all' },
                ]}
              />
              <p className="mt-2 text-xs text-ink-3">{acksHelp[acks]}</p>
            </Field>
            <div className="flex justify-end">
              <Button variant="primary" icon="send" onClick={send} loading={busy} disabled={!topic}>
                Publish
              </Button>
            </div>
          </div>
        </Card>

        <div className="space-y-4 xl:col-span-2">
          <Card title="Result">
            {!last ? (
              <EmptyState icon="send" title="Nothing published yet">
                The partition, offset and acknowledgement of your message appear here.
              </EmptyState>
            ) : last.status === 'failed' ? (
              <Callout tone="bad" icon="alert">
                <div className="font-medium text-bad">Not acknowledged</div>
                <div className="mt-1 break-words">{last.error}</div>
              </Callout>
            ) : (
              <div>
                <div className="mb-3 flex items-center gap-2">
                  <span className="grid h-8 w-8 place-items-center rounded-full bg-ok-soft text-ok">
                    <Icon name="check" />
                  </span>
                  <div>
                    <div className="font-semibold text-ink">{last.status === 'acknowledged' ? 'Acknowledged' : 'Sent, no acknowledgement'}</div>
                    <div className="text-xs text-ink-3">{fmtTime(last.timestamp)}</div>
                  </div>
                </div>
                <KV k="Message ID" v={<span className="font-mono">{last.messageId}</span>} />
                <KV k="Topic" v={<span className="font-mono">{last.topic}</span>} />
                <KV k="Partition" v={last.partition} />
                <KV k="Offset" v={last.offset >= 0 ? last.offset : 'unknown (acks=0 gets no reply)'} />
                <KV k="Acks" v={<Badge tone={last.acks === 'all' ? 'ok' : last.acks === '1' ? 'warn' : 'bad'}>acks={last.acks}</Badge>} />
                <KV k="Latency" v={`${last.latencyMs.toFixed(2)} ms`} />
                {last.status === 'acknowledged' && (
                  <div className="mt-3 text-xs">
                    <Link to={`/stream?topic=${encodeURIComponent(last.topic)}&partition=${last.partition}`} className="text-accent hover:underline">
                      See it in the live stream →
                    </Link>
                  </div>
                )}
              </div>
            )}
          </Card>
          <Card title="This session" subtitle="Messages published from this page">
            {history.length === 0 ? (
              <div className="py-4 text-center text-sm text-ink-3">No messages yet.</div>
            ) : (
              <Table head={['Time', 'Message ID', 'Acks', 'Status']}>
                {history.map((h, i) => (
                  <tr key={i}>
                    <Td mono className="text-ink-3">
                      {fmtTime(h.timestamp).slice(0, 8)}
                    </Td>
                    <Td mono>{h.messageId || '–'}</Td>
                    <Td>acks={h.acks}</Td>
                    <Td>
                      <Badge tone={h.status === 'acknowledged' ? 'ok' : h.status === 'failed' ? 'bad' : 'warn'}>{h.status}</Badge>
                    </Td>
                  </tr>
                ))}
              </Table>
            )}
          </Card>
        </div>
      </div>
    </div>
  );
}
