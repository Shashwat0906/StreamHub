import { useEffect, useState } from 'react';
import { api, errorMessage } from '../api/client';
import { useSnapshot } from '../api/live';
import type { DLQEntry } from '../api/types';
import { useToast } from '../components/toast';
import { Badge, Button, Callout, Card, EmptyState, ErrorState, Modal, PageHeader, Select, Skeleton, Table, Td, TextArea } from '../components/ui';
import { fmtAgo, fmtTime, prettyJSON } from '../lib/format';
import { useNow } from '../lib/hooks';

export function DLQPage() {
  const s = useSnapshot();
  const toast = useToast();
  const now = useNow(5000);
  const [entries, setEntries] = useState<DLQEntry[] | null>(null);
  const [maxRetries, setMaxRetries] = useState(3);
  const [err, setErr] = useState('');
  const [loading, setLoading] = useState(false);
  const [filter, setFilter] = useState<'all' | 'eligible' | 'retried'>('all');
  const [topicFilter, setTopicFilter] = useState('');
  const [editing, setEditing] = useState<DLQEntry | null>(null);
  const [busy, setBusy] = useState('');

  const load = async () => {
    setLoading(true);
    try {
      const r = await api.dlq();
      setEntries(r.entries);
      setMaxRetries(r.maxRetries);
      setErr('');
    } catch (e) {
      setErr(errorMessage(e));
    } finally {
      setLoading(false);
    }
  };
  // Reload when the live DLQ count changes (pushed with every snapshot),
  // instead of polling the DLQ endpoint.
  useEffect(() => {
    load();
  }, [s.totals.dlqMessages]);

  const retry = async (e: DLQEntry, payload?: string) => {
    setBusy(e.id);
    try {
      const r = await api.retryDLQ(e.id, payload);
      toast('success', `Re-published as ${r.messageId}`, payload !== undefined ? 'With the edited payload.' : 'If it fails again it returns to the DLQ with a higher retry count.');
      setEditing(null);
      await load();
    } catch (x) {
      toast('error', 'Retry failed', errorMessage(x));
    } finally {
      setBusy('');
    }
  };

  const dlqTopics = Array.from(new Set((entries ?? []).map((e) => e.originalTopic).filter(Boolean)));
  const shown = (entries ?? []).filter(
    (e) => (filter === 'all' || (filter === 'eligible' ? e.eligible : e.retried)) && (!topicFilter || e.originalTopic === topicFilter),
  );

  return (
    <div>
      <PageHeader
        title="Dead Letter Queue"
        subtitle={
          <>
            Messages a consumer could not process after {3} attempts are written (acks=all) to <code className="font-mono">&lt;topic&gt;.dlq</code>, with the error, origin and retry count. Retry
            re-publishes the original payload to the original topic.
          </>
        }
        actions={
          <Button icon="refresh" onClick={load} loading={loading}>
            Refresh
          </Button>
        }
      />
      <Callout>
        StreamHub itself has no built-in DLQ (Kafka doesn't either): dead-lettering is a consumer pattern. Here it is done by the dashboard's demo consumers, so the entries below are real records read from the DLQ topics. Retry eligibility (max {maxRetries} redrives per message, each entry retried once) is stored in the internal <code className="font-mono">__dlq_retries</code> topic, so it survives dashboard restarts.
      </Callout>

      <div className="mt-4 grid grid-cols-2 gap-4 md:grid-cols-4">
        {[
          ['DLQ records', s.totals.dlqMessages],
          ['Shown', shown.length],
          ['Eligible for retry', (entries ?? []).filter((e) => e.eligible).length],
          ['Already retried', (entries ?? []).filter((e) => e.retried).length],
        ].map(([k, v]) => (
          <div key={String(k)} className="rounded-xl border border-line bg-surface-1 p-4">
            <div className="text-xs uppercase tracking-wide text-ink-3">{k}</div>
            <div className="mt-1 text-2xl font-semibold tabular-nums text-ink">{v}</div>
          </div>
        ))}
      </div>

      <Card className="mt-4" bodyClassName="p-0">
        <div className="flex flex-wrap items-center gap-3 border-b border-line p-4">
          <div className="w-44">
            <Select
              value={filter}
              onChange={setFilter}
              options={[
                { value: 'all', label: 'All entries' },
                { value: 'eligible', label: 'Eligible for retry' },
                { value: 'retried', label: 'Already retried' },
              ]}
            />
          </div>
          <div className="w-48">
            <Select value={topicFilter} onChange={setTopicFilter} options={[{ value: '', label: 'All original topics' }, ...dlqTopics.map((t) => ({ value: t, label: t }))]} />
          </div>
        </div>
        <div className="p-4">
          {err ? (
            <ErrorState title="Could not read the DLQ topics" message={err} action={<Button onClick={load}>Try again</Button>} />
          ) : entries === null ? (
            <div className="space-y-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-10" />
              ))}
            </div>
          ) : shown.length === 0 ? (
            <EmptyState icon="dlq" title={entries.length ? 'No entry matches the filter' : 'The dead letter queue is empty'}>
              {!entries.length &&
                'Produce a message with "simulateFailure": true (template on the Produce page) to a topic that a demo consumer reads, or raise the failure rate of the traffic generator.'}
            </EmptyState>
          ) : (
            <Table head={['Failed', 'Original', 'Key', 'Error reason', 'Retries', 'Payload', '']}>
              {shown.map((e) => (
                <tr key={e.id} className="align-top">
                  <Td className="text-xs text-ink-2" title={new Date(e.failedAt).toISOString()}>
                    <div className="font-mono">{fmtTime(e.failedAt).slice(0, 8)}</div>
                    <div className="text-ink-3">{fmtAgo(e.failedAt, now)}</div>
                  </Td>
                  <Td mono className="text-xs">
                    <div className="text-ink">{e.originalTopic || '?'}</div>
                    <div className="text-ink-3">
                      partition {e.partition} · offset {e.offset}
                    </div>
                    <div className="text-ink-3">group {e.consumerGroup || '–'}</div>
                  </Td>
                  <Td mono className="text-xs text-info">
                    {e.key || 'null'}
                  </Td>
                  <Td className="text-xs text-bad">
                    <div className="w-48 whitespace-normal break-words">{e.error}</div>
                  </Td>
                  <Td>
                    <Badge tone={e.retryCount >= maxRetries ? 'bad' : e.retryCount > 0 ? 'warn' : 'default'}>
                      {e.retryCount}/{maxRetries}
                    </Badge>
                  </Td>
                  <Td className="font-mono text-xs text-ink-2" title={e.payload}>
                    <div className="w-40 truncate">{e.payload}</div>
                  </Td>
                  <Td>
                    {e.eligible ? (
                      <div className="flex gap-1.5">
                        <Button size="sm" variant="primary" icon="refresh" loading={busy === e.id} onClick={() => retry(e)}>
                          Retry
                        </Button>
                        <Button size="sm" icon="edit" onClick={() => setEditing(e)} title="Edit the payload, then retry">
                          Edit
                        </Button>
                      </div>
                    ) : (
                      <Badge title={e.ineligibleCause}>{e.retried ? 'retried' : e.ineligibleCause}</Badge>
                    )}
                  </Td>
                </tr>
              ))}
            </Table>
          )}
        </div>
      </Card>
      <EditRetryModal entry={editing} onClose={() => setEditing(null)} onRetry={retry} busy={busy !== ''} />
    </div>
  );
}

function EditRetryModal({ entry, onClose, onRetry, busy }: { entry: DLQEntry | null; onClose: () => void; onRetry: (e: DLQEntry, payload: string) => void; busy: boolean }) {
  const [payload, setPayload] = useState('');
  useEffect(() => {
    if (entry) setPayload(prettyJSON(entry.payload));
  }, [entry]);
  return (
    <Modal
      open={entry !== null}
      onClose={onClose}
      title={entry ? `Edit & retry ${entry.originalTopic}/${entry.partition}/${entry.offset}` : ''}
      wide
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" icon="send" loading={busy} onClick={() => entry && onRetry(entry, payload)}>
            Re-publish to {entry?.originalTopic}
          </Button>
        </>
      }
    >
      {entry && (
        <div className="space-y-3">
          <Callout tone="bad" icon="alert">
            {entry.error}
          </Callout>
          <p className="text-xs text-ink-3">
            Fix the payload (for example remove <code className="font-mono">"simulateFailure": true</code>) and re-publish it with the original key{' '}
            <code className="font-mono">{entry.key || 'null'}</code>.
          </p>
          <TextArea value={payload} onChange={setPayload} rows={12} />
        </div>
      )}
    </Modal>
  );
}
