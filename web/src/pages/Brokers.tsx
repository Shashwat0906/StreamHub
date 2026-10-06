import { useState } from 'react';
import { api, errorMessage } from '../api/client';
import { useSnapshot } from '../api/live';
import type { BrokerView, Snapshot } from '../api/types';
import { Sparkline } from '../components/charts';
import { brokerMap, brokerTone, ReplicaChip, ReplicaLegend } from '../components/cluster';
import { Icon } from '../components/icons';
import { useToast } from '../components/toast';
import { Badge, Button, Callout, Card, Dot, Drawer, EmptyState, KV, PageHeader, StatusBadge, Table, Td } from '../components/ui';
import { fmtBytes, fmtDuration, fmtNum, fmtRate, splitTP } from '../lib/format';
import { navigate } from '../lib/router';

export function BrokersPage({ selected }: { selected?: number }) {
  const s = useSnapshot();
  const sel = selected !== undefined ? s.brokers.find((b) => b.id === selected) : undefined;
  const maxStorage = Math.max(1, ...s.brokers.map((b) => b.storageBytes));

  return (
    <div>
      <PageHeader
        title="Broker Cluster"
        subtitle="Every broker in the metadata quorum. Status combines the broker's own /v1/state endpoint (reachable?) with the controller's committed metadata (fenced?)."
      />
      {s.brokers.length === 0 ? (
        <Card>
          <EmptyState icon="server" title="No brokers registered">
            The dashboard could not find any broker in cluster metadata yet.
          </EmptyState>
        </Card>
      ) : (
        <>
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {s.brokers.map((b) => (
              <BrokerCard key={b.id} b={b} s={s} maxStorage={maxStorage} />
            ))}
          </div>
          <Card className="mt-4" title="All brokers" subtitle="Click a row for partition-level details">
            <Table head={['Broker', 'Host:port', 'Status', 'Role', 'Leader partitions', 'Replica partitions', 'In msg/s', 'Out msg/s', 'Storage', 'Uptime']}>
              {s.brokers.map((b) => (
                <tr key={b.id} className="cursor-pointer hover:bg-surface-2/60" onClick={() => navigate(`/brokers/${b.id}`)}>
                  <Td className="font-medium text-ink">
                    <span className="inline-flex items-center gap-2">
                      <Dot tone={brokerTone(b.status)} pulse={b.status !== 'healthy'} />
                      {b.id}
                    </span>
                  </Td>
                  <Td mono className="text-ink-2">
                    {b.addr}
                  </Td>
                  <Td>
                    <StatusBadge status={b.status} />
                  </Td>
                  <Td>{b.isController ? <Badge tone="violet">controller</Badge> : <span className="text-ink-3">{b.raftRole ?? '–'}</span>}</Td>
                  <Td className="tabular-nums">{b.leaderPartitions.length}</Td>
                  <Td className="tabular-nums">{b.replicaPartitions.length}</Td>
                  <Td className="tabular-nums">{fmtRate(b.inMsgPerSec)}</Td>
                  <Td className="tabular-nums">{fmtRate(b.outMsgPerSec)}</Td>
                  <Td className="tabular-nums">{b.reachable ? fmtBytes(b.storageBytes) : '–'}</Td>
                  <Td className="tabular-nums text-ink-2">{b.reachable ? fmtDuration(b.uptimeSeconds) : '–'}</Td>
                </tr>
              ))}
            </Table>
          </Card>
        </>
      )}
      <Drawer
        open={sel !== undefined}
        onClose={() => navigate('/brokers')}
        title={
          sel && (
            <span className="inline-flex items-center gap-2">
              Broker {sel.id} <StatusBadge status={sel.status} /> {sel.isController && <Badge tone="violet">controller</Badge>}
            </span>
          )
        }
      >
        {sel && <BrokerDetail b={sel} s={s} />}
      </Drawer>
    </div>
  );
}

function BrokerCard({ b, s, maxStorage }: { b: BrokerView; s: Snapshot; maxStorage: number }) {
  const series = s.history.map((h) => h.brokerIn[String(b.id)] ?? 0);
  const border = b.status === 'healthy' ? 'border-line' : b.status === 'fenced' ? 'border-warn/60' : 'border-bad/60';
  return (
    <button
      onClick={() => navigate(`/brokers/${b.id}`)}
      className={`rounded-xl border ${border} bg-surface-1 p-4 text-left shadow-sm transition-colors hover:border-accent/60`}
    >
      <div className="flex items-center gap-2">
        <span className={`grid h-9 w-9 place-items-center rounded-lg ${b.status === 'healthy' ? 'bg-ok-soft text-ok' : b.status === 'fenced' ? 'bg-warn-soft text-warn' : 'bg-bad-soft text-bad'}`}>
          <Icon name="server" className="h-4 w-4" />
        </span>
        <div className="min-w-0">
          <div className="font-semibold text-ink">Broker {b.id}</div>
          <div className="truncate font-mono text-xs text-ink-3">{b.addr}</div>
        </div>
        <div className="ml-auto flex flex-col items-end gap-1">
          <StatusBadge status={b.status} />
          {b.isController && <Badge tone="violet">controller</Badge>}
        </div>
      </div>
      {b.status !== 'healthy' && (
        <div className="mt-3 rounded-md bg-surface-2 px-2.5 py-1.5 text-xs text-ink-2">
          {b.status === 'offline' ? `Not responding: ${b.statusDetail ?? 'unreachable'}.` : 'Fenced by the controller: it leads nothing until it re-registers.'}
        </div>
      )}
      <div className="mt-4 grid grid-cols-3 gap-2 text-center">
        <div className="rounded-lg bg-surface-2 py-2">
          <div className="text-lg font-semibold tabular-nums text-accent">{b.leaderPartitions.length}</div>
          <div className="text-[10px] uppercase tracking-wide text-ink-3">Leader</div>
        </div>
        <div className="rounded-lg bg-surface-2 py-2">
          <div className="text-lg font-semibold tabular-nums text-ink">{b.replicaPartitions.length}</div>
          <div className="text-[10px] uppercase tracking-wide text-ink-3">Replica</div>
        </div>
        <div className="rounded-lg bg-surface-2 py-2">
          <div className="text-lg font-semibold tabular-nums text-ink">{fmtRate(b.inMsgPerSec)}</div>
          <div className="text-[10px] uppercase tracking-wide text-ink-3">msg/s in</div>
        </div>
      </div>
      <div className="mt-3">
        <Sparkline values={series} color={b.status === 'healthy' ? 'var(--info)' : 'var(--bad)'} />
      </div>
      <div className="mt-2 flex items-center justify-between text-xs text-ink-3">
        <span>Storage {b.reachable ? fmtBytes(b.storageBytes) : '–'}</span>
        <span>{b.reachable ? `up ${fmtDuration(b.uptimeSeconds)}` : b.process ? `process ${b.process.state}` : ''}</span>
      </div>
      <div className="mt-1.5 h-1 overflow-hidden rounded-full bg-surface-3">
        <div className="h-full rounded-full bg-violet transition-all" style={{ width: `${(b.storageBytes / maxStorage) * 100}%` }} />
      </div>
    </button>
  );
}

function BrokerDetail({ b, s }: { b: BrokerView; s: Snapshot }) {
  const bmap = brokerMap(s);
  const toast = useToast();
  const [busy, setBusy] = useState('');
  const hosted = [...b.leaderPartitions, ...b.replicaPartitions].sort();
  const act = async (action: 'kill' | 'stop' | 'start') => {
    setBusy(action);
    try {
      await api.brokerAction(b.id, action);
      toast('success', `Broker ${b.id}: ${action} sent`);
    } catch (e) {
      toast('error', `Broker ${b.id}: ${action} failed`, errorMessage(e));
    } finally {
      setBusy('');
    }
  };
  const running = !b.process || b.process.state === 'running' || b.process.state === 'starting';
  return (
    <div className="space-y-5">
      <div className="grid gap-x-8 sm:grid-cols-2">
        <div>
          <KV k="Protocol address" v={<span className="font-mono">{b.addr}</span>} />
          <KV k="HTTP (metrics)" v={b.httpAddr ? <a className="font-mono text-accent hover:underline" href={`http://${b.httpAddr}/metrics`} target="_blank" rel="noreferrer">{b.httpAddr}</a> : '–'} />
          <KV k="Reachable" v={b.reachable ? 'yes' : <span className="text-bad">no ({b.statusDetail})</span>} />
          <KV k="Fenced in metadata" v={b.fenced ? <span className="text-warn">yes</span> : 'no'} />
        </div>
        <div>
          <KV k="Raft role" v={b.raftRole ? `${b.raftRole} (term ${b.raftTerm})` : '–'} />
          <KV k="Uptime" v={b.reachable ? fmtDuration(b.uptimeSeconds) : '–'} />
          <KV k="Storage" v={b.reachable ? fmtBytes(b.storageBytes) : '–'} />
          {b.process && <KV k="Process" v={`${b.process.state}${b.process.pid ? ` · pid ${b.process.pid}` : ''}`} />}
        </div>
      </div>

      {s.capabilities.brokerControl && (
        <div className="flex flex-wrap gap-2">
          <Button variant="danger" icon="skull" onClick={() => act('kill')} loading={busy === 'kill'} disabled={!running}>
            Kill (SIGKILL)
          </Button>
          <Button icon="stop" onClick={() => act('stop')} loading={busy === 'stop'} disabled={!running}>
            Graceful stop
          </Button>
          <Button variant="primary" icon="play" onClick={() => act('start')} loading={busy === 'start'} disabled={running}>
            Start
          </Button>
        </div>
      )}

      {b.counters && (
        <div>
          <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-ink-3">Counters since broker start</h3>
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
            {Object.entries(b.counters).map(([k, v]) => (
              <div key={k} className="rounded-lg bg-surface-2 px-3 py-2">
                <div className="text-[10px] uppercase tracking-wide text-ink-3">{k.replace(/_/g, ' ')}</div>
                <div className="font-semibold tabular-nums text-ink">{fmtNum(v)}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div>
        <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
          <h3 className="text-xs font-semibold uppercase tracking-wide text-ink-3">Hosted partitions ({hosted.length})</h3>
          <ReplicaLegend />
        </div>
        {hosted.length === 0 ? (
          <Callout>This broker hosts no partition replicas.</Callout>
        ) : (
          <Table head={['Partition', 'Role here', 'Replicas', 'ISR', 'HW', 'Log end']}>
            {hosted.map((key) => {
              const [topic, id] = splitTP(key);
              const p = s.topics.find((t) => t.name === topic)?.partitions.find((x) => x.id === id);
              if (!p) return null;
              const leader = p.leader === b.id;
              const inSync = p.isr.includes(b.id);
              return (
                <tr key={key}>
                  <Td mono>{key}</Td>
                  <Td>
                    {leader ? (
                      <Badge tone="ok">
                        <Icon name="crown" className="h-3 w-3" /> Leader
                      </Badge>
                    ) : inSync ? (
                      <Badge>Follower · in sync</Badge>
                    ) : (
                      <Badge tone="warn">Follower · out of sync</Badge>
                    )}
                  </Td>
                  <Td>
                    <span className="flex gap-1">
                      {p.replicas.map((r) => (
                        <ReplicaChip key={r} id={r} partition={p} brokers={bmap} />
                      ))}
                    </span>
                  </Td>
                  <Td mono className="text-ink-2">
                    [{p.isr.join(', ')}]
                  </Td>
                  <Td className="tabular-nums">{p.highWatermark >= 0 ? fmtNum(p.highWatermark) : '–'}</Td>
                  <Td className="tabular-nums">{p.logEnd >= 0 ? fmtNum(p.logEnd) : '–'}</Td>
                </tr>
              );
            })}
          </Table>
        )}
      </div>
    </div>
  );
}
