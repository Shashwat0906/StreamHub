import { useLive, useSnapshot } from '../api/live';
import { BarChart, Ring, TimeSeriesChart } from '../components/charts';
import { brokerTone, EventTimeline } from '../components/cluster';
import { Icon } from '../components/icons';
import { Badge, Card, Dot, Meter, PageHeader, StatCard, StatusBadge } from '../components/ui';
import { fmtBytes, fmtCompact, fmtNum, fmtRate } from '../lib/format';
import { useNow } from '../lib/hooks';
import { Link } from '../lib/router';

export function OverviewPage() {
  const s = useSnapshot();
  const { events } = useLive();
  const now = useNow(1000);
  const t = s.totals;
  const times = s.history.map((h) => h.t);
  const failed = t.produceErrors + t.processingFailures;

  return (
    <div>
      <PageHeader
        title="Overview"
        subtitle="Live state of the StreamHub cluster, pushed from the dashboard server every second (Server-Sent Events)."
      />

      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6">
        <StatCard
          label="Brokers"
          icon="server"
          tone={t.healthyBrokers === t.brokers ? 'ok' : 'bad'}
          value={`${t.healthyBrokers}/${t.brokers}`}
          sub={
            t.offlineBrokers > 0 || t.healthyBrokers < t.brokers ? (
              <span className="text-bad">
                {t.offlineBrokers} offline · {t.brokers - t.healthyBrokers - t.offlineBrokers} fenced
              </span>
            ) : (
              'all healthy'
            )
          }
        />
        <StatCard label="Topics" icon="topics" value={fmtNum(t.topics)} sub="user topics (internal & DLQ excluded)" />
        <StatCard
          label="Partitions"
          icon="database"
          tone={t.offlinePartitions > 0 ? 'bad' : t.underReplicated > 0 ? 'warn' : 'default'}
          value={fmtNum(t.partitions)}
          sub={
            t.offlinePartitions + t.underReplicated > 0
              ? `${t.offlinePartitions} offline · ${t.underReplicated} under-replicated`
              : 'all fully replicated'
          }
        />
        <StatCard label="Active groups" icon="groups" value={fmtNum(t.activeGroups)} sub={`${s.groups.length} known groups`} />
        <StatCard label="Produced" icon="send" tone="info" value={fmtCompact(t.messagesProduced)} sub={`${fmtRate(t.producedPerSec)} msg/s now`} />
        <StatCard label="Consumed" icon="check" tone="ok" value={fmtCompact(t.messagesConsumed)} sub={`${fmtRate(t.consumedPerSec)} msg/s (committed)`} />
        <StatCard label="Messages / sec" icon="gauge" tone="info" value={fmtRate(t.producedPerSec)} sub="produced, cluster-wide" />
        <StatCard
          label="Consumer lag"
          icon="clock"
          tone={t.consumerLag > 1000 ? 'bad' : t.consumerLag > 100 ? 'warn' : 'default'}
          value={fmtCompact(t.consumerLag)}
          sub="records behind high-watermark"
        />
        <StatCard
          label="Failed messages"
          icon="alert"
          tone={failed > 0 ? 'warn' : 'default'}
          value={fmtNum(failed)}
          sub={`${fmtNum(t.produceErrors)} produce errors · ${fmtNum(t.processingFailures)} processing`}
        />
        <StatCard
          label="DLQ messages"
          icon="dlq"
          tone={t.dlqMessages > 0 ? 'bad' : 'default'}
          value={fmtNum(t.dlqMessages)}
          sub={
            <Link to="/dlq" className="text-accent hover:underline">
              Open dead letter queue →
            </Link>
          }
        />
        <StatCard
          label="Controller"
          icon="crown"
          tone="violet"
          value={s.controllerId >= 0 ? `Broker ${s.controllerId}` : 'None'}
          sub="Raft leader of the metadata quorum"
        />
        <StatCard
          label="Storage"
          icon="database"
          value={fmtBytes(s.brokers.reduce((a, b) => a + b.storageBytes, 0))}
          sub="all replicas, all brokers"
        />
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-3">
        <Card
          className="xl:col-span-2"
          title="Throughput"
          subtitle="Records per second over the last 3 minutes. Consumed = rate of committed group offsets (it bursts right after a coordinator failover, when delayed commits land at once)."
          actions={
            <div className="flex items-center gap-3 text-xs text-ink-3">
              <span className="inline-flex items-center gap-1.5">
                <span className="h-2 w-2 rounded-full bg-info" />
                produced
              </span>
              <span className="inline-flex items-center gap-1.5">
                <span className="h-2 w-2 rounded-full bg-ok" />
                consumed
              </span>
            </div>
          }
        >
          <TimeSeriesChart
            times={times}
            height={230}
            series={[
              { key: 'p', label: 'Produced/s', color: 'var(--info)', values: s.history.map((h) => h.producedPerSec), area: true },
              { key: 'c', label: 'Consumed/s', color: 'var(--ok)', values: s.history.map((h) => h.consumedPerSec) },
            ]}
          />
        </Card>
        <Card title="Consumer lag" subtitle="Total records not yet committed by any group">
          <TimeSeriesChart
            times={times}
            height={230}
            format={fmtCompact}
            series={[{ key: 'lag', label: 'Lag', color: 'var(--warn)', values: s.history.map((h) => h.lag), area: true }]}
          />
        </Card>
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-3">
        <Card title="Broker health" subtitle="Status, role and load per broker" actions={<Link to="/brokers" className="text-xs text-accent hover:underline">Details →</Link>}>
          <div className="mb-4 flex items-center gap-4">
            <Ring value={t.healthyBrokers} total={t.brokers} color={t.healthyBrokers === t.brokers ? 'var(--ok)' : 'var(--bad)'} label="healthy brokers" />
            <div className="text-sm text-ink-2">
              {t.healthyBrokers === t.brokers ? 'Every broker is serving and registered with the controller.' : 'Some brokers are offline or fenced: their partitions are served by other ISR members.'}
            </div>
          </div>
          <ul className="divide-y divide-line">
            {s.brokers.map((b) => (
              <li key={b.id}>
                <Link to={`/brokers/${b.id}`} className="flex items-center gap-3 py-2 hover:bg-surface-2/50">
                  <Dot tone={brokerTone(b.status)} pulse={b.status !== 'healthy'} />
                  <span className="w-20 shrink-0 whitespace-nowrap font-medium text-ink">Broker {b.id}</span>
                  <StatusBadge status={b.status} />
                  {b.isController && <Badge tone="violet">controller</Badge>}
                  <span className="ml-auto text-xs tabular-nums text-ink-3">
                    {b.leaderPartitions.length} leaders · {fmtRate(b.inMsgPerSec)} msg/s
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        </Card>
        <Card title="Messages in per broker" subtitle="Produce records/s handled by each broker (as partition leader)">
          <BarChart
            height={220}
            data={s.brokers.map((b) => ({
              label: `B${b.id}`,
              value: b.inMsgPerSec,
              color: b.status === 'healthy' ? 'var(--info)' : 'var(--bad)',
              note: b.status,
            }))}
          />
        </Card>
        <Card title="Recent events" subtitle="Derived from cluster state changes" actions={<Link to="/failures" className="text-xs text-accent hover:underline">Timeline →</Link>}>
          <div className="max-h-[260px] overflow-y-auto pr-1">
            <EventTimeline events={events} limit={12} now={now} compact empty="No cluster events yet. They appear when leaders, ISRs, brokers or groups change." />
          </div>
        </Card>
      </div>

      <Card className="mt-4" title="Partition health by topic" subtitle="Leaders, replication and lag for every user topic">
        {s.topics.filter((x) => !x.internal).length === 0 ? (
          <div className="py-6 text-center text-sm text-ink-3">
            No topics yet. <Link to="/topics" className="text-accent hover:underline">Create one</Link>.
          </div>
        ) : (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {s.topics
              .filter((x) => !x.internal)
              .map((tp) => {
                const under = tp.partitions.filter((p) => p.underReplicated).length;
                const off = tp.partitions.filter((p) => p.offline).length;
                const lag = tp.partitions.reduce((a, p) => a + p.lag, 0);
                const healthy = tp.partitions.length - Math.max(under, off);
                return (
                  <Link key={tp.name} to={`/topics?topic=${encodeURIComponent(tp.name)}`} className="block rounded-lg border border-line p-3 hover:border-accent/50">
                    <div className="flex items-center gap-2">
                      <Icon name={tp.dlq ? 'dlq' : 'topics'} className="h-4 w-4 text-ink-3" />
                      <span className="truncate font-medium text-ink">{tp.name}</span>
                      {tp.dlq && <Badge tone="bad">DLQ</Badge>}
                      <span className="ml-auto text-xs text-ink-3">{tp.partitions.length} partitions</span>
                    </div>
                    <Meter className="mt-3" value={healthy} max={tp.partitions.length} tone={off > 0 ? 'bad' : under > 0 ? 'warn' : 'ok'} />
                    <div className="mt-2 flex justify-between text-xs text-ink-3">
                      <span>{fmtCompact(tp.messages)} records</span>
                      <span>
                        {off > 0 && <span className="text-bad">{off} offline · </span>}
                        {under > 0 && <span className="text-warn">{under} under-replicated · </span>}
                        lag {fmtCompact(lag)}
                      </span>
                    </div>
                  </Link>
                );
              })}
          </div>
        )}
      </Card>
    </div>
  );
}
