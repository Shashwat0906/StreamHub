// Mirrors internal/dashboard/types.go. Keep the two in sync.

export type BrokerStatus = 'healthy' | 'fenced' | 'offline';

export interface Snapshot {
  time: number;
  mode: 'managed' | 'attached';
  reachable: boolean;
  error?: string;
  controllerId: number;
  totals: Totals;
  brokers: BrokerView[];
  topics: TopicView[];
  groups: GroupView[];
  history: HistoryPoint[];
  demo: DemoView;
  dlqIndex: string[];
  capabilities: Capabilities;
  recentEvents?: ClusterEvent[];
}

export interface Capabilities {
  brokerControl: boolean;
  consumerControl: boolean;
  traffic: boolean;
  note?: string;
}

export interface Totals {
  brokers: number;
  healthyBrokers: number;
  offlineBrokers: number;
  topics: number;
  partitions: number;
  offlinePartitions: number;
  underReplicated: number;
  activeGroups: number;
  messagesProduced: number;
  messagesConsumed: number;
  producedPerSec: number;
  consumedPerSec: number;
  consumerLag: number;
  produceErrors: number;
  processingFailures: number;
  dlqMessages: number;
}

export interface ProcessView {
  pid: number;
  state: 'running' | 'killed' | 'stopped' | 'exited' | 'starting';
  logPath: string;
}

export interface BrokerView {
  id: number;
  addr: string;
  httpAddr: string;
  status: BrokerStatus;
  statusDetail?: string;
  reachable: boolean;
  fenced: boolean;
  isController: boolean;
  leaderPartitions: string[];
  replicaPartitions: string[];
  inMsgPerSec: number;
  outMsgPerSec: number;
  storageBytes: number;
  uptimeSeconds: number;
  raftRole?: string;
  raftTerm?: number;
  counters?: Record<string, number>;
  process?: ProcessView;
}

export interface TopicView {
  name: string;
  internal: boolean;
  dlq: boolean;
  configs?: Record<string, string>;
  partitions: PartitionView[];
  messages: number;
}

export interface PartitionView {
  id: number;
  leader: number;
  leaderEpoch: number;
  replicas: number[];
  isr: number[];
  logStart: number;
  logEnd: number;
  highWatermark: number;
  sizeBytes: number;
  lag: number;
  groupLag?: Record<string, number>;
  offline: boolean;
  underReplicated: boolean;
}

export interface GroupView {
  id: string;
  state: string;
  strategy: string;
  generation: number;
  members: MemberView[];
  offsets: GroupOffsetView[];
  totalLag: number;
  /** Coordinator unreachable: last known values, kept up to 30 s during failover. */
  stale?: boolean;
  staleSince?: number;
  error?: string;
}

export interface MemberView {
  id: string;
  clientId: string;
  partitions: string[];
}

export interface GroupOffsetView {
  topic: string;
  partition: number;
  committed: number;
  end: number;
  lag: number;
  owner?: string;
}

export interface HistoryPoint {
  t: number;
  producedPerSec: number;
  consumedPerSec: number;
  lag: number;
  brokerIn: Record<string, number>;
  healthyBrokers: number;
}

export type Phase = 'unavailable' | 'detected' | 'leader_failure' | 'election' | 'isr' | 'reassignment' | 'recovery';

export interface ClusterEvent {
  id: number;
  time: number;
  kind: string;
  phase?: Phase;
  severity: 'info' | 'warn' | 'error' | 'success';
  message: string;
  broker?: number;
  topic?: string;
  partition?: number;
  group?: string;
}

export interface DemoView {
  traffic: TrafficView;
  consumers: ConsumerView[];
}

export interface TrafficView {
  running: boolean;
  topic: string;
  rate: number;
  failureRate: number;
  sent: number;
  errors: number;
  lastError?: string;
}

export interface ConsumerView {
  id: string;
  group: string;
  topic: string;
  status: 'starting' | 'running' | 'crashed' | 'stopped';
  memberId: string;
  generation: number;
  partitions: string[];
  delayMs: number;
  processed: number;
  failed: number;
  lastError?: string;
}

export interface StreamMessage {
  id: string;
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
  key: string;
  value: string;
  size: number;
  truncated: boolean;
  ack: string;
}

export type AcksMode = '0' | '1' | 'all';

export interface ProduceRequest {
  topic: string;
  key: string;
  value: string;
  acks: AcksMode;
  partition?: number;
}

export interface ProduceResult {
  messageId: string;
  topic: string;
  partition: number;
  offset: number;
  acks: AcksMode;
  status: 'acknowledged' | 'sent-no-ack' | 'failed';
  error?: string;
  latencyMs: number;
  timestamp: number;
}

export interface DLQEntry {
  id: string;
  dlqTopic: string;
  dlqPartition: number;
  dlqOffset: number;
  originalTopic: string;
  partition: number;
  offset: number;
  key: string;
  payload: string;
  error: string;
  retryCount: number;
  consumerGroup: string;
  failedAt: number;
  retried: boolean;
  eligible: boolean;
  ineligibleCause?: string;
}
