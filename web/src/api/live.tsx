// Live cluster state over Server-Sent Events.
//
// One EventSource connection to /api/events delivers:
//   snapshot - the whole cluster view, once per second
//   history  - recent timeline events, once on connect
//   event    - each new timeline event as it happens
// The browser never polls; EventSource reconnects automatically.
import { createContext, useContext, useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import type { ClusterEvent, Snapshot } from './types';

export type ConnState = 'connecting' | 'live' | 'reconnecting';

interface LiveState {
  snapshot: Snapshot | null;
  events: ClusterEvent[];
  conn: ConnState;
  lastUpdate: number;
  /** Subscribe to individual new events (for toasts). Returns unsubscribe. */
  onEvent: (fn: (e: ClusterEvent) => void) => () => void;
}

const LiveContext = createContext<LiveState>({
  snapshot: null,
  events: [],
  conn: 'connecting',
  lastUpdate: 0,
  onEvent: () => () => {},
});

const MAX_EVENTS = 400;

export function LiveProvider({ children }: { children?: ReactNode }) {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [events, setEvents] = useState<ClusterEvent[]>([]);
  const [conn, setConn] = useState<ConnState>('connecting');
  const [lastUpdate, setLastUpdate] = useState(0);
  const listeners = useRef(new Set<(e: ClusterEvent) => void>());

  useEffect(() => {
    const es = new EventSource('/api/events');
    es.addEventListener('open', () => setConn('live'));
    es.addEventListener('error', () => setConn(es.readyState === EventSource.CLOSED ? 'reconnecting' : 'reconnecting'));
    es.addEventListener('snapshot', (m: MessageEvent) => {
      setSnapshot(JSON.parse(m.data) as Snapshot);
      setLastUpdate(Date.now());
      setConn('live');
    });
    es.addEventListener('history', (m: MessageEvent) => {
      const list = JSON.parse(m.data) as ClusterEvent[];
      setEvents(list.slice(-MAX_EVENTS));
    });
    es.addEventListener('event', (m: MessageEvent) => {
      const ev = JSON.parse(m.data) as ClusterEvent;
      setEvents((prev) => (prev.some((p) => p.id === ev.id) ? prev : [...prev, ev].slice(-MAX_EVENTS)));
      listeners.current.forEach((fn) => fn(ev));
    });
    return () => es.close();
  }, []);

  const onEvent = (fn: (e: ClusterEvent) => void) => {
    listeners.current.add(fn);
    return () => {
      listeners.current.delete(fn);
    };
  };

  return (
    <LiveContext.Provider value={{ snapshot, events, conn, lastUpdate, onEvent }}>{children}</LiveContext.Provider>
  );
}

export function useLive(): LiveState {
  return useContext(LiveContext);
}

/** Snapshot that is guaranteed non-null (render inside <RequireSnapshot>). */
export function useSnapshot(): Snapshot {
  const { snapshot } = useLive();
  if (!snapshot) throw new Error('useSnapshot used before the first snapshot arrived');
  return snapshot;
}
