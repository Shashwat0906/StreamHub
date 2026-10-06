// Charts are plain React-rendered SVG; d3 only computes scales and paths.
import { area, curveMonotoneX, line, max as d3max, scaleBand, scaleLinear, scaleTime } from 'd3';
import { useState } from 'react';
import { useWidth } from '../lib/hooks';
import { fmtRate, fmtTime } from '../lib/format';

export interface Series {
  key: string;
  label: string;
  color: string; // CSS colour, e.g. var(--accent)
  values: number[];
  area?: boolean;
}

const margin = { top: 10, right: 12, bottom: 22, left: 42 };

/** Multi-series time chart with hover crosshair. */
export function TimeSeriesChart({
  times,
  series,
  height = 200,
  format = fmtRate,
  empty = 'Waiting for data…',
}: {
  times: number[];
  series: Series[];
  height?: number;
  format?: (v: number) => string;
  empty?: string;
}) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);
  const w = Math.max(width, 100);
  const innerW = w - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  const hasData = times.length >= 2;

  const x = scaleTime()
    .domain(hasData ? [times[0], times[times.length - 1]] : [Date.now() - 60000, Date.now()])
    .range([0, innerW]);
  const ymax = Math.max(1, d3max(series.flatMap((s) => s.values)) ?? 1);
  const y = scaleLinear().domain([0, ymax * 1.15]).nice(4).range([innerH, 0]);
  const yTicks = y.ticks(4);
  const xTicks = x.ticks(Math.max(2, Math.floor(innerW / 110)));

  const onMove = (e: { clientX: number; currentTarget: { getBoundingClientRect(): DOMRect } }) => {
    if (!hasData) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const t = x.invert(e.clientX - rect.left - margin.left).getTime();
    let best = 0;
    for (let i = 1; i < times.length; i++) if (Math.abs(times[i] - t) < Math.abs(times[best] - t)) best = i;
    setHover(best);
  };

  return (
    <div ref={ref} className="relative w-full select-none" style={{ height }}>
      {width > 0 && (
        <svg width={w} height={height} onMouseMove={onMove} onMouseLeave={() => setHover(null)}>
          <g transform={`translate(${margin.left},${margin.top})`}>
            {yTicks.map((t) => (
              <g key={t} transform={`translate(0,${y(t)})`}>
                <line x2={innerW} stroke="var(--chart-grid)" />
                <text x={-8} dy="0.32em" textAnchor="end" fontSize={10} fill="var(--ink-3)">
                  {format(t)}
                </text>
              </g>
            ))}
            {xTicks.map((t) => (
              <text key={+t} x={x(t)} y={innerH + 15} textAnchor="middle" fontSize={10} fill="var(--ink-3)">
                {t.toLocaleTimeString('en-GB', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })}
              </text>
            ))}
            {hasData &&
              series.map((s) => {
                const pts: [number, number][] = s.values.map((v, i) => [x(times[i]), y(v)]);
                const l = line().curve(curveMonotoneX)(pts) ?? '';
                const a = area().curve(curveMonotoneX).y0(innerH)(pts) ?? '';
                return (
                  <g key={s.key}>
                    {s.area && <path d={a} fill={s.color} opacity={0.12} />}
                    <path d={l} fill="none" stroke={s.color} strokeWidth={1.75} />
                  </g>
                );
              })}
            {hover !== null && hasData && (
              <g>
                <line x1={x(times[hover])} x2={x(times[hover])} y2={innerH} stroke="var(--ink-3)" strokeDasharray="3 3" />
                {series.map((s) => (
                  <circle key={s.key} cx={x(times[hover])} cy={y(s.values[hover])} r={3} fill={s.color} />
                ))}
              </g>
            )}
          </g>
        </svg>
      )}
      {!hasData && <div className="absolute inset-0 grid place-items-center text-xs text-ink-3">{empty}</div>}
      {hover !== null && hasData && (
        <div
          className="pointer-events-none absolute top-1 z-10 rounded-lg border border-line bg-surface-1 px-2.5 py-1.5 text-xs shadow-lg"
          style={{ left: Math.min(Math.max(margin.left + x(times[hover]) + 10, 0), w - 170) }}
        >
          <div className="mb-1 font-mono text-[10px] text-ink-3">{fmtTime(times[hover])}</div>
          {series.map((s) => (
            <div key={s.key} className="flex items-center gap-2">
              <span className="h-2 w-2 rounded-full" style={{ background: s.color }} />
              <span className="text-ink-2">{s.label}</span>
              <span className="ml-auto pl-3 font-medium tabular-nums text-ink">{format(s.values[hover])}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/** Horizontal-category bar chart. */
export function BarChart({
  data,
  height = 180,
  format = fmtRate,
}: {
  data: { label: string; value: number; color: string; note?: string }[];
  height?: number;
  format?: (v: number) => string;
}) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const w = Math.max(width, 100);
  const m = { top: 14, right: 8, bottom: 22, left: 8 };
  const innerW = w - m.left - m.right;
  const innerH = height - m.top - m.bottom;
  const x = scaleBand().domain(data.map((d) => d.label)).range([0, innerW]).padding(0.35);
  const y = scaleLinear().domain([0, Math.max(1, d3max(data, (d) => d.value) ?? 1) * 1.15]).range([innerH, 0]);
  return (
    <div ref={ref} className="w-full" style={{ height }}>
      {width > 0 && (
        <svg width={w} height={height}>
          <g transform={`translate(${m.left},${m.top})`}>
            <line x2={innerW} y1={innerH} y2={innerH} stroke="var(--chart-grid)" />
            {data.map((d) => (
              <g key={d.label} transform={`translate(${x(d.label) ?? 0},0)`}>
                <rect y={y(d.value)} width={x.bandwidth()} height={Math.max(0, innerH - y(d.value))} rx={4} fill={d.color} opacity={0.85}>
                  <title>{`${d.label}: ${format(d.value)}${d.note ? ' — ' + d.note : ''}`}</title>
                </rect>
                <text x={x.bandwidth() / 2} y={y(d.value) - 4} textAnchor="middle" fontSize={10} fill="var(--ink-2)">
                  {format(d.value)}
                </text>
                <text x={x.bandwidth() / 2} y={innerH + 15} textAnchor="middle" fontSize={10} fill="var(--ink-3)">
                  {d.label}
                </text>
              </g>
            ))}
          </g>
        </svg>
      )}
    </div>
  );
}

/** Tiny inline trend line. */
export function Sparkline({ values, color = 'var(--accent)', height = 28 }: { values: number[]; color?: string; height?: number }) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const w = Math.max(width, 40);
  const x = scaleLinear().domain([0, Math.max(1, values.length - 1)]).range([1, w - 1]);
  const y = scaleLinear().domain([0, Math.max(1, d3max(values) ?? 1)]).range([height - 2, 2]);
  const pts: [number, number][] = values.map((v, i) => [x(i), y(v)]);
  return (
    <div ref={ref} className="w-full" style={{ height }}>
      {width > 0 && values.length > 1 && (
        <svg width={w} height={height}>
          <path d={area().curve(curveMonotoneX).y0(height)(pts) ?? ''} fill={color} opacity={0.12} />
          <path d={line().curve(curveMonotoneX)(pts) ?? ''} fill="none" stroke={color} strokeWidth={1.5} />
        </svg>
      )}
    </div>
  );
}

/** Ring showing a fraction (e.g. healthy brokers). */
export function Ring({ value, total, color, size = 56, label }: { value: number; total: number; color: string; size?: number; label?: string }) {
  const r = size / 2 - 5;
  const c = 2 * Math.PI * r;
  const frac = total > 0 ? value / total : 0;
  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} aria-label={label}>
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="var(--surface-3)" strokeWidth={6} />
      <circle
        cx={size / 2}
        cy={size / 2}
        r={r}
        fill="none"
        stroke={color}
        strokeWidth={6}
        strokeDasharray={`${c * frac} ${c}`}
        strokeLinecap="round"
        transform={`rotate(-90 ${size / 2} ${size / 2})`}
        style={{ transition: 'stroke-dasharray 0.5s' }}
      />
      <text x="50%" y="50%" dy="0.35em" textAnchor="middle" fontSize={13} fontWeight={600} fill="var(--ink)">
        {value}/{total}
      </text>
    </svg>
  );
}
