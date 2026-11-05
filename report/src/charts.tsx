import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  LabelList,
  ReferenceArea,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { faultsOf, LOCKINGS, OUTCOME_LABELS, OUTCOMES, type FaultWindow, type Locking, type Run } from './data'
import { formatNumber } from './format'
import { useTheme } from './theme'

type TooltipEntry = { dataKey?: string | number; value?: number | string; color?: string; name?: string }

const axisTick = (muted: string) => ({ fill: muted, fontSize: 12 })

function OutcomeTooltip({ active, payload, label, faults }: {
  active?: boolean
  payload?: TooltipEntry[]
  label?: number
  faults: FaultWindow[]
}) {
  if (!active || !payload?.length || label === undefined) return null
  const during = faults.filter((f) => label >= Math.floor(f.start) && label <= f.end).map((f) => f.fault)
  return (
    <div className="tooltip">
      <div className="tooltip-title">second {label}{during.length > 0 ? `, during ${during.join(', ')}` : ''}</div>
      {[...payload].reverse().map((entry) => (
        <div className="tooltip-row" key={String(entry.dataKey)}>
          <span className="line-key" style={{ background: entry.color }} />
          <strong>{formatNumber(Number(entry.value))}</strong>
          <span className="secondary">{OUTCOME_LABELS[entry.dataKey as keyof typeof OUTCOME_LABELS]}</span>
        </div>
      ))}
    </div>
  )
}

export function OutcomesChart({ run }: { run: Run }) {
  const theme = useTheme()
  const faults = faultsOf(run)
  return (
    <figure className="chart">
      <figcaption>Client answers per second <span className="unit">(attempts, faults shaded)</span></figcaption>
      <ResponsiveContainer width="100%" height={280}>
        <AreaChart data={run.stats.timeline} margin={{ top: 20, right: 16, bottom: 0, left: 0 }}>
          <CartesianGrid vertical={false} stroke={theme.grid} />
          {faults.map((f, i) => (
            <ReferenceArea
              key={`${f.fault}-${i}`}
              x1={f.start}
              x2={f.end}
              fill={theme.faultBand}
              fillOpacity={1}
              ifOverflow="hidden"
              label={{ value: f.fault, position: 'insideTop', className: 'fault-label', offset: -14 }}
            />
          ))}
          <XAxis dataKey="t" type="number" domain={[0, 'dataMax']} tickLine={false} axisLine={{ stroke: theme.axis }} tick={axisTick(theme.muted)} unit=" s" />
          <YAxis width={56} tickLine={false} axisLine={false} tick={axisTick(theme.muted)} tickFormatter={(v) => formatNumber(Number(v))} />
          <Tooltip cursor={{ stroke: theme.axis }} content={<OutcomeTooltip faults={faults} />} />
          {OUTCOMES.map((outcome) => (
            <Area
              key={outcome}
              type="stepAfter"
              dataKey={outcome}
              stackId="answers"
              stroke={theme.surface}
              strokeWidth={2}
              fill={theme.outcome[outcome]}
              fillOpacity={1}
              isAnimationActive={false}
              activeDot={false}
            />
          ))}
        </AreaChart>
      </ResponsiveContainer>
      <ul className="legend">
        {OUTCOMES.map((outcome) => (
          <li key={outcome}><span className="area-key" style={{ background: theme.outcome[outcome] }} />{OUTCOME_LABELS[outcome]}</li>
        ))}
        {faults.length > 0 && <li><span className="band-key" />fault active</li>}
      </ul>
    </figure>
  )
}

type Group = { group: string } & Partial<Record<Locking, Run>>

function LockingTooltip({ active, payload, label, unit }: { active?: boolean; payload?: TooltipEntry[]; label?: string; unit: string }) {
  if (!active || !payload?.length) return null
  return (
    <div className="tooltip">
      <div className="tooltip-title">{label}</div>
      {payload.map((entry) => (
        <div className="tooltip-row" key={String(entry.dataKey)}>
          <span className="line-key" style={{ background: entry.color }} />
          <strong>{formatNumber(Number(entry.value))} {unit}</strong>
          <span className="secondary">{entry.dataKey}</span>
        </div>
      ))}
    </div>
  )
}

export function LockingBars({ groups, title, unit, value }: {
  groups: Group[]
  title: string
  unit: string
  value: (run: Run) => number
}) {
  const theme = useTheme()
  const data = groups.map((g) => ({
    group: g.group,
    ...Object.fromEntries(LOCKINGS.filter((l) => g[l]).map((l) => [l, value(g[l]!)])),
  }))
  return (
    <figure className="chart">
      <figcaption>{title} <span className="unit">({unit})</span></figcaption>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 20, right: 8, bottom: 0, left: 0 }} barCategoryGap="24%" barGap={2}>
          <CartesianGrid vertical={false} stroke={theme.grid} />
          <XAxis dataKey="group" tickLine={false} axisLine={{ stroke: theme.axis }} tick={axisTick(theme.muted)} />
          <YAxis width={56} tickLine={false} axisLine={false} tick={axisTick(theme.muted)} tickFormatter={(v) => formatNumber(Number(v))} />
          <Tooltip cursor={false} content={<LockingTooltip unit={unit} />} />
          {LOCKINGS.map((locking) => (
            <Bar key={locking} dataKey={locking} fill={theme.locking[locking]} radius={[4, 4, 0, 0]} stroke={theme.surface} strokeWidth={2} isAnimationActive={false}>
              <LabelList dataKey={locking} position="top" formatter={(v) => formatNumber(Number(v))} className="bar-label" />
            </Bar>
          ))}
        </BarChart>
      </ResponsiveContainer>
    </figure>
  )
}

export function LockingLegend() {
  const theme = useTheme()
  return (
    <ul className="legend">
      {LOCKINGS.map((locking) => (
        <li key={locking}><span className="area-key" style={{ background: theme.locking[locking] }} />{locking === 'ordered' ? 'ordered FOR UPDATE' : 'SERIALIZABLE with retry'}</li>
      ))}
    </ul>
  )
}
