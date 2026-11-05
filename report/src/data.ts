export type InvariantResult = {
  id: string
  rule: string
  pass: boolean
  violations: number
  examples?: string[]
}

export type Second = { t: number; ok: number; fail: number; unknown: number }

export type FaultWindow = { fault: string; start: number; end: number }

export type Stats = {
  seconds: number
  keys?: number
  attempts: number
  ok: number
  fail: number
  unknown: number
  okPerSecond: number
  p50Ms: number
  p99Ms: number
  maxMs: number
  commits: number
  rollbacks: number
  timeline: Second[]
  faults: FaultWindow[] | null
}

export type Run = {
  id: string
  mode: string
  locking: string
  mustFail: string[]
  ok: boolean
  verdict: string
  quiet: boolean
  results: InvariantResult[]
  stats: Stats
  run?: { scenario?: string; accounts?: number; workers?: number; duration?: string; seed?: number }
}

export const OUTCOMES = ['ok', 'fail', 'unknown'] as const
export type Outcome = (typeof OUTCOMES)[number]

export const OUTCOME_LABELS: Record<Outcome, string> = {
  ok: 'ok (201, 200)',
  fail: 'fail (4xx)',
  unknown: 'unknown (timeout, 5xx)',
}

export const LOCKINGS = ['ordered', 'serializable'] as const
export type Locking = (typeof LOCKINGS)[number]

export async function loadRuns(): Promise<Run[]> {
  const response = await fetch('./runs.json', { cache: 'no-store' })
  if (!response.ok) throw new Error(`runs.json: ${response.status}`)
  return response.json()
}

export const scenarioOf = (run: Run) => run.run?.scenario ?? 'unknown'

export const runLabel = (run: Run) =>
  `${run.mode}${run.locking === 'serializable' ? ' (serializable)' : ''} / ${scenarioOf(run)}`

export const faultsOf = (run: Run) => run.stats.faults ?? []

export const isLockingRun = (run: Run) => run.mode === 'correct' && scenarioOf(run) === 'contention'

export const rollbacksPer100 = (run: Run) => (run.stats.ok > 0 ? (100 * run.stats.rollbacks) / run.stats.ok : 0)

export function lockingComparison(runs: Run[]) {
  const latest = new Map<string, Run>()
  for (const run of runs.filter(isLockingRun)) {
    latest.set(`${run.run?.accounts}|${run.locking}`, run)
  }
  const accounts = [...new Set([...latest.values()].map((run) => run.run?.accounts ?? 0))].sort((a, b) => a - b)
  return accounts.map((count) => ({
    group: `${count} accounts`,
    ordered: latest.get(`${count}|ordered`),
    serializable: latest.get(`${count}|serializable`),
  }))
}
