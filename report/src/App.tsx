import { useEffect, useState } from 'react'
import { LockingBars, LockingLegend, OutcomesChart } from './charts'
import {
  faultsOf,
  isLockingRun,
  loadRuns,
  lockingComparison,
  rollbacksPer100,
  runLabel,
  scenarioOf,
  type InvariantResult,
  type Run,
} from './data'
import { formatInteger, formatNumber } from './format'

function Status({ good, label }: { good: boolean; label: string }) {
  return (
    <span className={`status ${good ? 'status-good' : 'status-critical'}`}>
      <span className="status-icon" aria-hidden="true">{good ? '✓' : '✕'}</span>
      {label}
    </span>
  )
}

const failed = (run: Run) => run.results.filter((r) => !r.pass)

type CellState = 'pass' | 'caught' | 'unexpected' | 'missed'

const CELL: Record<CellState, { status: string; icon: string; text: (r: InvariantResult) => string }> = {
  pass: { status: 'good', icon: '✓', text: () => '' },
  caught: { status: 'serious', icon: '!', text: (r) => formatInteger(r.violations) },
  unexpected: { status: 'critical', icon: '✕', text: (r) => formatInteger(r.violations) },
  missed: { status: 'critical', icon: '✕', text: () => 'held' },
}

function cellState(run: Run, result: InvariantResult): CellState {
  const mustFail = run.mustFail.includes(result.id)
  if (result.pass) return mustFail ? 'missed' : 'pass'
  return run.mustFail.length > 0 ? 'caught' : 'unexpected'
}

function Cell({ state, result }: { state: CellState; result: InvariantResult }) {
  const cell = CELL[state]
  return (
    <span className={`status status-${cell.status}`}>
      <span className="status-icon" aria-hidden="true">{cell.icon}</span>
      {cell.text(result)}
      <span className="visually-hidden">{state}</span>
    </span>
  )
}

function RunsTable({ runs }: { runs: Run[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Mode</th>
            <th>Scenario</th>
            <th className="num">Transfers sent</th>
            <th className="num">Faults injected</th>
            <th className="num">ok/s</th>
            <th className="num">p99 ms</th>
            <th>Violations found</th>
            <th>Verdict</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <tr key={run.id}>
              <td>{run.mode}{run.locking === 'serializable' ? ' (serializable)' : ''}</td>
              <td>{scenarioOf(run)}{isLockingRun(run) && run.run?.accounts ? `, ${run.run.accounts} accounts` : ''}</td>
              <td className="num">{run.stats.keys === undefined ? 'n/a' : formatInteger(run.stats.keys)}</td>
              <td className="num">{formatInteger(faultsOf(run).length)}</td>
              <td className="num">{formatNumber(run.stats.okPerSecond)}</td>
              <td className="num">{formatInteger(run.stats.p99Ms)}</td>
              <td>{failed(run).length === 0 ? 'none' : failed(run).map((r) => `${r.id} (${formatInteger(r.violations)})`).join(', ')}</td>
              <td><Status good={run.ok} label={run.ok ? 'as expected' : 'unexpected'} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function InvariantMatrix({ runs }: { runs: Run[] }) {
  const ids = runs[0]?.results.map((r) => r.id) ?? []
  const rules = new Map(runs[0]?.results.map((r) => [r.id, r.rule]))
  return (
    <div className="table-wrap">
      <table className="matrix">
        <thead>
          <tr>
            <th>Run</th>
            {ids.map((id) => <th key={id} title={rules.get(id)}>{id}</th>)}
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <tr key={run.id}>
              <td>{runLabel(run)}</td>
              {run.results.map((r) => (
                <td key={r.id} title={`${r.id}: ${r.rule}${r.pass ? '' : `, ${r.violations} violations`}`} className={run.mustFail.includes(r.id) ? 'must' : undefined}>
                  <Cell state={cellState(run, r)} result={r} />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function InvariantList({ results, mustFail }: { results: InvariantResult[]; mustFail: string[] }) {
  return (
    <ul className="invariants">
      {results.map((r) => (
        <li key={r.id}>
          <Status good={r.pass} label={`${r.id} ${r.pass ? 'holds' : `fails, ${formatInteger(r.violations)} violations`}${mustFail.includes(r.id) ? ' (must fail in this mode)' : ''}`} />
          <p className="secondary">{r.rule}</p>
          {r.examples?.slice(0, 2).map((example) => <p className="example" key={example}>{example}</p>)}
        </li>
      ))}
    </ul>
  )
}

function RunDetail({ run }: { run: Run }) {
  return (
    <>
      <p className="secondary">{run.verdict}. {formatInteger(run.stats.attempts)} attempts over {formatInteger(run.stats.seconds)} s, p50 {formatInteger(run.stats.p50Ms)} ms, p99 {formatInteger(run.stats.p99Ms)} ms, {formatInteger(run.stats.rollbacks)} rolled back transactions in the ledger database.</p>
      <OutcomesChart run={run} />
      <details>
        <summary>Answers per second as a table</summary>
        <div className="table-wrap">
          <table>
            <thead><tr><th className="num">Second</th><th className="num">ok</th><th className="num">fail</th><th className="num">unknown</th><th>Faults</th></tr></thead>
            <tbody>
              {run.stats.timeline.map((s) => (
                <tr key={s.t}>
                  <td className="num">{s.t}</td>
                  <td className="num">{formatInteger(s.ok)}</td>
                  <td className="num">{formatInteger(s.fail)}</td>
                  <td className="num">{formatInteger(s.unknown)}</td>
                  <td>{faultsOf(run).filter((f) => s.t >= Math.floor(f.start) && s.t <= f.end).map((f) => f.fault).join(', ')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
      <h3>Invariants</h3>
      <InvariantList results={run.results} mustFail={run.mustFail} />
    </>
  )
}

function LockingSection({ runs }: { runs: Run[] }) {
  const groups = lockingComparison(runs)
  if (groups.length === 0) return null
  return (
    <section>
      <div className="section-header">
        <div>
          <h2>Locking under contention</h2>
          <p className="lede">The correct mode with no faults, 64 workers. Ordered locks every account with <code>FOR UPDATE</code> in id order. SERIALIZABLE takes no row locks and retries the whole transaction on a serialization failure. Fewer accounts means more transfers on the same rows.</p>
        </div>
        <LockingLegend />
      </div>
      <div className="grid">
        <LockingBars groups={groups} title="Throughput" unit="ok answers per second" value={(run) => run.stats.okPerSecond} />
        <LockingBars groups={groups} title="p99 latency" unit="ms" value={(run) => run.stats.p99Ms} />
        <LockingBars groups={groups} title="Rollbacks" unit="per 100 ok answers" value={rollbacksPer100} />
      </div>
      <div className="table-wrap">
        <table>
          <thead><tr><th>Accounts</th><th>Locking</th><th className="num">ok/s</th><th className="num">p50 ms</th><th className="num">p99 ms</th><th className="num">Rollbacks</th><th className="num">Unknown answers</th></tr></thead>
          <tbody>
            {groups.flatMap((g) => (['ordered', 'serializable'] as const).filter((l) => g[l]).map((l) => {
              const run = g[l]!
              return (
                <tr key={run.id}>
                  <td>{g.group}</td>
                  <td>{l}</td>
                  <td className="num">{formatNumber(run.stats.okPerSecond)}</td>
                  <td className="num">{formatInteger(run.stats.p50Ms)}</td>
                  <td className="num">{formatInteger(run.stats.p99Ms)}</td>
                  <td className="num">{formatInteger(run.stats.rollbacks)}</td>
                  <td className="num">{formatInteger(run.stats.unknown)}</td>
                </tr>
              )
            }))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default function App() {
  const [runs, setRuns] = useState<Run[]>()
  const [error, setError] = useState<string>()
  const [selected, setSelected] = useState<string>()

  useEffect(() => {
    loadRuns().then(setRuns, (e: Error) => setError(e.message))
  }, [])

  if (error) return <main><p className="notice">Could not load the results: {error}</p></main>
  if (!runs) return <main><p className="secondary">Loading results...</p></main>
  if (runs.length === 0) return <main><p className="notice">No runs yet. Run <code>make chaos</code>, then <code>make report</code>.</p></main>

  const faultRuns = runs.filter((run) => !isLockingRun(run))
  const current = runs.find((run) => run.id === selected) ?? faultRuns[0] ?? runs[0]

  return (
    <main>
      <header className="page-header">
        <div>
          <h1>ledger-chaos results</h1>
          <p className="lede">Each run sends transfers from 64 workers while the nemesis injects the scenario's faults. The checker then reads both databases, the topic and the client history. The correct mode must pass every invariant. Each naive mode must fail the invariants in bold.</p>
        </div>
      </header>

      {faultRuns.length > 0 && (
        <>
          <section>
            <h2>Every run</h2>
            <RunsTable runs={faultRuns} />
          </section>

          <section>
            <h2>Invariants per run</h2>
            <p className="secondary">Count of violations where an invariant fails. Bold cells are the invariants the mode must fail. <span className="status status-serious"><span className="status-icon" aria-hidden="true">!</span>caught</span> is a failure in a naive mode, caused by the bug it was built with. <span className="status status-critical"><span className="status-icon" aria-hidden="true">✕</span>unexpected</span> is a failure in the correct mode, or a must-fail invariant that held.</p>
            <InvariantMatrix runs={faultRuns} />
          </section>
        </>
      )}

      <LockingSection runs={runs} />

      <section>
        <h2>One run</h2>
        <div className="controls">
          <label htmlFor="run">Run</label>
          <select id="run" value={current.id} onChange={(e) => setSelected(e.target.value)}>
            {runs.map((run) => <option key={run.id} value={run.id}>{runLabel(run)}{isLockingRun(run) && run.run?.accounts ? `, ${run.run.accounts} accounts` : ''}</option>)}
          </select>
        </div>
        <RunDetail run={current} />
      </section>
    </main>
  )
}
