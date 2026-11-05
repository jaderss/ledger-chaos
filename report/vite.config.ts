import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import react from '@vitejs/plugin-react'
import { defineConfig, type Plugin } from 'vite'

const resultsDir = resolve(__dirname, process.env.RESULTS_DIR ?? '../results/sample')

function collectRuns() {
  const names = existsSync(resultsDir) ? readdirSync(resultsDir).sort() : []
  const runs = names
    .filter((name) => existsSync(join(resultsDir, name, 'report.json')))
    .map((name) => ({ id: name, ...JSON.parse(readFileSync(join(resultsDir, name, 'report.json'), 'utf8')) }))
    .filter((run) => run.stats)
  return JSON.stringify(runs)
}

function runs(): Plugin {
  return {
    name: 'runs',
    configureServer(server) {
      server.middlewares.use('/runs.json', (_req, res) => {
        res.setHeader('Content-Type', 'application/json')
        res.end(collectRuns())
      })
    },
    generateBundle() {
      this.emitFile({ type: 'asset', fileName: 'runs.json', source: collectRuns() })
    },
  }
}

export default defineConfig({
  base: './',
  plugins: [react(), runs()],
  build: { chunkSizeWarningLimit: 800 },
})
