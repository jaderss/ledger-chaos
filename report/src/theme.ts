import { useSyncExternalStore } from 'react'
import type { Locking, Outcome } from './data'

const darkQuery = window.matchMedia('(prefers-color-scheme: dark)')

function subscribe(onChange: () => void) {
  darkQuery.addEventListener('change', onChange)
  return () => darkQuery.removeEventListener('change', onChange)
}

const read = (name: string) => getComputedStyle(document.documentElement).getPropertyValue(name).trim()

export type Theme = {
  outcome: Record<Outcome, string>
  locking: Record<Locking, string>
  grid: string
  axis: string
  muted: string
  surface: string
  faultBand: string
}

let cached: { dark: boolean; theme: Theme } | undefined

function snapshot(): Theme {
  if (cached?.dark !== darkQuery.matches) {
    cached = {
      dark: darkQuery.matches,
      theme: {
        outcome: { ok: read('--series-1'), fail: read('--series-2'), unknown: read('--series-3') },
        locking: { ordered: read('--series-1'), serializable: read('--series-2') },
        grid: read('--grid'),
        axis: read('--axis'),
        muted: read('--text-muted'),
        surface: read('--surface-1'),
        faultBand: read('--fault-band'),
      },
    }
  }
  return cached.theme
}

export const useTheme = () => useSyncExternalStore(subscribe, snapshot)
