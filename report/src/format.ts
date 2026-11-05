const fractionDigits = (value: number) => (value >= 100 ? 0 : value >= 10 ? 1 : 2)

export const formatNumber = (value: number) =>
  value.toLocaleString('en-US', { maximumFractionDigits: fractionDigits(Math.abs(value)) })

export const formatInteger = (value: number) => value.toLocaleString('en-US', { maximumFractionDigits: 0 })
