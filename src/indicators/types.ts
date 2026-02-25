export type Tf = string;

// New Blockly runtime signature.
// `smoothing` currently supports "Wilder" (Wilder/RMA). Other values may be provided by UI;
// runtime should safely fallback.
export type RSIParams = { tf: Tf; source?: unknown; period: number; smoothing?: unknown };
export type EmaCrossParams = { tf: Tf; fast: number; slow: number };
export type SmaCrossParams = { tf: Tf; fast: number; slow: number };
export type MacdCrossParams = { tf: Tf; fast: number; slow: number; signal: number };

export type IndicatorContext = {
  exchange: string;
  symbol: string;
  /** Optional — used to namespace PREV buffers so multi-project runners don't collide. */
  projectId?: string;
};
