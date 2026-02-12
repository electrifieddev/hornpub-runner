export type Tf = string;

export type RSIParams = { tf: Tf; period: number };
export type EmaCrossParams = { tf: Tf; fast: number; slow: number };
export type SmaCrossParams = { tf: Tf; fast: number; slow: number };
export type MacdCrossParams = { tf: Tf; fast: number; slow: number; signal: number };

export type IndicatorContext = {
  exchange: string;
  symbol: string;
};
