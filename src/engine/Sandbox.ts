import vm from "node:vm";

export type SandboxApi = Record<string, unknown>;

export type RunInSandboxOpts = {
  timeoutMs?: number;
};

function freeze<T extends object>(obj: T): T {
  return Object.freeze(obj);
}

/**
 * Executes user strategy code with a very small, explicit surface area.
 * - No fs / process / network
 * - No require / import
 * - Only receives the API you provide
 */
export async function runInSandbox(
  userCode: string,
  api: SandboxApi,
  opts: RunInSandboxOpts = {}
): Promise<unknown> {
  const timeoutMs = opts.timeoutMs ?? 1500;

  // Build a null-prototype global to reduce surprises.
  const sandbox: Record<string, unknown> = Object.create(null);

  // Expose only whitelisted API.
  for (const [k, v] of Object.entries(api)) sandbox[k] = v;

  // Provide harmless builtins that real JS expects.
  sandbox.Math = freeze(Math);
  sandbox.Date = Date;
  sandbox.Number = Number;
  sandbox.String = String;
  sandbox.Boolean = Boolean;
  sandbox.Array = Array;
  sandbox.Object = Object;
  sandbox.JSON = JSON;
  sandbox.BigInt = BigInt;

  const context = vm.createContext(sandbox, {
    codeGeneration: { strings: false, wasm: false },
  });

  // Wrap to support top-level await for HP.buy/sell.
  const wrapped = `(async () => {\n${userCode}\n})()`;

  const script = new vm.Script(wrapped, {
    filename: "strategy.js",
  });

  const result = script.runInContext(context, { timeout: timeoutMs });
  // result is a Promise due to wrapper
  return await Promise.resolve(result);
}
