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
  // B-09: freeze the mutable globals so a malicious strategy cannot mutate
  // Array.prototype / Object.prototype and corrupt subsequent sandboxed runs.
  // Number, String, Boolean, BigInt are frozen implicitly (primitives/wrappers);
  // we freeze the four container/utility globals that are actually mutable.
  sandbox.Math = freeze(Math);
  sandbox.Date = freeze(Date);
  sandbox.Number = Number;
  sandbox.String = String;
  sandbox.Boolean = Boolean;
  sandbox.Array = freeze(Array);
  sandbox.Object = freeze(Object);
  sandbox.JSON = freeze(JSON);
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

  // The vm `timeout` above fires only during *synchronous* execution of the script.
  // Our IIFE wrapper is async, so runInContext returns a Promise almost immediately
  // and the synchronous timeout stops protecting us after that point.
  // Race against a manual deadline so strategies that loop on async calls
  // (e.g. while(true) { await HP.buy(...) }) cannot hang the runner indefinitely.
  const asyncTimeoutPromise = new Promise<never>((_, reject) =>
    setTimeout(
      () => reject(new Error(`Strategy execution timed out after ${timeoutMs}ms`)),
      timeoutMs
    )
  );

  return await Promise.race([Promise.resolve(result), asyncTimeoutPromise]);
}
