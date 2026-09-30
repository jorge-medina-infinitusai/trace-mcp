/**
 * Per-language regex extraction for Temporal workflow/activity code.
 *
 * Temporal's four SDKs mark workflows/activities very differently:
 *
 *   Python (temporalio):    @workflow.defn class + @workflow.run/@workflow.signal/
 *                            @workflow.query methods; @activity.defn functions.
 *   TypeScript (@temporalio/*): no decorators — workflows are exported functions in
 *                            files importing '@temporalio/workflow'; activities are
 *                            plain exported functions referenced via proxyActivities().
 *   Java (io.temporal):      @WorkflowInterface + @WorkflowMethod on an interface;
 *                            @ActivityInterface + @ActivityMethod on another.
 *   Go (go.temporal.io/sdk): no annotations at all — functions are identified only
 *                            by being passed to worker.RegisterWorkflow/RegisterActivity.
 *                            This is the most heuristic of the four.
 */

export type TemporalRole = 'workflow' | 'activity' | 'signal' | 'query';

export interface TemporalSymbolRef {
  name: string;
  role: TemporalRole;
  line: number;
}

function lineOf(source: string, index: number): number {
  return source.slice(0, index).split('\n').length;
}

// ─── Python (temporalio) ───────────────────────────────────────────────────

const PY_WORKFLOW_DEFN_CLASS_RE = /@workflow\.defn(?:\([^)]*\))?\s*\n\s*class\s+(\w+)/g;
const PY_WORKFLOW_RUN_METHOD_RE = /@workflow\.run\s*\n\s*(?:async\s+)?def\s+(\w+)/g;
const PY_WORKFLOW_SIGNAL_METHOD_RE =
  /@workflow\.signal(?:\([^)]*\))?\s*\n\s*(?:async\s+)?def\s+(\w+)/g;
const PY_WORKFLOW_QUERY_METHOD_RE =
  /@workflow\.query(?:\([^)]*\))?\s*\n\s*(?:async\s+)?def\s+(\w+)/g;
const PY_ACTIVITY_DEFN_RE = /@activity\.defn(?:\([^)]*\))?\s*\n\s*(?:async\s+)?def\s+(\w+)/g;
export const PY_EXECUTE_ACTIVITY_RE = /\bworkflow\.execute_activity(?:_method)?\s*\(\s*([\w.]+)/g;

export function extractTemporalPython(source: string): TemporalSymbolRef[] {
  const refs: TemporalSymbolRef[] = [];
  for (const re of [PY_WORKFLOW_DEFN_CLASS_RE, PY_WORKFLOW_RUN_METHOD_RE]) {
    const r = new RegExp(re.source, 'g');
    let m: RegExpExecArray | null;
    while ((m = r.exec(source)) !== null) {
      refs.push({ name: m[1], role: 'workflow', line: lineOf(source, m.index) });
    }
  }
  const signalRe = new RegExp(PY_WORKFLOW_SIGNAL_METHOD_RE.source, 'g');
  let m: RegExpExecArray | null;
  while ((m = signalRe.exec(source)) !== null) {
    refs.push({ name: m[1], role: 'signal', line: lineOf(source, m.index) });
  }
  const queryRe = new RegExp(PY_WORKFLOW_QUERY_METHOD_RE.source, 'g');
  while ((m = queryRe.exec(source)) !== null) {
    refs.push({ name: m[1], role: 'query', line: lineOf(source, m.index) });
  }
  const activityRe = new RegExp(PY_ACTIVITY_DEFN_RE.source, 'g');
  while ((m = activityRe.exec(source)) !== null) {
    refs.push({ name: m[1], role: 'activity', line: lineOf(source, m.index) });
  }
  return refs;
}

// ─── TypeScript (@temporalio/*) ────────────────────────────────────────────

const TS_WORKFLOW_IMPORT_RE = /from\s+['"]@temporalio\/workflow['"]/;
const TS_EXPORTED_FUNCTION_RE = /export\s+(?:async\s+)?function\s+(\w+)/g;
const TS_EXPORTED_CONST_FN_RE = /export\s+const\s+(\w+)\s*=\s*(?:async\s+)?\(/g;

// `proxyActivities<...>(...)` is bound one of two ways in real Temporal TS code:
//   const activities = proxyActivities<typeof activities>(...)       — namespace form,
//                                                                       calls as activities.foo()
//   const { greet } = proxyActivities<typeof activities>(...)        — destructured form,
//                                                                       calls as greet()
// Each also has an inline `typeof import('./x')` variant and a
// `import [type] * as X from './x'` + `typeof X` alias variant.
const TS_PROXY_NAMESPACE_INLINE_RE =
  /const\s+(\w+)\s*=\s*proxyActivities<\s*typeof\s+import\(\s*['"]([^'"]+)['"]\s*\)\s*>\s*\(/g;
const TS_PROXY_NAMESPACE_ALIAS_RE =
  /const\s+(\w+)\s*=\s*proxyActivities<\s*typeof\s+(\w+)\s*>\s*\(/g;
const TS_PROXY_DESTRUCTURED_INLINE_RE =
  /const\s*\{\s*([^}]+?)\s*\}\s*=\s*proxyActivities<\s*typeof\s+import\(\s*['"]([^'"]+)['"]\s*\)\s*>\s*\(/g;
const TS_PROXY_DESTRUCTURED_ALIAS_RE =
  /const\s*\{\s*([^}]+?)\s*\}\s*=\s*proxyActivities<\s*typeof\s+(\w+)\s*>\s*\(/g;
/** `import [type] * as X from '...'` — resolves a namespace alias to its module specifier. */
const TS_NAMESPACE_IMPORT_RE = /import\s+(?:type\s+)?\*\s+as\s+(\w+)\s+from\s+['"]([^'"]+)['"]/g;

export interface DestructuredActivityBinding {
  /** The exported name on the activities module, e.g. `greet` in `{ greet as sayHello }`. */
  exportName: string;
  /** The local name calls actually use, e.g. `sayHello` in `{ greet as sayHello }`. */
  localName: string;
}

export interface TsActivitiesProxy {
  /** Raw import specifier for the activities module, e.g. `'./activities'`. */
  modulePath: string;
  /** Set for `const activities = proxyActivities(...)` — calls look like `activities.foo()`. */
  binding?: string;
  /** Set for `const { greet } = proxyActivities(...)` — calls look like `greet()` directly. */
  names?: DestructuredActivityBinding[];
}

function parseDestructuredNames(raw: string): DestructuredActivityBinding[] {
  return raw
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const asMatch = /^(\w+)\s+as\s+(\w+)$/.exec(part);
      return asMatch
        ? { exportName: asMatch[1], localName: asMatch[2] }
        : { exportName: part, localName: part };
    });
}

/**
 * Finds every `proxyActivities<...>()` call in a workflow file and resolves
 * it to the activities module specifier plus how it's bound locally —
 * either a namespace object (`activities.foo()`) or destructured names
 * (`foo()` directly) — so callers can find the right call-site pattern.
 */
export function findTsActivitiesProxies(source: string): TsActivitiesProxy[] {
  const proxies: TsActivitiesProxy[] = [];

  const nsInlineRe = new RegExp(TS_PROXY_NAMESPACE_INLINE_RE.source, 'g');
  let m: RegExpExecArray | null;
  while ((m = nsInlineRe.exec(source)) !== null) {
    proxies.push({ binding: m[1], modulePath: m[2] });
  }

  const destructuredInlineRe = new RegExp(TS_PROXY_DESTRUCTURED_INLINE_RE.source, 'g');
  while ((m = destructuredInlineRe.exec(source)) !== null) {
    proxies.push({ names: parseDestructuredNames(m[1]), modulePath: m[2] });
  }

  const namespaceImports = new Map<string, string>();
  const nsImportRe = new RegExp(TS_NAMESPACE_IMPORT_RE.source, 'g');
  while ((m = nsImportRe.exec(source)) !== null) {
    namespaceImports.set(m[1], m[2]);
  }

  const nsAliasRe = new RegExp(TS_PROXY_NAMESPACE_ALIAS_RE.source, 'g');
  while ((m = nsAliasRe.exec(source)) !== null) {
    const modulePath = namespaceImports.get(m[2]);
    if (modulePath) proxies.push({ binding: m[1], modulePath });
  }

  const destructuredAliasRe = new RegExp(TS_PROXY_DESTRUCTURED_ALIAS_RE.source, 'g');
  while ((m = destructuredAliasRe.exec(source)) !== null) {
    const modulePath = namespaceImports.get(m[2]);
    if (modulePath) proxies.push({ names: parseDestructuredNames(m[1]), modulePath });
  }

  return proxies;
}

export function extractTemporalTypeScript(source: string): TemporalSymbolRef[] {
  const refs: TemporalSymbolRef[] = [];
  if (!TS_WORKFLOW_IMPORT_RE.test(source)) return refs;

  const fnRe = new RegExp(TS_EXPORTED_FUNCTION_RE.source, 'g');
  let m: RegExpExecArray | null;
  while ((m = fnRe.exec(source)) !== null) {
    refs.push({ name: m[1], role: 'workflow', line: lineOf(source, m.index) });
  }
  const constRe = new RegExp(TS_EXPORTED_CONST_FN_RE.source, 'g');
  while ((m = constRe.exec(source)) !== null) {
    refs.push({ name: m[1], role: 'workflow', line: lineOf(source, m.index) });
  }
  return refs;
}

// ─── Java (io.temporal) ────────────────────────────────────────────────────

const JAVA_WORKFLOW_INTERFACE_RE =
  /@WorkflowInterface\s*\n\s*(?:public\s+)?interface\s+(\w+)\s*\{([^}]*)\}/g;
const JAVA_ACTIVITY_INTERFACE_RE =
  /@ActivityInterface\s*\n\s*(?:public\s+)?interface\s+(\w+)\s*\{([^}]*)\}/g;
const JAVA_INTERFACE_METHOD_RE = /(\w+)\s*\([^;]*\)\s*;/g;
export const JAVA_NEW_ACTIVITY_STUB_RE =
  /(\w+)\s+(\w+)\s*=\s*Workflow\.newActivityStub\(\s*(\w+)\.class/g;

interface JavaInterfaceExtraction {
  refs: TemporalSymbolRef[];
  /** interfaceName -> method names declared inside it */
  interfaceMethods: Map<string, Set<string>>;
}

export function extractTemporalJava(source: string): JavaInterfaceExtraction {
  const refs: TemporalSymbolRef[] = [];
  const interfaceMethods = new Map<string, Set<string>>();

  for (const [re, role] of [
    [JAVA_WORKFLOW_INTERFACE_RE, 'workflow'],
    [JAVA_ACTIVITY_INTERFACE_RE, 'activity'],
  ] as const) {
    const r = new RegExp(re.source, 'g');
    let m: RegExpExecArray | null;
    while ((m = r.exec(source)) !== null) {
      const [, interfaceName, body] = m;
      refs.push({ name: interfaceName, role, line: lineOf(source, m.index) });

      const methods = new Set<string>();
      const methodRe = new RegExp(JAVA_INTERFACE_METHOD_RE.source, 'g');
      let mm: RegExpExecArray | null;
      while ((mm = methodRe.exec(body)) !== null) {
        methods.add(mm[1]);
        refs.push({ name: mm[1], role, line: lineOf(source, m.index) });
      }
      interfaceMethods.set(interfaceName, methods);
    }
  }

  return { refs, interfaceMethods };
}

// ─── Go (go.temporal.io/sdk) ───────────────────────────────────────────────

const GO_REGISTER_WORKFLOW_RE = /\bRegisterWorkflow(?:WithOptions)?\s*\(\s*(\w+)/g;
const GO_REGISTER_ACTIVITY_RE = /\bRegisterActivity(?:WithOptions)?\s*\(\s*(\w+)/g;
export const GO_EXECUTE_ACTIVITY_RE = /\bworkflow\.ExecuteActivity\s*\(\s*ctx\s*,\s*(\w+)/g;

export function extractTemporalGo(source: string): TemporalSymbolRef[] {
  const refs: TemporalSymbolRef[] = [];
  const wfRe = new RegExp(GO_REGISTER_WORKFLOW_RE.source, 'g');
  let m: RegExpExecArray | null;
  while ((m = wfRe.exec(source)) !== null) {
    refs.push({ name: m[1], role: 'workflow', line: lineOf(source, m.index) });
  }
  const actRe = new RegExp(GO_REGISTER_ACTIVITY_RE.source, 'g');
  while ((m = actRe.exec(source)) !== null) {
    refs.push({ name: m[1], role: 'activity', line: lineOf(source, m.index) });
  }
  return refs;
}
