/**
 * Temporal workflow/activity plugin.
 *
 * Detects Temporal orchestration code across the four official SDKs
 * (Python, TypeScript, Java, Go) and resolves `temporal_executes_activity`
 * edges from a workflow to the activities it calls.
 *
 * Symbol role is intentionally NOT conveyed via `RawSymbol.metadata` on
 * existing symbols — `SymbolRepository.insertSymbol` does an
 * `ON CONFLICT(symbol_id) DO UPDATE` upsert that would overwrite the real
 * symbol's byte offsets/fqn/signature with this plugin's placeholder values
 * if the auto-filled symbol id collided (see `src/db/repositories/symbol-repository.ts`).
 * Instead, like Kafka/Celery/NestJS, this plugin leaves `FileParseResult.symbols`
 * empty and conveys role via `frameworkRole` (file-level) + `routes` entries;
 * the workflow/activity relationship itself is conveyed purely by the edge.
 */
import path from 'node:path';
import { ok } from '../../../../../errors.js';
import type {
  FileParseResult,
  FrameworkPlugin,
  ParsedDependency,
  ProjectContext,
  RawEdge,
  ResolveContext,
} from '../../../../../plugin-api/types.js';
import { escapeRegExp } from '../../../../../utils/security.js';
import {
  extractTemporalGo,
  extractTemporalJava,
  extractTemporalPython,
  extractTemporalTypeScript,
  findTsActivitiesProxies,
  GO_EXECUTE_ACTIVITY_RE,
  JAVA_NEW_ACTIVITY_STUB_RE,
  PY_EXECUTE_ACTIVITY_RE,
  type TemporalRole,
  type TemporalSymbolRef,
} from './extract.js';

const TS_EXTENSIONS = new Set(['typescript', 'javascript', 'tsx', 'jsx']);

function routeMethodFor(role: TemporalRole): string {
  switch (role) {
    case 'workflow':
      return 'TEMPORAL_WORKFLOW';
    case 'activity':
      return 'TEMPORAL_ACTIVITY';
    case 'signal':
      return 'TEMPORAL_SIGNAL';
    case 'query':
      return 'TEMPORAL_QUERY';
  }
}

function hasDependency(deps: ParsedDependency[], predicate: (name: string) => boolean): boolean {
  return deps.some((d) => predicate(d.name));
}

interface ResolveSymbol {
  id: number;
  symbolId: string;
  name: string;
  kind: string;
  fqn: string | null;
  lineStart?: number | null;
  lineEnd?: number | null;
}

function findEnclosingSymbol(
  symbols: ResolveSymbol[],
  line: number,
  kinds: string[],
): ResolveSymbol | undefined {
  let best: ResolveSymbol | undefined;
  let bestSpan = Infinity;
  for (const s of symbols) {
    if (!kinds.includes(s.kind)) continue;
    if (s.lineStart == null || s.lineEnd == null) continue;
    if (line < s.lineStart || line > s.lineEnd) continue;
    const span = s.lineEnd - s.lineStart;
    if (span < bestSpan) {
      best = s;
      bestSpan = span;
    }
  }
  return best;
}

function makeEdge(sourceId: number, targetId: number): RawEdge {
  return {
    sourceNodeType: 'symbol',
    sourceRefId: sourceId,
    targetNodeType: 'symbol',
    targetRefId: targetId,
    edgeType: 'temporal_executes_activity',
    resolution: 'text_matched',
  };
}

/** Resolves a relative TS/JS import specifier to a file the project actually indexed. */
function resolveTsModulePath(
  importerPath: string,
  spec: string,
  pathToFile: Map<string, { id: number; path: string }>,
): { id: number; path: string } | undefined {
  if (!spec.startsWith('.')) return undefined;
  const dir = path.posix.dirname(importerPath);
  const joined = path.posix.normalize(path.posix.join(dir, spec));
  const candidates = [
    joined,
    `${joined}.ts`,
    `${joined}.tsx`,
    `${joined}.js`,
    `${joined}.mjs`,
    path.posix.join(joined, 'index.ts'),
    path.posix.join(joined, 'index.js'),
  ];
  for (const candidate of candidates) {
    const hit = pathToFile.get(candidate);
    if (hit) return hit;
  }
  return undefined;
}

export class TemporalPlugin implements FrameworkPlugin {
  manifest = {
    name: 'temporal',
    version: '1.0.0',
    priority: 30,
    category: 'messaging' as const,
  };

  detect(ctx: ProjectContext): boolean {
    return hasDependency(
      ctx.allDependencies,
      (name) =>
        name.startsWith('@temporalio/') ||
        name === 'temporalio' ||
        name === 'io.temporal:temporal-sdk' ||
        name === 'go.temporal.io/sdk',
    );
  }

  registerSchema() {
    return {
      edgeTypes: [
        {
          name: 'temporal_executes_activity',
          category: 'messaging',
          directed: true,
          description: 'A Temporal workflow invokes an activity',
        },
      ],
    };
  }

  extractNodes(
    filePath: string,
    content: Buffer | string,
    language: string,
  ): ReturnType<NonNullable<FrameworkPlugin['extractNodes']>> {
    const source = content.toString();
    let refs: TemporalSymbolRef[] = [];

    if (language === 'python') {
      refs = extractTemporalPython(source);
    } else if (TS_EXTENSIONS.has(language)) {
      refs = extractTemporalTypeScript(source);
    } else if (language === 'java') {
      refs = extractTemporalJava(source).refs;
    } else if (language === 'go') {
      refs = extractTemporalGo(source);
    } else {
      return ok({ status: 'ok', symbols: [] } satisfies FileParseResult);
    }

    if (refs.length === 0) {
      return ok({ status: 'ok', symbols: [] } satisfies FileParseResult);
    }

    const result: FileParseResult = {
      status: 'ok',
      symbols: [],
      routes: refs.map((ref) => ({
        method: routeMethodFor(ref.role),
        uri: ref.name,
        line: ref.line,
      })),
    };
    result.frameworkRole = refs.some((r) => r.role === 'workflow')
      ? 'temporal_workflow'
      : 'temporal_activity';
    return ok(result);
  }

  resolveEdges(ctx: ResolveContext): ReturnType<NonNullable<FrameworkPlugin['resolveEdges']>> {
    const edges: RawEdge[] = [];
    const allFiles = ctx.getAllFiles();

    const pyActivityIndex = new Map<string, ResolveSymbol>();
    const goActivityIndex = new Map<string, ResolveSymbol>();
    const javaInterfaceMethodIndex = new Map<string, Map<string, ResolveSymbol>>();
    const tsFilesByPath = new Map<string, { id: number; path: string }>();

    for (const file of allFiles) {
      if (TS_EXTENSIONS.has(file.language ?? '')) {
        tsFilesByPath.set(file.path, { id: file.id, path: file.path });
      }
    }

    for (const file of allFiles) {
      if (file.language !== 'python' && file.language !== 'go' && file.language !== 'java')
        continue;
      const source = ctx.readFile(file.path);
      if (!source) continue;
      const symbols = ctx.getSymbolsByFile(file.id) as ResolveSymbol[];

      if (file.language === 'python') {
        for (const ref of extractTemporalPython(source)) {
          if (ref.role !== 'activity') continue;
          const sym = symbols.find((s) => s.name === ref.name && s.kind === 'function');
          if (sym) pyActivityIndex.set(ref.name, sym);
        }
      } else if (file.language === 'go') {
        for (const ref of extractTemporalGo(source)) {
          if (ref.role !== 'activity') continue;
          const sym = symbols.find((s) => s.name === ref.name && s.kind === 'function');
          if (sym) goActivityIndex.set(ref.name, sym);
        }
      } else if (file.language === 'java') {
        const { interfaceMethods } = extractTemporalJava(source);
        for (const [interfaceName, methods] of interfaceMethods) {
          const methodMap =
            javaInterfaceMethodIndex.get(interfaceName) ?? new Map<string, ResolveSymbol>();
          for (const methodName of methods) {
            const sym = symbols.find((s) => s.name === methodName && s.kind === 'method');
            if (sym) methodMap.set(methodName, sym);
          }
          if (methodMap.size > 0) javaInterfaceMethodIndex.set(interfaceName, methodMap);
        }
      }
    }

    for (const file of allFiles) {
      const source = ctx.readFile(file.path);
      if (!source) continue;
      const symbols = ctx.getSymbolsByFile(file.id) as ResolveSymbol[];

      if (file.language === 'python') {
        const re = new RegExp(PY_EXECUTE_ACTIVITY_RE.source, 'g');
        let m: RegExpExecArray | null;
        while ((m = re.exec(source)) !== null) {
          const identifier = m[1].split('.').pop()!;
          const target = pyActivityIndex.get(identifier);
          if (!target) continue;
          const line = source.slice(0, m.index).split('\n').length;
          const enclosing = findEnclosingSymbol(symbols, line, ['function', 'method']);
          if (!enclosing) continue;
          edges.push(makeEdge(enclosing.id, target.id));
        }
      } else if (file.language === 'go') {
        const re = new RegExp(GO_EXECUTE_ACTIVITY_RE.source, 'g');
        let m: RegExpExecArray | null;
        while ((m = re.exec(source)) !== null) {
          const target = goActivityIndex.get(m[1]);
          if (!target) continue;
          const line = source.slice(0, m.index).split('\n').length;
          const enclosing = findEnclosingSymbol(symbols, line, ['function']);
          if (!enclosing) continue;
          edges.push(makeEdge(enclosing.id, target.id));
        }
      } else if (file.language === 'java') {
        const stubRe = new RegExp(JAVA_NEW_ACTIVITY_STUB_RE.source, 'g');
        const stubs: { varName: string; interfaceName: string }[] = [];
        let m: RegExpExecArray | null;
        while ((m = stubRe.exec(source)) !== null) {
          stubs.push({ varName: m[2], interfaceName: m[3] });
        }
        for (const stub of stubs) {
          const methodMap = javaInterfaceMethodIndex.get(stub.interfaceName);
          if (!methodMap) continue;
          const callRe = new RegExp(
            `\\b${escapeRegExp(stub.varName)}\\s*\\.\\s*(\\w+)\\s*\\(`,
            'g',
          );
          let cm: RegExpExecArray | null;
          while ((cm = callRe.exec(source)) !== null) {
            const target = methodMap.get(cm[1]);
            if (!target) continue;
            const line = source.slice(0, cm.index).split('\n').length;
            const enclosing = findEnclosingSymbol(symbols, line, ['method']);
            if (!enclosing) continue;
            edges.push(makeEdge(enclosing.id, target.id));
          }
        }
      } else if (TS_EXTENSIONS.has(file.language ?? '')) {
        for (const proxy of findTsActivitiesProxies(source)) {
          const moduleFile = resolveTsModulePath(file.path, proxy.modulePath, tsFilesByPath);
          if (!moduleFile) continue;
          const moduleSymbols = ctx.getSymbolsByFile(moduleFile.id) as ResolveSymbol[];
          const exportsByName = new Map(
            moduleSymbols.filter((s) => s.kind === 'function').map((s) => [s.name, s]),
          );

          if (proxy.binding) {
            const callRe = new RegExp(
              `\\b${escapeRegExp(proxy.binding)}\\s*\\.\\s*(\\w+)\\s*\\(`,
              'g',
            );
            let cm: RegExpExecArray | null;
            while ((cm = callRe.exec(source)) !== null) {
              const target = exportsByName.get(cm[1]);
              if (!target) continue;
              const line = source.slice(0, cm.index).split('\n').length;
              const enclosing = findEnclosingSymbol(symbols, line, ['function']);
              if (!enclosing) continue;
              edges.push(makeEdge(enclosing.id, target.id));
            }
          } else if (proxy.names) {
            for (const { exportName, localName } of proxy.names) {
              const target = exportsByName.get(exportName);
              if (!target) continue;
              const callRe = new RegExp(`(?<!\\.)\\b${escapeRegExp(localName)}\\s*\\(`, 'g');
              let cm: RegExpExecArray | null;
              while ((cm = callRe.exec(source)) !== null) {
                const line = source.slice(0, cm.index).split('\n').length;
                const enclosing = findEnclosingSymbol(symbols, line, ['function']);
                if (!enclosing) continue;
                edges.push(makeEdge(enclosing.id, target.id));
              }
            }
          }
        }
      }
    }

    return ok(edges);
  }
}
