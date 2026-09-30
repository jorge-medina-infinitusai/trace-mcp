import { describe, expect, it } from 'vitest';
import {
  extractTemporalGo,
  extractTemporalJava,
  extractTemporalPython,
  extractTemporalTypeScript,
  findTsActivitiesProxies,
} from '../../../src/indexer/plugins/integration/messaging/temporal/extract.js';
import { TemporalPlugin } from '../../../src/indexer/plugins/integration/messaging/temporal/index.js';
import type { ProjectContext, ResolveContext } from '../../../src/plugin-api/types.js';

function makeCtx(overrides: Partial<ProjectContext> = {}): ProjectContext {
  return {
    rootPath: '/tmp/no-such',
    configFiles: [],
    detectedVersions: [],
    allDependencies: [],
    ...overrides,
  };
}

describe('TemporalPlugin — detection', () => {
  it('detects via @temporalio/workflow', () => {
    const plugin = new TemporalPlugin();
    expect(
      plugin.detect(
        makeCtx({ allDependencies: [{ name: '@temporalio/workflow', version: '1.0.0' }] }),
      ),
    ).toBe(true);
  });

  it('detects via temporalio (Python)', () => {
    const plugin = new TemporalPlugin();
    expect(
      plugin.detect(makeCtx({ allDependencies: [{ name: 'temporalio', version: '1.0.0' }] })),
    ).toBe(true);
  });

  it('detects via io.temporal:temporal-sdk (Java/Gradle)', () => {
    const plugin = new TemporalPlugin();
    expect(
      plugin.detect(
        makeCtx({ allDependencies: [{ name: 'io.temporal:temporal-sdk', version: '1.22.0' }] }),
      ),
    ).toBe(true);
  });

  it('detects via go.temporal.io/sdk (Go)', () => {
    const plugin = new TemporalPlugin();
    expect(
      plugin.detect(
        makeCtx({ allDependencies: [{ name: 'go.temporal.io/sdk', version: 'v1.25.0' }] }),
      ),
    ).toBe(true);
  });

  it('rejects without any Temporal dependency', () => {
    const plugin = new TemporalPlugin();
    expect(plugin.detect(makeCtx({ allDependencies: [{ name: 'lodash', version: '*' }] }))).toBe(
      false,
    );
  });
});

describe('TemporalPlugin — schema', () => {
  it('registers temporal_executes_activity edge type', () => {
    const schema = new TemporalPlugin().registerSchema();
    const names = schema.edgeTypes?.map((e) => e.name) ?? [];
    expect(names).toContain('temporal_executes_activity');
  });
});

describe('extractTemporalPython', () => {
  it('tags @workflow.defn class and @workflow.run method as workflow', () => {
    const refs = extractTemporalPython(`
@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(say_hello, name)
`);
    expect(refs.find((r) => r.name === 'GreetingWorkflow')?.role).toBe('workflow');
    expect(refs.find((r) => r.name === 'run')?.role).toBe('workflow');
  });

  it('tags @workflow.signal and @workflow.query methods', () => {
    const refs = extractTemporalPython(`
@workflow.defn
class OrderWorkflow:
    @workflow.signal
    def cancel(self):
        pass

    @workflow.query
    def status(self) -> str:
        return "pending"
`);
    expect(refs.find((r) => r.name === 'cancel')?.role).toBe('signal');
    expect(refs.find((r) => r.name === 'status')?.role).toBe('query');
  });

  it('tags @activity.defn function as activity', () => {
    const refs = extractTemporalPython(`
@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"
`);
    expect(refs).toEqual([{ name: 'say_hello', role: 'activity', line: 2 }]);
  });
});

describe('extractTemporalTypeScript', () => {
  it('tags exported functions in a workflow file as workflow', () => {
    const refs = extractTemporalTypeScript(`
import { proxyActivities } from '@temporalio/workflow';
import type * as activities from './activities';

const { greet } = proxyActivities<typeof activities>({ startToCloseTimeout: '1 minute' });

export async function greetingWorkflow(name: string): Promise<string> {
  return await greet(name);
}
`);
    expect(refs).toEqual([{ name: 'greetingWorkflow', role: 'workflow', line: 7 }]);
  });

  it('ignores files that do not import @temporalio/workflow', () => {
    const refs = extractTemporalTypeScript(`export function greet(name: string) { return name; }`);
    expect(refs).toEqual([]);
  });
});

describe('findTsActivitiesProxies', () => {
  it('resolves the inline typeof import() form', () => {
    const proxies = findTsActivitiesProxies(`
const activities = proxyActivities<typeof import('./activities')>({ startToCloseTimeout: '1 minute' });
`);
    expect(proxies).toEqual([{ binding: 'activities', modulePath: './activities' }]);
  });

  it('resolves the namespace-import alias form', () => {
    const proxies = findTsActivitiesProxies(`
import type * as activities from './activities';
const acts = proxyActivities<typeof activities>({ startToCloseTimeout: '1 minute' });
`);
    expect(proxies).toEqual([{ binding: 'acts', modulePath: './activities' }]);
  });
});

describe('extractTemporalJava', () => {
  it('tags @WorkflowInterface and its methods as workflow', () => {
    const { refs, interfaceMethods } = extractTemporalJava(`
@WorkflowInterface
public interface GreetingWorkflow {
  @WorkflowMethod
  String getGreeting(String name);
}
`);
    expect(refs.find((r) => r.name === 'GreetingWorkflow')?.role).toBe('workflow');
    expect(refs.find((r) => r.name === 'getGreeting')?.role).toBe('workflow');
    expect(interfaceMethods.get('GreetingWorkflow')?.has('getGreeting')).toBe(true);
  });

  it('tags @ActivityInterface and its methods as activity', () => {
    const { refs, interfaceMethods } = extractTemporalJava(`
@ActivityInterface
public interface GreetingActivities {
  @ActivityMethod
  String composeGreeting(String name);
}
`);
    expect(refs.find((r) => r.name === 'GreetingActivities')?.role).toBe('activity');
    expect(interfaceMethods.get('GreetingActivities')?.has('composeGreeting')).toBe(true);
  });
});

describe('extractTemporalGo', () => {
  it('tags functions passed to RegisterWorkflow/RegisterActivity', () => {
    const refs = extractTemporalGo(`
func main() {
  w.RegisterWorkflow(GreetingWorkflow)
  w.RegisterActivity(ComposeGreeting)
}
`);
    expect(refs).toEqual([
      { name: 'GreetingWorkflow', role: 'workflow', line: 3 },
      { name: 'ComposeGreeting', role: 'activity', line: 4 },
    ]);
  });
});

describe('TemporalPlugin.extractNodes — full integration', () => {
  it('emits temporal_workflow frameworkRole + TEMPORAL_WORKFLOW route for Python', async () => {
    const source = `
@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(say_hello, name)
`;
    const result = await new TemporalPlugin().extractNodes(
      'workflows.py',
      Buffer.from(source),
      'python',
    );
    expect(result.isOk()).toBe(true);
    const parsed = result._unsafeUnwrap();
    expect(parsed.frameworkRole).toBe('temporal_workflow');
    expect(
      parsed.routes?.some((r) => r.method === 'TEMPORAL_WORKFLOW' && r.uri === 'GreetingWorkflow'),
    ).toBe(true);
  });

  it('emits temporal_activity frameworkRole for a Python activity-only file', async () => {
    const source = `
@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"
`;
    const result = await new TemporalPlugin().extractNodes(
      'activities.py',
      Buffer.from(source),
      'python',
    );
    const parsed = result._unsafeUnwrap();
    expect(parsed.frameworkRole).toBe('temporal_activity');
    expect(parsed.routes).toEqual([{ method: 'TEMPORAL_ACTIVITY', uri: 'say_hello', line: 2 }]);
  });

  it('returns empty result for unsupported languages', async () => {
    const result = await new TemporalPlugin().extractNodes('a.rb', Buffer.from('whatever'), 'ruby');
    const parsed = result._unsafeUnwrap();
    expect(parsed.symbols).toEqual([]);
  });
});

describe('TemporalPlugin.resolveEdges — same-file Python', () => {
  it('resolves workflow.execute_activity to the activity symbol in the same file', () => {
    const src = `
@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(say_hello, name)

@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"
`;
    const ctx: ResolveContext = {
      rootPath: '/x',
      getAllFiles: () => [{ id: 1, path: 'workflow.py', language: 'python' }],
      getSymbolsByFile: () => [
        { id: 10, symbolId: 'w', name: 'run', kind: 'method', fqn: null, lineStart: 5, lineEnd: 6 },
        {
          id: 20,
          symbolId: 'a',
          name: 'say_hello',
          kind: 'function',
          fqn: null,
          lineStart: 9,
          lineEnd: 10,
        },
      ],
      getSymbolByFqn: () => undefined,
      getNodeId: () => undefined,
      createNodeIfNeeded: () => 0,
      readFile: () => src,
    } as unknown as ResolveContext;

    const edges = new TemporalPlugin().resolveEdges(ctx)._unsafeUnwrap();
    expect(edges).toHaveLength(1);
    expect(edges[0]).toMatchObject({
      sourceRefId: 10,
      targetRefId: 20,
      edgeType: 'temporal_executes_activity',
      resolution: 'text_matched',
    });
  });
});

describe('TemporalPlugin.resolveEdges — cross-file TypeScript', () => {
  it('resolves proxyActivities() call through to the activities module export', () => {
    const workflowSrc = `
import { proxyActivities } from '@temporalio/workflow';
import type * as activities from './activities';

const { greet } = proxyActivities<typeof activities>({ startToCloseTimeout: '1 minute' });

export async function greetingWorkflow(name: string): Promise<string> {
  return await greet(name);
}
`;
    const activitiesSrc = `
export async function greet(name: string): Promise<string> {
  return \`Hello, \${name}!\`;
}
`;
    const files = [
      { id: 1, path: 'workflows/greeting.ts', language: 'typescript' },
      { id: 2, path: 'workflows/activities.ts', language: 'typescript' },
    ];
    const symbolsByFile: Record<number, unknown[]> = {
      1: [
        {
          id: 100,
          symbolId: 'wf',
          name: 'greetingWorkflow',
          kind: 'function',
          fqn: null,
          lineStart: 7,
          lineEnd: 9,
        },
      ],
      2: [
        {
          id: 200,
          symbolId: 'act',
          name: 'greet',
          kind: 'function',
          fqn: null,
          lineStart: 2,
          lineEnd: 4,
        },
      ],
    };
    const ctx: ResolveContext = {
      rootPath: '/x',
      getAllFiles: () => files,
      getSymbolsByFile: (fileId: number) => symbolsByFile[fileId] ?? [],
      getSymbolByFqn: () => undefined,
      getNodeId: () => undefined,
      createNodeIfNeeded: () => 0,
      readFile: (relPath: string) =>
        relPath === 'workflows/greeting.ts' ? workflowSrc : activitiesSrc,
    } as unknown as ResolveContext;

    const edges = new TemporalPlugin().resolveEdges(ctx)._unsafeUnwrap();
    expect(edges).toHaveLength(1);
    expect(edges[0]).toMatchObject({
      sourceRefId: 100,
      targetRefId: 200,
      edgeType: 'temporal_executes_activity',
      resolution: 'text_matched',
    });
  });
});

describe('TemporalPlugin.resolveEdges — cross-file Java', () => {
  it('resolves Workflow.newActivityStub call chain to the activity interface method', () => {
    const workflowSrc = `
public class GreetingWorkflowImpl implements GreetingWorkflow {
  private final GreetingActivities activities =
      Workflow.newActivityStub(GreetingActivities.class);

  public String getGreeting(String name) {
    return activities.composeGreeting(name);
  }
}
`;
    const activitiesSrc = `
@ActivityInterface
public interface GreetingActivities {
  @ActivityMethod
  String composeGreeting(String name);
}
`;
    const files = [
      { id: 1, path: 'GreetingWorkflowImpl.java', language: 'java' },
      { id: 2, path: 'GreetingActivities.java', language: 'java' },
    ];
    const symbolsByFile: Record<number, unknown[]> = {
      1: [
        {
          id: 100,
          symbolId: 'm',
          name: 'getGreeting',
          kind: 'method',
          fqn: null,
          lineStart: 6,
          lineEnd: 8,
        },
      ],
      2: [
        {
          id: 200,
          symbolId: 'a',
          name: 'composeGreeting',
          kind: 'method',
          fqn: null,
          lineStart: 4,
          lineEnd: 4,
        },
      ],
    };
    const ctx: ResolveContext = {
      rootPath: '/x',
      getAllFiles: () => files,
      getSymbolsByFile: (fileId: number) => symbolsByFile[fileId] ?? [],
      getSymbolByFqn: () => undefined,
      getNodeId: () => undefined,
      createNodeIfNeeded: () => 0,
      readFile: (relPath: string) =>
        relPath === 'GreetingWorkflowImpl.java' ? workflowSrc : activitiesSrc,
    } as unknown as ResolveContext;

    const edges = new TemporalPlugin().resolveEdges(ctx)._unsafeUnwrap();
    expect(edges).toHaveLength(1);
    expect(edges[0]).toMatchObject({
      sourceRefId: 100,
      targetRefId: 200,
      edgeType: 'temporal_executes_activity',
      resolution: 'text_matched',
    });
  });
});

describe('TemporalPlugin.resolveEdges — same-file Go', () => {
  it('resolves workflow.ExecuteActivity(ctx, Fn) to the registered activity function', () => {
    const src = `
func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
  var result string
  err := workflow.ExecuteActivity(ctx, ComposeGreeting, name).Get(ctx, &result)
  return result, err
}

func ComposeGreeting(ctx context.Context, name string) (string, error) {
  return "Hello, " + name, nil
}

func main() {
  w.RegisterWorkflow(GreetingWorkflow)
  w.RegisterActivity(ComposeGreeting)
}
`;
    const ctx: ResolveContext = {
      rootPath: '/x',
      getAllFiles: () => [{ id: 1, path: 'workflow.go', language: 'go' }],
      getSymbolsByFile: () => [
        {
          id: 10,
          symbolId: 'w',
          name: 'GreetingWorkflow',
          kind: 'function',
          fqn: null,
          lineStart: 2,
          lineEnd: 6,
        },
        {
          id: 20,
          symbolId: 'a',
          name: 'ComposeGreeting',
          kind: 'function',
          fqn: null,
          lineStart: 8,
          lineEnd: 10,
        },
      ],
      getSymbolByFqn: () => undefined,
      getNodeId: () => undefined,
      createNodeIfNeeded: () => 0,
      readFile: () => src,
    } as unknown as ResolveContext;

    const edges = new TemporalPlugin().resolveEdges(ctx)._unsafeUnwrap();
    expect(edges).toHaveLength(1);
    expect(edges[0]).toMatchObject({
      sourceRefId: 10,
      targetRefId: 20,
      edgeType: 'temporal_executes_activity',
      resolution: 'text_matched',
    });
  });
});
