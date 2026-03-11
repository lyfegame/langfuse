import { describe, it, expect, vi } from "vitest";
import {
  checkHeaderBasedDirectWrite,
  checkSdkVersionRequirements,
  getSdkInfoFromResourceSpans,
  synthesizeMissingTraceCreateEvents,
  writeInlineOtelEntities,
  type SdkInfo,
} from "../otelIngestionQueue";

describe("checkHeaderBasedDirectWrite", () => {
  it.each<{
    input: Parameters<typeof checkHeaderBasedDirectWrite>[0];
    expected: boolean;
    label: string;
  }>([
    // Python SDK version checks
    {
      input: { sdkName: "python", sdkVersion: "4.0.0" },
      expected: true,
      label: "python 4.0.0 (exact minimum)",
    },
    {
      input: { sdkName: "python", sdkVersion: "4.2.1" },
      expected: true,
      label: "python 4.2.1 (above minimum)",
    },
    {
      input: { sdkName: "python", sdkVersion: "3.9.0" },
      expected: false,
      label: "python 3.9.0 (below minimum)",
    },

    // JS SDK version checks
    {
      input: { sdkName: "javascript", sdkVersion: "5.0.0" },
      expected: true,
      label: "javascript 5.0.0 (exact minimum)",
    },
    {
      input: { sdkName: "javascript", sdkVersion: "5.1.3" },
      expected: true,
      label: "javascript 5.1.3 (above minimum)",
    },
    {
      input: { sdkName: "javascript", sdkVersion: "4.6.0" },
      expected: false,
      label: "javascript 4.6.0 (below minimum)",
    },

    // Pre-release versions (stripped before comparison)
    {
      input: { sdkName: "python", sdkVersion: "4.0.0-rc.1" },
      expected: true,
      label: "python 4.0.0-rc.1 (pre-release at minimum)",
    },
    {
      input: { sdkName: "javascript", sdkVersion: "5.0.0-beta.1" },
      expected: true,
      label: "javascript 5.0.0-beta.1 (pre-release at minimum)",
    },
    {
      input: { sdkName: "python", sdkVersion: "4.1.0-rc.1" },
      expected: true,
      label: "python 4.1.0-rc.1 (pre-release above minimum)",
    },
    {
      input: { sdkName: "python", sdkVersion: "3.9.0-rc.1" },
      expected: false,
      label: "python 3.9.0-rc.1 (pre-release below minimum)",
    },

    // Unknown / missing SDK name
    {
      input: { sdkName: "ruby", sdkVersion: "1.0.0" },
      expected: false,
      label: "unknown SDK name",
    },
    {
      input: { sdkName: "python" },
      expected: false,
      label: "sdkName without sdkVersion",
    },
    {
      input: { sdkVersion: "4.0.0" },
      expected: false,
      label: "sdkVersion without sdkName",
    },

    // Malformed versions
    {
      input: { sdkName: "python", sdkVersion: "not-a-version" },
      expected: false,
      label: "non-semver sdkVersion",
    },
    {
      input: { sdkName: "python", sdkVersion: "" },
      expected: false,
      label: "empty sdkVersion",
    },

    // ingestionVersion
    {
      input: { ingestionVersion: "4" },
      expected: true,
      label: "ingestionVersion '4'",
    },
    {
      input: {
        ingestionVersion: "4",
        sdkName: undefined,
        sdkVersion: undefined,
      },
      expected: true,
      label: "ingestionVersion '4' without SDK headers",
    },
    {
      input: { ingestionVersion: "3" },
      expected: false,
      label: "ingestionVersion '3' (below)",
    },
    {
      input: { ingestionVersion: "1" },
      expected: false,
      label: "ingestionVersion '1' (below)",
    },
    {
      input: { sdkName: "python", sdkVersion: "3.0.0", ingestionVersion: "4" },
      expected: true,
      label: "ingestionVersion '4' overrides old SDK version",
    },

    // No headers
    { input: {}, expected: false, label: "empty input" },
    {
      input: {
        sdkName: undefined,
        sdkVersion: undefined,
        ingestionVersion: undefined,
      },
      expected: false,
      label: "all undefined",
    },
  ])("$label → $expected", ({ input, expected }) => {
    expect(checkHeaderBasedDirectWrite(input)).toBe(expected);
  });
});

describe("checkSdkVersionRequirements (legacy fallback)", () => {
  it.each<{
    sdkInfo: SdkInfo;
    isExperiment: boolean;
    expected: boolean;
    label: string;
  }>([
    {
      sdkInfo: {
        scopeName: "openlit",
        scopeVersion: "3.9.0",
        telemetrySdkLanguage: "python",
      },
      isExperiment: true,
      expected: false,
      label: "non-Langfuse scope name",
    },
    {
      sdkInfo: {
        scopeName: "langfuse-sdk",
        scopeVersion: "3.9.0",
        telemetrySdkLanguage: "python",
      },
      isExperiment: false,
      expected: false,
      label: "experiment batch false",
    },
    {
      sdkInfo: {
        scopeName: "langfuse-sdk",
        scopeVersion: "3.9.0",
        telemetrySdkLanguage: "python",
      },
      isExperiment: true,
      expected: true,
      label: "python 3.9.0 (exact minimum)",
    },
    {
      sdkInfo: {
        scopeName: "langfuse-sdk",
        scopeVersion: "4.4.0",
        telemetrySdkLanguage: "js",
      },
      isExperiment: true,
      expected: true,
      label: "js 4.4.0 (exact minimum)",
    },
  ])("$label → $expected", ({ sdkInfo, isExperiment, expected }) => {
    expect(checkSdkVersionRequirements(sdkInfo, isExperiment)).toBe(expected);
  });
});

describe("getSdkInfoFromResourceSpans (legacy fallback)", () => {
  it.each<{
    input: Parameters<typeof getSdkInfoFromResourceSpans>[0];
    expected: ReturnType<typeof getSdkInfoFromResourceSpans>;
    label: string;
  }>([
    {
      input: {
        resource: {
          attributes: [
            { key: "telemetry.sdk.language", value: { stringValue: "python" } },
          ],
        },
        scopeSpans: [
          { scope: { name: "langfuse-sdk", version: "3.14.1" }, spans: [] },
        ],
      },
      expected: {
        scopeName: "langfuse-sdk",
        scopeVersion: "3.14.1",
        telemetrySdkLanguage: "python",
      },
      label: "well-formed input",
    },
    {
      input: {},
      expected: {
        scopeName: null,
        scopeVersion: null,
        telemetrySdkLanguage: null,
      },
      label: "empty input",
    },
  ])("$label", ({ input, expected }) => {
    expect(getSdkInfoFromResourceSpans(input)).toEqual(expected);
  });
});

describe("synthesizeMissingTraceCreateEvents", () => {
  it("synthesizes one shallow trace-create per missing observation trace", () => {
    const observations = [
      {
        id: "event-1",
        type: "span-create",
        timestamp: "2026-03-11T18:15:19.422Z",
        body: {
          id: "obs-1",
          traceId: "trace-1",
          startTime: "2026-03-11T18:15:19.422Z",
          endTime: "2026-03-11T18:15:20.422Z",
          environment: "production",
          metadata: {
            langfuse_session_id: "session-1",
            thread_id: "thread-1",
            langfuse_user_id: "user-1",
            langfuse_tags: ["tag-a", "tag-b", "tag-a"],
          },
        },
      },
      {
        id: "event-2",
        type: "generation-create",
        timestamp: "2026-03-11T18:15:21.000Z",
        body: {
          id: "obs-2",
          traceId: "trace-1",
          startTime: "2026-03-11T18:15:21.000Z",
          endTime: "2026-03-11T18:15:22.000Z",
          completionStartTime: null,
          environment: "production",
          metadata: {
            thread_id: "thread-1",
          },
        },
      },
    ] as unknown as Parameters<
      typeof synthesizeMissingTraceCreateEvents
    >[0]["observations"];

    const synthesized = synthesizeMissingTraceCreateEvents({
      observations,
      traces: [],
    });

    expect(synthesized).toHaveLength(1);
    expect(synthesized[0]).toMatchObject({
      type: "trace-create",
      timestamp: "2026-03-11T18:15:19.422Z",
      body: {
        id: "trace-1",
        timestamp: "2026-03-11T18:15:19.422Z",
        environment: "production",
        sessionId: "session-1",
        userId: "user-1",
        tags: ["tag-a", "tag-b"],
        metadata: {
          langfuse_session_id: "session-1",
          thread_id: "thread-1",
          langfuse_user_id: "user-1",
        },
      },
    });
    expect(synthesized[0].id).toBeTruthy();
  });

  it("falls back to thread_id when session metadata is absent", () => {
    const observations = [
      {
        id: "event-3",
        type: "event-create",
        timestamp: "2026-03-11T18:20:00.000Z",
        body: {
          id: "obs-3",
          traceId: "trace-2",
          startTime: "2026-03-11T18:20:00.000Z",
          environment: "default",
          metadata: {
            thread_id: "thread-2",
          },
        },
      },
    ] as unknown as Parameters<
      typeof synthesizeMissingTraceCreateEvents
    >[0]["observations"];

    const synthesized = synthesizeMissingTraceCreateEvents({
      observations,
      traces: [],
    });

    expect(synthesized).toHaveLength(1);
    expect(synthesized[0].body.sessionId).toBe("thread-2");
    expect(synthesized[0].body.metadata).toEqual({
      thread_id: "thread-2",
    });
  });

  it("skips trace ids that already have a trace-create event", () => {
    const observations = [
      {
        id: "event-4",
        type: "span-create",
        timestamp: "2026-03-11T18:25:00.000Z",
        body: {
          id: "obs-4",
          traceId: "trace-3",
          startTime: "2026-03-11T18:25:00.000Z",
          endTime: "2026-03-11T18:25:01.000Z",
          environment: "production",
        },
      },
    ] as unknown as Parameters<
      typeof synthesizeMissingTraceCreateEvents
    >[0]["observations"];

    const traces = [
      {
        id: "trace-event-1",
        type: "trace-create",
        timestamp: "2026-03-11T18:24:59.000Z",
        body: {
          id: "trace-3",
          timestamp: "2026-03-11T18:24:59.000Z",
          environment: "production",
        },
      },
    ] as unknown as Parameters<
      typeof synthesizeMissingTraceCreateEvents
    >[0]["traces"];

    const synthesized = synthesizeMissingTraceCreateEvents({
      observations,
      traces,
    });

    expect(synthesized).toEqual([]);
  });
});


describe("writeInlineOtelEntities", () => {
  it("coalesces OTEL trace and observation writes into one durable commit group", async () => {
    const commitGroup = {
      touchedTables: new Set(),
      pendingWrites: [],
      isCommitted: false,
    };
    const mergeAndWrite = vi.fn().mockResolvedValue(undefined);
    const commitGroupSpy = vi.fn().mockResolvedValue(undefined);

    await writeInlineOtelEntities({
      ingestionService: {
        mergeAndWrite,
      } as never,
      clickhouseWriter: {
        createCommitGroup: () => commitGroup,
        commitGroup: commitGroupSpy,
      } as never,
      projectId: "project-1",
      traceEvents: [
        {
          id: "trace-event-1",
          type: "trace-create",
          timestamp: "2026-03-11T21:00:00.000Z",
          body: {
            id: "trace-1",
            timestamp: "2026-03-11T21:00:00.000Z",
            environment: "default",
          },
        },
        {
          id: "trace-event-2",
          type: "trace-create",
          timestamp: "2026-03-11T21:00:01.000Z",
          body: {
            id: "trace-1",
            timestamp: "2026-03-11T21:00:01.000Z",
            environment: "default",
          },
        },
      ] as never,
      observations: [
        {
          id: "obs-event-1",
          type: "span-create",
          timestamp: "2026-03-11T21:00:00.500Z",
          body: {
            id: "obs-1",
            traceId: "trace-1",
            name: "step",
            startTime: "2026-03-11T21:00:00.500Z",
            environment: "default",
          },
        },
        {
          id: "obs-event-2",
          type: "span-create",
          timestamp: "2026-03-11T21:00:01.500Z",
          body: {
            id: "obs-1",
            traceId: "trace-1",
            name: "step",
            startTime: "2026-03-11T21:00:01.500Z",
            environment: "default",
          },
        },
      ] as never,
      forwardToEventsTable: false,
      createdAtTimestamp: new Date("2026-03-11T21:00:05.000Z"),
    });

    expect(mergeAndWrite).toHaveBeenCalledTimes(2);
    expect(mergeAndWrite).toHaveBeenNthCalledWith(
      1,
      "trace",
      "project-1",
      "trace-1",
      new Date("2026-03-11T21:00:05.000Z"),
      expect.arrayContaining([
        expect.objectContaining({ id: "trace-event-1" }),
        expect.objectContaining({ id: "trace-event-2" }),
      ]),
      false,
      expect.objectContaining({
        commitGroup,
        postCommitActions: expect.any(Array),
      }),
    );
    expect(mergeAndWrite).toHaveBeenNthCalledWith(
      2,
      "observation",
      "project-1",
      "obs-1",
      new Date("2026-03-11T21:00:05.000Z"),
      expect.arrayContaining([
        expect.objectContaining({ id: "obs-event-1" }),
        expect.objectContaining({ id: "obs-event-2" }),
      ]),
      false,
      expect.objectContaining({
        commitGroup,
        postCommitActions: expect.any(Array),
      }),
    );
    expect(commitGroupSpy).toHaveBeenCalledTimes(1);
    expect(commitGroupSpy).toHaveBeenCalledWith(commitGroup);
  });

  it("skips the commit path when there are no inline OTEL entities", async () => {
    const mergeAndWrite = vi.fn().mockResolvedValue(undefined);
    const commitGroupSpy = vi.fn().mockResolvedValue(undefined);
    const createCommitGroupSpy = vi.fn();

    await writeInlineOtelEntities({
      ingestionService: {
        mergeAndWrite,
      } as never,
      clickhouseWriter: {
        createCommitGroup: createCommitGroupSpy,
        commitGroup: commitGroupSpy,
      } as never,
      projectId: "project-1",
      traceEvents: [],
      observations: [],
      forwardToEventsTable: false,
    });

    expect(mergeAndWrite).not.toHaveBeenCalled();
    expect(createCommitGroupSpy).not.toHaveBeenCalled();
    expect(commitGroupSpy).not.toHaveBeenCalled();
  });
});
