import { beforeEach, describe, expect, it, vi } from "vitest";
import { Job } from "bullmq";
import { QueueName, type TQueueJobTypes } from "@langfuse/shared/src/server";

const {
  mergeAndWriteMock,
  createCommitGroupMock,
  addToCommitGroupMock,
  commitGroupMock,
  redisExistsMock,
  redisSetMock,
  downloadMock,
} = vi.hoisted(() => ({
  mergeAndWriteMock: vi.fn(),
  createCommitGroupMock: vi.fn(() => ({ touchedTables: new Set(), pendingWrites: [], isCommitted: false })),
  addToCommitGroupMock: vi.fn(),
  commitGroupMock: vi.fn(),
  redisExistsMock: vi.fn(),
  redisSetMock: vi.fn(),
  downloadMock: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {},
}));

vi.mock("../../env", async (importOriginal) => {
  const original = (await importOriginal()) as {};
  return {
    ...original,
    env: {
      LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE: "true",
      LANGFUSE_ENABLE_BLOB_STORAGE_FILE_LOG: "true",
      LANGFUSE_S3_EVENT_UPLOAD_BUCKET: "bucket",
      LANGFUSE_S3_EVENT_UPLOAD_PREFIX: "events/",
      LANGFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE: "false",
      QUEUE_CONSUMER_EVENT_PROPAGATION_QUEUE_IS_ENABLED: "false",
      LANGFUSE_EXPERIMENT_EARLY_EXIT_EVENT_BATCH_JOB: "false",
      LANGFUSE_SECONDARY_INGESTION_QUEUE_ENABLED_PROJECT_IDS: undefined,
      LANGFUSE_S3_CONCURRENT_READS: 1,
    },
  };
});

vi.mock("../../services/IngestionService", () => ({
  IngestionService: vi.fn().mockImplementation(() => ({
    mergeAndWrite: mergeAndWriteMock,
  })),
}));

vi.mock("../../services/ClickhouseWriter", async (importOriginal) => {
  const original = (await importOriginal()) as {};
  return {
    ...original,
    ClickhouseWriter: {
      getInstance: vi.fn().mockReturnValue({
        createCommitGroup: createCommitGroupMock,
        addToCommitGroup: addToCommitGroupMock,
        commitGroup: commitGroupMock,
      }),
    },
  };
});

vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const original = (await importOriginal()) as Record<string, unknown>;
  return {
    ...original,
    logger: {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    },
    getCurrentSpan: vi.fn().mockReturnValue({
      setAttribute: vi.fn(),
    }),
    getS3EventStorageClient: vi.fn().mockReturnValue({
      download: downloadMock,
    }),
    getQueue: vi.fn().mockReturnValue(null),
    hasS3SlowdownFlag: vi.fn().mockResolvedValue(false),
    isS3SlowDownError: vi.fn().mockReturnValue(false),
    markProjectS3Slowdown: vi.fn(),
    clickhouseClient: vi.fn().mockReturnValue({}),
    getClickhouseEntityType: vi.fn().mockReturnValue("trace"),
    recordDistribution: vi.fn(),
    recordHistogram: vi.fn(),
    recordIncrement: vi.fn(),
    traceException: vi.fn(),
    redis: {
      exists: redisExistsMock,
      set: redisSetMock,
    },
  };
});

import { ingestionQueueProcessorBuilder } from "../ingestionQueue";
import { TableName } from "../../services/ClickhouseWriter";

describe("ingestionQueueProcessorBuilder durability semantics", () => {
  const createJob = (): Job<TQueueJobTypes[QueueName.IngestionQueue]> =>
    ({
      data: {
        id: "job-1",
        name: QueueName.IngestionQueue,
        timestamp: new Date(),
        payload: {
          authCheck: {
            validKey: true,
            scope: {
              projectId: "project-1",
            },
          },
          data: {
            type: "trace-create",
            eventBodyId: "trace-1",
            fileKey: "file-1",
            skipS3List: true,
          },
        },
      },
    }) as unknown as Job<TQueueJobTypes[QueueName.IngestionQueue]>;

  beforeEach(() => {
    vi.clearAllMocks();
    mergeAndWriteMock.mockResolvedValue(undefined);
    createCommitGroupMock.mockClear();
    addToCommitGroupMock.mockResolvedValue(undefined);
    commitGroupMock.mockResolvedValue(undefined);
    redisExistsMock.mockResolvedValue(0);
    redisSetMock.mockResolvedValue("OK");
    downloadMock.mockResolvedValue(
      JSON.stringify({
        id: "trace-1",
        type: "trace-create",
        timestamp: "2026-03-09T18:31:00.000Z",
        body: {},
      }),
    );
  });

  it("writes file-log and seen cache only after mergeAndWrite succeeds", async () => {
    const processor = ingestionQueueProcessorBuilder(false);

    await processor(createJob());

    expect(mergeAndWriteMock).toHaveBeenCalledTimes(1);
    expect(createCommitGroupMock).toHaveBeenCalledTimes(1);
    expect(addToCommitGroupMock).toHaveBeenCalledWith(
      TableName.BlobStorageFileLog,
      expect.objectContaining({
        project_id: "project-1",
        event_id: "file-1",
        entity_id: "trace-1",
      }),
      expect.any(Object),
    );
    expect(commitGroupMock).toHaveBeenCalledTimes(1);
    expect(redisSetMock).toHaveBeenCalled();

    expect(mergeAndWriteMock.mock.invocationCallOrder[0]).toBeLessThan(
      addToCommitGroupMock.mock.invocationCallOrder[0],
    );
    expect(addToCommitGroupMock.mock.invocationCallOrder[0]).toBeLessThan(
      commitGroupMock.mock.invocationCallOrder[0],
    );
    expect(commitGroupMock.mock.invocationCallOrder[0]).toBeLessThan(
      redisSetMock.mock.invocationCallOrder[0],
    );
  });

  it("does not set file-log or seen cache if mergeAndWrite fails", async () => {
    mergeAndWriteMock.mockRejectedValueOnce(new Error("boom"));
    const processor = ingestionQueueProcessorBuilder(false);

    await expect(processor(createJob())).rejects.toThrow("boom");

    expect(addToCommitGroupMock).not.toHaveBeenCalled();
    expect(commitGroupMock).not.toHaveBeenCalled();
    expect(redisSetMock).not.toHaveBeenCalled();
  });
});
