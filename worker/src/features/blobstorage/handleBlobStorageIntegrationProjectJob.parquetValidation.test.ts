import { Readable } from "stream";
import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import {
  BlobStorageIntegrationFileType,
  BlobStorageIntegrationType,
  BlobStorageExportMode,
} from "@langfuse/shared";

const mocks = vi.hoisted(() => ({
  findUnique: vi.fn(),
  update: vi.fn(),
  uploadFile: vi.fn(),
  getInstance: vi.fn(),
  getTracesForBlobStorageExportParquet: vi.fn(),
  logger: {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  },
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    blobStorageIntegration: {
      findUnique: mocks.findUnique,
      update: mocks.update,
    },
  },
}));

vi.mock("@langfuse/shared/encryption", () => ({
  decrypt: vi.fn().mockReturnValue("decrypted-secret"),
}));

vi.mock("../../env", () => ({
  env: {
    LANGFUSE_BLOB_STORAGE_EXPORT_CATCHUP_INTERVAL_MS: 0,
    LANGFUSE_BLOB_STORAGE_EXPORT_TRACE_ONLY_PROJECT_IDS: ["project-1"],
  },
}));

vi.mock("@langfuse/shared/src/server", async () => {
  class MockClickHouseResourceError extends Error {
    public readonly errorType = "unknown";

    static wrapIfResourceError(error: Error): Error {
      return error;
    }
  }

  return {
    QueueName: {
      BlobStorageIntegrationProcessingQueue:
        "BlobStorageIntegrationProcessingQueue",
    },
    QueueJobs: {
      BlobStorageIntegrationProcessingJob:
        "BlobStorageIntegrationProcessingJob",
    },
    logger: mocks.logger,
    StorageServiceFactory: {
      getInstance: mocks.getInstance,
    },
    streamTransformations: {
      JSON: vi.fn(),
      CSV: vi.fn(),
      JSONL: vi.fn(),
    },
    getTracesForBlobStorageExportParquet:
      mocks.getTracesForBlobStorageExportParquet,
    getObservationsForBlobStorageExportParquet: vi.fn(),
    getScoresForBlobStorageExportParquet: vi.fn(),
    getEventsForBlobStorageExport: vi.fn(),
    queryClickhouse: vi.fn(),
    getCurrentSpan: vi.fn().mockReturnValue(null),
    BlobStorageIntegrationProcessingQueue: {
      getInstance: vi.fn().mockReturnValue(null),
    },
    recordHistogram: vi.fn(),
    ClickHouseResourceError: MockClickHouseResourceError,
  };
});

import { handleBlobStorageIntegrationProjectJob } from "./handleBlobStorageIntegrationProjectJob";

const PROJECT_ID = "project-1";

const consumeUploadData = async (data: Readable | string): Promise<void> => {
  if (typeof data === "string") {
    return;
  }

  for await (const _chunk of data) {
    // Consume stream fully so validation sees all bytes.
  }
};

const createIntegration = () => {
  const now = new Date();
  const fortyFiveSecondsBeforeUncappedNow = new Date(
    now.getTime() - 30 * 60 * 1000 - 45 * 1000,
  );

  return {
    projectId: PROJECT_ID,
    enabled: true,
    lastSyncAt: fortyFiveSecondsBeforeUncappedNow,
    exportMode: BlobStorageExportMode.FROM_TODAY,
    exportStartDate: null,
    bucketName: "bucket",
    endpoint: null,
    region: "auto",
    accessKeyId: "access-key-id",
    secretAccessKey: "encrypted-secret",
    prefix: "exports/",
    forcePathStyle: true,
    type: BlobStorageIntegrationType.S3,
    exportFrequency: "hourly",
    exportSource: "TRACES_OBSERVATIONS",
    fileType: BlobStorageIntegrationFileType.JSON,
  };
};

describe("handleBlobStorageIntegrationProjectJob parquet validation", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-06T10:00:00.000Z"));
    vi.clearAllMocks();

    mocks.findUnique.mockResolvedValue(createIntegration());
    mocks.update.mockResolvedValue(undefined);
    mocks.getInstance.mockReturnValue({
      uploadFile: mocks.uploadFile,
    });
    mocks.uploadFile.mockImplementation(async (params: { data: Readable }) => {
      await consumeUploadData(params.data);
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("does not advance cursor when parquet payload is invalid", async () => {
    mocks.getTracesForBlobStorageExportParquet.mockResolvedValue(
      Readable.from([
        Buffer.from("Code: 241. DB::Exception: MEMORY_LIMIT_EXCEEDED"),
      ]),
    );

    await expect(
      handleBlobStorageIntegrationProjectJob({
        data: { payload: { projectId: PROJECT_ID } },
      } as never),
    ).rejects.toThrow(/Invalid Parquet file/);

    expect(mocks.update).not.toHaveBeenCalled();

    const validationErrorCall = mocks.logger.error.mock.calls.find(
      ([arg]) => arg?.message === "blob_export_parquet_validation_failed",
    );

    expect(validationErrorCall).toBeDefined();
    expect(validationErrorCall?.[0]).toEqual(
      expect.objectContaining({
        projectId: PROJECT_ID,
        table: "traces",
        filePath: expect.stringContaining(`${PROJECT_ID}/traces/`),
        sizeBytes: expect.any(Number),
      }),
    );
  });

  it("advances cursor when parquet payload is valid", async () => {
    const validParquet = Buffer.alloc(2048);
    validParquet[0] = 0x50; // P
    validParquet[1] = 0x41; // A
    validParquet[2] = 0x52; // R
    validParquet[3] = 0x31; // 1

    mocks.getTracesForBlobStorageExportParquet.mockResolvedValue(
      Readable.from([validParquet]),
    );

    await expect(
      handleBlobStorageIntegrationProjectJob({
        data: { payload: { projectId: PROJECT_ID } },
      } as never),
    ).resolves.toBeUndefined();

    expect(mocks.update).toHaveBeenCalledTimes(1);
    const updateInput = mocks.update.mock.calls[0][0];
    expect(updateInput.where).toEqual({ projectId: PROJECT_ID });
    expect(updateInput.data.lastSyncAt.toISOString()).toBe(
      "2026-03-06T09:30:00.000Z",
    );
    expect(updateInput.data.nextSyncAt.toISOString()).toBe(
      "2026-03-06T10:30:00.000Z",
    );
  });
});
