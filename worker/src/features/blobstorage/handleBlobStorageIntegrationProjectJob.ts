import { pipeline } from "stream";
import { Job } from "bullmq";
import { prisma } from "@langfuse/shared/src/db";
import {
  QueueName,
  TQueueJobTypes,
  logger,
  StorageService,
  StorageServiceFactory,
  streamTransformations,
  getObservationsForBlobStorageExportParquet,
  getTracesForBlobStorageExportParquet,
  getScoresForBlobStorageExportParquet,
  getEventsForBlobStorageExport,
  getCurrentSpan,
  BlobStorageIntegrationProcessingQueue,
  queryClickhouse,
  QueueJobs,
  recordHistogram,
  ClickHouseResourceError,
} from "@langfuse/shared/src/server";
import {
  BlobStorageIntegrationType,
  BlobStorageIntegrationFileType,
  BlobStorageExportMode,
} from "@langfuse/shared";
import { decrypt } from "@langfuse/shared/encryption";
import { randomUUID } from "crypto";
import { env } from "../../env";

const getMinTimestampForExport = async (
  projectId: string,
  lastSyncAt: Date | null,
  exportMode: BlobStorageExportMode,
  exportStartDate: Date | null,
): Promise<Date> => {
  // If we have a lastSyncAt, use it (this is for subsequent exports)
  if (lastSyncAt) {
    return lastSyncAt;
  }

  // For first export, use the export mode to determine start date
  switch (exportMode) {
    case BlobStorageExportMode.FULL_HISTORY:
      // Query ClickHouse for the actual minimum timestamp from traces, observations, and scores tables
      try {
        const result = await queryClickhouse<{ min_timestamp: number | null }>({
          query: `
              SELECT min(toUnixTimestamp(ts)) * 1000 as min_timestamp
              FROM (
                SELECT min(timestamp) as ts
                FROM traces
                WHERE project_id = {projectId: String}

                UNION ALL

                SELECT min(start_time) as ts
                FROM observations
                WHERE project_id = {projectId: String}

                UNION ALL

                SELECT min(timestamp) as ts
                FROM scores
                WHERE project_id = {projectId: String}
              )
              WHERE ts > 0 -- Ignore 0 results (usually empty tables)
            `,
          params: { projectId },
        });

        // Extract the minimum timestamp
        logger.info({
          message: "[BLOB INTEGRATION] ClickHouse min_timestamp query result",
          projectId,
          minTimestamp: result[0]?.min_timestamp,
          type: typeof result[0]?.min_timestamp,
        });
        const minTimestampValue = Number(result[0]?.min_timestamp);

        if (minTimestampValue && minTimestampValue > 0) {
          const date = new Date(minTimestampValue);
          logger.info({
            message: "[BLOB INTEGRATION] Resolved min_timestamp for export",
            projectId,
            date: date.toISOString(),
            epochMs: date.getTime(),
          });
          return date;
        }

        // If no data exists, use current time as a fallback
        logger.info({
          message: "[BLOB INTEGRATION] No historical data found, using epoch",
          projectId,
        });
        return new Date(0);
      } catch (error) {
        logger.error({
          message:
            "[BLOB INTEGRATION] Error querying ClickHouse for minimum timestamp",
          projectId,
          error,
        });
        throw new Error(`Failed to fetch minimum timestamp: ${error}`);
      }
    case BlobStorageExportMode.FROM_TODAY:
    case BlobStorageExportMode.FROM_CUSTOM_DATE:
      return exportStartDate || new Date(); // Use export start date or current time as fallback
    default:
      // eslint-disable-next-line no-case-declarations
      const _exhaustiveCheck: never = exportMode;
      throw new Error(`Invalid export mode: ${exportMode}`);
  }
};

/**
 * Get the frequency interval in milliseconds for a given export frequency.
 * This is used to chunk historic exports into manageable time windows.
 */
const getFrequencyIntervalMs = (frequency: string): number => {
  switch (frequency) {
    case "hourly":
      return 60 * 60 * 1000; // 1 hour
    case "daily":
      return 24 * 60 * 60 * 1000; // 1 day
    case "weekly":
      return 7 * 24 * 60 * 60 * 1000; // 1 week
    default:
      throw new Error(`Unsupported export frequency: ${frequency}`);
  }
};

const getFileTypeProperties = (fileType: BlobStorageIntegrationFileType) => {
  switch (fileType) {
    case BlobStorageIntegrationFileType.JSON:
      return {
        contentType: "application/json; charset=utf-8",
        extension: "json",
      };
    case BlobStorageIntegrationFileType.CSV:
      return {
        contentType: "text/csv; charset=utf-8",
        extension: "csv",
      };
    case BlobStorageIntegrationFileType.JSONL:
      return {
        contentType: "application/x-ndjson; charset=utf-8",
        extension: "jsonl",
      };
    default:
      // eslint-disable-next-line no-case-declarations
      const _exhaustiveCheck: never = fileType;
      throw new Error(`Unsupported file type: ${fileType}`);
  }
};

// Minimum time window for adaptive splitting on memory errors.
// If a chunk still exceeds ClickHouse max_memory_usage at this size,
// the error propagates to BullMQ for retry with backoff.
const MIN_EXPORT_WINDOW_MS = 60_000; // 1 minute

const processBlobStorageExport = async (config: {
  projectId: string;
  minTimestamp: Date;
  maxTimestamp: Date;
  bucketName: string;
  endpoint: string | null;
  region: string;
  accessKeyId: string | undefined;
  secretAccessKey: string | undefined;
  prefix?: string;
  forcePathStyle?: boolean;
  type: BlobStorageIntegrationType;
  table: "traces" | "observations" | "scores" | "observations_v2"; // observations_v2 is the events table
  fileType: BlobStorageIntegrationFileType;
}) => {
  const exportStartTime = Date.now();

  logger.info({
    message: "[BLOB INTEGRATION] Processing table export",
    table: config.table,
    projectId: config.projectId,
    windowStart: config.minTimestamp.toISOString(),
    windowEnd: config.maxTimestamp.toISOString(),
  });

  // Initialize the storage service
  // KMS SSE is not supported for this integration.
  const storageService: StorageService = StorageServiceFactory.getInstance({
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    bucketName: config.bucketName,
    endpoint: config.endpoint ?? undefined,
    region: config.region,
    forcePathStyle: config.forcePathStyle ?? false,
    awsSse: undefined,
    awsSseKmsKeyId: undefined,
    useAzureBlob: config.type === BlobStorageIntegrationType.AZURE_BLOB_STORAGE,
  });

  try {
    // Create the file path with prefix if available
    const timestamp = config.maxTimestamp
      .toISOString()
      .replace(/:/g, "-")
      .substring(0, 19);

    // Use Parquet for traces/observations/scores (native ClickHouse output with zstd).
    // Events (observations_v2) use the legacy JSON→transform path since they use
    // EventsQueryBuilder which already handles dedup without FINAL.
    const useParquet =
      config.table === "traces" ||
      config.table === "observations" ||
      config.table === "scores";

    if (useParquet) {
      const filePath = `${config.prefix ?? ""}${config.projectId}/${config.table}/${timestamp}.parquet`;

      const table = config.table as "traces" | "observations" | "scores";
      const parquetStreamFn = {
        traces: getTracesForBlobStorageExportParquet,
        observations: getObservationsForBlobStorageExportParquet,
        scores: getScoresForBlobStorageExportParquet,
      }[table];

      const stream = await parquetStreamFn(
        config.projectId,
        config.minTimestamp,
        config.maxTimestamp,
      );

      await storageService.uploadFile({
        fileName: filePath,
        fileType: "application/vnd.apache.parquet",
        data: stream,
        partSize: 100 * 1024 * 1024, // 100 MB part size
      });
    } else {
      // Legacy path for events (observations_v2): JSON → transform → upload
      const blobStorageProps = getFileTypeProperties(config.fileType);
      const filePath = `${config.prefix ?? ""}${config.projectId}/${config.table}/${timestamp}.${blobStorageProps.extension}`;

      const dataStream = getEventsForBlobStorageExport(
        config.projectId,
        config.minTimestamp,
        config.maxTimestamp,
      );

      const fileStream = pipeline(
        dataStream,
        streamTransformations[config.fileType](),
        (err) => {
          if (err) {
            logger.error({
              message: "[BLOB INTEGRATION] Stream pipeline failed",
              table: config.table,
              projectId: config.projectId,
              error: err,
            });
          }
        },
      );

      await storageService.uploadFile({
        fileName: filePath,
        fileType: blobStorageProps.contentType,
        data: fileStream,
        partSize: 100 * 1024 * 1024, // 100 MB part size
      });
    }

    const durationMs = Date.now() - exportStartTime;
    logger.info({
      message: `[BLOB INTEGRATION] Successfully exported ${config.table} records for project ${config.projectId}`,
      table: config.table,
      projectId: config.projectId,
      windowStart: config.minTimestamp.toISOString(),
      windowEnd: config.maxTimestamp.toISOString(),
      durationMs,
    });
    recordHistogram("langfuse.blob_export.table_duration_ms", durationMs, {
      table: config.table,
    });
  } catch (rawError) {
    // Adaptive window splitting: on ANY error, split the time window in half and
    // retry each half if the window is large enough. This ensures export always
    // makes forward progress instead of getting permanently stuck on an oversized
    // chunk. Being stuck is always worse than producing extra (smaller) files —
    // each half covers a disjoint time range, and deterministic filenames mean
    // retries overwrite.
    //
    // Error classification is used for observability only, not for the split decision.
    // This handles all failure modes: ClickHouse memory limits, timeouts, overcommit,
    // mid-stream "socket hang up" (where error text is lost), and transient errors.
    const windowMs =
      config.maxTimestamp.getTime() - config.minTimestamp.getTime();

    if (windowMs > MIN_EXPORT_WINDOW_MS) {
      const midpoint = new Date(
        config.minTimestamp.getTime() + Math.floor(windowMs / 2),
      );

      // Classify error for observability (not for control flow)
      const classified =
        rawError instanceof ClickHouseResourceError
          ? rawError
          : rawError instanceof Error
            ? ClickHouseResourceError.wrapIfResourceError(rawError)
            : rawError;
      const errorType =
        classified instanceof ClickHouseResourceError
          ? classified.errorType
          : "unknown";

      logger.warn({
        message: "blob_export_error_splitting",
        table: config.table,
        projectId: config.projectId,
        errorType,
        errorMessage:
          rawError instanceof Error ? rawError.message : String(rawError),
        originalWindowMs: windowMs,
        splitWindowMs: Math.floor(windowMs / 2),
        minTimestamp: config.minTimestamp.toISOString(),
        maxTimestamp: config.maxTimestamp.toISOString(),
      });
      recordHistogram("langfuse.blob_export.split_count", 1, {
        table: config.table,
        errorType,
      });

      await processBlobStorageExport({ ...config, maxTimestamp: midpoint });
      await processBlobStorageExport({ ...config, minTimestamp: midpoint });
      return;
    }

    // Window at minimum (≤60s) — can't split further.
    // Let the error propagate to BullMQ for retry with backoff.
    const durationMs = Date.now() - exportStartTime;
    logger.error({
      message: `[BLOB INTEGRATION] Error exporting ${config.table} for project ${config.projectId}`,
      table: config.table,
      projectId: config.projectId,
      durationMs,
      error: rawError,
    });
    throw rawError;
  }
};

export const handleBlobStorageIntegrationProjectJob = async (
  job: Job<TQueueJobTypes[QueueName.BlobStorageIntegrationProcessingQueue]>,
) => {
  const { projectId } = job.data.payload;

  const span = getCurrentSpan();
  if (span) {
    span.setAttribute("messaging.bullmq.job.input.jobId", job.data.id);
    span.setAttribute("messaging.bullmq.job.input.projectId", projectId);
  }

  logger.info({
    message: "[BLOB INTEGRATION] Processing blob storage integration job",
    projectId,
  });

  const blobStorageIntegration = await prisma.blobStorageIntegration.findUnique(
    {
      where: {
        projectId,
      },
    },
  );

  if (!blobStorageIntegration) {
    logger.warn({
      message: "[BLOB INTEGRATION] Integration not found",
      projectId,
    });
    return;
  }
  if (!blobStorageIntegration.enabled) {
    logger.info({
      message: "[BLOB INTEGRATION] Integration is disabled",
      projectId,
    });
    return;
  }

  // Sync between lastSyncAt and now - 30 minutes
  // Cap the export to one frequency period to enable chunked historic exports
  const minTimestamp = await getMinTimestampForExport(
    projectId,
    blobStorageIntegration.lastSyncAt,
    blobStorageIntegration.exportMode,
    blobStorageIntegration.exportStartDate,
  );

  logger.info({
    message: "[BLOB INTEGRATION] Calculated minTimestamp",
    projectId,
    minTimestamp: minTimestamp.toISOString(),
    exportMode: blobStorageIntegration.exportMode,
    lastSyncAt: blobStorageIntegration.lastSyncAt?.toISOString() ?? null,
    exportStartDate:
      blobStorageIntegration.exportStartDate?.toISOString() ?? null,
  });

  const now = new Date();
  const uncappedMaxTimestamp = new Date(now.getTime() - 30 * 60 * 1000); // 30-minute lag buffer
  const frequencyIntervalMs = getFrequencyIntervalMs(
    blobStorageIntegration.exportFrequency,
  );

  // Use smaller chunks when catching up to reduce per-query ClickHouse memory.
  // Large observation tables can peak at high memory for wide time windows;
  // 15-minute chunks keep memory bounded.
  const catchupIntervalMs =
    env.LANGFUSE_BLOB_STORAGE_EXPORT_CATCHUP_INTERVAL_MS;
  const isCatchingUp =
    catchupIntervalMs > 0 &&
    minTimestamp.getTime() + frequencyIntervalMs <
      uncappedMaxTimestamp.getTime();
  const chunkIntervalMs = isCatchingUp
    ? Math.min(frequencyIntervalMs, catchupIntervalMs)
    : frequencyIntervalMs;

  if (isCatchingUp) {
    logger.info({
      message: "[BLOB INTEGRATION] Catch-up mode active",
      projectId,
      chunkIntervalSec: chunkIntervalMs / 1000,
      lagHours: Math.round(
        (uncappedMaxTimestamp.getTime() - minTimestamp.getTime()) / 3600000,
      ),
    });
  }

  // Cap maxTimestamp to one chunk ahead of minTimestamp
  // This ensures large historic exports are broken into manageable chunks
  const maxTimestamp = new Date(
    Math.min(
      minTimestamp.getTime() + chunkIntervalMs,
      uncappedMaxTimestamp.getTime(),
    ),
  );

  logger.info({
    message: "[BLOB INTEGRATION] Calculated maxTimestamp",
    projectId,
    maxTimestamp: maxTimestamp.toISOString(),
    frequencyIntervalMs,
  });

  // Skip export if the time window is empty or invalid
  if (minTimestamp >= maxTimestamp) {
    logger.info({
      message: "[BLOB INTEGRATION] Skipping export, time window is empty",
      projectId,
      minTimestamp: minTimestamp.toISOString(),
      maxTimestamp: maxTimestamp.toISOString(),
    });
    return;
  }

  try {
    const jobStartMs = Date.now();

    // Process the export based on the integration configuration
    const executionConfig = {
      projectId,
      minTimestamp,
      maxTimestamp,
      bucketName: blobStorageIntegration.bucketName,
      endpoint: blobStorageIntegration.endpoint,
      region: blobStorageIntegration.region || "auto",
      accessKeyId: blobStorageIntegration.accessKeyId || undefined,
      secretAccessKey: blobStorageIntegration.secretAccessKey
        ? decrypt(blobStorageIntegration.secretAccessKey)
        : undefined,
      prefix: blobStorageIntegration.prefix || undefined,
      forcePathStyle: blobStorageIntegration.forcePathStyle || undefined,
      type: blobStorageIntegration.type,
      fileType: blobStorageIntegration.fileType,
    };

    // Check if this project should only export traces (legacy behavior via env var)
    const isTraceOnlyProject =
      env.LANGFUSE_BLOB_STORAGE_EXPORT_TRACE_ONLY_PROJECT_IDS.includes(
        projectId,
      );

    let tablesExportedCount = 0;

    if (isTraceOnlyProject) {
      // Only process traces table for projects in the trace-only list (legacy behavior)
      logger.info({
        message: "[BLOB INTEGRATION] Trace-only export mode (env var override)",
        projectId,
      });
      await processBlobStorageExport({ ...executionConfig, table: "traces" });
      tablesExportedCount = 1;
    } else {
      // Process tables sequentially to reduce peak memory and ClickHouse load.
      // Parallel exports (Promise.all) caused ~4x concurrent memory pressure and
      // S3 upload contention for large projects (18 GiB observations/hour).
      const exportTasks: Array<() => Promise<void>> = [];

      // Always include scores
      exportTasks.push(() =>
        processBlobStorageExport({ ...executionConfig, table: "scores" }),
      );

      // Traces and observations - for TRACES_OBSERVATIONS and TRACES_OBSERVATIONS_EVENTS
      if (
        blobStorageIntegration.exportSource === "TRACES_OBSERVATIONS" ||
        blobStorageIntegration.exportSource === "TRACES_OBSERVATIONS_EVENTS"
      ) {
        exportTasks.push(
          () =>
            processBlobStorageExport({ ...executionConfig, table: "traces" }),
          () =>
            processBlobStorageExport({
              ...executionConfig,
              table: "observations",
            }),
        );
      }

      // Events - for EVENTS and TRACES_OBSERVATIONS_EVENTS
      // events are stored in the observations_v2 directory in blob storage
      if (
        blobStorageIntegration.exportSource === "EVENTS" ||
        blobStorageIntegration.exportSource === "TRACES_OBSERVATIONS_EVENTS"
      ) {
        exportTasks.push(() =>
          processBlobStorageExport({
            ...executionConfig,
            table: "observations_v2",
          }),
        );
      }

      for (const task of exportTasks) {
        await task();
      }
      tablesExportedCount = exportTasks.length;
    }

    logger.info({
      message: "blob_export_job_completed",
      projectId,
      durationMs: Date.now() - jobStartMs,
      isCatchingUp,
      windowStart: minTimestamp.toISOString(),
      windowEnd: maxTimestamp.toISOString(),
      tablesExported: tablesExportedCount,
    });

    // Determine if we've caught up with present-day data
    const caughtUp = maxTimestamp.getTime() >= uncappedMaxTimestamp.getTime();

    let nextSyncAt: Date;
    if (caughtUp) {
      // Normal mode: schedule for the next frequency period
      nextSyncAt = new Date(maxTimestamp.getTime() + frequencyIntervalMs);
      logger.info({
        message: "[BLOB INTEGRATION] Caught up with exports",
        projectId,
        nextSyncAt: nextSyncAt.toISOString(),
      });
    } else {
      // Catch-up mode: schedule next chunk immediately
      nextSyncAt = new Date();
      logger.info({
        message: "[BLOB INTEGRATION] Still catching up, scheduling next chunk",
        projectId,
        processedUpTo: maxTimestamp.toISOString(),
      });
    }

    // Update integration after successful processing
    await prisma.blobStorageIntegration.update({
      where: {
        projectId,
      },
      data: {
        lastSyncAt: maxTimestamp,
        nextSyncAt,
      },
    });

    // If still catching up, immediately queue the next chunk job
    if (!caughtUp) {
      const queue = BlobStorageIntegrationProcessingQueue.getInstance();
      if (queue) {
        const jobId = `${projectId}-${maxTimestamp.toISOString()}`;
        await queue.add(
          QueueJobs.BlobStorageIntegrationProcessingJob,
          {
            id: randomUUID(),
            name: QueueJobs.BlobStorageIntegrationProcessingJob,
            timestamp: new Date(),
            payload: { projectId },
          },
          { jobId },
        );
        logger.info({
          message: "[BLOB INTEGRATION] Queued next catch-up chunk",
          projectId,
          jobId,
        });
      }
    }

    const jobDurationMs = Date.now() - now.getTime();
    logger.info({
      message:
        "[BLOB INTEGRATION] Successfully processed blob storage integration",
      projectId,
      windowStart: minTimestamp.toISOString(),
      windowEnd: maxTimestamp.toISOString(),
      durationMs: jobDurationMs,
      mode: isCatchingUp ? "catchup" : "steady",
      caughtUp,
    });
    recordHistogram("langfuse.blob_export.job_duration_ms", jobDurationMs, {
      mode: isCatchingUp ? "catchup" : "steady",
    });
  } catch (error) {
    logger.error({
      message: `[BLOB INTEGRATION] Error processing blob storage integration for project ${projectId}`,
      projectId,
      error,
    });
    throw error; // Rethrow to trigger retries
  }
};
