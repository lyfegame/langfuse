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
  getObservationsForBlobStorageExport,
  getTracesForBlobStorageExport,
  getScoresForBlobStorageExport,
  getObservationsForBlobStorageExportParquet,
  getTracesForBlobStorageExportParquet,
  getScoresForBlobStorageExportParquet,
  getEventsForBlobStorageExport,
  getCurrentSpan,
  BlobStorageIntegrationProcessingQueue,
  queryClickhouse,
  QueueJobs,
  recordHistogram,
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
        logger.info(
          `[BLOB INTEGRATION] ClickHouse min_timestamp for project ${projectId}: ${result[0]?.min_timestamp}, type: ${typeof result[0]?.min_timestamp}`,
        );
        const minTimestampValue = Number(result[0]?.min_timestamp);

        if (minTimestampValue && minTimestampValue > 0) {
          const date = new Date(minTimestampValue);
          logger.info(
            `[BLOB INTEGRATION] Created Date from min_timestamp for project ${projectId}: ${date}, isValid: ${!isNaN(date.getTime())}, getTime: ${date.getTime()}`,
          );
          return date;
        }

        // If no data exists, use current time as a fallback
        logger.info(
          `[BLOB INTEGRATION] No historical data found for project ${projectId}, using current time`,
        );
        return new Date(0);
      } catch (error) {
        logger.error(
          `[BLOB INTEGRATION] Error querying ClickHouse for minimum timestamp for project ${projectId}`,
          error,
        );
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

  logger.info(
    `[BLOB INTEGRATION] Processing ${config.table} export for project ${config.projectId}`,
  );

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
            logger.error(
              "[BLOB INTEGRATION] Getting data from DB for blob storage integration failed: ",
              err,
            );
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
  } catch (error) {
    const durationMs = Date.now() - exportStartTime;
    logger.error({
      message: `[BLOB INTEGRATION] Error exporting ${config.table} for project ${config.projectId}`,
      table: config.table,
      projectId: config.projectId,
      durationMs,
      error,
    });
    throw error;
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

  logger.info(
    `[BLOB INTEGRATION] Processing blob storage integration for project ${projectId}`,
  );

  const blobStorageIntegration = await prisma.blobStorageIntegration.findUnique(
    {
      where: {
        projectId,
      },
    },
  );

  if (!blobStorageIntegration) {
    logger.warn(
      `[BLOB INTEGRATION] Blob storage integration not found for project ${projectId}`,
    );
    return;
  }
  if (!blobStorageIntegration.enabled) {
    logger.info(
      `[BLOB INTEGRATION] Blob storage integration is disabled for project ${projectId}`,
    );
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

  logger.info(
    `[BLOB INTEGRATION] Calculated minTimestamp for project ${projectId}: ${minTimestamp}, isValid: ${!isNaN(minTimestamp.getTime())}, getTime: ${minTimestamp.getTime()}, exportMode: ${blobStorageIntegration.exportMode}, lastSyncAt: ${blobStorageIntegration.lastSyncAt}, exportStartDate: ${blobStorageIntegration.exportStartDate}`,
  );

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
    logger.info(
      `[BLOB INTEGRATION] Catch-up mode for project ${projectId}: using ${chunkIntervalMs / 1000}s chunks (lag: ${Math.round((uncappedMaxTimestamp.getTime() - minTimestamp.getTime()) / 3600000)}h)`,
    );
  }

  // Cap maxTimestamp to one chunk ahead of minTimestamp
  // This ensures large historic exports are broken into manageable chunks
  const maxTimestamp = new Date(
    Math.min(
      minTimestamp.getTime() + chunkIntervalMs,
      uncappedMaxTimestamp.getTime(),
    ),
  );

  logger.info(
    `[BLOB INTEGRATION] Calculated maxTimestamp for project ${projectId}: ${maxTimestamp}, isValid: ${!isNaN(maxTimestamp.getTime())}, getTime: ${maxTimestamp.getTime()}, frequencyIntervalMs: ${frequencyIntervalMs}`,
  );

  // Skip export if the time window is empty or invalid
  if (minTimestamp >= maxTimestamp) {
    logger.info(
      `[BLOB INTEGRATION] Skipping export for project ${projectId}: time window is empty (min: ${minTimestamp.toISOString()}, max: ${maxTimestamp.toISOString()})`,
    );
    return;
  }

  try {
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

    if (isTraceOnlyProject) {
      // Only process traces table for projects in the trace-only list (legacy behavior)
      logger.info(
        `[BLOB INTEGRATION] Project ${projectId} is configured for trace-only export via env var, skipping observations, scores, and events`,
      );
      await processBlobStorageExport({ ...executionConfig, table: "traces" });
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
    }

    // Determine if we've caught up with present-day data
    const caughtUp = maxTimestamp.getTime() >= uncappedMaxTimestamp.getTime();

    let nextSyncAt: Date;
    if (caughtUp) {
      // Normal mode: schedule for the next frequency period
      nextSyncAt = new Date(maxTimestamp.getTime() + frequencyIntervalMs);
      logger.info(
        `[BLOB INTEGRATION] Caught up with exports for project ${projectId}. Next sync at ${nextSyncAt.toISOString()}`,
      );
    } else {
      // Catch-up mode: schedule next chunk immediately
      nextSyncAt = new Date();
      logger.info(
        `[BLOB INTEGRATION] Still catching up for project ${projectId}. Scheduling next chunk immediately (processed up to ${maxTimestamp.toISOString()})`,
      );
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
        logger.info(
          `[BLOB INTEGRATION] Queued next catch-up chunk for project ${projectId} with jobId ${jobId}`,
        );
      }
    }

    const jobDurationMs = Date.now() - now.getTime();
    logger.info({
      message: `[BLOB INTEGRATION] Successfully processed blob storage integration for project ${projectId}`,
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
