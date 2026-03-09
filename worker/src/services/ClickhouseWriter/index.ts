import {
  clickhouseClient,
  ClickhouseClientType,
  BlobStorageFileLogInsertType,
  getCurrentSpan,
  ObservationRecordInsertType,
  ObservationBatchStagingRecordInsertType,
  recordGauge,
  recordHistogram,
  recordIncrement,
  ScoreRecordInsertType,
  TraceRecordInsertType,
  TraceNullRecordInsertType,
  DatasetRunItemRecordInsertType,
  EventRecordInsertType,
  ClickhouseWriterDlqQueue,
  QueueJobs,
} from "@langfuse/shared/src/server";

import { env } from "../../env";
import { logger } from "@langfuse/shared/src/server";
import { instrumentAsync } from "@langfuse/shared/src/server";
import { backOff } from "exponential-backoff";

class PartiallyVerifiedWriteError extends Error {
  constructor(
    public readonly originalError: Error,
    public readonly verifiedRecordKeys: string[],
  ) {
    super(originalError.message);
    this.name = "PartiallyVerifiedWriteError";
    Object.setPrototypeOf(this, PartiallyVerifiedWriteError.prototype);
  }
}

export class ClickhouseWriter {
  private static instance: ClickhouseWriter | null = null;
  private static client: ClickhouseClientType | null = null;
  batchSize: number;
  writeInterval: number;
  maxAttempts: number;
  queue: ClickhouseQueue;

  isIntervalFlushInProgress: boolean;
  intervalId: NodeJS.Timeout | null = null;
  private flushChains: Partial<Record<TableName, Promise<void>>> = {};

  private constructor() {
    this.batchSize = env.LANGFUSE_INGESTION_CLICKHOUSE_WRITE_BATCH_SIZE;
    this.writeInterval = env.LANGFUSE_INGESTION_CLICKHOUSE_WRITE_INTERVAL_MS;
    this.maxAttempts = env.LANGFUSE_INGESTION_CLICKHOUSE_MAX_ATTEMPTS;

    this.isIntervalFlushInProgress = false;

    this.queue = {
      [TableName.Traces]: [],
      [TableName.TracesNull]: [],
      [TableName.Scores]: [],
      [TableName.Observations]: [],
      [TableName.ObservationsBatchStaging]: [],
      [TableName.BlobStorageFileLog]: [],
      [TableName.DatasetRunItems]: [],
      [TableName.Events]: [],
    };

    this.start();
  }

  /**
   * Get the singleton instance of ClickhouseWriter.
   * Client parameter is only used for testing.
   */
  public static getInstance(clickhouseClient?: ClickhouseClientType) {
    if (clickhouseClient) {
      ClickhouseWriter.client = clickhouseClient;
    }

    if (!ClickhouseWriter.instance) {
      ClickhouseWriter.instance = new ClickhouseWriter();
    }

    return ClickhouseWriter.instance;
  }

  private start() {
    logger.info(
      `Starting ClickhouseWriter. Max interval: ${this.writeInterval} ms, Max batch size: ${this.batchSize}`,
    );

    this.intervalId = setInterval(() => {
      if (this.isIntervalFlushInProgress) return;

      this.isIntervalFlushInProgress = true;

      logger.debug("Flush interval elapsed, flushing all queues...");

      this.flushAll().finally(() => {
        this.isIntervalFlushInProgress = false;
      });
    }, this.writeInterval);
  }

  public async shutdown(): Promise<void> {
    logger.info("Shutting down ClickhouseWriter...");

    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }

    await this.flushAll(true);

    logger.info("ClickhouseWriter shutdown complete.");
  }

  private async flushAll(fullQueue = false) {
    return instrumentAsync(
      {
        name: "write-to-clickhouse",
      },
      async () => {
        recordIncrement("langfuse.queue.clickhouse_writer.request");
        await Promise.all([
          this.flushTable(TableName.Traces, fullQueue),
          this.flushTable(TableName.TracesNull, fullQueue),
          this.flushTable(TableName.Scores, fullQueue),
          this.flushTable(TableName.Observations, fullQueue),
          this.flushTable(TableName.ObservationsBatchStaging, fullQueue),
          this.flushTable(TableName.BlobStorageFileLog, fullQueue),
          this.flushTable(TableName.DatasetRunItems, fullQueue),
          this.flushTable(TableName.Events, fullQueue),
        ]).catch((err) => {
          logger.error("ClickhouseWriter.flushAll", err);
        });
      },
    );
  }

  private flushTable<T extends TableName>(
    tableName: T,
    fullQueue = false,
  ): Promise<void> {
    const currentFlush = this.flushChains[tableName] ?? Promise.resolve();
    const nextFlush = currentFlush
      .catch(() => undefined)
      .then(() => this.flush(tableName, fullQueue));
    const trackedFlush = nextFlush.finally(() => {
      if (this.flushChains[tableName] === trackedFlush) {
        delete this.flushChains[tableName];
      }
    });
    this.flushChains[tableName] = trackedFlush;
    return trackedFlush;
  }

  private getRetryableErrorType(
    error: unknown,
  ): "network" | "clickhouse_timeout" | "clickhouse_concurrency" | null {
    if (!error || typeof error !== "object") return null;

    const errorMessage = (error as Error).message?.toLowerCase() || "";

    if (
      [
        "socket hang up",
        "econnreset",
        "connection reset",
        "connection terminated",
        "connection closed",
        "read econnreset",
        "write epipe",
        "broken pipe",
      ].some((pattern) => errorMessage.includes(pattern))
    ) {
      return "network";
    }

    if (errorMessage.includes("too many simultaneous queries")) {
      return "clickhouse_concurrency";
    }

    if (errorMessage.includes("timeout error")) {
      return "clickhouse_timeout";
    }

    return null;
  }

  private isRetryableError(error: unknown): boolean {
    return this.getRetryableErrorType(error) !== null;
  }

  private isSizeError(error: unknown): boolean {
    if (!error || typeof error !== "object") return false;

    const errorMessage = (error as Error).message?.toLowerCase() || "";

    return (
      // Check for ClickHouse size errors
      errorMessage.includes("size of json object") &&
      errorMessage.includes("extremely large") &&
      errorMessage.includes("expected not greater than")
    );
  }

  private isStringLengthError(error: unknown): boolean {
    if (!error || typeof error !== "object") return false;

    const errorMessage = (error as Error).message?.toLowerCase() || "";

    // Node.js string size errors
    return errorMessage.includes("invalid string length");
  }

  /**
   * handleStringLength takes the queueItems and splits the queue in half.
   * It returns to lists, one items that are to be retried (first half), and a list that
   * should be re-added to the queue (second half).
   * That way, we should eventually avoid the JS string length error that happens due to the
   * concatenation.
   */
  private handleStringLengthError<T extends TableName>(
    tableName: T,
    queueItems: ClickhouseWriterQueueItem<T>[],
  ): {
    retryItems: ClickhouseWriterQueueItem<T>[];
    requeueItems: ClickhouseWriterQueueItem<T>[];
  } {
    // If batch size is 1, fallback to truncation to prevent infinite loops
    if (queueItems.length === 1) {
      const truncatedRecord = this.truncateOversizedRecord(
        tableName,
        queueItems[0].data,
      );
      logger.warn(
        `String length error with single record for ${tableName}, falling back to truncation`,
        {
          recordId: queueItems[0].data.id,
        },
      );
      return {
        retryItems: [{ ...queueItems[0], data: truncatedRecord }],
        requeueItems: [],
      };
    }

    const splitPoint = Math.floor(queueItems.length / 2);
    const retryItems = queueItems.slice(0, splitPoint);
    const requeueItems = queueItems.slice(splitPoint);

    logger.info(
      `Splitting batch for ${tableName} due to string length error. Retrying ${retryItems.length}, requeueing ${requeueItems.length}`,
    );

    return { retryItems, requeueItems };
  }

  private truncateOversizedRecord<T extends TableName>(
    tableName: T,
    record: RecordInsertType<T>,
  ): RecordInsertType<T> {
    const maxFieldSize = 1024 * 1024; // 1MB per field as safety margin
    const truncationMessage = "[TRUNCATED: Field exceeded size limit]";

    // Helper function to safely truncate string fields
    const truncateField = (value: string | null | undefined): string | null => {
      if (!value) return value || null;
      if (value.length > maxFieldSize) {
        return (
          // Keep the first 500KB and append a truncation message
          value.substring(0, 500 * 1024) + truncationMessage
        );
      }
      return value;
    };

    // Truncate input field if present
    if (
      "input" in record &&
      record.input &&
      record.input.length > maxFieldSize
    ) {
      record.input = truncateField(record.input);
      logger.info(
        `Truncated oversized input field for record ${record.id} of type ${tableName}`,
        {
          projectId: record.project_id,
        },
      );
    }

    // Truncate output field if present
    if (
      "output" in record &&
      record.output &&
      record.output.length > maxFieldSize
    ) {
      record.output = truncateField(record.output);
      logger.info(
        `Truncated oversized output field for record ${record.id} of type ${tableName}`,
        {
          projectId: record.project_id,
        },
      );
    }

    // Truncate metadata field if present
    if ("metadata" in record && record.metadata) {
      const metadata = record.metadata;
      const truncatedMetadata: Record<string, string> = {};
      for (const [key, value] of Object.entries(metadata)) {
        if (value && value.length > maxFieldSize) {
          truncatedMetadata[key] = truncateField(value) || "";
          logger.info(
            `Truncated oversized metadata for record ${record.id} of type ${tableName} and key ${key}`,
            {
              projectId: record.project_id,
            },
          );
        } else {
          truncatedMetadata[key] = value;
        }
      }
      record.metadata = truncatedMetadata;
    }

    return record;
  }

  private async flush<T extends TableName>(tableName: T, fullQueue = false) {
    const entityQueue = this.queue[tableName];
    if (entityQueue.length === 0) return;

    let queueItems = entityQueue.splice(
      0,
      fullQueue ? entityQueue.length : this.batchSize,
    );

    // Log wait time
    queueItems.forEach((item) => {
      const waitTime = Date.now() - item.createdAt;
      recordHistogram("langfuse.queue.clickhouse_writer.wait_time", waitTime, {
        unit: "milliseconds",
      });
    });

    const currentSpan = getCurrentSpan();
    if (currentSpan) {
      currentSpan.setAttributes({
        [`${tableName}-length`]: queueItems.length,
      });
    }

    try {
      const processingStartTime = Date.now();

      let recordsToWrite = queueItems.map((item) => item.data);
      let hasBeenTruncated = false;

      await backOff(
        async () =>
          this.writeToClickhouse({
            table: tableName,
            records: recordsToWrite,
          }),
        {
          numOfAttempts: env.LANGFUSE_INGESTION_CLICKHOUSE_MAX_ATTEMPTS,
          retry: (error: Error, attemptNumber: number) => {
            if (error instanceof PartiallyVerifiedWriteError) {
              const verifiedRecordKeySet = new Set(error.verifiedRecordKeys);
              const verifiedItems = queueItems.filter((item) =>
                verifiedRecordKeySet.has(
                  this.getRecordVerificationKey(tableName, item.data),
                ),
              );
              const remainingItems = queueItems.filter(
                (item) =>
                  !verifiedRecordKeySet.has(
                    this.getRecordVerificationKey(tableName, item.data),
                  ),
              );

              verifiedItems.forEach((item) => item.resolve?.());
              queueItems = remainingItems;
              recordsToWrite = remainingItems.map((item) => item.data);

              logger.warn(
                `ClickHouse Writer verified ${verifiedItems.length} ${tableName} record(s) after an ambiguous write error and will retry ${remainingItems.length} unresolved record(s)`,
                {
                  attemptNumber,
                  verifiedCount: verifiedItems.length,
                  retryCount: remainingItems.length,
                },
              );
              currentSpan?.addEvent("clickhouse-query-verified-subset", {
                "retry.attempt": attemptNumber,
                "write.verified_count": verifiedItems.length,
                "write.retry_count": remainingItems.length,
              });
              return remainingItems.length > 0;
            }

            const retryableErrorType = this.getRetryableErrorType(error);
            const isRetryable = retryableErrorType !== null;
            const isSizeError = this.isSizeError(error);
            const isStringLengthError = this.isStringLengthError(error);

            if (isRetryable) {
              logger.warn(
                `ClickHouse Writer failed with retryable error for ${tableName} (attempt ${attemptNumber}/${env.LANGFUSE_INGESTION_CLICKHOUSE_MAX_ATTEMPTS}): ${error.message}`,
                {
                  error: error.message,
                  attemptNumber,
                  retryableErrorType,
                },
              );
              currentSpan?.addEvent("clickhouse-query-retry", {
                "retry.attempt": attemptNumber,
                "retry.error": error.message,
                "retry.error_type": retryableErrorType,
              });
              return true;
            } else if (isStringLengthError) {
              logger.warn(
                `ClickHouse Writer failed with string length error for ${tableName} (attempt ${attemptNumber}/${env.LANGFUSE_INGESTION_CLICKHOUSE_MAX_ATTEMPTS}): Splitting batch and retrying`,
                {
                  error: error.message,
                  attemptNumber,
                  batchSize: queueItems.length,
                },
              );

              const { retryItems, requeueItems } = this.handleStringLengthError(
                tableName,
                queueItems,
              );

              // Update records to write with only the retry items
              recordsToWrite = retryItems.map((item) => item.data);
              queueItems = retryItems;

              // Prepend requeue items to the front of the queue to maintain order as much as possible with parallel execution.
              if (requeueItems.length > 0) {
                entityQueue.unshift(...requeueItems);
              }

              currentSpan?.addEvent("clickhouse-query-split-retry", {
                "retry.attempt": attemptNumber,
                "retry.error": error.message,
                "split.retry_count": retryItems.length,
                "split.requeue_count": requeueItems.length,
              });
              return true;
            } else if (isSizeError && !hasBeenTruncated) {
              logger.warn(
                `ClickHouse Writer failed with size error for ${tableName} (attempt ${attemptNumber}/${env.LANGFUSE_INGESTION_CLICKHOUSE_MAX_ATTEMPTS}): Truncating oversized records and retrying`,
                {
                  error: error.message,
                  attemptNumber,
                },
              );

              // Truncate oversized records
              recordsToWrite = recordsToWrite.map((record) =>
                this.truncateOversizedRecord(tableName, record),
              );
              hasBeenTruncated = true;

              currentSpan?.addEvent("clickhouse-query-truncate-retry", {
                "retry.attempt": attemptNumber,
                "retry.error": error.message,
                truncated: true,
              });
              return true;
            } else {
              logger.error(
                `ClickHouse query failed with non-retryable error: ${error.message}`,
                {
                  error: error.message,
                },
              );
              return false;
            }
          },
          startingDelay:
            env.LANGFUSE_INGESTION_CLICKHOUSE_RETRY_INITIAL_DELAY_MS,
          timeMultiple: env.LANGFUSE_INGESTION_CLICKHOUSE_RETRY_TIME_MULTIPLE,
          maxDelay: env.LANGFUSE_INGESTION_CLICKHOUSE_RETRY_MAX_DELAY_MS,
        },
      );

      queueItems.forEach((item) => item.resolve?.());

      // Log processing time
      recordHistogram(
        "langfuse.queue.clickhouse_writer.processing_time",
        Date.now() - processingStartTime,
        {
          unit: "milliseconds",
        },
      );

      logger.debug(
        `Flushed ${queueItems.length} records to Clickhouse ${tableName}. New queue length: ${entityQueue.length}`,
      );

      recordGauge(
        "ingestion_clickhouse_insert_queue_length",
        entityQueue.length,
        {
          unit: "records",
          entityType: tableName,
        },
      );
    } catch (err) {
      logger.error(`ClickhouseWriter.flush ${tableName}`, err);
      const flushError = err instanceof Error ? err : new Error(String(err));

      const dlqItems: ClickhouseWriterQueueItem<T>[] = [];
      queueItems.forEach((item) => {
        if (item.reject) {
          item.reject(flushError);
          return;
        }

        if (item.attempts < this.maxAttempts) {
          entityQueue.push({
            ...item,
            attempts: item.attempts + 1,
          });
        } else {
          recordIncrement("langfuse.queue.clickhouse_writer.error");
          dlqItems.push(item);
        }
      });

      if (dlqItems.length > 0) {
        await this.enqueueFailedItemsToDlq(tableName, dlqItems, flushError);
      }
    }
  }

  public addToQueue<T extends TableName>(
    tableName: T,
    data: RecordInsertType<T>,
  ) {
    this.enqueueItem(tableName, data);
  }

  public addToQueueAndWait<T extends TableName>(
    tableName: T,
    data: RecordInsertType<T>,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.enqueueItem(tableName, data, { resolve, reject });
    });
  }

  private enqueueItem<T extends TableName>(
    tableName: T,
    data: RecordInsertType<T>,
    callbacks?: Pick<ClickhouseWriterQueueItem<T>, "resolve" | "reject">,
  ) {
    const entityQueue = this.queue[tableName];
    entityQueue.push({
      createdAt: Date.now(),
      attempts: 1,
      data,
      ...callbacks,
    });

    if (entityQueue.length >= this.batchSize) {
      logger.debug(`Queue is full. Flushing ${tableName}...`);

      this.flushTable(tableName).catch((err) => {
        logger.error("ClickhouseWriter.addToQueue flush", err);
      });
    }
  }

  private async enqueueFailedItemsToDlq<T extends TableName>(
    tableName: T,
    queueItems: ClickhouseWriterQueueItem<T>[],
    error: Error,
  ): Promise<void> {
    const dlqQueue = ClickhouseWriterDlqQueue.getInstance();

    if (!dlqQueue) {
      logger.error(
        `ClickhouseWriter: Max attempts reached and DLQ queue is unavailable for ${queueItems.length} ${tableName} record(s)`,
      );
      return;
    }

    const enqueueResults = await Promise.allSettled(
      queueItems.map((item) =>
        dlqQueue.add(
          QueueJobs.ClickhouseWriterDlqJob,
          {
            timestamp: new Date(),
            id: `${tableName}:${this.getRecordVerificationKey(tableName, item.data)}`,
            payload: {
              tableName,
              projectId: this.getRecordProjectId(item.data),
              record: item.data as Record<string, unknown>,
              errorMessage: error.message,
              originalAttempts: item.attempts,
              originalCreatedAt: new Date(item.createdAt),
              verificationKey: this.getRecordVerificationKey(
                tableName,
                item.data,
              ),
            },
            name: QueueJobs.ClickhouseWriterDlqJob,
          },
          {
            jobId: `${tableName}:${this.getRecordVerificationKey(tableName, item.data)}:${item.createdAt}`,
          },
        ),
      ),
    );

    const failedEnqueues = enqueueResults.filter(
      (result): result is PromiseRejectedResult => result.status === "rejected",
    );

    enqueueResults.forEach((result) => {
      if (result.status === "fulfilled") {
        recordIncrement("langfuse.queue.clickhouse_writer.dlq_enqueued");
      }
    });

    logger.error(
      `ClickhouseWriter: Max attempts reached, enqueued ${queueItems.length - failedEnqueues.length}/${queueItems.length} ${tableName} record(s) to DLQ`,
      failedEnqueues.length > 0
        ? {
            dlqEnqueueFailures: failedEnqueues.map((failed) =>
              failed.reason instanceof Error
                ? failed.reason.message
                : String(failed.reason),
            ),
          }
        : undefined,
    );
  }

  private getRecordProjectId<T extends TableName>(
    record: RecordInsertType<T>,
  ): string | undefined {
    const projectId = (record as { project_id?: unknown }).project_id;
    return typeof projectId === "string" ? projectId : undefined;
  }

  private getVerificationField(tableName: TableName): "id" | "event_id" {
    return tableName === TableName.BlobStorageFileLog ? "event_id" : "id";
  }

  private getRecordVerificationKey<T extends TableName>(
    tableName: T,
    record: RecordInsertType<T>,
  ): string {
    return this.getVerificationField(tableName) === "event_id"
      ? String((record as BlobStorageFileLogInsertType).event_id)
      : String(record.id);
  }

  private getExpectedEventTimestampMs<T extends TableName>(
    record: RecordInsertType<T>,
  ): number {
    const value = (record as { event_ts?: unknown }).event_ts;
    if (typeof value === "number") return value;
    if (typeof value === "string") {
      const numericValue = Number(value);
      if (Number.isFinite(numericValue)) return numericValue;
      const parsedDate = new Date(value).getTime();
      return Number.isFinite(parsedDate) ? parsedDate : 0;
    }
    if (value instanceof Date) return value.getTime();
    return 0;
  }

  private shouldVerifyAfterError(error: Error): boolean {
    const retryableErrorType = this.getRetryableErrorType(error);
    return (
      retryableErrorType === "network" ||
      retryableErrorType === "clickhouse_timeout"
    );
  }

  private async getVerifiedRecordKeys<T extends TableName>(params: {
    table: T;
    records: RecordInsertType<T>[];
  }): Promise<string[]> {
    if (params.records.length === 0) return [];

    const verificationField = this.getVerificationField(params.table);
    const client =
      ClickhouseWriter.client ??
      clickhouseClient({
        request_timeout: env.LANGFUSE_INGESTION_CLICKHOUSE_REQUEST_TIMEOUT_MS,
      });

    const recordsByProject = new Map<string, RecordInsertType<T>[]>();
    params.records.forEach((record) => {
      const projectRecords = recordsByProject.get(record.project_id) ?? [];
      projectRecords.push(record);
      recordsByProject.set(record.project_id, projectRecords);
    });

    const verifiedKeys = new Set<string>();

    for (const [projectId, projectRecords] of recordsByProject.entries()) {
      const expectedEventTimestampMsByKey = new Map(
        projectRecords.map((record) => [
          this.getRecordVerificationKey(params.table, record),
          this.getExpectedEventTimestampMs(record),
        ]),
      );
      const recordKeys = [...expectedEventTimestampMsByKey.keys()];

      const queryResult = await client.query({
        query: `
          SELECT
            ${verificationField} AS verification_key,
            toUnixTimestamp64Milli(max(event_ts)) AS max_event_ts_ms
          FROM ${params.table}
          WHERE project_id = {projectId: String}
            AND ${verificationField} IN {recordKeys: Array(String)}
          GROUP BY verification_key
          SETTINGS use_query_cache = false
        `,
        format: "JSONEachRow",
        query_params: {
          projectId,
          recordKeys,
        },
        clickhouse_settings: {
          log_comment: JSON.stringify({
            feature: "ingestion",
            type: params.table,
            operation_name: "verifyWriteToClickhouse",
            projectId,
          }),
        },
      });

      const rows = (await queryResult.json()) as Array<{
        verification_key: string;
        max_event_ts_ms: number | string;
      }>;

      rows.forEach((row) => {
        const expectedEventTimestampMs = expectedEventTimestampMsByKey.get(
          row.verification_key,
        );
        if (expectedEventTimestampMs === undefined) return;

        if (Number(row.max_event_ts_ms) >= expectedEventTimestampMs) {
          verifiedKeys.add(row.verification_key);
        }
      });
    }

    return [...verifiedKeys];
  }

  private async writeToClickhouse<T extends TableName>(params: {
    table: T;
    records: RecordInsertType<T>[];
  }): Promise<void> {
    if (params.records.length === 0) return;

    const startTime = Date.now();
    const client =
      ClickhouseWriter.client ??
      clickhouseClient({
        request_timeout: env.LANGFUSE_INGESTION_CLICKHOUSE_REQUEST_TIMEOUT_MS,
      });

    try {
      await client.insert({
        table: params.table,
        format: "JSONEachRow",
        values: params.records,
        clickhouse_settings: {
          log_comment: JSON.stringify({
            feature: "ingestion",
            type: params.table,
            operation_name: "writeToClickhouse",
            projectId:
              params.records.length > 0
                ? params.records[0].project_id
                : undefined,
          }),
        },
      });
    } catch (err) {
      const writeError = err instanceof Error ? err : new Error(String(err));

      if (this.shouldVerifyAfterError(writeError)) {
        const verifiedRecordKeys = await this.getVerifiedRecordKeys(params);
        if (verifiedRecordKeys.length === params.records.length) {
          logger.warn(
            `ClickhouseWriter.writeToClickhouse recovered ${params.table} write after ambiguous error by verifying durable materialization`,
            {
              error: writeError.message,
              verifiedCount: verifiedRecordKeys.length,
            },
          );
          return;
        }

        if (verifiedRecordKeys.length > 0) {
          throw new PartiallyVerifiedWriteError(writeError, verifiedRecordKeys);
        }
      }

      logger.error(`ClickhouseWriter.writeToClickhouse ${writeError}`);
      throw writeError;
    }

    logger.debug(
      `ClickhouseWriter.writeToClickhouse: ${Date.now() - startTime} ms`,
    );

    recordGauge("ingestion_clickhouse_insert", params.records.length);
  }
}

export enum TableName {
  Traces = "traces",
  TracesNull = "traces_null",
  Scores = "scores",
  Observations = "observations",
  ObservationsBatchStaging = "observations_batch_staging",
  BlobStorageFileLog = "blob_storage_file_log",
  DatasetRunItems = "dataset_run_items_rmt",
  Events = "events",
}

type RecordInsertType<T extends TableName> = T extends TableName.Scores
  ? ScoreRecordInsertType
  : T extends TableName.Observations
    ? ObservationRecordInsertType
    : T extends TableName.ObservationsBatchStaging
      ? ObservationBatchStagingRecordInsertType
      : T extends TableName.Traces
        ? TraceRecordInsertType
        : T extends TableName.TracesNull
          ? TraceNullRecordInsertType
          : T extends TableName.BlobStorageFileLog
            ? BlobStorageFileLogInsertType
            : T extends TableName.DatasetRunItems
              ? DatasetRunItemRecordInsertType
              : T extends TableName.Events
                ? EventRecordInsertType
                : never;

type ClickhouseQueue = {
  [T in TableName]: ClickhouseWriterQueueItem<T>[];
};

type ClickhouseWriterQueueItem<T extends TableName> = {
  createdAt: number;
  attempts: number;
  data: RecordInsertType<T>;
  resolve?: () => void;
  reject?: (error: Error) => void;
};
