import { randomUUID } from "crypto";
import { Job, Processor } from "bullmq";
import {
  clickhouseClient,
  createIngestionEventSchema,
  getClickhouseEntityType,
  getCurrentSpan,
  getS3EventStorageClient,
  type IngestionEventType,
  type ObservationEvent,
  logger,
  type TraceEventType,
  OtelIngestionProcessor,
  processEventBatch,
  QueueName,
  recordDistribution,
  recordHistogram,
  recordIncrement,
  redis,
  TQueueJobTypes,
  traceException,
  compareVersions,
  ResourceSpan,
} from "@langfuse/shared/src/server";
import {
  applyIngestionMasking,
  isIngestionMaskingEnabled,
} from "@langfuse/shared/src/server/ee/ingestionMasking";
import { env } from "../env";
import { IngestionService } from "../services/IngestionService";
import { prisma } from "@langfuse/shared/src/db";
import { ClickhouseWriter } from "../services/ClickhouseWriter";
import {
  ForbiddenError,
  convertEventRecordToObservationForEval,
} from "@langfuse/shared";
import {
  fetchObservationEvalConfigs,
  scheduleObservationEvals,
  createObservationEvalSchedulerDeps,
} from "../features/evaluation/observationEval";

/**
 * Check if HTTP headers from the SDK request indicate the batch is eligible
 * for direct event writes.
 *
 * Requirements:
 * - x-langfuse-sdk-name "python" with x-langfuse-sdk-version >= 4.0.0
 * - x-langfuse-sdk-name "javascript" with x-langfuse-sdk-version >= 5.0.0
 * - x-langfuse-ingestion-version === "4" (custom OTel exporter opt-in)
 */
export function checkHeaderBasedDirectWrite(params: {
  sdkName?: string;
  sdkVersion?: string;
  ingestionVersion?: string;
}): boolean {
  const { sdkName, sdkVersion, ingestionVersion } = params;

  // Check x-langfuse-ingestion-version (>= 4 means direct write eligible).
  // Values > 4 are rejected at the API route, so anything reaching here is valid.
  const parsed = ingestionVersion ? parseInt(ingestionVersion, 10) : NaN;
  if (!isNaN(parsed) && parsed >= 4) {
    return true;
  }

  // Check Langfuse SDK name + version
  if (!sdkName || !sdkVersion) {
    return false;
  }

  try {
    // compareVersions returns null when current >= minimum (no update needed).
    // Strip pre-release/build metadata so that e.g. 4.0.0-rc.1 qualifies as 4.0.0.
    const baseVersion = sdkVersion.split(/[-+]/)[0];

    if (sdkName === "python") {
      return compareVersions(baseVersion, "v4.0.0") === null;
    }

    if (sdkName === "javascript") {
      return compareVersions(baseVersion, "v5.0.0") === null;
    }
  } catch {
    logger.warn(
      `Failed to parse SDK version from headers: ${sdkName}@${sdkVersion}`,
    );
  }

  return false;
}

/**
 * SDK information extracted from OTEL resourceSpans.
 */
export type SdkInfo = {
  scopeName: string | null;
  scopeVersion: string | null;
  telemetrySdkLanguage: string | null;
};

/**
 * Extract SDK information from resourceSpans.
 * Gets scope name/version and telemetry SDK language from the OTEL structure.
 */
export function getSdkInfoFromResourceSpans(
  resourceSpans: ResourceSpan,
): SdkInfo {
  try {
    // Get the first scopeSpan (all spans in a batch share the same scope)
    const firstScopeSpan = resourceSpans?.scopeSpans?.[0];
    const scopeName = firstScopeSpan?.scope?.name ?? null;
    const scopeVersion = firstScopeSpan?.scope?.version ?? null;

    // Extract telemetry SDK language from resource attributes
    const resourceAttributes = resourceSpans?.resource?.attributes ?? [];
    const telemetrySdkLanguage =
      resourceAttributes.find((attr) => attr.key === "telemetry.sdk.language")
        ?.value?.stringValue ?? null;

    return { scopeName, scopeVersion, telemetrySdkLanguage };
  } catch (error) {
    logger.warn("Failed to extract SDK info from resourceSpans", error);
    return { scopeName: null, scopeVersion: null, telemetrySdkLanguage: null };
  }
}

type TraceCreateEvent = Extract<IngestionEventType, { type: "trace-create" }>;

const DEFAULT_TRACE_ENVIRONMENT = "default";

function getObservationMetadataRecord(
  metadata: unknown,
): Record<string, unknown> | undefined {
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return undefined;
  }

  return metadata as Record<string, unknown>;
}

function getMetadataStringValue(
  metadata: Record<string, unknown> | undefined,
  key: string,
): string | undefined {
  const value = metadata?.[key];
  return typeof value === "string" && value.trim().length > 0
    ? value
    : undefined;
}

function getMetadataTags(
  metadata: Record<string, unknown> | undefined,
): string[] | undefined {
  const raw = metadata?.langfuse_tags;

  const normalize = (candidate: unknown): string[] | undefined => {
    if (!Array.isArray(candidate)) {
      return undefined;
    }

    const tags = candidate.filter(
      (value): value is string =>
        typeof value === "string" && value.trim().length > 0,
    );

    return tags.length > 0 ? Array.from(new Set(tags)) : undefined;
  };

  const fromArray = normalize(raw);
  if (fromArray) {
    return fromArray;
  }

  if (typeof raw === "string") {
    try {
      return normalize(JSON.parse(raw));
    } catch {
      return undefined;
    }
  }

  return undefined;
}

export function synthesizeMissingTraceCreateEvents(params: {
  observations: IngestionEventType[];
  traces: IngestionEventType[];
}): TraceCreateEvent[] {
  const existingTraceIds = new Set(
    params.traces
      .filter(
        (event): event is TraceCreateEvent => event.type === "trace-create",
      )
      .map((event) => event.body.id)
      .filter((traceId): traceId is string => Boolean(traceId)),
  );

  const synthesized = new Map<string, TraceCreateEvent>();

  for (const event of params.observations) {
    if (getClickhouseEntityType(event.type) !== "observation") {
      continue;
    }

    const body = event.body as {
      traceId?: string | null;
      startTime?: string | null;
      environment?: string | null;
      metadata?: unknown;
    };
    const traceId = body.traceId;

    if (!traceId || existingTraceIds.has(traceId) || synthesized.has(traceId)) {
      continue;
    }

    const metadata = getObservationMetadataRecord(body.metadata);
    const langfuseSessionId = getMetadataStringValue(
      metadata,
      "langfuse_session_id",
    );
    const threadId = getMetadataStringValue(metadata, "thread_id");
    const userId = getMetadataStringValue(metadata, "langfuse_user_id");
    const tags = getMetadataTags(metadata);

    const promotedMetadata = Object.fromEntries(
      [
        ["langfuse_session_id", langfuseSessionId],
        ["thread_id", threadId],
        ["langfuse_user_id", userId],
      ].filter((entry): entry is [string, string] => Boolean(entry[1])),
    );

    const traceTimestamp = body.startTime ?? event.timestamp;

    synthesized.set(traceId, {
      id: randomUUID(),
      type: "trace-create",
      timestamp: traceTimestamp,
      body: {
        id: traceId,
        timestamp: traceTimestamp,
        environment: body.environment ?? DEFAULT_TRACE_ENVIRONMENT,
        sessionId: langfuseSessionId ?? threadId,
        userId,
        tags,
        metadata:
          Object.keys(promotedMetadata).length > 0
            ? promotedMetadata
            : undefined,
      },
    });
  }

  return Array.from(synthesized.values());
}

type EventWithBodyId = { body: { id?: string | null } };

function groupEventsByEntityId<T extends EventWithBodyId>(
  events: T[],
): Map<string, T[]> {
  const grouped = new Map<string, T[]>();

  for (const event of events) {
    const entityId = event.body.id;
    if (!entityId) continue;

    const existing = grouped.get(entityId);
    if (existing) {
      existing.push(event);
    } else {
      grouped.set(entityId, [event]);
    }
  }

  return grouped;
}

export async function writeInlineOtelEntities(params: {
  ingestionService: Pick<IngestionService, "mergeAndWrite">;
  clickhouseWriter: Pick<ClickhouseWriter, "createCommitGroup" | "commitGroup">;
  projectId: string;
  traceEvents: TraceEventType[];
  observations: ObservationEvent[];
  forwardToEventsTable: boolean;
  createdAtTimestamp?: Date;
}): Promise<void> {
  const {
    ingestionService,
    clickhouseWriter,
    projectId,
    traceEvents,
    observations,
    forwardToEventsTable,
  } = params;

  if (traceEvents.length === 0 && observations.length === 0) {
    return;
  }

  const commitGroup = clickhouseWriter.createCommitGroup();
  const postCommitActions: Array<() => Promise<void>> = [];
  const createdAtTimestamp = params.createdAtTimestamp ?? new Date();
  const clickhouseWriteContext = {
    commitGroup,
    postCommitActions,
  };

  const inlineTraceWrites = Array.from(groupEventsByEntityId(traceEvents)).map(
    ([traceId, groupedTraceEvents]) =>
      ingestionService.mergeAndWrite(
        "trace",
        projectId,
        traceId,
        createdAtTimestamp,
        groupedTraceEvents,
        forwardToEventsTable,
        clickhouseWriteContext,
      ),
  );

  const inlineObservationWrites = Array.from(
    groupEventsByEntityId(observations),
  ).map(([observationId, groupedObservationEvents]) =>
    ingestionService.mergeAndWrite(
      "observation",
      projectId,
      observationId,
      createdAtTimestamp,
      groupedObservationEvents,
      forwardToEventsTable,
      clickhouseWriteContext,
    ),
  );

  await Promise.all([...inlineTraceWrites, ...inlineObservationWrites]);
  await clickhouseWriter.commitGroup(commitGroup);
  await Promise.all(postCommitActions.map((action) => action()));
}

/**
 * Check if SDK meets version requirements for direct event writes.
 *
 * Requirements:
 * - Scope name must contain 'langfuse' (case-insensitive)
 * - Python SDK: scope_version >= 3.9.0
 * - JS/JavaScript SDK: scope_version >= 4.4.0
 */
export function checkSdkVersionRequirements(
  sdkInfo: SdkInfo,
  isSdkExperimentBatch: boolean,
): boolean {
  const { scopeName, scopeVersion, telemetrySdkLanguage } = sdkInfo;

  // Must be a Langfuse SDK
  if (!scopeName || !String(scopeName).toLowerCase().includes("langfuse")) {
    return false;
  }

  if (!scopeVersion || !telemetrySdkLanguage) {
    return false;
  }

  try {
    // Python SDK >= 3.9.0
    if (telemetrySdkLanguage === "python" && isSdkExperimentBatch) {
      const comparison = compareVersions(scopeVersion, "v3.9.0");
      return comparison === null; // null means current >= latest
    }

    // JS/JavaScript SDK >= 4.4.0
    if (
      (telemetrySdkLanguage === "js" ||
        telemetrySdkLanguage === "javascript") &&
      isSdkExperimentBatch
    ) {
      const comparison = compareVersions(scopeVersion, "v4.4.0");
      return comparison === null; // null means current >= latest
    }

    return false;
  } catch (error) {
    logger.warn(
      `Failed to parse SDK version ${scopeVersion} for language ${telemetrySdkLanguage}`,
      error,
    );
    return false;
  }
}

export const otelIngestionQueueProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.OtelIngestionQueue]>,
): Promise<void> => {
  try {
    const projectId = job.data.payload.authCheck.scope.projectId;
    const publicKey = job.data.payload.data.publicKey;
    const fileKey = job.data.payload.data.fileKey;
    const auth = job.data.payload.authCheck;

    const span = getCurrentSpan();
    if (span) {
      span.setAttribute("messaging.bullmq.job.input.id", job.data.id);
      span.setAttribute(
        "messaging.bullmq.job.input.projectId",
        job.data.payload.authCheck.scope.projectId,
      );
      span.setAttribute(
        "messaging.bullmq.job.input.fileKey",
        job.data.payload.data.fileKey,
      );
    }
    logger.debug(`Processing ${fileKey} for project ${projectId}`);

    // TODO: Do we need to add these files into the blob_storage_file_log?
    // We could recommend lifecycle rules due to the immutability properties.
    // Otherwise, we'd probably have to upsert one row per generated event further below.
    // Easy change, but needs alignment.

    // Download file from blob storage
    const resourceSpans = await getS3EventStorageClient(
      env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
    ).download(fileKey);

    recordHistogram(
      "langfuse.ingestion.s3_file_size_bytes",
      resourceSpans.length, // At this point it's still a string.
      {
        skippedS3List: "true",
        otel: "true",
      },
    );

    // Parse spans from S3 download
    let parsedSpans = JSON.parse(resourceSpans);

    // Apply ingestion masking if enabled (EE feature)
    if (isIngestionMaskingEnabled()) {
      const maskingResult = await applyIngestionMasking({
        data: parsedSpans,
        projectId,
        orgId: job.data.payload.authCheck.scope.orgId,
        propagatedHeaders: job.data.payload.propagatedHeaders,
      });

      if (!maskingResult.success) {
        // Fail-closed: drop event
        logger.warn(`Dropping OTEL event due to masking failure`, {
          projectId,
          error: maskingResult.error,
        });
        return;
      }
      parsedSpans = maskingResult.data;
    }

    // Generate events via OtelIngestionProcessor
    const processor = new OtelIngestionProcessor({
      projectId,
      publicKey,
    });
    const events: IngestionEventType[] =
      await processor.processToIngestionEvents(parsedSpans);
    // Here, we split the events into observations and non-observations.
    // Observations and trace events stay on the inline ingestion path, whereas
    // other non-observation events make another run through processEventBatch.
    const nonObservationEvents = events.filter(
      (e) => getClickhouseEntityType(e.type) !== "observation",
    );
    const inlineTraceEvents: TraceEventType[] = nonObservationEvents.filter(
      (e): e is TraceEventType => getClickhouseEntityType(e.type) === "trace",
    );
    const queuedNonObservationEvents = nonObservationEvents.filter(
      (e) => getClickhouseEntityType(e.type) !== "trace",
    );

    // We need to parse each incoming observation through our ingestion schema to make use of its included transformations.
    const ingestionSchema = createIngestionEventSchema();
    const observations: ObservationEvent[] = events
      .filter((e) => getClickhouseEntityType(e.type) === "observation")
      .map((o) => ingestionSchema.safeParse(o))
      .flatMap((o) => {
        if (!o.success) {
          logger.warn(
            `Failed to parse otel observation for project ${projectId} in ${fileKey}: ${o.error}`,
            o.error,
          );
          return [];
        }

        if (getClickhouseEntityType(o.data.type) !== "observation") {
          return [];
        }

        return [o.data as ObservationEvent];
      });

    const syntheticTraceCreateEvents = synthesizeMissingTraceCreateEvents({
      observations,
      traces: inlineTraceEvents,
    });
    if (syntheticTraceCreateEvents.length > 0) {
      inlineTraceEvents.push(...syntheticTraceCreateEvents);
      logger.info(
        `Synthesized ${syntheticTraceCreateEvents.length} shallow trace-create events for project ${projectId} in ${fileKey}`,
      );
      recordDistribution(
        "langfuse.ingestion.otel.synthetic_trace_create_count",
        syntheticTraceCreateEvents.length,
      );
      span?.setAttribute(
        "langfuse.ingestion.otel.synthetic_trace_create_count",
        syntheticTraceCreateEvents.length,
      );
    }

    recordIncrement("langfuse.ingestion.event", observations.length, {
      source: "otel",
    });
    recordDistribution(
      "langfuse.ingestion.otel.trace_count",
      inlineTraceEvents.length,
    );
    recordDistribution(
      "langfuse.ingestion.otel.observation_count",
      observations.length,
    );
    span?.setAttribute(
      "langfuse.ingestion.otel.trace_count",
      inlineTraceEvents.length,
    );
    span?.setAttribute(
      "langfuse.ingestion.otel.observation_count",
      observations.length,
    );

    // Ensure required infra config is present
    if (!redis) throw new Error("Redis not available");
    if (!prisma) throw new Error("Prisma not available");

    const clickhouseWriter = ClickhouseWriter.getInstance();
    const ingestionService = new IngestionService(
      redis,
      prisma,
      clickhouseWriter,
      clickhouseClient(),
    );

    // Decide whether observations should be processed via new flow (directly to events table)
    // or via the dual write (staging table and batch job to events).
    //
    // Priority 1: HTTP headers from the SDK request (batch-level decision).
    //   - x-langfuse-sdk-name/version: Python >= 4.0.0 or JS >= 5.0.0
    //   - x-langfuse-ingestion-version: "4" (custom OTel exporter opt-in)
    //   When headers qualify, ALL spans in the batch (including third-party scoped) use direct write.
    //
    // Priority 2 (fallback): Per-span OTEL scope inspection (legacy).
    //   - scope.name contains "langfuse", sdk-experiment environment, Python >= 3.9.0 or JS >= 4.4.0
    const headerBasedDirectWrite = checkHeaderBasedDirectWrite({
      sdkName: job.data.payload.sdkName,
      sdkVersion: job.data.payload.sdkVersion,
      ingestionVersion: job.data.payload.ingestionVersion,
    });

    let useDirectEventWrite = headerBasedDirectWrite;

    if (!useDirectEventWrite) {
      const hasExperimentEnvironment = observations.some((o) => {
        const body = o.body as { environment?: string };
        return body.environment === "sdk-experiment";
      });
      const sdkInfo =
        parsedSpans.length > 0
          ? getSdkInfoFromResourceSpans(parsedSpans[0])
          : {
              scopeName: null,
              scopeVersion: null,
              telemetrySdkLanguage: null,
            };
      useDirectEventWrite = checkSdkVersionRequirements(
        sdkInfo,
        hasExperimentEnvironment,
      );
    }

    const writePath = useDirectEventWrite
      ? headerBasedDirectWrite
        ? "direct_header"
        : "direct_scope"
      : "dual";
    span?.setAttribute("langfuse.ingestion.otel.write_path", writePath);
    recordIncrement("langfuse.ingestion.otel.write_path", 1, {
      path: writePath,
    });

    const shouldForwardToEventsTable =
      !useDirectEventWrite &&
      env.LANGFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE === "true" &&
      env.QUEUE_CONSUMER_EVENT_PROPAGATION_QUEUE_IS_ENABLED === "true" &&
      env.LANGFUSE_EXPERIMENT_EARLY_EXIT_EVENT_BATCH_JOB !== "true";

    // Keep observation and trace materialization on the same inline durable
    // path so shallow trace rows do not lag behind a backlogged ingestion queue.
    const inlineEntityWritePromise = writeInlineOtelEntities({
      ingestionService,
      clickhouseWriter,
      projectId: auth.scope.projectId,
      traceEvents: inlineTraceEvents,
      observations,
      forwardToEventsTable: shouldForwardToEventsTable,
      createdAtTimestamp: new Date(),
    });

    const queuedNonObservationWritePromise =
      queuedNonObservationEvents.length > 0
        ? processEventBatch(queuedNonObservationEvents, auth, {
            delay: 0,
            source: "otel",
            forwardToEventsTable: shouldForwardToEventsTable,
          })
        : Promise.resolve();

    await Promise.all([
      inlineEntityWritePromise,
      queuedNonObservationWritePromise,
    ]);

    // Process events for observation evals and direct event writes
    // This phase handles two independent concerns:
    // 1. Scheduling observation-level evals (if eval configs exist)
    // 2. Writing directly to events table (if SDK version requirements are met)
    //
    // Both require enriched event records with trace-level attributes
    // (userId, sessionId, tags, release) that processToEvent provides.
    const eventInputs = processor.processToEvent(parsedSpans);

    if (eventInputs.length === 0) {
      return;
    }

    // Determine what processing is needed
    const shouldWriteToEventsTable =
      env.LANGFUSE_EXPERIMENT_INSERT_INTO_EVENTS_TABLE === "true" &&
      useDirectEventWrite;

    const evalConfigs = await fetchObservationEvalConfigs(projectId).catch(
      (error) => {
        traceException(error);
        logger.warn(
          `Failed to fetch observation eval configs for project ${projectId}`,
          error,
        );

        return [];
      },
    );
    const hasEvalConfigs = evalConfigs.length > 0;

    // Early exit if no processing needed
    if (!hasEvalConfigs && !shouldWriteToEventsTable) {
      return;
    }

    // Create scheduler deps only if we have eval configs
    const evalSchedulerDeps = hasEvalConfigs
      ? createObservationEvalSchedulerDeps()
      : null;

    await Promise.all(
      // Process each event independently
      eventInputs.map(async (eventInput) => {
        // Step 1: Create enriched event record (required for both evals and writes)
        let eventRecord;
        try {
          eventRecord = await ingestionService.createEventRecord(
            eventInput,
            fileKey,
          );
        } catch (error) {
          traceException(error);
          logger.error(
            `Failed to create event record for project ${eventInput.projectId} and observation ${eventInput.spanId}`,
            error,
          );

          return;
        }

        // Step 2: Schedule observation evals (independent of event writes)
        if (hasEvalConfigs && evalSchedulerDeps) {
          try {
            const observation =
              convertEventRecordToObservationForEval(eventRecord);

            await scheduleObservationEvals({
              observation,
              configs: evalConfigs,
              schedulerDeps: evalSchedulerDeps,
            });
          } catch (error) {
            traceException(error);

            logger.error(
              `Failed to schedule observation evals for project ${eventInput.projectId} and observation ${eventInput.spanId}`,
              error,
            );
          }
        }

        // Step 3: Write to events table (independent of eval scheduling)
        if (shouldWriteToEventsTable) {
          try {
            ingestionService.writeEventRecord(eventRecord);
          } catch (error) {
            traceException(error);
            logger.error(
              `Failed to write event record for ${eventInput.spanId}`,
              error,
            );
          }
        }
      }),
    );
  } catch (e) {
    if (e instanceof ForbiddenError) {
      traceException(e);
      logger.warn(`Failed to parse otel observation: ${e.message}`, e);
      return;
    }

    logger.error(
      `Failed job otel ingestion processing for ${job.data.payload.authCheck.scope.projectId}`,
      e,
    );
    traceException(e);
    throw e;
  }
};
