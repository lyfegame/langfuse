import { BaseError } from "./BaseError";

export type TraceObservationPayloadTooLargeDetails = {
  traceId: string;
  observationCount: number;
  estimatedBytes: number;
  limitBytes: number;
  suggestion: string;
};

const formatMegabytes = (bytes: number) => `${(bytes / 1e6).toFixed(2)}MB`;

export class TraceObservationPayloadTooLargeError extends BaseError {
  public readonly details: TraceObservationPayloadTooLargeDetails;

  constructor(params: {
    traceId: string;
    observationCount: number;
    estimatedBytes: number;
    limitBytes: number;
  }) {
    const { traceId, observationCount, estimatedBytes, limitBytes } = params;
    const suggestion = [
      `Retry with GET /api/public/traces/${traceId}?includeIO=true`,
      "for the trace skeleton, then page",
      `GET /api/public/observations?traceId=${traceId}&type=GENERATION|TOOL&limit=20.`,
    ].join(" ");
    const message = [
      `Observation IO for trace ${traceId} is too large to return in one response:`,
      `${formatMegabytes(estimatedBytes)} across ${observationCount} observations`,
      `exceeds limit of ${formatMegabytes(limitBytes)}.`,
      suggestion,
    ].join(" ");

    super("TraceObservationPayloadTooLargeError", 413, message, true);

    this.details = {
      traceId,
      observationCount,
      estimatedBytes,
      limitBytes,
      suggestion,
    };
  }
}
