import { Transform, type Readable } from "stream";

// PAR1 magic bytes that start every valid Parquet file (0x50 0x41 0x52 0x31).
export const PAR1_MAGIC = Buffer.from([0x50, 0x41, 0x52, 0x31]);
// Minimum valid Parquet file size. A valid Parquet file with header + footer
// always exceeds 1 KiB. ClickHouse error text files are typically 230-255 bytes.
export const MIN_VALID_PARQUET_SIZE = 1024;

export type ParquetValidationResult = {
  sizeBytes: number;
  headerHex: string;
  trailerHex: string;
  footerLengthBytes: number | null;
  hasPar1Magic: boolean;
  hasPar1Trailer: boolean;
  hasValidFooterLength: boolean;
};

export class InvalidParquetPayloadError extends Error {
  public readonly validation: ParquetValidationResult;

  constructor(validation: ParquetValidationResult) {
    super(
      `Invalid Parquet file: size=${validation.sizeBytes} bytes, ` +
        `header=${validation.headerHex}, trailer=${validation.trailerHex}, footerLength=${validation.footerLengthBytes}. ` +
        `Expected PAR1 magic (50415231) at start and trailer, valid footer length, and minimum size threshold. ` +
        `This typically indicates a ClickHouse error response was uploaded instead of data.`,
    );
    this.name = "InvalidParquetPayloadError";
    this.validation = validation;
  }
}

type CreateParquetValidationStreamOptions = {
  minSizeBytes?: number;
};

/**
 * Wraps a Readable stream in a Transform that passes data through unchanged
 * while capturing the first 4 bytes, trailing 8-byte parquet trailer, and
 * total byte count for post-upload
 * Parquet validation. Call `validate()` after the upload completes.
 */
export function createParquetValidationStream(
  source: Readable,
  options?: CreateParquetValidationStreamOptions,
): {
  stream: Transform;
  getValidationResult: () => ParquetValidationResult;
  validate: () => void;
} {
  const minSizeBytes = options?.minSizeBytes ?? MIN_VALID_PARQUET_SIZE;
  let totalBytes = 0;
  let headerBytes = Buffer.alloc(0);
  let trailerBytes = Buffer.alloc(0);

  const transform = new Transform({
    transform(chunk, _encoding, callback) {
      const chunkBuffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);

      if (headerBytes.length < 4) {
        const needed = 4 - headerBytes.length;
        headerBytes = Buffer.concat([
          headerBytes,
          chunkBuffer.subarray(0, needed),
        ]);
      }

      const trailerCandidate = Buffer.concat([trailerBytes, chunkBuffer]);
      trailerBytes =
        trailerCandidate.length <= 8
          ? trailerCandidate
          : trailerCandidate.subarray(trailerCandidate.length - 8);

      totalBytes += chunkBuffer.length;
      callback(null, chunkBuffer);
    },
  });

  const forwardSourceError = (error: unknown) => {
    transform.destroy(
      error instanceof Error
        ? error
        : new Error(`Stream error: ${String(error)}`),
    );
  };

  source.once("error", forwardSourceError);
  transform.once("close", () => {
    source.off("error", forwardSourceError);
  });

  source.pipe(transform);

  const getValidationResult = (): ParquetValidationResult => {
    const hasPar1Magic =
      headerBytes.length === 4 && headerBytes.subarray(0, 4).equals(PAR1_MAGIC);
    const hasPar1Trailer =
      trailerBytes.length === 8 &&
      trailerBytes.subarray(4, 8).equals(PAR1_MAGIC);
    const footerLengthBytes =
      trailerBytes.length === 8 ? trailerBytes.readUInt32LE(0) : null;
    const hasValidFooterLength =
      footerLengthBytes !== null &&
      footerLengthBytes > 0 &&
      footerLengthBytes <= totalBytes - 8;

    return {
      sizeBytes: totalBytes,
      headerHex: headerBytes.toString("hex"),
      trailerHex: trailerBytes.toString("hex"),
      footerLengthBytes,
      hasPar1Magic,
      hasPar1Trailer,
      hasValidFooterLength,
    };
  };

  return {
    stream: transform,
    getValidationResult,
    validate() {
      const result = getValidationResult();

      if (
        result.sizeBytes <= minSizeBytes ||
        !result.hasPar1Magic ||
        !result.hasPar1Trailer ||
        !result.hasValidFooterLength
      ) {
        throw new InvalidParquetPayloadError(result);
      }
    },
  };
}
