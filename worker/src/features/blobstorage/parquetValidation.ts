import { Transform, type Readable } from "stream";

// PAR1 magic bytes that start every valid Parquet file (0x50 0x41 0x52 0x31).
export const PAR1_MAGIC = Buffer.from([0x50, 0x41, 0x52, 0x31]);
// Minimum valid Parquet file size. A valid Parquet file with header + footer
// always exceeds 1 KiB. ClickHouse error text files are typically 230-255 bytes.
export const MIN_VALID_PARQUET_SIZE = 1024;

export type ParquetValidationResult = {
  sizeBytes: number;
  headerHex: string;
  hasPar1Magic: boolean;
};

export class InvalidParquetPayloadError extends Error {
  public readonly validation: ParquetValidationResult;

  constructor(validation: ParquetValidationResult) {
    super(
      `Invalid Parquet file: size=${validation.sizeBytes} bytes, ` +
        `header=${validation.headerHex}. ` +
        `Expected PAR1 magic (50415231) and minimum > ${MIN_VALID_PARQUET_SIZE} bytes. ` +
        `This typically indicates a ClickHouse error response was uploaded instead of data.`,
    );
    this.name = "InvalidParquetPayloadError";
    this.validation = validation;
  }
}

/**
 * Wraps a Readable stream in a Transform that passes data through unchanged
 * while capturing the first 4 bytes and total byte count for post-upload
 * Parquet validation. Call `validate()` after the upload completes.
 */
export function createParquetValidationStream(source: Readable): {
  stream: Transform;
  getValidationResult: () => ParquetValidationResult;
  validate: () => void;
} {
  let totalBytes = 0;
  let headerBytes = Buffer.alloc(0);

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

    return {
      sizeBytes: totalBytes,
      headerHex: headerBytes.toString("hex"),
      hasPar1Magic,
    };
  };

  return {
    stream: transform,
    getValidationResult,
    validate() {
      const result = getValidationResult();

      if (result.sizeBytes <= MIN_VALID_PARQUET_SIZE || !result.hasPar1Magic) {
        throw new InvalidParquetPayloadError(result);
      }
    },
  };
}
