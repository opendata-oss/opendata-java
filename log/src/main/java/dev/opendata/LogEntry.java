package dev.opendata;

/**
 * An immutable log entry with heap-owned key and value data.
 *
 * <p>Unlike {@link LogEntryView}, the key and value arrays are safe to retain
 * indefinitely. Instances are produced by {@link LogScanIterator}.
 */
public record LogEntry(long sequence, long timestamp, byte[] key, byte[] value) {
}
