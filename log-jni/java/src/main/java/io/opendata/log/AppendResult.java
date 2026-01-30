package io.opendata.log;

/**
 * Result of an append operation to the log.
 *
 * @param sequence  the sequence number assigned to the appended entry
 * @param timestamp the timestamp (epoch millis) when the entry was persisted
 */
public record AppendResult(long sequence, long timestamp) {
}
