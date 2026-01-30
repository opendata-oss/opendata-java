package io.opendata.log;

/**
 * A single entry read from the log.
 *
 * @param sequence  the sequence number of this entry
 * @param timestamp the timestamp (epoch millis) when this entry was appended
 * @param key       the key this entry was appended under
 * @param value     the value of this entry
 */
public record LogEntry(long sequence, long timestamp, byte[] key, byte[] value) {
}
