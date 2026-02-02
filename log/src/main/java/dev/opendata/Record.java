package dev.opendata;

/**
 * A record to be appended to the log.
 *
 * <p>The timestamp is captured at record creation time to accurately measure
 * end-to-end latency, even when records are batched before being written.
 *
 * @param key         the key for this record
 * @param value       the value payload
 * @param timestampMs wall-clock time (epoch millis) when the record was created
 */
public record Record(byte[] key, byte[] value, long timestampMs) {

    /**
     * Creates a record with the current wall-clock time as timestamp.
     *
     * @param key   the key for this record
     * @param value the value payload
     */
    public Record(byte[] key, byte[] value) {
        this(key, value, System.currentTimeMillis());
    }
}
