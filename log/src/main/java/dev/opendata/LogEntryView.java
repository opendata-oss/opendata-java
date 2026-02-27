package dev.opendata;

import dev.opendata.common.Bytes;

/**
 * A zero-copy view of a log entry backed by native memory.
 *
 * <p>The {@link #key()} and {@link #value()} are valid only until the next
 * call to {@code next()} on the owning iterator, or until the iterator is
 * closed. After that, accessing the {@link Bytes} will throw
 * {@link IllegalStateException}.
 *
 * <p>To obtain a stable copy, call {@link Bytes#toArray()} on the key or value,
 * or use {@link LogScanIterator} which copies automatically.
 */
public final class LogEntryView {

    private final long sequence;
    private final long timestamp;
    private final Bytes key;
    private final Bytes value;

    LogEntryView(long sequence, long timestamp, Bytes key, Bytes value) {
        this.sequence = sequence;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the sequence number of this entry.
     */
    public long sequence() {
        return sequence;
    }

    /**
     * Returns the timestamp (epoch millis) when this entry was appended.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the key as a zero-copy {@link Bytes} view.
     * Valid until the next iterator advance or close.
     */
    public Bytes key() {
        return key;
    }

    /**
     * Returns the value as a zero-copy {@link Bytes} view.
     * Valid until the next iterator advance or close.
     */
    public Bytes value() {
        return value;
    }
}
