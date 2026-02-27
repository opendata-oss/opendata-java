package dev.opendata;

/**
 * An iterator over log scan results.
 *
 * <p>Wraps a native iterator handle and returns {@link LogEntry} instances
 * one at a time. Must be closed when done to release native resources.
 *
 * <p>Usage:
 * <pre>{@code
 * try (LogScanIterator iter = log.scan(key, 0)) {
 *     LogEntry entry;
 *     while ((entry = iter.next()) != null) {
 *         process(entry);
 *     }
 * }
 * }</pre>
 */
public final class LogScanIterator implements AutoCloseable {

    private NativeInterop.IteratorHandle handle;
    private boolean closed;

    LogScanIterator(NativeInterop.IteratorHandle handle) {
        this.handle = handle;
    }

    /**
     * Returns the next log entry, or {@code null} when the iterator is exhausted.
     *
     * @return the next entry, or null if no more entries
     */
    public LogEntry next() {
        NativeInterop.IteratorNextResult result = NativeInterop.iteratorNext(handle);
        if (!result.present()) {
            return null;
        }
        return new LogEntry(result.sequence(), result.timestamp(), result.key(), result.value());
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        handle.close();
        handle = null;
        closed = true;
    }
}
