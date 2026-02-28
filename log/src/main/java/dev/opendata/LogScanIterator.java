package dev.opendata;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A copying iterator over log scan results.
 *
 * <p>Wraps a {@link LogScanRawIterator} and copies each entry's key and value to
 * heap-owned {@code byte[]} arrays, producing {@link LogEntry} records that are
 * safe to retain after the iterator advances.
 *
 * <p>Implements {@link Iterator} for use with for-each loops and streams, and
 * {@link AutoCloseable} to release the underlying native iterator.
 *
 * <p><b>Note:</b> This is a single-use {@link Iterable} — {@link #iterator()} returns
 * {@code this}, so the iterator can only be traversed once.
 *
 * <p>Usage:
 * <pre>{@code
 * try (LogScanIterator iter = log.scan(key, 0)) {
 *     for (LogEntry entry : iter) {
 *         process(entry.key(), entry.value());
 *     }
 * }
 * }</pre>
 */
public final class LogScanIterator implements Iterator<LogEntry>, AutoCloseable, Iterable<LogEntry> {

    private final LogScanRawIterator inner;
    private LogEntry prefetched;
    private boolean done;

    LogScanIterator(LogScanRawIterator inner) {
        this.inner = inner;
    }

    @Override
    public boolean hasNext() {
        if (prefetched != null) {
            return true;
        }
        if (done) {
            return false;
        }
        LogEntryView view = inner.next();
        if (view == null) {
            done = true;
            return false;
        }
        prefetched = new LogEntry(
                view.sequence(),
                view.timestamp(),
                view.key().toArray(),
                view.value().toArray());
        return true;
    }

    @Override
    public LogEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        LogEntry entry = prefetched;
        prefetched = null;
        return entry;
    }

    @Override
    public Iterator<LogEntry> iterator() {
        return this;
    }

    @Override
    public void close() {
        inner.close();
    }
}
