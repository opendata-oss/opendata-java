package dev.opendata;

/**
 * Interface for read operations on the log.
 *
 * <p>This interface defines the common read API shared by {@link LogDb} and
 * {@link LogDbReader}. It provides methods for reading entries from the log.
 *
 * <p>Implementors:
 * <ul>
 *   <li>{@link LogDb} - The main log interface with both read and write access.
 *   <li>{@link LogDbReader} - A read-only view of the log.
 * </ul>
 */
public interface LogRead {

    /**
     * Scans entries from the log for the given key starting at a sequence number.
     *
     * <p>Returns a copying iterator that yields {@link LogEntry} records with
     * heap-owned key and value arrays. The caller must close the iterator when
     * done to release native resources.
     *
     * @param key           the key to scan
     * @param startSequence the sequence number to start scanning from
     * @return a copying iterator over log entries (caller must close)
     */
    default LogScanIterator scan(byte[] key, long startSequence) {
        return new LogScanIterator(scanRaw(key, startSequence));
    }

    /**
     * Scans entries from the log returning zero-copy views backed by native memory.
     *
     * <p>Each {@link LogEntryView} returned by the iterator is valid only until the
     * next call to {@code next()} or {@code close()}. Use {@link #scan(byte[], long)}
     * for an iterator that automatically copies entries.
     *
     * @param key           the key to scan
     * @param startSequence the sequence number to start scanning from
     * @return an iterator over zero-copy log entry views (caller must close)
     */
    LogScanRawIterator scanRaw(byte[] key, long startSequence);
}
