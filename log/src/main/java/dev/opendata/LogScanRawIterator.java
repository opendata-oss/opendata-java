package dev.opendata;

import dev.opendata.common.Bytes;

import java.lang.foreign.MemorySegment;

/**
 * An iterator over log scan results returning zero-copy views.
 *
 * <p>Wraps a native iterator handle and returns {@link LogEntryView} instances
 * one at a time. Must be closed when done to release native resources.
 *
 * <p>Each {@link LogEntryView} returned by {@link #next()} is a zero-copy view
 * backed by native memory. The entry's {@link Bytes} fields are valid only
 * until the next call to {@code next()} or {@code close()}.
 *
 * <p>For an iterator that automatically copies entries to heap memory, use
 * {@link LogScanIterator} via {@link LogRead#scan(byte[], long)}.
 *
 * <p>Usage:
 * <pre>{@code
 * try (LogScanRawIterator iter = log.scanRaw(key, 0)) {
 *     LogEntryView entry;
 *     while ((entry = iter.next()) != null) {
 *         // entry.key() and entry.value() are valid here
 *         process(entry.key(), entry.value());
 *     }
 * }
 * }</pre>
 */
public final class LogScanRawIterator implements AutoCloseable {

    private NativeInterop.IteratorHandle handle;
    private boolean closed;

    // Track previous entry's native memory for deferred free
    private MemorySegment pendingKeyPtr;
    private long pendingKeyLen;
    private MemorySegment pendingValuePtr;
    private long pendingValueLen;
    private LogEntryView currentEntry;

    LogScanRawIterator(NativeInterop.IteratorHandle handle) {
        this.handle = handle;
    }

    /**
     * Returns a zero-copy view of the next log entry, or {@code null} when exhausted.
     *
     * <p>The returned entry's {@link Bytes} fields are backed by native memory and
     * are valid only until the next call to {@code next()} or {@code close()}.
     *
     * @return the next entry view, or null if no more entries
     */
    public LogEntryView next() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed");
        }
        freePending();
        invalidateCurrent();

        NativeInterop.RawIteratorResult raw = NativeInterop.iteratorNextRaw(handle);
        if (!raw.present()) {
            return null;
        }

        // Wrap key as read-only Bytes (zero-copy)
        Bytes key = new Bytes(raw.keyPtr().reinterpret(raw.keyLen()).asReadOnly());

        // Wrap value payload (after timestamp header) as read-only Bytes
        Bytes value;
        long valueLen = raw.valueLen();
        if (valueLen >= NativeInterop.TIMESTAMP_HEADER_SIZE) {
            long payloadLen = valueLen - NativeInterop.TIMESTAMP_HEADER_SIZE;
            if (payloadLen > 0) {
                value = new Bytes(raw.valuePtr().reinterpret(valueLen)
                        .asSlice(NativeInterop.TIMESTAMP_HEADER_SIZE, payloadLen).asReadOnly());
            } else {
                value = new Bytes(MemorySegment.ofArray(new byte[0]));
            }
        } else {
            if (valueLen > 0) {
                value = new Bytes(raw.valuePtr().reinterpret(valueLen).asReadOnly());
            } else {
                value = new Bytes(MemorySegment.ofArray(new byte[0]));
            }
        }

        currentEntry = new LogEntryView(raw.sequence(), raw.timestamp(), key, value);
        pendingKeyPtr = raw.keyPtr();
        pendingKeyLen = raw.keyLen();
        pendingValuePtr = raw.valuePtr();
        pendingValueLen = raw.valueLen();
        return currentEntry;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        freePending();
        invalidateCurrent();
        handle.close();
        handle = null;
        closed = true;
    }

    private void freePending() {
        if (pendingKeyPtr != null) {
            NativeInterop.freeBytes(pendingKeyPtr, pendingKeyLen);
            pendingKeyPtr = null;
            pendingKeyLen = 0;
        }
        if (pendingValuePtr != null) {
            NativeInterop.freeBytes(pendingValuePtr, pendingValueLen);
            pendingValuePtr = null;
            pendingValueLen = 0;
        }
    }

    private void invalidateCurrent() {
        if (currentEntry != null) {
            currentEntry.key().invalidate();
            currentEntry.value().invalidate();
            currentEntry = null;
        }
    }
}
