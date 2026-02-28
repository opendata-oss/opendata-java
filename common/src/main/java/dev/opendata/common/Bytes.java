package dev.opendata.common;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A read-only view over native memory, backed by a {@link MemorySegment}.
 *
 * <p>Instances are created by iterators and are valid until the next
 * {@code next()} call or {@code close()} on the owning iterator. Accessing
 * a {@code Bytes} instance after it has been invalidated throws
 * {@link IllegalStateException}.
 *
 * <p>To obtain a stable copy, call {@link #toArray()}.
 */
public final class Bytes {

    private volatile MemorySegment segment;

    /**
     * Creates a new Bytes wrapping the given segment.
     *
     * @param segment the backing memory segment (should be read-only)
     */
    public Bytes(MemorySegment segment) {
        this.segment = segment;
    }

    /**
     * Returns the number of bytes.
     */
    public int length() {
        return (int) segment().byteSize();
    }

    /**
     * Returns the byte at the given index.
     *
     * @param index the zero-based byte index
     * @return the byte value
     * @throws IndexOutOfBoundsException if index is out of range
     */
    public byte get(int index) {
        return segment().get(ValueLayout.JAVA_BYTE, index);
    }

    /**
     * Copies the contents to a new {@code byte[]}.
     *
     * @return a freshly allocated byte array with all bytes copied
     */
    public byte[] toArray() {
        return segment().toArray(ValueLayout.JAVA_BYTE);
    }

    /**
     * Copies bytes from this Bytes into the destination array.
     *
     * @param dest       the destination array
     * @param destOffset the starting offset in the destination
     * @param srcOffset  the starting offset in this Bytes
     * @param len        the number of bytes to copy
     */
    public void copyTo(byte[] dest, int destOffset, int srcOffset, int len) {
        MemorySegment.copy(segment(), ValueLayout.JAVA_BYTE, srcOffset,
                MemorySegment.ofArray(dest), ValueLayout.JAVA_BYTE, destOffset, len);
    }

    /**
     * Returns the underlying {@link MemorySegment} for advanced callers.
     *
     * @return the backing memory segment
     * @throws IllegalStateException if this Bytes has been invalidated
     */
    public MemorySegment segment() {
        MemorySegment s = segment;
        if (s == null) {
            throw new IllegalStateException("Bytes has been invalidated — the backing native memory has been freed");
        }
        return s;
    }

    /**
     * Invalidates this Bytes. Called by the owning iterator when the
     * backing native memory is about to be freed.
     */
    public void invalidate() {
        segment = null;
    }
}
