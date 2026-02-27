package dev.opendata.common;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BytesTest {

    private static Bytes bytesOf(byte[] data) {
        return new Bytes(MemorySegment.ofArray(data));
    }

    @Test
    void shouldReturnCorrectLength() {
        Bytes bytes = bytesOf(new byte[]{1, 2, 3});
        assertThat(bytes.length()).isEqualTo(3);
    }

    @Test
    void shouldReturnCorrectLengthForEmpty() {
        Bytes bytes = bytesOf(new byte[0]);
        assertThat(bytes.length()).isEqualTo(0);
    }

    @Test
    void shouldGetByteAtIndex() {
        Bytes bytes = bytesOf(new byte[]{10, 20, 30});
        assertThat(bytes.get(0)).isEqualTo((byte) 10);
        assertThat(bytes.get(1)).isEqualTo((byte) 20);
        assertThat(bytes.get(2)).isEqualTo((byte) 30);
    }

    @Test
    void shouldThrowOnOutOfBoundsGet() {
        Bytes bytes = bytesOf(new byte[]{1, 2});
        assertThatThrownBy(() -> bytes.get(2))
                .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> bytes.get(-1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void shouldRoundtripToArray() {
        byte[] original = {0, 127, -128, 1, -1};
        Bytes bytes = bytesOf(original);
        assertThat(bytes.toArray()).isEqualTo(original);
    }

    @Test
    void shouldRoundtripEmptyToArray() {
        Bytes bytes = bytesOf(new byte[0]);
        assertThat(bytes.toArray()).isEmpty();
    }

    @Test
    void shouldCopyToDestination() {
        Bytes bytes = bytesOf(new byte[]{10, 20, 30, 40, 50});
        byte[] dest = new byte[3];
        bytes.copyTo(dest, 0, 1, 3);
        assertThat(dest).isEqualTo(new byte[]{20, 30, 40});
    }

    @Test
    void shouldCopyToWithOffset() {
        Bytes bytes = bytesOf(new byte[]{10, 20, 30});
        byte[] dest = new byte[5];
        bytes.copyTo(dest, 2, 0, 3);
        assertThat(dest).isEqualTo(new byte[]{0, 0, 10, 20, 30});
    }

    @Test
    void shouldReturnSegment() {
        MemorySegment segment = MemorySegment.ofArray(new byte[]{1});
        Bytes bytes = new Bytes(segment);
        assertThat(bytes.segment()).isSameAs(segment);
    }

    @Test
    void shouldThrowAfterInvalidateOnLength() {
        Bytes bytes = bytesOf(new byte[]{1});
        bytes.invalidate();
        assertThatThrownBy(bytes::length)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldThrowAfterInvalidateOnGet() {
        Bytes bytes = bytesOf(new byte[]{1});
        bytes.invalidate();
        assertThatThrownBy(() -> bytes.get(0))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldThrowAfterInvalidateOnToArray() {
        Bytes bytes = bytesOf(new byte[]{1});
        bytes.invalidate();
        assertThatThrownBy(bytes::toArray)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldThrowAfterInvalidateOnCopyTo() {
        Bytes bytes = bytesOf(new byte[]{1});
        bytes.invalidate();
        byte[] dest = new byte[1];
        assertThatThrownBy(() -> bytes.copyTo(dest, 0, 0, 1))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldThrowAfterInvalidateOnSegment() {
        Bytes bytes = bytesOf(new byte[]{1});
        bytes.invalidate();
        assertThatThrownBy(bytes::segment)
                .isInstanceOf(IllegalStateException.class);
    }
}
