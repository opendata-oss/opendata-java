package dev.opendata;

import org.junit.jupiter.api.Test;

import java.lang.foreign.ValueLayout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordBatchTest {

    @Test
    void shouldTrackCountAndTimestamp() {
        try (RecordBatch batch = RecordBatch.create()) {
            assertThat(batch.count()).isEqualTo(0);
            assertThat(batch.isEmpty()).isTrue();
            assertThat(batch.firstTimestampMs()).isEqualTo(0);

            batch.add(new byte[]{1}, new byte[]{2}, 1000L);
            assertThat(batch.count()).isEqualTo(1);
            assertThat(batch.isEmpty()).isFalse();
            assertThat(batch.firstTimestampMs()).isEqualTo(1000L);

            batch.add(new byte[]{3}, new byte[]{4}, 2000L);
            assertThat(batch.count()).isEqualTo(2);
            assertThat(batch.firstTimestampMs()).isEqualTo(1000L);
        }
    }

    @Test
    void shouldGrowKeySegmentWhenFull() {
        // Start with tiny capacity to force growth
        try (RecordBatch batch = RecordBatch.create(8, 2)) {
            byte[] key = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            batch.add(key, new byte[0], 100L);
            batch.add(key, new byte[0], 200L);

            assertThat(batch.count()).isEqualTo(2);

            // Verify key data is correct by checking lengths
            assertThat(batch.keyLengths()[0]).isEqualTo(10);
            assertThat(batch.keyLengths()[1]).isEqualTo(10);
        }
    }

    @Test
    void shouldGrowValueSegmentWhenFull() {
        // Start with tiny capacity to force growth (8 bytes for timestamp header alone)
        try (RecordBatch batch = RecordBatch.create(8, 2)) {
            byte[] value = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            batch.add(new byte[]{1}, value, 100L);
            batch.add(new byte[]{2}, value, 200L);

            assertThat(batch.count()).isEqualTo(2);
            // Value length = 8 (timestamp) + 10 (payload)
            assertThat(batch.valueLengths()[0]).isEqualTo(18);
            assertThat(batch.valueLengths()[1]).isEqualTo(18);
        }
    }

    @Test
    void shouldGrowRecordArrays() {
        try (RecordBatch batch = RecordBatch.create(4096, 2)) {
            for (int i = 0; i < 10; i++) {
                batch.add(new byte[]{(byte) i}, new byte[]{(byte) i}, 100L + i);
            }
            assertThat(batch.count()).isEqualTo(10);
        }
    }

    @Test
    void shouldThrowOnAddAfterClose() {
        RecordBatch batch = RecordBatch.create();
        batch.close();

        assertThatThrownBy(() -> batch.add(new byte[]{1}, new byte[]{2}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void shouldRejectNullKey() {
        try (RecordBatch batch = RecordBatch.create()) {
            assertThatThrownBy(() -> batch.add(null, new byte[0]))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("key");
        }
    }

    @Test
    void shouldRejectNullValue() {
        try (RecordBatch batch = RecordBatch.create()) {
            assertThatThrownBy(() -> batch.add(new byte[0], null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("value");
        }
    }

    @Test
    void shouldStoreTimestampHeaderForEmptyValue() {
        try (RecordBatch batch = RecordBatch.create()) {
            batch.add(new byte[]{1}, new byte[0], 42L);

            // Value length should be exactly 8 (timestamp header only)
            assertThat(batch.valueLengths()[0]).isEqualTo(8);

            // Verify the timestamp is correctly stored as big-endian
            long stored = Long.reverseBytes(
                    batch.valuesData().get(ValueLayout.JAVA_LONG_UNALIGNED, batch.valueOffsets()[0]));
            assertThat(stored).isEqualTo(42L);
        }
    }

    @Test
    void shouldUseCurrentTimeForDefaultTimestamp() {
        try (RecordBatch batch = RecordBatch.create()) {
            long before = System.currentTimeMillis();
            batch.add(new byte[]{1}, new byte[]{2});
            long after = System.currentTimeMillis();

            assertThat(batch.firstTimestampMs())
                    .isGreaterThanOrEqualTo(before)
                    .isLessThanOrEqualTo(after);
        }
    }

    @Test
    void shouldTolerateDoubleClose() {
        RecordBatch batch = RecordBatch.create();
        batch.close();
        batch.close(); // should not throw
    }

    @Test
    void shouldStoreContiguousKeyData() {
        try (RecordBatch batch = RecordBatch.create()) {
            batch.add(new byte[]{10, 20}, new byte[]{1}, 100L);
            batch.add(new byte[]{30, 40, 50}, new byte[]{2}, 200L);

            assertThat(batch.keyOffsets()[0]).isEqualTo(0);
            assertThat(batch.keyLengths()[0]).isEqualTo(2);
            assertThat(batch.keyOffsets()[1]).isEqualTo(2);
            assertThat(batch.keyLengths()[1]).isEqualTo(3);

            // Verify actual bytes
            byte b0 = batch.keysData().get(ValueLayout.JAVA_BYTE, 0);
            byte b1 = batch.keysData().get(ValueLayout.JAVA_BYTE, 1);
            byte b2 = batch.keysData().get(ValueLayout.JAVA_BYTE, 2);
            assertThat(b0).isEqualTo((byte) 10);
            assertThat(b1).isEqualTo((byte) 20);
            assertThat(b2).isEqualTo((byte) 30);
        }
    }
}
