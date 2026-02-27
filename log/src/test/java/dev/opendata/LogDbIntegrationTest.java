package dev.opendata;

import dev.opendata.common.ObjectStoreConfig;
import dev.opendata.common.StorageConfig;
import dev.opendata.common.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for LogDb that exercise the Panama FFM bindings.
 *
 * <p>These tests require the native C library to be built. Run:
 * <pre>
 *   cd ../opendata/log/c && cargo build --release
 * </pre>
 */
class LogDbIntegrationTest {

    private static List<LogEntry> collect(LogScanIterator iter) {
        List<LogEntry> entries = new ArrayList<>();
        iter.forEachRemaining(entries::add);
        return entries;
    }

    @Test
    void shouldOpenAndCloseInMemoryLog() {
        try (LogDb log = LogDb.openInMemory()) {
            assertThat(log).isNotNull();
        }
    }

    @Test
    void shouldOpenWithExplicitInMemoryConfig() {
        var config = new LogDbConfig(new StorageConfig.InMemory());
        try (LogDb log = LogDb.open(config)) {
            assertThat(log).isNotNull();
        }
    }

    @Test
    void shouldAppendAndReadSingleRecord() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);

            AppendResult result = log.tryAppend(key, value);

            assertThat(result.sequence()).isEqualTo(0);

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(1);
                assertThat(entries.get(0).sequence()).isEqualTo(0);
                assertThat(entries.get(0).key()).isEqualTo(key);
                assertThat(entries.get(0).value()).isEqualTo(value);
            }
        }
    }

    @Test
    void shouldAppendBatchOfRecords() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "batch-key".getBytes(StandardCharsets.UTF_8);
            Record[] records = new Record[] {
                new Record(key, "value-0".getBytes(StandardCharsets.UTF_8)),
                new Record(key, "value-1".getBytes(StandardCharsets.UTF_8)),
                new Record(key, "value-2".getBytes(StandardCharsets.UTF_8)),
            };

            AppendResult result = log.tryAppend(records);

            assertThat(result.sequence()).isEqualTo(0);

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(3);
                assertThat(entries.get(0).sequence()).isEqualTo(0);
                assertThat(entries.get(1).sequence()).isEqualTo(1);
                assertThat(entries.get(2).sequence()).isEqualTo(2);
                assertThat(new String(entries.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-0");
                assertThat(new String(entries.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("value-1");
                assertThat(new String(entries.get(2).value(), StandardCharsets.UTF_8)).isEqualTo("value-2");
            }
        }
    }

    @Test
    void shouldAssignSequentialSequencesAcrossAppends() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "seq-key".getBytes(StandardCharsets.UTF_8);

            log.tryAppend(key, "first".getBytes(StandardCharsets.UTF_8));
            log.tryAppend(key, "second".getBytes(StandardCharsets.UTF_8));
            AppendResult third = log.tryAppend(key, "third".getBytes(StandardCharsets.UTF_8));

            assertThat(third.sequence()).isEqualTo(2);

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(3);
                assertThat(entries.get(0).sequence()).isEqualTo(0);
                assertThat(entries.get(1).sequence()).isEqualTo(1);
                assertThat(entries.get(2).sequence()).isEqualTo(2);
            }
        }
    }

    @Test
    void shouldReadFromStartSequence() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "offset-key".getBytes(StandardCharsets.UTF_8);

            log.tryAppend(key, "value-0".getBytes(StandardCharsets.UTF_8));
            log.tryAppend(key, "value-1".getBytes(StandardCharsets.UTF_8));
            log.tryAppend(key, "value-2".getBytes(StandardCharsets.UTF_8));

            // Read starting from sequence 1
            try (LogScanIterator iter = log.scan(key, 1)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(2);
                assertThat(entries.get(0).sequence()).isEqualTo(1);
                assertThat(entries.get(1).sequence()).isEqualTo(2);
            }
        }
    }

    @Test
    void shouldIteratePartiallyAndCloseEarly() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "limit-key".getBytes(StandardCharsets.UTF_8);

            for (int i = 0; i < 10; i++) {
                log.tryAppend(key, ("value-" + i).getBytes(StandardCharsets.UTF_8));
            }

            // Iterate only 3 entries then close early
            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    assertThat(iter.hasNext()).isTrue();
                    entries.add(iter.next());
                }
                assertThat(entries).hasSize(3);
            }
        }
    }

    @Test
    void shouldReturnEmptyForUnknownKey() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "known".getBytes(StandardCharsets.UTF_8);
            log.tryAppend(key, "value".getBytes(StandardCharsets.UTF_8));

            byte[] unknownKey = "unknown".getBytes(StandardCharsets.UTF_8);
            try (LogScanIterator iter = log.scan(unknownKey, 0)) {
                assertThat(iter.hasNext()).isFalse();
            }
        }
    }

    @Test
    void shouldIsolateEntriesByKey() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] keyA = "key-a".getBytes(StandardCharsets.UTF_8);
            byte[] keyB = "key-b".getBytes(StandardCharsets.UTF_8);

            log.tryAppend(keyA, "value-a-0".getBytes(StandardCharsets.UTF_8));
            log.tryAppend(keyB, "value-b-0".getBytes(StandardCharsets.UTF_8));
            log.tryAppend(keyA, "value-a-1".getBytes(StandardCharsets.UTF_8));

            try (LogScanIterator iter = log.scan(keyA, 0)) {
                List<LogEntry> entriesA = collect(iter);
                assertThat(entriesA).hasSize(2);
                assertThat(new String(entriesA.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-a-0");
                assertThat(new String(entriesA.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("value-a-1");
            }

            try (LogScanIterator iter = log.scan(keyB, 0)) {
                List<LogEntry> entriesB = collect(iter);
                assertThat(entriesB).hasSize(1);
                assertThat(new String(entriesB.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-b-0");
            }
        }
    }

    @Test
    void shouldThrowWhenOperatingOnClosedLog() {
        LogDb log = LogDb.openInMemory();
        log.close();

        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> log.tryAppend(key, value))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void shouldOpenWithSlateDbLocalConfig(@TempDir Path tempDir) {
        var config = new LogDbConfig(
                new StorageConfig.SlateDb(
                        "test-data",
                        new ObjectStoreConfig.Local(tempDir.toString())
                )
        );

        try (LogDb log = LogDb.open(config)) {
            byte[] key = "persistent-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "persistent-value".getBytes(StandardCharsets.UTF_8);

            log.tryAppend(key, value);

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(1);
                assertThat(entries.get(0).value()).isEqualTo(value);
            }
        }
    }

    @Test
    void shouldHandleLargeValues() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "large-key".getBytes(StandardCharsets.UTF_8);
            byte[] largeValue = new byte[1024 * 1024]; // 1 MB
            for (int i = 0; i < largeValue.length; i++) {
                largeValue[i] = (byte) (i % 256);
            }

            log.tryAppend(key, largeValue);

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(1);
                assertThat(entries.get(0).value()).isEqualTo(largeValue);
            }
        }
    }

    @Test
    void shouldPreserveTimestamp() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "ts-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "ts-value".getBytes(StandardCharsets.UTF_8);

            long beforeAppend = System.currentTimeMillis();
            log.tryAppend(key, value);
            long afterAppend = System.currentTimeMillis();

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(1);
                // Timestamp should be within the append window
                assertThat(entries.get(0).timestamp())
                        .isGreaterThanOrEqualTo(beforeAppend)
                        .isLessThanOrEqualTo(afterAppend);
            }
        }
    }

    @Test
    void shouldReadFromSeparateLogDbReader(@TempDir Path tempDir) {
        var storage = new StorageConfig.SlateDb(
                "separate-reader-test",
                new ObjectStoreConfig.Local(tempDir.toString())
        );
        var writerConfig = new LogDbConfig(storage);
        var readerConfig = new LogDbReaderConfig(storage);

        byte[] key = "e2e-key".getBytes(StandardCharsets.UTF_8);

        // Write with LogDb and flush to ensure durability before reader opens
        try (LogDb writer = LogDb.open(writerConfig)) {
            writer.tryAppend(key, "value-0".getBytes(StandardCharsets.UTF_8));
            writer.tryAppend(key, "value-1".getBytes(StandardCharsets.UTF_8));
            writer.tryAppend(key, "value-2".getBytes(StandardCharsets.UTF_8));
            writer.flush();
        }

        // Read with separate LogDbReader
        try (LogDbReader reader = LogDbReader.open(readerConfig)) {
            try (LogScanIterator iter = reader.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);

                assertThat(entries).hasSize(3);
                assertThat(new String(entries.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-0");
                assertThat(new String(entries.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("value-1");
                assertThat(new String(entries.get(2).value(), StandardCharsets.UTF_8)).isEqualTo("value-2");
                assertThat(entries.get(0).sequence()).isEqualTo(0);
                assertThat(entries.get(1).sequence()).isEqualTo(1);
                assertThat(entries.get(2).sequence()).isEqualTo(2);
            }
        }
    }

    @Test
    void shouldCoexistWriterAndReaderWithoutFencingError(@TempDir Path tempDir) {
        var storage = new StorageConfig.SlateDb(
                "concurrent-test",
                new ObjectStoreConfig.Local(tempDir.toString())
        );
        var writerConfig = new LogDbConfig(storage);
        var readerConfig = new LogDbReaderConfig(storage);

        byte[] key = "concurrent-key".getBytes(StandardCharsets.UTF_8);

        // Open writer and keep it open
        try (LogDb writer = LogDb.open(writerConfig)) {
            // Write initial data and flush so reader can see it
            writer.tryAppend(key, "value-0".getBytes(StandardCharsets.UTF_8));
            writer.flush();

            // Open reader while writer is still open - this should NOT cause fencing error
            try (LogDbReader reader = LogDbReader.open(readerConfig)) {
                // Reader can read the data written by writer
                try (LogScanIterator iter = reader.scan(key, 0)) {
                    List<LogEntry> entries = collect(iter);
                    assertThat(entries).hasSize(1);
                    assertThat(new String(entries.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-0");
                }

                // Writer can still write more data while reader is open
                writer.tryAppend(key, "value-1".getBytes(StandardCharsets.UTF_8));
                writer.tryAppend(key, "value-2".getBytes(StandardCharsets.UTF_8));
            }

            // After reader closes, writer should still work
            writer.tryAppend(key, "value-3".getBytes(StandardCharsets.UTF_8));

            try (LogScanIterator iter = writer.scan(key, 0)) {
                List<LogEntry> finalEntries = collect(iter);
                assertThat(finalEntries).hasSize(4);
            }
        }
    }

    @Test
    void shouldReadEntriesOneAtATime() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "view-key".getBytes(StandardCharsets.UTF_8);
            log.tryAppend(key, "value-0".getBytes(StandardCharsets.UTF_8));
            log.tryAppend(key, "value-1".getBytes(StandardCharsets.UTF_8));
            log.tryAppend(key, "value-2".getBytes(StandardCharsets.UTF_8));

            try (LogScanRawIterator iter = log.scanRaw(key, 0)) {
                LogEntryView entry0 = iter.next();
                assertThat(entry0).isNotNull();
                assertThat(entry0.sequence()).isEqualTo(0);
                assertThat(entry0.key().toArray()).isEqualTo(key);
                assertThat(new String(entry0.value().toArray(), StandardCharsets.UTF_8)).isEqualTo("value-0");

                LogEntryView entry1 = iter.next();
                assertThat(entry1).isNotNull();
                assertThat(entry1.sequence()).isEqualTo(1);
                assertThat(new String(entry1.value().toArray(), StandardCharsets.UTF_8)).isEqualTo("value-1");

                LogEntryView entry2 = iter.next();
                assertThat(entry2).isNotNull();
                assertThat(entry2.sequence()).isEqualTo(2);
                assertThat(new String(entry2.value().toArray(), StandardCharsets.UTF_8)).isEqualTo("value-2");

                assertThat(iter.next()).isNull();
            }
        }
    }

    @Test
    void shouldInvalidatePreviousEntryOnNext() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "invalidate-key".getBytes(StandardCharsets.UTF_8);
            log.tryAppend(key, "value-0".getBytes(StandardCharsets.UTF_8));
            log.tryAppend(key, "value-1".getBytes(StandardCharsets.UTF_8));

            try (LogScanRawIterator iter = log.scanRaw(key, 0)) {
                LogEntryView entry0 = iter.next();
                assertThat(entry0).isNotNull();
                Bytes key0 = entry0.key();
                Bytes value0 = entry0.value();

                // Advance — previous entry's Bytes should be invalidated
                iter.next();

                assertThatThrownBy(key0::toArray)
                        .isInstanceOf(IllegalStateException.class);
                assertThatThrownBy(value0::toArray)
                        .isInstanceOf(IllegalStateException.class);
            }
        }
    }

    @Test
    void shouldInvalidateEntryOnClose() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "close-key".getBytes(StandardCharsets.UTF_8);
            log.tryAppend(key, "value-0".getBytes(StandardCharsets.UTF_8));

            Bytes savedKey;
            Bytes savedValue;
            try (LogScanRawIterator iter = log.scanRaw(key, 0)) {
                LogEntryView entry = iter.next();
                assertThat(entry).isNotNull();
                savedKey = entry.key();
                savedValue = entry.value();
                // Still valid before close
                assertThat(savedKey.toArray()).isEqualTo(key);
            }
            // After close, Bytes should be invalidated
            assertThatThrownBy(savedKey::toArray)
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(savedValue::toArray)
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void shouldHandleEmptyValue() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "empty-val-key".getBytes(StandardCharsets.UTF_8);
            byte[] emptyValue = new byte[0];

            log.tryAppend(key, emptyValue);

            try (LogScanIterator iter = log.scan(key, 0)) {
                assertThat(iter.hasNext()).isTrue();
                LogEntry entry = iter.next();
                assertThat(entry.value()).isEqualTo(emptyValue);
                assertThat(iter.hasNext()).isFalse();
            }
        }
    }

    @Test
    void shouldReturnNullRepeatedlyAfterExhaustion() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "exhaust-key".getBytes(StandardCharsets.UTF_8);
            log.tryAppend(key, "only".getBytes(StandardCharsets.UTF_8));

            try (LogScanRawIterator iter = log.scanRaw(key, 0)) {
                assertThat(iter.next()).isNotNull();
                assertThat(iter.next()).isNull();
                assertThat(iter.next()).isNull();
            }
        }
    }

    @Test
    void shouldTolerateDoubleCloseOnIterator() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "dbl-close-key".getBytes(StandardCharsets.UTF_8);
            log.tryAppend(key, "value".getBytes(StandardCharsets.UTF_8));

            LogScanRawIterator iter = log.scanRaw(key, 0);
            iter.close();
            iter.close(); // should not throw
        }
    }

    @Test
    void shouldThrowWhenScanningClosedLog() {
        LogDb log = LogDb.openInMemory();
        log.close();

        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> log.scanRaw(key, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void shouldThrowWhenFlushingClosedLog() {
        LogDb log = LogDb.openInMemory();
        log.close();

        assertThatThrownBy(log::flush)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void shouldTolerateDoubleClose() {
        LogDb log = LogDb.openInMemory();
        log.close();
        log.close(); // should not throw
    }

    @Test
    void shouldAppendWithTimeout() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "timeout-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "timeout-value".getBytes(StandardCharsets.UTF_8);

            AppendResult result = log.appendTimeout(key, value, 5000);
            assertThat(result.sequence()).isEqualTo(0);

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(1);
                assertThat(entries.get(0).key()).isEqualTo(key);
                assertThat(entries.get(0).value()).isEqualTo(value);
            }
        }
    }

    @Test
    void shouldFlushInMemoryLogWithoutError() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "flush-key".getBytes(StandardCharsets.UTF_8);
            log.tryAppend(key, "flush-value".getBytes(StandardCharsets.UTF_8));
            log.flush(); // should not throw
        }
    }

    @Test
    void shouldOpenReaderWithCustomRefreshInterval(@TempDir Path tempDir) {
        var storage = new StorageConfig.SlateDb(
                "custom-refresh-test",
                new ObjectStoreConfig.Local(tempDir.toString())
        );
        var writerConfig = new LogDbConfig(storage);
        // Use a custom refresh interval of 500ms
        var readerConfig = new LogDbReaderConfig(storage, 500L);

        byte[] key = "refresh-key".getBytes(StandardCharsets.UTF_8);

        // Write with LogDb and flush to ensure durability before reader opens
        try (LogDb writer = LogDb.open(writerConfig)) {
            writer.tryAppend(key, "value-0".getBytes(StandardCharsets.UTF_8));
            writer.flush();
        }

        // Read with LogDbReader using custom refresh interval
        try (LogDbReader reader = LogDbReader.open(readerConfig)) {
            try (LogScanIterator iter = reader.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(1);
                assertThat(new String(entries.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-0");
            }
        }
    }

    @Test
    void shouldThrowNoSuchElementWhenExhausted() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "nosuch-key".getBytes(StandardCharsets.UTF_8);
            log.tryAppend(key, "only".getBytes(StandardCharsets.UTF_8));

            try (LogScanIterator iter = log.scan(key, 0)) {
                iter.next(); // consume the single entry
                assertThatThrownBy(iter::next)
                        .isInstanceOf(java.util.NoSuchElementException.class);
            }
        }
    }

    @Test
    void shouldAppendRecordBatchAndReadBack() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "batch-key".getBytes(StandardCharsets.UTF_8);

            try (RecordBatch batch = RecordBatch.create()) {
                batch.add(key, "batch-0".getBytes(StandardCharsets.UTF_8), 1000L);
                batch.add(key, "batch-1".getBytes(StandardCharsets.UTF_8), 1001L);
                batch.add(key, "batch-2".getBytes(StandardCharsets.UTF_8), 1002L);

                AppendResult result = log.tryAppend(batch);
                assertThat(result.sequence()).isEqualTo(0);
            }

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(3);
                assertThat(entries.get(0).sequence()).isEqualTo(0);
                assertThat(entries.get(1).sequence()).isEqualTo(1);
                assertThat(entries.get(2).sequence()).isEqualTo(2);
                assertThat(new String(entries.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("batch-0");
                assertThat(new String(entries.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("batch-1");
                assertThat(new String(entries.get(2).value(), StandardCharsets.UTF_8)).isEqualTo("batch-2");
                assertThat(entries.get(0).key()).isEqualTo(key);
            }
        }
    }

    @Test
    void shouldAppendRecordBatchWithTimeout() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "batch-timeout-key".getBytes(StandardCharsets.UTF_8);

            try (RecordBatch batch = RecordBatch.create()) {
                batch.add(key, "value-0".getBytes(StandardCharsets.UTF_8), 500L);
                batch.add(key, "value-1".getBytes(StandardCharsets.UTF_8), 501L);

                AppendResult result = log.appendTimeout(batch, 5000);
                assertThat(result.sequence()).isEqualTo(0);
            }

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(2);
                assertThat(new String(entries.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-0");
                assertThat(new String(entries.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("value-1");
            }
        }
    }

    @Test
    void shouldPreserveTimestampThroughBatchRoundTrip() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "batch-ts-key".getBytes(StandardCharsets.UTF_8);
            long ts = 1234567890L;

            try (RecordBatch batch = RecordBatch.create()) {
                batch.add(key, "ts-value".getBytes(StandardCharsets.UTF_8), ts);
                log.tryAppend(batch);
            }

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(1);
                assertThat(entries.get(0).timestamp()).isEqualTo(ts);
            }
        }
    }

    @Test
    void shouldAssignContiguousSequencesAcrossBatches() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "multi-batch-key".getBytes(StandardCharsets.UTF_8);

            try (RecordBatch batch1 = RecordBatch.create()) {
                batch1.add(key, "a".getBytes(StandardCharsets.UTF_8), 100L);
                batch1.add(key, "b".getBytes(StandardCharsets.UTF_8), 101L);
                AppendResult r1 = log.tryAppend(batch1);
                assertThat(r1.sequence()).isEqualTo(0);
            }

            try (RecordBatch batch2 = RecordBatch.create()) {
                batch2.add(key, "c".getBytes(StandardCharsets.UTF_8), 200L);
                batch2.add(key, "d".getBytes(StandardCharsets.UTF_8), 201L);
                AppendResult r2 = log.tryAppend(batch2);
                assertThat(r2.sequence()).isEqualTo(2);
            }

            try (LogScanIterator iter = log.scan(key, 0)) {
                List<LogEntry> entries = collect(iter);
                assertThat(entries).hasSize(4);
                assertThat(entries.get(0).sequence()).isEqualTo(0);
                assertThat(entries.get(1).sequence()).isEqualTo(1);
                assertThat(entries.get(2).sequence()).isEqualTo(2);
                assertThat(entries.get(3).sequence()).isEqualTo(3);
            }
        }
    }

    @Test
    void shouldReturnSameResultForRepeatedHasNext() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "idempotent-key".getBytes(StandardCharsets.UTF_8);
            log.tryAppend(key, "value-0".getBytes(StandardCharsets.UTF_8));

            try (LogScanIterator iter = log.scan(key, 0)) {
                assertThat(iter.hasNext()).isTrue();
                assertThat(iter.hasNext()).isTrue();
                assertThat(iter.hasNext()).isTrue();

                LogEntry entry = iter.next();
                assertThat(new String(entry.value(), StandardCharsets.UTF_8)).isEqualTo("value-0");

                assertThat(iter.hasNext()).isFalse();
                assertThat(iter.hasNext()).isFalse();
            }
        }
    }

}
