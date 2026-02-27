package dev.opendata;

import dev.opendata.common.ObjectStoreConfig;
import dev.opendata.common.StorageConfig;
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

    private static List<LogEntry> collectEntries(LogScanIterator iter) {
        List<LogEntry> entries = new ArrayList<>();
        LogEntry entry;
        while ((entry = iter.next()) != null) {
            entries.add(entry);
        }
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
                List<LogEntry> entries = collectEntries(iter);
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
                List<LogEntry> entries = collectEntries(iter);
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
                List<LogEntry> entries = collectEntries(iter);
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
                List<LogEntry> entries = collectEntries(iter);
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
                    LogEntry entry = iter.next();
                    assertThat(entry).isNotNull();
                    entries.add(entry);
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
                assertThat(iter.next()).isNull();
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
                List<LogEntry> entriesA = collectEntries(iter);
                assertThat(entriesA).hasSize(2);
                assertThat(new String(entriesA.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-a-0");
                assertThat(new String(entriesA.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("value-a-1");
            }

            try (LogScanIterator iter = log.scan(keyB, 0)) {
                List<LogEntry> entriesB = collectEntries(iter);
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
                List<LogEntry> entries = collectEntries(iter);
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
                List<LogEntry> entries = collectEntries(iter);
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
                List<LogEntry> entries = collectEntries(iter);
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
                List<LogEntry> entries = collectEntries(iter);

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
            // Write initial data
            writer.tryAppend(key, "value-0".getBytes(StandardCharsets.UTF_8));

            // Open reader while writer is still open - this should NOT cause fencing error
            try (LogDbReader reader = LogDbReader.open(readerConfig)) {
                // Reader can read the data written by writer
                try (LogScanIterator iter = reader.scan(key, 0)) {
                    List<LogEntry> entries = collectEntries(iter);
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
                List<LogEntry> finalEntries = collectEntries(iter);
                assertThat(finalEntries).hasSize(4);
            }
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
                List<LogEntry> entries = collectEntries(iter);
                assertThat(entries).hasSize(1);
                assertThat(new String(entries.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-0");
            }
        }
    }

}
