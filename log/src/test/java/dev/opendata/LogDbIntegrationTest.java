package dev.opendata;

import dev.opendata.common.ObjectStoreConfig;
import dev.opendata.common.StorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for LogDb that exercise the native JNI bindings.
 *
 * <p>These tests require the native library to be built. Run:
 * <pre>
 *   cd log/native && cargo build --release
 * </pre>
 */
class LogDbIntegrationTest {

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

            AppendResult result = log.append(key, value);

            assertThat(result.sequence()).isEqualTo(0);

            List<LogEntry> entries = log.scan(key, 0, 10);
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).sequence()).isEqualTo(0);
            assertThat(entries.get(0).key()).isEqualTo(key);
            assertThat(entries.get(0).value()).isEqualTo(value);
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

            AppendResult result = log.append(records);

            assertThat(result.sequence()).isEqualTo(0);

            List<LogEntry> entries = log.scan(key, 0, 10);
            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).sequence()).isEqualTo(0);
            assertThat(entries.get(1).sequence()).isEqualTo(1);
            assertThat(entries.get(2).sequence()).isEqualTo(2);
            assertThat(new String(entries.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-0");
            assertThat(new String(entries.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("value-1");
            assertThat(new String(entries.get(2).value(), StandardCharsets.UTF_8)).isEqualTo("value-2");
        }
    }

    @Test
    void shouldAssignSequentialSequencesAcrossAppends() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "seq-key".getBytes(StandardCharsets.UTF_8);

            log.append(key, "first".getBytes(StandardCharsets.UTF_8));
            log.append(key, "second".getBytes(StandardCharsets.UTF_8));
            AppendResult third = log.append(key, "third".getBytes(StandardCharsets.UTF_8));

            assertThat(third.sequence()).isEqualTo(2);

            List<LogEntry> entries = log.scan(key, 0, 10);
            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).sequence()).isEqualTo(0);
            assertThat(entries.get(1).sequence()).isEqualTo(1);
            assertThat(entries.get(2).sequence()).isEqualTo(2);
        }
    }

    @Test
    void shouldReadFromStartSequence() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "offset-key".getBytes(StandardCharsets.UTF_8);

            log.append(key, "value-0".getBytes(StandardCharsets.UTF_8));
            log.append(key, "value-1".getBytes(StandardCharsets.UTF_8));
            log.append(key, "value-2".getBytes(StandardCharsets.UTF_8));

            // Read starting from sequence 1
            List<LogEntry> entries = log.scan(key, 1, 10);
            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).sequence()).isEqualTo(1);
            assertThat(entries.get(1).sequence()).isEqualTo(2);
        }
    }

    @Test
    void shouldRespectMaxEntries() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "limit-key".getBytes(StandardCharsets.UTF_8);

            for (int i = 0; i < 10; i++) {
                log.append(key, ("value-" + i).getBytes(StandardCharsets.UTF_8));
            }

            List<LogEntry> entries = log.scan(key, 0, 3);
            assertThat(entries).hasSize(3);
        }
    }

    @Test
    void shouldReturnEmptyListForUnknownKey() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "known".getBytes(StandardCharsets.UTF_8);
            log.append(key, "value".getBytes(StandardCharsets.UTF_8));

            byte[] unknownKey = "unknown".getBytes(StandardCharsets.UTF_8);
            List<LogEntry> entries = log.scan(unknownKey, 0, 10);
            assertThat(entries).isEmpty();
        }
    }

    @Test
    void shouldIsolateEntriesByKey() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] keyA = "key-a".getBytes(StandardCharsets.UTF_8);
            byte[] keyB = "key-b".getBytes(StandardCharsets.UTF_8);

            log.append(keyA, "value-a-0".getBytes(StandardCharsets.UTF_8));
            log.append(keyB, "value-b-0".getBytes(StandardCharsets.UTF_8));
            log.append(keyA, "value-a-1".getBytes(StandardCharsets.UTF_8));

            List<LogEntry> entriesA = log.scan(keyA, 0, 10);
            assertThat(entriesA).hasSize(2);
            assertThat(new String(entriesA.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-a-0");
            assertThat(new String(entriesA.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("value-a-1");

            List<LogEntry> entriesB = log.scan(keyB, 0, 10);
            assertThat(entriesB).hasSize(1);
            assertThat(new String(entriesB.get(0).value(), StandardCharsets.UTF_8)).isEqualTo("value-b-0");
        }
    }

    @Test
    void shouldThrowWhenOperatingOnClosedLog() {
        LogDb log = LogDb.openInMemory();
        log.close();

        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> log.append(key, value))
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

            log.append(key, value);

            List<LogEntry> entries = log.scan(key, 0, 10);
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).value()).isEqualTo(value);
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

            log.append(key, largeValue);

            List<LogEntry> entries = log.scan(key, 0, 10);
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).value()).isEqualTo(largeValue);
        }
    }

    @Test
    void shouldPreserveTimestamp() {
        try (LogDb log = LogDb.openInMemory()) {
            byte[] key = "ts-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "ts-value".getBytes(StandardCharsets.UTF_8);

            long beforeAppend = System.currentTimeMillis();
            log.append(key, value);
            long afterAppend = System.currentTimeMillis();

            List<LogEntry> entries = log.scan(key, 0, 10);
            assertThat(entries).hasSize(1);
            // Timestamp should be within the append window
            assertThat(entries.get(0).timestamp())
                    .isGreaterThanOrEqualTo(beforeAppend)
                    .isLessThanOrEqualTo(afterAppend);
        }
    }
}
