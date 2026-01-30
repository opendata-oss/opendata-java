package io.opendata.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the JNI bindings.
 *
 * <p>These tests require the native library to be built and available
 * in java.library.path. Run with:
 * <pre>
 * mvn test -Djava.library.path=../native/target/release
 * </pre>
 */
class LogIntegrationTest {

    private Log log;

    @BeforeEach
    void setUp() {
        // Opens an in-memory log (Config::default() in Rust)
        log = Log.open("unused-config-path");
    }

    @AfterEach
    void tearDown() {
        if (log != null) {
            log.close();
        }
    }

    @Test
    void should_append_and_return_result_with_timestamp() {
        // given
        byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
        long beforeAppend = System.currentTimeMillis();

        // when
        AppendResult result = log.append(key, value);

        // then
        long afterAppend = System.currentTimeMillis();
        assertThat(result.sequence()).isGreaterThanOrEqualTo(0);
        assertThat(result.timestamp())
                .isGreaterThanOrEqualTo(beforeAppend)
                .isLessThanOrEqualTo(afterAppend);
    }

    @Test
    void should_append_multiple_records_with_increasing_sequences() {
        // given
        byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);

        // when
        AppendResult first = log.append(key, "first".getBytes());
        AppendResult second = log.append(key, "second".getBytes());
        AppendResult third = log.append(key, "third".getBytes());

        // then
        assertThat(second.sequence()).isGreaterThan(first.sequence());
        assertThat(third.sequence()).isGreaterThan(second.sequence());
    }

    @Test
    void should_read_appended_entries() {
        // given
        byte[] key = "read-test".getBytes(StandardCharsets.UTF_8);
        byte[] value = "hello world".getBytes(StandardCharsets.UTF_8);
        AppendResult appendResult = log.append(key, value);

        // when
        LogReader reader = log.reader();
        List<LogEntry> entries = reader.read(key, 0, 10);
        reader.close();

        // then
        assertThat(entries).hasSize(1);
        LogEntry entry = entries.get(0);
        assertThat(entry.sequence()).isEqualTo(appendResult.sequence());
        assertThat(entry.timestamp()).isEqualTo(appendResult.timestamp());
        assertThat(entry.key()).isEqualTo(key);
        assertThat(entry.value()).isEqualTo(value);
    }

    @Test
    void should_read_multiple_entries_in_order() {
        // given
        byte[] key = "multi-read".getBytes(StandardCharsets.UTF_8);
        log.append(key, "first".getBytes());
        log.append(key, "second".getBytes());
        log.append(key, "third".getBytes());

        // when
        LogReader reader = log.reader();
        List<LogEntry> entries = reader.read(key, 0, 10);
        reader.close();

        // then
        assertThat(entries).hasSize(3);
        assertThat(new String(entries.get(0).value())).isEqualTo("first");
        assertThat(new String(entries.get(1).value())).isEqualTo("second");
        assertThat(new String(entries.get(2).value())).isEqualTo("third");

        // Sequences should be increasing
        assertThat(entries.get(1).sequence()).isGreaterThan(entries.get(0).sequence());
        assertThat(entries.get(2).sequence()).isGreaterThan(entries.get(1).sequence());
    }

    @Test
    void should_read_from_specific_sequence() {
        // given
        byte[] key = "seq-read".getBytes(StandardCharsets.UTF_8);
        log.append(key, "first".getBytes());
        AppendResult second = log.append(key, "second".getBytes());
        log.append(key, "third".getBytes());

        // when - read starting from second entry's sequence
        LogReader reader = log.reader();
        List<LogEntry> entries = reader.read(key, second.sequence(), 10);
        reader.close();

        // then - should get second and third entries
        assertThat(entries).hasSize(2);
        assertThat(new String(entries.get(0).value())).isEqualTo("second");
        assertThat(new String(entries.get(1).value())).isEqualTo("third");
    }

    @Test
    void should_return_empty_list_when_no_entries() {
        // given
        byte[] key = "empty-key".getBytes(StandardCharsets.UTF_8);

        // when
        LogReader reader = log.reader();
        List<LogEntry> entries = reader.read(key, 0, 10);
        reader.close();

        // then
        assertThat(entries).isEmpty();
    }

    @Test
    void should_handle_binary_payload() {
        // given
        byte[] key = "binary-key".getBytes(StandardCharsets.UTF_8);
        byte[] binaryValue = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryValue[i] = (byte) i;
        }

        // when
        log.append(key, binaryValue);
        LogReader reader = log.reader();
        List<LogEntry> entries = reader.read(key, 0, 10);
        reader.close();

        // then
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).value()).isEqualTo(binaryValue);
    }

    @Test
    void should_handle_large_payload() {
        // given
        byte[] key = "large-key".getBytes(StandardCharsets.UTF_8);
        byte[] largeValue = new byte[1024 * 1024]; // 1 MB
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) (i % 256);
        }

        // when
        AppendResult result = log.append(key, largeValue);
        LogReader reader = log.reader();
        List<LogEntry> entries = reader.read(key, 0, 10);
        reader.close();

        // then
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).value()).isEqualTo(largeValue);
        assertThat(entries.get(0).timestamp()).isEqualTo(result.timestamp());
    }

    @Test
    void should_isolate_keys() {
        // given
        byte[] key1 = "key-one".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = "key-two".getBytes(StandardCharsets.UTF_8);
        log.append(key1, "value-one".getBytes());
        log.append(key2, "value-two".getBytes());

        // when
        LogReader reader1 = log.reader();
        LogReader reader2 = log.reader();
        List<LogEntry> entries1 = reader1.read(key1, 0, 10);
        List<LogEntry> entries2 = reader2.read(key2, 0, 10);
        reader1.close();
        reader2.close();

        // then
        assertThat(entries1).hasSize(1);
        assertThat(entries2).hasSize(1);
        assertThat(new String(entries1.get(0).value())).isEqualTo("value-one");
        assertThat(new String(entries2.get(0).value())).isEqualTo("value-two");
    }
}
