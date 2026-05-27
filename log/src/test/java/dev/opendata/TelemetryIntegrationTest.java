package dev.opendata;

import dev.opendata.common.ObjectStoreConfig;
import dev.opendata.common.StorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link Telemetry} that exercise the Panama FFM
 * bindings for {@code opendata_log_init_telemetry} and
 * {@code opendata_log_render_metrics}.
 *
 * <p>{@code slatedb_db_*} metrics only emit when the SlateDB backend is in
 * use; the in-memory backend bypasses SlateDB entirely.
 */
class TelemetryIntegrationTest {

    @Test
    void shouldRenderSlateDbMetricsAfterAppendAndFlush(@TempDir Path tempDir) {
        Telemetry.init();

        var config = new LogDbConfig(new StorageConfig.SlateDb(
                "telemetry-test",
                new ObjectStoreConfig.Local(tempDir.toString())));

        try (LogDb log = LogDb.open(config)) {
            byte[] key = "telemetry-key".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 4; i++) {
                log.tryAppend(key, ("value-" + i).getBytes(StandardCharsets.UTF_8));
            }
            log.flush();
        }

        String metrics = Telemetry.renderMetrics();
        assertThat(metrics).contains("slatedb_db_");
    }

    @Test
    void shouldBeIdempotent() {
        Telemetry.init();
        Telemetry.init();
    }
}
