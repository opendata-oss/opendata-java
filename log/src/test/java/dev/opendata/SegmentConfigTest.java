package dev.opendata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentConfigTest {

    @Test
    void shouldCreateDefaultConfig() {
        var config = SegmentConfig.DEFAULT;

        assertThat(config.sealIntervalMs()).isNull();
    }

    @Test
    void shouldCreateWithSealInterval() {
        var config = SegmentConfig.withSealInterval(3600_000);

        assertThat(config.sealIntervalMs()).isEqualTo(3600_000L);
    }

    @Test
    void shouldRejectZeroInterval() {
        assertThatThrownBy(() -> SegmentConfig.withSealInterval(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void shouldRejectNegativeInterval() {
        assertThatThrownBy(() -> SegmentConfig.withSealInterval(-1000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }

    @Test
    void shouldAllowNullIntervalViaConstructor() {
        var config = new SegmentConfig(null);

        assertThat(config.sealIntervalMs()).isNull();
    }

    @Test
    void shouldAllowIntervalViaConstructor() {
        var config = new SegmentConfig(60_000L);

        assertThat(config.sealIntervalMs()).isEqualTo(60_000L);
    }
}
