package dev.opendata;

import dev.opendata.common.ObjectStoreConfig;
import dev.opendata.common.StorageConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogDbReaderConfigTest {

    @Test
    void shouldCreateWithStorageAndRefreshInterval() {
        var storage = new StorageConfig.InMemory();

        var config = new LogDbReaderConfig(storage, 500L);

        assertThat(config.storage()).isEqualTo(storage);
        assertThat(config.refreshIntervalMs()).isEqualTo(500L);
    }

    @Test
    void shouldCreateWithStorageOnlyUsingDefaultRefreshInterval() {
        var storage = new StorageConfig.InMemory();

        var config = new LogDbReaderConfig(storage);

        assertThat(config.storage()).isEqualTo(storage);
        assertThat(config.refreshIntervalMs()).isNull();
    }

    @Test
    void shouldCreateInMemoryConfig() {
        var config = LogDbReaderConfig.inMemory();

        assertThat(config.storage()).isInstanceOf(StorageConfig.InMemory.class);
        assertThat(config.refreshIntervalMs()).isNull();
    }

    @Test
    void shouldRejectNullStorage() {
        assertThatThrownBy(() -> new LogDbReaderConfig(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storage");
    }

    @Test
    void shouldRejectZeroRefreshInterval() {
        var storage = new StorageConfig.InMemory();
        assertThatThrownBy(() -> new LogDbReaderConfig(storage, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refreshIntervalMs");
    }

    @Test
    void shouldRejectNegativeRefreshInterval() {
        var storage = new StorageConfig.InMemory();
        assertThatThrownBy(() -> new LogDbReaderConfig(storage, -100L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refreshIntervalMs");
    }

    @Test
    void shouldCreateSlateDbConfig() {
        var config = new LogDbReaderConfig(
                new StorageConfig.SlateDb(
                        "data",
                        new ObjectStoreConfig.Aws("us-west-2", "my-bucket"),
                        "slatedb.toml"
                ),
                2000L
        );

        assertThat(config.storage()).isInstanceOf(StorageConfig.SlateDb.class);
        var slateDb = (StorageConfig.SlateDb) config.storage();
        assertThat(slateDb.path()).isEqualTo("data");
        assertThat(slateDb.objectStore()).isInstanceOf(ObjectStoreConfig.Aws.class);
        assertThat(config.refreshIntervalMs()).isEqualTo(2000L);
    }
}
