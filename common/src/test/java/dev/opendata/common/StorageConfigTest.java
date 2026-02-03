package dev.opendata.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StorageConfigTest {

    @Test
    void shouldCreateInMemoryConfig() {
        var config = new StorageConfig.InMemory();
        assertThat(config).isInstanceOf(StorageConfig.class);
    }

    @Test
    void shouldCreateSlateDbConfigWithAllFields() {
        var objectStore = new ObjectStoreConfig.Local("/data");
        var config = new StorageConfig.SlateDb("prefix", objectStore, "settings.toml");

        assertThat(config.path()).isEqualTo("prefix");
        assertThat(config.objectStore()).isEqualTo(objectStore);
        assertThat(config.settingsPath()).isEqualTo("settings.toml");
    }

    @Test
    void shouldCreateSlateDbConfigWithoutSettingsPath() {
        var objectStore = new ObjectStoreConfig.Local("/data");
        var config = new StorageConfig.SlateDb("prefix", objectStore);

        assertThat(config.path()).isEqualTo("prefix");
        assertThat(config.objectStore()).isEqualTo(objectStore);
        assertThat(config.settingsPath()).isNull();
    }

    @Test
    void shouldRejectNullPath() {
        var objectStore = new ObjectStoreConfig.Local("/data");
        assertThatThrownBy(() -> new StorageConfig.SlateDb(null, objectStore))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path");
    }

    @Test
    void shouldRejectBlankPath() {
        var objectStore = new ObjectStoreConfig.Local("/data");
        assertThatThrownBy(() -> new StorageConfig.SlateDb("  ", objectStore))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path");
    }

    @Test
    void shouldRejectNullObjectStore() {
        assertThatThrownBy(() -> new StorageConfig.SlateDb("prefix", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("objectStore");
    }

    @Test
    void shouldSupportInstanceofPatternMatching() {
        StorageConfig inMemory = new StorageConfig.InMemory();
        StorageConfig slateDb = new StorageConfig.SlateDb("path", new ObjectStoreConfig.InMemory());

        assertThat(describeConfig(inMemory)).isEqualTo("in-memory");
        assertThat(describeConfig(slateDb)).isEqualTo("slatedb:path");
    }

    private String describeConfig(StorageConfig config) {
        if (config instanceof StorageConfig.InMemory) {
            return "in-memory";
        } else if (config instanceof StorageConfig.SlateDb slateDb) {
            return "slatedb:" + slateDb.path();
        }
        throw new IllegalArgumentException("Unknown config type");
    }
}
