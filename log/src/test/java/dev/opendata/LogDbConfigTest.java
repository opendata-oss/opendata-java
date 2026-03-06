package dev.opendata;

import dev.opendata.common.ObjectStoreConfig;
import dev.opendata.common.StorageConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogDbConfigTest {

    @Test
    void shouldCreateWithStorageAndSegmentation() {
        var storage = new StorageConfig.InMemory();
        var segmentation = SegmentConfig.withSealInterval(3600_000);

        var config = new LogDbConfig(storage, segmentation);

        assertThat(config.storage()).isEqualTo(storage);
        assertThat(config.segmentation()).isEqualTo(segmentation);
        assertThat(config.readVisibility()).isEqualTo(ReadVisibility.MEMORY);
    }

    @Test
    void shouldCreateWithAllParameters() {
        var storage = new StorageConfig.InMemory();
        var segmentation = SegmentConfig.withSealInterval(3600_000);

        var config = new LogDbConfig(storage, segmentation, ReadVisibility.REMOTE);

        assertThat(config.storage()).isEqualTo(storage);
        assertThat(config.segmentation()).isEqualTo(segmentation);
        assertThat(config.readVisibility()).isEqualTo(ReadVisibility.REMOTE);
    }

    @Test
    void shouldCreateWithStorageOnlyUsingDefaults() {
        var storage = new StorageConfig.InMemory();

        var config = new LogDbConfig(storage);

        assertThat(config.storage()).isEqualTo(storage);
        assertThat(config.segmentation()).isEqualTo(SegmentConfig.DEFAULT);
        assertThat(config.readVisibility()).isEqualTo(ReadVisibility.MEMORY);
    }

    @Test
    void shouldCreateInMemoryConfig() {
        var config = LogDbConfig.inMemory();

        assertThat(config.storage()).isInstanceOf(StorageConfig.InMemory.class);
        assertThat(config.segmentation()).isEqualTo(SegmentConfig.DEFAULT);
        assertThat(config.readVisibility()).isEqualTo(ReadVisibility.MEMORY);
    }

    @Test
    void shouldRejectNullStorage() {
        assertThatThrownBy(() -> new LogDbConfig(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storage");
    }

    @Test
    void shouldRejectNullSegmentation() {
        var storage = new StorageConfig.InMemory();
        assertThatThrownBy(() -> new LogDbConfig(storage, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segmentation");
    }

    @Test
    void shouldRejectNullReadVisibility() {
        var storage = new StorageConfig.InMemory();
        assertThatThrownBy(() -> new LogDbConfig(storage, SegmentConfig.DEFAULT, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("readVisibility");
    }

    @Test
    void shouldCreateSlateDbConfig() {
        var config = new LogDbConfig(
                new StorageConfig.SlateDb(
                        "data",
                        new ObjectStoreConfig.Aws("us-west-2", "my-bucket"),
                        "slatedb.toml"
                ),
                SegmentConfig.withSealInterval(60_000)
        );

        assertThat(config.storage()).isInstanceOf(StorageConfig.SlateDb.class);
        var slateDb = (StorageConfig.SlateDb) config.storage();
        assertThat(slateDb.path()).isEqualTo("data");
        assertThat(slateDb.objectStore()).isInstanceOf(ObjectStoreConfig.Aws.class);
    }
}
