package dev.opendata.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObjectStoreConfigTest {

    @Test
    void shouldCreateInMemoryConfig() {
        var config = new ObjectStoreConfig.InMemory();
        assertThat(config).isInstanceOf(ObjectStoreConfig.class);
    }

    @Test
    void shouldCreateAwsConfig() {
        var config = new ObjectStoreConfig.Aws("us-west-2", "my-bucket");

        assertThat(config.region()).isEqualTo("us-west-2");
        assertThat(config.bucket()).isEqualTo("my-bucket");
    }

    @Test
    void shouldRejectNullRegion() {
        assertThatThrownBy(() -> new ObjectStoreConfig.Aws(null, "bucket"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("region");
    }

    @Test
    void shouldRejectBlankRegion() {
        assertThatThrownBy(() -> new ObjectStoreConfig.Aws("  ", "bucket"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("region");
    }

    @Test
    void shouldRejectNullBucket() {
        assertThatThrownBy(() -> new ObjectStoreConfig.Aws("us-west-2", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucket");
    }

    @Test
    void shouldRejectBlankBucket() {
        assertThatThrownBy(() -> new ObjectStoreConfig.Aws("us-west-2", "  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucket");
    }

    @Test
    void shouldCreateLocalConfig() {
        var config = new ObjectStoreConfig.Local("/data/storage");

        assertThat(config.path()).isEqualTo("/data/storage");
    }

    @Test
    void shouldRejectNullLocalPath() {
        assertThatThrownBy(() -> new ObjectStoreConfig.Local(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path");
    }

    @Test
    void shouldRejectBlankLocalPath() {
        assertThatThrownBy(() -> new ObjectStoreConfig.Local("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path");
    }

    @Test
    void shouldSupportInstanceofPatternMatching() {
        ObjectStoreConfig inMemory = new ObjectStoreConfig.InMemory();
        ObjectStoreConfig aws = new ObjectStoreConfig.Aws("us-east-1", "bucket");
        ObjectStoreConfig local = new ObjectStoreConfig.Local("/tmp");

        assertThat(describeConfig(inMemory)).isEqualTo("in-memory");
        assertThat(describeConfig(aws)).isEqualTo("s3://bucket");
        assertThat(describeConfig(local)).isEqualTo("file:///tmp");
    }

    private String describeConfig(ObjectStoreConfig config) {
        if (config instanceof ObjectStoreConfig.InMemory) {
            return "in-memory";
        } else if (config instanceof ObjectStoreConfig.Aws aws) {
            return "s3://" + aws.bucket();
        } else if (config instanceof ObjectStoreConfig.Local local) {
            return "file://" + local.path();
        }
        throw new IllegalArgumentException("Unknown config type");
    }
}
