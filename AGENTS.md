# AGENTS.md

**Start here**: Read [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow, code style, and testing patterns.

## Project Overview

OpenData Java provides Java bindings for [OpenData](https://github.com/opendata-oss/opendata) systems via JNI. Each binding module contains both Rust JNI glue code and Java interface classes.

**Important**: This project depends on a sibling clone of the `opendata` repository. The Rust JNI code references OpenData crates via relative paths (`../../../opendata/`).

### Modules

- **common**: Shared Java utilities, configuration records, and exceptions (`dev.opendata.common` package)
- **log**: Java bindings for OpenData Log with Rust JNI bridge (`dev.opendata` package)

### Directory Structure

```
opendata-java/
├── common/
│   └── src/main/java/dev/opendata/common/
│       ├── StorageConfig.java      # Sealed interface: InMemory, SlateDb
│       ├── ObjectStoreConfig.java  # Sealed interface: InMemory, Aws, Local
│       └── OpenDataNativeException.java
├── log/
│   ├── native/
│   │   ├── Cargo.toml              # Rust JNI crate
│   │   └── src/lib.rs              # JNI implementation
│   └── src/main/java/dev/opendata/
│       ├── LogDb.java              # Main write API
│       ├── LogDbReader.java        # Read-only API
│       ├── LogDbConfig.java        # Configuration record
│       └── ...
├── build.gradle                    # Gradle multi-module build
└── settings.gradle
```

## JNI Architecture

### Native Method Pattern

Java classes declare native methods and load the shared library:

```java
public class LogDb implements AutoCloseable {
    static {
        System.loadLibrary("opendata_log_jni");
    }

    private static native long nativeCreate(LogDbConfig config);
    private static native void nativeClose(long handle);
}
```

Rust implements JNI functions following the naming convention `Java_<package>_<class>_<method>`:

```rust
#[no_mangle]
pub extern "system" fn Java_dev_opendata_LogDb_nativeCreate(
    mut env: JNIEnv,
    _class: JClass,
    config: JObject,
) -> jlong { ... }
```

### Handle-Based Resource Management

- Native resources are represented as `long` handles in Java
- Rust stores actual objects (e.g., `LogDb`, Tokio runtime) behind the handle
- Java classes implement `AutoCloseable` to ensure cleanup via `nativeClose`

### Async Bridge

The Rust JNI layer bridges synchronous JNI calls to async OpenData operations:

- Uses two Tokio runtimes: one for user operations, one for SlateDB compaction (prevents deadlock)
- JNI methods call `runtime.block_on(async_operation)` to wait for results
- Error handling converts Rust errors to `OpenDataNativeException`

### Timestamp Header

The JNI layer prepends an 8-byte timestamp to values for latency measurement:

```
┌─────────────────────┬──────────────────────┐
│ timestamp_ms (8B)   │ original payload     │
│ big-endian i64      │                      │
└─────────────────────┴──────────────────────┘
```

## Development

### Prerequisites

- Rust stable toolchain
- Java 24+
- Sibling clone of `opendata` repository

### Building

```bash
# Build native JNI library
cd log/native
cargo build --release

# Build Java modules
cd ../..
./gradlew build
```

### Testing

```bash
# Rust tests
cd log/native
cargo test

# Java tests (requires native library)
./gradlew test
```

### Formatting and Linting

Always run before committing:

```bash
# Rust
cd log/native
cargo fmt
cargo clippy --all-targets -- -D warnings

# Java uses standard conventions (4-space indent)
```

## Code Conventions

### Configuration Records

Use Java sealed interfaces with records for type-safe configuration:

```java
public sealed interface StorageConfig {
    record InMemory() implements StorageConfig {}
    record SlateDb(String path, ObjectStoreConfig objectStore) implements StorageConfig {}
}
```

The Rust JNI layer extracts these via reflection (`instanceof` checks and field access).

### Tests

Use the **given/when/then** pattern with `should_` naming:

**Rust:**
```rust
#[test]
fn should_extract_config_fields() {
    // given
    let config = create_test_config();

    // when
    let result = extract_fields(&config);

    // then
    assert!(result.is_ok());
}
```

**Java:**
```java
@Test
void shouldAppendAndScanEntries() {
    // given
    var config = new LogDbConfig(new StorageConfig.InMemory(), null);

    // when
    try (var log = LogDb.create(config)) {
        log.append("key".getBytes(), "value".getBytes());
    }

    // then
    // assertions...
}
```

### Error Handling

- Rust JNI code should return errors via exceptions, not panics
- Use `OpenDataNativeException` for all native errors
- Document any overhead in `lib.rs` header comments

### Imports

Rust: Place all `use` statements at module level, not inside functions.

Java: Standard import ordering (java.*, external, project).
