# AGENTS.md

**Start here**: Read [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow, code style, and testing patterns.

## Project Overview

OpenData Java provides Java bindings for [OpenData](https://github.com/opendata-oss/opendata) systems via Panama FFM (Foreign Function & Memory). Each binding module uses jextract to generate Java bindings from C headers exposed by the OpenData native libraries.

**Important**: This project depends on a sibling clone of the `opendata` repository. The C library headers are referenced via relative paths (`../../opendata/`).

### Modules

- **common**: Shared Java utilities, configuration records, and exceptions (`dev.opendata.common` package)
- **log**: Java bindings for OpenData Log via Panama FFM (`dev.opendata` package)

### Directory Structure

```
opendata-java/
├── common/
│   └── src/main/java/dev/opendata/common/
│       ├── StorageConfig.java      # Sealed interface: InMemory, SlateDb
│       ├── ObjectStoreConfig.java  # Sealed interface: InMemory, Aws, Local
│       └── OpenDataNativeException.java
├── log/
│   └── src/main/java/dev/opendata/
│       ├── LogDb.java              # Main write API
│       ├── LogDbReader.java        # Read-only API
│       ├── RecordBatch.java        # Zero-copy batch builder for writes
│       ├── Record.java             # Single record (key + value + timestamp)
│       ├── LogScanIterator.java    # Heap-copying iterator over scan results
│       ├── LogScanRawIterator.java # Zero-copy iterator (native memory views)
│       ├── NativeInterop.java      # Panama FFM interop layer
│       ├── LogDbConfig.java        # Configuration record
│       └── ...
├── build.gradle                    # Gradle multi-module build
└── settings.gradle
```

## Panama FFM Architecture

### jextract Bindings

The `log/build.gradle` uses the `de.infolektuell.jextract` Gradle plugin to generate Java bindings from the C header at `opendata/log/c/include/opendata_log.h`. Generated code lives in the `dev.opendata.ffi` package under the `Native` class.

### NativeInterop Layer

`NativeInterop.java` is the package-private interop layer that wraps the generated FFM bindings:

```java
// Handle-based resource management with AtomicBoolean for thread safety
static abstract class NativeHandle implements AutoCloseable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile MemorySegment segment;
    // ...
}

// Concrete handles: LogHandle, ReaderHandle, IteratorHandle, ObjectStoreHandle
```

### Error Handling

Every C API call returns `opendata_log_result_t`. The `checkResult()` method inspects the error kind and throws the appropriate Java exception:

- `QUEUE_FULL` → `QueueFullException`
- `TIMEOUT` → `AppendTimeoutException`
- All others → `OpenDataNativeException`

### Write Paths

There are two write paths through `NativeInterop`:

- **`Record[]`** — `doAppend()` copies each record's `byte[] key` and `byte[] value` into arena-allocated segments at append time. Simple but involves per-record allocation.
- **`RecordBatch`** — `doAppendBatch()` slices into the batch's pre-built contiguous segments (zero-copy pointer building). The batch builder (`RecordBatch`) accumulates records into two contiguous `MemorySegment`s (keys and values) with timestamp headers already prepended, so the append path only needs to build the pointer arrays.

### Timestamp Header

The Java layer prepends an 8-byte timestamp to values for latency measurement:

```
┌─────────────────────┬──────────────────────┐
│ timestamp_ms (8B)   │ original payload     │
│ big-endian i64      │                      │
└─────────────────────┴──────────────────────┘
```

## Development

### Prerequisites

- Rust stable toolchain (for building the C library)
- Java 24+
- Sibling clone of `opendata` repository

### Building

```bash
# Build the C library
cd ../opendata/log/c
cargo build --release

# Build Java modules (generates FFM bindings via jextract)
cd ../../../opendata-java
./gradlew build
```

### Testing

```bash
# Java tests (requires C library built)
./gradlew test

# Log integration tests specifically
./gradlew :log:test --tests "dev.opendata.*"
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

### Tests

Use the **given/when/then** pattern with `should` naming:

```java
@Test
void shouldAppendAndScanEntries() {
    try (LogDb log = LogDb.openInMemory()) {
        log.tryAppend(key, value);

        try (LogScanIterator iter = log.scan(key, 0)) {
            // assertions...
        }
    }
}
```

### Error Handling

- Use `OpenDataNativeException` for all native errors
- Specific subclasses for recoverable errors (QueueFullException, AppendTimeoutException)

### Imports

Java: Standard import ordering (java.*, external, project).
