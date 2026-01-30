# OpenData Java

Java bindings for OpenData systems.

## Overview

This project provides Java bindings for OpenData components via JNI, enabling use from Java applications and frameworks like the OpenMessaging Benchmark.

## Modules

| Module | Maven Coordinates | Description |
|--------|-------------------|-------------|
| `common` | `dev.opendata:common` | Common utilities and exceptions |
| `log` | `dev.opendata:log` | Java bindings for OpenData Log |

## Project Structure

```
opendata-java/
├── common/
│   └── src/main/java/dev/opendata/common/
│       └── OpenDataNativeException.java
├── log/
│   ├── native/                    # Rust JNI implementation
│   │   ├── Cargo.toml
│   │   └── src/lib.rs
│   └── src/main/java/dev/opendata/
│       ├── Log.java               # Main entry point
│       ├── LogReader.java         # Read interface
│       ├── LogEntry.java          # Entry record
│       ├── AppendResult.java      # Append result record
│       └── StorageType.java       # Storage backend enum
└── pom.xml
```

## Usage

### Maven Dependency

```xml
<dependency>
    <groupId>dev.opendata</groupId>
    <artifactId>log</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Example

```java
import dev.opendata.Log;
import dev.opendata.LogReader;
import dev.opendata.LogEntry;
import dev.opendata.StorageType;

// Open a log with in-memory storage
try (Log log = Log.open(StorageType.IN_MEMORY, null, null, null, null)) {
    // Append an entry
    byte[] key = "my-key".getBytes();
    byte[] value = "hello world".getBytes();
    var result = log.append(key, value);
    System.out.println("Appended at sequence: " + result.sequence());

    // Read entries
    try (LogReader reader = log.reader()) {
        var entries = reader.read(key, 0, 100);
        for (LogEntry entry : entries) {
            System.out.println("Read: " + new String(entry.value()));
        }
    }
}
```

### Storage Backends

| Type | Description |
|------|-------------|
| `IN_MEMORY` | Fast, non-persistent storage for testing |
| `SLATEDB` | Persistent storage backed by SlateDB |

For SlateDB, you can configure the object store:

```java
// Local filesystem
Log log = Log.open(StorageType.SLATEDB, "/tmp/mylog", "local", null, null);

// Amazon S3
Log log = Log.open(StorageType.SLATEDB, "mylog", "s3", "my-bucket", "us-east-1");
```

## Building

### Prerequisites

- Rust stable toolchain ([rustup](https://rustup.rs/))
- Java 17+
- Maven 3.8+
- Local clone of [opendata](https://github.com/opendata-oss/opendata) as sibling directory

### Build Steps

```bash
# Build native library
cd log/native
cargo build --release

# Build and install Java modules
cd ../..
mvn clean install
```

## JNI Overhead

The JNI bindings introduce overhead compared to native Rust usage:

- **Data copies**: Java `byte[]` ↔ Rust `Bytes` requires heap copies
- **Async bridging**: `block_on()` for each JNI call into async Log API
- **JNI marshalling**: Baseline cost per native method invocation

See [`log/native/src/lib.rs`](log/native/src/lib.rs) for detailed documentation.

## OpenMessaging Benchmark Integration

The OMB driver for OpenData has moved to [openmessaging-benchmark](https://github.com/openmessaging/benchmark):

```
openmessaging-benchmark/driver-opendata/
```

See the driver's README for usage instructions.

## Related

- [OpenData](https://github.com/opendata-oss/opendata) - The upstream Rust implementation
- [OpenMessaging Benchmark](https://github.com/openmessaging/benchmark) - Benchmark framework
