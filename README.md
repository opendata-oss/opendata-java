# OpenData Java

Java bindings for OpenData systems.

## Modules

| Module | Description |
|--------|-------------|
| `common` | Common utilities and exceptions |
| `log` | Java bindings for OpenData Log |

## Building

Prerequisites:
- Rust toolchain
- Java 24+
- Sibling clone of [opendata](https://github.com/opendata-oss/opendata) repository
```bash
# Build the native C library
cd ../opendata/log/c
cargo build --release

# Build and test Java modules
cd ../../../opendata-java
./gradlew build
```
