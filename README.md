# OpenData Java

Java bindings for OpenData systems.

## Modules

| Module | Maven Coordinates | Description |
|--------|-------------------|-------------|
| `common` | `dev.opendata:common` | Common utilities and exceptions |
| `log` | `dev.opendata:log` | Java bindings for OpenData Log |

## Building

Prerequisites:
- Rust toolchain
- Java 17+
- Maven 3.8+
- Local clone of [opendata](https://github.com/opendata-oss/opendata) as sibling directory

```bash
# Build native library
cd log/native
cargo build --release

# Build Java modules
cd ../..
mvn clean install
```
