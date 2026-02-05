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
```bash
# Build native library (fetches opendata dependency via git automatically)
cd log/native
cargo build --release

# Build and test Java modules
cd ../..
mvn verify -Djava.library.path=log/native/target/release
```
