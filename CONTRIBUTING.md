# Contributing to OpenData Java

Thank you for your interest in contributing! This project provides Java bindings for OpenData systems.

## Table of Contents

- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
- [Development Setup](#development-setup)
- [Code Style](#code-style)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)

## Getting Started

This project contains:

- `log/native` - Rust JNI bindings to the OpenData Log
- `log/src` - Java interface classes (`dev.opendata` package)

Before contributing, read the [README](README.md).

## How to Contribute

### Reporting Bugs

Open an issue with:

- Steps to reproduce the issue
- Expected vs actual behavior
- Your environment (OS, Java version, Rust version)
- Relevant logs or error messages

### Suggesting Features

Open an issue describing:

- The feature and the problem it solves
- Any implementation ideas (optional)

### Contributing Code

1. For significant changes, open an issue first to discuss your approach
2. Fork the repository and create a branch for your work
3. Write code and tests
4. Submit a pull request

## Development Setup

### Prerequisites

- Rust stable toolchain ([rustup](https://rustup.rs/))
- Java 17+ ([SDKMAN](https://sdkman.io/) recommended)
- Maven 3.8+
- Git
- Local clone of [opendata](https://github.com/opendata-oss/opendata) as sibling directory

### Directory Structure

```
your-workspace/
├── opendata/          # Clone of opendata repo
└── opendata-java/     # This project (opendata-omb)
```

### Building

```bash
# Build native JNI library
cd log/native
cargo build --release

# Build Java modules
cd ../..
mvn clean install -DskipTests
```

## Code Style

### Rust

Run `cargo fmt` before committing:

```bash
cd log/native
cargo fmt
```

All code must pass clippy with no warnings:

```bash
cargo clippy --all-targets -- -D warnings
```

### Java

- Follow standard Java conventions
- Use 4-space indentation
- Prefer records for immutable data classes
- Package: `dev.opendata`

### Guidelines

- Write clear, self-documenting code
- Add comments for complex logic, especially JNI boundary handling
- Document any overhead introduced (see `lib.rs` header)
- Prefer returning errors over panicking in JNI code

## Testing

Include tests for new functionality.

### Rust Test Style

Use the `should_` prefix and given/when/then pattern:

```rust
#[test]
fn should_extract_timestamp_from_header() {
    // given
    let value = create_timestamped_value(12345, b"payload");

    // when
    let (timestamp, payload) = extract_timestamp_and_payload(&value);

    // then
    assert_eq!(timestamp, 12345);
    assert_eq!(payload, b"payload");
}
```

### Java Test Style

```java
@Test
void shouldAppendAndReadEntry() {
    // given
    byte[] key = "test-key".getBytes();
    byte[] value = "test-value".getBytes();

    // when
    AppendResult result = log.append(key, value);

    // then
    assertThat(result.sequence()).isGreaterThan(0);
}
```

### Running Tests

```bash
# Rust tests
cd log/native
cargo test

# Java tests (requires native library built)
mvn test -Djava.library.path=log/native/target/release
```

## Pull Request Process

1. Ensure your code builds and tests pass
2. Run `cargo fmt` and `cargo clippy`
3. Update documentation if needed
4. Add a clear description of changes
5. Reference any related issues

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
