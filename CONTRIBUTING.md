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

- `log/src` - Java interface classes and Panama FFM bindings (`dev.opendata` package)
- `common/src` - Shared utilities, configuration records, and exceptions

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
- Java 24+ ([SDKMAN](https://sdkman.io/) recommended)
- Git
- Local clone of [opendata](https://github.com/opendata-oss/opendata) as sibling directory

### Directory Structure

```
your-workspace/
├── opendata/          # Clone of opendata repo
└── opendata-java/     # This project
```

### Building

```bash
# Build the native C library
cd ../opendata/log/c
cargo build --release

# Build Java modules (generates FFM bindings via jextract)
cd ../../../opendata-java
./gradlew build
```

## Code Style

### Java

- Follow standard Java conventions
- Use 4-space indentation
- Prefer records for immutable data classes
- Package: `dev.opendata`

### Guidelines

- Write clear, self-documenting code
- Add comments for complex logic, especially FFM boundary handling
- Prefer returning errors over panicking in native code

## Testing

Include tests for new functionality.

### Java Test Style

```java
@Test
void shouldAppendAndReadEntry() {
    // given
    byte[] key = "test-key".getBytes();
    byte[] value = "test-value".getBytes();

    // when
    AppendResult result = log.tryAppend(key, value);

    // then
    assertThat(result.sequence()).isGreaterThan(0);
}
```

### Running Tests

```bash
# Build C library first
cd ../opendata/log/c && cargo build --release

# Java tests (requires C library built)
cd ../../../opendata-java
./gradlew test
```

## Pull Request Process

1. Ensure your code builds and tests pass
2. Update documentation if needed
3. Add a clear description of changes
4. Reference any related issues

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
