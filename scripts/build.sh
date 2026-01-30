#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Building OpenData OMB..."

# Build native JNI library
echo "Building native library..."
cd "$PROJECT_ROOT/log-jni/native"
cargo build --release

# Determine library extension based on OS
case "$(uname -s)" in
    Darwin*) LIB_EXT="dylib" ;;
    Linux*)  LIB_EXT="so" ;;
    *)       echo "Unsupported OS"; exit 1 ;;
esac

# Copy native library to a location Java can find
NATIVE_LIB="$PROJECT_ROOT/log-jni/native/target/release/libopendata_log_jni.$LIB_EXT"
if [ -f "$NATIVE_LIB" ]; then
    mkdir -p "$PROJECT_ROOT/log-jni/java/src/main/resources/native"
    cp "$NATIVE_LIB" "$PROJECT_ROOT/log-jni/java/src/main/resources/native/"
    echo "Native library copied to resources"
fi

# Build Java modules
echo "Building Java modules..."
cd "$PROJECT_ROOT"
mvn clean install -DskipTests

echo "Build complete!"
