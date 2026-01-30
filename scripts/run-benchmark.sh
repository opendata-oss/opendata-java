#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default paths
WORKLOAD="${1:-$PROJECT_ROOT/workloads/simple-1p-1c.yaml}"
DRIVER_CONFIG="${2:-$PROJECT_ROOT/driver-config/opendata.yaml}"
OMB_HOME="${OMB_HOME:-$HOME/benchmark}"

if [ ! -d "$OMB_HOME" ]; then
    echo "Error: OMB_HOME not found at $OMB_HOME"
    echo "Please set OMB_HOME to your OpenMessaging Benchmark installation"
    exit 1
fi

echo "Running benchmark..."
echo "  Workload: $WORKLOAD"
echo "  Driver config: $DRIVER_CONFIG"

# Add our driver to the classpath
DRIVER_JAR="$PROJECT_ROOT/benchmark-driver/target/benchmark-driver-0.1.0-SNAPSHOT.jar"
JNI_JAR="$PROJECT_ROOT/log-jni/java/target/log-jni-0.1.0-SNAPSHOT.jar"

# Set native library path
export JAVA_OPTS="${JAVA_OPTS:-} -Djava.library.path=$PROJECT_ROOT/log-jni/native/target/release"

cd "$OMB_HOME"
bin/benchmark \
    --drivers "$DRIVER_CONFIG" \
    --workers localhost \
    "$WORKLOAD"
