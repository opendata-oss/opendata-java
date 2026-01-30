#!/bin/bash
#
# OpenData OMB Benchmark Runner
#
# Usage:
#   ./run-benchmark.sh [options]
#
# Options:
#   --driver-config <file>   Driver config (default: driver-config/opendata.yaml)
#   --workload <file>        Workload config (default: workloads/simple.yaml)
#   --build                  Force rebuild of all components
#   --help                   Show this help message
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OMB_DIR="${SCRIPT_DIR}/../openmessaging-benchmark"
OPENDATA_DIR="${SCRIPT_DIR}/../opendata"

# Defaults
DRIVER_CONFIG="${SCRIPT_DIR}/driver-config/opendata.yaml"
WORKLOAD_CONFIG="${SCRIPT_DIR}/workloads/simple.yaml"
FORCE_BUILD=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --driver-config)
            DRIVER_CONFIG="$2"
            shift 2
            ;;
        --workload)
            WORKLOAD_CONFIG="$2"
            shift 2
            ;;
        --build)
            FORCE_BUILD=true
            shift
            ;;
        --help)
            sed -n '2,14p' "$0" | sed 's/^# //' | sed 's/^#//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=== OpenData OMB Benchmark ==="
echo ""

# Step 1: Check/Build OMB
echo "[1/4] Checking OpenMessaging Benchmark..."
if [[ ! -d "$OMB_DIR" ]]; then
    echo "  OMB not found at $OMB_DIR"
    echo "  Please clone: git clone https://github.com/openmessaging/benchmark.git ../openmessaging-benchmark"
    exit 1
fi

OMB_JAR="$HOME/.m2/repository/io/openmessaging/benchmark/benchmark-framework/0.0.1-SNAPSHOT/benchmark-framework-0.0.1-SNAPSHOT.jar"
if [[ ! -f "$OMB_JAR" ]] || [[ "$FORCE_BUILD" == "true" ]]; then
    echo "  Building OMB..."
    (cd "$OMB_DIR" && mvn install -pl driver-api,benchmark-framework -am -DskipTests \
        -Dspotless.check.skip=true -Dspotbugs.skip=true -Dcheckstyle.skip=true -q)
    echo "  OMB installed."
else
    echo "  OMB already installed."
fi

# Step 2: Build native library
echo "[2/4] Building native library..."
NATIVE_DIR="${SCRIPT_DIR}/log-jni/native"
NATIVE_LIB_NAME="libopendata_log_jni.dylib"
if [[ "$(uname)" == "Linux" ]]; then
    NATIVE_LIB_NAME="libopendata_log_jni.so"
fi
NATIVE_LIB="${NATIVE_DIR}/target/release/${NATIVE_LIB_NAME}"

if [[ ! -f "$NATIVE_LIB" ]] || [[ "$FORCE_BUILD" == "true" ]]; then
    echo "  Building Rust JNI library..."
    (cd "$NATIVE_DIR" && cargo build --release -q)
    echo "  Native library built."
else
    echo "  Native library already built."
fi

# Step 3: Build opendata-omb
echo "[3/4] Building OpenData OMB driver..."
DRIVER_JAR="${SCRIPT_DIR}/benchmark-driver/target/benchmark-driver-0.1.0-SNAPSHOT.jar"
if [[ ! -f "$DRIVER_JAR" ]] || [[ "$FORCE_BUILD" == "true" ]]; then
    (cd "$SCRIPT_DIR" && mvn install -DskipTests -q)
    echo "  Driver built."
else
    echo "  Driver already built."
fi

# Step 4: Create workload if it doesn't exist
echo "[4/4] Checking workload configuration..."
if [[ ! -f "$WORKLOAD_CONFIG" ]]; then
    mkdir -p "$(dirname "$WORKLOAD_CONFIG")"
    cat > "$WORKLOAD_CONFIG" << 'EOF'
# Simple test workload
name: simple-test

topics: 1
partitionsPerTopic: 1
messageSize: 1024
payloadFile:
subscriptionsPerTopic: 1
consumerPerSubscription: 1
producersPerTopic: 1
producerRate: 1000
consumerBacklogSizeGB: 0
warmupDurationMinutes: 1
testDurationMinutes: 5
EOF
    echo "  Created default workload: $WORKLOAD_CONFIG"
else
    echo "  Using workload: $WORKLOAD_CONFIG"
fi

# Set up library path
if [[ "$(uname)" == "Darwin" ]]; then
    export DYLD_LIBRARY_PATH="${NATIVE_DIR}/target/release:${DYLD_LIBRARY_PATH:-}"
else
    export LD_LIBRARY_PATH="${NATIVE_DIR}/target/release:${LD_LIBRARY_PATH:-}"
fi

# Build classpath
CLASSPATH="${SCRIPT_DIR}/benchmark-driver/target/classes"
CLASSPATH="${CLASSPATH}:${SCRIPT_DIR}/log-jni/java/target/classes"

# Add all dependencies from Maven
for jar in ${SCRIPT_DIR}/benchmark-driver/target/dependency/*.jar; do
    if [[ -f "$jar" ]]; then
        CLASSPATH="${CLASSPATH}:${jar}"
    fi
done

# If dependency jars don't exist, copy them
if [[ ! -d "${SCRIPT_DIR}/benchmark-driver/target/dependency" ]]; then
    echo "  Copying dependencies..."
    (cd "$SCRIPT_DIR" && mvn dependency:copy-dependencies -pl benchmark-driver -q)
fi

for jar in ${SCRIPT_DIR}/benchmark-driver/target/dependency/*.jar; do
    if [[ -f "$jar" ]]; then
        CLASSPATH="${CLASSPATH}:${jar}"
    fi
done

echo ""
echo "=== Running Benchmark ==="
echo "  Driver config: $DRIVER_CONFIG"
echo "  Workload: $WORKLOAD_CONFIG"
echo ""

# Run the benchmark (local mode - no workers needed)
NATIVE_LIB_DIR="${NATIVE_DIR}/target/release"
exec java \
    --enable-native-access=ALL-UNNAMED \
    -Djava.library.path="$NATIVE_LIB_DIR" \
    -cp "$CLASSPATH" \
    io.openmessaging.benchmark.Benchmark \
    --drivers "$DRIVER_CONFIG" \
    "$WORKLOAD_CONFIG"
