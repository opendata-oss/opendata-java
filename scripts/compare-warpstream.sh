#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# This script runs equivalent benchmarks on both OpenData Log and WarpStream
# for direct comparison.

echo "=== OpenData vs WarpStream Comparison Benchmark ==="
echo ""
echo "This script requires:"
echo "  1. OpenData Log running and configured"
echo "  2. WarpStream cluster running"
echo "  3. OMB installed with both drivers"
echo ""

# TODO: Implement comparison benchmark execution
# For now, this is a placeholder showing the intended workflow

cat << 'EOF'
Planned workflow:

1. Run OpenData benchmark:
   ./run-benchmark.sh ../workloads/simple-1p-1c.yaml ../driver-config/opendata.yaml

2. Run WarpStream benchmark with equivalent config:
   $OMB_HOME/bin/benchmark \
     --drivers warpstream.yaml \
     --workers localhost \
     workloads/simple-1p-1c.yaml

3. Compare results:
   - Throughput (msg/s, MB/s)
   - P50, P99, P99.9 latency
   - E2E latency (requires timestamp support)

See: https://github.com/opendata-oss/opendata/issues/130
EOF
