# OpenData OMB

OpenMessaging Benchmark driver for OpenData Log.

See: https://github.com/opendata-oss/opendata/issues/130

## Project Structure

```
opendata-omb/
├── log-jni/           # JNI bindings to Rust Log trait
│   ├── native/        # Rust JNI implementation
│   └── java/          # Java interface classes
├── benchmark-driver/  # OMB driver implementation
├── workloads/         # OMB workload configurations
├── driver-config/     # Driver configuration files
└── scripts/           # Build and run scripts
```

## Building

```bash
./scripts/build.sh
```

## Running Benchmarks

```bash
./scripts/run-benchmark.sh workloads/simple-1p-1c.yaml
```

## Benchmark Overhead

The JNI bindings introduce overhead compared to native Rust usage:

- **Data copies**: Java `byte[]` ↔ Rust `Bytes` requires heap copies (GC safety)
- **Async bridging**: `block_on()` for each JNI call into async Log API
- **JNI marshalling**: Baseline cost per native method invocation

See [`log-jni/native/src/lib.rs`](log-jni/native/src/lib.rs) for detailed documentation.

For fair comparison with native-client systems (e.g., WarpStream), consider that
this overhead is constant per operation and relatively smaller for larger payloads.

## Status

- [x] JNI bindings (compiles, uses local opendata dep)
- [ ] OMB driver (stubbed)
- [ ] Polling-based consumer
- [ ] Push-based consumer (pending upstream API)
- [ ] Timestamps in AppendResult/LogEntry (pending upstream)
