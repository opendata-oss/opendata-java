//! JNI bindings for OpenData LogDb.
//!
//! This crate provides Java Native Interface bindings to the OpenData LogDb,
//! enabling use from the OpenMessaging Benchmark framework.
//!
//! # Timestamp Header
//!
//! The upstream LogDb API does not yet support timestamps. To enable OMB latency
//! measurement, this layer prepends an 8-byte timestamp header to each value:
//!
//! ```text
//! ┌─────────────────────┬──────────────────────┐
//! │ timestamp_ms (8B)   │ original payload     │
//! │ big-endian i64      │                      │
//! └─────────────────────┴──────────────────────┘
//! ```
//!
//! - On `append`: timestamp from Java Record is prepended to the value (captured at submission time)
//! - On `read`: timestamp is extracted from the header and returned separately
//!
//! This is transparent to the Java caller and will be removed once upstream
//! adds native timestamp support.
//!
//! # Benchmark Overhead
//!
//! These bindings introduce overhead compared to native Rust usage. When
//! interpreting benchmark results, consider the following costs:
//!
//! ## Data Copies
//!
//! | Operation | Copies | Notes |
//! |-----------|--------|-------|
//! | `append(key, value)` | 1 + 1 | key: Java→Rust; value: Java→Rust (directly into timestamped buffer) |
//! | `read()` → entries | 2 per entry | Rust `Bytes` → Java `byte[]` for key and value |
//!
//! The value copy on append is optimized using `get_byte_array_region` to copy
//! directly into a pre-allocated buffer that includes space for the timestamp
//! header, avoiding an intermediate allocation.
//!
//! ## Async Runtime
//!
//! The LogDb API is async, but JNI calls are synchronous. We maintain a global
//! Tokio runtime and use `block_on()` for each operation. This adds:
//! - Thread context switching overhead
//! - Potential contention on the runtime's task scheduler
//!
//! ## JNI Call Overhead
//!
//! Each native method invocation has baseline JNI overhead (~tens of nanoseconds)
//! for argument marshalling and stack frame setup. This is negligible for
//! non-trivial operations but adds up for high-frequency calls.
//!
//! ## Comparison Baseline
//!
//! For fair comparison with systems like WarpStream (which use native clients),
//! consider that this JNI layer adds constant overhead per operation. The
//! overhead should be relatively smaller for larger payloads and batch sizes.

use bytes::Bytes;
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JValue};
use jni::sys::{jlong, jobject, jobjectArray};
use jni::JNIEnv;
use tokio::runtime::{Handle, Runtime};

/// Size of the timestamp header prepended to values.
const TIMESTAMP_HEADER_SIZE: usize = 8;

// Re-export log crate types with explicit naming to avoid confusion with std log
use common::storage::config::{
    AwsObjectStoreConfig, LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    StorageConfig,
};
use common::StorageRuntime;
use log::{AppendResult, Config, LogDb, LogDbBuilder, LogDbReader, LogEntry, LogRead, Record};

/// Handle to a LogDb instance with its associated Tokio runtime.
///
/// Uses block_on for JNI operations. A separate compaction runtime is used for
/// SlateDB's compaction/GC tasks to prevent deadlock when the main runtime's
/// threads are blocked in JNI calls.
struct LogHandle {
    /// The LogDb instance
    log: LogDb,
    /// Handle to the runtime for async operations
    runtime_handle: Handle,
    /// The main runtime (kept alive for the lifetime of the LogDb)
    runtime: Option<Runtime>,
    /// Separate runtime for SlateDB compaction/GC tasks
    compaction_runtime: Option<Runtime>,
}

// =============================================================================
// LogDb JNI Methods
// =============================================================================

/// Creates a new LogDb instance with the specified configuration.
///
/// # Arguments
/// * `config` - Java LogDbConfig object
///
/// # Safety
/// This is a JNI function - must be called from Java with valid JNIEnv.
#[no_mangle]
pub extern "system" fn Java_dev_opendata_LogDb_nativeCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config: JObject<'local>,
) -> jlong {
    // Extract storage config from LogDbConfig
    let storage_config = match extract_storage_config(&mut env, &config) {
        Ok(c) => c,
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", e);
            return 0;
        }
    };

    let config = Config {
        storage: storage_config,
        ..Config::default()
    };

    // Create a dedicated runtime for this LogDb instance (for user operations)
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("opendata-log")
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            return 0;
        }
    };

    // Create a SEPARATE runtime for SlateDB compaction/GC tasks.
    // This prevents deadlock when the main runtime's threads are blocked in JNI calls
    // while SlateDB's background tasks need to make progress.
    let compaction_runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("opendata-compaction")
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            return 0;
        }
    };

    // Open the LogDb using LogDbBuilder with separate compaction runtime
    let result = runtime.block_on(async {
        let storage_runtime =
            StorageRuntime::new().with_compaction_runtime(compaction_runtime.handle().clone());
        LogDbBuilder::new(config)
            .with_storage_runtime(storage_runtime)
            .build()
            .await
    });

    match result {
        Ok(log) => {
            let handle = Box::new(LogHandle {
                log,
                runtime_handle: runtime.handle().clone(),
                runtime: Some(runtime),
                compaction_runtime: Some(compaction_runtime),
            });
            Box::into_raw(handle) as jlong
        }
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            0
        }
    }
}

// =============================================================================
// Config Extraction Helpers
// =============================================================================

/// Extracts StorageConfig from a Java LogDbConfig object.
fn extract_storage_config(
    env: &mut JNIEnv<'_>,
    config: &JObject<'_>,
) -> Result<StorageConfig, String> {
    // Get the storage field from LogDbConfig
    let storage_obj = env
        .call_method(
            config,
            "storage",
            "()Ldev/opendata/common/StorageConfig;",
            &[],
        )
        .map_err(|e| format!("Failed to get storage: {}", e))?
        .l()
        .map_err(|e| format!("Failed to get storage object: {}", e))?;

    // Check which type of StorageConfig it is using instanceof
    let in_memory_class = env
        .find_class("dev/opendata/common/StorageConfig$InMemory")
        .map_err(|e| format!("Failed to find InMemory class: {}", e))?;

    let slatedb_class = env
        .find_class("dev/opendata/common/StorageConfig$SlateDb")
        .map_err(|e| format!("Failed to find SlateDb class: {}", e))?;

    if env
        .is_instance_of(&storage_obj, &in_memory_class)
        .map_err(|e| format!("instanceof check failed: {}", e))?
    {
        Ok(StorageConfig::InMemory)
    } else if env
        .is_instance_of(&storage_obj, &slatedb_class)
        .map_err(|e| format!("instanceof check failed: {}", e))?
    {
        extract_slatedb_config(env, &storage_obj)
    } else {
        Err("Unknown StorageConfig type".to_string())
    }
}

/// Extracts SlateDbStorageConfig from a Java StorageConfig.SlateDb record.
fn extract_slatedb_config(
    env: &mut JNIEnv<'_>,
    slatedb_obj: &JObject<'_>,
) -> Result<StorageConfig, String> {
    // Get path field
    let path_obj = env
        .call_method(slatedb_obj, "path", "()Ljava/lang/String;", &[])
        .map_err(|e| format!("Failed to get path: {}", e))?
        .l()
        .map_err(|e| format!("Failed to get path object: {}", e))?;
    let path: String = env
        .get_string((&path_obj).into())
        .map_err(|e| format!("Failed to convert path: {}", e))?
        .into();

    // Get objectStore field
    let object_store_obj = env
        .call_method(
            slatedb_obj,
            "objectStore",
            "()Ldev/opendata/common/ObjectStoreConfig;",
            &[],
        )
        .map_err(|e| format!("Failed to get objectStore: {}", e))?
        .l()
        .map_err(|e| format!("Failed to get objectStore object: {}", e))?;
    let object_store = extract_object_store_config(env, &object_store_obj)?;

    // Get settingsPath field (nullable)
    let settings_path_obj = env
        .call_method(slatedb_obj, "settingsPath", "()Ljava/lang/String;", &[])
        .map_err(|e| format!("Failed to get settingsPath: {}", e))?
        .l()
        .map_err(|e| format!("Failed to get settingsPath object: {}", e))?;
    let settings_path = if settings_path_obj.is_null() {
        None
    } else {
        Some(
            env.get_string((&settings_path_obj).into())
                .map_err(|e| format!("Failed to convert settingsPath: {}", e))?
                .into(),
        )
    };

    Ok(StorageConfig::SlateDb(SlateDbStorageConfig {
        path,
        object_store,
        settings_path,
    }))
}

/// Extracts ObjectStoreConfig from a Java ObjectStoreConfig object.
fn extract_object_store_config(
    env: &mut JNIEnv<'_>,
    obj: &JObject<'_>,
) -> Result<ObjectStoreConfig, String> {
    let in_memory_class = env
        .find_class("dev/opendata/common/ObjectStoreConfig$InMemory")
        .map_err(|e| format!("Failed to find ObjectStoreConfig.InMemory class: {}", e))?;

    let aws_class = env
        .find_class("dev/opendata/common/ObjectStoreConfig$Aws")
        .map_err(|e| format!("Failed to find ObjectStoreConfig.Aws class: {}", e))?;

    let local_class = env
        .find_class("dev/opendata/common/ObjectStoreConfig$Local")
        .map_err(|e| format!("Failed to find ObjectStoreConfig.Local class: {}", e))?;

    if env
        .is_instance_of(obj, &in_memory_class)
        .map_err(|e| format!("instanceof check failed: {}", e))?
    {
        Ok(ObjectStoreConfig::InMemory)
    } else if env
        .is_instance_of(obj, &aws_class)
        .map_err(|e| format!("instanceof check failed: {}", e))?
    {
        // Extract region and bucket from Aws record
        let region_obj = env
            .call_method(obj, "region", "()Ljava/lang/String;", &[])
            .map_err(|e| format!("Failed to get region: {}", e))?
            .l()
            .map_err(|e| format!("Failed to get region object: {}", e))?;
        let region: String = env
            .get_string((&region_obj).into())
            .map_err(|e| format!("Failed to convert region: {}", e))?
            .into();

        let bucket_obj = env
            .call_method(obj, "bucket", "()Ljava/lang/String;", &[])
            .map_err(|e| format!("Failed to get bucket: {}", e))?
            .l()
            .map_err(|e| format!("Failed to get bucket object: {}", e))?;
        let bucket: String = env
            .get_string((&bucket_obj).into())
            .map_err(|e| format!("Failed to convert bucket: {}", e))?
            .into();

        Ok(ObjectStoreConfig::Aws(AwsObjectStoreConfig {
            region,
            bucket,
        }))
    } else if env
        .is_instance_of(obj, &local_class)
        .map_err(|e| format!("instanceof check failed: {}", e))?
    {
        // Extract path from Local record
        let path_obj = env
            .call_method(obj, "path", "()Ljava/lang/String;", &[])
            .map_err(|e| format!("Failed to get path: {}", e))?
            .l()
            .map_err(|e| format!("Failed to get path object: {}", e))?;
        let path: String = env
            .get_string((&path_obj).into())
            .map_err(|e| format!("Failed to convert path: {}", e))?
            .into();

        Ok(ObjectStoreConfig::Local(LocalObjectStoreConfig { path }))
    } else {
        Err("Unknown ObjectStoreConfig type".to_string())
    }
}

/// Appends a batch of records to the log with timestamp headers.
///
/// Each value is stored as: `[8-byte timestamp (big-endian i64)] + [original payload]`
/// The timestamp is read from each Java Record object (captured at submission time).
///
/// # Arguments
/// * `handle` - Native LogDb pointer
/// * `records` - Array of Java Record objects (each with key, value, timestampMs)
///
/// # Returns
/// AppendResult jobject with start_sequence and timestamp of first record
///
/// # Safety
/// JNI function - handle must be a valid pointer returned by nativeCreate.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_dev_opendata_LogDb_nativeAppend<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    records: jobjectArray,
) -> jobject {
    if handle == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "LogDb handle is null");
        return std::ptr::null_mut();
    }

    let log_handle = unsafe { &*(handle as *const LogHandle) };

    // Convert Java Record[] to Rust Vec<Record>
    let records_array = unsafe { JObjectArray::from_raw(records) };
    let len = match env.get_array_length(&records_array) {
        Ok(l) => l as usize,
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            return std::ptr::null_mut();
        }
    };

    if len == 0 {
        let _ = env.throw_new(
            "java/lang/IllegalArgumentException",
            "Records array is empty",
        );
        return std::ptr::null_mut();
    }

    let mut rust_records = Vec::with_capacity(len);
    let mut first_timestamp_ms: i64 = 0;

    for i in 0..len {
        let record_obj = match env.get_object_array_element(&records_array, i as i32) {
            Ok(obj) => obj,
            Err(e) => {
                let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
                return std::ptr::null_mut();
            }
        };

        // Extract key byte[] from Record
        let key_obj = match env.call_method(&record_obj, "key", "()[B", &[]) {
            Ok(v) => v.l().unwrap(),
            Err(e) => {
                let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
                return std::ptr::null_mut();
            }
        };
        let key_array: JByteArray = key_obj.into();
        let key_bytes = match env.convert_byte_array(&key_array) {
            Ok(b) => Bytes::from(b),
            Err(e) => {
                let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
                return std::ptr::null_mut();
            }
        };

        // Extract value byte[] from Record
        let value_obj = match env.call_method(&record_obj, "value", "()[B", &[]) {
            Ok(v) => v.l().unwrap(),
            Err(e) => {
                let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
                return std::ptr::null_mut();
            }
        };
        let value_array: JByteArray = value_obj.into();

        // Extract timestampMs from Record
        let timestamp_ms = match env.call_method(&record_obj, "timestampMs", "()J", &[]) {
            Ok(v) => v.j().unwrap(),
            Err(e) => {
                let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
                return std::ptr::null_mut();
            }
        };

        if i == 0 {
            first_timestamp_ms = timestamp_ms;
        }

        // Convert value with timestamp header
        let value_bytes = match copy_value_with_timestamp(&mut env, &value_array, timestamp_ms) {
            Ok(b) => b,
            Err(e) => {
                let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
                return std::ptr::null_mut();
            }
        };

        rust_records.push(Record {
            key: key_bytes,
            value: value_bytes,
        });
    }

    // Use block_on with separate compaction runtime to avoid deadlocks
    let result = log_handle
        .runtime_handle
        .block_on(async { log_handle.log.append(rust_records).await });

    match result {
        Ok(append_result) => {
            // Create Java AppendResult object with first record's timestamp
            match create_append_result(&mut env, &append_result, first_timestamp_ms) {
                Ok(obj) => obj.into_raw(),
                Err(e) => {
                    let _ =
                        env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Copies a Java byte array into a Rust buffer with a prepended timestamp header.
///
/// This avoids an intermediate allocation by copying directly into the final buffer.
fn copy_value_with_timestamp(
    env: &mut JNIEnv<'_>,
    value: &JByteArray<'_>,
    timestamp_ms: i64,
) -> Result<Bytes, jni::errors::Error> {
    let payload_len = env.get_array_length(value)? as usize;

    // Allocate final buffer: 8-byte header + payload
    let mut buffer = vec![0u8; TIMESTAMP_HEADER_SIZE + payload_len];

    // Write timestamp header (big-endian)
    buffer[..TIMESTAMP_HEADER_SIZE].copy_from_slice(&timestamp_ms.to_be_bytes());

    // Copy payload directly from Java into buffer, avoiding intermediate Vec
    if payload_len > 0 {
        // Safety: buffer[TIMESTAMP_HEADER_SIZE..] has exactly payload_len bytes
        // get_byte_array_region expects i8 slice, so we need to cast
        let dest = &mut buffer[TIMESTAMP_HEADER_SIZE..];
        let dest_i8 =
            unsafe { std::slice::from_raw_parts_mut(dest.as_mut_ptr() as *mut i8, payload_len) };
        env.get_byte_array_region(value, 0, dest_i8)?;
    }

    Ok(Bytes::from(buffer))
}

/// Closes and frees a LogDb instance and its associated runtime.
///
/// # Safety
/// JNI function - handle must be a valid pointer returned by nativeCreate.
#[no_mangle]
pub extern "system" fn Java_dev_opendata_LogDb_nativeClose<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle != 0 {
        let log_handle = unsafe { Box::from_raw(handle as *mut LogHandle) };

        // Destructure to take ownership of components
        let LogHandle {
            log,
            runtime_handle,
            runtime,
            compaction_runtime,
        } = *log_handle;

        // Close the log using block_on
        let result = runtime_handle.block_on(async { log.close().await });

        if let Err(e) = result {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
        }

        // Shutdown the runtimes
        if let Some(rt) = compaction_runtime {
            rt.shutdown_background();
        }
        if let Some(rt) = runtime {
            rt.shutdown_background();
        }
    }
}

/// Scans entries from the log for a given key.
///
/// Uses the LogDb (which implements LogRead) to scan entries.
///
/// # Safety
/// JNI function - handle must be a valid pointer returned by nativeCreate.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_dev_opendata_LogDb_nativeScan<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    start_sequence: jlong,
    max_entries: jlong,
) -> jobjectArray {
    if handle == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "LogDb handle is null");
        return std::ptr::null_mut();
    }

    let log_handle = unsafe { &*(handle as *const LogHandle) };

    let key_bytes = match env.convert_byte_array(&key) {
        Ok(b) => Bytes::from(b),
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            return std::ptr::null_mut();
        }
    };

    let max = max_entries as usize;
    let start_seq = start_sequence as u64;

    // Scan entries using the LogDb (which implements LogRead)
    let entries_result = log_handle.runtime_handle.block_on(async {
        let mut iter = log_handle.log.scan(key_bytes, start_seq..).await?;
        let mut entries = Vec::with_capacity(max);
        while entries.len() < max {
            match iter.next().await? {
                Some(entry) => entries.push(entry),
                None => break,
            }
        }
        Ok::<Vec<LogEntry>, log::Error>(entries)
    });

    match entries_result {
        Ok(entries) => match create_log_entry_array(&mut env, &entries) {
            Ok(arr) => arr,
            Err(e) => {
                let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            std::ptr::null_mut()
        }
    }
}

// =============================================================================
// LogDbReader JNI Methods
// =============================================================================

/// Handle to a LogDbReader instance with its associated Tokio runtime.
///
/// Unlike LogReader which borrows from a parent LogDb, LogDbReader owns its
/// own storage connection and runtime. This allows it to coexist with a
/// separate LogDb writer for realistic end-to-end latency benchmarking.
struct LogDbReaderHandle {
    /// The LogDbReader instance
    reader: LogDbReader,
    /// Handle to the runtime for async operations
    runtime_handle: Handle,
    /// The runtime (kept alive for the lifetime of the reader)
    runtime: Option<Runtime>,
}

/// Creates a new LogDbReader instance with the specified configuration.
///
/// # Arguments
/// * `config` - Java LogDbConfig object
///
/// # Safety
/// This is a JNI function - must be called from Java with valid JNIEnv.
#[no_mangle]
pub extern "system" fn Java_dev_opendata_LogDbReader_nativeCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config: JObject<'local>,
) -> jlong {
    // Extract storage config from LogDbConfig
    let storage_config = match extract_storage_config(&mut env, &config) {
        Ok(c) => c,
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", e);
            return 0;
        }
    };

    let config = Config {
        storage: storage_config,
        ..Config::default()
    };

    // Create a dedicated runtime for this LogDbReader instance
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("opendata-reader")
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            return 0;
        }
    };

    // Open the LogDbReader
    let result = runtime.block_on(async { LogDbReader::open(config).await });

    match result {
        Ok(reader) => {
            let handle = Box::new(LogDbReaderHandle {
                reader,
                runtime_handle: runtime.handle().clone(),
                runtime: Some(runtime),
            });
            Box::into_raw(handle) as jlong
        }
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            0
        }
    }
}

/// Scans entries from the log for a given key using LogDbReader.
///
/// # Safety
/// JNI function - handle must be a valid pointer returned by nativeCreate.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "system" fn Java_dev_opendata_LogDbReader_nativeScan<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    start_sequence: jlong,
    max_entries: jlong,
) -> jobjectArray {
    if handle == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "LogDbReader handle is null");
        return std::ptr::null_mut();
    }

    let reader_handle = unsafe { &*(handle as *const LogDbReaderHandle) };

    let key_bytes = match env.convert_byte_array(&key) {
        Ok(b) => Bytes::from(b),
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            return std::ptr::null_mut();
        }
    };

    let max = max_entries as usize;
    let start_seq = start_sequence as u64;

    // Scan entries using the LogDbReader
    let entries_result = reader_handle.runtime_handle.block_on(async {
        let mut iter = reader_handle.reader.scan(key_bytes, start_seq..).await?;
        let mut entries = Vec::with_capacity(max);
        while entries.len() < max {
            match iter.next().await? {
                Some(entry) => entries.push(entry),
                None => break,
            }
        }
        Ok::<Vec<LogEntry>, log::Error>(entries)
    });

    match entries_result {
        Ok(entries) => match create_log_entry_array(&mut env, &entries) {
            Ok(arr) => arr,
            Err(e) => {
                let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            let _ = env.throw_new("dev/opendata/common/OpenDataNativeException", e.to_string());
            std::ptr::null_mut()
        }
    }
}

/// Closes and frees a LogDbReader instance.
///
/// # Safety
/// JNI function - handle must be a valid pointer returned by nativeCreate.
#[no_mangle]
pub extern "system" fn Java_dev_opendata_LogDbReader_nativeClose<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle != 0 {
        let reader_handle = unsafe { Box::from_raw(handle as *mut LogDbReaderHandle) };

        // Shutdown the runtime
        if let Some(rt) = reader_handle.runtime {
            rt.shutdown_background();
        }
        // LogDbReader drops automatically
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Creates a Java AppendResult object from a Rust AppendResult.
fn create_append_result<'local>(
    env: &mut JNIEnv<'local>,
    result: &AppendResult,
    timestamp_ms: i64,
) -> Result<JObject<'local>, jni::errors::Error> {
    let class = env.find_class("dev/opendata/AppendResult")?;

    // AppendResult is a record with (long sequence, long timestamp)
    let obj = env.new_object(
        class,
        "(JJ)V",
        &[
            JValue::Long(result.start_sequence as i64),
            JValue::Long(timestamp_ms),
        ],
    )?;

    Ok(obj)
}

/// Creates a Java LogEntry[] array from Rust LogEntry vector.
///
/// Extracts the timestamp header from each entry's value and returns the
/// original payload (without header) to Java.
fn create_log_entry_array<'local>(
    env: &mut JNIEnv<'local>,
    entries: &[LogEntry],
) -> Result<jobjectArray, jni::errors::Error> {
    let class = env.find_class("dev/opendata/LogEntry")?;

    let array = env.new_object_array(entries.len() as i32, &class, JObject::null())?;

    for (i, entry) in entries.iter().enumerate() {
        // Extract timestamp from header and get original payload
        let (timestamp_ms, payload) = extract_timestamp_and_payload(&entry.value);

        let key_arr = env.byte_array_from_slice(&entry.key)?;
        let value_arr = env.byte_array_from_slice(payload)?;

        // LogEntry is a record with (long sequence, long timestamp, byte[] key, byte[] value)
        let obj = env.new_object(
            &class,
            "(JJ[B[B)V",
            &[
                JValue::Long(entry.sequence as i64),
                JValue::Long(timestamp_ms),
                JValue::Object(&key_arr.into()),
                JValue::Object(&value_arr.into()),
            ],
        )?;

        env.set_object_array_element(&array, i as i32, &obj)?;
    }

    Ok(array.into_raw())
}

/// Extracts the timestamp header and original payload from a stored value.
///
/// Returns (timestamp_ms, payload_slice). If the value is too short to contain
/// a header, returns (0, full_value) for graceful degradation.
fn extract_timestamp_and_payload(value: &[u8]) -> (i64, &[u8]) {
    if value.len() < TIMESTAMP_HEADER_SIZE {
        // Value doesn't have header (shouldn't happen, but handle gracefully)
        return (0, value);
    }

    let timestamp_bytes: [u8; 8] = value[..TIMESTAMP_HEADER_SIZE]
        .try_into()
        .expect("slice is exactly 8 bytes");
    let timestamp_ms = i64::from_be_bytes(timestamp_bytes);
    let payload = &value[TIMESTAMP_HEADER_SIZE..];

    (timestamp_ms, payload)
}

/// Returns current wall-clock time as milliseconds since Unix epoch (for testing).
#[cfg(test)]
fn current_timestamp_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}

/// Creates a value with timestamp header prepended (for testing).
#[cfg(test)]
fn create_timestamped_value(timestamp_ms: i64, payload: &[u8]) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(TIMESTAMP_HEADER_SIZE + payload.len());
    buffer.extend_from_slice(&timestamp_ms.to_be_bytes());
    buffer.extend_from_slice(payload);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // current_timestamp_ms tests
    // =========================================================================

    #[test]
    fn should_return_reasonable_timestamp() {
        // given
        // Unix timestamp for 2020-01-01 00:00:00 UTC
        let min_expected: i64 = 1_577_836_800_000;
        // Unix timestamp for 2100-01-01 00:00:00 UTC
        let max_expected: i64 = 4_102_444_800_000;

        // when
        let timestamp = current_timestamp_ms();

        // then
        assert!(
            timestamp > min_expected,
            "timestamp {} should be after 2020",
            timestamp
        );
        assert!(
            timestamp < max_expected,
            "timestamp {} should be before 2100",
            timestamp
        );
    }

    #[test]
    fn should_return_monotonically_increasing_timestamps() {
        // given
        let first = current_timestamp_ms();

        // when
        let second = current_timestamp_ms();

        // then
        assert!(
            second >= first,
            "second timestamp {} should be >= first {}",
            second,
            first
        );
    }

    // =========================================================================
    // extract_timestamp_and_payload tests
    // =========================================================================

    #[test]
    fn should_extract_timestamp_and_payload() {
        // given
        let timestamp: i64 = 1_700_000_000_000;
        let payload = b"hello world";
        let value = create_timestamped_value(timestamp, payload);

        // when
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&value);

        // then
        assert_eq!(extracted_ts, timestamp);
        assert_eq!(extracted_payload, payload);
    }

    #[test]
    fn should_extract_timestamp_with_empty_payload() {
        // given
        let timestamp: i64 = 1_700_000_000_000;
        let payload = b"";
        let value = create_timestamped_value(timestamp, payload);

        // when
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&value);

        // then
        assert_eq!(extracted_ts, timestamp);
        assert_eq!(extracted_payload, b"");
    }

    #[test]
    fn should_handle_value_shorter_than_header() {
        // given
        let short_value = vec![1, 2, 3]; // Only 3 bytes, header needs 8

        // when
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&short_value);

        // then - graceful degradation
        assert_eq!(extracted_ts, 0);
        assert_eq!(extracted_payload, &[1, 2, 3]);
    }

    #[test]
    fn should_handle_empty_value() {
        // given
        let empty_value: Vec<u8> = vec![];

        // when
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&empty_value);

        // then - graceful degradation
        assert_eq!(extracted_ts, 0);
        assert!(extracted_payload.is_empty());
    }

    #[test]
    fn should_handle_exactly_header_size_value() {
        // given - value is exactly 8 bytes (header only, no payload)
        let timestamp: i64 = 1_700_000_000_000;
        let value = timestamp.to_be_bytes().to_vec();

        // when
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&value);

        // then
        assert_eq!(extracted_ts, timestamp);
        assert!(extracted_payload.is_empty());
    }

    #[test]
    fn should_preserve_large_payload() {
        // given
        let timestamp: i64 = 1_700_000_000_000;
        let payload: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let value = create_timestamped_value(timestamp, &payload);

        // when
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&value);

        // then
        assert_eq!(extracted_ts, timestamp);
        assert_eq!(extracted_payload, payload.as_slice());
    }

    // =========================================================================
    // Round-trip tests
    // =========================================================================

    #[test]
    fn should_roundtrip_timestamp_and_payload() {
        // given
        let original_timestamp: i64 = 1_705_123_456_789;
        let original_payload = b"test payload with special chars: \x00\xff\n\t";

        // when - create and extract
        let value = create_timestamped_value(original_timestamp, original_payload);
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&value);

        // then
        assert_eq!(extracted_ts, original_timestamp);
        assert_eq!(extracted_payload, original_payload);
    }

    #[test]
    fn should_roundtrip_zero_timestamp() {
        // given
        let timestamp: i64 = 0;
        let payload = b"payload";

        // when
        let value = create_timestamped_value(timestamp, payload);
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&value);

        // then
        assert_eq!(extracted_ts, 0);
        assert_eq!(extracted_payload, payload);
    }

    #[test]
    fn should_roundtrip_negative_timestamp() {
        // given - negative timestamp (before Unix epoch, unlikely but valid i64)
        let timestamp: i64 = -1_000_000;
        let payload = b"ancient history";

        // when
        let value = create_timestamped_value(timestamp, payload);
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&value);

        // then
        assert_eq!(extracted_ts, timestamp);
        assert_eq!(extracted_payload, payload);
    }

    #[test]
    fn should_roundtrip_max_timestamp() {
        // given
        let timestamp: i64 = i64::MAX;
        let payload = b"far future";

        // when
        let value = create_timestamped_value(timestamp, payload);
        let (extracted_ts, extracted_payload) = extract_timestamp_and_payload(&value);

        // then
        assert_eq!(extracted_ts, timestamp);
        assert_eq!(extracted_payload, payload);
    }
}
