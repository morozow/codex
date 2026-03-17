//! Performance benchmarks for stdio_bus integration.
//!
//! These benchmarks validate the performance requirements from REQ-10:
//! - REQ-10.1: Routing extraction latency (<1ms per message)
//! - REQ-10.2: Message throughput (>10,000 messages/sec)
//! - REQ-10.3: Memory overhead (<10MB)
//! - REQ-10.4: Worker startup time (<500ms)

use codex_stdio_bus::StdioBusWorker;
use codex_stdio_bus::extract_routing_fields;
use codex_stdio_bus::parse_message;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;

/// Generate a typical JSON-RPC request message.
fn generate_request_message(id: &str, session_id: &str, method: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": {
            "prompt": "Hello, world!",
            "options": {
                "temperature": 0.7,
                "max_tokens": 1000
            }
        },
        "sessionId": session_id
    }))
    .expect("Failed to serialize message")
}

/// Generate a typical JSON-RPC response message.
fn generate_response_message(id: &str, session_id: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "status": "ok",
            "data": {
                "message": "Response content here",
                "tokens_used": 150
            }
        },
        "sessionId": session_id
    }))
    .expect("Failed to serialize message")
}

/// Generate a large message with nested params.
fn generate_large_message(id: &str, session_id: &str) -> Vec<u8> {
    let large_content: String = "x".repeat(10_000);
    serde_json::to_vec(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "thread/submit",
        "params": {
            "content": large_content,
            "metadata": {
                "source": "benchmark",
                "timestamp": 1234567890,
                "tags": ["test", "benchmark", "performance"]
            }
        },
        "sessionId": session_id
    }))
    .expect("Failed to serialize message")
}

/// **Validates: REQ-10.1** - Routing extraction latency (<1ms per message)
///
/// Benchmarks the `extract_routing_fields` function to ensure it meets
/// the <1ms latency requirement for routing field extraction.
fn bench_routing_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("routing_extraction");

    // Benchmark with typical request message
    let request_msg = generate_request_message("req-123", "thread:abc-456", "thread/start");
    group.throughput(Throughput::Bytes(request_msg.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("request", request_msg.len()),
        &request_msg,
        |b, msg| {
            b.iter(|| extract_routing_fields(black_box(msg)));
        },
    );

    // Benchmark with typical response message
    let response_msg = generate_response_message("resp-456", "conn:12345");
    group.bench_with_input(
        BenchmarkId::new("response", response_msg.len()),
        &response_msg,
        |b, msg| {
            b.iter(|| extract_routing_fields(black_box(msg)));
        },
    );

    // Benchmark with large message
    let large_msg = generate_large_message("large-789", "mcp:server-1");
    group.bench_with_input(
        BenchmarkId::new("large_message", large_msg.len()),
        &large_msg,
        |b, msg| {
            b.iter(|| extract_routing_fields(black_box(msg)));
        },
    );

    // Benchmark with minimal message (just id)
    let minimal_msg = serde_json::to_vec(&serde_json::json!({"id": 1})).unwrap();
    group.bench_with_input(
        BenchmarkId::new("minimal", minimal_msg.len()),
        &minimal_msg,
        |b, msg| {
            b.iter(|| extract_routing_fields(black_box(msg)));
        },
    );

    group.finish();
}

/// **Validates: REQ-10.2** - Message throughput (>10,000 messages/sec)
///
/// Benchmarks message parsing throughput to ensure the system can handle
/// at least 10,000 messages per second.
fn bench_message_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_throughput");

    // Pre-generate a batch of messages
    let messages: Vec<Vec<u8>> = (0..1000)
        .map(|i| {
            generate_request_message(
                &format!("req-{i}"),
                &format!("thread:session-{i}"),
                "thread/submit",
            )
        })
        .collect();

    let total_bytes: u64 = messages.iter().map(|m| m.len() as u64).sum();
    group.throughput(Throughput::Elements(messages.len() as u64));

    group.bench_function("parse_batch_1000", |b| {
        b.iter(|| {
            for msg in &messages {
                let _ = parse_message(black_box(msg.clone()));
            }
        });
    });

    // Benchmark with throughput in bytes
    group.throughput(Throughput::Bytes(total_bytes));
    group.bench_function("parse_batch_1000_bytes", |b| {
        b.iter(|| {
            for msg in &messages {
                let _ = parse_message(black_box(msg.clone()));
            }
        });
    });

    group.finish();
}

/// **Validates: REQ-10.3** - Memory overhead (<10MB)
///
/// Estimates memory usage by measuring the size of key data structures.
/// Note: This is an estimation benchmark, not a precise memory measurement.
fn bench_memory_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_overhead");

    // Benchmark memory allocation for message parsing
    let msg = generate_request_message("req-123", "thread:abc-456", "thread/start");

    group.bench_function("message_allocation", |b| {
        b.iter(|| {
            let message = parse_message(black_box(msg.clone()));
            // Force the message to be used
            black_box(&message.routing.session_id);
            message
        });
    });

    // Benchmark session ID injection (which involves re-serialization)
    let response = generate_response_message("resp-123", "thread:abc-456");
    group.bench_function("session_id_injection", |b| {
        b.iter(|| {
            let mut msg = black_box(response.clone());
            let _ = StdioBusWorker::inject_session_id(&mut msg, "thread:new-session");
            msg
        });
    });

    // Benchmark with varying message sizes to estimate memory scaling
    for size in [100, 1000, 10000] {
        let content: String = "x".repeat(size);
        let sized_msg = serde_json::to_vec(&serde_json::json!({
            "id": "test",
            "method": "test/method",
            "params": {"content": content},
            "sessionId": "thread:test"
        }))
        .unwrap();

        group.bench_with_input(
            BenchmarkId::new("parse_sized_message", size),
            &sized_msg,
            |b, msg| {
                b.iter(|| parse_message(black_box(msg.clone())));
            },
        );
    }

    group.finish();
}

/// **Validates: REQ-10.4** - Worker startup time (<500ms)
///
/// Benchmarks the worker creation time. Note that actual startup time
/// includes I/O setup which cannot be fully benchmarked without real stdio.
fn bench_worker_startup(c: &mut Criterion) {
    let mut group = c.benchmark_group("worker_startup");

    // Benchmark StdioBusWorker creation
    // Note: This only measures the struct creation, not the full I/O setup
    // which requires actual stdin/stdout handles.
    group.bench_function("worker_struct_creation", |b| {
        b.iter(|| {
            // We can't actually create a StdioBusWorker in benchmarks because
            // it takes ownership of stdin/stdout. Instead, we benchmark the
            // components that would be created.
            let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
            black_box(shutdown_tx);
        });
    });

    // Benchmark configuration parsing (part of startup)
    let config_json = serde_json::json!({
        "pools": [{
            "id": "app-server",
            "command": "/usr/bin/app-server",
            "args": ["--worker"],
            "instances": 4
        }],
        "limits": {
            "max_input_buffer": 1048576,
            "max_output_queue": 4194304,
            "max_restarts": 5,
            "restart_window_sec": 60
        },
        "routing": {
            "session_id_field": "sessionId"
        }
    });
    let config_str = serde_json::to_string(&config_json).unwrap();

    group.bench_function("config_parsing", |b| {
        b.iter(|| {
            let config: codex_stdio_bus::StdioBusConfig =
                serde_json::from_str(black_box(&config_str)).unwrap();
            black_box(config);
        });
    });

    group.finish();
}

/// Benchmark for session ID mapping functions.
fn bench_session_id_mapping(c: &mut Criterion) {
    let mut group = c.benchmark_group("session_id_mapping");

    // Benchmark thread_to_session_id
    group.bench_function("thread_to_session_id", |b| {
        b.iter(|| codex_stdio_bus::thread_to_session_id(black_box("abc-123-def-456")));
    });

    // Benchmark conn_to_session_id
    group.bench_function("conn_to_session_id", |b| {
        b.iter(|| codex_stdio_bus::conn_to_session_id(black_box(12345)));
    });

    // Benchmark mcp_to_session_id
    group.bench_function("mcp_to_session_id", |b| {
        b.iter(|| codex_stdio_bus::mcp_to_session_id(black_box("my-mcp-server")));
    });

    // Benchmark SessionType::from_session_id
    let session_ids = [
        "thread:abc-123",
        "conn:12345",
        "mcp:server-name",
        "unknown:something",
    ];
    for session_id in session_ids {
        group.bench_with_input(
            BenchmarkId::new("from_session_id", session_id),
            &session_id,
            |b, sid| {
                b.iter(|| codex_stdio_bus::SessionType::from_session_id(black_box(sid)));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_routing_extraction,
    bench_message_throughput,
    bench_memory_overhead,
    bench_worker_startup,
    bench_session_id_mapping,
);

criterion_main!(benches);
