mod common;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{extract::State, routing::post, Router};
use bytes::Bytes;
use reqwest::StatusCode;
use tokio::net::TcpListener;

use smarthome_ingest::infrastructure::sink::{influx::InfluxSink, Sink, SinkError};
use smarthome_ingest::infrastructure::wal::cursor::read_cursor;
use smarthome_ingest::infrastructure::wal::forwarder::run_forwarder;
use smarthome_ingest::infrastructure::wal::types::WalEvent;

// ── mock InfluxDB server ──────────────────────────────────────────────────────

/// Spawns a minimal axum server on a random OS-assigned port that records every
/// POST body sent to `/api/v2/write` and always replies with 204 No Content.
///
/// Returns the base URL (e.g. `"http://127.0.0.1:51234"`) and a shared
/// `Vec<String>` accumulating received request bodies.
async fn spawn_mock_influx() -> (String, Arc<Mutex<Vec<String>>>) {
    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let app = Router::new()
        .route(
            "/api/v2/write",
            post(
                |State(recv): State<Arc<Mutex<Vec<String>>>>, body: Bytes| async move {
                    recv.lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&body).to_string());
                    axum::http::StatusCode::NO_CONTENT
                },
            ),
        )
        .with_state(received.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://127.0.0.1:{}", addr.port()), received)
}

#[derive(Clone)]
struct ScriptedInfluxState {
    received: Arc<Mutex<Vec<String>>>,
    statuses: Arc<Mutex<VecDeque<StatusCode>>>,
}

async fn spawn_scripted_mock_influx(
    statuses: Vec<StatusCode>,
) -> (String, Arc<Mutex<Vec<String>>>) {
    let state = ScriptedInfluxState {
        received: Arc::new(Mutex::new(Vec::new())),
        statuses: Arc::new(Mutex::new(statuses.into())),
    };
    let received = state.received.clone();

    let app = Router::new()
        .route(
            "/api/v2/write",
            post(
                |State(state): State<ScriptedInfluxState>, body: Bytes| async move {
                    state
                        .received
                        .lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&body).to_string());

                    state
                        .statuses
                        .lock()
                        .unwrap()
                        .pop_front()
                        .unwrap_or(StatusCode::NO_CONTENT)
                },
            ),
        )
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (format!("http://127.0.0.1:{}", addr.port()), received)
}

fn make_sink(url: &str) -> Arc<dyn Sink> {
    Arc::new(
        InfluxSink::new(
            url,
            "test_org",
            "test_bucket",
            secrecy::SecretString::new("test_token".to_string()),
        )
        .unwrap(),
    )
}

fn status_event(device_id: &str) -> WalEvent {
    WalEvent {
        topic: format!("smarthome/{device_id}/status"),
        ts_ms: 1_700_000_000_000,
        payload: format!(
            "device_status,device_id={device_id},device_class=esp32p4-bme680 rssi=-65i 1700000000000"
        ),
    }
}

fn post_count(received: &Arc<Mutex<Vec<String>>>) -> usize {
    received.lock().unwrap().len()
}

// ── direct sink tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn sink_write_posts_line_protocol_body() {
    let (url, received) = spawn_mock_influx().await;
    let sink = make_sink(&url);

    sink.write(&[status_event("dev-a"), status_event("dev-b")])
        .await
        .unwrap();

    let posts = received.lock().unwrap();
    assert_eq!(posts.len(), 1, "expected exactly 1 POST for one write");
    let body = &posts[0];
    assert!(body.contains("device_status"), "body: {body}");
    assert!(
        body.contains("dev-a") && body.contains("dev-b"),
        "body: {body}"
    );
    assert_eq!(body.lines().count(), 2, "one line per event");
}

#[tokio::test]
async fn sink_write_empty_batch_makes_no_request() {
    let (url, received) = spawn_mock_influx().await;
    let sink = make_sink(&url);

    sink.write(&[]).await.unwrap();

    assert_eq!(post_count(&received), 0, "empty batch must not POST");
}

#[tokio::test]
async fn sink_write_http_400_returns_permanent_without_retry() {
    let (url, received) =
        spawn_scripted_mock_influx(vec![StatusCode::BAD_REQUEST, StatusCode::NO_CONTENT]).await;
    let sink = make_sink(&url);

    let result = sink.write(&[status_event("bad-request")]).await;

    assert!(matches!(result, Err(SinkError::Permanent(_))));
    assert_eq!(
        post_count(&received),
        1,
        "400 must be treated as permanent and must not be retried"
    );
}

#[tokio::test]
async fn sink_write_http_503_retries_and_returns_retryable_after_exhaustion() {
    let (url, received) = spawn_scripted_mock_influx(vec![
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::SERVICE_UNAVAILABLE,
    ])
    .await;
    let sink = make_sink(&url);

    let result = sink.write(&[status_event("svc-unavailable")]).await;

    assert!(matches!(result, Err(SinkError::Retryable(_))));
    assert_eq!(
        post_count(&received),
        3,
        "503 must be retried three times before returning retryable error"
    );
}

// ── forwarder + WAL integration tests ─────────────────────────────────────────

#[tokio::test]
async fn forwarder_size_trigger_flushes_full_batch() {
    let (url, received) = spawn_mock_influx().await;
    let sink = make_sink(&url);
    let (wal, sub, _tmp) = common::open_temp_wal().await;

    // batch_size = 3, long flush interval so only the size trigger can fire.
    tokio::spawn(run_forwarder(sub, sink, 3, 10_000));

    for i in 0..3 {
        wal.try_append(status_event(&format!("dev-{i}"))).unwrap();
        tokio::task::yield_now().await;
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let posts = received.lock().unwrap();
    assert_eq!(posts.len(), 1, "size trigger should produce 1 POST");
    assert_eq!(posts[0].lines().count(), 3, "batch should hold 3 lines");
}

#[tokio::test]
async fn forwarder_time_trigger_flushes_partial_batch() {
    let (url, received) = spawn_mock_influx().await;
    let sink = make_sink(&url);
    let (wal, sub, _tmp) = common::open_temp_wal().await;

    // Large batch_size so only the time trigger can fire; short interval.
    tokio::spawn(run_forwarder(sub, sink, 1_000, 50));

    wal.try_append(status_event("only")).unwrap();

    // Wait for more than one flush interval so the time trigger fires.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let posts = received.lock().unwrap();
    assert_eq!(posts.len(), 1, "time trigger should produce 1 POST");
    assert!(posts[0].contains("only"), "body: {}", posts[0]);
}

#[tokio::test]
async fn forwarder_empty_wal_does_not_flush() {
    let (url, received) = spawn_mock_influx().await;
    let sink = make_sink(&url);
    let (_wal, sub, _tmp) = common::open_temp_wal().await;

    // Short interval, but nothing is ever appended.
    tokio::spawn(run_forwarder(sub, sink, 3, 50));

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        post_count(&received),
        0,
        "no POST expected when the WAL stays empty"
    );
}

#[tokio::test]
async fn forwarder_drains_final_batch_and_terminates_when_wal_closes() {
    let (url, received) = spawn_mock_influx().await;
    let sink = make_sink(&url);
    let (wal, sub, _tmp) = common::open_temp_wal().await;

    // Large batch + long interval so neither the size nor the time trigger fires;
    // only the shutdown drain can flush the buffered records.
    let handle = tokio::spawn(run_forwarder(sub, sink, 1_000, 60_000));

    for i in 0..5 {
        wal.try_append(status_event(&format!("dev-{i}"))).unwrap();
        tokio::task::yield_now().await;
    }

    // Close the WAL: the writer finishes and the forwarder must flush its final
    // batch before returning cleanly.
    drop(wal);

    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("forwarder drain timed out")
        .expect("forwarder task panicked")
        .expect("forwarder returned an error");

    let posts = received.lock().unwrap();
    let total_lines: usize = posts.iter().map(|b| b.lines().count()).sum();
    assert_eq!(
        total_lines, 5,
        "every appended event must reach the sink on graceful drain"
    );
}

#[tokio::test]
async fn forwarder_outage_retries_and_commits_only_after_recovery() {
    // Simulate one transient outage window for a single sink.write call:
    // 503, 503, then 204. InfluxSink retries these internally, so the forwarder
    // must hold the WAL batch and only commit after recovery.
    let (url, received) = spawn_scripted_mock_influx(vec![
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::NO_CONTENT,
    ])
    .await;
    let sink = make_sink(&url);
    let (wal, sub, tmp) = common::open_temp_wal().await;

    let handle = tokio::spawn(run_forwarder(sub, sink, 1, 1));

    wal.try_append(status_event("dev-outage")).unwrap();

    let request_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while post_count(&received) < 2 && tokio::time::Instant::now() < request_deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        post_count(&received) >= 2,
        "expected at least two failed write attempts during outage window"
    );
    assert_eq!(
        read_cursor(tmp.path()).unwrap(),
        None,
        "cursor must not advance while sink retries are still in-flight"
    );

    let commit_deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while read_cursor(tmp.path()).unwrap().is_none()
        && tokio::time::Instant::now() < commit_deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let committed = read_cursor(tmp.path()).unwrap();
    assert!(
        committed.is_some(),
        "cursor must advance after service recovery and successful write"
    );

    drop(wal);
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("forwarder shutdown timed out")
        .expect("forwarder task panicked")
        .expect("forwarder returned an error");

    assert_eq!(
        post_count(&received),
        3,
        "expected two 503 retries followed by one successful recovery write"
    );
}
