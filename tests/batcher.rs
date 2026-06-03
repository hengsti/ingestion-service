mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{extract::State, routing::post, Router};
use bytes::Bytes;
use tokio::net::TcpListener;

use smarthome_ingest::infrastructure::sink::{influx::InfluxSink, Sink};
use smarthome_ingest::infrastructure::wal::forwarder::run_forwarder;
use smarthome_ingest::infrastructure::wal::types::WalEvent;
use smarthome_ingest::model::messages::message::HandledMessage;
use smarthome_ingest::model::messages::status::StatusMessage;

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
        message: HandledMessage::Status(StatusMessage {
            device_id: device_id.to_string(),
            device_class: "esp32p4-bme680".to_string(),
            fw_version: "1.0.0".to_string(),
            ip: "192.168.1.42".to_string(),
            rssi: -65,
            time_ms: 1_700_000_000_000,
            time_iso: "2023-11-14T22:13:20Z".to_string(),
            time_valid: true,
            uptime: 3600,
            free_mem: 200_000,
            ssid: "HomeNet".to_string(),
        }),
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
