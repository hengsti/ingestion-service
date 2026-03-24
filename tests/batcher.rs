mod common;

use std::sync::{Arc, Mutex};

use axum::{extract::State, routing::post, Router};
use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use smarthome_ingest::infrastructure::database::influx::InfluxWriter;

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

fn make_influx(url: &str) -> InfluxWriter {
    InfluxWriter::new(url, "test_org", "test_bucket", "test_token").unwrap()
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn size_trigger_flushes_when_batch_is_full() {
    let (url, received) = spawn_mock_influx().await;
    let influx = make_influx(&url);
    let (tx, rx) = mpsc::channel::<String>(100);

    let batch_size = 3;
    let handle = tokio::spawn(async move {
        influx.run_batcher(rx, batch_size, 10_000).await.unwrap();
    });

    // Send exactly batch_size lines; the size trigger fires on the third send.
    tx.send("line1".to_string()).await.unwrap();
    tx.send("line2".to_string()).await.unwrap();
    tx.send("line3".to_string()).await.unwrap();

    // Give the async flush time to complete the HTTP round-trip to the mock.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let (len, body) = {
        let posts = received.lock().unwrap();
        (posts.len(), posts.first().cloned().unwrap_or_default())
    };
    assert_eq!(len, 1, "expected exactly 1 POST for a full batch");
    assert!(body.contains("line1"), "batch should contain line1");
    assert!(body.contains("line3"), "batch should contain line3");

    drop(tx);
    handle.await.unwrap();
}

#[tokio::test]
async fn time_trigger_flushes_before_batch_is_full() {
    let (url, received) = spawn_mock_influx().await;
    let influx = make_influx(&url);
    let (tx, rx) = mpsc::channel::<String>(100);

    // Large batch_size so size trigger never fires; short interval for speed.
    let flush_interval_ms = 50;
    let handle = tokio::spawn(async move {
        influx
            .run_batcher(rx, 1_000, flush_interval_ms)
            .await
            .unwrap();
    });

    // Let the first (immediate) tick fire on an empty buffer.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    tx.send("only_line".to_string()).await.unwrap();

    // Wait for more than one flush interval so the time trigger fires.
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    let (len, body) = {
        let posts = received.lock().unwrap();
        (posts.len(), posts.first().cloned().unwrap_or_default())
    };
    assert_eq!(len, 1, "expected exactly 1 POST from the time trigger");
    assert!(body.contains("only_line"));

    drop(tx);
    handle.await.unwrap();
}

#[tokio::test]
async fn channel_close_triggers_final_flush() {
    let (url, received) = spawn_mock_influx().await;
    let influx = make_influx(&url);
    let (tx, rx) = mpsc::channel::<String>(100);

    // batch_size=3 so the two lines won't trigger a size flush.
    let handle = tokio::spawn(async move {
        influx.run_batcher(rx, 3, 10_000).await.unwrap();
    });

    tx.send("line_a".to_string()).await.unwrap();
    tx.send("line_b".to_string()).await.unwrap();

    // Dropping the sender signals the batcher to do a final flush and exit.
    drop(tx);

    // Await the batcher task — it returns only after the final flush completes.
    handle.await.unwrap();

    let posts = received.lock().unwrap();
    assert_eq!(posts.len(), 1, "expected 1 POST for the final flush");
    let body = &posts[0];
    assert!(body.contains("line_a"));
    assert!(body.contains("line_b"));
}

#[tokio::test]
async fn empty_buffer_does_not_flush() {
    let (url, received) = spawn_mock_influx().await;
    let influx = make_influx(&url);
    let (tx, rx) = mpsc::channel::<String>(100);

    let flush_interval_ms = 50;
    let handle = tokio::spawn(async move {
        influx.run_batcher(rx, 3, flush_interval_ms).await.unwrap();
    });

    // Wait for several flush intervals without sending anything.
    tokio::time::sleep(std::time::Duration::from_millis(180)).await;

    let posts = received.lock().unwrap().clone();
    assert_eq!(
        posts.len(),
        0,
        "no POST expected when buffer is always empty"
    );

    drop(tx);
    handle.await.unwrap();
}
