use std::convert::Infallible;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{{sse::{Event, KeepAlive, Sse}}, IntoResponse},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use tokio_stream::{wrappers::{errors::BroadcastStreamRecvError, BroadcastStream}, StreamExt};

use super::state::{CacheState, SensorState, CacheEvent};

pub fn router(state: CacheState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/state", get(list_state))
        .route("/v1/state/{device_id}", get(get_state))
        .route("/v1/stream", get(stream_updates))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    StatusCode::OK
}

#[derive(Serialize)]
struct SensorEntry {
    device_id: String,
    stale: bool,
    last_seen_ms: u64,
    value: SensorState,
}

#[derive(Serialize)]
struct StateResponse {
    ttl_ms: u64,
    sensors: Vec<SensorEntry>,
}

async fn list_state(State(cache_state): State<CacheState>) -> impl IntoResponse {
    let ttl_ms = cache_state.ttl_ms();
    let sensors = cache_state
        .snapshot_all_sensors()
        .into_iter()
        .map(|(device_id, state, stale)| SensorEntry {
            device_id,
            stale,
            last_seen_ms: state.last_seen_ms,
            value: state,
        })
        .collect();

    Json(StateResponse {
        ttl_ms,
        sensors,
    })
}

#[derive(Serialize)]
struct DeviceStateResponse {
    ttl_ms: u64,
    device_id: String,
    sensor: Option<StatusWrapped<SensorState>>,
}

#[derive(Serialize)]
struct StatusWrapped<T> {
    stale: bool,
    last_seen_ms: u64,
    value: T,
}

async fn get_state(Path(device_id): Path<String>, State(cache_state): State<CacheState>) -> impl IntoResponse {
    let ttl_ms = cache_state.ttl_ms();
    let sensor = cache_state.snapshot_sensor(&device_id).map(|(state, stale)| StatusWrapped {
        stale,
        last_seen_ms: state.last_seen_ms,
        value: state,
    });

    Json(DeviceStateResponse {
        ttl_ms,
        device_id,
        sensor,
    })
}

async fn stream_updates(State(cache_state): State<CacheState>) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = cache_state.subscribe_events();
    
    let stream = BroadcastStream::new(rx).filter_map(|msg| {
        match msg {
            Ok(ev) => {
                // JSON payload (HomeKit-Bridge kann direkt konsumieren)
                let json = match serde_json::to_string(&ev) {
                    Ok(s) => s,
                    Err(_) => return None,
                };

                // Event-Typ für einfache Client-Filterung
                let event_type = match &ev {
                    CacheEvent::Sensor { .. } => "sensor",
                };

                Some(Ok(Event::default().event(event_type).data(json)))
            }
            // Receiver war zu langsam -> Events wurden gedropped
            Err(BroadcastStreamRecvError::Lagged(_)) => {
                // Optional: Client kann dann einmal /v1/state pollen
                Some(Ok(Event::default().event("lagged").data("{\"hint\":\"poll /v1/state\"}")))
            }
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("ping"),
    )
}