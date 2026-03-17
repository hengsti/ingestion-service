use std::convert::Infallible;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use serde::Serialize;
use tokio_stream::{
    StreamExt,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};

use super::state::{CacheEvent, CacheState};
use crate::model::messages::sensor::SensorData;

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
struct SensorDto {
    device_id: String,
    stale: bool,
    last_seen_ms: u64,
    value: SensorData,
}

#[derive(Serialize)]
struct SensorStateResponse {
    ttl_ms: u64,
    sensors: Vec<SensorDto>,
}

#[derive(Serialize)]
struct SseSensorEvent {
    kind: &'static str,
    device_id: String,
    last_seen_ms: u64,
    value: SensorData,
}

async fn list_state(State(cache_state): State<CacheState>) -> impl IntoResponse {
    let ttl_ms = cache_state.ttl_ms();
    let sensors = cache_state
        .snapshot_all_sensors()
        .into_iter()
        .map(|(device_id, state, stale)| SensorDto {
            device_id,
            stale,
            last_seen_ms: state.last_seen_ms,
            value: state.value,
        })
        .collect();

    Json(SensorStateResponse { ttl_ms, sensors })
}

#[derive(Serialize)]
struct SensorDtoNoId {
    stale: bool,
    last_seen_ms: u64,
    value: SensorData,
}

#[derive(Serialize)]
struct DeviceStateResponse {
    ttl_ms: u64,
    device_id: String,
    sensor: Option<SensorDtoNoId>,
}

async fn get_state(
    Path(device_id): Path<String>,
    State(cache_state): State<CacheState>,
) -> impl IntoResponse {
    let ttl_ms = cache_state.ttl_ms();
    let sensor = cache_state
        .snapshot_sensor(&device_id)
        .map(|(state, stale)| SensorDtoNoId {
            stale,
            last_seen_ms: state.last_seen_ms,
            value: state.value,
        });

    Json(DeviceStateResponse {
        ttl_ms,
        device_id,
        sensor,
    })
}

async fn stream_updates(
    State(cache_state): State<CacheState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = cache_state.subscribe_events();

    let stream = BroadcastStream::new(rx).filter_map(|msg| {
        match msg {
            Ok(CacheEvent::Sensor {
                device_id,
                last_seen_ms,
                value,
            }) => {
                // Event-type for SSE-Clients
                let payload = SseSensorEvent {
                    kind: "sensor",
                    device_id,
                    last_seen_ms,
                    value,
                };

                // JSON payload (HomeKit-Bridge kann direkt konsumieren)
                let json = match serde_json::to_string(&payload) {
                    Ok(s) => s,
                    Err(_) => return None,
                };

                Some(Ok(Event::default().event("sensor").data(json)))
            }
            // Receiver war zu langsam -> Events wurden gedropped
            Err(BroadcastStreamRecvError::Lagged(_)) => {
                // Optional: Client kann dann einmal /v1/state pollen
                Some(Ok(Event::default()
                    .event("lagged")
                    .data("{\"hint\":\"poll /v1/state\"}")))
            }
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("ping"),
    )
}
