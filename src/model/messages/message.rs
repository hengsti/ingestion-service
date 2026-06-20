use serde::{Deserialize, Serialize};

use crate::model::messages::sensor::SensorMessage;
use crate::model::messages::status::StatusMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    Sensor,
    Status,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HandledMessage {
    Sensor(SensorMessage),
    Status(StatusMessage),
}
