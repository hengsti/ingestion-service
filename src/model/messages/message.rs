use crate::model::messages::sensor::SensorMessage;
use crate::model::messages::status::StatusMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Sensor,
    Status,
}

#[derive(Debug, Clone)]
pub enum HandledMessage {
    Sensor(SensorMessage),
    Status(StatusMessage),
}
