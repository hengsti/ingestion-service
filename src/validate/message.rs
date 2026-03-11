use crate::model::sensor_msg::SensorMessage;
use crate::model::status_msg::StatusMessage;

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
