use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValueInput {
    pub value: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum OperationType {
    ADD,
    MULTIPLY,
}

impl Default for OperationType {
    fn default() -> Self { OperationType::ADD }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValueOperationInput {
    pub operation: OperationType,
    pub value: f64,
}
