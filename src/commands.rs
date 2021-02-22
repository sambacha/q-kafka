use crate::config::{Config};
use crate::producer::Producer;
use crate::commands_schema::{Command, Value, UpdateOperation};
use crate::inputs_schema::{ ValueInput, ValueOperationInput };
use log::{info, warn};
use rdkafka::producer::FutureRecord;
use std::convert::Infallible;
use uuid::Uuid;
use warp::http::StatusCode;
use serde_json;
use std::time::Duration;

// TODO : serialize as avro
pub async fn create_value(
    initial_value: ValueInput,
    producer: Producer,
    config: Config
) -> Result<impl warp::Reply, Infallible> {

    info!("Create value {:#?}", initial_value);

    let producer = producer.lock().await;
    let command_id = Uuid::new_v4();
    let value_id = Uuid::new_v4();
    let command = Command::CreateValue {id: command_id,
                                        data: Value {value_id : value_id,
                                                     value : initial_value.value}};
    let payload : String = serde_json::to_string(&command).expect ("Could not serialize command");

    match producer.send(FutureRecord::to(&config.commands_topic)
                        .payload(&payload)
                        .key(&format!("{}", &command_id)),
                        Duration::from_secs(0)).await {
        Ok(_) => info!("Succesfully sent command {:#?} to topic {}", command, &config.commands_topic),
        Err(why) => warn!("Error sending command: {:#?}", why)
    };

    Ok(warp::reply::with_status(warp::reply::json(&value_id),
                                warp::http::StatusCode::ACCEPTED))
}

pub async fn update_value(
    value_id: Uuid,
    operation : ValueOperationInput,
    producer: Producer,
    config: Config
) -> Result<impl warp::Reply, Infallible> {

    info!("Update value {:#?} with {:#?}", value_id, operation);

    let producer = producer.lock().await;
    let command_id = Uuid::new_v4();
    let command = Command::UpdateValue {id: command_id,
                                        data : UpdateOperation {value_id: value_id,
                                                                operation: operation.operation,
                                                                value: operation.value}};
    let payload : String = serde_json::to_string(&command).expect ("Could not serialize command");

    match producer.send(FutureRecord::to(&config.commands_topic)
                        .payload(&payload)
                        .key(&format!("{}", &command_id)),
                        Duration::from_secs(0)).await {
        Ok(_) => info!("Succesfully sent command {:#?} to topic {}", command, &config.commands_topic),
        Err(why) => warn!("Error sending command: {:#?}", why)
    };

    Ok(StatusCode::ACCEPTED)
}
