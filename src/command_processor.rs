use crate::commands_schema::{Command, Value, UpdateOperation};
use crate::config::Config;
use crate::consumer;
use crate::events_schema::Event;
use crate::producer::Producer;
use crate::producer;
use log::{debug, info, warn, error};
use maplit::hashmap;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;
use rdkafka::topic_partition_list::{TopicPartitionList, Offset};
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub async fn run (config : Arc<Config>) {

    let Config { broker, commands_group_id, commands_topic, .. } = &*config;

    // in-memory state for validating commands
    let mut state = HashMap::<Uuid, f64>::new();
    let producer = producer::init (&config);
    let consumer = consumer::init (String::from (broker), String::from (commands_group_id));

    consumer.subscribe(&[&commands_topic])
        .expect("Can't subscribe to the specified topic");

    // NOTE : sets offset for replaying all commands on system restart, in a production system you would rather use the offset stored in Kafka
    // and a persistent state for command validation
    let topic_map = hashmap!{
        (String::from (commands_topic), 0) => Offset::Beginning
    };

    let tpl : TopicPartitionList = TopicPartitionList::from_topic_map (&topic_map).unwrap ();
    consumer.assign (&tpl).expect ("Could not set topic partition list");

    loop {

        match consumer.recv().await {
            // NOTE: panics if comands topic does not exist
            Err(why) => panic!("Failed to read message from {:?} : {}", &tpl, why),
            Ok(m) => {
                match m.payload_view::<str>() {
                    None => warn!("Empty command payload"),
                    Some(Ok(payload)) => {

                        debug!("payload: {}", payload);

                        // TODO : read avro
                        match serde_json::from_str::<Command>(payload) {
                            Ok (command) => {

                                info!("Received command: {:#?}, partition: {}, offset: {}, timestamp: {:?}", command, m.partition(), m.offset(), m.timestamp());

                                // run validation and emit events
                                match command {
                                    Command::CreateValue {id, data} => validate_create_value (id, data, &config.clone (), &mut state, producer.clone ()).await,
                                    Command::UpdateValue {id, data} => validate_update_value (id, data, &config.clone (), &mut state, producer.clone ()).await,
                                };
                            },
                            Err (why) => error!("Could not deserialize command: {:?}", why)
                        };

                    },
                    Some(Err(e)) => error!("Error while deserializing command payload: {:?}", e)
                };

                match consumer.commit_message(&m, CommitMode::Async) {
                    Err(why) => error!("Failed to commit message offset: {}", why),
                    Ok (_) => debug!("Commited message offset: {}", m.offset ())
                };

            }
        };

    }
}

/// implements business logic
async fn validate_create_value (
    command_id: Uuid,
    data : Value,
    config: &Config,
    state : &mut HashMap<Uuid, f64>,
    producer : Producer
) {

    info!("validating command {} with data {:?}", command_id, data);

    let value_id = data.value_id;
    match state.contains_key(&value_id) {
        true => error!("command {} rejected: value with id {} already exists", command_id, value_id),
        false => {
            // emit event
            let event_id = Uuid::new_v4();
            let event = Event::ValueCreated {id: event_id,
                                             parent: command_id,
                                             data: data.clone ()};
            let payload : String = serde_json::to_string(&event).expect ("Could not serialize event");
            let producer = producer.lock().await;

            match producer.send(FutureRecord::to(&config.events_topic)
                                .payload(&payload)
                                .key(&format!("{}", &event_id)),
                                Duration::from_secs(0)).await {
                Ok(_) => {
                    info!("Succesfully sent event {:#?} to topic {}", event, &config.events_topic);
                    state.insert (value_id, data.value);
                },
                Err(why) => warn!("Error sending event: {:#?}", why)
            };
        }
    }
}

/// implements business logic
async fn validate_update_value (
    command_id: Uuid,
    data : UpdateOperation,
    config: &Config,
    state : &mut HashMap<Uuid, f64>,
    producer : Producer
) {

    info!("validating command {} with data {:?}", command_id, data);

    let value_id = data.value_id;
    match state.contains_key(&value_id) {
        false => error!("command {} rejected: value with id {} does not exist", command_id, value_id),
        true => {
            // emit event
            let event_id = Uuid::new_v4();
            let event = Event::ValueUpdated {id: event_id,
                                             parent: command_id,
                                             data: data.clone ()};

            let payload : String = serde_json::to_string(&event).expect ("Could not serialize event");
            let producer = producer.lock().await;

            match producer.send(FutureRecord::to(&config.events_topic)
                                .payload(&payload)
                                .key(&format!("{}", &event_id)),
                                Duration::from_secs(0)).await {
                Ok(_) => info!("Succesfully sent event {:#?} to topic {}", event, &config.events_topic),
                Err(why) => warn!("Error sending event: {:#?}", why),
            };
        }
    }
}
