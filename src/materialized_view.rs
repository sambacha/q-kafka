use crate::commands_schema::{Value, UpdateOperation};
use crate::config::Config;
use crate::consumer;
use crate::db::Db;
use crate::db;
use crate::events_schema::Event;
use crate::inputs_schema::OperationType ;
use log::{debug, info, warn, error};
use maplit::hashmap;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Message;
use rdkafka::topic_partition_list::{TopicPartitionList, Offset};
use std::sync::Arc;

pub async fn run (config : Arc<Config>, db: Db) {

    let Config { broker, events_group_id, events_topic, .. } = &*config;
    let consumer = consumer::init (String::from (broker), String::from (events_group_id));

    consumer.subscribe(&[&events_topic])
        .expect("Can't subscribe to the specified topic");

    // NOTE : uses last offset stored in kafka
    let topic_map = hashmap!{
        (String::from (events_topic), 0) => Offset::Stored
    };

    let tpl : TopicPartitionList = TopicPartitionList::from_topic_map (&topic_map).unwrap ();
    consumer.assign (&tpl).expect ("Could not set topic partition list");

    loop {

        match consumer.recv().await {
            // NOTE: panics if topic does not exist
            Err(why) => panic!("Failed to read message from {:?} : {}", &tpl, why),
            Ok(m) => {

                match m.payload_view::<str>() {
                    None => warn!("Empty payload"),
                    Some(Ok(payload)) => {

                        debug!("payload: {}", payload);

                        // TODO : read avro
                        match serde_json::from_str::<Event>(payload) {
                            Ok (event) => {
                                info!("Received event: {:#?}, partition: {}, offset: {}, timestamp: {:?}", event, m.partition(), m.offset(), m.timestamp());

                                // run validation and emit events
                                match event {
                                    Event::ValueCreated { data, .. } => handle_value_created (db.clone (), data).await,
                                    Event::ValueUpdated { data, .. } => handle_value_updated (db.clone (), data).await
                                };

                            },
                            Err (why) => error!("Could not deserialize : {:?}", why)
                        };

                    },
                    Some(Err(e)) => error!("Error while deserializing payload: {:?}", e)
                };

                match consumer.commit_message(&m, CommitMode::Async) {
                    Err(why) => error!("Failed to commit message offset: {}", why),
                    Ok (_) => debug!("Commited message offset: {}", m.offset ())
                };

            }
        };

    }

}

async fn handle_value_created (db: Db, data : Value) {
    let Value { value_id, value } = data;
    db::insert (&db, value_id, value).await;
}

async fn handle_value_updated (db: Db, data : UpdateOperation) {

    let UpdateOperation { value_id, operation, value } = data;

    match operation {
        OperationType::ADD => {
            let current_value = db::get (&db, &value_id).await.unwrap ();
            let new_value = current_value + value;
            db::insert (&db, value_id, new_value).await;
        },
        OperationType::MULTIPLY => {
            let current_value = db::get (&db, &value_id).await.unwrap ();
            let new_value = current_value * value;
            db::insert (&db, value_id, new_value).await;
        },
    }

}
