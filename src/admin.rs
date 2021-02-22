use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::admin::AdminOptions;
use std::sync::Arc;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::admin::NewTopic;
use rdkafka::admin::TopicReplication;
use log::info;

pub type KafkaAdmin = Arc<AdminClient<DefaultClientContext>>;

pub fn init (broker : &str) -> KafkaAdmin {

    let context = DefaultClientContext;

    let admin = ClientConfig::new ()
        .set("bootstrap.servers", broker)
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect ("Admin creation failed");

    Arc::new(admin)
}

pub async fn create_topic (admin: KafkaAdmin, topic_id : &str) {

    let admin = &*admin;

    let topic = NewTopic::new (topic_id, 1, TopicReplication::Fixed (3));

    match admin.create_topics (
        vec![&topic],
        &AdminOptions::new ()
    ).await {
        Err (why) => panic!("could not create topic {} {:?}", topic_id, why),
        Ok (_) => info!("created topic {}", topic_id)
    };

}
