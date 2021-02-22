use log::debug;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaResult;
use rdkafka::topic_partition_list::TopicPartitionList;

pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn commit_callback(&self, _result: KafkaResult<()>, offsets: &TopicPartitionList) {
        debug!("Committing offsets {:?}", offsets);
    }
}

pub type CustomConsumer = StreamConsumer<CustomContext>;

pub fn init (broker : String, group_id : String) -> CustomConsumer {

    let context = CustomContext;

    ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", broker)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false") // only commit the offsets explicitly
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed")
}
