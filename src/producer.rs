use crate::config::{Config};
use log::info;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::util::get_rdkafka_version;
use std::sync::Arc;
use tokio::sync::Mutex;

// allow having one producer shared across threads
pub type Producer = Arc<Mutex<FutureProducer>>;

pub fn init(config : &Config) -> Producer {

    let (_, version) = get_rdkafka_version();
    info!("librdkafka version: {}", version);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.broker)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    Arc::new(Mutex::new(producer))
}
